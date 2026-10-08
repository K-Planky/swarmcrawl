//! One owned URL → one GET → an outcome ready for atomic Redis publication.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use encoding_rs::{Encoding, UTF_8};
use reqwest::{Client, StatusCode, header};
use tokio::sync::Semaphore;

use crate::{
    config::FetchConfig,
    diagnostics::{Activity, Diagnostics, Stage, Timer},
    html::{HtmlError, is_html_content_type, parse_html},
    jobs::PageResult,
    urls::{CrawlScope, CrawlUrl},
};

pub const MAX_NODE_REQUESTS: usize = 10;
/// Acquire before spawning: bound both running and Tokio-queued CPU closures.
/// The node's owned-task cap separately bounds bodies waiting for this budget.
pub const MAX_NODE_CPU_WORK: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOutcome {
    pub result: PageResult,
    /// Allowed redirect targets and HTML links are discoveries, never inline GETs.
    /// Redis, not this fetcher, arbitrates cluster-wide ownership/deduplication.
    pub discoveries: Vec<CrawlUrl>,
}

/// Construct once per node and clone across every job/task. The client and budget
/// are private: all requests through this interface must hold the same permit.
#[derive(Clone)]
pub struct Fetcher {
    client: Client,
    budget: Arc<Semaphore>,
    diagnostics: Diagnostics,
    cpu_budget: Arc<Semaphore>,
    #[cfg(test)]
    cpu_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    publication_gate: Option<Arc<Semaphore>>,
}

impl fmt::Debug for Fetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fetcher")
            .field("max_requests", &MAX_NODE_REQUESTS)
            .field("max_cpu_work", &MAX_NODE_CPU_WORK)
            .finish_non_exhaustive()
    }
}

impl Fetcher {
    pub fn new(config: FetchConfig) -> Result<Self, FetchError> {
        Self::build(config, None)
    }

    /// Explicit additional trust for an isolated HTTPS fixture runner. The normal
    /// CLI never calls this; certificate and hostname verification remain enabled.
    pub fn with_root_certificate(
        config: FetchConfig,
        certificate: reqwest::Certificate,
    ) -> Result<Self, FetchError> {
        Self::build(config, Some(certificate))
    }

    pub fn with_diagnostics(mut self, diagnostics: Diagnostics) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    pub(crate) fn diagnostics(&self) -> &Diagnostics {
        &self.diagnostics
    }

    fn build(
        config: FetchConfig,
        certificate: Option<reqwest::Certificate>,
    ) -> Result<Self, FetchError> {
        let mut builder = Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .referer(false)
            // retry::never() disables reqwest's outer retries, not hyper-util's
            // stale-connection recovery. Audited reqwest 0.13.5 / hyper-util 0.1.21
            // / hyper 1.11.1 return a request for recovery only BEFORE HTTP/1
            // dispatch dequeues it (before serialization or any request bytes).
            // Started requests are never replayed. Re-audit on transport upgrades.
            .pool_max_idle_per_host(MAX_NODE_REQUESTS)
            .pool_idle_timeout(Duration::from_secs(30))
            .http1_only()
            .gzip(true)
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .user_agent(concat!("swarmcrawl/", env!("CARGO_PKG_VERSION")));
        if let Some(certificate) = certificate {
            builder = builder.tls_certs_merge([certificate]);
        }
        let client = builder.build().map_err(|_| FetchError::ClientSetup)?;
        Ok(Self {
            client,
            budget: Arc::new(Semaphore::new(MAX_NODE_REQUESTS)),
            diagnostics: Diagnostics::default(),
            cpu_budget: Arc::new(Semaphore::new(MAX_NODE_CPU_WORK)),
            #[cfg(test)]
            cpu_hook: None,
            #[cfg(test)]
            publication_gate: None,
        })
    }

    /// The caller must already own this URL. Do not retry an error: publish a
    /// terminal job failure rather than incomplete statistics or a second GET.
    pub async fn fetch(
        &self,
        scope: &CrawlScope,
        url: &CrawlUrl,
    ) -> Result<FetchOutcome, FetchError> {
        self.fetch_owned(scope, url, self.diagnostics.timer(Stage::HttpAdmission))
            .await
    }

    pub(crate) async fn fetch_owned(
        &self,
        scope: &CrawlScope,
        url: &CrawlUrl,
        admission: Timer,
    ) -> Result<FetchOutcome, FetchError> {
        if !scope.contains(url) {
            return Err(FetchError::OutsideScope);
        }
        let permit = self
            .budget
            .acquire()
            .await
            .map_err(|_| FetchError::BudgetClosed)?;
        drop(admission);
        let transfer = self.diagnostics.timer(Stage::HttpTransfer);
        let active_http = self.diagnostics.enter(Activity::Http);
        let response = self
            .client
            .get(url.as_str())
            .send()
            .await
            .map_err(|error| network_error(&error, FetchError::Transport))?;
        let status = response.status();

        if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
            let location = response
                .headers()
                .get(header::LOCATION)
                .ok_or(FetchError::InvalidRedirect)?
                .to_str()
                .map_err(|_| FetchError::InvalidRedirect)?;
            let discoveries = url
                .resolve(location)
                .ok()
                .filter(|target| scope.contains(target))
                .into_iter()
                .collect();
            return Ok(FetchOutcome {
                result: PageResult::NoFile,
                discoveries,
            });
        }
        if status.is_client_error()
            && !matches!(
                status,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
            )
        {
            return Ok(FetchOutcome {
                result: PageResult::NoFile,
                discoveries: Vec::new(),
            });
        }
        if !status.is_success() || status == StatusCode::PARTIAL_CONTENT {
            return Err(FetchError::UnexpectedStatus(status.as_u16()));
        }

        let mut content_types = response.headers().get_all(header::CONTENT_TYPE).iter();
        let content_type = content_types
            .next()
            .map(|value| value.to_str().map_err(|_| FetchError::InvalidHeader))
            .transpose()?
            .unwrap_or_default();
        if content_types.next().is_some() {
            return Err(FetchError::InvalidHeader);
        }
        if !is_html_content_type(content_type) {
            // Do not consume a non-HTML body, even when the suffix says .html.
            // Response drops BEFORE the permit, including all early-return paths.
            return Ok(FetchOutcome {
                result: PageResult::File { html_word_count: 0 },
                discoveries: Vec::new(),
            });
        }
        // Gzip is decoded by reqwest and its encoding header removed. Reject
        // encodings we cannot decode instead of parsing compressed bytes as HTML.
        for value in response.headers().get_all(header::CONTENT_ENCODING) {
            if !value
                .to_str()
                .map_err(|_| FetchError::InvalidHeader)?
                .trim()
                .eq_ignore_ascii_case("identity")
            {
                return Err(FetchError::UnsupportedContentEncoding);
            }
        }
        let mime: mime::Mime = content_type
            .parse()
            .map_err(|_| FetchError::InvalidHeader)?;
        let charset = mime.get_param(mime::CHARSET).map(|value| value.as_str());
        let encoding = match charset {
            Some(label) => {
                Encoding::for_label(label.as_bytes()).ok_or(FetchError::UnsupportedCharset)?
            }
            None => UTF_8,
        };
        let bytes = response
            .bytes()
            .await
            .map_err(|error| network_error(&error, FetchError::Body))?;
        // Full network body is now consumed/dropped; CPU parsing needs no permit.
        drop(active_http);
        drop(transfer);
        drop(permit);
        let scope = scope.clone();
        let url = url.clone();
        let diagnostics = self.diagnostics.clone();
        #[cfg(test)]
        let hook = self.cpu_hook.clone();
        self.cpu_work(move || {
            #[cfg(test)]
            if let Some(hook) = hook {
                hook();
            }
            let decode = diagnostics.timer(Stage::Decode);
            let (source, _, malformed) = encoding.decode(&bytes);
            drop(decode);
            if malformed {
                return Err(FetchError::InvalidTextEncoding);
            }
            let parse = diagnostics.timer(Stage::Parse);
            let parsed = parse_html(&scope, &url, &source).map_err(FetchError::Html)?;
            drop(parse);
            Ok(FetchOutcome {
                result: PageResult::File {
                    html_word_count: parsed.word_count,
                },
                discoveries: parsed.links,
            })
        })
        .await?
    }

    async fn cpu_work<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> Result<T, FetchError> {
        let admission = self.diagnostics.timer(Stage::CpuAdmission);
        let permit = self
            .cpu_budget
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| FetchError::CpuBudgetClosed)?;
        let diagnostics = self.diagnostics.clone();
        tokio::task::spawn_blocking(move || {
            // Includes Tokio blocking-pool waiting in admission, not parse time.
            drop(admission);
            // Keep the permit IN the closure: dropping a fetch future cannot
            // free a CPU slot while its non-abortable blocking work still runs.
            let _permit = permit;
            let _active = diagnostics.enter(Activity::Cpu);
            work()
        })
        .await
        .map_err(|_| FetchError::CpuTaskFailed)
    }

    #[cfg(test)]
    pub(crate) fn with_publication_gate(mut self, gate: Arc<Semaphore>) -> Self {
        self.publication_gate = Some(gate);
        self
    }

    #[cfg(test)]
    pub(crate) async fn wait_publication_gate(&self) {
        if let Some(gate) = &self.publication_gate {
            gate.acquire().await.unwrap().forget();
        }
    }

    #[cfg(test)]
    pub(crate) fn with_cpu_hook(mut self, hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.cpu_hook = Some(hook);
        self
    }
}

fn network_error(error: &reqwest::Error, fallback: FetchError) -> FetchError {
    if error.is_timeout() {
        FetchError::Timeout
    } else {
        fallback
    }
}

/// No raw reqwest error/source, URL, Location, header value or query is retained.
/// Application nodes add safe job/node context and publish terminal job failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchError {
    ClientSetup,
    BudgetClosed,
    CpuBudgetClosed,
    CpuTaskFailed,
    OutsideScope,
    Timeout,
    Transport,
    Body,
    UnexpectedStatus(u16),
    InvalidHeader,
    InvalidRedirect,
    UnsupportedContentEncoding,
    UnsupportedCharset,
    InvalidTextEncoding,
    Html(HtmlError),
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientSetup => {
                f.write_str("cannot initialize HTTP client; check TLS/runtime setup")
            }
            Self::BudgetClosed => f.write_str("HTTP request budget unexpectedly closed"),
            Self::CpuBudgetClosed => f.write_str("HTML CPU budget unexpectedly closed"),
            Self::CpuTaskFailed => f.write_str(
                "HTML CPU task panicked or was canceled; crawl is incomplete (no retry)",
            ),
            Self::OutsideScope => f.write_str("refusing to fetch URL outside its job scope"),
            Self::Timeout => {
                f.write_str("HTTP request deadline exceeded; crawl is incomplete (no retry)")
            }
            Self::Transport => f.write_str(
                "HTTP connection/request failed; check network/TLS reachability (no retry)",
            ),
            Self::Body => f.write_str(
                "HTML body transfer/decompression failed; crawl is incomplete (no retry)",
            ),
            Self::UnexpectedStatus(status) => write!(
                f,
                "HTTP status {status} cannot establish a complete file (no retry)"
            ),
            Self::InvalidHeader => {
                f.write_str("invalid HTTP content header; cannot classify/decode HTML safely")
            }
            Self::InvalidRedirect => {
                f.write_str("redirect has no valid Location header; crawl is incomplete")
            }
            Self::UnsupportedContentEncoding => f.write_str(
                "unsupported HTML Content-Encoding; only identity and gzip are supported",
            ),
            Self::UnsupportedCharset => f.write_str("unsupported HTML charset label"),
            Self::InvalidTextEncoding => {
                f.write_str("malformed HTML text encoding; refusing lossy parsing")
            }
            Self::Html(error) => write!(f, "HTML extraction failed: {error}"),
        }
    }
}

impl Error for FetchError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canceled_waiters_do_not_release_running_or_queued_cpu_permits_and_failures_are_explicit() {
        // Force three admitted closures to queue behind one running closure.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            use std::sync::{
                Condvar, Mutex,
                atomic::{AtomicUsize, Ordering},
            };
            let fetcher = Fetcher::new(FetchConfig::default()).unwrap();
            let gate = Arc::new((Mutex::new(false), Condvar::new()));
            struct Release(Arc<(Mutex<bool>, Condvar)>);
            impl Drop for Release {
                fn drop(&mut self) {
                    *self.0.0.lock().unwrap() = true;
                    self.0.1.notify_all();
                }
            }
            let release = Release(gate.clone());
            let started = Arc::new(AtomicUsize::new(0));
            let mut tasks = tokio::task::JoinSet::new();
            for _ in 0..2 * MAX_NODE_CPU_WORK {
                let fetcher = fetcher.clone();
                let gate = gate.clone();
                let started = started.clone();
                tasks.spawn(async move {
                    fetcher
                        .cpu_work(move || {
                            let mut released = gate.0.lock().unwrap();
                            started.fetch_add(1, Ordering::SeqCst);
                            while !*released {
                                let (next, expired) = gate
                                    .1
                                    .wait_timeout(released, Duration::from_secs(10))
                                    .unwrap();
                                released = next;
                                if expired.timed_out() && !*released {
                                    drop(released);
                                    panic!("CPU test gate exceeded its safety deadline");
                                }
                            }
                        })
                        .await
                });
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while fetcher.cpu_budget.available_permits() != 0
                    || started.load(Ordering::SeqCst) != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            assert_eq!(started.load(Ordering::SeqCst), 1);
            assert_eq!(fetcher.cpu_budget.available_permits(), 0);
            drop(release);
            tokio::time::timeout(Duration::from_secs(5), async {
                while fetcher.cpu_budget.available_permits() != MAX_NODE_CPU_WORK {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(started.load(Ordering::SeqCst), MAX_NODE_CPU_WORK);
            let failed = fetcher.cpu_work(|| panic!("test CPU panic")).await;
            assert_eq!(failed, Err::<(), _>(FetchError::CpuTaskFailed));
            assert_eq!(fetcher.cpu_budget.available_permits(), MAX_NODE_CPU_WORK);
            fetcher.cpu_budget.close();
            assert_eq!(
                fetcher.cpu_work(|| ()).await,
                Err(FetchError::CpuBudgetClosed)
            );
        });
    }

    #[test]
    fn interface_is_send_and_diagnostics_have_no_url_details() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Fetcher>();
        assert_send_sync::<FetchOutcome>();
        let fetcher = Fetcher::new(FetchConfig::default()).unwrap();
        assert_eq!(fetcher.budget.available_permits(), 10);
        assert!(format!("{fetcher:?}").contains("max_requests: 10"));
        let error = reqwest::Client::new()
            .get("http://example.invalid/?token=fixture-query")
            .header("bad header", "fixture-secret")
            .build()
            .unwrap_err();
        let safe = network_error(&error, FetchError::Transport);
        assert!(!format!("{safe}: {safe:?}").contains("fixture-"));
        assert!(safe.source().is_none());
    }
}
