//! One owned URL → one GET → an outcome ready for atomic Redis publication.

use std::{error::Error, fmt, sync::Arc};

use encoding_rs::{Encoding, UTF_8};
use reqwest::{Client, StatusCode, header};
use tokio::sync::Semaphore;

use crate::{
    config::FetchConfig,
    html::{HtmlError, is_html_content_type, parse_html},
    jobs::PageResult,
    urls::{CrawlScope, CrawlUrl},
};

pub const MAX_NODE_REQUESTS: usize = 10;

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
}

impl fmt::Debug for Fetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fetcher")
            .field("max_requests", &MAX_NODE_REQUESTS)
            .finish_non_exhaustive()
    }
}

impl Fetcher {
    pub fn new(config: FetchConfig) -> Result<Self, FetchError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .referer(false)
            // Do not reuse stale HTTP/1 connections through a lower-level
            // transparent canceled-request retry. TLS/client state is shared.
            .pool_max_idle_per_host(0)
            .http1_only()
            .gzip(true)
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .user_agent(concat!("swarmcrawl/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| FetchError::ClientSetup)?;
        Ok(Self {
            client,
            budget: Arc::new(Semaphore::new(MAX_NODE_REQUESTS)),
        })
    }

    /// The caller must already own this URL. Do not retry an error: publish a
    /// terminal job failure rather than incomplete statistics or a second GET.
    pub async fn fetch(
        &self,
        scope: &CrawlScope,
        url: &CrawlUrl,
    ) -> Result<FetchOutcome, FetchError> {
        if !scope.contains(url) {
            return Err(FetchError::OutsideScope);
        }
        let permit = self
            .budget
            .acquire()
            .await
            .map_err(|_| FetchError::BudgetClosed)?;
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
        drop(permit);
        let (source, _, malformed) = encoding.decode(&bytes);
        if malformed {
            return Err(FetchError::InvalidTextEncoding);
        }
        let parsed = parse_html(scope, url, &source).map_err(FetchError::Html)?;
        Ok(FetchOutcome {
            result: PageResult::File {
                html_word_count: parsed.word_count,
            },
            discoveries: parsed.links,
        })
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
