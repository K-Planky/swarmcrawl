//! Atomic ownership/publication; HTTP tasks receive a private-field typed claim.

use std::{collections::HashSet, fmt, str::FromStr};

use crate::urls::{CrawlScope, CrawlUrl};

use super::{JobFailure, JobId, JobStore, StoreError, bounded};

const CLAIM_SCRIPT: &str = concat!(
    include_str!("protocol.lua"),
    "\n",
    include_str!("claim.lua")
);
const ABORT_SCRIPT: &str = concat!(
    include_str!("protocol.lua"),
    "\n",
    include_str!("abort.lua")
);
const COMPLETE_SCRIPT: &str = concat!(
    include_str!("protocol.lua"),
    "\n",
    include_str!("complete.lua")
);

/// Caller-selected process identity. Nodes should use a fresh identity at startup;
/// it is not a lease and does not make lost claims recoverable.
#[derive(Clone, PartialEq, Eq)]
pub struct WorkerId(String);

impl FromStr for WorkerId {
    type Err = StoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        {
            return Err(StoreError::InvalidWorkerId);
        }
        Ok(Self(value.into()))
    }
}

impl fmt::Debug for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WorkerId([redacted])")
    }
}

/// Only JobStore::claim constructs this value. Cloning is allowed so accidental
/// repeated publication can be rejected safely, not counted again.
#[derive(Debug, Clone)]
pub struct WorkClaim {
    job: JobId,
    base: CrawlUrl,
    url: CrawlUrl,
    worker: WorkerId,
    namespace: String,
}

impl WorkClaim {
    pub fn job(&self) -> JobId {
        self.job
    }

    pub fn base(&self) -> &CrawlUrl {
        &self.base
    }

    pub fn url(&self) -> &CrawlUrl {
        &self.url
    }
}

/// Existence/classification is the fetcher's responsibility, not Redis's. The
/// extension is derived from the claimed URL, never supplied by a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageResult {
    File { html_word_count: u64 },
    NoFile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    Published {
        done: bool,
    },
    AlreadyCompleted,
    /// Deliberate user abort: discard this outcome without shutting down the node.
    Aborted,
    /// A terminal failure freezes partial results and outstanding work. It is
    /// never successful completion, even when a network task drained normally.
    Failed(JobFailure),
}

impl JobStore {
    /// Atomically abort a running job. Returns false for any retained terminal
    /// state; unknown/corrupt jobs fail without writes. Never retries ambiguous writes.
    pub async fn abort(&self, job: JobId) -> Result<bool, StoreError> {
        let reply: Vec<String> = bounded(
            self.timeout,
            "abort job",
            self.worker_command(ABORT_SCRIPT, job)
                .query_async(&mut self.connection.clone()),
        )
        .await?;
        match reply.as_slice() {
            [kind] if kind == "aborted" => Ok(true),
            [kind] if kind == "unchanged" => Ok(false),
            _ => Err(protocol_error(&reply)),
        }
    }

    /// Allocate a fresh process identity across hosts using the shared namespace.
    /// Retain the sequence with the job state; never retry an ambiguous write.
    pub async fn allocate_worker(&self) -> Result<WorkerId, StoreError> {
        let reply: Vec<String> = bounded(
            self.timeout,
            "allocate worker identity",
            redis::cmd("EVAL")
                .arg(include_str!("worker.lua"))
                .arg(1)
                .arg(self.key("next-worker-id"))
                .query_async(&mut self.connection.clone()),
        )
        .await?;
        match reply.as_slice() {
            [kind, identity] if kind == "worker" => identity
                .parse()
                .map_err(|_| StoreError::InvalidData("allocated worker identity")),
            [kind] if kind == "exhausted" => Err(StoreError::WorkerSequenceExhausted),
            _ => Err(StoreError::InvalidData("worker identity sequence")),
        }
    }

    /// Advisory scheduling snapshot. Jobs can become terminal after this read;
    /// claim rechecks state atomically. No scheduler or HTTP task is started here.
    pub async fn active_jobs(&self) -> Result<Vec<JobId>, StoreError> {
        let values: Vec<String> = bounded(
            self.timeout,
            "list active jobs",
            redis::cmd("SMEMBERS")
                .arg(self.key("active"))
                .query_async(&mut self.connection.clone()),
        )
        .await?;
        let mut jobs = values
            .iter()
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| StoreError::InvalidData("active job ID"))
            })
            .collect::<Result<Vec<JobId>, _>>()?;
        jobs.sort_by_key(|job| job.0);
        Ok(jobs)
    }

    /// None means no waiting work now, not completion: other owners may still
    /// discover links. Terminal jobs also return None and cannot be reopened.
    pub async fn claim(
        &self,
        job: JobId,
        worker: &WorkerId,
    ) -> Result<Option<WorkClaim>, StoreError> {
        let mut command = self.worker_command(CLAIM_SCRIPT, job);
        let reply: Vec<String> = bounded(
            self.timeout,
            "claim URL",
            command
                .arg(&worker.0)
                .query_async(&mut self.connection.clone()),
        )
        .await?;
        match reply.as_slice() {
            [kind] if kind == "idle" => Ok(None),
            [kind, base, url] if kind == "claimed" => {
                let base = canonical_url(base)?;
                let url = canonical_url(url)?;
                if !CrawlScope::new(base.clone()).contains(&url) {
                    return Err(StoreError::InvalidData("claimed URL scope"));
                }
                Ok(Some(WorkClaim {
                    job,
                    base,
                    url,
                    worker: worker.clone(),
                    namespace: self.namespace.clone(),
                }))
            }
            _ => Err(protocol_error(&reply)),
        }
    }

    /// Atomically publish one outcome and its discoveries, release ownership,
    /// and finalize if globally idle. Scope-filter and deduplicate before EVAL;
    /// Redis SADD is still the cross-process deduplication boundary.
    pub async fn complete(
        &self,
        claim: &WorkClaim,
        result: PageResult,
        discoveries: &[CrawlUrl],
    ) -> Result<Completion, StoreError> {
        let (outcome, words) = match result {
            PageResult::File { html_word_count } => ("file", html_word_count),
            PageResult::NoFile => ("no-file", 0),
        };
        let scope = CrawlScope::new(claim.base.clone());
        let mut seen = HashSet::new();
        let children: Vec<&str> = discoveries
            .iter()
            .filter(|url| scope.contains(url) && seen.insert(url.as_str()))
            .map(CrawlUrl::as_str)
            .collect();
        self.publish(claim, outcome, words, &children).await
    }

    /// Fail only a still-owned running claim. Freeze diagnostics, stop new claims
    /// and refuse final stats; no automatic retries, reset or partial success.
    pub async fn fail(
        &self,
        claim: &WorkClaim,
        reason: JobFailure,
    ) -> Result<Completion, StoreError> {
        let outcome = match reason {
            JobFailure::Fetch => "fetch",
            JobFailure::Statistics => "statistics",
            JobFailure::Protocol => "protocol",
        };
        self.publish(claim, outcome, 0, &[]).await
    }

    fn worker_command(&self, script: &str, job: JobId) -> redis::Cmd {
        let root = self.key(&format!("job:{job}"));
        let mut command = redis::cmd("EVAL");
        command.arg(script).arg(7);
        for suffix in [
            "meta",
            "seen",
            "frontier",
            "in-flight",
            "stats",
            "extensions",
        ] {
            command.arg(format!("{root}:{suffix}"));
        }
        command
            .arg(self.key("active"))
            .arg(job.to_string())
            .arg(usize::MAX.to_string());
        command
    }

    async fn publish(
        &self,
        claim: &WorkClaim,
        outcome: &str,
        words: u64,
        children: &[&str],
    ) -> Result<Completion, StoreError> {
        if claim.namespace != self.namespace {
            return Err(StoreError::OwnershipMismatch);
        }
        let mut command = self.worker_command(COMPLETE_SCRIPT, claim.job);
        command
            .arg(&claim.worker.0)
            .arg(claim.url.as_str())
            .arg(claim.base.as_str())
            .arg(outcome)
            .arg(words.to_string())
            .arg(claim.url.extension())
            .arg(children);
        let reply: Vec<String> = bounded(
            self.timeout,
            "publish URL outcome",
            command.query_async(&mut self.connection.clone()),
        )
        .await?;
        match reply.as_slice() {
            [kind, state] if kind == "published" && (state == "running" || state == "done") => {
                Ok(Completion::Published {
                    done: state == "done",
                })
            }
            [kind] if kind == "already" => Ok(Completion::AlreadyCompleted),
            [kind] if kind == "aborted" => Ok(Completion::Aborted),
            [kind, failure] if kind == "failed" => {
                let reason = match failure.as_str() {
                    "fetch" => JobFailure::Fetch,
                    "statistics" => JobFailure::Statistics,
                    "protocol" => JobFailure::Protocol,
                    _ => return Err(StoreError::InvalidData("protocol failure reply")),
                };
                Ok(Completion::Failed(reason))
            }
            _ => Err(protocol_error(&reply)),
        }
    }
}

fn canonical_url(value: &str) -> Result<CrawlUrl, StoreError> {
    let url = CrawlUrl::parse(value).map_err(|_| StoreError::InvalidData("claimed URL"))?;
    if url.as_str() != value {
        return Err(StoreError::InvalidData("canonical claimed URL"));
    }
    Ok(url)
}

fn protocol_error(reply: &[String]) -> StoreError {
    match reply.first().map(String::as_str) {
        Some("unknown") => StoreError::UnknownJob,
        Some("ownership") => StoreError::OwnershipMismatch,
        _ => StoreError::InvalidData("frontier protocol"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_identity_has_a_bounded_secret_safe_representation() {
        for bad in ["", "has space", "a:b", "fixture-secret?", &"a".repeat(129)] {
            let error = bad.parse::<WorkerId>().unwrap_err();
            assert_eq!(error, StoreError::InvalidWorkerId);
            assert!(!format!("{error}: {error:?}").contains("fixture-secret"));
        }
        let worker: WorkerId = "fixture-secret_123".parse().unwrap();
        assert!(!format!("{worker:?}").contains("fixture-secret"));
    }
}
