//! Versioned Redis jobs: atomic submission, ownership/publication and coherent reads.
//! Stored integers are canonical decimal strings, never Lua floats.

mod frontier;
pub use frontier::{Completion, PageResult, WorkClaim, WorkerId};

use std::{collections::HashMap, error::Error, fmt, future::Future, str::FromStr, time::Duration};

use redis::{AsyncConnectionConfig, aio::MultiplexedConnection};

use crate::{config::RedisConfig, stats::WebStats, urls::CrawlUrl};

pub const DEFAULT_JOB_NAMESPACE: &str = "swarmcrawl:v1";
const SUBMIT_SCRIPT: &str = include_str!("jobs/submit.lua");
type Fields = HashMap<String, String>;
// All six reads execute within one MULTI/EXEC; no writer can interleave them.
type StoredSnapshot = (Fields, u64, u64, u64, Fields, Fields);

/// Namespace-local, monotonically allocated Redis IDs. They are not URLs/hashes
/// and carry no secrets. Parsing rejects noncanonical or out-of-range IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(u64);

impl FromStr for JobId {
    type Err = StoreError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let number: u64 = decimal(value, "job ID").map_err(|_| StoreError::InvalidJobId)?;
        if number == 0 || number > i64::MAX as u64 {
            return Err(StoreError::InvalidJobId);
        }
        Ok(Self(number))
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobFailure {
    Fetch,
    Statistics,
    Protocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Running,
    Done,
    /// Owner-requested terminal stop; partial totals are not final statistics.
    Aborted,
    Failed(JobFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobSnapshot {
    pub job: JobId,
    pub base: CrawlUrl,
    pub state: JobState,
    /// Unique discovered URLs, including the seed and waiting/owned URLs.
    pub discovered: u64,
    /// Completed URL attempts, including broken links and redirects, not just HTML.
    pub processed: u64,
    pub frontier: u64,
    pub in_flight: u64,
    /// Successful unique files; redirects and broken links do not count.
    pub successful_files: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Submission {
    pub job: JobId,
    pub created: bool,
}

/// A cheap-to-clone multiplexed connection, shared configuration deadline and
/// explicit namespace. All cluster clients must use the same namespace/database.
#[derive(Clone)]
pub struct JobStore {
    connection: MultiplexedConnection,
    namespace: String,
    timeout: Duration,
}

impl fmt::Debug for JobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobStore")
            .field("connection", &"[redacted]")
            .field("namespace", &self.namespace)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// Validate the shared namespace without making a connection.
pub fn validate_namespace(namespace: &str) -> Result<(), StoreError> {
    if namespace.is_empty()
        || namespace.len() > 128
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b":_-".contains(&byte))
    {
        return Err(StoreError::InvalidNamespace);
    }
    Ok(())
}

impl JobStore {
    pub async fn connect(config: &RedisConfig) -> Result<Self, StoreError> {
        Self::connect_in_namespace(config, DEFAULT_JOB_NAMESPACE).await
    }

    /// Alternate namespaces are for isolated tests/deployments, not per-command
    /// randomization. No API here clears Redis or applies expiration.
    pub async fn connect_in_namespace(
        config: &RedisConfig,
        namespace: &str,
    ) -> Result<Self, StoreError> {
        validate_namespace(namespace)?;
        let client = redis::Client::open(config.connection_info.clone())
            .map_err(|error| StoreError::redis("initialize client", error))?;
        let connection_config = AsyncConnectionConfig::new()
            .set_connection_timeout(None)
            .set_response_timeout(None);
        let connection = bounded(
            config.timeout,
            "connect",
            client.get_multiplexed_async_connection_with_config(&connection_config),
        )
        .await?;
        Ok(Self {
            connection,
            namespace: namespace.to_owned(),
            timeout: config.timeout,
        })
    }

    /// Atomically retain one identity, seen seed, frontier entry and running state.
    /// Canonicalization is required by the type. Concurrent equal bases return the
    /// same job; later submissions never restart that job, even after failure.
    pub async fn submit(&self, base: &CrawlUrl) -> Result<Submission, StoreError> {
        let mut connection = self.connection.clone();
        let reply: Vec<String> = bounded(
            self.timeout,
            "submit job",
            redis::cmd("EVAL")
                .arg(SUBMIT_SCRIPT)
                .arg(3)
                .arg(self.key("submissions"))
                .arg(self.key("next-job-id"))
                .arg(self.key("active"))
                .arg(&self.namespace)
                .arg(base.as_str())
                .query_async(&mut connection),
        )
        .await?;
        match reply.as_slice() {
            [kind, id] if kind == "created" || kind == "existing" => Ok(Submission {
                job: id.parse().map_err(|_| StoreError::InvalidData("job ID"))?,
                created: kind == "created",
            }),
            [kind] if kind == "exhausted" => Err(StoreError::SequenceExhausted),
            [kind] if kind == "invalid" => Err(StoreError::InvalidData("submission storage")),
            _ => Err(StoreError::InvalidData("submission reply")),
        }
    }

    /// Snapshot of retained submission IDs, including terminal jobs. Reuse the
    /// authoritative submission hash so older deployments need no index backfill.
    /// States are read separately; this is not a cluster-wide progress snapshot.
    pub async fn jobs(&self) -> Result<Vec<JobId>, StoreError> {
        let values: Vec<String> = bounded(
            self.timeout,
            "list jobs",
            redis::cmd("HVALS")
                .arg(self.key("submissions"))
                .query_async(&mut self.connection.clone()),
        )
        .await?;
        let mut jobs = values
            .iter()
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| StoreError::InvalidData("submission job ID"))
            })
            .collect::<Result<Vec<JobId>, _>>()?;
        jobs.sort_unstable();
        if jobs.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StoreError::InvalidData("duplicate submission job ID"));
        }
        Ok(jobs)
    }

    pub async fn snapshot(&self, job: JobId) -> Result<JobSnapshot, StoreError> {
        let (snapshot, _) = self.read(job).await?;
        Ok(snapshot)
    }

    /// Never expose running/failed/aborted aggregates as final WebStats. The final-state
    /// and result reads share a single transaction; publication must be atomic too
    /// (the frontier protocol), not an out-of-band finalization step.
    pub async fn stats(&self, job: JobId) -> Result<WebStats, StoreError> {
        let (snapshot, stats) = self.read(job).await?;
        match snapshot.state {
            JobState::Running => Err(StoreError::NotFinished),
            JobState::Failed(reason) => Err(StoreError::JobFailed(reason)),
            JobState::Aborted => Err(StoreError::JobAborted),
            JobState::Done => Ok(stats),
        }
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.namespace)
    }

    async fn read(&self, job: JobId) -> Result<(JobSnapshot, WebStats), StoreError> {
        let root = self.key(&format!("job:{job}"));
        let mut connection = self.connection.clone();
        let stored = bounded(
            self.timeout,
            "read job",
            redis::pipe()
                .atomic()
                .cmd("HGETALL")
                .arg(format!("{root}:meta"))
                .cmd("SCARD")
                .arg(format!("{root}:seen"))
                .cmd("LLEN")
                .arg(format!("{root}:frontier"))
                .cmd("HLEN")
                .arg(format!("{root}:in-flight"))
                .cmd("HGETALL")
                .arg(format!("{root}:stats"))
                .cmd("HGETALL")
                .arg(format!("{root}:extensions"))
                .query_async(&mut connection),
        )
        .await?;
        decode_snapshot(job, stored)
    }
}

async fn bounded<T>(
    timeout: Duration,
    operation: &'static str,
    request: impl Future<Output = redis::RedisResult<T>>,
) -> Result<T, StoreError> {
    tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| StoreError::Timeout { operation })?
        .map_err(|error| StoreError::redis(operation, error))
}

fn decode_snapshot(
    job: JobId,
    (meta, discovered, frontier, in_flight, totals, extensions): StoredSnapshot,
) -> Result<(JobSnapshot, WebStats), StoreError> {
    if meta.is_empty() {
        if discovered != 0
            || frontier != 0
            || in_flight != 0
            || !totals.is_empty()
            || !extensions.is_empty()
        {
            return Err(StoreError::InvalidData("missing job metadata"));
        }
        return Err(StoreError::UnknownJob);
    }
    if meta.len() != 5 || field(&meta, "schema")? != "1" {
        return Err(StoreError::InvalidData("job schema"));
    }
    let base_string = field(&meta, "base")?;
    let base = CrawlUrl::parse(base_string).map_err(|_| StoreError::InvalidData("base URL"))?;
    if base.as_str() != base_string {
        return Err(StoreError::InvalidData("canonical base URL"));
    }
    let failure = field(&meta, "failure")?;
    let state = match (field(&meta, "state")?, failure) {
        ("running", "") => JobState::Running,
        ("done", "") => JobState::Done,
        ("aborted", "") => JobState::Aborted,
        ("failed", "fetch") => JobState::Failed(JobFailure::Fetch),
        ("failed", "statistics") => JobState::Failed(JobFailure::Statistics),
        ("failed", "protocol") => JobState::Failed(JobFailure::Protocol),
        _ => return Err(StoreError::InvalidData("job state/failure")),
    };
    let processed: u64 = decimal(field(&meta, "processed")?, "processed count")?;
    let stats = decode_stats(totals, extensions)?;
    let accounted = processed
        .checked_add(frontier)
        .and_then(|count| count.checked_add(in_flight))
        .ok_or(StoreError::InvalidData("progress overflow"))?;
    if discovered == 0
        || accounted != discovered
        || u64::try_from(stats.num_files).map_err(|_| StoreError::InvalidData("file count"))?
            > processed
        || (state == JobState::Done && (frontier != 0 || in_flight != 0))
        || (state == JobState::Running && frontier == 0 && in_flight == 0)
    {
        return Err(StoreError::InvalidData("progress invariants"));
    }
    Ok((
        JobSnapshot {
            job,
            base,
            state,
            discovered,
            processed,
            frontier,
            in_flight,
            successful_files: stats.num_files,
        },
        stats,
    ))
}

fn decode_stats(totals: Fields, extensions: Fields) -> Result<WebStats, StoreError> {
    if totals.len() != 3 {
        return Err(StoreError::InvalidData("statistics fields"));
    }
    let stats = WebStats {
        num_files: decimal(field(&totals, "num_files")?, "file count")?,
        num_exts: decimal(field(&totals, "num_exts")?, "extension count")?,
        total_word_count: decimal(field(&totals, "total_word_count")?, "word count")?,
        ext_counts: extensions
            .into_iter()
            .map(|(extension, count)| Ok((extension, decimal(&count, "extension file count")?)))
            .collect::<Result<_, StoreError>>()?,
    };
    stats
        .validate()
        .map_err(|_| StoreError::InvalidData("statistics invariants"))?;
    Ok(stats)
}

fn field<'a>(fields: &'a Fields, name: &'static str) -> Result<&'a str, StoreError> {
    fields
        .get(name)
        .map(String::as_str)
        .ok_or(StoreError::InvalidData(name))
}

fn decimal<T: FromStr>(value: &str, field: &'static str) -> Result<T, StoreError> {
    if value.is_empty()
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || (value.len() > 1 && value.starts_with('0'))
    {
        return Err(StoreError::InvalidData(field));
    }
    value.parse().map_err(|_| StoreError::InvalidData(field))
}

/// Errors carry only safe categories/field names, never Redis messages, stored
/// URLs, credential strings or payloads. An interrupted write can be ambiguous;
/// no automatic retry/recovery is promised by this baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    InvalidNamespace,
    InvalidJobId,
    InvalidWorkerId,
    OwnershipMismatch,
    UnknownJob,
    NotFinished,
    JobFailed(JobFailure),
    JobAborted,
    SequenceExhausted,
    WorkerSequenceExhausted,
    InvalidData(&'static str),
    Timeout {
        operation: &'static str,
    },
    Redis {
        operation: &'static str,
        kind: redis::ErrorKind,
    },
}

impl StoreError {
    fn redis(operation: &'static str, error: redis::RedisError) -> Self {
        Self::Redis {
            operation,
            kind: error.kind(),
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidNamespace => f.write_str("invalid job namespace; use 1–128 ASCII letters, digits, colons, underscores or hyphens"),
            Self::InvalidJobId => f.write_str("invalid job ID; use a positive decimal integer at most 9223372036854775807"),
            Self::InvalidWorkerId => f.write_str("invalid worker identity; use 1–128 ASCII letters, digits, underscores or hyphens"),
            Self::OwnershipMismatch => f.write_str("claim ownership mismatch; stop publication and investigate protocol state"),
            Self::UnknownJob => f.write_str("unknown job; verify its ID, Redis database and namespace"),
            Self::NotFinished => f.write_str("job is still running; final statistics are not available"),
            Self::JobFailed(reason) => write!(f, "job failed ({reason:?}); final statistics are not available"),
            Self::JobAborted => f.write_str("job was aborted; final statistics are not available"),
            Self::SequenceExhausted => f.write_str("job ID sequence is exhausted; select a new namespace for new jobs"),
            Self::WorkerSequenceExhausted => f.write_str("worker identity sequence is exhausted; select a new namespace for new jobs and nodes"),
            Self::InvalidData(field) => write!(f, "invalid Redis job data ({field}); verify the schema and exclusive namespace use"),
            Self::Timeout { operation } => write!(f, "Redis {operation} timed out; verify the service and network; a write may have taken effect"),
            Self::Redis { operation, kind } => write!(f, "Redis {operation} failed ({kind:?}); verify the service, authentication and key types"),
        }
    }
}

impl Error for StoreError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(entries: &[(&str, &str)]) -> Fields {
        entries
            .iter()
            .map(|(key, value)| ((*key).into(), (*value).into()))
            .collect()
    }

    #[test]
    fn aborted_snapshots_preserve_progress_invariants_and_require_no_failure_reason() {
        let id = "1".parse().unwrap();
        let mut meta = fields(&[
            ("schema", "1"),
            ("base", "https://example.org/"),
            ("state", "aborted"),
            ("processed", "1"),
            ("failure", ""),
        ]);
        let totals = fields(&[
            ("num_files", "1"),
            ("num_exts", "1"),
            ("total_word_count", "2"),
        ]);
        let exts = fields(&[("html", "1")]);
        let (snapshot, _) =
            decode_snapshot(id, (meta.clone(), 4, 2, 1, totals.clone(), exts.clone())).unwrap();
        assert_eq!(snapshot.state, JobState::Aborted);
        assert_eq!(
            (snapshot.processed, snapshot.frontier, snapshot.in_flight),
            (1, 2, 1)
        );
        assert!(
            decode_snapshot(id, (meta.clone(), 3, 2, 1, totals.clone(), exts.clone())).is_err()
        );
        meta.insert("failure".into(), "fetch".into());
        assert!(decode_snapshot(id, (meta, 4, 2, 1, totals, exts)).is_err());
    }

    #[test]
    fn ids_and_stored_numbers_require_exact_canonical_decimals() {
        for input in [
            "",
            "0",
            "01",
            "+1",
            "-1",
            " 1",
            "1.0",
            "1e3",
            "9223372036854775808",
        ] {
            assert_eq!(input.parse::<JobId>(), Err(StoreError::InvalidJobId));
        }
        for input in ["1", "9007199254740993", "9223372036854775807"] {
            assert_eq!(input.parse::<JobId>().unwrap().to_string(), input);
        }
        assert_eq!(decimal::<u64>(&u64::MAX.to_string(), "test"), Ok(u64::MAX));
        assert_eq!(
            decimal::<usize>(&usize::MAX.to_string(), "test"),
            Ok(usize::MAX)
        );
        for input in ["", "-1", "+1", "00", "01", "1.5", "18446744073709551616"] {
            assert!(decimal::<u64>(input, "test").is_err());
        }
        assert!(decimal::<usize>(&((usize::MAX as u128) + 1).to_string(), "test").is_err());
    }

    #[test]
    fn statistics_decoder_preserves_unsigned_range_and_rejects_bad_data() {
        let mut totals = fields(&[
            ("num_files", "1"),
            ("num_exts", "1"),
            ("total_word_count", "18446744073709551615"),
        ]);
        let extensions = fields(&[("html", "1")]);
        assert_eq!(
            decode_stats(totals.clone(), extensions.clone())
                .unwrap()
                .total_word_count,
            u64::MAX
        );
        for bad in ["-1", "18446744073709551616", "1e3", "01"] {
            totals.insert("total_word_count".into(), bad.into());
            assert!(decode_stats(totals.clone(), extensions.clone()).is_err());
        }
        totals.insert("total_word_count".into(), "0".into());
        for bad in [
            fields(&[("HTML", "1")]),
            fields(&[("html", "0")]),
            fields(&[("html", "2")]),
        ] {
            assert!(decode_stats(totals.clone(), bad).is_err());
        }
        totals.remove("num_files");
        assert!(decode_stats(totals.clone(), extensions).is_err());
        // File counts use the target's full usize range, not signed Redis INCR.
        totals.insert("num_files".into(), usize::MAX.to_string());
        let maximum_files = decode_stats(
            totals,
            HashMap::from([("html".into(), usize::MAX.to_string())]),
        )
        .unwrap();
        assert_eq!(maximum_files.num_files, usize::MAX);
        assert_eq!(maximum_files.ext_counts["html"], usize::MAX);
    }

    #[test]
    fn stored_payloads_and_server_errors_never_reach_diagnostics() {
        let raw =
            redis::RedisError::from((redis::ErrorKind::AuthenticationFailed, "fixture-secret"));
        let error = StoreError::redis("submit job", raw);
        assert!(!format!("{error:?}: {error}").contains("fixture-secret"));
        let id = "1".parse().unwrap();
        let meta = fields(&[
            ("schema", "1"),
            ("base", "https://example.org/?token=fixture-secret"),
            ("state", "fixture-secret"),
            ("processed", "0"),
            ("failure", ""),
        ]);
        let error =
            decode_snapshot(id, (meta, 1, 1, 0, HashMap::new(), HashMap::new())).unwrap_err();
        assert!(!format!("{error:?}: {error}").contains("fixture-secret"));
    }

    #[tokio::test]
    async fn bad_namespace_is_rejected_before_connecting() {
        let config = RedisConfig::new("redis://127.0.0.1:1/0", 1).unwrap();
        for namespace in ["", "has space", "*", "line\nbreak", &"a".repeat(129)] {
            assert!(matches!(
                JobStore::connect_in_namespace(&config, namespace).await,
                Err(StoreError::InvalidNamespace)
            ));
        }
    }

    #[tokio::test]
    async fn silent_server_cannot_block_job_store_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config =
            RedisConfig::new(&format!("redis://{}/0", listener.local_addr().unwrap()), 1).unwrap();
        let connect = tokio::spawn(async move { JobStore::connect(&config).await });
        let (_socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), connect)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                result,
                Err(StoreError::Timeout {
                    operation: "connect"
                })
            ),
            "{result:?}"
        );
    }
}
