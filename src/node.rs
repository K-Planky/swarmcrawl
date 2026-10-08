//! Bounded owned tasks, fair advisory job selection, and stop-claiming/drain lifecycle.

use std::{collections::VecDeque, error::Error, fmt, future::Future, time::Duration};

use tokio::{task::JoinSet, time::MissedTickBehavior};

use crate::{
    diagnostics::{Activity, Stage, Timer},
    fetch::{FetchError, Fetcher},
    html::HtmlError,
    jobs::{Completion, JobFailure, JobId, JobStore, StoreError, WorkClaim},
    urls::CrawlScope,
};

const JOB_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// A small amount of look-ahead lets HTTP overlap CPU/publication without an
/// unbounded frontier preclaim or body queue. Shared across every job in this node.
pub const MAX_NODE_OWNED_TASKS: usize = 20;

/// Stay available for later submissions. Exactly one fetcher is shared by every
/// owned task. At most 20 whole tasks may be owned, independently of the ten
/// HTTP permits and four CPU permits. This bounds claims, buffered body count,
/// and pending publications while allowing these phases to overlap.
///
/// Shutdown and unexpected errors stop new claims, but never cancel an in-progress
/// Redis claim/write: finish that bounded operation, then drain all owned tasks.
/// An ambiguous Redis error or task panic may strand its claim; no recovery/retry
/// is promised. A failed job remains failed, not a fabricated empty success.
pub async fn run_node(
    store: JobStore,
    fetcher: Fetcher,
    shutdown: impl Future<Output = Result<(), NodeError>>,
) -> Result<(), NodeError> {
    let worker = store
        .allocate_worker()
        .await
        .map_err(|error| NodeError::Store { job: None, error })?;
    eprintln!("node {} ready; polling Redis for jobs", std::process::id());
    tokio::pin!(shutdown);
    let mut tasks = JoinSet::new();
    let mut candidates = VecDeque::new();
    let mut cursor = None;
    let mut poll = tokio::time::interval(JOB_POLL_INTERVAL);
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut outcome = loop {
        // Observe shutdown/completed task errors before attempting another claim.
        // Claim awaits are deliberately not raced against shutdown: cancellation
        // could leave ownership acquired in Redis without a corresponding task.
        tokio::select! {
            biased;
            stopped = &mut shutdown => break stopped,
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = task_outcome(joined.expect("nonempty owned tasks")) {
                    break Err(error);
                }
                // A finished parent may have made an idle job runnable. Refresh
                // before refilling slots instead of monopolizing them with a
                // different job's already-cached frontier.
                poll.reset_immediately();
            }
            // Refresh ahead of ready scheduling, so a busy job cannot indefinitely
            // delay discovery of a newly submitted job.
            _ = poll.tick(), if tasks.len() < MAX_NODE_OWNED_TASKS => {
                match store.active_jobs().await {
                    Ok(jobs) => candidates = after_cursor(jobs, cursor),
                    Err(error) => break Err(NodeError::Store { job: None, error }),
                }
                // A healthy but slow Redis read must not make an already overdue
                // tick win forever ahead of claims. Start the next interval now.
                poll.reset();
            }
            _ = std::future::ready(()), if tasks.len() < MAX_NODE_OWNED_TASKS && !candidates.is_empty() => {
                let job = candidates.pop_front().expect("nonempty candidates");
                match store.claim(job, &worker).await {
                    Ok(Some(claim)) => {
                        cursor = Some(job);
                        let store = store.clone();
                        let fetcher = fetcher.clone();
                        let admission = fetcher.diagnostics().timer(Stage::HttpAdmission);
                        let owned = fetcher.diagnostics().enter(Activity::Owned);
                        tasks.spawn(async move {
                            let _owned = owned;
                            crawl_owned(&store, &fetcher, claim, admission).await
                        });
                        candidates.push_back(job);
                    }
                    Ok(None) => {} // Idle now, not proof of global completion.
                    Err(error) => break Err(NodeError::Store { job: Some(job), error }),
                }
            }
        }
    };

    match &outcome {
        Ok(()) => eprintln!(
            "node {} stopping claims; draining owned work",
            std::process::id()
        ),
        Err(error) => eprintln!(
            "node {} stopping claims after error: {error}; draining owned work",
            std::process::id()
        ),
    }
    // Do not drop JoinSet on ordinary shutdown/error: that would abort owners.
    while let Some(joined) = tasks.join_next().await {
        if let Err(error) = task_outcome(joined) {
            eprintln!("node {} drain error: {error}", std::process::id());
            if outcome.is_ok() {
                outcome = Err(error);
            }
        }
    }
    fetcher.diagnostics().report();
    eprintln!("node {} drained; exiting", std::process::id());
    outcome
}

fn after_cursor(mut jobs: Vec<JobId>, cursor: Option<JobId>) -> VecDeque<JobId> {
    // Sorted IDs resume after the last successful claim, wrapping once. New jobs
    // get a turn even while one large frontier remains nonempty.
    if let Some(cursor) = cursor {
        let split = jobs.partition_point(|job| *job <= cursor);
        jobs.rotate_left(split);
    }
    jobs.into()
}

async fn crawl_owned(
    store: &JobStore,
    fetcher: &Fetcher,
    claim: WorkClaim,
    admission: Timer,
) -> Result<(), NodeError> {
    let job = claim.job();
    let scope = CrawlScope::new(claim.base().clone());
    let fetched = fetcher.fetch_owned(&scope, claim.url(), admission).await;
    let publication = fetcher.diagnostics().timer(Stage::Publication);
    #[cfg(test)]
    fetcher.wait_publication_gate().await;
    let completion = match &fetched {
        Ok(outcome) => {
            store
                .complete(&claim, outcome.result, &outcome.discoveries)
                .await
        }
        Err(error) => store.fail(&claim, failure_category(*error)).await,
    }
    .map_err(|error| NodeError::Store {
        job: Some(job),
        error,
    })?;
    drop(publication);
    // Abort discards late HTTP/decoding outcomes, not node CPU execution faults.
    // A parser panic used to surface as an owned-task panic even after abort;
    // moving it to a blocking worker must not silently hide that operational error.
    if let Err(error) = fetched
        && (completion != Completion::Aborted
            || matches!(
                error,
                FetchError::CpuTaskFailed | FetchError::CpuBudgetClosed
            ))
    {
        return Err(NodeError::Fetch { job, error });
    }
    match completion {
        Completion::Aborted => Ok(()),
        Completion::Published { done } => {
            if done {
                eprintln!("node {} job {job}: done", std::process::id());
            }
            Ok(())
        }
        Completion::Failed(reason) => Err(NodeError::JobFailed { job, reason }),
        Completion::AlreadyCompleted => Err(NodeError::DuplicateCompletion { job }),
    }
}

fn failure_category(error: FetchError) -> JobFailure {
    match error {
        FetchError::Html(HtmlError::WordCountOverflow) => JobFailure::Statistics,
        FetchError::OutsideScope
        | FetchError::BudgetClosed
        | FetchError::CpuBudgetClosed
        | FetchError::CpuTaskFailed
        | FetchError::ClientSetup
        | FetchError::Html(HtmlError::PageOutsideScope) => JobFailure::Protocol,
        _ => JobFailure::Fetch,
    }
}

fn task_outcome(
    joined: Result<Result<(), NodeError>, tokio::task::JoinError>,
) -> Result<(), NodeError> {
    // JoinError may contain panic payloads; never copy them into diagnostics.
    joined.map_err(|_| NodeError::TaskFailed)?
}

/// Register handlers before starting work. A second signal does not abort drain;
/// a forced kill still loses claims and remains outside the supported model.
#[cfg(unix)]
pub fn shutdown_signal() -> Result<impl Future<Output = Result<(), NodeError>>, NodeError> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt()).map_err(|_| NodeError::Signal)?;
    let mut terminate = signal(SignalKind::terminate()).map_err(|_| NodeError::Signal)?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
        }
        // Tokio's installed OS handlers do not revert to default behavior when
        // these streams drop; further signals therefore do not abort the drain.
        Ok(())
    })
}

#[cfg(not(unix))]
pub fn shutdown_signal() -> Result<impl Future<Output = Result<(), NodeError>>, NodeError> {
    Ok(async { tokio::signal::ctrl_c().await.map_err(|_| NodeError::Signal) })
}

#[derive(Debug)]
pub enum NodeError {
    Store {
        job: Option<JobId>,
        error: StoreError,
    },
    Fetch {
        job: JobId,
        error: FetchError,
    },
    JobFailed {
        job: JobId,
        reason: JobFailure,
    },
    DuplicateCompletion {
        job: JobId,
    },
    TaskFailed,
    Signal,
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store { job, error } => {
                if let Some(job) = job {
                    write!(f, "job {job}: ")?;
                }
                error.fmt(f)
            }
            Self::Fetch { job, error } => write!(f, "job {job}: {error}"),
            Self::JobFailed { job, reason } => write!(f, "job {job} failed ({reason:?}); final statistics are unavailable"),
            Self::DuplicateCompletion { job } => write!(f, "job {job}: unexpected repeated ownership completion; investigate node/protocol state"),
            Self::TaskFailed => f.write_str("owned task panicked or was canceled; its claim may be stranded; investigate node code (no recovery)"),
            Self::Signal => f.write_str("cannot register/wait for shutdown signals; check host/runtime support"),
        }
    }
}

impl Error for NodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_rotation_wraps_and_admits_new_jobs() {
        let ids = || ["1", "3", "5"].map(|id| id.parse().unwrap()).to_vec();
        for (cursor, expected) in [
            (None, ["1", "3", "5"]),
            (Some("1"), ["3", "5", "1"]),
            (Some("2"), ["3", "5", "1"]),
            (Some("3"), ["5", "1", "3"]),
            (Some("6"), ["1", "3", "5"]),
        ] {
            assert_eq!(
                after_cursor(ids(), cursor.map(|id| id.parse().unwrap())),
                expected.map(|id| id.parse().unwrap())
            );
        }
        assert!(after_cursor(Vec::new(), Some("1".parse().unwrap())).is_empty());
    }

    #[test]
    fn operational_statistics_and_protocol_failures_are_distinct() {
        assert_eq!(failure_category(FetchError::Timeout), JobFailure::Fetch);
        assert_eq!(
            failure_category(FetchError::UnexpectedStatus(503)),
            JobFailure::Fetch
        );
        assert_eq!(
            failure_category(FetchError::Html(HtmlError::WordCountOverflow)),
            JobFailure::Statistics
        );
        assert_eq!(
            failure_category(FetchError::OutsideScope),
            JobFailure::Protocol
        );
        assert_eq!(
            failure_category(FetchError::Html(HtmlError::PageOutsideScope)),
            JobFailure::Protocol
        );
        for error in [FetchError::CpuTaskFailed, FetchError::CpuBudgetClosed] {
            assert_eq!(failure_category(error), JobFailure::Protocol);
        }
    }
}
