//! User commands use only the Redis job API; no fetcher or worker execution.

use std::{
    error::Error,
    fmt,
    io::{self, Write},
    time::Duration,
};

use swarmcrawl::{
    jobs::{JobId, JobSnapshot, JobState, JobStore, StoreError},
    stats::WebStats,
    urls::{CrawlUrl, UrlError},
};

const FOLLOW_INTERVAL: Duration = Duration::from_millis(500);

/// Validate the entire batch before connecting/submitting. Invalid input cannot
/// leave a surprising partly-submitted batch, and diagnostics never echo URLs.
pub fn validate_urls(inputs: &[String]) -> Result<Vec<CrawlUrl>, CommandError> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            CrawlUrl::parse(input).map_err(|error| CommandError::InvalidUrl {
                input: index + 1,
                error,
            })
        })
        .collect()
}

pub async fn submit(store: &JobStore, bases: &[CrawlUrl]) -> Result<(), CommandError> {
    let mut output = io::stdout().lock();
    // Redis operations are sequential, not one batch transaction. Flush each ID
    // so earlier successes remain usable even if a later write/output fails.
    for (index, base) in bases.iter().enumerate() {
        let submission = store
            .submit(base)
            .await
            .map_err(|error| CommandError::Submit {
                input: index + 1,
                error,
            })?;
        writeln!(
            output,
            "job {}  input {}  {}",
            submission.job,
            index + 1,
            if submission.created {
                "created"
            } else {
                "existing"
            }
        )
        .and_then(|()| output.flush())
        .map_err(|_| CommandError::Output)?;
    }
    Ok(())
}

pub async fn status(store: &JobStore, job: JobId, follow: bool) -> Result<(), CommandError> {
    if !follow {
        return watch_status(store, job, false).await;
    }
    // Reads can safely be canceled: this command owns no work and mutates no job.
    // Poll Ctrl-C first to install its handler before starting the first read.
    tokio::select! {
        biased;
        interrupted = tokio::signal::ctrl_c() => {
            interrupted.map_err(|_| CommandError::Signal)?;
            Err(CommandError::Interrupted)
        }
        result = watch_status(store, job, true) => result,
    }
}

async fn watch_status(store: &JobStore, job: JobId, follow: bool) -> Result<(), CommandError> {
    let mut previous = None;
    let mut output = io::stdout().lock();
    loop {
        let snapshot = store
            .snapshot(job)
            .await
            .map_err(|error| CommandError::Job { job, error })?;
        if previous.as_ref() != Some(&snapshot) {
            write_snapshot(&mut output, &snapshot).map_err(|_| CommandError::Output)?;
            output.flush().map_err(|_| CommandError::Output)?;
        }
        match snapshot.state {
            JobState::Done => return Ok(()),
            JobState::Failed(reason) => {
                return Err(CommandError::Job {
                    job,
                    error: StoreError::JobFailed(reason),
                });
            }
            JobState::Running if !follow => return Ok(()),
            JobState::Running => {}
        }
        previous = Some(snapshot);
        // Sleep after the bounded read: slow Redis cannot cause catch-up bursts.
        tokio::time::sleep(FOLLOW_INTERVAL).await;
    }
}

pub async fn stats(store: &JobStore, job: JobId) -> Result<(), CommandError> {
    let stats = store
        .stats(job)
        .await
        .map_err(|error| CommandError::Job { job, error })?;
    let mut output = io::stdout().lock();
    write_stats(&mut output, &stats)
        .and_then(|()| output.flush())
        .map_err(|_| CommandError::Output)
}

fn write_snapshot(output: &mut impl Write, snapshot: &JobSnapshot) -> io::Result<()> {
    write!(
        output,
        "job {}  crawled {}  frontier {}  in flight {}  files {}  discovered {}  ",
        snapshot.job,
        snapshot.processed,
        snapshot.frontier,
        snapshot.in_flight,
        snapshot.successful_files,
        snapshot.discovered
    )?;
    match snapshot.state {
        JobState::Running => writeln!(output, "running"),
        JobState::Done => writeln!(output, "done"),
        JobState::Failed(reason) => writeln!(
            output,
            "failed ({})",
            match reason {
                swarmcrawl::jobs::JobFailure::Fetch => "fetch",
                swarmcrawl::jobs::JobFailure::Statistics => "statistics",
                swarmcrawl::jobs::JobFailure::Protocol => "protocol",
            }
        ),
    }
}

fn write_stats(output: &mut impl Write, stats: &WebStats) -> io::Result<()> {
    writeln!(
        output,
        "files: {}   extensions: {}   words: {}",
        stats.num_files, stats.num_exts, stats.total_word_count
    )?;
    let mut extensions: Vec<_> = stats.ext_counts.iter().collect();
    extensions.sort_unstable_by_key(|(extension, _)| *extension);
    for (extension, count) in extensions {
        writeln!(output, "  {extension} {count}")?;
    }
    Ok(())
}

#[derive(Debug)]
pub enum CommandError {
    InvalidUrl { input: usize, error: UrlError },
    Submit { input: usize, error: StoreError },
    Job { job: JobId, error: StoreError },
    Output,
    Signal,
    Interrupted,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUrl { input, error } => write!(f, "submit input {input}: {error}; no URLs submitted"),
            Self::Submit { input, error } => write!(f, "submit input {input}: {error}; earlier printed IDs remain valid; resubmit to retrieve retained IDs (this write may have taken effect)"),
            Self::Job { job, error } => write!(f, "job {job}: {error}"),
            Self::Output => f.write_str("cannot write CLI output; any submissions may have taken effect; resubmit to retrieve retained IDs"),
            Self::Signal => f.write_str("cannot wait for Ctrl-C; check host/runtime signal support"),
            Self::Interrupted => f.write_str("status follow interrupted; the job continues unchanged"),
        }
    }
}

impl Error for CommandError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn stats_order_is_stable_and_large_unsigned_words_are_exact() {
        let stats = WebStats {
            num_files: 3,
            num_exts: 3,
            ext_counts: HashMap::from([("jpg".into(), 1), ("html".into(), 1), ("jpeg".into(), 1)]),
            total_word_count: u64::MAX,
        };
        let mut output = Vec::new();
        write_stats(&mut output, &stats).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "files: 3   extensions: 3   words: 18446744073709551615\n  html 1\n  jpeg 1\n  jpg 1\n"
        );
        let mut output = Vec::new();
        write_stats(&mut output, &WebStats::default()).unwrap();
        assert_eq!(output, b"files: 0   extensions: 0   words: 0\n");
    }

    #[test]
    fn batch_validation_errors_are_indexed_and_redacted() {
        let inputs = [
            "https://example.org/?token=fixture-query",
            "https://user:fixture-secret@example.org/",
        ]
        .map(str::to_owned);
        let error = validate_urls(&inputs).unwrap_err();
        assert!(error.to_string().contains("submit input 2"));
        assert!(error.to_string().contains("no URLs submitted"));
        assert!(!format!("{error}: {error:?}").contains("fixture-"));
        assert_eq!(validate_urls(&inputs[..1]).unwrap().len(), 1);
    }
}
