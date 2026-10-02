//! Real Redis component tests, not workers or final-publication protocol tests.
//! Each case owns a unique namespace; cleanup runs after assertion panics too.

mod support;
use support::{Context, with_redis};

use std::{
    collections::HashMap,
    process::{Child, Command, Stdio},
    time::Duration,
};

use redis::AsyncConnectionConfig;
use swarmcrawl::{
    config::RedisConfig,
    jobs::{JobFailure, JobId, JobState, JobStore, StoreError},
    stats::WebStats,
    urls::CrawlUrl,
};
use tokio::task::JoinSet;

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn concurrent_canonical_submission_initializes_one_retained_seed() {
    with_redis(|context| async move {
        // Separate connections model independent clients, not a local mutex.
        let mut racers = JoinSet::new();
        for index in 0..32 {
            let context = context.clone();
            racers.spawn(async move {
                let base = CrawlUrl::parse(if index % 2 == 0 {
                    "HTTPS://EXAMPLE.org:443/docs/./#intro"
                } else {
                    "https://example.org/docs/#usage"
                })
                .unwrap();
                context.store().await.submit(&base).await.unwrap()
            });
        }
        let mut submissions = Vec::new();
        while let Some(result) = racers.join_next().await {
            submissions.push(result.unwrap());
        }
        let job = submissions[0].job;
        assert_eq!(
            submissions
                .iter()
                .filter(|submission| submission.created)
                .count(),
            1
        );
        assert!(submissions.iter().all(|submission| submission.job == job));
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let store = context.store().await;
        let snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(snapshot.base, base);
        assert_eq!(snapshot.state, JobState::Running);
        assert_eq!(
            (
                snapshot.discovered,
                snapshot.processed,
                snapshot.frontier,
                snapshot.in_flight,
                snapshot.successful_files
            ),
            (1, 0, 1, 0, 0)
        );
        assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
        let seed: Vec<String> = context
            .query(
                redis::cmd("LRANGE")
                    .arg(context.job_key(job, "frontier"))
                    .arg(0)
                    .arg(-1),
            )
            .await;
        assert_eq!(seed, vec![base.as_str()]);
        assert_eq!(
            store.stats("999999".parse().unwrap()).await,
            Err(StoreError::UnknownJob)
        );
        assert_eq!(
            store.snapshot("999999".parse().unwrap()).await,
            Err(StoreError::UnknownJob)
        );

        let other_base = CrawlUrl::parse("https://example.org/docs/?other=1").unwrap();
        let other = store.submit(&other_base).await.unwrap();
        assert!(other.created);
        assert_ne!(other.job, job);
        assert_eq!(store.snapshot(other.job).await.unwrap().base, other_base);
        assert_eq!(
            context
                .query::<u64>(redis::cmd("HLEN").arg(context.key("submissions")))
                .await,
            2
        );
        assert_eq!(
            context
                .query::<u64>(redis::cmd("SCARD").arg(context.key("active")))
                .await,
            2
        );
        drop(store);
        let reopened = context.store().await;
        let repeated = reopened.submit(&base).await.unwrap();
        assert!(!repeated.created);
        assert_eq!(repeated.job, job);
        assert_eq!(reopened.snapshot(job).await.unwrap(), snapshot);
        for key in context.keys().await {
            assert_eq!(context.query::<i64>(redis::cmd("TTL").arg(key)).await, -1);
        }
    })
    .await
    .unwrap();
}

/// Fabricate a valid completed result using a transaction solely to exercise the
/// read schema. This is NOT the worker/publication implementation (S04).
async fn completed_fixture(context: &Context, job: JobId, base: &CrawlUrl, words: u64) {
    redis::pipe()
        .atomic()
        .cmd("DEL")
        .arg(context.job_key(job, "frontier"))
        .ignore()
        .cmd("SADD")
        .arg(context.job_key(job, "seen"))
        .arg(base.resolve("a").unwrap().as_str())
        .arg(base.resolve("b").unwrap().as_str())
        .arg(base.resolve("missing").unwrap().as_str())
        .ignore()
        .cmd("HSET")
        .arg(context.job_key(job, "meta"))
        .arg("state")
        .arg("done")
        .arg("processed")
        .arg("4")
        .ignore()
        .cmd("HSET")
        .arg(context.job_key(job, "stats"))
        .arg("num_files")
        .arg("3")
        .arg("num_exts")
        .arg("2")
        .arg("total_word_count")
        .arg(words.to_string())
        .ignore()
        .cmd("HSET")
        .arg(context.job_key(job, "extensions"))
        .arg("html")
        .arg("2")
        .arg("jpg")
        .arg("1")
        .ignore()
        .cmd("SREM")
        .arg(context.key("active"))
        .arg(job.to_string())
        .ignore()
        .query_async::<()>(&mut context.connection.clone())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn result_read_contract_retains_exact_unsigned_words_and_terminal_identity() {
    with_redis(|context| async move {
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let store = context.store().await;
        let job = store.submit(&base).await.unwrap().job;
        completed_fixture(&context, job, &base, 0).await;
        for words in [9_007_199_254_740_993, (i64::MAX as u64) + 1, u64::MAX] {
            context
                .hash_set(
                    &context.job_key(job, "stats"),
                    "total_word_count",
                    &words.to_string(),
                )
                .await;
            assert_eq!(
                store.stats(job).await.unwrap(),
                WebStats {
                    num_files: 3,
                    num_exts: 2,
                    ext_counts: HashMap::from([("html".into(), 2), ("jpg".into(), 1)]),
                    total_word_count: words,
                }
            );
        }
        let snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(snapshot.state, JobState::Done);
        assert_eq!(
            (
                snapshot.discovered,
                snapshot.processed,
                snapshot.frontier,
                snapshot.in_flight,
                snapshot.successful_files
            ),
            (4, 4, 0, 0, 3)
        );
        drop(store);
        let reopened = context.store().await;
        assert_eq!(
            reopened.stats(job).await.unwrap().total_word_count,
            u64::MAX
        );
        assert!(!reopened.submit(&base).await.unwrap().created);
        assert_eq!(reopened.snapshot(job).await.unwrap(), snapshot);

        let missing_base = CrawlUrl::parse("https://example.org/missing/").unwrap();
        let empty_job = reopened.submit(&missing_base).await.unwrap().job;
        redis::pipe()
            .atomic()
            .cmd("DEL")
            .arg(context.job_key(empty_job, "frontier"))
            .ignore()
            .cmd("HSET")
            .arg(context.job_key(empty_job, "meta"))
            .arg("state")
            .arg("done")
            .arg("processed")
            .arg("1")
            .ignore()
            .cmd("SREM")
            .arg(context.key("active"))
            .arg(empty_job.to_string())
            .ignore()
            .query_async::<()>(&mut context.connection.clone())
            .await
            .unwrap();
        assert_eq!(
            reopened.stats(empty_job).await.unwrap(),
            WebStats::default()
        );

        let failed_base = CrawlUrl::parse("https://example.org/failed/").unwrap();
        let failed = reopened.submit(&failed_base).await.unwrap().job;
        redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(context.job_key(failed, "meta"))
            .arg("state")
            .arg("failed")
            .arg("failure")
            .arg("fetch")
            .ignore()
            .cmd("SREM")
            .arg(context.key("active"))
            .arg(failed.to_string())
            .ignore()
            .query_async::<()>(&mut context.connection.clone())
            .await
            .unwrap();
        assert_eq!(
            reopened.snapshot(failed).await.unwrap().state,
            JobState::Failed(JobFailure::Fetch)
        );
        assert_eq!(
            reopened.stats(failed).await,
            Err(StoreError::JobFailed(JobFailure::Fetch))
        );
        assert!(!reopened.submit(&failed_base).await.unwrap().created);
        assert_eq!(
            reopened.stats(job).await.unwrap().total_word_count,
            u64::MAX
        );
        for key in context.keys().await {
            assert_eq!(context.query::<i64>(redis::cmd("TTL").arg(key)).await, -1);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn malformed_storage_is_rejected_without_exposing_values_or_partial_stats() {
    with_redis(|context| async move {
        let base = CrawlUrl::parse("https://example.org/docs/?token=fixture-secret").unwrap();
        let store = context.store().await;
        let job = store.submit(&base).await.unwrap().job;
        let meta = context.job_key(job, "meta");
        for (field, bad, good) in [
            ("schema", "2", "1"),
            (
                "base",
                "https://user:fixture-secret@example.org/",
                base.as_str(),
            ),
            ("base", "https://EXAMPLE.org/docs/", base.as_str()),
            ("state", "fixture-secret", "running"),
            ("state", "done", "running"), // queued work cannot be done
            ("failure", "fetch", ""),
            ("processed", "-1", "0"),
            ("processed", "18446744073709551616", "0"),
            ("processed", "18446744073709551615", "0"), // checked addition overflow
            ("processed", "1", "0"),                    // breaks the discovered invariant
        ] {
            context.hash_set(&meta, field, bad).await;
            let error = store.snapshot(job).await.unwrap_err();
            assert!(matches!(error, StoreError::InvalidData(_)), "{error}");
            assert!(!format!("{error:?}: {error}").contains("fixture-secret"));
            context.hash_set(&meta, field, good).await;
        }
        let totals = context.job_key(job, "stats");
        for (field, bad) in [
            ("num_files", "18446744073709551616"),
            ("num_exts", "01"),
            ("total_word_count", "18446744073709551616"),
            ("total_word_count", "1"),
        ] {
            context.hash_set(&totals, field, bad).await;
            assert!(matches!(
                store.snapshot(job).await,
                Err(StoreError::InvalidData(_))
            ));
            context.hash_set(&totals, field, "0").await;
        }
        context
            .query::<usize>(redis::cmd("HDEL").arg(&totals).arg("num_files"))
            .await;
        assert!(matches!(
            store.stats(job).await,
            Err(StoreError::InvalidData(_))
        ));
        context.hash_set(&totals, "num_files", "0").await;
        let extensions = context.job_key(job, "extensions");
        for (extension, value) in [
            ("HTML", "1"),
            ("html", "0"),
            ("html", "-1"),
            ("html", "18446744073709551616"),
            ("html", "1"),
        ] {
            context.hash_set(&extensions, extension, value).await;
            assert!(matches!(
                store.snapshot(job).await,
                Err(StoreError::InvalidData(_))
            ));
            context
                .query::<usize>(redis::cmd("DEL").arg(&extensions))
                .await;
        }
        let frontier = context.job_key(job, "frontier");
        context
            .query::<usize>(redis::cmd("DEL").arg(&frontier))
            .await;
        context
            .query::<()>(redis::cmd("SET").arg(&frontier).arg("fixture-secret"))
            .await;
        let error = store.snapshot(job).await.unwrap_err();
        assert!(matches!(error, StoreError::Redis { .. }));
        assert!(!format!("{error:?}: {error}").contains("fixture-secret"));
        context
            .query::<usize>(redis::cmd("DEL").arg(&frontier))
            .await;
        context
            .query::<usize>(redis::cmd("RPUSH").arg(&frontier).arg(base.as_str()))
            .await;
        context.query::<usize>(redis::cmd("DEL").arg(&meta)).await;
        assert_eq!(
            store.snapshot(job).await,
            Err(StoreError::InvalidData("missing job metadata"))
        );
        // Never replace an identity whose retained storage has been damaged.
        assert_eq!(
            store.submit(&base).await,
            Err(StoreError::InvalidData("submission storage"))
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn submission_sequence_is_exact_and_corruption_never_creates_a_partial_job() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        context
            .query::<()>(
                redis::cmd("SET")
                    .arg(context.key("active"))
                    .arg("wrong-type"),
            )
            .await;
        assert_eq!(
            store.submit(&base).await,
            Err(StoreError::InvalidData("submission storage"))
        );
        assert_eq!(context.keys().await, vec![context.key("active")]);
        context
            .query::<usize>(redis::cmd("DEL").arg(context.key("active")))
            .await;

        for invalid in ["-1", "01", "9223372036854775808"] {
            context
                .query::<()>(
                    redis::cmd("SET")
                        .arg(context.key("next-job-id"))
                        .arg(invalid),
                )
                .await;
            assert_eq!(
                store.submit(&base).await,
                Err(StoreError::InvalidData("submission storage"))
            );
            assert_eq!(context.keys().await, vec![context.key("next-job-id")]);
        }
        context
            .query::<()>(
                redis::cmd("SET")
                    .arg(context.key("next-job-id"))
                    .arg("9223372036854775807"),
            )
            .await;
        assert_eq!(
            store.submit(&base).await,
            Err(StoreError::SequenceExhausted)
        );
        assert_eq!(context.keys().await, vec![context.key("next-job-id")]);
        context
            .query::<()>(
                redis::cmd("SET")
                    .arg(context.key("next-job-id"))
                    .arg("9007199254740992"),
            )
            .await;
        let first = store.submit(&base).await.unwrap();
        assert_eq!(first.job.to_string(), "9007199254740993");
        let second = store.submit(&base.resolve("?q=2").unwrap()).await.unwrap();
        assert_eq!(second.job.to_string(), "9007199254740994");
        assert_eq!(store.submit(&base).await.unwrap().job, first.job);
        assert_eq!(store.snapshot(first.job).await.unwrap().frontier, 1);

        // An unexpected preexisting job key consumes an ID gap only; there is no
        // submission/active identity or partially initialized seed for that ID.
        context
            .query::<()>(redis::cmd("SET").arg(context.key("next-job-id")).arg("0"))
            .await;
        context
            .hash_set(&context.key("job:1:meta"), "orphan", "fixture")
            .await;
        let fresh = base.resolve("?fresh=1").unwrap();
        assert_eq!(
            store.submit(&fresh).await,
            Err(StoreError::InvalidData("submission storage"))
        );
        for suffix in ["seen", "frontier", "in-flight", "stats", "extensions"] {
            assert_eq!(
                context
                    .query::<u64>(redis::cmd("EXISTS").arg(context.key(&format!("job:1:{suffix}"))))
                    .await,
                0
            );
        }
        assert_eq!(
            context
                .query::<u64>(redis::cmd("HLEN").arg(context.key("submissions")))
                .await,
            2
        );
        assert_eq!(
            context
                .query::<u64>(redis::cmd("SCARD").arg(context.key("active")))
                .await,
            2
        );
        assert_eq!(store.submit(&fresh).await.unwrap().job.to_string(), "2");
    })
    .await
    .unwrap();
}

// Switch between two internally consistent running fixtures. A non-transactional
// reader could observe impossible mixed counters/results. This checks read
// isolation only; claiming/completion correctness is still S04.
async fn write_running_fixture(context: &Context, job: JobId, base: &CrawlUrl, advanced: bool) {
    let mut pipe = redis::pipe();
    pipe.atomic()
        .cmd("DEL")
        .arg(context.job_key(job, "frontier"))
        .arg(context.job_key(job, "in-flight"))
        .arg(context.job_key(job, "extensions"))
        .arg(context.job_key(job, "seen"))
        .ignore()
        .cmd("SADD")
        .arg(context.job_key(job, "seen"))
        .arg(base.as_str())
        .ignore();
    if advanced {
        let child = base.resolve("child").unwrap();
        pipe.cmd("SADD")
            .arg(context.job_key(job, "seen"))
            .arg(child.as_str())
            .ignore()
            .cmd("HSET")
            .arg(context.job_key(job, "in-flight"))
            .arg(child.as_str())
            .arg("fixture-owner")
            .ignore()
            .cmd("HSET")
            .arg(context.job_key(job, "extensions"))
            .arg("html")
            .arg("1")
            .ignore();
    } else {
        pipe.cmd("RPUSH")
            .arg(context.job_key(job, "frontier"))
            .arg(base.as_str())
            .ignore();
    }
    let count = if advanced { "1" } else { "0" };
    pipe.cmd("HSET")
        .arg(context.job_key(job, "meta"))
        .arg("processed")
        .arg(count)
        .ignore()
        .cmd("HSET")
        .arg(context.job_key(job, "stats"))
        .arg("num_files")
        .arg(count)
        .arg("num_exts")
        .arg(count)
        .arg("total_word_count")
        .arg(if advanced { "42" } else { "0" })
        .ignore()
        .query_async::<()>(&mut context.connection.clone())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn snapshots_are_coherent_during_concurrent_fixture_transactions() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let writer_context = context.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            for index in 0..200 {
                write_running_fixture(&writer_context, job, &base, index % 2 == 0).await;
            }
        });
        tasks.spawn(async move {
            for _ in 0..200 {
                let snapshot = store.snapshot(job).await.unwrap();
                let observed = (
                    snapshot.discovered,
                    snapshot.processed,
                    snapshot.frontier,
                    snapshot.in_flight,
                    snapshot.successful_files,
                );
                assert!(
                    observed == (1, 0, 1, 0, 0) || observed == (2, 1, 0, 1, 1),
                    "mixed snapshot: {snapshot:?}"
                );
                assert_eq!(snapshot.state, JobState::Running);
                assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
            }
        });
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
}

// Child processes execute this same exact test in a controlled submission-only
// role. No extra CLI command or production test hook is necessary.
struct OwnedChild(Option<Child>);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn process_submission(mut child: OwnedChild) -> (JobId, bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while child.0.as_mut().unwrap().try_wait().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded submission process");
    let output = child.0.take().unwrap().wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "submission child failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("SWARMCRAWL_SUBMISSION "))
        .expect("child submission reply");
    let (id, created) = line.split_once(' ').unwrap();
    (id.parse().unwrap(), created.parse().unwrap())
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn independent_processes_share_submission_identity() {
    if let Ok(namespace) = std::env::var("SWARMCRAWL_TEST_SUBMISSION_NAMESPACE") {
        let url = std::env::var("SWARMCRAWL_REDIS_URL").unwrap();
        let config = RedisConfig::new(&url, 5).unwrap();
        let store = JobStore::connect_in_namespace(&config, &namespace)
            .await
            .unwrap();
        let connection_config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(5)))
            .set_response_timeout(Some(Duration::from_secs(10)));
        let mut connection = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection_with_config(&connection_config)
            .await
            .unwrap();
        redis::cmd("SADD")
            .arg(format!("{namespace}:process-ready"))
            .arg(std::process::id())
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
        let gate: Option<(String, String)> = redis::cmd("BLPOP")
            .arg(format!("{namespace}:process-gate"))
            .arg(5)
            .query_async(&mut connection)
            .await
            .unwrap();
        assert!(gate.is_some(), "submission process gate timed out");
        let base = CrawlUrl::parse(&format!(
            "HTTPS://EXAMPLE.org:443/docs/./#{}",
            std::process::id()
        ))
        .unwrap();
        let submission = store.submit(&base).await.unwrap();
        println!(
            "SWARMCRAWL_SUBMISSION {} {}",
            submission.job, submission.created
        );
        return;
    }
    with_redis(|context| async move {
        let mut children = Vec::new();
        for _ in 0..4 {
            children.push(OwnedChild(Some(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "independent_processes_share_submission_identity",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("SWARMCRAWL_TEST_SUBMISSION_NAMESPACE", &context.namespace)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            )));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while context
                .query::<u64>(redis::cmd("SCARD").arg(context.key("process-ready")))
                .await
                != 4
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all submission processes ready");
        context
            .query::<usize>(
                redis::cmd("RPUSH")
                    .arg(context.key("process-gate"))
                    .arg(&["go", "go", "go", "go"]),
            )
            .await;
        let mut replies = Vec::new();
        for child in children {
            replies.push(process_submission(child).await);
        }
        assert_eq!(replies.iter().filter(|(_, created)| *created).count(), 1);
        assert!(replies.iter().all(|(job, _)| *job == replies[0].0));
        assert_eq!(
            context
                .store()
                .await
                .snapshot(replies[0].0)
                .await
                .unwrap()
                .frontier,
            1
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn assertion_failure_cleanup_preserves_other_namespaces() {
    with_redis(|outer| async move {
        outer
            .query::<()>(redis::cmd("SET").arg(outer.key("sentinel")).arg("keep"))
            .await;
        let (send, receive) = tokio::sync::oneshot::channel();
        let outcome = with_redis(|inner| async move {
            inner
                .store()
                .await
                .submit(&CrawlUrl::parse("https://example.org/").unwrap())
                .await
                .unwrap();
            send.send(inner)
                .unwrap_or_else(|_| panic!("cleanup test receiver dropped"));
            panic!("intentional assertion failure to verify owned cleanup");
        })
        .await;
        assert!(outcome.unwrap_err().is_panic());
        let inner = receive.await.unwrap();
        assert!(inner.keys().await.is_empty());
        assert_eq!(
            outer
                .query::<String>(redis::cmd("GET").arg(outer.key("sentinel")))
                .await,
            "keep"
        );
    })
    .await
    .unwrap();
}
