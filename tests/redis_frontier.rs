//! Real Redis protocol tests; mock page outcomes, not HTTP fetch evidence.
mod support;

use std::{
    collections::{HashMap, HashSet},
    process::{Child, Command, Stdio},
    time::Duration,
};

use support::{Context, with_redis};
use swarmcrawl::{
    config::RedisConfig,
    jobs::{Completion, JobFailure, JobId, JobState, JobStore, PageResult, StoreError, WorkerId},
    stats::WebStats,
    urls::CrawlUrl,
};
use tokio::task::JoinSet;

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn abort_is_idempotent_freezes_late_publications_and_preserves_terminal_jobs() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/abort/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let claim = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let before = store.snapshot(job).await.unwrap();
        let mut racers = JoinSet::new();
        for _ in 0..32 {
            let context = context.clone();
            racers.spawn(async move { context.store().await.abort(job).await.unwrap() });
        }
        let mut changes = 0;
        while let Some(result) = racers.join_next().await {
            changes += usize::from(result.unwrap());
        }
        assert_eq!(changes, 1);
        let mut expected = before;
        expected.state = JobState::Aborted;
        assert_eq!(store.snapshot(job).await.unwrap(), expected);
        assert_eq!(store.stats(job).await, Err(StoreError::JobAborted));
        assert!(store.active_jobs().await.unwrap().is_empty());
        assert!(store.claim(job, &worker(1)).await.unwrap().is_none());
        for _ in 0..2 {
            assert_eq!(
                store
                    .complete(&claim, file(7), &[base.resolve("late").unwrap()])
                    .await
                    .unwrap(),
                Completion::Aborted
            );
            assert_eq!(
                store.fail(&claim, JobFailure::Fetch).await.unwrap(),
                Completion::Aborted
            );
        }
        assert_eq!(store.snapshot(job).await.unwrap(), expected);
        assert_eq!(store.submit(&base).await.unwrap().job, job);
        assert!(!store.submit(&base).await.unwrap().created);
        assert_eq!(
            store.abort("999".parse().unwrap()).await,
            Err(StoreError::UnknownJob)
        );

        let done = store
            .submit(&base.resolve("done").unwrap())
            .await
            .unwrap()
            .job;
        let claim = store.claim(done, &worker(0)).await.unwrap().unwrap();
        store.complete(&claim, file(2), &[]).await.unwrap();
        let stats = store.stats(done).await.unwrap();
        assert!(!store.abort(done).await.unwrap());
        assert_eq!(store.stats(done).await.unwrap(), stats);
        let failed = store
            .submit(&base.resolve("failed").unwrap())
            .await
            .unwrap()
            .job;
        let claim = store.claim(failed, &worker(0)).await.unwrap().unwrap();
        store.fail(&claim, JobFailure::Fetch).await.unwrap();
        let before = store.snapshot(failed).await.unwrap();
        assert!(!store.abort(failed).await.unwrap());
        assert_eq!(store.snapshot(failed).await.unwrap(), before);

        let corrupt = store
            .submit(&base.resolve("corrupt").unwrap())
            .await
            .unwrap()
            .job;
        context
            .hash_set(
                &context.job_key(corrupt, "meta"),
                "processed",
                "fixture-secret",
            )
            .await;
        assert!(matches!(
            store.abort(corrupt).await,
            Err(StoreError::InvalidData(_))
        ));
        assert_eq!(
            context
                .query::<String>(
                    redis::cmd("HGET")
                        .arg(context.job_key(corrupt, "meta"))
                        .arg("state")
                )
                .await,
            "running"
        );
        assert_eq!(store.active_jobs().await.unwrap(), [corrupt]);
        assert_eq!(store.jobs().await.unwrap(), [job, done, failed, corrupt]);
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn abort_races_claim_and_final_publication_without_partial_transitions() {
    with_redis(|context| async move {
        let store = context.store().await;
        for index in 0..32 {
            let base = CrawlUrl::parse(&format!("https://example.org/race/{index}/")).unwrap();
            let job = store.submit(&base).await.unwrap().job;
            let peer = context.store().await;
            let worker = worker(index);
            let (claim, aborted) = tokio::join!(store.claim(job, &worker), peer.abort(job));
            assert!(aborted.unwrap());
            if let Some(claim) = claim.unwrap() {
                assert_eq!(
                    store.complete(&claim, file(1), &[]).await.unwrap(),
                    Completion::Aborted
                );
            }
            assert_eq!(store.snapshot(job).await.unwrap().state, JobState::Aborted);
            assert!(store.claim(job, &worker).await.unwrap().is_none());

            let job = store
                .submit(&base.resolve("final").unwrap())
                .await
                .unwrap()
                .job;
            let claim = store.claim(job, &worker).await.unwrap().unwrap();
            let (published, aborted) =
                tokio::join!(store.complete(&claim, file(3), &[]), peer.abort(job));
            match (published.unwrap(), aborted.unwrap()) {
                (Completion::Published { done: true }, false) => {
                    assert_eq!(store.stats(job).await.unwrap().total_word_count, 3);
                    assert_eq!(store.snapshot(job).await.unwrap().state, JobState::Done);
                }
                (Completion::Aborted, true) => {
                    assert_eq!(store.stats(job).await, Err(StoreError::JobAborted));
                    let snapshot = store.snapshot(job).await.unwrap();
                    assert_eq!((snapshot.processed, snapshot.successful_files), (0, 0));
                    assert_eq!(snapshot.state, JobState::Aborted);
                }
                outcome => panic!("non-atomic abort/publication outcome: {outcome:?}"),
            }
        }
        assert!(store.active_jobs().await.unwrap().is_empty());
    })
    .await
    .unwrap();
}

fn worker(index: usize) -> WorkerId {
    format!("test-worker-{index}").parse().unwrap()
}
fn file(words: u64) -> PageResult {
    PageResult::File {
        html_word_count: words,
    }
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn racing_discovery_claims_and_duplicate_completion_contribute_once() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let seed = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let parents: Vec<_> = (0..32)
            .map(|i| base.resolve(&format!("p{i}")).unwrap())
            .collect();
        assert_eq!(
            store.complete(&seed, file(1), &parents).await.unwrap(),
            Completion::Published { done: false }
        );
        let mut claims = Vec::new();
        for i in 0..32 {
            claims.push(store.claim(job, &worker(i)).await.unwrap().unwrap());
        }
        let mut tasks = JoinSet::new();
        for claim in claims {
            let context = context.clone();
            let base = base.clone();
            tasks.spawn(async move {
                let store = context.store().await;
                let links = [
                    base.resolve("shared#one").unwrap(),
                    base.resolve("shared#two").unwrap(),
                    base.resolve("photo.JPG").unwrap(),
                    base.clone(),
                    claim.url().clone(),
                    CrawlUrl::parse("https://example.org/private").unwrap(),
                    CrawlUrl::parse("https://other.example/docs/").unwrap(),
                ];
                store.complete(&claim, file(2), &links).await.unwrap()
            });
        }
        // Reads execute concurrently with real publication, not fabricated writes.
        let reader = context.store().await;
        tasks.spawn(async move {
            for _ in 0..100 {
                let snapshot = reader.snapshot(job).await.unwrap();
                assert_eq!(snapshot.state, JobState::Running);
                assert_eq!(
                    snapshot.discovered,
                    snapshot.processed + snapshot.frontier + snapshot.in_flight
                );
                assert!(snapshot.successful_files as u64 <= snapshot.processed);
                assert_eq!(reader.stats(job).await, Err(StoreError::NotFinished));
            }
            Completion::Published { done: false }
        });
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap(), Completion::Published { done: false });
        }
        let snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(
            (
                snapshot.discovered,
                snapshot.processed,
                snapshot.frontier,
                snapshot.in_flight
            ),
            (35, 33, 2, 0)
        );
        let mut racers = JoinSet::new();
        for i in 0..32 {
            let context = context.clone();
            racers
                .spawn(async move { context.store().await.claim(job, &worker(i)).await.unwrap() });
        }
        let mut winners = Vec::new();
        while let Some(result) = racers.join_next().await {
            if let Some(claim) = result.unwrap() {
                winners.push(claim);
            }
        }
        assert_eq!(winners.len(), 2);
        assert_ne!(winners[0].url(), winners[1].url());
        let shared = winners
            .iter()
            .find(|claim| claim.url().extension() == "html")
            .unwrap();
        let photo = winners
            .iter()
            .find(|claim| claim.url().extension() == "jpg")
            .unwrap();
        let mut repeats = JoinSet::new();
        for _ in 0..32 {
            let context = context.clone();
            let claim = shared.clone();
            repeats.spawn(async move {
                context
                    .store()
                    .await
                    .complete(&claim, file(5), &[])
                    .await
                    .unwrap()
            });
        }
        let mut published = 0;
        while let Some(result) = repeats.join_next().await {
            match result.unwrap() {
                Completion::Published { done: false } => published += 1,
                Completion::AlreadyCompleted => {}
                other => panic!("unexpected duplicate result {other:?}"),
            }
        }
        assert_eq!(published, 1);
        assert_eq!(
            store.complete(photo, file(0), &[]).await.unwrap(),
            Completion::Published { done: true }
        );
        assert_eq!(
            store.stats(job).await.unwrap(),
            WebStats {
                num_files: 35,
                num_exts: 2,
                ext_counts: HashMap::from([("html".into(), 34), ("jpg".into(), 1)]),
                total_word_count: 70,
            }
        );
        assert!(store.active_jobs().await.unwrap().is_empty());
        assert!(store.claim(job, &worker(100)).await.unwrap().is_none());
        let final_snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(
            store
                .complete(&seed, file(u64::MAX), &[base.resolve("late").unwrap()])
                .await
                .unwrap(),
            Completion::AlreadyCompleted
        );
        assert_eq!(
            store.fail(&seed, JobFailure::Fetch).await.unwrap(),
            Completion::AlreadyCompleted
        );
        assert_eq!(store.snapshot(job).await.unwrap(), final_snapshot);
        assert!(!store.submit(&base).await.unwrap().created);
        assert_eq!(
            context
                .store()
                .await
                .stats(job)
                .await
                .unwrap()
                .total_word_count,
            70
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn gated_last_parent_publishes_children_before_completion_and_jobs_are_isolated() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let claim = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let other_base = base.resolve("other/").unwrap();
        let other_job = store.submit(&other_base).await.unwrap().job;
        let other_claim = store.claim(other_job, &worker(1)).await.unwrap().unwrap();
        assert_eq!(store.active_jobs().await.unwrap(), vec![job, other_job]);
        let (release, gate) = tokio::sync::oneshot::channel();
        let publishing_store = context.store().await;
        let links = [base.resolve("child").unwrap(), other_base.clone()];
        let mut publishers = JoinSet::new();
        publishers.spawn(async move {
            gate.await.unwrap();
            publishing_store
                .complete(&claim, file(7), &links)
                .await
                .unwrap()
        });
        for _ in 0..20 {
            let snapshot = store.snapshot(job).await.unwrap();
            assert_eq!(
                (snapshot.frontier, snapshot.in_flight, snapshot.processed),
                (0, 1, 0)
            );
            assert_eq!(snapshot.state, JobState::Running);
            assert!(store.claim(job, &worker(2)).await.unwrap().is_none());
            assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
        }
        assert_eq!(
            store.complete(&other_claim, file(99), &[]).await.unwrap(),
            Completion::Published { done: true }
        );
        assert_eq!(store.stats(other_job).await.unwrap().total_word_count, 99);
        release.send(()).unwrap();
        assert_eq!(
            publishers.join_next().await.unwrap().unwrap(),
            Completion::Published { done: false }
        );
        let child = store.claim(job, &worker(2)).await.unwrap().unwrap();
        let overlap = store.claim(job, &worker(3)).await.unwrap().unwrap();
        assert_eq!(overlap.url(), &other_base); // Same URL is independent across jobs.
        assert_eq!(
            store
                .complete(&child, PageResult::NoFile, &[])
                .await
                .unwrap(),
            Completion::Published { done: false }
        );
        assert_eq!(
            store.complete(&overlap, file(2), &[]).await.unwrap(),
            Completion::Published { done: true }
        );
        let done = store.snapshot(job).await.unwrap();
        assert_eq!(
            (done.discovered, done.processed, done.successful_files),
            (3, 3, 2)
        );
        assert_eq!(
            context
                .store()
                .await
                .stats(job)
                .await
                .unwrap()
                .total_word_count,
            9
        );
        assert_eq!(
            context
                .store()
                .await
                .stats(other_job)
                .await
                .unwrap()
                .total_word_count,
            99
        );
        assert!(store.active_jobs().await.unwrap().is_empty());
        // A job containing only a broken/redirect attempt yields valid empty stats.
        let empty = store
            .submit(&base.resolve("missing/").unwrap())
            .await
            .unwrap()
            .job;
        let claim = store.claim(empty, &worker(4)).await.unwrap().unwrap();
        assert_eq!(
            store
                .complete(&claim, PageResult::NoFile, &[])
                .await
                .unwrap(),
            Completion::Published { done: true }
        );
        assert_eq!(store.stats(empty).await.unwrap(), WebStats::default());
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn exact_unsigned_publication_and_overflow_freeze_without_partial_writes() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let seed = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let high = 9_007_199_254_740_993;
        store
            .complete(&seed, file(high), &[base.resolve("child").unwrap()])
            .await
            .unwrap();
        let child = store.claim(job, &worker(1)).await.unwrap().unwrap();
        assert_eq!(
            store
                .complete(&child, file(u64::MAX - high), &[])
                .await
                .unwrap(),
            Completion::Published { done: true }
        );
        assert_eq!(store.stats(job).await.unwrap().total_word_count, u64::MAX);
        assert_eq!(
            store.complete(&child, file(1), &[]).await.unwrap(),
            Completion::AlreadyCompleted
        );
        let overflow_base = base.resolve("overflow/").unwrap();
        let overflow = store.submit(&overflow_base).await.unwrap().job;
        let seed = store.claim(overflow, &worker(2)).await.unwrap().unwrap();
        let children = [
            overflow_base.resolve("a").unwrap(),
            overflow_base.resolve("b").unwrap(),
        ];
        store
            .complete(&seed, file(u64::MAX), &children)
            .await
            .unwrap();
        let first = store.claim(overflow, &worker(3)).await.unwrap().unwrap();
        let second = store.claim(overflow, &worker(4)).await.unwrap().unwrap();
        let before = store.snapshot(overflow).await.unwrap();
        assert_eq!(
            store
                .complete(
                    &first,
                    file(1),
                    &[overflow_base.resolve("must-not-publish").unwrap()]
                )
                .await
                .unwrap(),
            Completion::Failed(JobFailure::Statistics)
        );
        let after = store.snapshot(overflow).await.unwrap();
        assert_eq!(after.state, JobState::Failed(JobFailure::Statistics));
        assert_eq!(
            (
                after.discovered,
                after.processed,
                after.frontier,
                after.in_flight,
                after.successful_files
            ),
            (
                before.discovered,
                before.processed,
                before.frontier,
                before.in_flight,
                before.successful_files
            )
        );
        assert_eq!(
            store.stats(overflow).await,
            Err(StoreError::JobFailed(JobFailure::Statistics))
        );
        assert_eq!(
            store
                .complete(
                    &second,
                    file(0),
                    &[overflow_base.resolve("also-must-not-publish").unwrap()]
                )
                .await
                .unwrap(),
            Completion::Failed(JobFailure::Statistics)
        );
        assert_eq!(
            store.fail(&first, JobFailure::Fetch).await.unwrap(),
            Completion::Failed(JobFailure::Statistics)
        );
        assert_eq!(store.snapshot(overflow).await.unwrap(), after);
        assert!(store.claim(overflow, &worker(5)).await.unwrap().is_none());
        assert!(!store.submit(&overflow_base).await.unwrap().created);
        assert!(store.active_jobs().await.unwrap().is_empty());
        let words: String = context
            .query(
                redis::cmd("HGET")
                    .arg(context.job_key(overflow, "stats"))
                    .arg("total_word_count"),
            )
            .await;
        assert_eq!(words, u64::MAX.to_string());
        for reason in [JobFailure::Fetch, JobFailure::Protocol] {
            let failure_base = base.resolve(&format!("{reason:?}/")).unwrap();
            let job = store.submit(&failure_base).await.unwrap().job;
            let claim = store.claim(job, &worker(6)).await.unwrap().unwrap();
            assert_eq!(
                store.fail(&claim, reason).await.unwrap(),
                Completion::Failed(reason)
            );
            assert_eq!(store.stats(job).await, Err(StoreError::JobFailed(reason)));
            assert_eq!(
                context.store().await.snapshot(job).await.unwrap().state,
                JobState::Failed(reason)
            );
            assert!(!store.submit(&failure_base).await.unwrap().created);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn preflight_rejects_corruption_and_wrong_ownership_without_mutation() {
    with_redis(|context| async move {
        let store = context.store().await;
        assert!(store.active_jobs().await.unwrap().is_empty());
        assert!(matches!(
            store.claim("999".parse().unwrap(), &worker(0)).await,
            Err(StoreError::UnknownJob)
        ));
        let base = CrawlUrl::parse("https://example.org/docs/?token=fixture-secret").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let initial = store.snapshot(job).await.unwrap();
        context
            .hash_set(&context.job_key(job, "meta"), "processed", "01")
            .await;
        assert!(matches!(
            store.claim(job, &worker(0)).await,
            Err(StoreError::InvalidData(_))
        ));
        context
            .hash_set(&context.job_key(job, "meta"), "processed", "0")
            .await;
        assert_eq!(store.snapshot(job).await.unwrap(), initial);
        context
            .query::<()>(
                redis::cmd("LSET")
                    .arg(context.job_key(job, "frontier"))
                    .arg(0)
                    .arg("https://example.org/private"),
            )
            .await;
        assert!(matches!(
            store.claim(job, &worker(0)).await,
            Err(StoreError::InvalidData(_))
        ));
        context
            .query::<()>(
                redis::cmd("LSET")
                    .arg(context.job_key(job, "frontier"))
                    .arg(0)
                    .arg(base.as_str()),
            )
            .await;
        assert_eq!(store.snapshot(job).await.unwrap(), initial);
        let claim = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let other_namespace_claim = claim.clone();
        let other_base = base.clone();
        with_redis(|inner| async move {
            let other = inner.store().await;
            let other_job = other.submit(&other_base).await.unwrap().job;
            let own_claim = other.claim(other_job, &worker(0)).await.unwrap().unwrap();
            let before = other.snapshot(other_job).await.unwrap();
            assert_eq!(
                other.complete(&other_namespace_claim, file(99), &[]).await,
                Err(StoreError::OwnershipMismatch)
            );
            assert_eq!(other.snapshot(other_job).await.unwrap(), before);
            other.complete(&own_claim, file(1), &[]).await.unwrap();
        })
        .await
        .unwrap();
        assert!(!format!("{claim:?}").contains("fixture-secret"));
        let before = store.snapshot(job).await.unwrap();
        let flight = context.job_key(job, "in-flight");
        context
            .hash_set(&flight, base.as_str(), "someone-else")
            .await;
        assert_eq!(
            store.complete(&claim, file(1), &[]).await,
            Err(StoreError::OwnershipMismatch)
        );
        assert_eq!(
            store.fail(&claim, JobFailure::Fetch).await,
            Err(StoreError::OwnershipMismatch)
        );
        assert_eq!(store.snapshot(job).await.unwrap(), before);
        context
            .hash_set(&flight, base.as_str(), "test-worker-0")
            .await;
        for suffix in ["frontier", "extensions"] {
            let key = context.job_key(job, suffix);
            context
                .query::<()>(redis::cmd("SET").arg(&key).arg("fixture-secret"))
                .await;
            let error = store.complete(&claim, file(1), &[]).await.unwrap_err();
            assert!(matches!(error, StoreError::InvalidData(_)));
            assert!(!format!("{error}: {error:?}").contains("fixture-secret"));
            context.query::<usize>(redis::cmd("DEL").arg(key)).await;
            assert_eq!(store.snapshot(job).await.unwrap(), before);
        }
        context
            .hash_set(
                &context.job_key(job, "stats"),
                "total_word_count",
                "18446744073709551616",
            )
            .await;
        assert!(matches!(
            store.complete(&claim, file(1), &[]).await,
            Err(StoreError::InvalidData(_))
        ));
        context
            .hash_set(&context.job_key(job, "stats"), "total_word_count", "0")
            .await;
        assert_eq!(store.snapshot(job).await.unwrap(), before);
        assert_eq!(
            store.complete(&claim, file(3), &[]).await.unwrap(),
            Completion::Published { done: true }
        );
        assert_eq!(store.stats(job).await.unwrap().total_word_count, 3);
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn bounded_file_counter_overflow_uses_the_same_checked_publication_path() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let seed = store.claim(job, &worker(0)).await.unwrap().unwrap();
        store
            .complete(&seed, file(2), &[base.resolve("a.JPG").unwrap()])
            .await
            .unwrap();
        let child = store.claim(job, &worker(1)).await.unwrap().unwrap();
        // Reaching usize::MAX files cannot fit a real fixture. Run the exact same
        // producer script with a bound of one, exercising file/extension overflow.
        let script = concat!(
            include_str!("../src/jobs/protocol.lua"),
            "\n",
            include_str!("../src/jobs/complete.lua")
        );
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
            command.arg(context.job_key(job, suffix));
        }
        command
            .arg(context.key("active"))
            .arg(job.to_string())
            .arg("1")
            .arg("test-worker-1")
            .arg(child.url().as_str())
            .arg(base.as_str())
            .arg("file")
            .arg("0")
            .arg("jpg");
        let reply: Vec<String> = context.query(&mut command).await;
        assert_eq!(reply, vec!["failed", "statistics"]);
        assert_eq!(store.snapshot(job).await.unwrap().successful_files, 1);
        assert_eq!(
            store.stats(job).await,
            Err(StoreError::JobFailed(JobFailure::Statistics))
        );
        let extensions: HashMap<String, String> = context
            .query(redis::cmd("HGETALL").arg(context.job_key(job, "extensions")))
            .await;
        assert_eq!(extensions, HashMap::from([("html".into(), "1".into())]));
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn observing_done_during_last_completions_always_allows_final_stats() {
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let seed = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let links: Vec<_> = (0..16)
            .map(|i| base.resolve(&format!("p{i}")).unwrap())
            .collect();
        store
            .complete(&seed, PageResult::NoFile, &links)
            .await
            .unwrap();
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = JoinSet::new();
        for i in 0..16 {
            let claim = store.claim(job, &worker(i)).await.unwrap().unwrap();
            let context = context.clone();
            let barrier = barrier.clone();
            tasks.spawn(async move {
                let store = context.store().await;
                barrier.wait().await;
                store.complete(&claim, file(2), &[]).await.unwrap()
            });
        }
        let reader = context.store().await;
        assert_eq!(reader.snapshot(job).await.unwrap().state, JobState::Running);
        barrier.wait().await;
        loop {
            let snapshot = reader.snapshot(job).await.unwrap();
            if snapshot.state == JobState::Done {
                assert_eq!(snapshot.frontier, 0);
                assert_eq!(snapshot.in_flight, 0);
                assert_eq!(snapshot.processed, 17);
                assert_eq!(
                    reader.stats(job).await.unwrap(),
                    WebStats {
                        num_files: 16,
                        num_exts: 1,
                        ext_counts: HashMap::from([("html".into(), 16)]),
                        total_word_count: 32,
                    }
                );
                break;
            }
            assert_eq!(snapshot.state, JobState::Running);
        }
        let mut finalizers = 0;
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Completion::Published { done: true } => finalizers += 1,
                Completion::Published { done: false } => {}
                other => panic!("unexpected last completion {other:?}"),
            }
        }
        assert_eq!(finalizers, 1);
        assert!(store.active_jobs().await.unwrap().is_empty());
    })
    .await
    .unwrap();
}

struct OwnedChild(Option<Child>);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn wait_for(context: &Context, suffix: &str, count: u64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while context
            .query::<u64>(redis::cmd("SCARD").arg(context.key(suffix)))
            .await
            != count
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("bounded process barrier");
}

async fn process_worker(namespace: &str, job: JobId) {
    let url = std::env::var("SWARMCRAWL_REDIS_URL").unwrap();
    let config = RedisConfig::new(&url, 5).unwrap();
    let store = JobStore::connect_in_namespace(&config, namespace)
        .await
        .unwrap();
    let client = redis::Client::open(url).unwrap();
    let mut connection = client
        .get_multiplexed_async_connection_with_config(
            &redis::AsyncConnectionConfig::new()
                .set_connection_timeout(Some(Duration::from_secs(5)))
                .set_response_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .unwrap();
    let identity: WorkerId = format!("process-{}", std::process::id()).parse().unwrap();
    let claim = store.claim(job, &identity).await.unwrap().unwrap();
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
    assert!(gate.is_some(), "worker process gate timed out");
    let mut next = Some(claim);
    while let Some(claim) = next {
        let shared = claim.base().resolve("shared").unwrap();
        let result = store.complete(&claim, file(3), &[shared]).await.unwrap();
        assert!(matches!(result, Completion::Published { .. }));
        println!("SWARMCRAWL_CLAIM {}", claim.url().as_str());
        next = store.claim(job, &identity).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn independent_processes_claim_and_publish_the_same_job_once() {
    if let Ok(namespace) = std::env::var("SWARMCRAWL_TEST_FRONTIER_NAMESPACE") {
        let job = std::env::var("SWARMCRAWL_TEST_FRONTIER_JOB")
            .unwrap()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(15), process_worker(&namespace, job))
            .await
            .expect("bounded child worker");
        return;
    }
    with_redis(|context| async move {
        let store = context.store().await;
        let base = CrawlUrl::parse("https://example.org/docs/").unwrap();
        let job = store.submit(&base).await.unwrap().job;
        let seed = store.claim(job, &worker(0)).await.unwrap().unwrap();
        let links: Vec<_> = (0..24)
            .map(|i| base.resolve(&format!("p{i}")).unwrap())
            .collect();
        store.complete(&seed, file(2), &links).await.unwrap();
        let mut children = Vec::new();
        for _ in 0..4 {
            children.push(OwnedChild(Some(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "independent_processes_claim_and_publish_the_same_job_once",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("SWARMCRAWL_TEST_FRONTIER_NAMESPACE", &context.namespace)
                    .env("SWARMCRAWL_TEST_FRONTIER_JOB", job.to_string())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap(),
            )));
        }
        wait_for(&context, "process-ready", 4).await;
        assert_eq!(store.snapshot(job).await.unwrap().in_flight, 4);
        context
            .query::<usize>(
                redis::cmd("RPUSH")
                    .arg(context.key("process-gate"))
                    .arg(&["go", "go", "go", "go"]),
            )
            .await;
        let mut claimed = Vec::new();
        for mut child in children {
            tokio::time::timeout(Duration::from_secs(10), async {
                while child.0.as_mut().unwrap().try_wait().unwrap().is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("bounded worker process");
            let output = child.0.take().unwrap().wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "worker child failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            let process_claims: Vec<_> = stdout
                .lines()
                .filter_map(|line| line.strip_prefix("SWARMCRAWL_CLAIM "))
                .map(str::to_owned)
                .collect();
            assert!(!process_claims.is_empty(), "every process owned work");
            claimed.extend(process_claims);
        }
        assert_eq!(claimed.len(), 25);
        assert_eq!(claimed.iter().collect::<HashSet<_>>().len(), 25);
        let snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(snapshot.state, JobState::Done);
        assert_eq!(
            (
                snapshot.discovered,
                snapshot.processed,
                snapshot.frontier,
                snapshot.in_flight
            ),
            (26, 26, 0, 0)
        );
        assert_eq!(
            context.store().await.stats(job).await.unwrap(),
            WebStats {
                num_files: 26,
                num_exts: 1,
                ext_counts: HashMap::from([("html".into(), 26)]),
                total_word_count: 77,
            }
        );
        assert!(store.active_jobs().await.unwrap().is_empty());
    })
    .await
    .unwrap();
}
