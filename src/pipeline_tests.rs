//! Gates, not elapsed-time assertions: real fetching/parsing/publication and drain.

use std::{
    error::Error,
    sync::{Arc, Condvar, Mutex},
};

// Shared integration fixtures expose more helpers than these cases need.
#[allow(dead_code)]
#[path = "../tests/support/http.rs"]
mod http;
#[allow(dead_code)]
#[path = "../tests/support/mod.rs"]
mod support;

use crate::{
    config::FetchConfig,
    diagnostics::Diagnostics,
    fetch::{FetchError, Fetcher, MAX_NODE_CPU_WORK, MAX_NODE_REQUESTS},
    jobs::{JobFailure, JobState, StoreError},
    node::{MAX_NODE_OWNED_TASKS, NodeError, run_node},
};
use http::{HttpFixture, Reply, wait_until};
use tokio::sync::{Semaphore, oneshot};

#[derive(Default)]
struct Gate {
    state: Mutex<(usize, bool)>,
    changed: Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        self.changed.notify_all();
        while !state.1 {
            // An inline-CPU regression could block the only Tokio worker, so a
            // Tokio timeout alone cannot protect this synchronous test gate.
            let (next, expired) = self
                .changed
                .wait_timeout(state, std::time::Duration::from_secs(10))
                .unwrap();
            state = next;
            if expired.timed_out() && !state.1 {
                drop(state); // Do not poison the cleanup gate when panicking.
                panic!("CPU test gate exceeded its safety deadline");
            }
        }
    }

    fn started(&self) -> usize {
        self.state.lock().unwrap().0
    }

    fn release(&self) {
        self.state.lock().unwrap().1 = true;
        self.changed.notify_all();
    }
}

// Failed assertions must not leave non-abortable blocking workers hung forever.
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn cpu_work_does_not_block_the_runtime_or_keep_an_http_permit() {
    let gate = Arc::new(Gate::default());
    let _release = Release(gate.clone());
    let diagnostics = Diagnostics::enabled();
    let owned = gate.clone();
    let fetcher = Fetcher::new(FetchConfig::default())
        .unwrap()
        .with_diagnostics(diagnostics.clone())
        .with_cpu_hook(Arc::new(move || owned.wait()));
    let fixture = HttpFixture::start(
        (0..20).map(|i| (format!("/p{i}"), Reply::new(200, "text/html", "Full words"))),
    )
    .await;
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..20 {
        let fetcher = fetcher.clone();
        let scope = fixture.scope("/");
        let url = fixture.url(&format!("/p{i}"));
        tasks.spawn(async move { fetcher.fetch(&scope, &url).await });
    }
    // This test runs on a single Tokio worker; reaching this await while four
    // closures are blocked also proves the parser work left the async worker.
    wait_until(|| gate.started() == MAX_NODE_CPU_WORK).await;
    fixture.wait_for_completed(20).await;
    wait_until(|| diagnostics.snapshot()["http_transfer"]["count"] == 20).await;
    assert!(tasks.try_join_next().is_none());
    let snapshot = diagnostics.snapshot();
    assert_eq!(snapshot["http"]["active"], 0);
    assert!(snapshot["http"]["peak"].as_u64().unwrap() <= MAX_NODE_REQUESTS as u64);
    assert_eq!(snapshot["cpu"]["active"], MAX_NODE_CPU_WORK);
    assert_eq!(snapshot["cpu"]["peak"], MAX_NODE_CPU_WORK);
    assert_eq!(
        gate.started(),
        MAX_NODE_CPU_WORK,
        "no unlimited blocking queue"
    );
    gate.release();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(joined) = tasks.join_next().await {
            assert!(joined.unwrap().is_ok());
        }
    })
    .await
    .unwrap();
    assert_eq!(diagnostics.snapshot()["cpu"]["active"], 0);
    assert_eq!(diagnostics.snapshot()["parse"]["count"], 20);
    fixture.assert_healthy();
}

#[tokio::test]
#[ignore = "requires isolated real Redis; run scripts/redis-smoke.sh"]
async fn bounded_ownership_overlaps_cpu_and_publication_and_shutdown_drains_both() {
    support::with_redis(|context| async move {
        let gate = Arc::new(Gate::default());
        let _release = Release(gate.clone());
        let invocation = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let publication = Arc::new(Semaphore::new(1)); // Only the seed may publish.
        let diagnostics = Diagnostics::enabled();
        let owned = gate.clone();
        let fetcher = Fetcher::new(FetchConfig::default())
            .unwrap()
            .with_diagnostics(diagnostics.clone())
            .with_publication_gate(publication.clone())
            .with_cpu_hook(Arc::new(move || {
                // Seed publishes the initial frontier before child CPU gating.
                if invocation.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                    owned.wait();
                }
            }));
        let width = 2 * MAX_NODE_OWNED_TASKS;
        let source = format!(
            "<p>Seed words</p>{}",
            (0..width)
                .map(|i| format!("<a href='p{i}'></a>"))
                .collect::<String>()
        );
        let mut routes = vec![("/load/".into(), Reply::new(200, "text/html", source))];
        for i in 0..width {
            routes.push((
                format!("/load/p{i}"),
                Reply::new(200, "text/html", "<p>Child words</p><a href='late'></a>"),
            ));
        }
        routes.push((
            "/load/late".into(),
            Reply::new(200, "text/html", "Late words"),
        ));
        let fixture = HttpFixture::start(routes).await;
        let store = context.store().await;
        let job = store.submit(&fixture.url("/load/")).await.unwrap().job;
        let (stop, shutdown) = oneshot::channel();
        let (observed, stopped) = oneshot::channel();
        let node_store = store.clone();
        // Own parent node tasks too, so an assertion aborts them before cleanup.
        let mut nodes = tokio::task::JoinSet::new();
        let node = nodes.spawn(run_node(node_store, fetcher, async {
            shutdown.await.unwrap();
            observed.send(()).unwrap();
            Ok(())
        }));
        wait_until(|| gate.started() == MAX_NODE_CPU_WORK).await;
        fixture.wait_for_completed(MAX_NODE_OWNED_TASKS + 1).await;
        let held = store.snapshot(job).await.unwrap();
        assert_eq!(
            (held.processed, held.in_flight, held.frontier),
            (1, MAX_NODE_OWNED_TASKS as u64, MAX_NODE_OWNED_TASKS as u64)
        );
        assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
        assert_eq!(
            diagnostics.snapshot()["owned"]["peak"],
            MAX_NODE_OWNED_TASKS
        );
        assert_eq!(diagnostics.snapshot()["cpu"]["peak"], MAX_NODE_CPU_WORK);
        assert_eq!(fixture.requests().len(), MAX_NODE_OWNED_TASKS + 1);
        stop.send(()).unwrap();
        stopped.await.unwrap();
        assert!(!node.is_finished(), "running/queued CPU work must drain");
        gate.release();
        wait_until(|| diagnostics.snapshot()["parse"]["count"] == MAX_NODE_OWNED_TASKS + 1).await;
        assert!(!node.is_finished(), "pending publications must also drain");
        assert_eq!(store.snapshot(job).await.unwrap(), held);
        assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
        publication.add_permits(MAX_NODE_OWNED_TASKS);
        tokio::time::timeout(std::time::Duration::from_secs(5), nodes.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        let drained = store.snapshot(job).await.unwrap();
        assert_eq!(drained.state, JobState::Running);
        assert_eq!(
            (drained.processed, drained.in_flight, drained.frontier),
            (
                (MAX_NODE_OWNED_TASKS + 1) as u64,
                0,
                (MAX_NODE_OWNED_TASKS + 1) as u64
            )
        );
        assert_eq!(
            fixture.requests().len(),
            MAX_NODE_OWNED_TASKS + 1,
            "no new claims during drain"
        );
        assert_eq!(diagnostics.snapshot()["owned"]["active"], 0);
        assert_eq!(
            diagnostics.snapshot()["publication"]["count"],
            MAX_NODE_OWNED_TASKS + 1
        );
        assert_eq!(diagnostics.snapshot()["cpu"]["active"], 0);
        let (stop, shutdown) = oneshot::channel();
        nodes.spawn(run_node(
            store.clone(),
            Fetcher::new(FetchConfig::default()).unwrap(),
            async {
                shutdown.await.unwrap();
                Ok(())
            },
        ));
        loop {
            if store.snapshot(job).await.unwrap().state == JobState::Done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let stats = store.stats(job).await.unwrap();
        assert_eq!(
            (stats.num_files, stats.total_word_count),
            (width + 2, 2 * (width + 2) as u64)
        );
        stop.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), nodes.join_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(fixture.requests().len(), width + 2);
        let mut urls = std::collections::HashSet::new();
        assert!(
            fixture
                .requests()
                .iter()
                .all(|request| urls.insert(request.target.clone()))
        );
        fixture.assert_healthy();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated real Redis; run scripts/redis-smoke.sh"]
async fn abort_does_not_hide_a_cpu_execution_failure() {
    support::with_redis(|context| async move {
        let gate = Arc::new(Gate::default());
        let _release = Release(gate.clone());
        let owned = gate.clone();
        let fixture = HttpFixture::start([(
            "/abort/".into(),
            Reply::new(200, "text/html", "Never publish"),
        )])
        .await;
        let store = context.store().await;
        let job = store.submit(&fixture.url("/abort/")).await.unwrap().job;
        let fetcher = Fetcher::new(FetchConfig::default())
            .unwrap()
            .with_cpu_hook(Arc::new(move || {
                owned.wait();
                panic!("test CPU panic after abort");
            }));
        let mut nodes = tokio::task::JoinSet::new();
        nodes.spawn(run_node(store.clone(), fetcher, std::future::pending()));
        wait_until(|| gate.started() == 1).await;
        assert!(store.abort(job).await.unwrap());
        let frozen = store.snapshot(job).await.unwrap();
        gate.release();
        let result = nodes.join_next().await.unwrap().unwrap();
        assert!(matches!(
            result,
            Err(NodeError::Fetch {
                error: FetchError::CpuTaskFailed,
                ..
            })
        ));
        assert_eq!(store.snapshot(job).await.unwrap(), frozen);
        assert_eq!(store.stats(job).await, Err(StoreError::JobAborted));
        assert_eq!(fixture.requests().len(), 1);
        fixture.assert_healthy();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated real Redis; run scripts/redis-smoke.sh"]
async fn cpu_panic_fails_the_owned_job_without_partial_stats_and_drains_other_cpu_work() {
    support::with_redis(|context| async move {
        let bad_headers = Arc::new(Semaphore::new(0));
        let good_cpu = Arc::new(Gate::default());
        let _release = Release(good_cpu.clone());
        let fixture = HttpFixture::start([
            ("/fail/".into(), Reply::new(200, "text/html",
                "<p>Never publish</p><a href='never'></a>").gated_headers(&bad_headers)),
            ("/good/".into(), Reply::new(200, "text/html", "Good words")),
        ]).await;
        let store = context.store().await;
        let bad = store.submit(&fixture.url("/fail/")).await.unwrap().job;
        let good = store.submit(&fixture.url("/good/")).await.unwrap().job;
        let owned = good_cpu.clone();
        let invocation = std::sync::atomic::AtomicUsize::new(0);
        let fetcher = Fetcher::new(FetchConfig::default()).unwrap()
            .with_cpu_hook(Arc::new(move || {
                // The bad response cannot arrive until good CPU work is blocked.
                if invocation.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    owned.wait();
                } else {
                    panic!("test CPU panic");
                }
            }));
        let mut nodes = tokio::task::JoinSet::new();
        let node = nodes.spawn(run_node(store.clone(), fetcher, std::future::pending()));
        fixture.wait_for_requests(2).await;
        wait_until(|| good_cpu.started() == 1).await;
        bad_headers.add_permits(1);
        loop {
            if store.snapshot(bad).await.unwrap().state == JobState::Failed(JobFailure::Protocol) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(!node.is_finished(), "other owned CPU work must drain after the failure");
        let failed = store.snapshot(bad).await.unwrap();
        assert_eq!((failed.processed, failed.successful_files, failed.discovered), (0, 0, 1));
        assert_eq!(store.stats(bad).await, Err(StoreError::JobFailed(JobFailure::Protocol)));
        assert_eq!(store.stats(good).await, Err(StoreError::NotFinished));
        good_cpu.release();
        let result = nodes.join_next().await.unwrap().unwrap();
        assert!(matches!(result, Err(NodeError::Fetch { job, error: FetchError::CpuTaskFailed }) if job == bad));
        assert_eq!(store.stats(good).await.unwrap().total_word_count, 2);
        assert_eq!(store.snapshot(bad).await.unwrap(), failed);
        assert_eq!(fixture.requests().len(), 2);
        assert!(FetchError::CpuTaskFailed.source().is_none());
        fixture.assert_healthy();
    }).await.unwrap();
}
