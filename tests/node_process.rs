//! Actual host-run swarmcrawl nodes, real Redis, and response-gated local HTTP.
#![cfg(unix)]

#[path = "support/http.rs"]
mod http;
mod support;

use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use http::{HttpFixture, Reply};
use support::{Context, with_redis};
use swarmcrawl::{
    jobs::{JobFailure, JobId, JobState, JobStore, StoreError},
    stats::WebStats,
};
use tokio::sync::Semaphore;

struct NodeProcess {
    child: Child,
    log: Arc<Mutex<String>>,
    reader: Option<thread::JoinHandle<()>>,
}

impl NodeProcess {
    fn start(context: &Context, timeout: u64) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_swarmcrawl"))
            .args([
                "node",
                "--namespace",
                &context.namespace,
                "--fetch-timeout-secs",
                &timeout.to_string(),
            ])
            .env("SWARMCRAWL_REDIS_TIMEOUT_SECS", "5")
            // Retain only the Redis endpoint intentionally provided to this suite.
            .env_remove("SWARMCRAWL_FETCH_TIMEOUT_SECS")
            .env_remove("SWARMCRAWL_JOB_NAMESPACE")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start actual swarmcrawl node");
        let stderr = child.stderr.take().unwrap();
        let log = Arc::new(Mutex::new(String::new()));
        let owned = log.clone();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let line = line.expect("read node diagnostics");
                let mut log = owned.lock().unwrap();
                assert!(log.len() + line.len() < 64 * 1024, "bounded node log");
                log.push_str(&line);
                log.push('\n');
            }
        });
        Self {
            child,
            log,
            reader: Some(reader),
        }
    }

    async fn wait_log(&mut self, message: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.log.lock().unwrap().contains(message) {
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "node exited early: {}",
                    self.log()
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bounded node diagnostic wait");
    }

    fn log(&self) -> String {
        self.log.lock().unwrap().clone()
    }

    fn signal(&self, name: &str) {
        let status = Command::new("timeout")
            .args(["5s", "kill", name, &self.child.id().to_string()])
            .status()
            .expect("send Unix shutdown signal");
        assert!(status.success());
    }

    async fn exit(&mut self) -> ExitStatus {
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bounded node exit/drain");
        self.reader.take().unwrap().join().unwrap();
        status
    }

    async fn stop(&mut self) {
        self.signal("-INT");
        let status = self.exit().await;
        assert!(status.success(), "node shutdown failed: {}", self.log());
        assert!(self.log().contains("drained; exiting"));
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        // On panic/deadline, kill and reap only this test's own child before the
        // surrounding Redis harness removes its namespace.
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

async fn done(store: &JobStore, job: JobId) -> WebStats {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = store.snapshot(job).await.unwrap();
            match snapshot.state {
                JobState::Done => break store.stats(job).await.unwrap(),
                JobState::Failed(reason) => panic!("job {job} unexpectedly failed: {reason:?}"),
                JobState::Running => assert!(snapshot.frontier + snapshot.in_flight > 0),
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded application job completion")
}

fn html(words: &str, links: impl IntoIterator<Item = String>) -> Reply {
    let mut source = format!("<p>{words}</p>");
    for link in links {
        source.push_str(&format!("<a href=\"{link}\"></a>"));
    }
    Reply::new(200, "text/html", source)
}

fn expected_html(files: usize, words: u64) -> WebStats {
    WebStats {
        num_files: files,
        num_exts: 1,
        ext_counts: HashMap::from([("html".into(), files)]),
        total_word_count: words,
    }
}

fn assert_unique(fixture: &HttpFixture, count: usize) {
    let requests = fixture.requests();
    assert_eq!(requests.len(), count);
    let mut counts = HashMap::new();
    for request in requests {
        assert!(request.head.contains(concat!(
            "user-agent: swarmcrawl/",
            env!("CARGO_PKG_VERSION")
        )));
        *counts.entry(request.target).or_insert(0) += 1;
    }
    assert!(
        counts.values().all(|count| *count == 1),
        "duplicate GET: {counts:?}"
    );
    fixture.assert_healthy();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn single_node_consumes_jobs_before_and_after_startup_and_handles_absence() {
    with_redis(|context| async move {
        let fixture = HttpFixture::start([
            (
                "/first/".into(),
                html(
                    "One two",
                    [
                        "next",
                        "next#fragment",
                        "pic.JPG",
                        "missing",
                        "go",
                        "/outside",
                    ]
                    .map(str::to_owned),
                ),
            ),
            ("/first/next".into(), html("Three four", ["./".into()])),
            (
                "/first/pic.JPG".into(),
                Reply::new(200, "image/jpeg", "bytes"),
            ),
            (
                "/first/go".into(),
                Reply::new(302, "text/plain", "").header("Location", "next#same"),
            ),
            ("/late/".into(), html("Five six", [])),
        ])
        .await;
        let store = context.store().await;
        let first = store
            .submit(fixture.scope("/first/").base())
            .await
            .unwrap()
            .job;
        let mut node = NodeProcess::start(&context, 5);
        node.wait_log("ready; polling").await;
        assert_eq!(
            done(&store, first).await,
            WebStats {
                num_files: 3,
                num_exts: 2,
                ext_counts: HashMap::from([("html".into(), 2), ("jpg".into(), 1)]),
                total_word_count: 4,
            }
        );
        assert_eq!(store.snapshot(first).await.unwrap().processed, 5);
        assert!(
            node.child.try_wait().unwrap().is_none(),
            "node must stay available"
        );
        let late = store.submit(&fixture.url("/late/")).await.unwrap().job;
        assert_eq!(done(&store, late).await, expected_html(1, 2));
        node.stop().await;
        assert_unique(&fixture, 6);
        assert_eq!(
            context.store().await.stats(first).await.unwrap().num_files,
            3
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn one_node_shares_ten_concurrent_requests_across_jobs() {
    with_redis(|context| async move {
        let gate = Arc::new(Semaphore::new(0));
        let mut routes = Vec::new();
        for base in ["a", "b"] {
            routes.push((
                format!("/{base}/"),
                html("Seed words", (0..14).map(|i| format!("p{i}"))),
            ));
            for i in 0..14 {
                routes.push((
                    format!("/{base}/p{i}"),
                    html("Child words", []).gated_body(" ", &gate),
                ));
            }
        }
        let fixture = HttpFixture::start(routes).await;
        let store = context.store().await;
        let a = store.submit(&fixture.url("/a/")).await.unwrap().job;
        let b = store.submit(&fixture.url("/b/")).await.unwrap().job;
        let mut node = NodeProcess::start(&context, 5);
        fixture.wait_for_headers(12).await; // Both seeds + ten bodies held open.
        let a_progress = store.snapshot(a).await.unwrap();
        let b_progress = store.snapshot(b).await.unwrap();
        assert_eq!(a_progress.in_flight + b_progress.in_flight, 10);
        assert!(a_progress.in_flight > 0 && b_progress.in_flight > 0);
        assert_eq!(a_progress.frontier + b_progress.frontier, 18);
        assert_eq!(fixture.requests().len(), 12);
        assert_eq!(fixture.peak(), 10);
        assert_eq!(store.stats(a).await, Err(StoreError::NotFinished));
        gate.add_permits(28);
        assert_eq!(done(&store, a).await, expected_html(15, 30));
        assert_eq!(done(&store, b).await, expected_html(15, 30));
        node.stop().await;
        assert_unique(&fixture, 30);
        assert_eq!(fixture.peak(), 10);
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn two_independent_nodes_own_a_convergent_graph_and_wait_for_delayed_discovery() {
    with_redis(|context| async move {
        let gate = Arc::new(Semaphore::new(0));
        let mut routes = vec![(
            "/graph/".into(),
            html("Seed words", (0..20).map(|i| format!("p{i}"))),
        )];
        for i in 0..20 {
            let tail = if i == 0 {
                "<a href=\"final\"></a>"
            } else {
                " "
            };
            routes.push((
                format!("/graph/p{i}"),
                html("Child words", ["shared#same".into(), "./".into()]).gated_body(tail, &gate),
            ));
        }
        routes.push(("/graph/shared".into(), html("Shared words", [])));
        routes.push(("/graph/final".into(), html("Final words", [])));
        let fixture = HttpFixture::start(routes).await;
        let store = context.store().await;
        let job = store.submit(&fixture.url("/graph/")).await.unwrap().job;
        let mut nodes = [
            NodeProcess::start(&context, 5),
            NodeProcess::start(&context, 5),
        ];
        fixture.wait_for_headers(21).await;
        let snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(snapshot.state, JobState::Running);
        assert_eq!(
            (snapshot.frontier, snapshot.in_flight, snapshot.processed),
            (0, 20, 1)
        );
        assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
        let owners: HashMap<String, String> = context
            .query(redis::cmd("HGETALL").arg(context.job_key(job, "in-flight")))
            .await;
        let mut per_node = HashMap::new();
        for owner in owners.into_values() {
            *per_node.entry(owner).or_insert(0) += 1;
        }
        assert_eq!(
            per_node.len(),
            2,
            "both actual host processes must own HTTP work"
        );
        assert!(per_node.values().all(|count| *count == 10));
        assert_eq!(fixture.peak(), 20);
        gate.add_permits(20);
        assert_eq!(done(&store, job).await, expected_html(23, 46));
        for node in &mut nodes {
            node.stop().await;
        }
        assert_unique(&fixture, 23);
        assert!(store.active_jobs().await.unwrap().is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn sigterm_stops_claims_and_drains_late_links_then_a_new_node_finishes() {
    with_redis(|context| async move {
        let gate = Arc::new(Semaphore::new(0));
        let mut routes = vec![(
            "/drain/".into(),
            html("Seed words", (0..12).map(|i| format!("p{i}"))),
        )];
        for i in 0..12 {
            let tail = if i == 0 { "<a href=\"late\"></a>" } else { " " };
            routes.push((
                format!("/drain/p{i}"),
                html("Child words", []).gated_body(tail, &gate),
            ));
        }
        routes.push(("/drain/late".into(), html("Late words", [])));
        let fixture = HttpFixture::start(routes).await;
        let store = context.store().await;
        let job = store.submit(&fixture.url("/drain/")).await.unwrap().job;
        let mut node = NodeProcess::start(&context, 5);
        fixture.wait_for_headers(11).await;
        node.signal("-TERM");
        node.wait_log("stopping claims; draining").await;
        node.signal("-INT"); // A second signal must not cancel the owned bodies.
        assert!(node.child.try_wait().unwrap().is_none());
        gate.add_permits(10);
        assert!(node.exit().await.success(), "{}", node.log());
        fixture.wait_for_completed(11).await;
        assert_eq!(
            fixture.requests().len(),
            11,
            "shutdown must not start waiting URLs"
        );
        let snapshot = store.snapshot(job).await.unwrap();
        assert_eq!(snapshot.state, JobState::Running);
        assert_eq!(
            (snapshot.processed, snapshot.frontier, snapshot.in_flight),
            (11, 3, 0)
        );
        assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
        gate.add_permits(2);
        let mut resumed = NodeProcess::start(&context, 5);
        assert_eq!(done(&store, job).await, expected_html(14, 28));
        resumed.stop().await;
        assert_unique(&fixture, 14);
        assert_eq!(
            context
                .query::<u64>(redis::cmd("GET").arg(context.key("next-worker-id")))
                .await,
            2
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn operational_failure_is_terminal_nonzero_secret_safe_and_drains_other_jobs() {
    with_redis(|context| async move {
        let bad_gate = Arc::new(Semaphore::new(0));
        let good_gate = Arc::new(Semaphore::new(0));
        let fixture = HttpFixture::start([
            (
                "/bad/?token=fixture-sensitive-query".into(),
                Reply::new(503, "text/html", "Server error").gated_headers(&bad_gate),
            ),
            (
                "/good/".into(),
                html("Good words", []).gated_body(" ", &good_gate),
            ),
        ])
        .await;
        let store = context.store().await;
        let bad = store
            .submit(&fixture.url("/bad/?token=fixture-sensitive-query"))
            .await
            .unwrap()
            .job;
        let good = store.submit(&fixture.url("/good/")).await.unwrap().job;
        let mut node = NodeProcess::start(&context, 5);
        fixture.wait_for_requests(2).await;
        fixture.wait_for_headers(1).await;
        bad_gate.add_permits(1);
        node.wait_log("stopping claims after error").await;
        assert!(
            node.child.try_wait().unwrap().is_none(),
            "healthy owned body must drain"
        );
        assert_eq!(
            store.snapshot(bad).await.unwrap().state,
            JobState::Failed(JobFailure::Fetch)
        );
        assert_eq!(
            store.stats(bad).await,
            Err(StoreError::JobFailed(JobFailure::Fetch))
        );
        good_gate.add_permits(1);
        assert_eq!(node.exit().await.code(), Some(1));
        assert!(node.log().contains(&format!("job {bad}: HTTP status 503")));
        assert!(!node.log().contains("fixture-sensitive-query"));
        assert_eq!(done(&store, good).await, expected_html(1, 2));
        let mut replacement = NodeProcess::start(&context, 5);
        replacement.wait_log("ready; polling").await;
        replacement.stop().await;
        assert_unique(&fixture, 2); // Failed jobs are never re-fetched/reopened.
        assert_eq!(
            store
                .submit(&fixture.url("/bad/?token=fixture-sensitive-query"))
                .await
                .unwrap()
                .job,
            bad
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn request_timeout_fails_the_job_without_retry_or_final_partial_stats() {
    with_redis(|context| async move {
        let gate = Arc::new(Semaphore::new(0));
        let fixture = HttpFixture::start([(
            "/timeout/".into(),
            html("Never sent", []).gated_headers(&gate),
        )])
        .await;
        let store = context.store().await;
        let job = store.submit(&fixture.url("/timeout/")).await.unwrap().job;
        let mut node = NodeProcess::start(&context, 1);
        fixture.wait_for_requests(1).await;
        assert_eq!(node.exit().await.code(), Some(1));
        assert!(node.log().contains("HTTP request deadline exceeded"));
        assert_eq!(
            store.snapshot(job).await.unwrap().state,
            JobState::Failed(JobFailure::Fetch)
        );
        assert_eq!(
            store.stats(job).await,
            Err(StoreError::JobFailed(JobFailure::Fetch))
        );
        fixture.wait_for_completed(1).await;
        assert_unique(&fixture, 1);
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn redis_protocol_error_stops_the_node_without_reset_or_success() {
    with_redis(|context| async move {
        context
            .query::<String>(
                redis::cmd("SET")
                    .arg(context.key("active"))
                    .arg("fixture-sensitive-value"),
            )
            .await;
        let mut node = NodeProcess::start(&context, 5);
        assert_eq!(node.exit().await.code(), Some(1));
        assert!(node.log().contains("Redis list active jobs failed"));
        assert!(!node.log().contains("fixture-sensitive-value"));
        assert_eq!(
            context
                .query::<String>(redis::cmd("GET").arg(context.key("active")))
                .await,
            "fixture-sensitive-value"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn process_identities_are_unique_exact_retained_and_fail_closed() {
    with_redis(|context| async move {
        let store = context.store().await;
        context
            .query::<String>(
                redis::cmd("SET")
                    .arg(context.key("next-worker-id"))
                    .arg("9007199254740992"),
            )
            .await;
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let context = context.clone();
            tasks.spawn(async move { context.store().await.allocate_worker().await.unwrap() });
        }
        let mut workers = Vec::new();
        while let Some(worker) = tasks.join_next().await {
            workers.push(worker.unwrap());
        }
        for (i, worker) in workers.iter().enumerate() {
            assert!(!workers[..i].contains(worker));
        }
        assert_eq!(
            context
                .query::<String>(redis::cmd("GET").arg(context.key("next-worker-id")))
                .await,
            "9007199254741024"
        );
        let new_worker = context.store().await.allocate_worker().await.unwrap();
        assert!(!workers.contains(&new_worker));
        for bad in [
            "-1",
            "01",
            "18446744073709551615",
            "9223372036854775808",
            "fixture-sensitive-query",
        ] {
            context
                .query::<String>(
                    redis::cmd("SET")
                        .arg(context.key("next-worker-id"))
                        .arg(bad),
                )
                .await;
            let error = store.allocate_worker().await.unwrap_err();
            assert!(matches!(error, StoreError::InvalidData(_)));
            assert!(!format!("{error}: {error:?}").contains("fixture-sensitive-query"));
            assert_eq!(
                context
                    .query::<String>(redis::cmd("GET").arg(context.key("next-worker-id")))
                    .await,
                bad
            );
        }
        context
            .query::<String>(
                redis::cmd("SET")
                    .arg(context.key("next-worker-id"))
                    .arg(i64::MAX.to_string()),
            )
            .await;
        assert_eq!(
            store.allocate_worker().await,
            Err(StoreError::WorkerSequenceExhausted)
        );
        context
            .query::<usize>(redis::cmd("DEL").arg(context.key("next-worker-id")))
            .await;
        context
            .hash_set(&context.key("next-worker-id"), "wrong", "type")
            .await;
        assert!(matches!(
            store.allocate_worker().await,
            Err(StoreError::InvalidData(_))
        ));
    })
    .await
    .unwrap();
}
