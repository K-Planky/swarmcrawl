//! Broader end-to-end proofs, compiled as part of the real CLI/process suite.
//! All network work uses actual host binaries; no mocked page outcomes.

use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{
    http::{HttpFixture, Reply},
    ids,
    process::{Process, run},
    start_node, stop_node, success,
    support::{Context, with_redis},
};
use swarmcrawl::{
    jobs::{JobFailure, JobId, JobSnapshot, JobState, JobStore, StoreError},
    stats::WebStats,
};
use tokio::sync::Semaphore;

fn html(words: &str, links: impl IntoIterator<Item = String>) -> Reply {
    let mut source = format!("<p>{words}</p>");
    for link in links {
        source.push_str(&format!("<a href=\"{link}\"></a>"));
    }
    Reply::new(200, "text/html", source)
}

fn html_stats(files: usize) -> WebStats {
    let words = u64::try_from(files).unwrap().checked_mul(2).unwrap();
    WebStats::from_counts(HashMap::from([("html".into(), files)]), words).unwrap()
}

async fn progress(
    store: &JobStore,
    job: JobId,
    predicate: impl Fn(&JobSnapshot) -> bool,
) -> JobSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = store.snapshot(job).await.unwrap();
            assert_eq!(
                snapshot.discovered,
                snapshot.processed + snapshot.frontier + snapshot.in_flight
            );
            assert!(u64::try_from(snapshot.successful_files).unwrap() <= snapshot.processed);
            assert!(
                !matches!(snapshot.state, JobState::Failed(_)),
                "unexpected job failure: {snapshot:?}"
            );
            if predicate(&snapshot) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("bounded distributed progress milestone")
}

async fn finished(store: &JobStore, job: JobId, expected: &WebStats, attempts: usize) {
    let snapshot = progress(store, job, |s| s.state == JobState::Done).await;
    assert_eq!((snapshot.frontier, snapshot.in_flight), (0, 0));
    assert_eq!(snapshot.processed, u64::try_from(attempts).unwrap());
    // Read at the first observed done, with no node restart/finalization step.
    assert_eq!(&store.stats(job).await.unwrap(), expected);
    expected.validate().unwrap();
}

fn request_counts(fixture: &HttpFixture, offset: usize) -> HashMap<String, usize> {
    fixture.assert_healthy();
    let mut counts = HashMap::new();
    for request in fixture.requests().into_iter().skip(offset) {
        assert!(request.head.contains(concat!(
            "user-agent: swarmcrawl/",
            env!("CARGO_PKG_VERSION")
        )));
        *counts.entry(request.target).or_default() += 1;
    }
    counts
}

async fn assert_seen(context: &Context, job: JobId, fixture: &HttpFixture, paths: &[String]) {
    let mut seen: Vec<String> = context
        .query(redis::cmd("SMEMBERS").arg(context.job_key(job, "seen")))
        .await;
    let mut expected: Vec<_> = paths
        .iter()
        .map(|path| fixture.url(path).as_str().to_owned())
        .collect();
    seen.sort();
    expected.sort();
    assert_eq!(seen, expected, "job-specific discovery/scope mismatch");
}

const GRAPH_PATHS: [&str; 30] = [
    "",
    "left.html",
    "right.html",
    "shared.html",
    "sub/",
    "sub/late.html",
    "sub/final",
    "go-a",
    "middle",
    "go-b",
    "query?view=1",
    "query?view=2",
    "loop-a",
    "loop-b",
    "outside-redirect",
    "foreign-redirect",
    "missing",
    "gone",
    "denied",
    "plain.HTML",
    "asset.JPG",
    "asset.JPEG",
    "api",
    "mime.DAT",
    "doc.PDF",
    "intro",
    "empty",
    "base.html",
    "template.html",
    "sheet.CSS",
];

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn adversarial_graph_has_identical_hand_checked_stats_for_one_two_and_three_nodes() {
    // Each N gets fresh Redis identity/state and fresh ephemeral HTTP servers.
    // Equality to the same hand-calculated oracle proves N-equivalence, not just
    // agreement between implementations that could share a counting mistake.
    for node_count in [1, 2, 3] {
        with_redis(move |context| run_adversarial_graph(context, node_count))
            .await
            .unwrap();
    }
}

async fn run_adversarial_graph(context: Context, node_count: usize) {
    let seed_gate = Arc::new(Semaphore::new(0));
    let wave_gate = Arc::new(Semaphore::new(0));
    let gate = Arc::new(Semaphore::new(0));
    let foreign = HttpFixture::start([]).await;
    let foreign_url = foreign.url("/site/escape");
    let index = include_str!("../fixtures/distributed-index.html")
        .replace("{{foreign}}", foreign_url.as_str());
    let mut routes = vec![
        ("/site/".into(), Reply::new(200, "text/html", index)),
        (
            "/site/left.html".into(),
            html("Left words", ["shared.html#left", "./"].map(str::to_owned)),
        ),
        (
            "/site/right.html".into(),
            html("Right words", ["./shared.html#right".into()]),
        ),
        (
            "/site/shared.html".into(),
            html("Shared words", ["left.html#cycle".into()]),
        ),
        (
            "/site/sub/".into(),
            html("Nested words", ["late.html".into()]),
        ),
        (
            "/site/sub/late.html".into(),
            html("Late words", []).gated_body("<a href='final'></a>", &gate),
        ),
        (
            "/site/sub/final".into(),
            html("Final words", ["./", "/site/"].map(str::to_owned)),
        ),
        (
            "/site/query?view=1".into(),
            html("Query one", ["?view=2#same".into()]),
        ),
        (
            "/site/query?view=2".into(),
            html("Query two", ["?view=1#same".into()]),
        ),
        (
            "/site/plain.HTML".into(),
            Reply::new(200, "text/plain", "<a href='hidden'>Not HTML words</a>"),
        ),
        (
            "/site/asset.JPG".into(),
            Reply::new(200, "image/jpeg", "JPG"),
        ),
        (
            "/site/asset.JPEG".into(),
            Reply::new(200, "image/jpeg", "JPEG"),
        ),
        (
            "/site/api".into(),
            Reply::new(200, "application/octet-stream", "not HTML"),
        ),
        (
            "/site/mime.DAT".into(),
            Reply::new(
                200,
                "TEXT/HTML; charset=UTF-8",
                "<p>MIME words</p><a href='intro'></a>",
            ),
        ),
        (
            "/site/doc.PDF".into(),
            Reply::new(200, "application/pdf", "PDF"),
        ),
        ("/site/intro".into(), html("Intro words", [])),
        ("/site/empty".into(), Reply::new(204, "text/html", "")),
        (
            "/site/base.html".into(),
            Reply::new(
                200,
                "text/html",
                "<a href='late.html#early'></a><base href='sub/'><a href='../intro'></a>",
            ),
        ),
        ("/site/template.html".into(), html("Template words", [])),
        (
            "/site/sheet.CSS".into(),
            Reply::new(200, "text/css", "<a href='css-hidden'>ignored</a>"),
        ),
        (
            "/site/missing".into(),
            Reply::new(404, "text/html", "<a href='error-hidden'>ignored</a>"),
        ),
        ("/site/gone".into(), Reply::new(410, "text/html", "ignored")),
        (
            "/site/denied".into(),
            Reply::new(403, "text/html", "ignored"),
        ),
    ];
    for (path, status, target) in [
        ("go-a", 301, "middle"),
        ("middle", 302, "query?view=1#redirect"),
        ("go-b", 303, "query?view=1#converge"),
        ("loop-a", 307, "loop-b"),
        ("loop-b", 308, "loop-a#cycle"),
        ("outside-redirect", 302, "/outside"),
        ("foreign-redirect", 302, foreign_url.as_str()),
    ] {
        routes.push((
            format!("/site/{path}"),
            Reply::new(status, "text/html", "<a href='redirect-hidden'>ignored</a>")
                .header("Location", target),
        ));
    }
    // Hold the seed until every process is ready, then freeze the first wave of
    // 23 children. This guarantees all N nodes actually fetch graph work rather
    // than merely starting N processes after one has already finished the graph.
    let fixture = HttpFixture::start(routes.into_iter().map(|(path, reply)| {
        let headers = if path == "/site/" {
            &seed_gate
        } else {
            &wave_gate
        };
        (path, reply.gated_headers(headers))
    }))
    .await;
    let base = fixture.url("/site/");
    // Race real CLI processes, not library-only submissions. No nodes yet.
    let fragments: Vec<_> = (0..8)
        .map(|i| format!("{}#submit-{i}", base.as_str()))
        .collect();
    let mut submits: Vec<_> = fragments
        .iter()
        .map(|url| Process::start(&context, &["submit", url]))
        .collect();
    let mut jobs = Vec::new();
    let mut created = 0;
    for submit in &mut submits {
        let output = submit.exit().await;
        created += usize::from(output.stdout.ends_with("created\n"));
        jobs.push(ids(&output)[0]);
    }
    assert_eq!(created, 1);
    assert!(jobs.iter().all(|job| *job == jobs[0]));
    assert!(fixture.requests().is_empty());
    let job = jobs[0];
    let id = job.to_string();
    let store = context.store().await;
    let mut follow = Process::start(&context, &["status", "-f", &id]);
    follow.wait_stdout("frontier 1  in flight 0").await;
    let mut nodes = Vec::new();
    for _ in 0..node_count {
        nodes.push(start_node(&context).await);
    }
    seed_gate.add_permits(1);
    let first_wave = (10 * node_count).min(23);
    fixture.wait_for_requests(first_wave + 1).await;
    let owners: HashMap<String, String> = context
        .query(redis::cmd("HGETALL").arg(context.job_key(job, "in-flight")))
        .await;
    let mut per_node = HashMap::<_, usize>::new();
    for owner in owners.values() {
        *per_node.entry(owner).or_default() += 1;
    }
    assert_eq!(owners.len(), first_wave);
    assert_eq!(
        per_node.len(),
        node_count,
        "all N processes must participate"
    );
    assert!(per_node.values().all(|count| *count <= 10));
    for request in fixture
        .requests()
        .into_iter()
        .filter(|r| r.target != "/site/")
    {
        assert!(owners.contains_key(fixture.url(&request.target).as_str()));
    }
    wave_gate.add_permits(32); // 29 outer non-seed URLs + three nested-job URLs.
    let held = progress(&store, job, |s| s.processed == 28 && s.in_flight == 1).await;
    assert_eq!(held.state, JobState::Running);
    assert_eq!(held.frontier, 0);
    assert_eq!(held.discovered, 29);
    follow
        .wait_stdout("crawled 28  frontier 0  in flight 1")
        .await;
    assert!(follow.running());
    assert_eq!(store.stats(job).await, Err(StoreError::NotFinished));
    assert!(
        !fixture
            .requests()
            .iter()
            .any(|r| r.target == "/site/sub/final")
    );
    gate.add_permits(2); // Outer late page now; nested job's copy later.
    // 15 html files + one each css/dat/jpeg/jpg/pdf = 20 files, 6 extensions.
    // Seed: 15 words (2+2+2+3+3+3); eleven other HTML bodies: 2 each.
    let expected = WebStats::from_counts(
        HashMap::from([
            ("html".into(), 15),
            ("css".into(), 1),
            ("dat".into(), 1),
            ("jpeg".into(), 1),
            ("jpg".into(), 1),
            ("pdf".into(), 1),
        ]),
        37,
    )
    .unwrap();
    finished(&store, job, &expected, 30).await;
    success(&follow.exit().await);
    let paths: Vec<_> = GRAPH_PATHS
        .iter()
        .map(|path| format!("/site/{path}"))
        .collect();
    assert_seen(&context, job, &fixture, &paths).await;
    assert_eq!(
        request_counts(&fixture, 0),
        paths.iter().map(|p| (p.clone(), 1)).collect()
    );
    let stats = run(&context, &["stats", &id]).await;
    success(&stats);
    assert_eq!(
        stats.stdout,
        "files: 20   extensions: 6   words: 37\n  css 1\n  dat 1\n  html 15\n  jpeg 1\n  jpg 1\n  pdf 1\n"
    );
    // Independent overlapping scope may fetch the same URL again in its
    // own job. Sequential logs here give unambiguous per-job attribution;
    // concurrent overlapping jobs are exercised by the gated test below.
    let nested = fixture.url("/site/sub/");
    let offset = fixture.requests().len();
    let nested_job = ids(&run(&context, &["submit", nested.as_str()]).await)[0];
    assert_ne!(nested_job, job);
    finished(&store, nested_job, &html_stats(3), 3).await;
    let nested_paths = ["/site/sub/", "/site/sub/late.html", "/site/sub/final"].map(str::to_owned);
    assert_seen(&context, nested_job, &fixture, &nested_paths).await;
    assert_eq!(
        request_counts(&fixture, offset),
        nested_paths.into_iter().map(|p| (p, 1)).collect()
    );
    for node in &mut nodes {
        stop_node(node).await;
    }
    let mut restarted = start_node(&context).await;
    assert_eq!(ids(&run(&context, &["submit", base.as_str()]).await), [job]);
    let retained = run(&context, &["stats", &id]).await;
    success(&retained);
    assert_eq!(retained.stdout, stats.stdout);
    let followed = run(&context, &["status", "-f", &id]).await;
    success(&followed);
    assert_eq!(followed.stdout.lines().count(), 1);
    assert!(followed.stdout.ends_with("done\n"));
    stop_node(&mut restarted).await;
    assert_eq!(fixture.requests().len(), 33);
    assert!(foreign.requests().is_empty());
    foreign.assert_healthy();
    assert!(store.active_jobs().await.unwrap().is_empty());
}

async fn assert_held_budget(
    context: &Context,
    jobs: &[JobId],
    node_count: usize,
    fixture: &HttpFixture,
    completed_bodies: usize,
) {
    // Bodies remain gated, so ownership is stable. Correlating these hashes with
    // received GETs observes actual processes without adding a test-only HTTP
    // header or inferring node identity from a TCP connection/source port.
    let mut per_node = HashMap::<String, usize>::new();
    let mut held_urls = HashMap::<String, usize>::new();
    for job in jobs {
        let owners: HashMap<String, String> = context
            .query(redis::cmd("HGETALL").arg(context.job_key(*job, "in-flight")))
            .await;
        for (url, owner) in owners {
            *held_urls.entry(url).or_default() += 1;
            *per_node.entry(owner).or_default() += 1;
        }
    }
    let mut received = HashMap::<String, usize>::new();
    for request in fixture.requests() {
        if request.target != "/load/" && request.target != "/load/sub/" {
            *received
                .entry(fixture.url(&request.target).as_str().to_owned())
                .or_default() += 1;
        }
    }
    // Every owned item at this frozen milestone has issued a real GET. Shared
    // URLs can appear once in each job, so compare multiplicities, not a set.
    for (url, count) in held_urls {
        let observed = received
            .get_mut(&url)
            .expect("owned body must have a received GET");
        *observed = observed.checked_sub(count).expect("ownership exceeds GETs");
    }
    assert_eq!(received.values().sum::<usize>(), completed_bodies);
    assert_eq!(per_node.len(), node_count);
    assert!(
        per_node.values().all(|count| *count == 10),
        "per-process held HTTP ownership: {per_node:?}"
    );
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn overlapping_jobs_share_each_process_budget_and_survive_cluster_drain_restart() {
    for node_count in [1, 2, 3] {
        with_redis(move |context| run_overlapping_jobs(context, node_count))
            .await
            .unwrap();
    }
}

async fn run_overlapping_jobs(context: Context, node_count: usize) {
    let width = 10 * node_count + 3;
    let gate = Arc::new(Semaphore::new(0));
    let mut routes = vec![
        (
            "/load/".into(),
            html(
                "Outer words",
                std::iter::once("sub/".into()).chain((0..width).map(|i| format!("p{i}"))),
            ),
        ),
        (
            "/load/sub/".into(),
            html("Nested words", (0..width).map(|i| format!("q{i}"))),
        ),
    ];
    for i in 0..width {
        for path in [format!("/load/p{i}"), format!("/load/sub/q{i}")] {
            routes.push((
                path,
                html("Child words", ["./".into()]).gated_body(" ", &gate),
            ));
        }
    }
    let fixture = HttpFixture::start(routes).await;
    let outer = fixture.url("/load/");
    let nested = fixture.url("/load/sub/");
    let jobs = ids(&run(&context, &["submit", outer.as_str(), nested.as_str()]).await);
    let store = context.store().await;
    let mut nodes = Vec::new();
    for _ in 0..node_count {
        nodes.push(start_node(&context).await);
    }
    fixture.wait_for_headers(10 * node_count + 3).await;
    assert_eq!(fixture.requests().len(), 10 * node_count + 3);
    assert_eq!(fixture.peak(), 10 * node_count);
    for job in &jobs {
        let snapshot = store.snapshot(*job).await.unwrap();
        assert_eq!(snapshot.state, JobState::Running);
        assert!(snapshot.in_flight > 0, "each overlapping job must advance");
        assert_eq!(store.stats(*job).await, Err(StoreError::NotFinished));
    }
    assert_held_budget(&context, &jobs, node_count, &fixture, 0).await;
    gate.add_permits(1);
    fixture.wait_for_headers(10 * node_count + 4).await;
    // One completed body admits exactly one new GET, not a second pool
    // of ten for another job, nor early permit release after headers.
    assert_eq!(fixture.requests().len(), 10 * node_count + 4);
    assert_held_budget(&context, &jobs, node_count, &fixture, 1).await;
    assert_eq!(fixture.peak(), 10 * node_count);
    for node in &nodes {
        node.interrupt();
    }
    for node in &mut nodes {
        node.wait_stderr("stopping claims; draining").await;
    }
    gate.add_permits(3 * width);
    for node in &mut nodes {
        let output = node.exit().await;
        assert_eq!(output.code, Some(0), "{}", output.stderr);
        assert!(output.stderr.contains("drained; exiting"));
    }
    assert_eq!(
        fixture.requests().len(),
        10 * node_count + 4,
        "no new claims during drain"
    );
    for job in &jobs {
        let snapshot = store.snapshot(*job).await.unwrap();
        assert_eq!(snapshot.state, JobState::Running);
        assert_eq!(snapshot.in_flight, 0);
        assert!(snapshot.frontier > 0);
    }
    let mut resumed = Vec::new();
    for _ in 0..node_count {
        resumed.push(start_node(&context).await);
    }
    finished(&store, jobs[0], &html_stats(2 * width + 2), 2 * width + 2).await;
    finished(&store, jobs[1], &html_stats(width + 1), width + 1).await;
    for node in &mut resumed {
        stop_node(node).await;
    }
    let outer_paths: Vec<_> = ["/load/".into(), "/load/sub/".into()]
        .into_iter()
        .chain((0..width).map(|i| format!("/load/p{i}")))
        .chain((0..width).map(|i| format!("/load/sub/q{i}")))
        .collect();
    let nested_paths: Vec<_> = std::iter::once("/load/sub/".into())
        .chain((0..width).map(|i| format!("/load/sub/q{i}")))
        .collect();
    assert_seen(&context, jobs[0], &fixture, &outer_paths).await;
    assert_seen(&context, jobs[1], &fixture, &nested_paths).await;
    let mut expected = HashMap::new();
    for path in outer_paths.into_iter().chain(nested_paths) {
        *expected.entry(path).or_default() += 1;
    }
    assert_eq!(
        request_counts(&fixture, 0),
        expected,
        "one GET per URL per job, including shared URLs"
    );
    assert!(fixture.peak() <= 10 * node_count);
    for (job, expected_files) in [(jobs[0], 2 * width + 2), (jobs[1], width + 1)] {
        let output = run(&context, &["stats", &job.to_string()]).await;
        success(&output);
        assert_eq!(
            output.stdout,
            format!(
                "files: {expected_files}   extensions: 1   words: {}\n  html {expected_files}\n",
                2 * expected_files
            )
        );
    }
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn large_non_html_streams_release_slots_and_finish_without_consuming_body_tails() {
    with_redis(|context| async move {
        let headers = Arc::new(Semaphore::new(0));
        let tails = Arc::new(Semaphore::new(0));
        let mut routes = vec![(
            "/stream/".into(),
            html(
                "Seed words",
                (0..12)
                    .map(|i| format!("large-{i}.ZIP"))
                    .chain(["healthy".into()]),
            ),
        )];
        for i in 0..12 {
            routes.push((
                format!("/stream/large-{i}.ZIP"),
                Reply::new(200, "application/zip", "prefix only")
                    .header("Content-Length", "104857600")
                    .gated_headers(&headers)
                    .gated_body("never sent tail", &tails),
            ));
        }
        routes.push(("/stream/healthy".into(), html("Healthy words", [])));
        let fixture = HttpFixture::start(routes).await;
        let base = fixture.url("/stream/");
        let job = ids(&run(&context, &["submit", base.as_str()]).await)[0];
        let mut node = start_node(&context).await;
        fixture.wait_for_requests(11).await;
        assert_eq!(fixture.peak(), 10);
        let store = context.store().await;
        let held = store.snapshot(job).await.unwrap();
        assert_eq!((held.processed, held.in_flight, held.frontier), (1, 10, 3));
        headers.add_permits(12);
        let expected =
            WebStats::from_counts(HashMap::from([("html".into(), 2), ("zip".into(), 12)]), 4)
                .unwrap();
        finished(&store, job, &expected, 14).await;
        fixture.wait_for_completed(14).await; // Server observes early socket close.
        assert_eq!(tails.available_permits(), 0, "no tail gate was ever opened");
        stop_node(&mut node).await;
        let counts = request_counts(&fixture, 0);
        assert_eq!(counts.len(), 14);
        assert!(counts.values().all(|count| *count == 1));
        let output = run(&context, &["stats", &job.to_string()]).await;
        success(&output);
        assert_eq!(
            output.stdout,
            "files: 14   extensions: 2   words: 4\n  html 2\n  zip 12\n"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn cross_node_failure_freezes_late_publication_but_drains_a_healthy_job() {
    with_redis(|context| async move {
        let bad_gate = Arc::new(Semaphore::new(0));
        let peers = Arc::new(Semaphore::new(0));
        let mut routes = vec![
            (
                "/fail/".into(),
                html(
                    "Seed words",
                    std::iter::once("bad?token=fixture-query".into())
                        .chain((0..12).map(|i| format!("p{i}"))),
                ),
            ),
            (
                "/fail/bad?token=fixture-query".into(),
                Reply::new(503, "text/html", "Server error").gated_headers(&bad_gate),
            ),
            (
                "/healthy/".into(),
                html("Healthy words", []).gated_body(" ", &peers),
            ),
        ];
        for i in 0..12 {
            routes.push((
                format!("/fail/p{i}"),
                html("Peer words", []).gated_body("<a href='late'></a>", &peers),
            ));
        }
        let fixture = HttpFixture::start(routes).await;
        let bad = fixture.url("/fail/");
        let good = fixture.url("/healthy/");
        let jobs = ids(&run(&context, &["submit", bad.as_str(), good.as_str()]).await);
        let store = context.store().await;
        let mut nodes = [start_node(&context).await, start_node(&context).await];
        fixture.wait_for_requests(15).await;
        fixture.wait_for_headers(14).await;
        let owners: HashMap<String, String> = context
            .query(redis::cmd("HGETALL").arg(context.job_key(jobs[0], "in-flight")))
            .await;
        let mut per_node = HashMap::<_, usize>::new();
        for owner in owners.into_values() {
            *per_node.entry(owner).or_default() += 1;
        }
        assert_eq!(per_node.len(), 2, "both processes must own the failing job");
        assert!(per_node.values().all(|count| *count <= 10));
        bad_gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.snapshot(jobs[0]).await.unwrap().state == JobState::Running {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bounded failed-state publication");
        let frozen = store.snapshot(jobs[0]).await.unwrap();
        assert_eq!(frozen.state, JobState::Failed(JobFailure::Fetch));
        assert_eq!(
            (frozen.processed, frozen.successful_files, frozen.in_flight),
            (1, 1, 13)
        );
        let failed_follow = run(&context, &["status", "-f", &jobs[0].to_string()]).await;
        assert_eq!(failed_follow.code, Some(1));
        assert!(failed_follow.stdout.ends_with("failed (fetch)\n"));
        assert_eq!(failed_follow.stdout.lines().count(), 1);
        peers.add_permits(13);
        for node in &mut nodes {
            let output = node.exit().await;
            assert_eq!(output.code, Some(1), "{}", output.stderr);
            assert!(output.stderr.contains("drained; exiting"));
            assert!(!output.stderr.contains("fixture-query"));
        }
        assert_eq!(
            store.snapshot(jobs[0]).await.unwrap(),
            frozen,
            "late peer publications must not reopen or contribute to a failed job"
        );
        finished(&store, jobs[1], &html_stats(1), 1).await;
        let failed_stats = run(&context, &["stats", &jobs[0].to_string()]).await;
        assert_eq!(failed_stats.code, Some(1));
        assert!(failed_stats.stdout.is_empty());
        assert!(failed_stats.stderr.contains("job failed"));
        let mut replacement = start_node(&context).await;
        assert_eq!(
            ids(&run(&context, &["submit", bad.as_str()]).await),
            [jobs[0]]
        );
        stop_node(&mut replacement).await;
        let counts = request_counts(&fixture, 0);
        assert_eq!(counts.len(), 15);
        assert!(counts.values().all(|count| *count == 1));
        assert!(!counts.contains_key("/fail/late"));
        assert_eq!(store.snapshot(jobs[0]).await.unwrap(), frozen);
    })
    .await
    .unwrap();
}
