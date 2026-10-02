//! Actual user commands against isolated real Redis; only nodes contact HTTP.
#![cfg(unix)]

#[path = "support/http.rs"]
mod http;
#[path = "support/process.rs"]
mod process;
mod support;

use std::{sync::Arc, time::Duration};

use http::{HttpFixture, Reply};
use process::{Output, Process, run};
use support::{Context, with_redis};
use swarmcrawl::jobs::{JobFailure, JobId, JobState, PageResult};
use tokio::sync::Semaphore;

fn success(output: &Output) {
    assert_eq!(output.code, Some(0), "{}", output.stderr);
    assert!(output.stderr.is_empty(), "{}", output.stderr);
}

fn ids(output: &Output) -> Vec<JobId> {
    success(output);
    output
        .stdout
        .lines()
        .map(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            assert_eq!(fields.len(), 5);
            assert_eq!(fields[0], "job");
            assert_eq!(fields[2], "input");
            fields[1].parse().unwrap()
        })
        .collect()
}

async fn start_node(context: &Context) -> Process {
    let mut node = Process::start(context, &["node", "--fetch-timeout-secs", "5"]);
    node.wait_stderr("ready; polling").await;
    node
}

async fn stop_node(node: &mut Process) {
    node.interrupt();
    let output = node.exit().await;
    assert_eq!(output.code, Some(0), "{}", output.stderr);
    assert!(output.stderr.contains("drained; exiting"));
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn batch_submission_returns_without_nodes_and_cli_never_retrieves_targets() {
    with_redis(|context| async move {
        let fixture = HttpFixture::start([
            ("/a/".into(), Reply::new(200, "text/html", "One two")),
            (
                "/b/?token=fixture-query".into(),
                Reply::new(200, "text/html", "Three four"),
            ),
        ])
        .await;
        let a = fixture.scope("/a/").base().clone();
        let b = fixture.url("/b/?token=fixture-query");
        let fragment = format!("{}#same", a.as_str());
        // A node would reject this HTTP setting. User commands must not even
        // construct a fetcher, and valid submissions must not wait for workers.
        let mut submit = Process::with_env(
            &context,
            &["submit", a.as_str(), b.as_str(), &fragment],
            &[("CRAWL_FETCH_TIMEOUT_SECS", "invalid-sensitive-setting")],
        );
        let output = submit.exit().await;
        let jobs = ids(&output);
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0], jobs[2]);
        assert_ne!(jobs[0], jobs[1]);
        let first = jobs[0];
        let second = jobs[1];
        assert_eq!(output.stdout, format!(
            "job {first}  input 1  created\njob {second}  input 2  created\njob {first}  input 3  existing\n"
        ));
        assert!(!output.stdout.contains("fixture-query"));
        let repeated = run(
            &context,
            &["submit", &format!("{}#other", a.as_str()), b.as_str()],
        )
        .await;
        assert_eq!(ids(&repeated), jobs[..2]);
        assert!(repeated.stdout.lines().all(|line| line.ends_with("existing")));
        for job in &jobs[..2] {
            let id = job.to_string();
            let snapshot = run(&context, &["status", &id]).await;
            success(&snapshot);
            assert_eq!(snapshot.stdout, format!(
                "job {job}  crawled 0  frontier 1  in flight 0  files 0  discovered 1  running\n"
            ));
            let stats = run(&context, &["stats", &id]).await;
            assert_eq!(stats.code, Some(1));
            assert!(stats.stdout.is_empty());
            assert!(stats.stderr.contains("still running"));
            assert_eq!(
                context.store().await.snapshot(*job).await.unwrap().state,
                JobState::Running,
            );
        }
        assert_eq!(
            context.query::<u64>(redis::cmd("HLEN").arg(context.key("submissions"))).await,
            2,
        );
        assert!(
            fixture.requests().is_empty(),
            "non-node CLI must never retrieve HTTP",
        );
        fixture.assert_healthy();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn invalid_batches_and_job_ids_fail_safely_without_creating_jobs() {
    with_redis(|context| async move {
        let invalid = run(
            &context,
            &[
                "submit",
                "https://example.org/valid/?token=fixture-query",
                "https://user:fixture-secret@example.org/",
            ],
        )
        .await;
        assert_eq!(invalid.code, Some(1));
        assert!(invalid.stdout.is_empty());
        assert!(invalid.stderr.contains("submit input 2"));
        assert!(invalid.stderr.contains("no URLs submitted"));
        assert!(!invalid.stderr.contains("fixture-"));
        assert!(context.keys().await.is_empty());
        for command in ["status", "stats"] {
            let invalid = run(&context, &[command, "fixture-secret"]).await;
            assert_eq!(invalid.code, Some(1));
            assert!(invalid.stdout.is_empty());
            assert!(invalid.stderr.contains("invalid job ID"));
            assert!(!invalid.stderr.contains("fixture-secret"));
            let unknown = run(&context, &[command, "999999"]).await;
            assert_eq!(unknown.code, Some(1));
            assert!(unknown.stdout.is_empty());
            assert!(unknown.stderr.contains("unknown job"));
        }
        let unknown = run(&context, &["status", "-f", "999999"]).await;
        assert_eq!(unknown.code, Some(1));
        assert!(unknown.stdout.is_empty());
        assert!(unknown.stderr.contains("unknown job"));
        assert!(context.keys().await.is_empty());
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn follow_prints_coherent_changes_then_exits_and_new_cli_reads_final_stats() {
    with_redis(|context| async move {
        let seed_gate = Arc::new(Semaphore::new(0));
        let child_gate = Arc::new(Semaphore::new(0));
        let fixture = HttpFixture::start([
            (
                "/site/".into(),
                Reply::new(200, "text/html", "<p>One two</p><img src=pic.JPG>").gated_body(
                    "<a href=next#late></a><a href=go></a><a href=missing></a>",
                    &seed_gate,
                ),
            ),
            (
                "/site/next".into(),
                Reply::new(200, "text/html", "<p>Three four</p>").gated_body(" ", &child_gate),
            ),
            (
                "/site/pic.JPG".into(),
                Reply::new(200, "image/jpeg", "bytes"),
            ),
            (
                "/site/go".into(),
                Reply::new(302, "text/plain", "").header("Location", "next#same"),
            ),
        ])
        .await;
        let base = fixture.url("/site/");
        let job = ids(&run(&context, &["submit", base.as_str()]).await)[0];
        let id = job.to_string();
        let mut follow = Process::start(&context, &["status", "-f", &id]);
        follow
            .wait_stdout("crawled 0  frontier 1  in flight 0")
            .await;
        let mut node = start_node(&context).await;
        fixture.wait_for_headers(1).await;
        follow
            .wait_stdout("crawled 0  frontier 0  in flight 1")
            .await;
        assert!(follow.running());
        seed_gate.add_permits(1);
        fixture.wait_for_headers(5).await;
        follow
            .wait_stdout("crawled 4  frontier 0  in flight 1")
            .await;
        let unfinished = run(&context, &["stats", &id]).await;
        assert_eq!(unfinished.code, Some(1));
        assert!(unfinished.stdout.is_empty(), "no partial WebStats");
        assert!(unfinished.stderr.contains("still running"));
        assert!(node.running(), "no node restart/finalization needed");
        child_gate.add_permits(1);
        let followed = follow.exit().await;
        success(&followed);
        let updates: Vec<_> = followed.stdout.lines().collect();
        assert!(
            updates.len() >= 4,
            "the gated milestones must all be printed"
        );
        assert!(updates.windows(2).all(|pair| pair[0] != pair[1]));
        for update in updates {
            let fields: Vec<_> = update.split_whitespace().collect();
            let crawled: u64 = fields[3].parse().unwrap();
            let frontier: u64 = fields[5].parse().unwrap();
            let in_flight: u64 = fields[8].parse().unwrap();
            let files: u64 = fields[10].parse().unwrap();
            let discovered: u64 = fields[12].parse().unwrap();
            assert_eq!(crawled + frontier + in_flight, discovered);
            assert!(files <= crawled);
        }
        assert!(followed.stdout.ends_with(&format!(
            "job {job}  crawled 5  frontier 0  in flight 0  files 3  discovered 5  done\n"
        )));
        let stats = run(&context, &["stats", &id]).await;
        success(&stats);
        assert_eq!(
            stats.stdout,
            "files: 3   extensions: 2   words: 4\n  html 2\n  jpg 1\n"
        );
        stop_node(&mut node).await;
        let retained = run(&context, &["stats", &id]).await;
        success(&retained);
        assert_eq!(retained.stdout, stats.stdout);
        let done_follow = run(&context, &["status", "--follow", &id]).await;
        success(&done_follow);
        assert_eq!(done_follow.stdout.lines().count(), 1);
        assert!(done_follow.stdout.ends_with("done\n"));
        let repeated = run(&context, &["submit", base.as_str()]).await;
        assert_eq!(ids(&repeated), [job]);
        assert!(repeated.stdout.ends_with("existing\n"));
        fixture.wait_for_completed(5).await;
        let mut targets = Vec::new();
        for request in fixture.requests() {
            assert!(request.head.contains(concat!(
                "user-agent: swarmcrawl/",
                env!("CARGO_PKG_VERSION")
            )));
            targets.push(request.target);
        }
        targets.sort();
        assert_eq!(
            targets,
            [
                "/site/",
                "/site/go",
                "/site/missing",
                "/site/next",
                "/site/pic.JPG"
            ]
        );
        fixture.assert_healthy();
        assert!(fixture.peak() <= 10);
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix signals and real Redis; run scripts/redis-smoke.sh"]
async fn ctrl_c_stops_follow_with_130_and_does_not_modify_or_cancel_the_job() {
    with_redis(|context| async move {
        let job = ids(&run(
            &context,
            &["submit", "https://example.org/docs/?token=fixture-query"],
        )
        .await)[0];
        let store = context.store().await;
        let before = store.snapshot(job).await.unwrap();
        let mut follow = Process::start(&context, &["status", "-f", &job.to_string()]);
        follow.wait_stdout("running").await;
        // A bounded quiet window crosses two poll intervals: unchanged progress
        // must not spam output. Response gates, not timing, drive progress tests.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(follow.running());
        assert_eq!(follow.stdout().lines().count(), 1);
        follow.interrupt();
        let output = follow.exit().await;
        assert_eq!(output.code, Some(130));
        assert!(output.stderr.contains("job continues unchanged"));
        assert!(!format!("{}{}", output.stdout, output.stderr).contains("fixture-query"));
        assert_eq!(store.snapshot(job).await.unwrap(), before);
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Unix processes and real Redis; run scripts/redis-smoke.sh"]
async fn failed_follow_snapshot_and_stats_are_nonzero_without_partial_totals_or_secrets() {
    with_redis(|context| async move {
        let gate = Arc::new(Semaphore::new(0));
        let fixture = HttpFixture::start([(
            "/bad/?token=fixture-query".into(),
            Reply::new(503, "text/html", "Server error").gated_headers(&gate),
        )])
        .await;
        let base = fixture.url("/bad/?token=fixture-query");
        let job = ids(&run(&context, &["submit", base.as_str()]).await)[0];
        let id = job.to_string();
        let mut follow = Process::start(&context, &["status", "-f", &id]);
        follow.wait_stdout("running").await;
        let mut node = start_node(&context).await;
        fixture.wait_for_requests(1).await;
        gate.add_permits(1);
        let node_output = node.exit().await;
        assert_eq!(node_output.code, Some(1));
        assert!(!node_output.stderr.contains("fixture-query"));
        let output = follow.exit().await;
        assert_eq!(output.code, Some(1));
        assert!(output.stdout.ends_with("failed (fetch)\n"));
        assert!(output.stderr.contains("final statistics are not available"));
        for args in [
            vec!["status", &id],
            vec!["status", "-f", &id],
            vec!["stats", &id],
        ] {
            let output = run(&context, &args).await;
            assert_eq!(output.code, Some(1));
            assert!(output.stderr.contains("job failed"));
            if args[0] == "stats" {
                assert!(output.stdout.is_empty());
            } else {
                assert!(output.stdout.ends_with("failed (fetch)\n"));
            }
            assert!(!format!("{}{}", output.stdout, output.stderr).contains("fixture-query"));
        }
        assert_eq!(
            context.store().await.snapshot(job).await.unwrap().state,
            JobState::Failed(JobFailure::Fetch)
        );
        let repeated = run(&context, &["submit", base.as_str()]).await;
        assert_eq!(ids(&repeated), [job]);
        assert!(repeated.stdout.ends_with("existing\n"));
        assert_eq!(fixture.requests().len(), 1);
        fixture.assert_healthy();
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn namespaces_and_global_flags_work_before_or_after_every_job_command() {
    with_redis(|context| async move {
        let base = "https://example.org/docs/";
        let env = [("CRAWL_JOB_NAMESPACE", "invalid namespace")];
        let mut before = Process::with_env(
            &context,
            &["--namespace", &context.namespace, "submit", base],
            &env,
        );
        let job = ids(&before.exit().await)[0];
        let id = job.to_string();
        let mut after = Process::with_env(
            &context,
            &["submit", base, "--namespace", &context.namespace],
            &env,
        );
        assert_eq!(ids(&after.exit().await), [job]);
        for args in [
            vec!["--namespace", &context.namespace, "status", &id],
            vec!["status", &id, "--namespace", &context.namespace],
        ] {
            success(&Process::with_env(&context, &args, &env).exit().await);
        }
        let other = format!("{}:other", context.namespace);
        let different = run(&context, &["submit", base, "--namespace", &other]).await;
        let other_job = ids(&different)[0];
        assert_eq!(job, other_job, "IDs are namespace-local");
        let stats = run(&context, &["stats", &id, "--namespace", &other]).await;
        assert_eq!(stats.code, Some(1));
        assert!(stats.stderr.contains("still running"));
        assert_eq!(
            context
                .query::<u64>(redis::cmd("HLEN").arg(format!("{other}:submissions")))
                .await,
            1
        );
        assert_eq!(
            context
                .query::<u64>(redis::cmd("HLEN").arg(context.key("submissions")))
                .await,
            1
        );
        let invalid = Process::with_env(&context, &["status", &id], &env)
            .exit()
            .await;
        assert_eq!(invalid.code, Some(1));
        assert!(invalid.stderr.contains("invalid job namespace"));
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn redis_errors_preserve_printed_ids_and_fail_follow_without_partial_results() {
    with_redis(|context| async move {
        let base = "https://example.org/existing/";
        let job = ids(&run(&context, &["submit", base]).await)[0];
        context
            .query::<String>(
                redis::cmd("SET")
                    .arg(context.key("next-job-id"))
                    .arg("fixture-secret"),
            )
            .await;
        let partial = run(&context, &["submit", base, "https://example.org/new/"]).await;
        assert_eq!(partial.code, Some(1));
        assert_eq!(partial.stdout, format!("job {job}  input 1  existing\n"));
        assert!(partial.stderr.contains("submit input 2"));
        assert!(partial.stderr.contains("earlier printed IDs remain valid"));
        assert!(!partial.stderr.contains("fixture-secret"));
        assert_eq!(
            context
                .query::<u64>(redis::cmd("HLEN").arg(context.key("submissions")))
                .await,
            1
        );
        context
            .hash_set(&context.job_key(job, "meta"), "processed", "fixture-secret")
            .await;
        for command in ["status", "stats"] {
            let output = run(&context, &[command, &job.to_string()]).await;
            assert_eq!(output.code, Some(1));
            assert!(output.stdout.is_empty());
            assert!(output.stderr.contains("invalid Redis job data"));
            assert!(!output.stderr.contains("fixture-secret"));
        }
        let follow = run(&context, &["status", "-f", &job.to_string()]).await;
        assert_eq!(follow.code, Some(1));
        assert!(follow.stdout.is_empty());
        assert!(follow.stderr.contains("invalid Redis job data"));
        assert!(!follow.stderr.contains("fixture-secret"));
        assert_eq!(
            context
                .query::<String>(
                    redis::cmd("HGET")
                        .arg(context.job_key(job, "meta"))
                        .arg("processed")
                )
                .await,
            "fixture-secret"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires real Redis; run scripts/redis-smoke.sh"]
async fn terminal_cli_reads_preserve_empty_results_and_full_unsigned_word_range() {
    with_redis(|context| async move {
        // Produce terminal states with the real protocol, not fabricated hashes.
        // HTTP behavior is covered above; here the CLI's numeric boundary is key.
        let output = run(
            &context,
            &[
                "submit",
                "https://example.org/empty/",
                "https://example.org/large/",
            ],
        )
        .await;
        let jobs = ids(&output);
        let store = context.store().await;
        let worker = store.allocate_worker().await.unwrap();
        for (job, result) in [
            (jobs[0], PageResult::NoFile),
            (
                jobs[1],
                PageResult::File {
                    html_word_count: u64::MAX,
                },
            ),
        ] {
            let claim = store.claim(job, &worker).await.unwrap().unwrap();
            store.complete(&claim, result, &[]).await.unwrap();
            let followed = run(&context, &["status", "-f", &job.to_string()]).await;
            success(&followed);
            assert_eq!(followed.stdout.lines().count(), 1);
            assert!(followed.stdout.ends_with("done\n"));
        }
        let empty = run(&context, &["stats", &jobs[0].to_string()]).await;
        success(&empty);
        assert_eq!(empty.stdout, "files: 0   extensions: 0   words: 0\n");
        let large = run(
            &context,
            &[
                "--namespace",
                &context.namespace,
                "stats",
                &jobs[1].to_string(),
            ],
        )
        .await;
        success(&large);
        assert_eq!(
            large.stdout,
            "files: 1   extensions: 1   words: 18446744073709551615\n  html 1\n"
        );
    })
    .await
    .unwrap();
}
