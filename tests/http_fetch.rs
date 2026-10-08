//! Fetcher component tests, not application-node or distributed scheduling evidence.

#[path = "support/http.rs"]
mod http;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    io::Write,
    sync::Arc,
    time::Duration,
};

use flate2::{Compression, write::GzEncoder};
use swarmcrawl::{
    config::FetchConfig,
    fetch::{FetchError, Fetcher, MAX_NODE_REQUESTS},
    jobs::PageResult,
    stats::WebStats,
    urls::{CrawlScope, CrawlUrl},
};
use tokio::{
    sync::{Barrier, Semaphore},
    task::JoinSet,
};

use http::{HttpFixture, Reply};

async fn bounded(case: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(15), case)
        .await
        .expect("bounded fetcher test case");
}

fn fetcher() -> Fetcher {
    Fetcher::new(FetchConfig::default()).unwrap()
}

#[tokio::test]
async fn sequential_distinct_urls_share_a_tcp_connection_unless_server_says_close() {
    bounded(async {
        for close in [false, true] {
            let paths: Vec<_> = (0..12).map(|i| format!("/docs/page-{i}")).collect();
            let fixture = HttpFixture::start(paths.iter().map(|path| {
                let reply = Reply::new(200, "text/html", "Whole body");
                (
                    path.clone(),
                    if close {
                        reply.header("Connection", "close")
                    } else {
                        reply
                    },
                )
            }))
            .await;
            let fetcher = fetcher();
            for path in &paths {
                assert_eq!(
                    fetcher
                        .fetch(&fixture.scope("/docs/"), &fixture.url(path))
                        .await
                        .unwrap()
                        .result,
                    PageResult::File { html_word_count: 2 }
                );
            }
            let expected_connections = if close { paths.len() } else { 1 };
            assert_eq!(fixture.connections(), expected_connections);
            let requests = fixture.requests();
            assert_eq!(
                requests.iter().map(|r| &r.target).collect::<Vec<_>>(),
                paths.iter().collect::<Vec<_>>()
            );
            assert_eq!(
                requests
                    .iter()
                    .map(|r| r.connection)
                    .collect::<HashSet<_>>()
                    .len(),
                expected_connections
            );
            // Dropping the shared client releases idle sockets as well as requests.
            drop(fetcher);
            http::wait_until(|| fixture.closed_connections() == expected_connections).await;
            fixture.assert_healthy();
        }
    })
    .await;
}

#[tokio::test]
async fn a_gated_stale_idle_connection_is_replaced_without_duplicate_gets() {
    bounded(async {
        let close = Arc::new(Semaphore::new(0));
        let mut warm = Reply::new(200, "text/html", "Warm body");
        warm.idle_close_gate = Some(close.clone());
        let fixture = HttpFixture::start([
            ("/docs/warm".into(), warm.clone()),
            ("/docs/warm-again".into(), warm),
            (
                "/docs/next".into(),
                Reply::new(200, "text/html", "Next body"),
            ),
        ])
        .await;
        let fetcher = fetcher();
        let scope = fixture.scope("/docs/");
        for path in ["/docs/warm", "/docs/warm-again"] {
            fetcher.fetch(&scope, &fixture.url(path)).await.unwrap();
        }
        assert_eq!(
            fixture.connections(),
            1,
            "prove the socket was reusable before closing it"
        );
        fixture.wait_for_completed(2).await;
        close.add_permits(1);
        http::wait_until(|| fixture.closed_connections() == 1).await;
        assert_eq!(
            fetcher
                .fetch(&scope, &fixture.url("/docs/next"))
                .await
                .unwrap()
                .result,
            PageResult::File { html_word_count: 2 }
        );
        assert_eq!(fixture.connections(), 2);
        let requests = fixture.requests();
        assert_eq!(
            requests
                .iter()
                .map(|r| r.target.as_str())
                .collect::<Vec<_>>(),
            ["/docs/warm", "/docs/warm-again", "/docs/next"]
        );
        assert_eq!(
            requests.iter().map(|r| r.connection).collect::<Vec<_>>(),
            [1, 1, 2]
        );
        drop(fetcher);
        http::wait_until(|| fixture.closed_connections() == 2).await;
        fixture.assert_healthy();
        // This observes a server-closed idle socket. Whether hyper notices the
        // close before checkout or recovers an unstarted request is private;
        // safety of that exact boundary is established by the pinned-source audit.
    })
    .await;
}

#[tokio::test]
async fn silent_server_closure_after_a_complete_response_does_not_replay_it() {
    bounded(async {
        let mut reply = Reply::new(200, "text/html", "Complete body");
        reply.close_after_body = true; // No Connection: close notification.
        let fixture = HttpFixture::start([
            ("/docs/first".into(), reply),
            (
                "/docs/second".into(),
                Reply::new(200, "text/html", "Second body"),
            ),
        ])
        .await;
        let fetcher = fetcher();
        for path in ["/docs/first", "/docs/second"] {
            assert_eq!(
                fetcher
                    .fetch(&fixture.scope("/docs/"), &fixture.url(path))
                    .await
                    .unwrap()
                    .result,
                PageResult::File { html_word_count: 2 }
            );
        }
        assert_eq!(fixture.connections(), 2);
        assert_eq!(fixture.requests().len(), 2);
        assert_eq!(fixture.requests()[0].connection, 1);
        assert_eq!(fixture.requests()[1].connection, 2);
        drop(fetcher);
        http::wait_until(|| fixture.closed_connections() == 2).await;
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn started_gets_on_reused_connections_are_not_replayed_on_header_body_or_status_faults() {
    bounded(async {
        let partial = "<p>Partial words</p><a href='never'>Never</a>";
        for fault in ["no-headers", "truncated", "chunked", "status"] {
            let gate = Arc::new(Semaphore::new(0));
            let (mut broken, expected) = match fault {
                "no-headers" => (
                    Reply::new(0, "text/html", "").gated_headers(&gate),
                    FetchError::Transport,
                ),
                "truncated" => (
                    Reply::new(200, "text/html", partial)
                        .header("Content-Length", "10000")
                        .gated_body("", &gate),
                    FetchError::Body,
                ),
                "chunked" => (
                    Reply::new(
                        200,
                        "text/html",
                        format!("{:x}\r\n{partial}\r\n", partial.len()),
                    )
                    .header("Transfer-Encoding", "chunked")
                    .gated_body("", &gate),
                    FetchError::Body,
                ),
                "status" => (
                    Reply::new(503, "text/html", partial).gated_headers(&gate),
                    FetchError::UnexpectedStatus(503),
                ),
                _ => unreachable!(),
            };
            broken.close_after_body = true;
            let fixture = HttpFixture::start([
                (
                    "/docs/warm".into(),
                    Reply::new(200, "text/html", "Warm body"),
                ),
                ("/docs/broken".into(), broken),
            ])
            .await;
            let fetcher = fetcher();
            fetcher
                .fetch(&fixture.scope("/docs/"), &fixture.url("/docs/warm"))
                .await
                .unwrap();
            let clone = fetcher.clone();
            let scope = fixture.scope("/docs/");
            let url = fixture.url("/docs/broken");
            // JoinSet also cancels this owned future if the test fails or times out.
            let mut tasks = JoinSet::new();
            tasks.spawn(async move { clone.fetch(&scope, &url).await });
            fixture.wait_for_requests(2).await;
            if matches!(fault, "truncated" | "chunked") {
                fixture.wait_for_headers(2).await;
            }
            assert!(
                tasks.try_join_next().is_none(),
                "fault gate must hold the fetch: {fault}"
            );
            assert_eq!(
                fixture.connections(),
                1,
                "fault must occur on a reused socket: {fault}"
            );
            gate.add_permits(1);
            assert_eq!(
                tasks.join_next().await.unwrap().unwrap(),
                Err(expected),
                "{fault}"
            );
            // No successful outcome or discoveries, even though the HTML prefix
            // alone contains countable words and an in-scope link.
            drop(fetcher);
            http::wait_until(|| fixture.closed_connections() == fixture.connections()).await;
            assert_eq!(
                fixture.connections(),
                1,
                "no reconnect/replay of a started GET: {fault}"
            );
            assert_eq!(
                fixture
                    .requests()
                    .iter()
                    .map(|r| r.target.as_str())
                    .collect::<Vec<_>>(),
                ["/docs/warm", "/docs/broken"],
                "{fault}"
            );
            fixture.assert_healthy();
        }
    })
    .await;
}

#[tokio::test]
async fn mime_not_suffix_controls_full_document_extraction_and_file_results() {
    bounded(async {
        let source = format!(
            "<title>Ant ant</title><!-- ignored words --><p title='ignored'>Bee 123cat Élan</p>\
             <script>ignored words</script><style>ignored words</style><p>{}</p>\
             <a href='last#one'>Tail</a><a href='./last#two'></a>\
             <a href='/outside'></a><img src='asset.JPG'>",
            "word ".repeat(10_000)
        );
        let mut no_mime = Reply::new(200, "", "Not classified as HTML");
        no_mime.headers.clear();
        let fixture = HttpFixture::start([
            (
                "/docs/page.bin".into(),
                Reply::new(200, "TEXT/HTML; charset=UTF-8", source),
            ),
            (
                "/docs/plain.html".into(),
                Reply::new(200, "text/plain", "<a href='hidden'>Not HTML</a>"),
            ),
            ("/docs/api".into(), Reply::new(200, "image/png", "PNG")),
            ("/docs/no-mime".into(), no_mime),
            (
                "/docs/page.xml".into(),
                Reply::new(200, "application/xhtml+xml", "<p>XHTML works</p>"),
            ),
            ("/docs/empty".into(), Reply::new(204, "text/html", "")),
        ])
        .await;
        let fetcher = fetcher();
        let scope = fixture.scope("/docs/");
        let result = fetcher
            .fetch(&scope, &fixture.url("/docs/page.bin"))
            .await
            .unwrap();
        assert_eq!(
            result.result,
            PageResult::File {
                html_word_count: 10_004
            }
        );
        assert_eq!(
            result.discoveries,
            vec![fixture.url("/docs/last"), fixture.url("/docs/asset.JPG")]
        );
        for path in [
            "/docs/plain.html",
            "/docs/api",
            "/docs/no-mime",
            "/docs/empty",
        ] {
            let result = fetcher.fetch(&scope, &fixture.url(path)).await.unwrap();
            assert_eq!(result.result, PageResult::File { html_word_count: 0 });
            assert!(result.discoveries.is_empty());
        }
        assert_eq!(fixture.url("/docs/api").extension(), "html");
        let result = fetcher
            .fetch(&scope, &fixture.url("/docs/page.xml"))
            .await
            .unwrap();
        assert_eq!(result.result, PageResult::File { html_word_count: 2 });
        assert_eq!(fixture.requests().len(), 6);
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn broken_responses_are_no_file_but_unreliable_or_partial_outcomes_are_errors() {
    bounded(async {
        let absent = [400, 401, 403, 404, 410, 451];
        let unreliable = [206, 300, 304, 305, 408, 429, 500, 503];
        let routes = absent.into_iter().chain(unreliable).map(|status| {
            (
                format!("/docs/{status}"),
                Reply::new(
                    status,
                    "text/html",
                    "<a href='hidden'>Never parse error bodies</a>",
                ),
            )
        });
        let fixture = HttpFixture::start(routes).await;
        let fetcher = fetcher();
        let scope = fixture.scope("/docs/");
        for status in absent {
            let outcome = fetcher
                .fetch(&scope, &fixture.url(&format!("/docs/{status}")))
                .await
                .unwrap();
            assert_eq!(outcome.result, PageResult::NoFile);
            assert!(outcome.discoveries.is_empty());
        }
        for status in unreliable {
            let error = fetcher
                .fetch(&scope, &fixture.url(&format!("/docs/{status}")))
                .await
                .unwrap_err();
            assert_eq!(error, FetchError::UnexpectedStatus(status));
        }
        assert_eq!(fixture.requests().len(), absent.len() + unreliable.len());
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn redirect_chains_loops_and_convergence_are_discoveries_not_hidden_fetches() {
    bounded(async {
        let external = HttpFixture::start([]).await;
        let fixture = HttpFixture::start([
            ("/docs/".into(), Reply::new(200, "text/html", "\
                <a href='one'>One</a><a href='two'>Two</a><a href='cycle-a'>Loop</a>\
                <a href='outside'>Out</a><a href='foreign'>Foreign</a><a href='credentials'>Credentials</a>")),
            ("/docs/one".into(), Reply::new(301, "text/html", "Ignored body").header("Location", "middle")),
            ("/docs/middle".into(), Reply::new(302, "text/plain", "").header("Location", "shared?view=1#one")),
            ("/docs/two".into(), Reply::new(303, "text/plain", "").header("Location", "shared?view=1#two")),
            ("/docs/cycle-a".into(), Reply::new(307, "text/plain", "").header("Location", "cycle-b")),
            ("/docs/cycle-b".into(), Reply::new(308, "text/plain", "").header("Location", "cycle-a#loop")),
            ("/docs/outside".into(), Reply::new(302, "text/plain", "").header("Location", "/escape")),
            ("/docs/foreign".into(), Reply::new(302, "text/plain", "").header("Location", external.url("/docs/escape").as_str())),
            ("/docs/credentials".into(), Reply::new(302, "text/plain", "").header("Location", "http://user:fixture-secret@127.0.0.1/docs/")),
            ("/docs/shared?view=1".into(), Reply::new(200, "text/html", "<p>Final words</p><img src='asset.PDF'><a href='shared?view=1#self'></a>")),
            ("/docs/asset.PDF".into(), Reply::new(200, "application/pdf", "PDF")),
        ]).await;
        let fetcher = fetcher();
        let scope = fixture.scope("/docs/");
        let mut queue = VecDeque::from([scope.base().clone()]);
        let mut seen = HashSet::from([scope.base().clone()]);
        let mut stats = WebStats::default();
        // This tiny test-only queue models a caller admitting discoveries. Real
        // cluster-wide deduplication is separately tested by the Redis frontier suite.
        while let Some(url) = queue.pop_front() {
            let before = fixture.requests().len();
            let outcome = fetcher.fetch(&scope, &url).await.unwrap();
            assert_eq!(fixture.requests().len(), before + 1, "one GET per fetch call");
            if let PageResult::File { html_word_count } = outcome.result {
                stats.checked_merge(&WebStats::for_file(&url, html_word_count)).unwrap();
            }
            for discovery in outcome.discoveries {
                assert!(scope.contains(&discovery));
                if seen.insert(discovery.clone()) {
                    queue.push_back(discovery);
                }
            }
        }
        assert_eq!(stats, WebStats::from_counts(HashMap::from([("html".into(), 2), ("pdf".into(), 1)]), 8).unwrap());
        assert_eq!(fixture.requests().len(), 11);
        assert_eq!(fixture.requests().iter().map(|request| &request.target).collect::<HashSet<_>>().len(), 11);
        assert!(external.requests().is_empty());
        assert!(!fixture.requests().iter().any(|request| request.target == "/escape"));
        fixture.assert_healthy();
        external.assert_healthy();
    }).await;
}

#[tokio::test]
async fn scope_is_checked_before_get_and_redirect_headers_are_validated_safely() {
    bounded(async {
        let fixture = HttpFixture::start([
            ("/docs/missing".into(), Reply::new(301, "text/plain", "")),
            (
                "/docs/non-ascii".into(),
                Reply::new(302, "text/plain", "").header("Location", "é"),
            ),
            (
                "/docs/unsupported".into(),
                Reply::new(303, "text/plain", "").header("Location", "mailto:a@example.org"),
            ),
            (
                "/docs/self?token=fixture-query".into(),
                Reply::new(308, "text/plain", "").header("Location", "#fragment"),
            ),
        ])
        .await;
        let fetcher = fetcher();
        let scope = fixture.scope("/docs/");
        assert_eq!(
            fetcher
                .fetch(&scope, &fixture.url("/outside?token=fixture-query"))
                .await,
            Err(FetchError::OutsideScope)
        );
        assert!(fixture.requests().is_empty());
        for path in ["/docs/missing", "/docs/non-ascii"] {
            let error = fetcher.fetch(&scope, &fixture.url(path)).await.unwrap_err();
            assert_eq!(error, FetchError::InvalidRedirect);
            assert!(!format!("{error}: {error:?}").contains("fixture-query"));
        }
        let outcome = fetcher
            .fetch(&scope, &fixture.url("/docs/unsupported"))
            .await
            .unwrap();
        assert_eq!(outcome.result, PageResult::NoFile);
        assert!(outcome.discoveries.is_empty());
        let url = fixture.url("/docs/self?token=fixture-query");
        let outcome = fetcher.fetch(&scope, &url).await.unwrap();
        assert_eq!(outcome.discoveries, vec![url]);
        assert!(!format!("{outcome:?}").contains("fixture-query"));
        assert_eq!(fixture.requests().len(), 4);
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn html_encoding_is_explicit_complete_and_never_lossy() {
    bounded(async {
        let source = "<p>Alpha beta</p><a href='tail'>Tail</a>";
        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(source.as_bytes()).unwrap();
        let gzip = gzip.finish().unwrap();
        let mut utf16 = vec![0xff, 0xfe];
        utf16.extend(source.encode_utf16().flat_map(u16::to_le_bytes));
        let mut utf8_bom = vec![0xef, 0xbb, 0xbf];
        utf8_bom.extend_from_slice(source.as_bytes());
        let fixture = HttpFixture::start([
            (
                "/docs/gzip".into(),
                Reply::new(200, "text/html", gzip).header("Content-Encoding", "gzip"),
            ),
            ("/docs/utf16".into(), Reply::new(200, "text/html", utf16)),
            (
                "/docs/bom".into(),
                Reply::new(200, "text/html; charset=windows-1252", utf8_bom),
            ),
            (
                "/docs/legacy".into(),
                Reply::new(
                    200,
                    "text/html; charset=\"windows-1252\"",
                    b"<p>caf\xe9 ant</p><a href='tail'>Tail</a>".to_vec(),
                ),
            ),
            (
                "/docs/identity".into(),
                Reply::new(200, "text/html", source).header("Content-Encoding", "identity"),
            ),
            (
                "/docs/unknown-charset".into(),
                Reply::new(200, "text/html; charset=x-unknown", source),
            ),
            (
                "/docs/invalid-text".into(),
                Reply::new(200, "text/html", vec![0xff, b'a']),
            ),
            (
                "/docs/unsupported-encoding".into(),
                Reply::new(200, "text/html", source).header("Content-Encoding", "br"),
            ),
            (
                "/docs/bad-gzip".into(),
                Reply::new(200, "text/html", "not gzip").header("Content-Encoding", "gzip"),
            ),
            (
                "/docs/bad-mime".into(),
                Reply::new(200, "text/html; charset", source),
            ),
            (
                "/docs/non-ascii-mime".into(),
                Reply::new(200, "text/html; note=é", source),
            ),
            (
                "/docs/duplicate-mime".into(),
                Reply::new(200, "text/html", source).header("Content-Type", "text/plain"),
            ),
        ])
        .await;
        let fetcher = fetcher();
        let scope = fixture.scope("/docs/");
        for path in [
            "/docs/gzip",
            "/docs/utf16",
            "/docs/bom",
            "/docs/legacy",
            "/docs/identity",
        ] {
            let result = fetcher.fetch(&scope, &fixture.url(path)).await.unwrap();
            assert_eq!(result.result, PageResult::File { html_word_count: 3 });
            assert_eq!(result.discoveries, vec![fixture.url("/docs/tail")]);
        }
        for (path, expected) in [
            ("/docs/unknown-charset", FetchError::UnsupportedCharset),
            ("/docs/invalid-text", FetchError::InvalidTextEncoding),
            (
                "/docs/unsupported-encoding",
                FetchError::UnsupportedContentEncoding,
            ),
            ("/docs/bad-gzip", FetchError::Body),
            ("/docs/bad-mime", FetchError::InvalidHeader),
            ("/docs/non-ascii-mime", FetchError::InvalidHeader),
            ("/docs/duplicate-mime", FetchError::InvalidHeader),
        ] {
            assert_eq!(
                fetcher.fetch(&scope, &fixture.url(path)).await,
                Err(expected)
            );
        }
        let requests = fixture.requests();
        assert_eq!(requests.len(), 12);
        assert!(requests.iter().all(|request| {
            request
                .head
                .to_ascii_lowercase()
                .contains("accept-encoding: gzip\r\n")
        }));
        assert!(
            requests
                .iter()
                .all(|request| !request.head.to_ascii_lowercase().contains("referer:"))
        );
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn large_non_html_bodies_are_dropped_without_waiting_for_or_reading_the_tail() {
    bounded(async {
        let gate = Arc::new(Semaphore::new(0));
        let mut routes: Vec<_> = (0..MAX_NODE_REQUESTS)
            .map(|index| {
                (
                    format!("/docs/large-{index}.zip"),
                    Reply::new(200, "application/zip", "prefix only")
                        .header("Content-Length", "104857600") // Advertise 100 MiB.
                        .gated_body("never requested tail", &gate),
                )
            })
            .collect();
        routes.push((
            "/docs/healthy".into(),
            Reply::new(200, "text/html", "Healthy"),
        ));
        let fixture = HttpFixture::start(routes).await;
        let fetcher = fetcher();
        let mut tasks = JoinSet::new();
        for index in 0..MAX_NODE_REQUESTS {
            let clone = fetcher.clone();
            let scope = fixture.scope("/docs/");
            let url = fixture.url(&format!("/docs/large-{index}.zip"));
            tasks.spawn(async move { clone.fetch(&scope, &url).await });
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(result) = tasks.join_next().await {
                let outcome = result.unwrap().unwrap();
                assert_eq!(outcome.result, PageResult::File { html_word_count: 0 });
                assert!(outcome.discoveries.is_empty());
            }
        })
        .await
        .expect("non-HTML fetch must not await a gated body tail");
        // The fixture sees the client close its socket while the tail gate stays shut.
        fixture.wait_for_completed(MAX_NODE_REQUESTS).await;
        assert_eq!(gate.available_permits(), 0);
        let outcome = fetcher
            .fetch(&fixture.scope("/docs/"), &fixture.url("/docs/healthy"))
            .await
            .unwrap();
        assert_eq!(outcome.result, PageResult::File { html_word_count: 1 });
        assert_eq!(fixture.requests().len(), MAX_NODE_REQUESTS + 1);
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn cloned_fetcher_enforces_one_ten_request_budget_across_jobs_and_body_lifetime() {
    bounded(async {
        let gate = Arc::new(Semaphore::new(0));
        let paths: Vec<_> = (0..31)
            .map(|index| format!("/{}/page-{index}", if index % 2 == 0 { "a" } else { "b" }))
            .collect();
        let fixture = HttpFixture::start(paths.iter().map(|path| {
            (
                path.clone(),
                Reply::new(200, "text/html", "Alpha ").gated_body("beta", &gate),
            )
        }))
        .await;
        let fetcher = fetcher();
        let start = Arc::new(Barrier::new(paths.len() + 1));
        let mut tasks = JoinSet::new();
        for (index, path) in paths.iter().enumerate() {
            let clone = fetcher.clone();
            let scope = fixture.scope(if index % 2 == 0 { "/a/" } else { "/b/" });
            let url = fixture.url(path);
            let start = start.clone();
            tasks.spawn(async move {
                start.wait().await;
                clone.fetch(&scope, &url).await
            });
        }
        start.wait().await;
        fixture.wait_for_headers(MAX_NODE_REQUESTS).await;
        assert_eq!(fixture.requests().len(), MAX_NODE_REQUESTS);
        assert_eq!(fixture.peak(), MAX_NODE_REQUESTS);
        // Nine responses remain in body use; completing exactly one admits only
        // the eleventh GET, whose body also blocks. No per-job budget can do this.
        gate.add_permits(1);
        fixture.wait_for_headers(MAX_NODE_REQUESTS + 1).await;
        assert_eq!(fixture.requests().len(), MAX_NODE_REQUESTS + 1);
        assert_eq!(fixture.peak(), MAX_NODE_REQUESTS);
        gate.add_permits(paths.len());
        while let Some(result) = tasks.join_next().await {
            assert_eq!(
                result.unwrap().unwrap().result,
                PageResult::File { html_word_count: 2 }
            );
        }
        assert_eq!(fixture.requests().len(), paths.len());
        assert_eq!(fixture.peak(), MAX_NODE_REQUESTS);
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn deadlines_transfer_failures_and_disconnects_are_not_broken_links_or_retried() {
    bounded(async {
        let header_gate = Arc::new(Semaphore::new(0));
        let body_gate = Arc::new(Semaphore::new(0));
        let mut routes: Vec<_> = (0..MAX_NODE_REQUESTS)
            .map(|index| {
                (
                    format!("/docs/silent-{index}"),
                    Reply::new(200, "text/html", "Hello").gated_headers(&header_gate),
                )
            })
            .collect();
        routes.extend([
            (
                "/docs/slow-body".into(),
                Reply::new(200, "text/html", "Alpha ").gated_body("beta", &body_gate),
            ),
            (
                "/docs/truncated".into(),
                Reply::new(200, "text/html", "short")
                    .header("Content-Length", "100")
                    .header("Connection", "close"),
            ),
            (
                "/docs/disconnect?token=fixture-query".into(),
                Reply::new(0, "text/plain", ""),
            ),
            (
                "/docs/healthy".into(),
                Reply::new(200, "text/html", "Healthy"),
            ),
        ]);
        let fixture = HttpFixture::start(routes).await;
        let fetcher = Fetcher::new(FetchConfig::new(1).unwrap()).unwrap();
        let mut tasks = JoinSet::new();
        for index in 0..MAX_NODE_REQUESTS {
            let clone = fetcher.clone();
            let scope = fixture.scope("/docs/");
            let url = fixture.url(&format!("/docs/silent-{index}"));
            tasks.spawn(async move { clone.fetch(&scope, &url).await });
        }
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap(), Err(FetchError::Timeout));
        }
        fixture.wait_for_completed(MAX_NODE_REQUESTS).await;
        let scope = fixture.scope("/docs/");
        for (path, expected) in [
            ("/docs/slow-body", FetchError::Timeout),
            ("/docs/truncated", FetchError::Body),
            (
                "/docs/disconnect?token=fixture-query",
                FetchError::Transport,
            ),
        ] {
            let error = fetcher.fetch(&scope, &fixture.url(path)).await.unwrap_err();
            assert_eq!(error, expected);
            assert!(!format!("{error}: {error:?}").contains("fixture-query"));
            assert!(!format!("{error}: {error:?}").contains("127.0.0.1"));
        }
        let outcome = fetcher
            .fetch(&scope, &fixture.url("/docs/healthy"))
            .await
            .unwrap();
        assert_eq!(outcome.result, PageResult::File { html_word_count: 1 });
        assert_eq!(fixture.requests().len(), MAX_NODE_REQUESTS + 4);
        assert_eq!(
            fixture
                .requests()
                .iter()
                .map(|request| &request.target)
                .collect::<HashSet<_>>()
                .len(),
            MAX_NODE_REQUESTS + 4
        );
        fixture.assert_healthy();
    })
    .await;
}

#[tokio::test]
async fn cancelling_fetch_futures_releases_responses_and_the_shared_budget() {
    bounded(async {
        let gate = Arc::new(Semaphore::new(0));
        let mut routes: Vec<_> = (0..MAX_NODE_REQUESTS)
            .map(|index| {
                (
                    format!("/docs/blocked-{index}"),
                    Reply::new(200, "text/html", "Alpha ").gated_body("beta", &gate),
                )
            })
            .collect();
        routes.push((
            "/docs/healthy".into(),
            Reply::new(200, "text/html", "Healthy"),
        ));
        let fixture = HttpFixture::start(routes).await;
        let fetcher = fetcher();
        let mut tasks = JoinSet::new();
        for index in 0..MAX_NODE_REQUESTS {
            let clone = fetcher.clone();
            let scope = fixture.scope("/docs/");
            let url = fixture.url(&format!("/docs/blocked-{index}"));
            tasks.spawn(async move { clone.fetch(&scope, &url).await });
        }
        fixture.wait_for_headers(MAX_NODE_REQUESTS).await;
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap_err().is_cancelled());
        }
        fixture.wait_for_completed(MAX_NODE_REQUESTS).await;
        let outcome = fetcher
            .fetch(&fixture.scope("/docs/"), &fixture.url("/docs/healthy"))
            .await
            .unwrap();
        assert_eq!(outcome.result, PageResult::File { html_word_count: 1 });
        fixture.wait_for_requests(MAX_NODE_REQUESTS + 1).await;
        fixture.assert_healthy();
        // This is local resource cleanup, not job cancellation or lost-claim recovery.
    })
    .await;
}

#[tokio::test]
async fn unreachable_http_peer_returns_a_secret_safe_operational_error() {
    bounded(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = CrawlUrl::parse(&format!(
            "http://{}/?token=fixture-query",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        drop(listener);
        let error = fetcher()
            .fetch(&CrawlScope::new(url.clone()), &url)
            .await
            .unwrap_err();
        assert_eq!(error, FetchError::Transport);
        assert!(!format!("{error}: {error:?}").contains("fixture-query"));
    })
    .await;
}
