//! Static hand-count oracle for the live demo, not distributed/network evidence.

use swarmcrawl::{
    html::parse_html,
    stats::WebStats,
    urls::{CrawlScope, CrawlUrl},
};

#[test]
fn live_demo_graph_has_hand_checked_words_extensions_and_scope() {
    for base in [
        "http://127.0.0.1:8000/docs/",
        "http://127.0.0.1:8000/other/",
    ] {
        let scope = CrawlScope::new(CrawlUrl::parse(base).unwrap());
        let links = (1..=24)
            .map(|number| format!("<a href=\"page-{number:02}.html\">Page {number:02}</a>"))
            .collect::<Vec<_>>()
            .join("\n");
        let index = include_str!("../demo/index.html").replace("{{pages}}", &links);
        let parsed = parse_html(&scope, scope.base(), &index).unwrap();
        // 2 title + 2 paragraph + 24 Page labels + Alias/Missing/Outside.
        assert_eq!(parsed.word_count, 31);
        // 24 leaves, 4 assets, alias and missing; outside/fragment duplicates excluded.
        assert_eq!(parsed.links.len(), 30);
        assert!(parsed.links.iter().all(|url| scope.contains(url)));
        let mut stats = WebStats::default();
        stats
            .checked_merge(&WebStats::for_file(scope.base(), parsed.word_count))
            .unwrap();
        for url in &parsed.links {
            let suffix = url.as_str().strip_prefix(base).unwrap();
            if matches!(suffix, "alias" | "missing") {
                continue;
            }
            let words = if suffix.starts_with("page-") {
                let parsed = parse_html(
                    &scope,
                    url,
                    "<p>Leaf ant</p><a href=\"shared.html#convergence\"></a><a href=\"./#cycle\"></a>",
                )
                .unwrap();
                assert_eq!(parsed.word_count, 2);
                assert_eq!(parsed.links.len(), 2);
                parsed.word_count
            } else {
                0 // Header-only existing non-HTML resources, including data -> html.
            };
            stats
                .checked_merge(&WebStats::for_file(url, words))
                .unwrap();
        }
        let shared = scope.base().resolve("shared.html").unwrap();
        let parsed = parse_html(&scope, &shared, "<p>Shared leaf</p><a href=\"./\"></a>").unwrap();
        assert_eq!(parsed.word_count, 2);
        stats
            .checked_merge(&WebStats::for_file(&shared, parsed.word_count))
            .unwrap();
        assert_eq!(
            (stats.num_files, stats.num_exts, stats.total_word_count),
            (30, 4, 81)
        );
        assert_eq!(stats.ext_counts["html"], 27);
        for extension in ["jpeg", "jpg", "pdf"] {
            assert_eq!(stats.ext_counts[extension], 1);
        }
        stats.validate().unwrap();
    }
}
