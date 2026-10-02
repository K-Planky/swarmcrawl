//! A static, hand-checkable graph exercises domain contracts together. The table
//! supplies existence and MIME observations; this is NOT HTTP/Redis verification.

use std::collections::{HashMap, HashSet, VecDeque};

use swarmcrawl::{
    html::{is_html_content_type, parse_html},
    stats::WebStats,
    urls::{CrawlScope, CrawlUrl},
};

#[test]
fn hand_checked_fixture_combines_identity_mime_links_words_and_extensions() {
    // about.dat is HTML despite its suffix; api is not HTML despite its fallback
    // extension. missing.png is linked but absent from the existence table.
    let resources = HashMap::from([
        (
            "",
            (
                "text/html; charset=utf-8",
                include_str!("fixtures/domain-index.html"),
            ),
        ),
        (
            "about.dat",
            (
                "application/xhtml+xml",
                include_str!("fixtures/domain-about.html"),
            ),
        ),
        ("photo.JPG", ("image/jpeg", "")),
        ("photo.jpeg", ("image/jpeg", "")),
        ("api", ("application/json", "non HTML words do not count")),
    ]);
    let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/site/").unwrap());
    let seed = parse_html(&scope, scope.base(), resources[""].1).unwrap();
    // 6 title/heading/paragraph words + 9 anchor-text words = 15.
    assert_eq!(seed.word_count, 15);
    let suffixes: Vec<_> = seed
        .links
        .iter()
        .map(|url| url.as_str().strip_prefix(scope.base().as_str()).unwrap())
        .collect();
    assert_eq!(
        suffixes,
        ["about.dat", "photo.JPG", "photo.jpeg", "api", "missing.png"]
    );

    let mut frontier = VecDeque::from([scope.base().clone()]);
    let mut seen = HashSet::from([scope.base().clone()]);
    let mut stats = WebStats::default();
    while let Some(url) = frontier.pop_front() {
        assert!(scope.contains(&url));
        let suffix = url.as_str().strip_prefix(scope.base().as_str()).unwrap();
        let Some((mime, body)) = resources.get(suffix) else {
            continue;
        };
        let words = if is_html_content_type(mime) {
            let parsed = parse_html(&scope, &url, body).unwrap();
            for link in parsed.links {
                if seen.insert(link.clone()) {
                    frontier.push_back(link);
                }
            }
            parsed.word_count
        } else {
            0
        };
        stats
            .checked_merge(&WebStats::for_file(&url, words))
            .unwrap();
    }
    // Root 15 words + about.dat (1 title + 3 paragraph + 2 anchors) = 21.
    // The cycle and duplicate fragments contribute once; the missing file never does.
    assert_eq!(
        stats,
        WebStats {
            num_files: 5,
            num_exts: 4,
            ext_counts: HashMap::from([
                ("html".into(), 2),
                ("dat".into(), 1),
                ("jpg".into(), 1),
                ("jpeg".into(), 1)
            ]),
            total_word_count: 21,
        }
    );
    assert_eq!(stats.ext_counts.values().sum::<usize>(), stats.num_files);
    assert_eq!(stats.ext_counts.len(), stats.num_exts);
    assert_eq!(stats.validate(), Ok(()));
}
