//! Synchronous full-document extraction. Only owned, Send data leaves this module.

use std::{collections::HashSet, error::Error, fmt};

use html5ever::{driver::ParseOpts, tendril::TendrilSink, tree_builder::TreeBuilderOpts};
use scraper::{Html, HtmlTreeSink, node::Element};

use crate::urls::{CrawlScope, CrawlUrl};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedHtml {
    /// Locally deduplicated in-scope links; cluster-wide ownership belongs to Redis.
    pub links: Vec<CrawlUrl>,
    pub word_count: u64,
}

/// MIME parameters/case do not change classification; suffixes never participate.
pub fn is_html_content_type(content_type: &str) -> bool {
    let mime = content_type.split(';').next().unwrap_or_default().trim();
    mime.eq_ignore_ascii_case("text/html") || mime.eq_ignore_ascii_case("application/xhtml+xml")
}

/// `source` is decoded HTML, not response bytes. Decode/status policy belongs to
/// the fetcher. HTML5 recovery accepts malformed markup without truncating it.
pub fn parse_html(
    scope: &CrawlScope,
    page: &CrawlUrl,
    source: &str,
) -> Result<ParsedHtml, HtmlError> {
    if !scope.contains(page) {
        return Err(HtmlError::PageOutsideScope);
    }
    // Disable scripting in the parser too, so noscript hyperlinks remain markup.
    let options = ParseOpts {
        tree_builder: TreeBuilderOpts {
            scripting_enabled: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let document =
        html5ever::parse_document(HtmlTreeSink::new(Html::new_document()), options).one(source);
    // An out-of-scope base may resolve references, but never expands crawl scope.
    // Only the first non-template base[href] applies, even if it is invalid.
    let resolution_base = document
        .tree
        .root()
        .descendants()
        .find_map(|node| {
            let element = node.value().as_element()?;
            if element.name() != "base"
                || node.ancestors().any(|ancestor| {
                    ancestor
                        .value()
                        .as_element()
                        .is_some_and(|element| element.name() == "template")
                })
            {
                return None;
            }
            element.attr("href")
        })
        .and_then(|reference| page.resolve(reference).ok())
        .unwrap_or_else(|| page.clone());

    let mut result = ParsedHtml {
        links: Vec::new(),
        word_count: 0,
    };
    let mut seen = HashSet::new();
    for node in document.tree.root().descendants() {
        if let Some(element) = node.value().as_element() {
            for reference in resource_references(element) {
                if let Ok(url) = resolution_base.resolve(reference)
                    && scope.contains(&url)
                    && seen.insert(url.clone())
                {
                    result.links.push(url);
                }
            }
        }
        if let Some(text) = node.value().as_text() {
            let excluded = node.ancestors().any(|ancestor| {
                ancestor.value().as_element().is_some_and(|element| {
                    matches!(element.name(), "script" | "style" | "template")
                })
            });
            if !excluded {
                // Text-node boundaries act as whitespace. Comments/attributes are
                // not text; entity decoding has already happened in the parser.
                for token in text.to_lowercase().split_whitespace() {
                    if token.as_bytes().first().is_some_and(u8::is_ascii_lowercase) {
                        result.word_count = result
                            .word_count
                            .checked_add(1)
                            .ok_or(HtmlError::WordCountOverflow)?;
                    }
                }
            }
        }
    }
    Ok(result)
}

fn resource_references(element: &Element) -> Vec<&str> {
    let attributes: &[&str] = match element.name() {
        "a" | "area" | "link" | "image" | "use" => &["href"],
        "img" | "source" => &["src", "srcset"],
        "script" | "iframe" | "frame" | "audio" | "track" | "embed" | "input" => &["src"],
        "video" => &["src", "poster"],
        "object" => &["data"],
        _ => &[],
    };
    let mut references = Vec::new();
    for &attribute in attributes {
        let value = element.attr(attribute).or_else(|| {
            // SVG's legacy xlink:href is namespace-qualified, not a literal
            // "xlink:href" in scraper's attribute lookup. Modern href wins.
            if attribute != "href" {
                return None;
            }
            element
                .attrs
                .iter()
                .find(|(name, _)| {
                    name.local.as_ref() == "href"
                        && name.ns.as_ref() == "http://www.w3.org/1999/xlink"
                })
                .map(|(_, value)| value.as_ref())
        });
        if let Some(value) = value {
            if attribute == "srcset" {
                references.extend(srcset_urls(value));
            } else {
                references.push(value);
            }
        }
    }
    references
}

/// Inventory every srcset candidate (descriptors do not restrict static crawling).
/// URLs end at HTML whitespace, not interior commas: a data URL contains commas.
/// Descriptor parentheses may also contain commas. Invalid/unsupported URLs are
/// subsequently rejected by URL resolution, just like other link attributes.
fn srcset_urls(mut input: &str) -> Vec<&str> {
    let mut urls = Vec::new();
    while !input.is_empty() {
        input = input.trim_start_matches(|c| html_space(c) || c == ',');
        if input.is_empty() {
            break;
        }
        let end = input.find(html_space).unwrap_or(input.len());
        let url = &input[..end];
        input = &input[end..];
        urls.push(url.trim_end_matches(','));
        if url.ends_with(',') {
            continue;
        }
        let mut parentheses = 0usize;
        let mut end = input.len();
        for (index, c) in input.char_indices() {
            match c {
                '(' => parentheses += 1,
                ')' => parentheses = parentheses.saturating_sub(1),
                ',' if parentheses == 0 => {
                    end = index + 1;
                    break;
                }
                _ => {}
            }
        }
        input = &input[end..];
    }
    urls
}

fn html_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{000c}')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HtmlError {
    PageOutsideScope,
    WordCountOverflow,
}

impl fmt::Display for HtmlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PageOutsideScope => "cannot parse a crawl page outside its job scope",
            Self::WordCountOverflow => "HTML word count exceeds the supported integer range",
        })
    }
}

impl Error for HtmlError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(source: &str) -> ParsedHtml {
        let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/docs/").unwrap());
        parse_html(&scope, scope.base(), source).unwrap()
    }

    fn paths(parsed: ParsedHtml) -> Vec<String> {
        parsed
            .links
            .iter()
            .map(|url| {
                url.as_str()
                    .strip_prefix("https://example.org/docs/")
                    .unwrap()
                    .into()
            })
            .collect()
    }

    #[test]
    fn mime_is_case_insensitive_with_parameters_and_not_inferred_from_suffix() {
        for mime in [
            "text/html",
            "TEXT/HTML; charset=UTF-8",
            " application/xhtml+xml ; charset=utf-8 ",
        ] {
            assert!(is_html_content_type(mime));
        }
        for mime in [
            "",
            "text/plain",
            "image/jpeg",
            "application/xml",
            "application/html",
            "text/htmlish",
            "text/html, text/plain",
        ] {
            assert!(!is_html_content_type(mime));
        }
    }

    #[test]
    fn words_lowercase_then_count_whitespace_tokens_with_ascii_leading_letters() {
        let parsed = parse(
            r#"<title>Ant ant</title><p data-words="not counted">Bee 42cats Élan ZEBRA</p>
            <!-- many comment words --> <img alt="many attribute words">
            <script>many script words</script><style>many style words</style>
            <template>many template words</template><noscript>Fallback text</noscript>
            <p>cat,dog can't foo-bar 'quoted' _under 123abc Kelvin</p>
            <p>split<b>joined</b>again</p><p>one&nbsp;two &amp; three</p>"#,
        );
        // 2 title + 2 first p + 2 noscript + 4 punctuation/Unicode lowercase
        // + 3 separate text nodes + 3 entity-separated tokens = 16.
        assert_eq!(parsed.word_count, 16);
    }

    #[test]
    fn full_document_links_resolve_filter_and_deduplicate_fragments() {
        let parsed = parse(
            r#"<a href="child#one">one</a><a href="./child#two">two</a>
            <a href="?q=1">query</a><area href="/docs/map">
            <a href="/outside">outside</a><a href="../escape">escape</a>
            <a href="https://other.example/docs/child">other host</a>
            <a href="http://example.org/docs/child">other scheme</a>
            <a href="javascript:alert(1)">script</a><a href="mailto:a@example.org">mail</a>
            <a href="https://user:fixture-secret@example.org/docs/auth">credentials</a>
            <a href="//EXAMPLE.org:443/docs/end">end</a>"#,
        );
        assert_eq!(paths(parsed), ["child", "?q=1", "map", "end"]);
    }

    #[test]
    fn first_base_resolves_all_links_and_invalid_first_base_does_not_choose_second() {
        assert_eq!(
            paths(parse(
                r#"<a href="early"></a><base href="sub/"><base href="ignored/">
            <a href="late"></a><a href="../root"></a>"#
            )),
            ["sub/early", "sub/late", "root"]
        );
        assert_eq!(
            paths(parse(
                r#"<base href="javascript:bad"><base href="sub/"><a href="child"></a>"#
            )),
            ["child"]
        );
        assert_eq!(
            paths(parse(
                r#"<template><base href="ignored/"></template><base href="sub/"><a href="child"></a>"#
            )),
            ["sub/child"]
        );
    }

    #[test]
    fn external_html_base_never_expands_scope() {
        assert_eq!(
            paths(parse(
                r#"<base href="https://other.example/">
            <a href="relative"></a><a href="/docs/root"></a>
            <a href="https://example.org/docs/allowed"></a>"#
            )),
            ["allowed"]
        );
        assert_eq!(
            paths(parse(
                r#"<base href="/outside/"><a href="relative"></a><a href="/docs/allowed"></a>"#
            )),
            ["allowed"]
        );
    }

    #[test]
    fn inventories_hyperlinks_and_static_resources_even_in_noscript_and_templates() {
        let source = r#"<link href="style.CSS"><img src="image.JPG" srcset="small.png 1x, large.png 2x">
            <picture><source src="source.webp" srcset="wide.webp 600w"></picture>
            <script src="app.js"></script><iframe src="frame.html"></iframe>
            <audio src="audio.ogg"></audio><video src="video.mp4" poster="poster.png"></video>
            <track src="captions.vtt"><embed src="embed.pdf"><object data="object.pdf"></object>
            <input type="image" src="button.png"><noscript><a href="fallback.html">Fallback</a></noscript>
            <template><a href="template.html">Inert text</a></template>
            <svg><a href="svg.html"><text>SVG</text></a><image href="svg.png"/>
            <use xlink:href="icons.svg#one"/><image href="modern.png" xlink:href="legacy.png"/></svg>"#;
        assert_eq!(
            paths(parse(source)),
            [
                "style.CSS",
                "image.JPG",
                "small.png",
                "large.png",
                "source.webp",
                "wide.webp",
                "app.js",
                "frame.html",
                "audio.ogg",
                "video.mp4",
                "poster.png",
                "captions.vtt",
                "embed.pdf",
                "object.pdf",
                "button.png",
                "fallback.html",
                "template.html",
                "svg.html",
                "svg.png",
                "icons.svg",
                "modern.png",
            ]
        );
        let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/docs/").unwrap());
        let frame_page = scope.base().resolve("legacy-frame.html").unwrap();
        assert_eq!(
            parse_html(
                &scope,
                &frame_page,
                "<frameset><frame src='frame.html'></frameset>"
            )
            .unwrap()
            .links,
            vec![scope.base().resolve("frame.html").unwrap()]
        );
    }

    #[test]
    fn srcset_handles_data_urls_trailing_commas_and_descriptor_parentheses() {
        assert_eq!(
            srcset_urls("a.png, b.png 2x, c.png 300w, d.png"),
            ["a.png", "b.png", "c.png", "d.png"]
        );
        assert_eq!(
            srcset_urls("data:image/png;base64,AAAA 1x, good.png 2x"),
            ["data:image/png;base64,AAAA", "good.png"]
        );
        assert_eq!(srcset_urls("a.png bad(a,b), b.png 2x"), ["a.png", "b.png"]);
        assert_eq!(
            paths(parse(
                r#"<img srcset="data:image/png;base64,AAAA 1x, good.png 2x, /outside.png 3x">"#
            )),
            ["good.png"]
        );
        assert!(srcset_urls(" , \n,,").is_empty());
    }

    #[test]
    fn malformed_markup_recovers_and_the_end_of_the_document_is_not_ignored() {
        let source = format!(
            "<p>First<b>Second</p><p>{}<a href='last#x'>Last",
            "word ".repeat(10_000)
        );
        let parsed = parse(&source);
        assert_eq!(parsed.word_count, 10_003);
        assert_eq!(paths(parsed), ["last"]);
    }

    #[test]
    fn rejects_outside_page_and_returns_only_send_owned_data() {
        fn assert_send<T: Send>() {}
        assert_send::<ParsedHtml>();
        let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/docs/").unwrap());
        let outside = CrawlUrl::parse("https://example.org/outside").unwrap();
        assert_eq!(
            parse_html(&scope, &outside, "<a href='/docs/child'>text</a>"),
            Err(HtmlError::PageOutsideScope)
        );
    }
}
