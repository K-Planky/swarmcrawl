//! Canonical HTTP(S) identity and the crawl's literal base-prefix safety boundary.

use std::{error::Error, fmt};

use url::Url;

/// A credential-free HTTP(S) URL with no fragment.
///
/// Equality/hashing use the URL parser's normalized serialization. Explicit access
/// to `as_str()` is for storage/requests, not diagnostics: queries may be secrets.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CrawlUrl(Url);

impl CrawlUrl {
    pub fn parse(input: &str) -> Result<Self, UrlError> {
        Self::from_url(Url::parse(input).map_err(|_| UrlError::InvalidUrl)?)
    }

    fn from_url(mut url: Url) -> Result<Self, UrlError> {
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(UrlError::UnsupportedUrl);
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(UrlError::CredentialsNotAllowed);
        }
        if url.port() == Some(0) {
            return Err(UrlError::InvalidUrl);
        }
        url.set_fragment(None);
        Ok(Self(url))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Resolve first, then check scope before retrieval. This also serves redirect
    /// resolution; resolving a URL is not permission to fetch it.
    pub fn resolve(&self, reference: &str) -> Result<Self, UrlError> {
        Self::from_url(self.0.join(reference).map_err(|_| UrlError::InvalidUrl)?)
    }

    /// Final serialized path suffix, without a dot. Queries are irrelevant.
    /// Simple dotfiles, empty suffixes and directory paths fall back to `html`.
    /// Percent escapes are deliberately not decoded into new path syntax.
    pub fn extension(&self) -> String {
        let filename = self.0.path().rsplit('/').next().unwrap_or_default();
        match filename.rsplit_once('.') {
            Some((stem, suffix)) if !stem.is_empty() && !suffix.is_empty() => suffix.to_lowercase(),
            _ => "html".into(),
        }
    }
}

impl fmt::Debug for CrawlUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CrawlUrl([redacted])")
    }
}

/// Same origin AND normalized serialized URL prefix, not a directory heuristic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrawlScope {
    base: CrawlUrl,
}

impl CrawlScope {
    pub fn new(base: CrawlUrl) -> Self {
        Self { base }
    }

    pub fn base(&self) -> &CrawlUrl {
        &self.base
    }

    pub fn contains(&self, candidate: &CrawlUrl) -> bool {
        candidate.0.origin() == self.base.0.origin()
            && candidate.as_str().starts_with(self.base.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlError {
    InvalidUrl,
    UnsupportedUrl,
    CredentialsNotAllowed,
}

impl fmt::Display for UrlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidUrl => "invalid URL; use an absolute HTTP(S) URL with a nonzero port",
            Self::UnsupportedUrl => "only HTTP(S) URLs with a host are supported",
            Self::CredentialsNotAllowed => "URL credentials are not supported",
        })
    }
}

impl Error for UrlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_normalizes_parser_equivalences_and_drops_fragments() {
        let a = CrawlUrl::parse("HTTPS://EXAMPLE.org:443/docs/./a/../page#intro").unwrap();
        let b = CrawlUrl::parse("https://example.org/docs/page#usage").unwrap();
        assert_eq!(a, b);
        assert_eq!(a.as_str(), "https://example.org/docs/page");
        assert_eq!(
            CrawlUrl::parse("http://EXAMPLE.org:80").unwrap().as_str(),
            "http://example.org/"
        );
        assert_eq!(
            CrawlUrl::parse("https://bücher.example/").unwrap().as_str(),
            "https://xn--bcher-kva.example/"
        );
    }

    #[test]
    fn identity_preserves_query_slash_case_and_encoded_path_distinctions() {
        for (a, b) in [
            ("docs", "docs/"),
            ("docs?a=1", "docs?a=2"),
            ("docs?a=1&b=2", "docs?b=2&a=1"),
            ("Docs", "docs"),
            ("docs/a%2Fb", "docs/a/b"),
            ("docs/%61", "docs/a"),
            ("docs?", "docs"),
        ] {
            assert_ne!(
                CrawlUrl::parse(&format!("https://example.org/{a}")).unwrap(),
                CrawlUrl::parse(&format!("https://example.org/{b}")).unwrap()
            );
        }
    }

    #[test]
    fn resolution_handles_relative_root_query_and_protocol_relative_references() {
        let page = CrawlUrl::parse("https://example.org/docs/sub/page?q=1").unwrap();
        for (reference, expected) in [
            ("../next#x", "https://example.org/docs/next"),
            ("/docs/root", "https://example.org/docs/root"),
            ("?q=2", "https://example.org/docs/sub/page?q=2"),
            ("#x", "https://example.org/docs/sub/page?q=1"),
            ("", "https://example.org/docs/sub/page?q=1"),
            ("//example.org:443/docs/a", "https://example.org/docs/a"),
        ] {
            assert_eq!(page.resolve(reference).unwrap().as_str(), expected);
        }
    }

    #[test]
    fn scope_is_literal_prefix_and_same_origin_not_whole_site() {
        let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/docs/").unwrap());
        for candidate in [
            "https://example.org/docs/",
            "https://example.org:443/docs/sub?q=1#x",
        ] {
            assert!(scope.contains(&CrawlUrl::parse(candidate).unwrap()));
        }
        for candidate in [
            "https://example.org/docs",
            "https://example.org/docs-extra/",
            "https://example.org/other/",
            "https://example.org/docs/../private",
            "https://example.org/docs/%2e%2e/private",
            "https://example.org/docs\\..\\private",
            "http://example.org/docs/",
            "https://example.org:444/docs/",
            "https://example.org.evil.invalid/docs/",
            "https://other.example/docs/",
        ] {
            assert!(!scope.contains(&CrawlUrl::parse(candidate).unwrap()));
        }
    }

    #[test]
    fn unusual_bases_keep_the_literal_query_and_non_directory_prefix() {
        let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/docs").unwrap());
        assert!(scope.contains(&CrawlUrl::parse("https://example.org/docs-extra").unwrap()));
        let scope = CrawlScope::new(CrawlUrl::parse("https://example.org/docs/?q=1#x").unwrap());
        assert!(scope.contains(&CrawlUrl::parse("https://example.org/docs/?q=1&b=2").unwrap()));
        assert!(scope.contains(&CrawlUrl::parse("https://example.org/docs/?q=10").unwrap()));
        assert!(!scope.contains(&CrawlUrl::parse("https://example.org/docs/child?q=1").unwrap()));
        assert!(!scope.contains(&CrawlUrl::parse("https://example.org/docs/?q=2").unwrap()));
    }

    #[test]
    fn rejects_invalid_unsupported_and_credentialed_urls_without_echoing_secrets() {
        for input in [
            "",
            "docs/",
            "http://",
            "https://example.org:0/",
            "ftp://example.org/docs/",
            "file:///docs/",
            "mailto:a@example.org",
            "javascript:alert(1)",
            "data:text/html,hello",
            "https://user:fixture-secret@example.org/?token=fixture-query",
        ] {
            let error = CrawlUrl::parse(input).unwrap_err();
            assert!(!error.to_string().contains("fixture-secret"));
            assert!(!error.to_string().contains("fixture-query"));
        }
        let page = CrawlUrl::parse("https://example.org/?token=fixture-query").unwrap();
        assert!(page.resolve("javascript:alert(1)").is_err());
        assert!(page.resolve("//user:fixture-secret@example.org/").is_err());
        assert!(!format!("{page:?}").contains("fixture-query"));
        assert!(!format!("{:?}", CrawlScope::new(page)).contains("fixture-query"));
    }

    #[test]
    fn extension_is_lowercase_path_only_with_explicit_dotfile_and_escape_policy() {
        for (path, expected) in [
            ("a.JPG", "jpg"),
            ("a.JPEG?name=x.zip", "jpeg"),
            ("archive.tar.GZ", "gz"),
            ("api/", "html"),
            ("intro", "html"),
            (".env", "html"),
            (".config.JSON", "json"),
            ("file.", "html"),
            ("dir.jpg/", "html"),
            ("file%2Ejpg", "html"),
            ("file.%4A%50%47", "%4a%50%47"),
            ("dir.JPG/file.PDF#x", "pdf"),
        ] {
            let url = CrawlUrl::parse(&format!("https://example.org/docs/{path}")).unwrap();
            assert_eq!(url.extension(), expected, "path {path}");
        }
    }
}
