//! Required file/word totals with checked, all-or-nothing aggregation.

use std::{collections::HashMap, error::Error, fmt};

use crate::urls::CrawlUrl;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebStats {
    pub num_files: usize,
    pub num_exts: usize,
    pub ext_counts: HashMap<String, usize>,
    pub total_word_count: u64,
}

impl WebStats {
    /// One existing file. The owner must deduplicate URLs before contributing;
    /// broken/redirect responses contribute nothing. Non-HTML files pass zero words.
    pub fn for_file(url: &CrawlUrl, html_word_count: u64) -> Self {
        Self {
            num_files: 1,
            num_exts: 1,
            ext_counts: HashMap::from([(url.extension(), 1)]),
            total_word_count: html_word_count,
        }
    }

    /// Reconstruct redundant totals from counts, checking the aggregate invariant.
    pub fn from_counts(
        ext_counts: HashMap<String, usize>,
        total_word_count: u64,
    ) -> Result<Self, StatsError> {
        let num_files = count_files(&ext_counts)?;
        if num_files == 0 && total_word_count != 0 {
            return Err(StatsError::InconsistentTotals);
        }
        Ok(Self {
            num_files,
            num_exts: ext_counts.len(),
            ext_counts,
            total_word_count,
        })
    }

    /// Public fields match the assignment; validate them at storage/read boundaries.
    pub fn validate(&self) -> Result<(), StatsError> {
        if count_files(&self.ext_counts)? != self.num_files
            || self.ext_counts.len() != self.num_exts
            || (self.num_files == 0 && self.total_word_count != 0)
        {
            return Err(StatsError::InconsistentTotals);
        }
        Ok(())
    }

    /// Neither overflow nor malformed input may leave partially updated totals.
    pub fn checked_merge(&mut self, other: &Self) -> Result<(), StatsError> {
        self.validate()?;
        other.validate()?;
        let mut next = self.clone();
        next.num_files = next
            .num_files
            .checked_add(other.num_files)
            .ok_or(StatsError::Overflow)?;
        next.total_word_count = next
            .total_word_count
            .checked_add(other.total_word_count)
            .ok_or(StatsError::Overflow)?;
        for (extension, count) in &other.ext_counts {
            let entry = next.ext_counts.entry(extension.clone()).or_default();
            *entry = entry.checked_add(*count).ok_or(StatsError::Overflow)?;
        }
        next.num_exts = next.ext_counts.len();
        *self = next;
        Ok(())
    }
}

fn count_files(counts: &HashMap<String, usize>) -> Result<usize, StatsError> {
    counts.iter().try_fold(0usize, |total, (extension, count)| {
        if extension.is_empty() || extension != &extension.to_lowercase() {
            return Err(StatsError::InvalidExtension);
        }
        if *count == 0 {
            return Err(StatsError::ZeroExtensionCount);
        }
        total.checked_add(*count).ok_or(StatsError::Overflow)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsError {
    InvalidExtension,
    ZeroExtensionCount,
    InconsistentTotals,
    Overflow,
}

impl fmt::Display for StatsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidExtension => "statistics require nonempty lowercase extensions",
            Self::ZeroExtensionCount => "statistics contain an extension with no files",
            Self::InconsistentTotals => {
                "statistics file, extension or word totals are inconsistent"
            }
            Self::Overflow => "statistics exceed the supported integer range",
        })
    }
}

impl Error for StatsError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_merged_statistics_keep_both_invariants() {
        let mut stats = WebStats::default();
        assert_eq!(stats.validate(), Ok(()));
        for (path, words) in [
            ("/intro", 5),
            ("/other.HTML", 3),
            ("/a.JPG", 0),
            ("/a.JPEG", 0),
        ] {
            let url = CrawlUrl::parse(&format!("https://example.org{path}")).unwrap();
            stats
                .checked_merge(&WebStats::for_file(&url, words))
                .unwrap();
        }
        assert_eq!(
            stats,
            WebStats {
                num_files: 4,
                num_exts: 3,
                ext_counts: HashMap::from([
                    ("html".into(), 2),
                    ("jpg".into(), 1),
                    ("jpeg".into(), 1)
                ]),
                total_word_count: 8,
            }
        );
        assert_eq!(stats.validate(), Ok(()));
    }

    #[test]
    fn rejects_invalid_counts_and_redundant_totals() {
        for (extension, count, expected) in [
            ("", 1, StatsError::InvalidExtension),
            ("JPG", 1, StatsError::InvalidExtension),
            ("jpg", 0, StatsError::ZeroExtensionCount),
        ] {
            assert_eq!(
                WebStats::from_counts(HashMap::from([(extension.into(), count)]), 0),
                Err(expected)
            );
        }
        assert_eq!(
            WebStats::from_counts(HashMap::new(), 1),
            Err(StatsError::InconsistentTotals)
        );
        let words_without_files = WebStats {
            total_word_count: 1,
            ..WebStats::default()
        };
        assert_eq!(
            words_without_files.validate(),
            Err(StatsError::InconsistentTotals)
        );
        let mut stats = WebStats::from_counts(HashMap::from([("html".into(), 2)]), 9).unwrap();
        stats.num_files = 1;
        assert_eq!(stats.validate(), Err(StatsError::InconsistentTotals));
        stats.num_files = 2;
        stats.num_exts = 2;
        assert_eq!(stats.validate(), Err(StatsError::InconsistentTotals));
    }

    #[test]
    fn word_totals_above_float_precision_remain_exact_in_the_domain() {
        let words = 9_007_199_254_740_993;
        let mut stats = WebStats::from_counts(HashMap::from([("html".into(), 1)]), words).unwrap();
        let more = WebStats::from_counts(HashMap::from([("html".into(), 1)]), 2).unwrap();
        stats.checked_merge(&more).unwrap();
        assert_eq!(stats.total_word_count, 9_007_199_254_740_995);
        assert_eq!(stats.validate(), Ok(()));
    }

    #[test]
    fn aggregate_file_counts_cannot_wrap() {
        assert_eq!(
            WebStats::from_counts(
                HashMap::from([("html".into(), usize::MAX), ("jpg".into(), 1)]),
                0
            ),
            Err(StatsError::Overflow)
        );
        let mut stats =
            WebStats::from_counts(HashMap::from([("html".into(), usize::MAX)]), 0).unwrap();
        let before = stats.clone();
        let more = WebStats::from_counts(HashMap::from([("html".into(), 1)]), 0).unwrap();
        assert_eq!(stats.checked_merge(&more), Err(StatsError::Overflow));
        assert_eq!(stats, before);
    }

    #[test]
    fn failed_word_overflow_or_invalid_merge_is_all_or_nothing() {
        let mut stats =
            WebStats::from_counts(HashMap::from([("html".into(), 1)]), u64::MAX).unwrap();
        let before = stats.clone();
        let more = WebStats::from_counts(HashMap::from([("jpg".into(), 1)]), 1).unwrap();
        assert_eq!(stats.checked_merge(&more), Err(StatsError::Overflow));
        assert_eq!(stats, before);
        let invalid = WebStats {
            num_files: 1,
            ..WebStats::default()
        };
        assert_eq!(
            stats.checked_merge(&invalid),
            Err(StatsError::InconsistentTotals)
        );
        assert_eq!(stats, before);
    }
}
