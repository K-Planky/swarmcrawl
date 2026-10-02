//! Testable crawl policies, bounded HTTP fetching, and atomic Redis job coordination.
//! Application node orchestration and the crawl CLI remain separate integration work.

pub mod config;
pub mod fetch;
pub mod html;
pub mod jobs;
pub mod redis;
pub mod stats;
pub mod urls;
