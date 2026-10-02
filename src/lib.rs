//! Testable crawl policies, shared configuration, and Redis job storage/read contracts.
//! Worker ownership, completion publication and HTTP fetching are not implemented yet.

pub mod config;
pub mod html;
pub mod jobs;
pub mod redis;
pub mod stats;
pub mod urls;
