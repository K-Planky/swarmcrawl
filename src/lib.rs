//! Testable crawl policies, bounded HTTP fetching, and atomic Redis job coordination.
//! Host-run nodes connect these contracts; user job commands are separate CLI work.

pub mod config;
pub mod fetch;
pub mod html;
pub mod jobs;
pub mod node;
pub mod redis;
pub mod stats;
pub mod urls;
