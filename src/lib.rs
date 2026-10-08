//! Testable crawl policies, bounded HTTP fetching, and atomic Redis job coordination.
//! Host-run nodes connect these contracts; user commands use only the Redis job API.

pub mod config;
pub mod diagnostics;
pub mod fetch;
pub mod html;
pub mod jobs;
pub mod node;
pub mod redis;
pub mod stats;
pub mod urls;

// Reuse the integration fixtures in gated white-box CPU/lifecycle tests.
#[cfg(test)]
extern crate self as swarmcrawl;
#[cfg(test)]
mod pipeline_tests;
