use std::{error::Error, process::ExitCode};

use clap::{Parser, Subcommand};
use swarmcrawl::{
    config::{ConfigError, DEFAULT_REDIS_TIMEOUT_SECS, DEFAULT_REDIS_URL, RedisConfig},
    redis::check_connection,
};

#[derive(Parser)]
#[command(
    name = "crawl",
    version,
    about = "Distributed crawler foundation (crawling is not implemented yet)"
)]
struct Cli {
    /// Redis TCP URL (prefer the environment variable for credentials)
    #[arg(
        long,
        global = true,
        env = "CRAWL_REDIS_URL",
        default_value = DEFAULT_REDIS_URL,
        hide_env_values = true
    )]
    redis_url: String,

    /// Total Redis check timeout in seconds (1-60)
    #[arg(
        long,
        global = true,
        env = "CRAWL_REDIS_TIMEOUT_SECS",
        default_value_t = DEFAULT_REDIS_TIMEOUT_SECS.to_string(),
        hide_env_values = true
    )]
    redis_timeout_secs: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check Redis connectivity with a read-only PING
    Check,
}

async fn run(cli: Cli) -> Result<(), Box<dyn Error>> {
    // Validate after Clap merges global flags and environment values. Parsing
    // globals earlier can reject an invalid env value even when a flag overrides it.
    let timeout_secs = cli
        .redis_timeout_secs
        .parse::<u64>()
        .map_err(|_| ConfigError::InvalidTimeout)?;
    let config = RedisConfig::new(&cli.redis_url, timeout_secs)?;
    match cli.command {
        Command::Check => {
            check_connection(&config).await?;
            println!("Redis connectivity: OK (PONG)");
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
