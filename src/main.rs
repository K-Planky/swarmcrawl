mod commands;

use std::{error::Error, process::ExitCode};

use clap::{Parser, Subcommand};
use swarmcrawl::{
    config::{
        ConfigError, DEFAULT_FETCH_TIMEOUT_SECS, DEFAULT_REDIS_TIMEOUT_SECS, DEFAULT_REDIS_URL,
        FetchConfig, RedisConfig,
    },
    fetch::Fetcher,
    jobs::{DEFAULT_JOB_NAMESPACE, JobStore},
    node::{run_node, shutdown_signal},
    redis::check_connection,
};

#[derive(Parser)]
#[command(
    name = "swarmcrawl",
    version,
    about = "Redis-coordinated distributed crawler"
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

    /// Redis connection/operation timeout in seconds (1-60)
    #[arg(
        long,
        global = true,
        env = "CRAWL_REDIS_TIMEOUT_SECS",
        default_value_t = DEFAULT_REDIS_TIMEOUT_SECS.to_string(),
        hide_env_values = true
    )]
    redis_timeout_secs: String,

    /// Shared job namespace (all nodes and job commands must use the same one)
    #[arg(
        long,
        global = true,
        env = "CRAWL_JOB_NAMESPACE",
        default_value = DEFAULT_JOB_NAMESPACE,
        hide_env_values = true
    )]
    namespace: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check Redis connectivity with a read-only PING
    Check,

    /// Run a crawler node; Ctrl-C/SIGTERM stops claims and drains owned work
    Node {
        /// Total HTTP request timeout in seconds (1-300)
        #[arg(
            long,
            env = "CRAWL_FETCH_TIMEOUT_SECS",
            default_value_t = DEFAULT_FETCH_TIMEOUT_SECS.to_string(),
            hide_env_values = true
        )]
        fetch_timeout_secs: String,
    },

    /// Submit one job per URL and return IDs without waiting for nodes
    #[command(
        long_about = "Submit one job per URL without waiting for nodes. All URLs are validated before any submission; an invalid input rejects the whole batch. Repeated canonical URLs return retained IDs. Output identifies inputs by position, never by potentially sensitive URL."
    )]
    Submit {
        /// Absolute HTTP(S) seed/base URLs (one job each; fragments ignored)
        #[arg(required = true, num_args = 1.., value_name = "URL")]
        urls: Vec<String>,
    },

    /// Read a coherent progress snapshot; -f polls until done or failed
    Status {
        /// Poll every 500 ms and print changes until terminal; Ctrl-C exits 130
        #[arg(short = 'f', long)]
        follow: bool,

        /// Namespace-local positive decimal job ID
        #[arg(value_name = "JOB")]
        job: String,
    },

    /// Print final WebStats with sorted extensions (only for done jobs)
    Stats {
        /// Namespace-local positive decimal job ID
        #[arg(value_name = "JOB")]
        job: String,
    },
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
        Command::Node { fetch_timeout_secs } => {
            let fetch_timeout_secs = fetch_timeout_secs
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidFetchTimeout)?;
            let fetcher = Fetcher::new(FetchConfig::new(fetch_timeout_secs)?)?;
            let shutdown = shutdown_signal()?;
            let store = JobStore::connect_in_namespace(&config, &cli.namespace).await?;
            run_node(store, fetcher, shutdown).await?;
        }
        Command::Submit { urls } => {
            let bases = commands::validate_urls(&urls)?;
            let store = JobStore::connect_in_namespace(&config, &cli.namespace).await?;
            commands::submit(&store, &bases).await?;
        }
        Command::Status { job, follow } => {
            let job = job.parse()?;
            let store = JobStore::connect_in_namespace(&config, &cli.namespace).await?;
            commands::status(&store, job, follow).await?;
        }
        Command::Stats { job } => {
            let job = job.parse()?;
            let store = JobStore::connect_in_namespace(&config, &cli.namespace).await?;
            commands::stats(&store, job).await?;
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
            if matches!(
                error.downcast_ref::<commands::CommandError>(),
                Some(commands::CommandError::Interrupted)
            ) {
                ExitCode::from(130)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
