mod cluster;
mod commands;
mod saved;

use std::{error::Error, process::ExitCode};

use clap::{Parser, Subcommand};
use swarmcrawl::{
    config::{
        ConfigError, DEFAULT_FETCH_TIMEOUT_SECS, DEFAULT_REDIS_TIMEOUT_SECS, DEFAULT_REDIS_URL,
        FetchConfig, RedisConfig,
    },
    diagnostics::Diagnostics,
    fetch::Fetcher,
    jobs::{DEFAULT_JOB_NAMESPACE, JobStore},
    node::{run_node, shutdown_signal},
    redis::check_connection,
};

#[derive(Parser)]
#[command(
    name = "swarmcrawl",
    version,
    about = "Redis-coordinated distributed crawler",
    after_help = "Connection precedence: flags > environment > saved configuration > defaults.\nSaved settings live in the private per-user configuration directory; SWARMCRAWL_CONFIG_DIR overrides its location.\nFirst PC: cluster init IP, then node. Other PCs: cluster join HOST, then node. See cluster --help and README.md."
)]
struct Cli {
    /// Redis TCP URL (prefer saved configuration or environment for credentials)
    #[arg(
        long,
        global = true,
        env = "SWARMCRAWL_REDIS_URL",
        hide_env_values = true
    )]
    redis_url: Option<String>,

    /// Redis connection/operation timeout in seconds (1-60)
    #[arg(
        long,
        global = true,
        env = "SWARMCRAWL_REDIS_TIMEOUT_SECS",
        default_value_t = DEFAULT_REDIS_TIMEOUT_SECS.to_string(),
        hide_env_values = true
    )]
    redis_timeout_secs: String,

    /// Shared job namespace (all nodes and job commands must use the same one)
    #[arg(
        long,
        global = true,
        env = "SWARMCRAWL_JOB_NAMESPACE",
        hide_env_values = true
    )]
    namespace: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Set up or join Redis; manage only this CLI's owned disposable deployment
    #[command(
        long_about = "Set up authenticated Docker Redis on this PC with cluster init IP and your own password, or join an existing deployment without Docker. Redis TCP is plaintext: use only a trusted LAN/private VPN and restrict the firewall yourself.\n\nInit/join never overwrite saved settings. Both prompt without echo or accept one password line with --password-stdin; interactive init asks for confirmation. cluster info shows saved connection details, never the password.\n\nStop ALL nodes gracefully before cluster stop or remove. Stop destroys all Redis jobs/results but retains settings for start. Remove additionally deletes the saved connection/credential without backup, allowing a fresh init. On a joining PC, remove only forgets its local connection; remote Redis is untouched.\n\nCluster commands use saved metadata, not global endpoint overrides. Unset SWARMCRAWL_REDIS_URL; --redis-url is not accepted here. Init/join select the namespace; info/start/stop/remove use the saved namespace."
    )]
    Cluster {
        #[command(subcommand)]
        command: cluster::ClusterCommand,
    },

    /// Check Redis connectivity with a read-only PING
    Check,

    /// Run a crawler node; Ctrl-C/SIGTERM stops claims and drains owned work
    Node {
        /// Total HTTP request timeout in seconds (1-300)
        #[arg(
            long,
            env = "SWARMCRAWL_FETCH_TIMEOUT_SECS",
            default_value_t = DEFAULT_FETCH_TIMEOUT_SECS.to_string(),
            hide_env_values = true
        )]
        fetch_timeout_secs: String,

        /// Emit aggregate node timings/utilization on graceful exit (no URLs)
        #[arg(long, env = "SWARMCRAWL_DIAGNOSTICS", hide_env_values = true)]
        diagnostics: bool,
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

    /// List all retained jobs (running, done, failed and aborted), ordered by ID
    #[command(
        long_about = "List every retained job in the selected Redis database/namespace, ordered by numeric ID. Each progress row is coherent; rows are not one global snapshot. Does not print URLs or credentials. Terminal jobs, including failed/aborted jobs, do not make listing fail."
    )]
    Jobs,

    /// Permanently abort one job, or all currently active jobs in this namespace
    #[command(
        long_about = "Permanently abort a running job, or use --all for the active-job set captured at command start in this database/namespace. Stops new claims and rejects late publication. Already-owned work drains, including queued HTTP and HTML processing; HTTP deadlines still apply and nodes stay available. Partial counters are retained for diagnosis, never exposed as final stats. No resume or restart by resubmission. Completed/failed/aborted jobs are unchanged. Each abort is atomic; --all is sequential, not a batch transaction, and does not include later submissions. The explicit command is confirmation; there is no extra prompt."
    )]
    Abort {
        /// Namespace-local positive decimal job ID (mutually exclusive with --all)
        #[arg(
            value_name = "JOB",
            required_unless_present = "all",
            conflicts_with = "all"
        )]
        job: Option<String>,
        /// Abort the captured active-job set; completed results remain intact
        #[arg(long)]
        all: bool,
    },

    /// Read a coherent progress snapshot; -f polls until done, failed or aborted
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
    // Lifecycle uses saved ownership, never a transient endpoint override.
    RedisConfig::new(DEFAULT_REDIS_URL, timeout_secs)?;
    if let Command::Cluster { command } = cli.command {
        if cli.redis_url.is_some() {
            return Err("cluster commands use IP/HOST or saved ownership, not --redis-url; unset SWARMCRAWL_REDIS_URL first".into());
        }
        return cluster::run(
            command,
            cli.namespace
                .unwrap_or_else(|| DEFAULT_JOB_NAMESPACE.into()),
            timeout_secs,
        )
        .await;
    }
    let saved = saved::Storage::locate()?.load()?;
    let redis_url = cli
        .redis_url
        .as_deref()
        .or_else(|| saved.as_ref().map(|saved| saved.redis_url.as_str()))
        .unwrap_or(DEFAULT_REDIS_URL);
    let namespace = cli
        .namespace
        .as_deref()
        .or_else(|| saved.as_ref().map(|saved| saved.namespace.as_str()))
        .unwrap_or(DEFAULT_JOB_NAMESPACE);
    let config = RedisConfig::new(redis_url, timeout_secs)?;
    match cli.command {
        Command::Cluster { .. } => unreachable!("cluster commands dispatched above"),
        Command::Check => {
            check_connection(&config).await?;
            println!("Redis connectivity: OK (PONG)");
        }
        Command::Node {
            fetch_timeout_secs,
            diagnostics,
        } => {
            let fetch_timeout_secs = fetch_timeout_secs
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidFetchTimeout)?;
            let fetcher = Fetcher::new(FetchConfig::new(fetch_timeout_secs)?)?.with_diagnostics(
                if diagnostics {
                    Diagnostics::enabled()
                } else {
                    Diagnostics::default()
                },
            );
            let shutdown = shutdown_signal()?;
            let store = JobStore::connect_in_namespace(&config, namespace).await?;
            run_node(store, fetcher, shutdown).await?;
        }
        Command::Submit { urls } => {
            let bases = commands::validate_urls(&urls)?;
            let store = JobStore::connect_in_namespace(&config, namespace).await?;
            commands::submit(&store, &bases).await?;
        }
        Command::Jobs => {
            let store = JobStore::connect_in_namespace(&config, namespace).await?;
            commands::jobs(&store).await?;
        }
        Command::Abort { job, all: _ } => {
            let job = job.map(|value| value.parse()).transpose()?;
            let store = JobStore::connect_in_namespace(&config, namespace).await?;
            commands::abort(&store, job).await?;
        }
        Command::Status { job, follow } => {
            let job = job.parse()?;
            let store = JobStore::connect_in_namespace(&config, namespace).await?;
            commands::status(&store, job, follow).await?;
        }
        Command::Stats { job } => {
            let job = job.parse()?;
            let store = JobStore::connect_in_namespace(&config, namespace).await?;
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
