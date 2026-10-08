//! Local benchmark only: explicit certificate trust, otherwise the real node path.
use swarmcrawl::{
    config::{FetchConfig, RedisConfig},
    diagnostics::Diagnostics,
    fetch::Fetcher,
    jobs::JobStore,
    node::{run_node, shutdown_signal},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let certificate = std::fs::read(
        std::env::args_os()
            .nth(1)
            .ok_or("certificate path required")?,
    )?;
    let fetcher = Fetcher::with_root_certificate(
        FetchConfig::new(30)?,
        reqwest::Certificate::from_pem(&certificate)?,
    )?
    .with_diagnostics(Diagnostics::enabled());
    let config = RedisConfig::new(&std::env::var("SWARMCRAWL_REDIS_URL")?, 5)?;
    let store =
        JobStore::connect_in_namespace(&config, &std::env::var("SWARMCRAWL_JOB_NAMESPACE")?)
            .await?;
    run_node(store, fetcher, shutdown_signal()?).await?;
    Ok(())
}
