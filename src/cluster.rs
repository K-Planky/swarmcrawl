//! Local Docker provisioning only; no node registration or remote control plane.
use std::{
    io::{self, BufRead, IsTerminal, Read, Write},
    net::{IpAddr, Ipv4Addr},
    process::Stdio,
    time::Duration,
};

use clap::Subcommand;
use swarmcrawl::{
    config::{DEFAULT_REDIS_URL, RedisConfig},
    jobs::{DEFAULT_JOB_NAMESPACE, validate_namespace},
    redis::check_connection,
};
use tokio::process::Command;
use url::Url;

use crate::saved::{Owned, Result, Saved, Storage, private_metadata};

const IMAGE: &str = "redis:7.4-alpine";
const OWNER_LABEL: &str = "org.swarmcrawl.owner";
const MANAGED_LABEL: &str = "org.swarmcrawl.managed";

#[derive(Subcommand)]
pub enum ClusterCommand {
    /// Create authenticated disposable Redis and save this PC's connection
    Init {
        /// Required host IP to publish; use 127.0.0.1 explicitly for this PC only
        #[arg(value_name = "IP")]
        ip: IpAddr,
        /// Read your password as one stdin line instead of hidden prompts
        #[arg(long)]
        password_stdin: bool,
        /// Host TCP port (1-65535)
        #[arg(long, default_value_t = 6379, value_parser = clap::value_parser!(u16).range(1..))]
        port: u16,
        /// Redis database shared by every client (0-15)
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=15))]
        database: u8,
    },
    /// Save an existing Redis connection after authenticated PING; no Docker needed
    Join {
        /// Redis host IP or DNS name, not a URL
        host: String,
        #[arg(long, default_value_t = 6379, value_parser = clap::value_parser!(u16).range(1..))]
        port: u16,
        /// Must match the owner's database (0-15)
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=15))]
        database: u8,
        /// Optional Redis ACL username (default user if omitted)
        #[arg(long)]
        username: Option<String>,
        /// Read one password line from stdin instead of a hidden terminal prompt
        #[arg(long)]
        password_stdin: bool,
    },
    /// Start only this CLI's owned Redis; already-running Redis is not restarted
    Start,
    /// Stop/remove owned Redis, permanently losing ALL jobs/results in every database
    Stop {
        /// Confirm data loss and that you have gracefully stopped all nodes
        #[arg(long)]
        yes: bool,
    },
    /// Show saved address, port, database and namespace; never display credentials
    Info,
    /// Remove owned Redis and saved connection files, without backup; joined PCs only forget locally
    Remove {
        /// Confirm permanent data/credential removal and that all nodes are stopped
        #[arg(long)]
        yes: bool,
    },
}

pub async fn run(command: ClusterCommand, namespace: String, timeout: u64) -> Result<()> {
    let storage = Storage::locate()?;
    let _lock = storage.lock()?;
    let existing = storage.load()?;
    if let Some(saved) = &existing {
        saved.validate()?;
    }
    match command {
        ClusterCommand::Init {
            ip: bind,
            password_stdin,
            port,
            database,
        } => {
            refuse_existing(&existing)?;
            validate_namespace(&namespace)?;
            validate_bind(bind)?;
            docker_available().await?;
            let password = read_password(password_stdin, true)?;
            let saved = Saved {
                version: 1,
                redis_url: endpoint(&bind.to_string(), port, database, None, &password)?,
                namespace,
                owned: Some(Owned {
                    token: random_hex(16)?,
                    bind,
                    port,
                }),
            };
            // Save ownership before invoking Docker. An interrupted create/start is
            // recoverable with `cluster start`; never delete state on ambiguous failure.
            storage.save_new(&saved)?;
            if let Err(error) = start(&storage, &saved, timeout).await {
                eprintln!(
                    "Setup incomplete; private configuration retained. Fix the cause, then run swarmcrawl cluster start. No existing deployment was replaced."
                );
                return Err(error);
            }
            let address = display_host(bind);
            println!(
                "Authenticated Redis ready at {address}:{port}; database {database}, namespace {}.",
                saved.namespace
            );
            if bind.is_loopback() {
                println!("This PC only: other PCs cannot use this loopback endpoint.");
            } else {
                println!(
                    "On the other PC: swarmcrawl cluster join {bind} --port {port} --database {database} --namespace {}",
                    saved.namespace
                );
                println!(
                    "Share your chosen password through a secure channel. cluster info shows connection details, never the password."
                );
            }
            println!(
                "Connection saved. Run swarmcrawl node. Redis TCP is plaintext: use only a trusted LAN/private VPN and restrict the firewall to trusted clients."
            );
        }
        ClusterCommand::Join {
            host,
            port,
            database,
            username,
            password_stdin,
        } => {
            refuse_existing(&existing)?;
            validate_namespace(&namespace)?;
            // Validate address before asking for a secret.
            endpoint(&host, port, database, username.as_deref(), "placeholder")?;
            let password = read_password(password_stdin, false)?;
            let saved = Saved {
                version: 1,
                redis_url: endpoint(&host, port, database, username.as_deref(), &password)?,
                namespace,
                owned: None,
            };
            check_connection(&RedisConfig::new(&saved.redis_url, timeout)?).await?;
            storage.save_new(&saved)?;
            println!("Connection saved after authenticated PING. Run swarmcrawl node.");
            println!(
                "PING does not verify namespace agreement or firewall restrictions. Confirm the database/namespace with the owner; use only a trusted LAN/private VPN (plaintext Redis TCP)."
            );
        }
        ClusterCommand::Start => {
            let saved = existing
                .ok_or("no saved deployment; run cluster init IP or cluster join HOST first")?;
            require_owned(&saved)?;
            docker_available().await?;
            start(&storage, &saved, timeout).await?;
            println!(
                "Owned Redis ready (authenticated PING). Connection unchanged; run swarmcrawl node."
            );
        }
        ClusterCommand::Stop { yes } => {
            let saved = existing.ok_or("no saved deployment")?;
            let owned = require_owned(&saved)?;
            docker_available().await?;
            let Some(container) = inspect_owned(owned).await? else {
                println!("Owned Redis is already absent; no changes made.");
                return Ok(());
            };
            eprintln!(
                "WARNING: stopping disposable Redis permanently loses ALL jobs/results in every database. Gracefully stop every node first (Ctrl-C and wait for exit). This CLI cannot prove remote nodes have stopped."
            );
            if !yes && !confirm("stop")? {
                return Err(
                    "stop canceled; Redis was not stopped (use --yes for deliberate automation)"
                        .into(),
                );
            }
            stop_and_remove(&container).await?;
            println!(
                "Owned Redis stopped and removed. Jobs/results are lost. cluster start reuses the credentials with empty Redis state."
            );
        }
        ClusterCommand::Info => {
            let saved =
                existing.ok_or("no saved connection; run cluster init IP or cluster join HOST")?;
            print_info(&saved)?;
        }
        ClusterCommand::Remove { yes } => {
            let Some(saved) = existing else {
                println!("No saved cluster connection; nothing removed.");
                return Ok(());
            };
            // Preflight local files before any Docker mutation. Never recursively
            // delete the directory: it may contain unrelated user files.
            storage.check_removable()?;
            let container = if let Some(owned) = &saved.owned {
                docker_available().await?;
                let container = inspect_owned(owned).await?;
                eprintln!(
                    "WARNING: remove permanently deletes owned Redis, ALL jobs/results in every database, and this PC's saved credentials/configuration. No backup is created. Gracefully stop ALL nodes first; this CLI cannot prove remote nodes have stopped."
                );
                container
            } else {
                eprintln!(
                    "WARNING: remove deletes only this PC's saved connection/credential, without backup. Remote Redis and its jobs are NOT changed. Gracefully stop this PC's nodes first."
                );
                None
            };
            if !yes && !confirm("remove")? {
                return Err(
                    "remove canceled; nothing removed (use --yes for deliberate automation)".into(),
                );
            }
            if let Some(container) = container {
                stop_and_remove(&container).await?;
            }
            // Keep ownership metadata until Docker removal has succeeded. Local
            // cleanup removes config.json last so a partial failure can be retried.
            storage.remove_connection()?;
            println!(
                "Cluster connection removed without backup. You can now run cluster init IP or cluster join HOST again."
            );
        }
    }
    Ok(())
}

fn refuse_existing(existing: &Option<Saved>) -> Result<()> {
    if existing.is_some() {
        return Err("saved configuration already exists; refusing to replace it. Use cluster start for an owned deployment, cluster info to inspect it, or confirmed cluster remove before a fresh init/join".into());
    }
    Ok(())
}

fn require_owned(saved: &Saved) -> Result<&Owned> {
    saved.owned.as_ref().ok_or_else(|| {
        "this PC joined an external deployment; only its owner can start/stop Redis".into()
    })
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut random = vec![0; bytes];
    getrandom::fill(&mut random).map_err(|_| "OS secure random generator unavailable")?;
    Ok(random.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn display_host(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(_) => ip.to_string(),
        IpAddr::V6(_) => format!("[{ip}]"),
    }
}

fn endpoint(
    host: &str,
    port: u16,
    database: u8,
    username: Option<&str>,
    password: &str,
) -> Result<String> {
    let host = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    let mut url = Url::parse("redis://localhost/").expect("constant URL");
    if let Ok(ip) = host.parse::<IpAddr>() {
        url.set_ip_host(ip).map_err(|_| "invalid Redis host")?;
    } else {
        // Host::parse rejects URL syntax, rather than silently dropping a path or credentials.
        let parsed = url::Host::parse(host)
            .map_err(|_| "invalid Redis host; provide an IP or DNS name, not a URL")?;
        url.set_host(Some(&parsed.to_string()))
            .map_err(|_| "invalid Redis host")?;
    }
    url.set_port(Some(port)).map_err(|_| "invalid Redis port")?;
    url.set_path(&format!("/{database}"));
    url.set_username(username.unwrap_or(""))
        .map_err(|_| "invalid Redis username")?;
    url.set_password(Some(password))
        .map_err(|_| "invalid Redis password")?;
    Ok(url.into())
}

fn read_password(from_stdin: bool, confirmation: bool) -> Result<String> {
    let password = if from_stdin {
        let mut line = String::new();
        io::stdin()
            .lock()
            .take(4098)
            .read_line(&mut line)
            .map_err(|_| "cannot read password from stdin")?;
        if line.ends_with('\n') {
            line.pop();
        }
        if line.ends_with('\r') {
            line.pop();
        }
        line
    } else {
        if !io::stdin().is_terminal() {
            return Err("no interactive terminal; supply the password using --password-stdin (never a command-line argument)".into());
        }
        rpassword::prompt_password("Redis password (hidden): ")
            .map_err(|_| "cannot read hidden password from terminal")?
    };
    validate_password(&password)?;
    if confirmation && !from_stdin {
        let repeated = rpassword::prompt_password("Confirm Redis password (hidden): ")
            .map_err(|_| "cannot read hidden password confirmation")?;
        if repeated != password {
            return Err("passwords do not match; no connection saved or container created".into());
        }
    }
    Ok(password)
}

fn validate_password(password: &str) -> Result<()> {
    if password.is_empty() || password.len() > 4096 || password.contains(['\n', '\r', '\0']) {
        return Err("password must contain 1-4096 bytes without NUL or line breaks".into());
    }
    Ok(())
}

fn validate_bind(ip: IpAddr) -> Result<()> {
    if ip.is_unspecified() || ip.is_multicast() || ip == IpAddr::V4(Ipv4Addr::BROADCAST) {
        return Err(
            "IP must select one concrete unicast host address, never all interfaces".into(),
        );
    }
    Ok(())
}

fn print_info(saved: &Saved) -> Result<()> {
    use redis::IntoConnectionInfo;
    let info = saved
        .redis_url
        .clone()
        .into_connection_info()
        .map_err(|_| "invalid saved Redis connection")?;
    let defaults = DEFAULT_REDIS_URL
        .into_connection_info()
        .expect("constant Redis URL");
    let redis::ConnectionAddr::Tcp(host, port) = info.addr() else {
        return Err("saved connection must use TCP".into());
    };
    let redis::ConnectionAddr::Tcp(default_host, default_port) = defaults.addr() else {
        unreachable!("constant TCP default");
    };
    let host = host.trim_matches(['[', ']']);
    let label = if host.parse::<IpAddr>().is_ok() {
        "IP"
    } else {
        "Host"
    };
    let marker = |is_default| if is_default { " (default)" } else { "" };
    // Render only allowlisted fields; never serialize/debug the URL or credentials.
    let mut out = io::stdout().lock();
    writeln!(
        out,
        "Saved connection (not transient flag/environment overrides):"
    )?;
    writeln!(
        out,
        "{label}: {}{}",
        host.escape_default(),
        marker(host == default_host)
    )?;
    writeln!(out, "Port: {port}{}", marker(port == default_port))?;
    writeln!(
        out,
        "Database: {}{}",
        info.redis_settings().db(),
        marker(info.redis_settings().db() == defaults.redis_settings().db())
    )?;
    writeln!(
        out,
        "Namespace: {}{}",
        saved.namespace,
        marker(saved.namespace == DEFAULT_JOB_NAMESPACE)
    )?;
    Ok(())
}

fn confirm(action: &str) -> Result<bool> {
    if !io::stdin().is_terminal() {
        return Ok(false);
    }
    eprint!("Type '{action}' to confirm data loss: ");
    io::stderr()
        .flush()
        .map_err(|_| "cannot display destructive-operation confirmation")?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .map_err(|_| "cannot read destructive-operation confirmation")?;
    Ok(answer.trim() == action)
}

/// Return only static labels, never stringify subprocess arguments: they may
/// contain private host paths or arbitrary identifiers from Docker.
fn docker_operation(args: &[&str]) -> &'static str {
    match args {
        ["info", ..] => "daemon check (docker info)",
        ["container", "ls", ..] => "container lookup (docker container ls)",
        ["container", "inspect", ..] => "ownership/state inspection (docker container inspect)",
        ["create", ..] => "container creation (docker create)",
        ["start", ..] => "container start (docker start)",
        ["stop", ..] => "container stop (docker stop)",
        ["rm", ..] => "container removal (docker rm)",
        _ => "Docker operation",
    }
}

/// Docker has no structured error codes for these failures. Classify familiar
/// diagnostic phrases into fixed hints, NOT a redacted copy of arbitrary output.
/// Unknown/localized/version-specific messages must fall back without echoing them.
fn docker_failure_hint(stderr: &[u8]) -> &'static str {
    let message = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    let contains_any = |phrases: &[&str]| phrases.iter().any(|phrase| message.contains(phrase));
    // Invalid IP takes precedence over the generic "ports are not available"
    // wrapper emitted by Docker Desktop for both IP and port failures.
    if contains_any(&[
        "cannot assign requested address",
        "can't assign requested address",
        "requested address is not valid in its context",
    ]) {
        "[bind-address] Docker reports that the selected IP is unavailable on its host. Compare cluster info with the actual LAN/VPN IP of the Redis-hosting PC; do not copy the example IP. With WSL/Docker Desktop, verify Windows-side port publishing, not just the WSL virtual IP. No interface or saved IP was changed."
    } else if contains_any(&[
        "port is already allocated",
        "address already in use",
        "ports are not available",
        "only one usage of each socket address",
        "socket in a way forbidden by its access permissions",
    ]) {
        "[host-port] Docker reports an unavailable host port: it may be occupied, reserved or denied by the OS. Check the port in cluster info, other containers and host services; on Windows also check reserved/excluded TCP ports. Do not stop unrelated services blindly."
    } else if contains_any(&[
        "mounts denied",
        "is not shared from the host",
        "bind source path does not exist",
        "invalid mount config",
        "error while creating mount source path",
        "error mounting",
        "not a directory",
    ]) {
        "[config-mount] Docker could not mount the private Redis configuration. Use a local Docker daemon with access to the configuration directory; check Docker Desktop file sharing/WSL integration and host-path permissions. Keep credential files private; do not chmod them world-readable."
    } else if contains_any(&[
        "pull access denied",
        "manifest unknown",
        "failed to resolve reference",
        "failed to pull image",
        "error getting credentials",
        "unauthorized: authentication required",
        "toomanyrequests",
    ]) {
        "[image-access] Docker could not obtain redis:7.4-alpine. Check registry/network access, Docker authentication/credential-helper setup and pull rate limits. Retry cluster start after fixing access; no saved credentials need to be replaced."
    } else if contains_any(&[
        "cannot connect to the docker daemon",
        "is the docker daemon running",
        "error during connect",
    ]) {
        "[daemon-access] Docker could not reach its daemon. Start Docker/Docker Desktop, check the active Docker context or DOCKER_HOST, and enable WSL integration if applicable."
    } else if message.contains("permission denied") || message.contains("operation not permitted") {
        "[permission] Docker reports a permissions failure. Check this user's daemon/socket access and Docker's access to the private bind mount. Do not weaken credential-file permissions or run the whole crawler as root as a workaround."
    } else if message.contains("container name") && message.contains("already in use") {
        "[name-conflict] Docker reports an existing container with the requested name. Saved ownership must match before start/stop/remove can act; unrelated containers will not be adopted or deleted."
    } else if message.contains("no space left on device") {
        "[storage-full] Docker reports exhausted storage. Check host/Docker disk space; do not prune unrelated volumes or containers blindly."
    } else {
        "[unclassified] Docker's diagnostic was not recognized. Check the named operation, daemon access, image availability, host IP/port and private bind-mount support. Share this safe error, cluster info and your OS/Docker setup for diagnosis; do not share config.json, redis.conf or raw logs."
    }
}

async fn docker(args: &[&str], seconds: u64) -> Result<Vec<u8>> {
    let operation = docker_operation(args);
    let output = tokio::time::timeout(
        Duration::from_secs(seconds),
        Command::new("docker").args(args).stdin(Stdio::null())
            .kill_on_drop(true).output(),
    ).await
        .map_err(|_| format!("Docker command timed out during {operation} after {seconds} seconds; inspect Docker availability before retrying. The operation may have taken effect; saved configuration was not deleted"))?
        .map_err(|_| format!("cannot execute Docker during {operation}; install Docker and ensure docker is on PATH and executable by this user"))?;
    if !output.status.success() {
        let status = output.status.code().map_or_else(
            || "terminated by signal".into(),
            |code| format!("exit {code}"),
        );
        return Err(format!(
            "Docker command failed during {operation} ({status}): {} Raw Docker diagnostics withheld to protect secrets.",
            docker_failure_hint(&output.stderr),
        ).into());
    }
    Ok(output.stdout)
}

async fn docker_available() -> Result<()> {
    let output = docker(&["info", "--format", "{{.OSType}}"], 10).await?;
    if output != b"linux\n" {
        return Err(
            "cluster setup requires a local Linux Docker daemon (on Windows use WSL integration)"
                .into(),
        );
    }
    Ok(())
}

struct Container {
    id: String,
    running: bool,
}

async fn inspect_owned(owned: &Owned) -> Result<Option<Container>> {
    let name = format!("swarmcrawl-{}", owned.token);
    let filter = format!("name=^/{name}$");
    let ids = docker(
        &[
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            &filter,
            "--format",
            "{{.ID}}",
        ],
        10,
    )
    .await?;
    let ids = std::str::from_utf8(&ids)
        .map_err(|_| "invalid Docker container listing")?
        .trim();
    if ids.is_empty() {
        return Ok(None);
    }
    if ids.len() != 64 || !ids.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("unexpected Docker container identity; refusing operation".into());
    }
    let output = docker(
        &[
            "container",
            "inspect",
            "--format",
            "{{json .Config.Labels}}",
            ids,
        ],
        10,
    )
    .await?;
    let labels: std::collections::HashMap<String, String> = serde_json::from_slice(&output)
        .map_err(|_| "missing Docker ownership labels; refusing operation")?;
    if labels.get(OWNER_LABEL) != Some(&owned.token)
        || labels.get(MANAGED_LABEL).map(String::as_str) != Some("v1")
    {
        return Err("Docker ownership labels do not match saved metadata; refusing to adopt, start or stop this container".into());
    }
    let running = docker(
        &[
            "container",
            "inspect",
            "--format",
            "{{.State.Running}}",
            ids,
        ],
        10,
    )
    .await?;
    match running.as_slice() {
        b"true\n" | b"false\n" => Ok(Some(Container {
            id: ids.into(),
            running: running == b"true\n",
        })),
        _ => Err("invalid Docker container state".into()),
    }
}

async fn stop_and_remove(container: &Container) -> Result<()> {
    if container.running {
        docker(&["stop", "--time", "10", &container.id], 20).await?;
    }
    // Disposable state: removal also clears stale Docker forwarding metadata.
    docker(&["rm", "--volumes", &container.id], 15).await?;
    Ok(())
}

async fn start(storage: &Storage, saved: &Saved, timeout: u64) -> Result<()> {
    let owned = require_owned(saved)?;
    let container = match inspect_owned(owned).await? {
        Some(container) => container,
        None => {
            create(storage, saved, owned).await?;
            inspect_owned(owned)
                .await?
                .ok_or("created container not found; private recovery metadata retained")?
        }
    };
    if !container.running {
        docker(&["start", &container.id], 20).await?;
    }
    // Retry only read-only readiness probes. This is not a crawler write retry.
    let config = RedisConfig::new(&saved.redis_url, timeout)?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if check_connection(&config).await.is_ok() { break; }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }).await.map_err(|_| "authenticated Redis readiness timed out after 30 seconds; check bind/port, Docker mount permissions and firewall. Configuration and owned container retained. For stale Docker forwarding, gracefully stop nodes, then cluster stop --yes and cluster start (ALL Redis data is lost)")?;
    Ok(())
}

#[cfg(unix)]
async fn create(storage: &Storage, saved: &Saved, owned: &Owned) -> Result<()> {
    let info = redis::IntoConnectionInfo::into_connection_info(saved.redis_url.clone())
        .map_err(|_| "invalid saved connection")?;
    let password = info
        .redis_settings()
        .password()
        .ok_or("owned deployment password missing")?;
    validate_password(password)?;
    // Redis double-quoted \xNN escapes encode each UTF-8 byte. Arbitrary user
    // passwords cannot inject directives or change tokenization (quotes/spaces/#).
    let escaped: String = password
        .bytes()
        .map(|byte| format!("\\x{byte:02x}"))
        .collect();
    let render = |value: &str| {
        format!(
            "bind 0.0.0.0\nprotected-mode yes\nport 6379\nsave \"\"\nappendonly no\nrequirepass {value}\nlogfile \"\"\n"
        )
    };
    let config = render(&format!("\"{escaped}\""));
    let path = storage.dir.join("redis.conf");
    if path
        .try_exists()
        .map_err(|_| "cannot inspect private Redis configuration")?
    {
        private_metadata(&path, false)?;
        let existing =
            std::fs::read(&path).map_err(|_| "cannot read private Redis configuration")?;
        // Retain compatibility with earlier generated-hex deployments without
        // rewriting their mounted file or exposing an unquoted user password.
        let legacy = password.len() == 64
            && password.bytes().all(|byte| byte.is_ascii_hexdigit())
            && existing == render(password).as_bytes();
        if existing != config.as_bytes() && !legacy {
            return Err(
                "private Redis configuration differs from saved deployment; refusing replacement"
                    .into(),
            );
        }
    } else {
        storage.write_new("redis.conf", config.as_bytes())?;
    }
    // Run as the host file owner: a 0600 bind mount must NOT be made world-readable
    // to accommodate the image's default redis UID. No persistence needs /data writes.
    let user = format!(
        "{}:{}",
        rustix::process::geteuid().as_raw(),
        rustix::process::getegid().as_raw()
    );
    let source = path
        .to_str()
        .ok_or("configuration path must be UTF-8 for Docker")?;
    if source.contains(',') {
        return Err("Docker bind-mount configuration path cannot contain a comma".into());
    }
    let mount =
        format!("type=bind,source={source},target=/usr/local/etc/redis/redis.conf,readonly");
    let publish = format!("{}:{}:6379", display_host(owned.bind), owned.port);
    let name = format!("swarmcrawl-{}", owned.token);
    let label = format!("{OWNER_LABEL}={}", owned.token);
    docker(
        &[
            "create",
            "--name",
            &name,
            "--label",
            &label,
            "--label",
            "org.swarmcrawl.managed=v1",
            "--publish",
            &publish,
            "--user",
            &user,
            "--read-only",
            "--tmpfs",
            "/data",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--mount",
            &mount,
            "--entrypoint",
            "redis-server",
            IMAGE,
            "/usr/local/etc/redis/redis.conf",
        ],
        120,
    )
    .await?;
    Ok(())
}

#[cfg(not(unix))]
async fn create(_: &Storage, _: &Saved, _: &Owned) -> Result<()> {
    Err("owned cluster setup requires Unix/WSL permissions".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_diagnostics_are_classified_without_echoing_payloads() {
        for (message, category) in [
            (
                "Ports are not available: listen tcp 192.0.2.17:6379: bind: cannot assign requested address",
                "[bind-address]",
            ),
            (
                "ports are not available: The requested address is not valid in its context",
                "[bind-address]",
            ),
            (
                "Bind for 127.0.0.1:6379 failed: port is already allocated",
                "[host-port]",
            ),
            ("listen tcp: bind: address already in use", "[host-port]"),
            (
                "An attempt was made to access a socket in a way forbidden by its access permissions",
                "[host-port]",
            ),
            (
                "invalid mount config for type bind: bind source path does not exist",
                "[config-mount]",
            ),
            (
                "Mounts denied: The path is not shared from the host",
                "[config-mount]",
            ),
            (
                "OCI runtime create failed: error mounting private-path: permission denied",
                "[config-mount]",
            ),
            ("pull access denied for image", "[image-access]"),
            (
                "error getting credentials - err: exit status 1",
                "[image-access]",
            ),
            ("toomanyrequests: image pull limit", "[image-access]"),
            (
                "Cannot connect to the Docker daemon. Is the docker daemon running?",
                "[daemon-access]",
            ),
            (
                "permission denied while trying to connect to the Docker daemon socket",
                "[permission]",
            ),
            (
                "Conflict. The container name is already in use",
                "[name-conflict]",
            ),
            ("no space left on device", "[storage-full]"),
            ("new or localized message", "[unclassified]"),
            ("", "[unclassified]"),
        ] {
            let payload = format!(
                "{message}\nredis://user:fixture-secret@host/0 /private/fixture-path?token=fixture-query\x1b[31m"
            );
            let hint = docker_failure_hint(payload.as_bytes());
            assert!(hint.starts_with(category), "wrong safe category: {hint}");
            assert!(!hint.contains("fixture-"));
            assert!(!hint.contains("\x1b"));
        }
        assert!(docker_failure_hint(&[0xff, 0xfe]).starts_with("[unclassified]"));
    }

    #[test]
    fn docker_operation_labels_never_copy_arguments() {
        for (args, expected) in [
            (vec!["info", "fixture-secret"], "daemon check (docker info)"),
            (
                vec!["create", "--mount", "fixture-secret"],
                "container creation (docker create)",
            ),
            (
                vec!["start", "fixture-secret"],
                "container start (docker start)",
            ),
            (
                vec!["stop", "fixture-secret"],
                "container stop (docker stop)",
            ),
            (
                vec!["rm", "fixture-secret"],
                "container removal (docker rm)",
            ),
            (
                vec!["container", "inspect", "fixture-secret"],
                "ownership/state inspection (docker container inspect)",
            ),
            (
                vec!["container", "ls", "fixture-secret"],
                "container lookup (docker container ls)",
            ),
            (vec!["fixture-secret"], "Docker operation"),
        ] {
            assert_eq!(docker_operation(&args), expected);
        }
    }

    #[test]
    fn endpoint_encodes_password_without_changing_host_or_database() {
        let url = endpoint("::1", 6380, 4, None, "fixture:/@?#% secret").unwrap();
        let info = redis::IntoConnectionInfo::into_connection_info(url).unwrap();
        assert_eq!(
            info.redis_settings().password(),
            Some("fixture:/@?#% secret")
        );
        assert_eq!(info.redis_settings().db(), 4);
        assert!(endpoint("redis://secret@host/path", 6379, 0, None, "x").is_err());
    }

    #[test]
    fn wildcard_and_multicast_bindings_are_rejected() {
        for address in ["0.0.0.0", "::", "224.0.0.1", "ff02::1", "255.255.255.255"] {
            assert!(validate_bind(address.parse().unwrap()).is_err());
        }
        assert!(validate_bind("127.0.0.1".parse().unwrap()).is_ok());
        assert!(validate_bind("192.168.1.50".parse().unwrap()).is_ok());
    }
}
