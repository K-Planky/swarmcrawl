# swarmcrawl

A Rust/Tokio distributed crawler coordinated through one Docker Redis. The host-run
binary, Cargo package and library are all named `swarmcrawl`; environment settings
use `SWARMCRAWL_*`.

**Current capability:** `swarmcrawl cluster init <IP>` / `join <HOST>` save a private connection
so `node`, `submit`, `status`/`status -f`, and `stats` work across terminals without
repeating settings. `jobs` lists retained running/completed/failed/aborted jobs;
`abort <job>` or `abort --all` permanently stops selected running work.
`cluster start` / `stop` manage only CLI-owned disposable Redis.
The crawler commands work against the shared Redis job protocol. Host-run nodes
concurrently claim/fetch/publish work, stay available for later submissions, and
drain owned work on graceful shutdown. Job commands talk only to Redis; finished statistics
are immediately readable by a new CLI process and retained after nodes exit.
`swarmcrawl --help`, `--version`, and the read-only Redis `check` also work. Domain,
HTTP, Redis and CLI/node process tests pass, including adversarial traversal with
identical hand-checked totals for **1, 2 and 3 participating nodes** and concurrent
overlapping jobs. A [deterministic live-demo runbook](demo/README.md) supplies two
local jobs, exact expected totals, three-node operation and owned-state reset.
The [final technical audit](#final-technical-audit) passed from a fresh checkout;
owner-controlled demo/release actions remain separate.

## Start here

1. Build/install with [development setup](#development-setup), then follow the
   **[CLI-managed two-PC setup](#cli-managed-two-pc-setup)**. For a single PC,
   explicitly use `cluster init 127.0.0.1` and skip `cluster join`.
2. For a repeatable presentation, use **[demo/README.md](demo/README.md)**. It starts
   a supplied gated local site and three host nodes; no public Internet is needed.
3. For your own target, launch [N nodes](#running-nodes-and-lifecycle), then
   [submit, follow and read stats](#user-job-commands) from any Redis-connected host.

Reference: [configuration](#cli-configuration), [work flow](#how-the-components-fit-together),
[URL/text policies](#crawl-domain-policies), [atomic coordination](#atomic-frontier-protocol),
[tests](#quality-gates-and-tests), [source map](#source-organization-and-conventions),
[owner hand-in checklist](#owner-demo-and-hand-in-checklist).

## Development setup

Prerequisites:

- [Rustup](https://rustup.rs/) and a platform C compiler/linker/build toolchain.
  Install CMake too for platforms where the bundled Rustls crypto build needs it.
  `rust-toolchain.toml` pins Rust **1.99.0**, minimal profile, rustfmt and Clippy.
  `Cargo.toml` requires Rust **1.99.0** or newer; the project uses edition 2024.
  Rustup installs the pinned toolchain when invoked in this repository.
- **On the Redis-hosting PC only:** Docker with a reachable local Linux-container
  daemon and access to `redis:7.4-alpine` (the first initialization may download it).
  Joining PCs do **not** need Docker. Only Redis runs in Docker, not the Rust binary.
  CLI-managed private storage currently requires Unix permissions (Linux/WSL/macOS);
  native Windows users should run it in WSL. On WSL, start Docker Desktop and enable
  distro integration; a Windows `docker.exe` on PATH alone is not enough. Nothing
  installs Docker/system packages or changes firewalls automatically.
- GNU `timeout` (coreutils) on PATH for setup CLI tests; Bash too for the isolated
  Redis smoke script. Python 3 is needed for the opt-in interactive terminal tests
  (and the demo fixture), not for the application.
  ShellCheck is optional for shell linting.

From the repository root:

```sh
rustup show
cargo build --locked
cargo run --locked -- --help
cargo run --locked -- --version
```

The application lockfile is committed. Dependency updates are intentional changes;
normal builds/tests use `--locked`. No `.env` file is loaded automatically.

## CLI-managed two-PC setup

Build the same revision on both PCs. To make the examples available as `swarmcrawl`
from any terminal, install once on each PC:

```sh
cargo install --locked --path .
# Ensure ~/.cargo/bin is on PATH, or use ./target/debug/swarmcrawl after cargo build.
```

### 1. Redis-hosting PC

```sh
swarmcrawl cluster init 192.168.1.50
# Enter your chosen Redis password at the hidden prompt, then confirm it.
swarmcrawl node
```

Replace the example with the **trusted LAN/private VPN IP belonging to this PC and
reachable from the second PC**. An IP is always required, even interactively: there
is no address menu, implicit loopback selection, or `--bind` option. For one-PC use,
explicitly enter `127.0.0.1`; wildcard/multicast addresses are rejected. Virtual/WSL
addresses may not be reachable from another physical PC.

Choose a strong, unique password and keep it in a password manager. Init does not
generate or display a password. The interactive password confirmation catches typos
before saving anything or creating a container. For automation, pass one password
line from a protected file or secret manager, never a command-line password:

```sh
swarmcrawl cluster init 192.168.1.50 --password-stdin < /private/path/redis-password
# Optional initial settings, shared by every client:
# swarmcrawl cluster init 192.168.1.50 --port 6380 --database 3 --namespace mycrawl:v1
```

These are **alternative first-time commands**, not commands to run after an existing
initialization. Passwords must be 1–4096 UTF-8 bytes without NUL or line breaks;
spaces, quotes, backslashes and Unicode are preserved and safely encoded in the
Redis configuration. Stdin supplies the password once (no confirmation line).

Init writes private files, creates authenticated Redis with persistence disabled,
publishes only the supplied host IP, and waits at most 30 seconds for authenticated
readiness after Docker startup. Each Docker operation is bounded (image/container
creation: 120 seconds; other operations: 10–20 seconds). It saves ownership before
Docker creation so an interrupted setup is recoverable. No command displays the
saved password.

Allow the Redis TCP port through the host firewall **only from intended client
hosts**. Docker publishing/firewall behavior is platform-specific; check the actual
rules, especially with Docker Desktop/WSL or VPN routing. Swarmcrawl does not alter
or verify them. **Redis connections are plaintext TCP, not TLS**: use a trusted
isolated LAN or private VPN, never a public interface/Internet port forward.
Authentication is not encryption. Every Redis client is trusted; namespaces are
state separation, not access control. Nodes need no inbound crawler port.

### 2. Inspect connection details and share your password securely

In another terminal on the owner PC:

```sh
swarmcrawl cluster info
```

Example output for the address above:

```text
Saved connection (not transient flag/environment overrides):
IP: 192.168.1.50
Port: 6379 (default)
Database: 0 (default)
Namespace: swarmcrawl:v1 (default)
```

Info is offline: no Docker or Redis connection is required. It shows the **saved**
settings, never a username/password or full credential-bearing URL. Values matching
built-in defaults carry `(default)`; `127.0.0.1` also gets that marker, but init still
requires you to supply it explicitly. A joined DNS endpoint is shown as `Host:`
without resolving it or claiming an IP. Info is not a connectivity or firewall check.

Share your chosen password through an authenticated secure channel, such as your
password manager. Do not put it into command arguments, shell history, chat logs,
or the repository. Share the address/port/database/namespace from info too; ordinary
init output also prints a non-secret join command. `cluster credential` has been
removed—there is no password-reveal command.

### 3. Second PC (no Docker required)

```sh
swarmcrawl cluster join 192.168.1.50
# Enter the shared password at the hidden prompt.
swarmcrawl node
```

Replace the example address. If the owner selected other settings, copy them exactly:

```sh
swarmcrawl cluster join 192.168.1.50 --port 6380 --database 3 --namespace mycrawl:v1
```

Joining runs an authenticated PING and saves only on success. It does not register
a node, install a server, or prove namespace agreement/firewall restrictions. An
unused or wrong namespace can still PING. Database selection is 0–15 for this managed
Redis; `--username` supports a named ACL user when joining an existing deployment.

For automation, use `--password-stdin` with one password line from a protected file
or a secret-manager pipe, never a literal password in the shell command:

```sh
swarmcrawl cluster join 192.168.1.50 --password-stdin < /private/path/redis-password
```

The normal hidden prompt requires a terminal; noninteractive invocations must opt
into stdin. Password input is limited to 4096 bytes, excluding its line ending.

### 4. Submit and observe from either PC

Keep both foreground nodes running. In another terminal, with no environment setup:

```sh
swarmcrawl check
swarmcrawl submit https://example.org/docs/
swarmcrawl status -f 1
swarmcrawl stats 1
```

Use your own target and the job ID actually returned by `submit`. The target must
be reachable from **every node** (not one PC's loopback HTTP server). Finished results
remain available after nodes exit while Redis remains running.

### 5. Shutdown and subsequent startup

First press **Ctrl-C in every node terminal and wait for each node to exit**. This
stops new claims and drains owned work. Then, on the Redis owner PC:

```sh
swarmcrawl cluster stop
# Read the data-loss warning and type: stop
# Automation, only after stopping nodes: swarmcrawl cluster stop --yes
```

**ALL Redis jobs, IDs and results in every database/namespace are permanently lost.**
Redis is disposable (`save ""`, `appendonly no`); this is not a backup/persistence
feature. The CLI cannot prove remote nodes have stopped. Stop removes only its
matching owned container, retaining private credentials/configuration. A joining
PC cannot use start/stop to affect the remote owner. Later, on the owner:

```sh
swarmcrawl cluster start
swarmcrawl node
# Second PC: swarmcrawl node
```

Start recreates a removed container from the private settings and credentials.
An already-running owned Redis is checked, **not restarted**; its jobs are preserved.
Container names alone never establish ownership: the saved random deployment identity
must match explicit Docker ownership labels, and operations then use the exact ID.
There is no background node service, SSH deployment, crash recovery or new control plane.

### 6. Remove the cluster and initialize again (no backup)

To discard the deployment rather than retain its settings for `start`, gracefully
stop **all nodes** and wait for them to exit, then on the owner PC:

```sh
swarmcrawl cluster remove
# Read the warning and type: remove
# Deliberate automation: swarmcrawl cluster remove --yes
swarmcrawl cluster init 192.168.1.50
# Choose a password again, then restart/rejoin nodes as needed.
```

Removal stops/removes only the label-verified owned Redis container and deletes
`redis.conf` and `config.json`, including the saved credential. **No backup is made;
all jobs/results in every database are lost.** It works after `stop` or incomplete
initialization too. A Docker ownership/removal failure retains local recovery
metadata instead of silently forgetting a potentially running deployment.

On a **joining PC**, `cluster remove` only deletes its local saved connection and
credential; it does not contact Docker or change remote Redis or jobs. Stop that
PC's nodes first. You can then `cluster join` another deployment. Owner removal
cannot erase saved settings from remote PCs: remove/rejoin there if settings or
passwords change.

Removal deletes only known connection files, not arbitrary contents of the user
configuration directory or shared Docker images. An empty `deployment.lock` and
the private directory remain so removal and reinitialization cannot race on different
lock files; these contain no deployment settings or credentials. No manual file
moving or backups are required to initialize again. File deletion is not a promise
of forensic secure erasure or deletion of backups you made separately.

### Saved configuration and diagnostics

One configuration is stored outside the repository:

- Linux/WSL: `$XDG_CONFIG_HOME/swarmcrawl/` if set, otherwise
  `~/.config/swarmcrawl/`.
- macOS: `~/Library/Application Support/swarmcrawl/`.
- `SWARMCRAWL_CONFIG_DIR` can select an **absolute** private directory (useful for
  isolated tests); it is a location override, not a deployment-profile framework.

The directory must be owned by this user with mode `700`; `config.json`, the owner’s
`redis.conf`, and the lifecycle lock use mode `600`. Do not share these files or put
backups in a public directory. Files/symlinks with unsafe ownership/permissions are
rejected rather than silently relaxed. The Redis process runs as the host file
owner with an explicit entrypoint, read-only mount/root filesystem and dropped
capabilities, so the mounted `600` configuration need not be world-readable. The
Docker daemon and host account remain trusted. Bind mounts require a local daemon
and a host directory Docker can access (macOS/Desktop may require file sharing).

- **Repeat init/join:** refuses to replace any saved configuration, even with the
  same endpoint. Use `cluster start` for the owner; just run `node` on a joined PC.
  To deliberately switch deployments/IP, gracefully stop nodes and use confirmed
  `cluster remove` before a fresh init/join. Existing jobs are not migrated or backed
  up. Use `cluster info` to inspect saved settings without exposing credentials.
- **Docker unavailable:** install/start Docker yourself; check daemon permissions,
  Linux-container mode and WSL integration. No automatic package installation occurs.
- **Docker command failed:** errors identify the operation (for example `docker
  create` versus `docker start`), exit status and a safe diagnostic category:
  `[bind-address]` for an unavailable host IP, `[host-port]` for an occupied/reserved
  or denied port, `[config-mount]` for private configuration mount failures, and
  `[image-access]` for image/registry or credential-helper failures. Daemon access,
  permission, name-conflict and exhausted-storage errors have separate hints too.
  Classification uses known Docker messages; unrecognized/localized messages report
  `[unclassified]`, never raw output. Do not change to all-interface publishing or
  relax credential-file permissions as a workaround.

  If an older installed binary only prints the generic error, run the updated source
  from the repository without deleting your saved configuration:

  ```sh
  cargo run --locked -- cluster info
  cargo run --locked -- cluster start
  ```

  The second command retries owned setup and does not restart an already-running
  Redis. For help, share the safe error, info output, exact command (no password),
  and whether this is Linux, WSL/Docker Desktop, or macOS. Do **not** share
  `config.json`, `redis.conf`, passwords or raw container/daemon logs. An example IP
  in this README is not necessarily assigned to your PC; in WSL, distinguish the
  Windows-side LAN/VPN address from the WSL virtual address.
- **Partial init / busy port:** settings are retained, never overwritten or deleted.
  Free the chosen port or restore the selected interface, then `cluster start`.
  Docker Desktop can retain broken forwarding after a failed bind; after stopping
  nodes, `cluster stop --yes` then `cluster start` recreates only the owned container
  (destructive: all Redis data is lost). No automatic destructive recovery occurs.
- **Readiness timeout:** check the exact bind/port, Docker mount access, local
  routing/firewall and private-file permissions. Setup/start never report ready
  without authenticated PING. Raw Docker/server diagnostics are withheld to avoid
  leaking secrets; CLI errors name safe likely causes.
- **Join/check fails:** check the owner is running, address/port, credential and
  selected database. Never use the second PC's loopback address for the owner.
  A failed join writes no connection file and can be retried.
- **Unexpected job IDs/results:** confirm the same database and namespace on every
  PC; PING cannot do that. Check for overriding flags or old environment variables.
- **Cluster endpoint overrides:** cluster commands deliberately reject `--redis-url`
  / `SWARMCRAWL_REDIS_URL`; init uses its required IP, join uses its HOST, and lifecycle
  uses saved ownership. Unset an old Redis URL variable first. Init/join honor the
  global namespace flag/environment; info/start/stop/remove use saved metadata.

Physical two-PC networking and native macOS setup are not verified by local process
checks. The existing no-saved-config flag/environment workflow remains available.

## Start a development Redis

This optional manual, unauthenticated loopback-only fixture remains useful for
protocol development/tests; normal users can use `cluster init` instead.

Use an unused local port (6379 below) and an unused container name:

```sh
docker run --detach --rm --name swarmcrawl-redis-dev \
  --publish 127.0.0.1:6379:6379 \
  redis:7.4-alpine redis-server --save '' --appendonly no

docker exec swarmcrawl-redis-dev redis-cli ping
cargo run --locked -- check
# Once built, the equivalent host command is:
./target/debug/swarmcrawl check
```

Expect `PONG` from redis-cli and `Redis connectivity: OK (PONG)` from `swarmcrawl`.
If Redis is not ready yet, retry the read-only check; each `swarmcrawl check` is bounded
by its configured timeout. It exits nonzero on failure and never prints success
first. This container uses disposable state with persistence disabled. Job identities
and results have no TTL and last until Redis is cleared or shut down; persistence
across shutdown is not required.

Cleanup targets only this development container:

```sh
docker stop swarmcrawl-redis-dev
```

The published address is loopback, **not all interfaces**. Do not expose this
unauthenticated fixture to other PCs; use the authenticated [managed setup](#cli-managed-two-pc-setup).
If saved configuration already exists, explicitly select the fixture with
`--redis-url redis://127.0.0.1:6379/0` (and a test namespace for job commands).
Plain TCP `redis://` is supported, not TLS (`rediss://`) or Unix sockets.
The supplied demo HTTP server intentionally cannot serve other hosts.

## CLI configuration

Global options may appear before or after any subcommand. Precedence is
**explicit flag > environment variable > saved configuration > built-in default**.
Saved configuration supplies the Redis URL (including database/authentication) and
namespace; Redis/HTTP deadlines still use flags/environment/defaults. Values are validated
after selection, so an overridden invalid environment value does not cause failure.
Environment settings use the `SWARMCRAWL_*` names listed below.

| Option | Environment | Default | Contract |
| --- | --- | --- | --- |
| `--redis-url` | `SWARMCRAWL_REDIS_URL` | `redis://127.0.0.1:6379/0` | TCP Redis URL; port 1–65535, nonnegative database, optional username/password |
| `--redis-timeout-secs` | `SWARMCRAWL_REDIS_TIMEOUT_SECS` | `5` | Integer 1–60; one deadline for check connection+PING; job commands/nodes use it for connection setup and each operation |
| `--namespace` | `SWARMCRAWL_JOB_NAMESPACE` | `swarmcrawl:v1` | 1–128 ASCII letters, digits, `:`, `_`, `-`; every node and job command must share it and the Redis database |

Examples with no credentials:

```sh
swarmcrawl --redis-url redis://127.0.0.1:6380/0 check
SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6380/0 swarmcrawl check --redis-timeout-secs 10
```

Use `./target/debug/swarmcrawl` unless the binary has been installed or added to PATH.
Prefer `cluster init` / `join` saved credentials, or an externally supplied environment
variable rather than command-line URLs (command lines can appear in shell history/process listings).
Never commit actual credentials. Help hides environment values; configuration
Debug output redacts connection information; errors include a safe operation/error
category, not raw URLs, server error messages, or credentials. Success goes to stdout
and errors to stderr. Exit codes: `0` success/help/version, `1` configuration or
runtime failure, `2` command-line syntax/usage errors, `130` interrupted status
follow. A failed/aborted-job status prints its diagnostic snapshot then exits `1`;
failed, aborted or unfinished stats exit `1` without partial numbers. Nodes report
safe PID/job/operation context to stderr and never log base URLs, request queries or Redis
credentials. Node runtime errors exit nonzero only after draining other owned work.

`node` additionally accepts these implemented settings with the same precedence:

| Node option | Environment | Default | Contract |
| --- | --- | --- | --- |
| `--fetch-timeout-secs` | `SWARMCRAWL_FETCH_TIMEOUT_SECS` | `30` | Integer 1–300; total HTTP network deadline, separate from Redis deadlines |

The ten-request node-wide maximum is fixed, not an option to raise. Namespaces
are for isolated deployments/tests, not random per-process identities. Use
exclusive, nonoverlapping prefixes; never reset keys while nodes use them. `check`
is only PING and does not access or validate job namespaces.

## User job commands

Start Redis as above and build `cargo build --locked`. Use the host binary directly;
launch one or more `./target/debug/swarmcrawl node` processes in other terminals when
ready to crawl. Submission also works while no nodes run. All commands use the
configuration above; use `swarmcrawl <command> --help` for command-specific help.
Replace these example loopback URLs with your own reachable HTTP(S) bases:

```sh
./target/debug/swarmcrawl submit http://127.0.0.1:8000/docs/ http://127.0.0.1:8000/other/
# Example on a fresh namespace (IDs may differ on an existing one):
# job 1  input 1  created
# job 2  input 2  created
./target/debug/swarmcrawl submit http://127.0.0.1:8000/docs/#intro
# job 1  input 1  existing
./target/debug/swarmcrawl status 1
./target/debug/swarmcrawl status -f 1
./target/debug/swarmcrawl stats 1
./target/debug/swarmcrawl jobs
# Deliberate, permanent abort (choose one, when needed):
./target/debug/swarmcrawl abort 2
./target/debug/swarmcrawl abort --all
# Same deployment namespace can be set before or after any job command:
./target/debug/swarmcrawl --namespace swarmcrawl:v1 status 1
./target/debug/swarmcrawl stats 1 --namespace swarmcrawl:v1
```

- **`submit <url>...`:** each input is an independent job with that URL as seed and
  base. Validate **every URL before connecting/submitting**; one invalid URL rejects
  the entire invocation, exits `1`, and identifies its one-based input position
  without echoing it. With valid inputs, submit sequentially through Redis, print
  and flush one ID per input in input order, and return **without waiting for any
  node or HTTP request**. Canonical duplicates return `existing`, including after
  completion, failure or abort; they never restart a job. URLs are not printed because
  queries may contain secrets. `created`/`existing` describes submission identity,
  not crawl success.
- Submission writes are **not a batch transaction**: a later Redis/output failure
  exits `1`, but earlier printed IDs remain valid and an interrupted write may
  have taken effect. Under a healthy Redis, resubmitting retrieves retained IDs
  without duplicate work. There is no automatic write retry, rollback, or reset.
- **`status <job>`:** one coherent snapshot, e.g.
  `job 1  crawled 12  frontier 3  in flight 2  files 9  discovered 17  running`.
  **crawled means processed unique URL attempts**, including redirects and broken
  links; `files` means successful existing files, not just HTML. `discovered` equals
  crawled + frontier + in flight. State is `running`, `done`, `aborted`, or `failed`
  with a safe failure category. No URL/query appears in the snapshot. Running/done
  snapshots exit `0`; failed/aborted snapshots exit `1` with a final-statistics-unavailable
  error. Failed/aborted counts are frozen diagnostics, not live network activity.
- **`status -f <job>` / `status --follow <job>`:** print an immediate snapshot, then
  poll **500 ms after each bounded Redis read** and print only changed snapshots.
  Intermediate changes between polls may be coalesced. Exit `0` at done or `1` at
  failed/aborted/Redis error/unknown ID; an already-terminal job prints once and exits.
  Ctrl-C while following exits `130` with an interruption message. It stops only
  this observer, **not the crawl**, and performs no job writes. There is no overall
  follow deadline: a queued job without nodes keeps waiting until interrupted.
- **`stats <job>`:** read final validated `WebStats` only when done. Print decimal
  `files`, `extensions`, and `words`, then one extension/count per line in sorted
  extension order. For example, `files: 3   extensions: 2   words: 4` followed by
  `html 2` and `jpg 1`. Zero-file completed jobs are valid. Full unsigned word totals
  remain exact, not floating-point. Running, failed, aborted, unknown, or corrupt jobs exit
  `1` without printing partial totals. A new CLI can read results immediately when
  done, with no node restart/manual finalization; results remain after nodes exit.

Job IDs are positive canonical decimal integers, local to a Redis database and
namespace. Invalid IDs are rejected before connecting; unknown IDs suggest checking
that configuration. Redis and CLI output errors are nonzero, not success messages.
Only the `node` command creates an HTTP fetcher or executes crawler work. There is
no direct CLI-to-node connection and no individual job reset/resume command.

### List and abort jobs

- **`jobs`:** list every retained job in the selected Redis database/namespace in
  numeric ID order, including `running`, `done` (completed), `failed`, and `aborted`.
  Each row uses the same coherent counters as `status`; rows can represent different
  instants, not one global snapshot. No URLs or credentials are printed. The ID set
  is captured once from retained submissions, so later submissions appear on the next
  invocation. An empty namespace prints `No jobs...`. Listing failed/aborted jobs is
  successful (exit `0`); Redis/corrupt-data/output errors exit `1`, possibly after
  earlier rows were printed. This simple listing loads all retained IDs into memory,
  then reads one bounded snapshot per ID; it is not a paginated history service.
- **`abort <job>`:** atomically move a running job to permanent `aborted` state and
  remove it from scheduling. Prints `job <id>  aborted`. Unknown/invalid/corrupt jobs
  return a nonzero error without resetting their data. A completed, failed, or
  already-aborted job is unchanged (exit `0`); completed statistics remain readable.
  The explicit command is confirmation—there is no further prompt.
- **`abort --all`:** capture the current active-job set and abort it in numeric ID
  order in the **selected database/namespace only**, not every Redis database or
  namespace. Each transition is atomic; the batch is **sequential, not all-or-nothing**.
  It does not include jobs submitted after that set was captured or keep future
  jobs from running. A job finishing before its abort is reported unchanged. A later
  failure exits `1` but earlier aborts remain in effect; output is flushed per job.
  Ambiguous writes are never automatically retried; inspect `jobs`/`status` afterward.

An abort prevents **new Redis claims and late result/link publication**, not instant
network interruption. Tasks already owning URLs may still start/finish their requests
under existing HTTP deadlines (default 30 seconds), then discard both successes and
errors without shutting down the node. Nodes continue serving other/future jobs.
The CLI does not wait for drain or prove that all remote requests have ended; HTTP
body deadlines do not bound synchronous parsing CPU time. Gracefully stop nodes and
wait for exit before stopping/removing Redis, even after `abort --all`.

Aborted frontier/owner sets and partial counters remain **frozen for diagnosis**;
`in flight` therefore reflects ownership at abort, not a current socket count.
`status -f` exits `1` on abort and `stats` refuses partial results. Abort does not
remove the submission identity: resubmitting the same canonical URL returns the
same aborted job, not a retry. There is no resume, reclaim or per-job deletion.
This requested maintenance feature extends the baseline's optional cancellation
scope without adding killed-node recovery.

**Upgrade every node and CLI to this version before using abort.** Existing jobs and
submission indexes need no migration, but older binaries do not understand `aborted`
and may exit on it. Mixed-version abort handling is not supported.

## How the components fit together

```text
CLI submit/status/stats/jobs/abort ─► one Redis (Docker)
                                  ▲  ▲  ▲
                                  │  │  │ atomic ownership/publication
                              host crawler nodes (Tokio)
                                  │
                                  ▼ one scope-checked GET per owned URL
                              HTTP(S) targets
```

The CLI has no node or target connection. Redis owns submission identity, each
job's seen/frontier/owners, coherent progress and retained statistics. Nodes are
interchangeable; a rotating scheduler admits at most ten owned tasks across all
jobs, using one shared asynchronous fetcher budget through response-body lifetime.
Synchronous HTML parsing returns owned data before any subsequent await. The
fetcher reports an outcome and links; only the Redis publisher may contribute a
file or enqueue discoveries. Completion publishes children **before releasing the
parent in the same atomic script**, and marks done only with zero waiting work and
zero owners. Final statistics already exist at that instant. See the
[protocol invariants and completion argument](#atomic-frontier-protocol) for the
precise boundary and numeric safeguards.

## Crawl domain policies

These contracts are implemented in the library and tested independently of network
access. Fetchers and application nodes use these same contracts with real local
HTTP responses. Fetching, Redis coordination and lifecycle are described below.

### URL identity and scope

- Accept absolute HTTP(S) URLs with a host and nonzero port. Reject URL credentials
  rather than forwarding authentication. Remove fragments from all identities.
  Use the `url` crate's WHATWG normalization: lowercase/IDNA hostnames, omitted
  default ports, resolved dot segments, and an implicit `/` for an empty path.
- Keep path case, trailing slash, query values/order, and empty-query distinctions.
  Do not decode percent escapes or normalize their spelling beyond the URL parser.
  Encoded separators stay encoded; server-specific rewriting is not inferred.
- An eligible URL must have the **same origin and begin with the canonical base
  URL string**, including its query if present. This is a literal prefix, not a
  directory heuristic: `/docs` also permits `/docs-extra`, while `/docs/` does not.
  A base `/docs/?q=1` permits `/docs/?q=1&x=2` (also `?q=10`) but not
  `/docs/child?q=1`. Submit a trailing-slash directory base without a query when
  that is the intended scope. Scope never expands to the rest of the host.
- Resolve relative, root-relative, query-only and protocol-relative references
  first; reject invalid/unsupported URLs and filter scope **before retrieval**.
  Resolving alone does not authorize fetching. Use the first non-template
  `<base href>` throughout the page; an invalid first base falls back to the page
  URL, not a later base. An external base can resolve references but cannot expand
  job scope. Fragment variants deduplicate locally; the Redis protocol also
  deduplicates discoveries globally within each job.
- URL Debug output and typed errors do not expose URLs or query values. Full
  canonical strings are available explicitly for storage/requests, not logs.

### HTML, linked resources, and words

HTML classification recognizes `text/html` and `application/xhtml+xml`, ignoring
case and MIME parameters; URL suffixes are irrelevant. Both use HTML5 parsing and
its malformed-markup recovery (not strict XML validation). The parser consumes the
whole supplied decoded document synchronously and returns only owned `Send` data.
Scripting is disabled in the parser, so `noscript` hyperlinks are parsed too. No
JavaScript executes and no DOM/parser object leaves the extraction function.

Static URL attributes currently inventoried:

| Elements | Attributes |
| --- | --- |
| `a`, `area`, `link` | `href` |
| `img`, `source` | `src`, every `srcset` candidate |
| `script`, `iframe`, `frame`, `audio`, `track`, `embed`, `input` | `src` |
| `video` | `src`, `poster` |
| `object` | `data` |
| SVG `a`, `image`, `use` | `href`, or legacy namespace-qualified `xlink:href` if `href` is absent |

Links in templates are inventoried even though template text is excluded from
words. `srcset` descriptors do not restrict static discovery; commas inside URLs
(e.g. data URLs) are not split into fake links. Unsupported schemes are discarded.
CSS URLs/imports, JavaScript-created links, inline `srcdoc`, form submission,
meta-refresh, and browser interactions are not executed or interpreted.

Count text nodes only: tags, attributes and comments contribute no words. Exclude
`script`, `style` and `template` descendant text, but include titles, `noscript`,
SVG text and ordinary hidden text (no CSS visibility inference). Decode HTML
entities, lowercase Unicode text, then split on Unicode whitespace; count tokens
whose first byte is ASCII `a-z`. Punctuation stays attached: `cat,dog`, `can't`
and `foo-bar` each count once; `'quoted'`, `123abc` and `éclair` do not count.
Text-node boundaries act as whitespace (`one<b>two</b>` counts two words).
This deliberately loose definition measures total words, not distinct words.

### Extensions and totals

Take the final serialized URL path segment's last nonempty suffix, without its
dot, and lowercase it. Ignore queries/fragments; keep `jpg` distinct from `jpeg`.
Extensionless paths, directory paths, simple dotfiles (`.env`) and trailing dots
count as `html` regardless of MIME. `.config.JSON` is `json`; `archive.tar.GZ` is
`gz`. Percent escapes are not decoded: `file%2Ejpg` is extensionless and
`file.%4A%50%47` has extension `%4a%50%47`.

`WebStats` preserves the required `usize` file/extension counts, extension map,
and `u64` word total. Empty statistics are valid; extension keys must be nonempty
and lowercase with positive counts. Validation enforces
`sum(ext_counts) = num_files` and `num_exts = ext_counts.len()`; no files implies
zero words. Checked merging rejects overflow/invalid input without partial mutation
and never uses floats.
A file contribution presupposes unique ownership and an existing successful file;
missing/redirect responses must not contribute, and non-HTML files pass zero words.
The domain alone does not determine existence or enforce cluster ownership. Redis
job reads validate these same invariants and parse decimal integers exactly, with
checked range validation (see below).

## Bounded HTTP fetching

`fetch::Fetcher::new(FetchConfig)` builds one async reqwest client and one private
**ten-request semaphore**. A node must construct it once and clone it for all
jobs/tasks; clones share both client and budget. `fetch(&scope, &url)` checks scope
before any request and returns `FetchOutcome { result: PageResult, discoveries }`.
It does not claim, schedule, publish, retry, or remember URLs. The caller must own
the URL first, then publish the outcome/discoveries through `JobStore::complete`.
An operational error must instead fail the owned job; it is not an empty outcome.
`node::run_node` connects these interfaces without bypassing either ownership or
the shared request budget.

Each call issues **one GET**, never HEAD followed by GET. Automatic redirects and
reqwest retries are disabled. HTTP/1 is used without idle connection reuse, avoiding
transparent stale-connection reuse retries; the async client/TLS state is shared.
No proxy configuration, cookies, Referer forwarding, browser execution or automatic
requests to discovered resources are enabled. HTTPS uses Rustls with platform
certificate verification; certificate errors are operational failures, not ignored.

`FetchConfig::default()` sets a **30-second total network deadline**; the library
constructor accepts integer seconds **1–300**. It covers connection, response
headers and HTML body transfer/decompression together, not just inactivity between
chunks. Waiting for the shared permit is excluded. Keep the permit until the HTML
body is fully consumed or any other response is dropped. Decode/parse only after
network body consumption, without a parser document across an await. Errors and
future cancellation release local network resources/permits, but **do not** make
lost Redis ownership recoverable.

### HTTP outcomes and redirects

| Response | Outcome |
| --- | --- |
| 2xx except 206 | Existing file; HTML contributes parsed words/links, non-HTML contributes zero words |
| 301, 302, 303, 307, 308 | No file for the redirect URL; resolve Location and return an allowed target as discovery |
| 4xx except 408/429 | No file or discoveries; inaccessible/broken links do not contribute |
| 206, 408, 429, 5xx, other terminal statuses | Operational error; partial/throttled/unreliable responses cannot prove a complete crawl |
| Transport, timeout, incomplete HTML body, decompression/decoding/extraction failure | Operational error, never final partial statistics; no retry |

Redirect resolution drops fragments and applies exactly the same origin/base-prefix
boundary before any target retrieval. Invalid, unsupported, credentialed and
out-of-scope targets are not admitted. Missing/non-ASCII Location headers are
explicit errors. Redirect bodies are not parsed. Chains, loops, self redirects and
converging targets return discoveries only; **Redis ownership/deduplication**, not
recursive client fetching, decides whether another GET may occur. Redirect URLs do
not count as files; successful eligible targets do, using their own extensions.

### Body and encoding policy

- Classify only by Content-Type using the domain MIME rules. A missing/empty or
  non-HTML type is a non-HTML file, with no sniffing based on suffix/body. Reject
  ambiguous multiple or non-ASCII Content-Type headers; malformed HTML MIME
  parameters cannot silently choose an encoding.
- Fully consume HTML; there is no truncation/size cap presented as a complete
  crawl. Large HTML documents require corresponding memory for bytes, decoded
  text and the parser. The network deadline does not bound synchronous CPU parsing.
- Drop non-HTML responses immediately after successful headers; do not wait for or
  deliberately consume the full body. Successful headers establish existence even
  when the remaining body is not verified. Some bytes can already be buffered or
  arrive before socket cancellation: this is **not** a zero-byte download promise.
- Advertise/support gzip; reqwest decompresses it during bounded body reads.
  Identity HTML is supported; remaining unsupported Content-Encoding values fail
  rather than treating compressed bytes as text. Corrupt gzip is a transfer error.
  Non-HTML encodings do not need decoding because those bodies are not consumed.
- Validate declared MIME charset labels using `encoding_rs` (including common
  legacy labels); default to UTF-8. A UTF-8/UTF-16 BOM can override a recognized
  declared charset. Do not sniff `<meta charset>` or apply locale heuristics.
  Unknown labels or malformed decoded text fail, rather than replacement-character
  parsing that might miss hyperlinks. HTML entities remain the parser's job.
- `FetchError` keeps safe categories/status codes only, with no raw reqwest source,
  URL, Location, header value, credentials or query in Display/Debug. Node error
  reporting adds safe job/PID context without copying network payloads or URLs.

## Redis job storage and read contracts

The library exposes `jobs::JobStore::{connect, submit, jobs, abort, snapshot, stats}` plus the
frontier operations described below. It uses an async multiplexed connection and
the shared `RedisConfig`, with a deadline for
connection setup and each operation. `submit` accepts a validated `CrawlUrl`; it
returns a `Submission { job, created }` without waiting for any worker. Job IDs
are namespace-local positive decimal integers (`1`, `2`, …), capped at Redis's
signed sequence maximum `9223372036854775807`. They are stable, not random secrets
or URL hashes. IDs can have gaps after detected orphan-key corruption.

The default namespace is **`swarmcrawl:v1`**. All nodes and CLI/library submitters/readers
must share that namespace and Redis database. `connect_in_namespace` and the global
`--namespace` allow isolated tests/deployments (1–128 ASCII letters, digits,
`:`, `_`, `-`). Use exclusive, nonoverlapping namespaces. Schema v1 is strict, with no
migration or automatic reset of incompatible/corrupt data. Redis Cluster is not
supported; coordination uses the assignment's single Redis instance.

With prefix `P` and job ID `J`:

| Key | Redis type | Meaning |
| --- | --- | --- |
| `P:submissions` | hash | canonical base URL → retained job ID |
| `P:next-job-id` | string | signed Redis `INCR` job sequence |
| `P:next-worker-id` | string | retained atomic process-identity sequence, not a heartbeat or recovery lease |
| `P:active` | set | Running job IDs available to worker scheduling |
| `P:job:J:meta` | hash | `schema=1`, canonical `base`, `state`, `processed`, `failure` |
| `P:job:J:seen` | set | unique canonical discovered URLs, including seed |
| `P:job:J:frontier` | list | discovered URLs waiting for ownership |
| `P:job:J:in-flight` | hash | owned URL → worker identity |
| `P:job:J:stats` | hash | `num_files`, `num_exts`, `total_word_count` aggregates |
| `P:job:J:extensions` | hash | lowercase extension → positive file count |

Empty Redis collections are absent keys (e.g. initial in-flight/extensions).
No job key/identity/result expires. There is no public delete/reset API; do not
clear a shared Redis to reset tests. Tests remove only their unique namespaces.
Job base URLs/queries are stored verbatim after canonicalization: Redis must be
trusted/private, and neither diagnostics nor Debug output should expose them.

### Atomic submission

One small `EVAL` transition checks the canonical submission hash first. An existing
base returns its retained ID, including for a completed, failed or aborted job, without
adding another seed or restarting it. A new base gets a sequence ID, running
metadata, seen seed, one frontier entry, zero statistics, submission mapping and
active membership together. All clients use this same transition; no local lock
or separate coordinator is involved. It creates work even with no nodes running.
The script reads the sequence back as a decimal string after `INCR`; it never uses
Lua's floating-point reply as an ID, so IDs above `2^53` remain exact.

Redis scripts are serialized but **do not roll back on errors**. Shared key types,
sequence range and new-job key absence are checked before initialization. A
preexisting job-key collision can consume a sequence ID only; it never creates a
new submission identity or a partial seed. Invalid data is surfaced, not deleted.
Infrastructure loss/resource exhaustion during writes is outside the baseline;
a timeout may mean a write took effect. No automatic retry or crash recovery is
implemented. Idempotent resubmission under a healthy Redis still returns the same ID.

### Progress, state and statistics reads

A single `MULTI/EXEC` reads metadata, seen cardinality, frontier length, in-flight
length and both statistics hashes. No worker write can interleave those reads.
The snapshot reports:

- `processed`: unique finished URL attempts, including broken links and redirects;
  this is the CLI's **crawled** count, not HTML pages or successful files.
- `frontier`, `in_flight`, `discovered`: waiting URLs, owned URLs, and all discovered
  URLs. Validate `discovered = processed + frontier + in_flight` with checked sums.
- `successful_files`: existing files only, at most `processed`, from the aggregate.
- `state`: `running`, `done`, `aborted`, or `failed` with a fixed `fetch`, `statistics`,
  or `protocol` failure category. Aborted uses an empty failure field. Running requires
  outstanding work; done requires an empty frontier and no owners. Failed/aborted
  retain outstanding diagnostic state and can never supply final statistics. Both
  freeze diagnostic state; nodes stop claiming those jobs and drain owned network
  tasks. Abort is deliberate user intent, not a node operational failure.

Submission starts at `(discovered, processed, frontier, in_flight, successful_files)
= (1, 0, 1, 0, 0)`. Unknown jobs report an explicit error. Invalid metadata, key
shapes, canonical URLs, state combinations or statistics are rejected with safe
operation/field categories, not raw Redis error messages or stored values.
All stored counters/totals are canonical unsigned decimal strings: no negative,
signed, padded, fractional or exponential forms. Reads preserve the target's full
`usize` and `u64` ranges, including words above `i64::MAX`, and reject overflow.

`stats` returns validated `WebStats` **only for done jobs**; running, failed and aborted
jobs return distinct errors rather than partial statistics. Its state/result reads are
coherent with the atomic publisher below; there is no separate finalization step.
An empty frontier with owned work remains running.

## Atomic frontier protocol

The worker protocol extends the schema above; the fetcher and node scheduler are
separate components that use this same ownership/publication boundary. Its correctness
rests on these invariants:

1. Within a job, each canonical URL is in `seen` exactly once. Every seen URL is
   either waiting, owned by exactly one worker, or processed:
   `|seen| = processed + |frontier| + |in-flight|`.
2. Claiming atomically moves one waiting URL into `in-flight`, without changing
   `seen` or `processed`. Only a matching ownership token can publish that claim.
   There is no lease, reclaim or requeue path.
3. Completion validates ownership and arithmetic **before** changing collections.
   It adds unseen in-scope children to both `seen` and the frontier, contributes
   at most one file, removes the parent owner and increments `processed` in one
   Redis script. No observer can see the parent released without its children.
4. The same script marks `done` and removes active membership only when both the
   frontier and in-flight hash are empty. Aggregates are already stored when done
   becomes visible, so a new reader can immediately obtain final `WebStats`.
5. Done/failed/aborted states never return to running. Failure and abort retain
   diagnostic state, remove active membership and never expose partial aggregates
   as final results.
   Redis interruption/ambiguous writes and abrupt worker loss remain unsupported.

Redis serializes each script across processes; local mutexes are not involved.
Submission and all worker mutations must use this protocol, not direct key writes.
The implemented library API is:

- `active_jobs()`: sorted advisory IDs. Another worker can finish a listed job;
  the claim operation always rechecks its state.
- `claim(job, worker)`: atomically move the next FIFO URL to ownership and return
  a private-field `WorkClaim` containing the job/base/URL/worker/namespace. Worker
  IDs are 1–128 ASCII letters/digits/underscores/hyphens. Library callers can select
  them; application nodes call `allocate_worker()` at startup, which atomically
  increments a retained sequence and returns a fresh `node-<decimal>` identity
  across hosts. The sequence is checked before mutation and read back as a string
  for exact values above `2^53`. All nodes share the same Redis/database.
  `None` means no waiting work **now**, including for terminal jobs; it is not
  proof that a running job is finished. Unknown IDs are explicit errors.
- `complete(claim, PageResult, discoveries)`: `File { html_word_count }` contributes
  one file using the **claimed URL's** extension; `NoFile` contributes no statistics
  (broken response or redirect). Either may publish discoveries. Rust filters
  canonical typed URLs by same-origin/base-prefix scope and deduplicates the input;
  Redis `SADD` deduplicates across all processes. Only the fetcher can establish
  existence/MIME and decide which outcome is appropriate.
- `fail(claim, reason)`: terminal fetch/statistics/protocol failure for a still-owned
  claim. Preserve queued/owned work and partial aggregates for diagnosis, without
  processing the failed parent. Already-owned tasks can finish their network work
  but publication returns the retained failure and makes no changes. No new claims,
  links, contributions or restart are allowed; partial stats never become final.

- `abort(job)`: use the same shared protocol preflight and atomic Lua boundary as
  claims/publication. Transition running → aborted and remove active membership;
  return unchanged for any terminal job. If completion wins the race first, final
  results are preserved. If abort wins, later owned successes/failures return
  `Aborted` without mutation. A claim serialized before abort remains diagnostic
  ownership; one serialized afterward returns no work.

Publication returns `Published { done }`, `AlreadyCompleted`, `Aborted`, or `Failed(reason)`.
Repeating a published claim is a safe no-op, even with a different outcome/links;
wrong ownership or namespace is an error, never another contribution. Claims are
not serialized or transferred between workers. Clones can use the same claim, but
only the first successful publication changes state. This guards application-level
repeated completion; it does **not** provide recovery from ambiguous network writes.
Do not automatically retry a Redis timeout or transport error.

The scripts preflight key types, schema, progress/numeric aggregate invariants, active
membership, ownership and required arithmetic before writes. Invalid storage is
an error, not a reset. Exact decimal digit addition preserves the full unsigned
word/file ranges (and checks the target's `usize` bound); whole totals never pass
through Lua `tonumber` or signed Redis increments. Overflow atomically marks a
statistics failure **without** publishing children, releasing the parent or
partially changing totals. Redis collection cardinalities beyond Lua's exact
integer range (`2^53 - 1`) are explicitly rejected rather than rounded. This limit
is on collection sizes, not on word totals or job IDs.

Under healthy Redis/nodes/network and exclusive protocol writes, submission
establishes the invariants; claiming preserves their partition; completion extends
it only through atomic unseen-child admission and one parent-to-processed move.
Since every future discovery must come from a current owner, empty frontier **and**
no owners means no publisher can produce more work. The final contribution and
terminal state are in the same script, serialized with transactional readers.
Therefore each URL has one ownership interval and at most one contribution,
completion cannot precede delayed discoveries, and final totals do not depend on
worker interleaving. There is no re-fetch/recovery path. Failure is explicitly not
a completed crawl. Real host-run node tests below connect this protocol to HTTP
fetching; user-CLI process tests cover the same reads/submission boundary. The
adversarial distributed suite below checks the whole path with real requests and
hand-calculated results for several node counts.

## Running nodes and lifecycle

Start the development Redis above, build once, and run each node directly on the
host in its own terminal (not a crawler container):

```sh
cargo build --locked
./target/debug/swarmcrawl node
# Repeat in another terminal for N >= 1, or point each host at the same Redis:
./target/debug/swarmcrawl --redis-url redis://127.0.0.1:6379/0 node
# Optional HTTP deadline and shared deployment namespace:
./target/debug/swarmcrawl node --fetch-timeout-secs 60 --namespace swarmcrawl:v1
```

A ready message on stderr means the node connected and allocated its identity.
It polls the advisory active-job set every **100 ms when it has task capacity**,
and refreshes immediately after owned tasks finish (a parent may make an idle job
runnable). No inbound port or CLI-to-node connection exists. One sorted snapshot
rotates after the last successfully claimed job, then claims one FIFO URL per job
in round-robin order. Idle/terminal jobs leave that scheduling pass; refresh admits
new work/jobs before refilling slots. An idle claim attempt does not move the
fairness cursor. Refresh takes priority over ready candidates so an existing busy
frontier cannot monopolize capacity while a different parent's children become
ready. The next poll deadline starts after its Redis read, avoiding an overdue-poll
loop with a slow healthy Redis.

Each claimed URL immediately gets a Tokio task using the **one process-wide
Fetcher**. There are at most ten owned tasks, including parsing and Redis
publication, as well as the fetcher's ten-request cap through response-body
lifetime. No unbounded pool of waiting tasks/claims exists. Successful outcomes
and discoveries go through the same atomic `complete` transition; redirects and
ordinary 4xx do not strand jobs. Nodes remain running after jobs finish, ready for
later submissions. Use the submit/status/stats commands above from any host with
Redis access; these commands never contact the nodes or crawl targets.

Press **Ctrl-C**, or send **SIGTERM on Unix**, to stop new claims and drain all
owned tasks before exit. Shutdown never cancels a Redis claim/write midway: an
already-started bounded claim may finish and gets its task drained too. Pending
HTTP bodies finish or hit their configured deadline; discovered children are
published even during drain but are left queued for another/new node. A second
shutdown signal does not abort a Unix drain. Successful graceful exit is code 0;
errors during drain produce code 1. Redis/job/result state has no expiry and needs
no manual finalization or restart to read final library statistics.

Unexpected HTTP/decoding/timeout errors first fail the owned job atomically, then
stop that node's new claims, drain its remaining tasks and exit **1** with safe
job/PID context. Word overflow is a statistics failure; invalid scope/internal
budget state is a protocol failure. If another node already failed the job,
publication retains that failure, never contributes partial final stats. Other
owned healthy jobs can still publish while draining; queued healthy jobs can be
finished by remaining/new nodes. Invalid Redis/protocol data, ambiguous Redis
operations, task panics and signal-handler errors also stop claims and surface a
nonzero error. Raw Redis messages and joined panic payloads are not copied into
node errors; Rust's default panic hook still emits runtime diagnostics for an
unsupported code panic. There is no automatic HTTP or Redis write retry.

**Not crash recovery:** a forced kill, task panic, external cancellation of
`run_node`, Redis loss, or an ambiguous coordination write can leave ownership
stranded. Graceful draining does not reclaim those claims. Do not delete/reset a
live namespace or promise resumability after these excluded failures.

## Quality gates and tests

Abort/listing maintenance coverage runs in the regular Redis smoke suite:
`tests/redis_jobs.rs` checks retained listing, exact numeric ordering through
`i64::MAX`, existing indexes and corrupt IDs; `tests/redis_frontier.rs` checks
racing aborts, claims and final publication, no late contributions/discoveries,
retained identities, unchanged completed/failed results and fail-closed corruption.
`tests/cli_jobs.rs` checks listing all four states, namespace-only abort-all and
honest partial-batch failure. A gated two-node test aborts with 20 owners plus queued
work, then releases successful HTML and HTTP 503s: late links/results are discarded,
follow exits, stats stay unavailable, both nodes remain usable for a healthy job,
and normal graceful shutdown succeeds. These local process tests do not claim
immediate HTTP interruption or physical multi-host verification.

Run all of these for relevant changes (CI uses the same commands):

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
bash scripts/redis-smoke.sh
```

Managed setup tests (Unix + GNU `timeout`) are in `tests/cluster_setup.rs`. Default
cases cover saved/environment/flag precedence, secret-free info/default markers,
required IP and password validation, private-file modes and symlink rejection,
failed join, repeat init/join refusal, unavailable Docker, safe operation/cause
classification without copying diagnostic payloads, partial setup metadata,
ownership rejection, removal confirmation, lock safety and preservation of unrelated
files. Fake Docker tests cover failure contracts, not provisioning compatibility.
Run the **real Docker and interactive PTY** cases separately (also run in CI;
Python 3 is required for the PTY case):

```sh
cargo test --locked --test cluster_setup -- --ignored --test-threads=1
```

It provisions authenticated Redis on an isolated loopback port, verifies no-password
and wrong-password rejection, joins with Docker absent, checks shared database/namespace
job visibility, preserves jobs on repeated start, rejects unconfirmed stop/remove,
verifies stop/start data loss and retained user-supplied credentials (including
spaces, quotes, backslashes, Unicode and the 4096-byte boundary), retains compatibility
with earlier generated-hex configuration files, verifies real port-conflict diagnostics/recovery,
and refuses an unrelated container with a matching name but missing ownership labels.
It also verifies owner removal and fresh initialization, and joined-PC removal/rejoin
without touching remote Redis. RAII cleanup removes only test-created resources even
after assertions fail. Each CLI process is bounded to 180 seconds and Docker cleanup
to 130 seconds.

`scripts/test-cluster-terminal.py` drives actual Unix pseudo-terminals with a fake
Docker executable. It checks hidden passwords without echo, init confirmation mismatch,
explicit-IP enforcement, rejection/acceptance of stop/remove confirmation, no-backup
cleanup and the hidden join prompt. Each terminal process is bounded to ten seconds
and killed/reaped on failure; the entire script has a 60-second test deadline. This
checks interactive safety, not Docker provisioning or physical two-PC networking.

Default tests require no external Redis: domain policy and checked-arithmetic
unit tests, library configuration/redaction checks, local silent TCP peers for
the check/job-connection deadlines, and actual CLI processes for help/version,
validation, configuration precedence, and credential redaction.
Silent peers test timeout behavior only, not Redis compatibility.
Fetcher tests use scripted loopback HTTP sockets with no external website, Redis
or Docker prerequisite.

`tests/domain_policies.rs` combines the domain contracts using two HTML fixtures
and a supplied existence/MIME table: five unique existing files (`html: 2`,
`dat: 1`, `jpg: 1`, `jpeg: 1`) and 21 words (15 + 6). It checks cycles, fragment
duplicates, outside links, a broken link, MIME/suffix disagreement and an
extensionless non-HTML file. This is static domain verification, **not** evidence
of HTTP fetching, Redis deduplication or a working distributed crawler.

`tests/http_fetch.rs` has ten real local-HTTP component tests, using the reusable
`tests/support/http.rs` fixture (ephemeral loopback ports, request logs, response
gates, in-flight peak measurement, and RAII socket/task cleanup on success/failure).
Cases are bounded to 15 seconds and fixture polling to five. They cover MIME/suffix
disagreement and missing MIME; full-page late links and 10,004 hand-checked words;
HTTP absence versus operational statuses; all five redirect statuses, chains,
loops, shared targets, query/fragment identity, outside/credentialed targets and
no hidden GETs; three-file/eight-word traversal totals using a test-only deduplicating
queue; strict charset/BOM/gzip handling and invalid headers; ten advertised 100 MiB
non-HTML bodies canceled while their tails remain gated; **31 gated HTML requests
across two scopes/clones with peak exactly ten**, with only one new GET admitted
when one body completes; header/body deadlines, truncated transfers/disconnects
without retries; unreachable peers; and resource cleanup after canceled futures.
These establish fetcher behavior, not Redis scheduling or cross-process HTTP
at-most-once guarantees. Run them separately with:

```sh
cargo test --locked --test http_fetch
```

The real-Redis integration tests are **opt-in** and ignored by `cargo test`.
`scripts/redis-smoke.sh` starts a fresh Redis 7.4 Alpine container on an automatically
allocated **loopback-only** port, waits for internal PONG and host TCP/PING readiness
with bounded read-only polling (Docker forwarding can lag on WSL), runs the real
CLI check and ignored connectivity/job/frontier/node-process/user-CLI/distributed tests, and removes
only its own container/temp metadata on success or failure. Docker startup and Cargo
subprocesses have finite limits. It never calls `FLUSHDB`/`FLUSHALL` or touches
another container. Cleanup failures are reported as failures with the owned
container ID for manual removal.

For an already isolated Redis, the equivalent opt-in invocation is:

```sh
SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked --test redis_connectivity --test redis_jobs --test redis_frontier --test node_process --test cli_jobs -- --ignored
```

The connectivity test only sends PING. `tests/support/mod.rs` shares an isolated
namespace/cleanup harness for Redis and node-process suites, deleting only
test-owned keys after success or assertion failure. The original seven tests in `tests/redis_jobs.rs` check:
32 independently connected racing submissions; four independent submission
processes released through a Redis gate; different-base isolation and retention;
exact final-result fixture reads through `u64::MAX`; invalid/schema/overflow
rejection; ID precision above `2^53`, exhaustion and orphan-key handling; coherent
snapshots during concurrent fixture transactions; and cleanup after an intentional
panic while preserving another namespace. Each case is bounded to 30 seconds,
cleanup to 10 seconds, and submission subprocesses are killed/reaped on failure.
The nested panic in the cleanup test is expected, not a suppressed product failure.
Completed/failed states in the storage-read suite are **test fixtures**; the frontier
suite separately tests their actual production.

The original seven tests in `tests/redis_frontier.rs` check 32 concurrent parent discoveries;
32 competing claimers for two shared children; 32 publications of the same claim
with one contribution; a gated last parent publishing children while another job
finishes independently; empty valid results and overlapping per-job URLs; coherent
reads during actual publication; immediate final reads by a racing independent
reader; exact addition through `u64::MAX` and fail-closed overflow (including a
small test-only file-count bound using the real script); fixed failure categories
and terminal freezing; preflight key/numeric/ownership/namespace/scope corruption;
and four gated independent processes claiming/publishing a convergent graph with
25 unique child ownerships and hand-checked totals (26 files, one extension,
77 words). Process cleanup and case deadlines match the storage suite.
The frontier suite uses **mock page outcomes**, not an HTTP server or application
node binary. The separate `tests/node_process.rs` suite has eight opt-in cases
using the actual `swarmcrawl node` binary, real Redis and gated local HTTP:

- Jobs submitted before and after startup, mixed HTML/image/404/redirect outcomes,
  cycles/fragment duplicates and scope exclusion: 3 files, 2 extensions, 4 words
  for the first job; 1 file/2 words for a later job without restarting the node.
- Two concurrent jobs sharing exactly ten held HTTP bodies in one node: 15
  files/30 words each, peak exactly ten, and unique request logs.
- Two independent host processes owning ten bodies each on a convergent graph:
  empty frontier plus 20 owners stays running, delayed links are published, shared
  children deduplicate, and final results are 23 files/46 words with 23 unique GETs.
- SIGTERM with ten owned bodies and queued work: stop claiming, survive a second
  SIGINT, drain and publish a late child, then a fresh node finishes the remaining
  work with 14 files/28 words and no duplicate GETs.
- HTTP 503 fails a job, retains unavailable final stats, returns code 1 without
  query secrets, drains a healthy job and does not restart the failed job.
- A one-second HTTP timeout fails without retry or final partial statistics.
- Redis key/protocol corruption stops the node nonzero, redacts payloads and does
  not delete/reset the corrupt data.
- Thirty-two racing fresh worker identities above `2^53`, retention across clients,
  exact sequence values and fail-closed malformed/type/exhausted sequence handling.

These Unix process tests additionally require `kill` and GNU `timeout`. Each case
has the shared 30-second deadline/owned-key cleanup; node/fixture polling is bounded
to five seconds, node exit to ten. RAII kills/reaps only owned child processes on
failure before Redis cleanup; normal tests use graceful signals and await drain.
The fetcher suite separately establishes encoding boundaries. The distributed suite
below adds broader traversal, node-count equivalence and actual-node non-HTML
cancellation; physical multi-host networking remains unverified.

The original eight opt-in cases in `tests/cli_jobs.rs` use independent real user-command
processes, the same isolated Redis harness, and actual nodes/gated HTTP where
applicable. They prove multi-input nonblocking submission with no nodes and no
HTTP from user commands; retained fragment-duplicate IDs; rejected mixed-input
batches with no writes; invalid/unknown IDs; exact coherent snapshot fields;
follow milestones/exit despite an empty frontier with a held body and late links;
Ctrl-C exit 130 with unchanged job state and no repeated unchanged snapshots;
failed-job snapshots/follow exits and no partial stats; immediate sorted final
stats (3 files, 2 extensions, 4 words from 5 URL attempts) and retention after node
exit; global namespace placement/precedence/isolation; safe later-submission and
corrupt-read errors preserving printed IDs and stored data; and actual protocol
publication of empty results and full `u64::MAX` word totals read by the CLI.
`tests/distributed/mod.rs` adds four end-to-end cases, compiled as a submodule of
`cli_jobs` to reuse its command/process helpers rather than duplicate a harness.
These run automatically in the smoke script and CI, not in the default unit pass:

- **N = 1, 2, 3, fresh state each time:** eight concurrent actual `submit` processes
  return one identity. A seed/first-wave gate proves every started node actually
  owns received GETs. The graph covers converging/cyclic and fragment links,
  relative/query/base resolution, noscript/template/srcset resources, all five
  redirect statuses, chains/convergence/loops, foreign and outside targets,
  403/404/410, MIME/suffix disagreement, extensionless binaries and word exclusions.
  The last held HTML body leaves **28 processed, zero waiting, one in flight**;
  follow stays running and no final stats appear until its late child is crawled.
  Each run yields **20 files, 6 extensions, 37 words from 30 unique GETs**. A later
  nested-base job legitimately repeats three shared URLs exactly once each, with
  **3 files/6 words**. New CLI/node processes retain IDs/results after all nodes
  exit; already-done follow exits immediately. Neither foreign nor outside targets
  receive requests, and each job's Redis discovery set matches its exact scope.
- **Concurrent overlapping jobs for N = 1, 2, 3:** held HTTP bodies are correlated
  with Redis URL/worker ownership across both jobs. Every process holds exactly
  ten requests; completing one body admits exactly one new GET. Both jobs advance.
  Cluster-wide graceful drain starts no new GETs, publishes owned results and
  leaves queued work for fresh nodes. Final per-job totals/discovery sets and
  aggregate request multiplicities prove shared URLs occur once per job, not once
  globally or twice within one job. Cluster peaks are exactly **10, 20, 30**;
  stable gated ownership proves the per-node bound without test-only HTTP headers.
- **Non-HTML streaming through the real node:** ten blocked header responses fill
  the budget; twelve ZIPs each advertise 100 MiB and never release their body-tail
  gates. The node closes those sockets, advances queued HTML and finishes with
  **14 files (`html: 2`, `zip: 12`), 4 words**, using one GET per URL. Some buffered
  bytes may arrive; this proves no deliberate full-body consumption, not zero bytes.
- **Two-node operational failure:** both processes own a job when a gated 503
  fails it. Late peer HTML completions cannot add children/contributions or reopen
  the frozen failure; both nodes drain and exit nonzero without query leaks. An
  already-owned healthy job still finishes. Follow/stats report failure, no final
  partial totals appear, and a replacement node does not retry/reopen the failed job.

The adversarial fixture's totals are deliberately hand-checkable:

| Existing files | Count | Words |
| --- | ---: | ---: |
| HTML seed | 1 | 15 = 2 title + 2 ordinary + 2 noscript + 3 punctuation + 3 split text nodes + 3 entity-separated |
| Other two-word HTML bodies (left/right/shared, sub/late/final, two queries, MIME DAT, intro, template) | 11 | 22 |
| Zero-word files (plain HTML suffix, extensionless binary, 204, base HTML, JPG, JPEG, PDF, CSS) | 8 | 0 |
| **Total** | **20** | **37** |

Extensions are **html: 15; css/dat/jpeg/jpg/pdf: 1 each**. Seven redirect URLs and
three broken/inaccessible URLs are processed but not files, accounting for all
30 unique attempts. The seed is `tests/fixtures/distributed-index.html`; the
remaining explicit responses and exact eligible paths are in the test module.
Run only these four cases against an already isolated Redis with:

```sh
SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked --test cli_jobs distributed:: -- --ignored
```

Each fresh-state case uses the same 30-second deadline/owned-key cleanup (the N
comparison tests run three such cases sequentially). Gates drive milestones, not
lucky sleeps; polling is bounded to five seconds and process exits to ten. The
existing node/HTTP suites exercise request deadlines, transfer failures and normal
HTTP absence in the same full verification run. Abrupt kills, Redis loss and
ambiguous-write recovery are explicitly unsupported, not passing recovery tests.

`tests/support/process.rs` captures bounded stdout/stderr incrementally so tests
observe flushed updates, bounds waits to five seconds/exits to ten, and kills/reaps
only owned children on assertion/deadline before Redis namespace cleanup.
A default test pass is **not** evidence of Docker/Redis or distributed crawl
verification. CI runs the same gates for new commits.

The presentation fixture also has dependency-free Python control/body-gate checks
and a static Rust hand-count oracle (`tests/demo_policies.rs`). These are not a
replacement for process-level distributed evidence. Python 3.10+ is needed only
for the demo and its checks, not for the crawler or existing Redis suite:

```sh
python3 -B -m unittest discover -s demo -p 'test_*.py'
cargo test --locked --test demo_policies
```

CI runs the Python fixture checks too. `-B` avoids generating Python bytecode in the
source tree. Optional shell checks:

```sh
bash -n scripts/redis-smoke.sh
shellcheck scripts/redis-smoke.sh
```

## Final technical audit

**Audited candidate:** `f8adc4356b52a42458995d032a7352679e5bb54c`, on
**2026-10-03**. The audit reviewed the full submission → ownership → fetch/parse →
publication → completion path; no runtime, schema or dependency changes were needed.
This evidence describes that candidate, not an automatic guarantee for later changes.

A fresh clone containing only committed files, with no `target/` or Redis state,
passed the [complete quality gates](#quality-gates-and-tests): locked build,
formatting, strict Clippy, **59 default tests**, **35 opt-in real-Docker Redis/process
tests**, and **3 Python fixture tests**. Redis/process checks were repeated with fresh
containers. Setup, help/version and the default development Redis/PING instructions
also passed. Commands ran with a cleared environment containing only `HOME`, `PATH`
and `LANG`, plus the explicit demo settings from the runbook. The installed Rust
compiler/toolchain and Cargo registry cache were reused; this was not a cold-cache
or toolchain-installation test.

| Technical contract | Passing evidence |
| --- | --- |
| Rust/Tokio source, locked build and consistent CLI configuration | Quality gates, help/version and configuration/precedence/redaction tests |
| Full reachable HTML traversal, in-base safety, URL identity and redirects | Domain/HTTP tests and adversarial request logs, including zero foreign/outside requests |
| Existing files, broken-link exclusion, MIME-based HTML, words/extensions and exact `WebStats` | Hand-counted fixtures, non-HTML body cancellation, checked arithmetic and full-range Redis/CLI round trips |
| N ≥ 1 host nodes, one Docker Redis and Redis-only user commands | Independent node/CLI processes, no-node submission and fresh-checkout setup |
| Idempotent submission, one GET/contribution per URL per job and independent jobs | Concurrent submission/discovery/publication races and overlapping-job request multiplicities |
| Concurrent node work with ≤10 active requests across jobs/body lifetime | Gated N=1/2/3 runs, per-worker ownership and single-body slot admission |
| Coherent progress/follow, no premature completion, immediately readable retained stats | Delayed final discovery, publication/read races, follow exit and fresh-process reads after drain/restart |
| Same answer for any tested node count; explicit failure/lifecycle boundary | N=1/2/3 hand-checked equality, HTTP timeout/503 tests, SIGINT/SIGTERM drain and frozen-failure checks |

The [three-node runbook](demo/README.md) was rehearsed **twice**, resetting both the
owned disposable Redis and HTTP server between runs. Each rehearsal showed two
queued jobs, retained duplicate IDs, **30 held bodies (ten per node)**, running-only
stats rejection, follow completion and exact **30 files / 4 extensions / 81 words
per job**. Each site recorded **64 eligible GETs, each exactly once**, zero
outside/unexpected requests and zero gate expirations. Results/IDs remained readable
after all nodes exited; normal cleanup removed only owned processes/containers.

Environment: Linux WSL2 x86_64, Rust/Cargo **1.99.0**, Docker client/engine **29.8.0**,
Redis **7.4.11** (`redis:7.4-alpine`), Python **3.12.3**. Shell syntax/ShellCheck and
local documentation link/example checks passed. Physical multi-host networking,
non-Unix shutdown and peer comparison remain unverified. Crash/Redis-loss/ambiguous
write recovery remains excluded, not a hidden completed feature. The helper, live
demo, grading tag, post-demo publication and LMS submission still require the
[owner actions below](#owner-demo-and-hand-in-checklist).

## Source organization and conventions

- `src/main.rs`: Clap presentation, shared configuration-source selection, command
  dispatch and exit behavior; no Redis schema or network policy in CLI parsing.
- `src/cluster.rs`: explicit-IP setup, hidden/stdin password input, non-secret info,
  authenticated readiness and label-verified owned Docker lifecycle/removal.
- `src/saved.rs`: private per-user connection storage, atomic no-overwrite publication,
  permissions checks and local setup/lifecycle locking.
- `src/commands.rs`: Redis-only user-command execution, whole-batch URL validation,
  safe ordered IDs, coherent status/follow presentation and final-only sorted stats.
- `src/config.rs`: validated Redis/HTTP deadlines and typed, secret-safe errors.
- `src/node.rs`: bounded owned Tokio tasks, rotating job selection, safe failure
  propagation, signal handling and stop-claiming/drain lifecycle.
- `src/fetch.rs`: shared async client/request budget, scope-safe GET classification,
  explicit redirect discoveries, complete HTML decoding and operational errors.
- `src/redis.rs`: async multiplexed Redis connectivity and bounded read-only check.
- `src/jobs.rs`, `src/jobs/submit.lua`: typed job IDs/states, bounded async Redis
  storage, atomic submission/seed initialization and transactional validated reads.
- `src/jobs/frontier.rs`, `src/jobs/{worker,protocol,claim,complete,abort}.lua`: fresh
  process identities, typed ownership, scope filtering, preflight/exact decimal
  arithmetic, and atomic claiming,
  publication, failure, abort and finalization.
- `src/urls.rs`: canonical HTTP(S) identity, resolution, scope and path extensions.
- `src/html.rs`: MIME classification, full synchronous link/text extraction.
- `src/stats.rs`: required `WebStats` and validated checked aggregation.
- `src/lib.rs`: testable library boundary; `tests/` contains domain fixtures and
  CLI/Redis integration checks; `.github/workflows/checks.yml` runs the local gates
  on Ubuntu.

Use Rust naming conventions and direct domain-specific modules/functions.
Libraries expose typed `Result` errors; the CLI reports terminal errors at the
boundary. No logging framework is needed: node stderr diagnostics include safe
PID/job/operation context without URL credentials, sensitive queries or Redis
credentials. Tokio's signal feature implements graceful shutdown (and adds its
small signal registry/errno dependencies); no identity/scheduling framework is
needed because Redis allocates process identities atomically. Add dependencies
only for implemented needs: Clap for command parsing, Tokio for async work/deadlines,
Redis's Tokio support for the check/job store, `url` for identity/scope and `scraper`
for HTML extraction. Direct `html5ever` access configures scraper's parser with
scripting disabled; it adds no second parser. Reqwest has only Rustls/gzip features
(no blocking client, cookies, HTTP/2, system proxies, or unused format helpers).
`mime` validates HTML charset parameters and `encoding_rs` supplies strict text
decoding; dev-only `flate2` generates deterministic compressed HTTP fixtures.
Redis default features remain off; submission uses direct `EVAL`, not the optional
script-cache helper, so no extra script/hash dependency is needed.
Managed setup uses `dirs` for the per-user location, `rpassword` for hidden terminal
input, and OS-backed `getrandom` for ownership tokens (passwords are user-supplied). `serde`/`serde_json` encode the private configuration and parse
Docker labels; `tempfile` publishes complete files without overwriting; `rustix`
provides safe Unix UID/GID access for permission checks and the container user.
Tokio's process feature bounds Docker subprocesses; a standard-library file lock
serializes local setup/lifecycle operations.

Crash recovery, robots/politeness, and JavaScript rendering remain excluded.
Explicit job abort is an owner-requested extension beyond the assignment baseline.
A working connectivity check alone does not exercise the library's cluster ownership, completion or final-statistics guarantees.

## Manual sites and peer comparison

The local fixtures are the automated correctness oracle. For supplementary manual
checks, choose a few small, bounded HTTP(S) bases, submit them in a separate namespace
and use a finite node HTTP deadline. Inspect their links/scope before retrieval;
this crawler has no robots/politeness system. An interrupted follow is not cancellation,
and a transport failure is not a completed partial crawl. Public content, availability,
TLS and network paths can change, so do not encode public totals as tests. Compare
policies/results with a classmate when available; peer comparison is advice, not a
substitute for the protocol tests. Do not publish URLs containing private queries.

Supplementary observations on **2026-10-03**, using a fresh isolated namespace,
one host node, a 10-second HTTP deadline and a 30-second follow bound per job:

| Base | Observed files | Extensions | Words |
| --- | ---: | --- | ---: |
| `https://example.com/` | 2 | html 1, js 1 | 27 |
| `https://example.org/` | 2 | html 1, js 1 | 27 |
| `https://httpbin.org/html` | 1 | html 1 | 604 |

All three reached done, new CLI processes read final stats, and nodes drained
normally. These are time-specific smoke observations, **not expected public-site
answers** or a comprehensive HTTPS audit. Peer comparison was not available in
this agent session; the owner can compare notes separately.

## Owner demo and hand-in checklist

This is a release checklist, **not a claim that external actions have occurred**.
Technical final-audit evidence and owner confirmations must be recorded separately.

- [x] Confirm the individual GitHub repository is **private until after the demo**:
  `K-Planky/swarmcrawl` was verified **PRIVATE** during the 2026-10-03 audit.
  Keep it private until the demo and preserve descriptive commits/working history.
- [ ] Arrange another person to help on demo day; share [the runbook](demo/README.md),
  terminal roles, expected output, release timing and safe reset instructions.
- [x] Run the complete quality/real-Redis suite and rehearse from a fresh checkout.
  The [technical audit](#final-technical-audit) passed, including two reset-separated
  rehearsals with three live nodes plus the CLI. Rerun for later code changes.
- [ ] Select the verified grading commit and create the **exact tag `1.0.0`**. Check
  for an existing local/remote tag first; never overwrite/move a shared tag. Tagging
  is an explicit owner-approved release action, not an automatic normal push.
- [ ] Perform the live demo with the helper and several host-run crawler processes.
- [ ] **After** the live demo, make the repository public and verify graders can
  access source, Cargo files/lockfile, Redis setup and README/runbook.
- [ ] Submit the repository URL on the course LMS and retain confirmation.

No crash-recovery challenge, browser rendering, cancellation or politeness system
is required for the baseline; the explicit abort command is a later requested extension. Demo readiness does not imply that the live demo,
visibility change, grading tag or LMS delivery is finished.
