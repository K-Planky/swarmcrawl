# swarmcrawl

A Rust/Tokio distributed crawler coordinated through one Docker Redis. The host-run
binary is named `crawl`; the Cargo package and library remain `swarmcrawl`.

**Current capability:** development foundation only. `crawl --help`, `--version`,
and `check` work. `check` connects to Redis and sends a read-only PING; it does not
create or delete keys. Crawling, jobs, `node`, `submit`, `status`, and `stats` are
**not implemented yet**.

## Development setup

Prerequisites:

- [Rustup](https://rustup.rs/) and a normal platform C linker/build toolchain.
  `rust-toolchain.toml` selects Rust **1.97**, minimal profile, rustfmt and Clippy.
  This release series supports edition 2024; local verification used Rust/Cargo
  **1.97.1**. Rustup installs the selected toolchain when invoked in this repository.
- Docker with a reachable running daemon. Only Redis runs in Docker, not the Rust
  binary. On Windows/WSL, start Docker Desktop and enable integration for the distro;
  a Windows `docker.exe` on PATH alone does not establish a usable Linux daemon.
- Bash and GNU `timeout` (coreutils) for the isolated Redis smoke script.
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

## Start a development Redis

Use an unused local port (6379 below) and an unused container name:

```sh
docker run --detach --rm --name swarmcrawl-redis-dev \
  --publish 127.0.0.1:6379:6379 \
  redis:7.4-alpine redis-server --save '' --appendonly no

docker exec swarmcrawl-redis-dev redis-cli ping
cargo run --locked -- check
# Once built, the equivalent host command is:
./target/debug/crawl check
```

Expect `PONG` from redis-cli and `Redis connectivity: OK (PONG)` from `crawl`.
If Redis is not ready yet, retry the read-only check; each `crawl check` is bounded
by its configured timeout. It exits nonzero on failure and never prints success
first. This container uses disposable state with persistence disabled. Future job
results need only last until Redis is cleared or shut down, not across shutdown.

Cleanup targets only this development container:

```sh
docker stop swarmcrawl-redis-dev
```

The default published address is loopback, **not all interfaces**. For clients on
other machines, publish on the Redis host's private/trusted interface instead of
`127.0.0.1`, restrict its firewall to the trusted node/CLI hosts, and configure Redis
ACL/passwords through a private configuration file. Point each client at that
host's reachable IP, not its own loopback address, and run `crawl check` from each
host. Do not expose Redis to the public Internet or disable security protections.
The current build supports plain TCP `redis://` only, not TLS (`rediss://`) or Unix
sockets; credentials on plaintext TCP require a trusted isolated network.
Physical multi-host connectivity has not been verified yet.

## Current CLI configuration

Global options may appear before or after `check`. For each setting, precedence is
**explicit flag > environment variable > built-in default**. Values are validated
after selection, so an overridden invalid environment value does not cause failure.

| Option | Environment | Default | Contract |
| --- | --- | --- | --- |
| `--redis-url` | `CRAWL_REDIS_URL` | `redis://127.0.0.1:6379/0` | TCP Redis URL; port 1–65535, nonnegative database, optional username/password |
| `--redis-timeout-secs` | `CRAWL_REDIS_TIMEOUT_SECS` | `5` | Integer 1–60; one deadline for connection setup and PING combined |

Examples with no credentials:

```sh
crawl --redis-url redis://127.0.0.1:6380/0 check
CRAWL_REDIS_URL=redis://127.0.0.1:6380/0 crawl check --redis-timeout-secs 10
```

Use `./target/debug/crawl` unless the binary has been installed or added to PATH.
Prefer an externally supplied environment variable rather than command-line URLs
for credentials (command lines can appear in shell history/process listings).
Never commit actual credentials. Help hides environment values; configuration
Debug output redacts connection information; errors include a safe operation/error
category, not raw URLs, server error messages, or credentials. Success goes to stdout
and errors to stderr. Exit codes: `0` success/help/version, `1` configuration or
connectivity failure, `2` command-line syntax/usage errors.

These are Redis-check settings, **not HTTP-fetch timeouts**. HTTP timeout/status
policies will be defined with the fetcher. The node-wide maximum of ten requests
is a requirement, not an option to raise. Job namespaces/keys will be defined with
job storage; no unused settings or placeholder crawl commands are exposed now.

## Quality gates and tests

Run all of these for relevant changes (CI uses the same commands):

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
bash scripts/redis-smoke.sh
```

Default tests require no external Redis: library configuration/redaction checks,
a local silent TCP peer for the check deadline, and actual CLI processes for
help/version, validation, configuration precedence, and credential redaction.
The silent peer tests timeout behavior only, not Redis compatibility.

The real-Redis integration test is **opt-in** and is ignored by `cargo test`.
`scripts/redis-smoke.sh` starts a fresh Redis 7.4 Alpine container on an automatically
allocated **loopback-only** port, waits for PONG with bounded polling, runs the real
CLI check and ignored integration test, and removes only its own container/temp
metadata on success or failure. Docker startup and Cargo subprocesses have finite
limits. It never calls `FLUSHDB`/`FLUSHALL` or touches another container. Cleanup
failures are reported as failures with the owned container ID for manual removal.

For an already isolated Redis, the equivalent opt-in invocation is:

```sh
CRAWL_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked --test redis_connectivity -- --ignored
```

This test only sends PING; later coordination/process tests will need unique
namespaces and test-owned key cleanup. A default test pass is **not** evidence of
Docker/Redis verification or distributed crawl correctness. The CI workflow is
configured but has not yet been run on GitHub.

Optional shell checks:

```sh
bash -n scripts/redis-smoke.sh
shellcheck scripts/redis-smoke.sh
```

## Source organization and conventions

- `src/main.rs`: Clap presentation, configuration-source selection, exit behavior;
  no Redis schema or network policy lives in CLI parsing.
- `src/config.rs`: validated shared Redis settings and typed, secret-safe errors.
- `src/redis.rs`: async multiplexed Redis connectivity and bounded read-only check.
- `src/lib.rs`: testable library boundary; `tests/` contains CLI/Redis integration
  checks; `.github/workflows/checks.yml` runs the local gates on Ubuntu.

Use Rust naming conventions and direct domain-specific modules/functions. Add URL,
HTML/statistics, job storage, HTTP fetching and node orchestration modules only as
their plan steps implement real contracts. Libraries expose typed `Result` errors;
the CLI reports them once at the boundary. There is no logging framework yet;
worker diagnostics must later add safe job/node/operation context without exposing
URL credentials, sensitive query values, or Redis credentials. Add dependencies
only for implemented needs: Clap for command parsing, Tokio for async work/deadlines,
and Redis's Tokio support for the current check. HTTP/parser dependencies wait for
their owning steps. Redis default features (including scripts) are off until needed.

Baseline crash recovery, cancellation, robots/politeness, and JavaScript rendering
remain excluded by the assignment. A working connectivity check does not implement
any cluster deduplication, completion, or final-statistics guarantees.
