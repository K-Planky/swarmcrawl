# swarmcrawl

A distributed web crawler built with Rust and Tokio. Run one or more worker processes, coordinate through Redis, and collect file, extension, and HTML word counts for each crawl.

- Cluster-wide URL deduplication within each job.
- Concurrent jobs, with at most **10 HTTP requests per node** across all jobs.
- Live progress and retained results, accessible from any CLI connected to the same Redis database and namespace.

## Quick start

Requires **Rust 1.99.0** (pinned in `rust-toolchain.toml`) and Docker with Linux containers. Run the following from the repository root on Linux, macOS, or WSL2. Only Redis runs in Docker; nodes run on the host.

### 1. Build

```sh
cargo build --release --locked
export PATH="$PWD/target/release:$PATH"
```

Repeat the `PATH` export in each terminal, or use the binary's full path.

### 2. Start Redis

```sh
swarmcrawl cluster init 127.0.0.1
swarmcrawl check
```

`init` prompts for a password, starts authenticated `redis:7.4-alpine` in Docker, and saves the connection privately for this user. The loopback address keeps Redis local to this machine. Existing saved settings are never overwritten.

### 3. Launch N nodes

Run this in **N separate terminals**, where N ≥ 1:

```sh
swarmcrawl node
```

Nodes stay running and accept new jobs. No leader or direct node-to-node connection is needed.

### 4. Submit and inspect a crawl

```sh
swarmcrawl submit https://books.toscrape.com/
# job 1  input 1  created

swarmcrawl status -f 1
swarmcrawl stats 1
```

Use the ID printed by `submit`, not necessarily `1`. Submission returns immediately; `status -f` prints changes every 500 ms and exits when the job becomes terminal. Public sites can change or fail; crawl only sites you are authorized to access.

### 5. Shut down

Press **Ctrl-C in each node terminal** and wait for it to drain its owned work. Results remain readable while Redis is running.

```sh
swarmcrawl cluster stop --yes
```

**This destroys all jobs and results in the managed Redis instance, across every database and namespace.** `cluster start` starts an empty instance using the retained settings. `cluster remove --yes` also deletes saved settings and credentials; on a joining machine it only forgets the local connection.

## Multiple machines

Build the binary on every machine. On the Redis host, use its trusted LAN or private VPN address **instead of loopback** when initializing:

```sh
swarmcrawl cluster init 192.168.1.10
swarmcrawl node
```

On each additional machine:

```sh
swarmcrawl cluster join 192.168.1.10
swarmcrawl node
```

Replace the example IP with the host's actual address and enter the same password when joining. Docker is needed only on the Redis host. All clients must agree on the port, database, and namespace; use `cluster info` to inspect saved settings. The CLI can run on any joined machine.

**Redis traffic is plaintext.** Use a trusted LAN/private VPN, restrict the Redis port to trusted clients with a firewall, and never expose it publicly. Each node must also be able to reach the target website.

## Commands

| Command                       | Purpose                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------- |
| `node`                        | Run a worker until interrupted.                                                       |
| `submit <url>...`             | Submit one independent job per URL; repeated canonical URLs return the existing ID.   |
| `jobs`                        | List all retained jobs, including terminal jobs.                                      |
| `status <job>`                | Read a coherent progress snapshot.                                                    |
| `status -f <job>`             | Follow until done, failed, or aborted; Ctrl-C stops following, not the job.           |
| `stats <job>`                 | Print final file, extension, and word counts; reject unfinished or unsuccessful jobs. |
| `abort <job>` / `abort --all` | Permanently abort one job or the currently active jobs in this namespace.             |
| `check`                       | Verify Redis connectivity with PING.                                                  |
| `cluster <command>`           | Initialize, join, inspect, start, stop, or remove a deployment.                       |

Aborting stops new claims and discards late results; already-owned HTTP work drains. Resubmitting a done, failed, or aborted job does **not** restart it. Use a new namespace for an independent run. See `swarmcrawl --help` or `swarmcrawl <command> --help` for options.

## Configuration

Connection precedence: **flags → environment → saved settings → defaults**.

| Flag                        | Environment variable            | Default                    |
| --------------------------- | ------------------------------- | -------------------------- |
| `--redis-url`               | `SWARMCRAWL_REDIS_URL`          | `redis://127.0.0.1:6379/0` |
| `--namespace`               | `SWARMCRAWL_JOB_NAMESPACE`      | `swarmcrawl:v1`            |
| `--redis-timeout-secs`      | `SWARMCRAWL_REDIS_TIMEOUT_SECS` | `5` (range 1–60)           |
| `node --fetch-timeout-secs` | `SWARMCRAWL_FETCH_TIMEOUT_SECS` | `30` (range 1–300)         |

Only the connection and namespace are saved. `SWARMCRAWL_CONFIG_DIR` overrides the per-user configuration directory. Prefer password prompts or environment configuration over credentials in command-line arguments. Redis TLS and Unix sockets are not supported.

`cluster` lifecycle commands use saved deployment metadata, not endpoint overrides: unset `SWARMCRAWL_REDIS_URL` before using them. `init` and `join` accept `--port`, `--database`, and `--namespace`.

## How coordination works

The CLI talks only to Redis. Each job has its own base URL, seen set, frontier queue, ownership records, counters, and extension totals. Nodes rotate among active jobs and perform HTTP work in Tokio tasks.

Redis Lua scripts make each state transition atomic:

1. **Submit:** retain one job ID per canonical base URL and enqueue its seed once.
2. **Claim:** move a frontier URL into in-flight ownership before fetching it.
3. **Publish:** verify ownership, add only unseen links to the frontier, merge statistics once, and release the completed claim.
4. **Finish:** mark the job done only when **both frontier and in-flight counts are zero**, after publishing discoveries and statistics. An empty queue alone is not completion.

Progress is read in one Redis transaction. `crawled` counts processed URLs, including broken links and redirects; `files` counts successful files. `frontier` is queued work, `in flight` is claimed work, and `discovered` is the unique URL count. Failed or aborted jobs retain frozen diagnostic counters, not final statistics.

Final `WebStats` are readable as soon as a job is done and have no expiry. Deduplication and atomic publication give the same totals for one or many healthy nodes crawling the same unchanged site.

## Crawl and counting rules

- **Scope:** the seed is also the base. Only HTTP(S) URLs with the same origin and a normalized URL beginning with that base are fetched. Use a trailing slash for a directory boundary: `/docs/` excludes `/docs-extra`, while `/docs` does not. A query in the base is part of the prefix.
- **Identity:** fragments are dropped; host/scheme case, default ports, and dot segments are normalized by the URL parser. Path case, trailing slashes, query values/order, and encoded-path distinctions are preserved. URL credentials are rejected.
- **Discovery:** parse the full HTML document, following hyperlinks and common resource references such as images, stylesheets, scripts, frames, and `srcset`. Relative links honor the first applicable `<base href>` without expanding scope. CSS and JavaScript contents are not searched for URLs.
- **Files:** each successful URL counts once per job, including the seed. Use one GET per claimed URL, not a HEAD/GET pair; non-HTML bodies are not consumed. Most 4xx responses count as no file. Redirects (301/302/303/307/308) do not count as files; in-scope targets enter the deduplicated frontier rather than being followed automatically.
- **HTML:** determined by `Content-Type` (`text/html` or `application/xhtml+xml`), not the extension. HTML bodies are read fully; gzip and declared supported charsets are decoded, with UTF-8 as the default.
- **Words:** lowercase decoded text, split on whitespace at text-node boundaries, and count tokens beginning with ASCII `a-z`. Tags, attributes, comments, and script/style/template text are excluded. Punctuation is otherwise retained; repeated words count repeatedly.
- **Extensions:** use the lowercased final path suffix, ignoring queries. `jpg` and `jpeg` remain different; directories, extensionless names, simple dotfiles, and empty suffixes count as `html`. Percent escapes are not decoded into extension syntax.

`stats` reports `num_files`, `num_exts`, `ext_counts` (sorted for display), and `total_word_count` as a compact text summary.

## Limits and failures

There are no automatic HTTP retries. Timeouts, transport/decoding errors, 408/429, 5xx, and unsupported response statuses fail the job rather than produce partial final results. A node encountering an operational error stops claiming, drains owned work, and exits nonzero; inspect its diagnostics and restart it to serve other jobs.

Crash recovery is not implemented: killing a node or losing Redis can strand work. Graceful shutdown is not crash recovery. There is no JavaScript rendering, `robots.txt` handling, politeness delay, or page/HTML-body size limit. Use a narrow base on trusted targets; the per-node request cap is not a site-wide rate limit.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
bash scripts/redis-smoke.sh
```

The smoke script requires Docker, Bash, and GNU `timeout`; it provisions isolated Redis, runs opt-in Redis/process/distributed tests, and cleans up its own container. `cargo test` alone skips these checks. To also test real managed-cluster setup and teardown:

```sh
cargo test --locked --test cluster_setup -- --ignored
```
