# swarmcrawl

A Rust/Tokio distributed crawler coordinated through one Docker Redis. The host-run
binary is named `crawl`; the Cargo package and library remain `swarmcrawl`.

**Current capability:** development foundation and tested URL/HTML/statistics
library policies. `crawl --help`, `--version`, and `check` work. `check` connects to
Redis and sends a read-only PING; it does not create or delete keys. Crawling, jobs,
`node`, `submit`, `status`, and `stats` are **not implemented yet**.

## Development setup

Prerequisites:

- [Rustup](https://rustup.rs/) and a normal platform C linker/build toolchain.
  `rust-toolchain.toml` pins Rust **1.99.0**, minimal profile, rustfmt and Clippy.
  `Cargo.toml` requires Rust **1.99.0** or newer; the project uses edition 2024.
  Rustup installs the pinned toolchain when invoked in this repository.
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

## Crawl domain policies

These contracts are implemented in the library and tested without network access;
no worker uses them yet. HTTP outcomes/timeouts/decoding and Redis coordination are
still future work.

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
  job scope. Fragment variants deduplicate locally; global deduplication is not
  implemented yet.
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
numeric encoding/decoding remains to be implemented with checked conversions.

## Quality gates and tests

Run all of these for relevant changes (CI uses the same commands):

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
bash scripts/redis-smoke.sh
```

Default tests require no external Redis: domain policy and checked-arithmetic
unit tests, library configuration/redaction checks, a local silent TCP peer for
the check deadline, and actual CLI processes for help/version, validation,
configuration precedence, and credential redaction. The silent peer tests timeout
behavior only, not Redis compatibility.

`tests/domain_policies.rs` combines the domain contracts using two HTML fixtures
and a supplied existence/MIME table: five unique existing files (`html: 2`,
`dat: 1`, `jpg: 1`, `jpeg: 1`) and 21 words (15 + 6). It checks cycles, fragment
duplicates, outside links, a broken link, MIME/suffix disagreement and an
extensionless non-HTML file. This is static domain verification, **not** evidence
of HTTP fetching, Redis deduplication or a working distributed crawler.

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
Docker/Redis verification or distributed crawl correctness. GitHub CI passed for
the development foundation; the workflow runs the same gates for new commits.

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
- `src/urls.rs`: canonical HTTP(S) identity, resolution, scope and path extensions.
- `src/html.rs`: MIME classification, full synchronous link/text extraction.
- `src/stats.rs`: required `WebStats` and validated checked aggregation.
- `src/lib.rs`: testable library boundary; `tests/` contains domain fixtures and
  CLI/Redis integration checks; `.github/workflows/checks.yml` runs the local gates
  on Ubuntu.

Use Rust naming conventions and direct domain-specific modules/functions. Add job
storage, HTTP fetching and node orchestration modules only when they implement
real contracts. Libraries expose typed `Result` errors;
the CLI reports them once at the boundary. There is no logging framework yet;
worker diagnostics must later add safe job/node/operation context without exposing
URL credentials, sensitive query values, or Redis credentials. Add dependencies
only for implemented needs: Clap for command parsing, Tokio for async work/deadlines,
Redis's Tokio support for the current check, `url` for identity/scope and `scraper`
for HTML extraction. Direct `html5ever` access configures scraper's parser with
scripting disabled; it adds no second parser. HTTP dependencies wait for fetching.
Redis default features (including scripts) are off until needed.

Baseline crash recovery, cancellation, robots/politeness, and JavaScript rendering
remain excluded by the assignment. A working connectivity check does not implement
any cluster deduplication, completion, or final-statistics guarantees.
