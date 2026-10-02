# swarmcrawl

A Rust/Tokio distributed crawler coordinated through one Docker Redis. The host-run
binary, Cargo package and library are all named `swarmcrawl`; environment settings
use `SWARMCRAWL_*`. There are no legacy executable or configuration aliases.

**Current capability:** `swarmcrawl node`, `submit`, `status`/`status -f`, and `stats`
work against the shared Redis job protocol. Host-run nodes concurrently
claim/fetch/publish work, stay available for later submissions, and drain owned
work on graceful shutdown. User commands talk only to Redis; finished statistics
are immediately readable by a new CLI process and retained after nodes exit.
`swarmcrawl --help`, `--version`, and the read-only Redis `check` also work. Domain,
HTTP, Redis and CLI/node process tests pass, including adversarial traversal with
identical hand-checked totals for **1, 2 and 3 participating nodes** and concurrent
overlapping jobs. A [deterministic live-demo runbook](demo/README.md) supplies two
local jobs, exact expected totals, three-node operation and owned-state reset.
Final clean-checkout auditing and owner-controlled demo/release actions are separate.

## Start here

1. Follow [development setup](#development-setup) and [start Redis](#start-a-development-redis).
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

The default published address is loopback, **not all interfaces**. For clients on
other machines, publish on the Redis host's private/trusted interface instead of
`127.0.0.1`, restrict its firewall to the trusted node/CLI hosts, and configure Redis
ACL/passwords through a private configuration file. Point each client at that
host's reachable IP, not its own loopback address, and run `swarmcrawl check` from each
host. Do not expose Redis to the public Internet or disable security protections.
The current build supports plain TCP `redis://` only, not TLS (`rediss://`) or Unix
sockets; credentials on plaintext TCP require a trusted isolated network.
Physical multi-host connectivity has not been verified yet. For an actual multi-host
setup:

1. On the Redis host, keep its ACL/password configuration **outside the repository**
   with restrictive file permissions. Use a Docker bind mount for that configuration
   and start `redis-server` with its mounted path; do not put passwords on the command
   line. Publish `TRUSTED_REDIS_IP:6379:6379`, where the IP belongs to a private host
   interface, rather than the loopback binding in the development command.
2. Allow TCP 6379 through that host's firewall **only from the intended node/CLI
   hosts**. Keep Redis protected mode/authentication enabled. A namespace is state
   isolation, not access control; every Redis client is trusted.
3. Build the same revision on each node/CLI host. Supply `SWARMCRAWL_REDIS_URL`
   externally with the Redis host's reachable address and configured authentication.
   Use the same Redis database and `SWARMCRAWL_JOB_NAMESPACE` on every client;
   verify `swarmcrawl check` from each machine before starting host-run nodes.
4. Submit a target reachable from **every node**, not a node's loopback HTTP address.
   Redis hosts do not need access to the website, and nodes need no inbound crawler
   port. The supplied demo server intentionally cannot serve other hosts.

Do not change repository visibility or claim physical multi-host verification just
because multiple processes on one host pass.

## CLI configuration

Global options may appear before or after any subcommand. Precedence is
**explicit flag > environment variable > built-in default**. Values are validated
after selection, so an overridden invalid environment value does not cause failure.
Only `SWARMCRAWL_*` environment settings are recognized. The former `CRAWL_*`
settings are ignored, not compatibility aliases; update shell/service exports.

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
Prefer an externally supplied environment variable rather than command-line URLs
for credentials (command lines can appear in shell history/process listings).
Never commit actual credentials. Help hides environment values; configuration
Debug output redacts connection information; errors include a safe operation/error
category, not raw URLs, server error messages, or credentials. Success goes to stdout
and errors to stderr. Exit codes: `0` success/help/version, `1` configuration or
runtime failure, `2` command-line syntax/usage errors, `130` interrupted status
follow. A failed-job status prints its diagnostic snapshot then exits `1`; failed
or unfinished stats exit `1` without partial numbers. Nodes report safe PID/job/
operation context to stderr and never log base URLs, request queries or Redis
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
  completion or failure; they never restart a job. URLs are not printed because
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
  crawled + frontier + in flight. State is `running`, `done`, or `failed` with a safe
  failure category. No URL/query appears in the snapshot. Running/done snapshots
  exit `0`; failed snapshots exit `1` with a final-statistics-unavailable error.
- **`status -f <job>` / `status --follow <job>`:** print an immediate snapshot, then
  poll **500 ms after each bounded Redis read** and print only changed snapshots.
  Intermediate changes between polls may be coalesced. Exit `0` at done or `1` at
  failed/Redis error/unknown ID; an already-terminal job prints once and exits.
  Ctrl-C while following exits `130` with an interruption message. It stops only
  this observer, **not the crawl**, and performs no job writes. There is no overall
  follow deadline: a queued job without nodes keeps waiting until interrupted.
- **`stats <job>`:** read final validated `WebStats` only when done. Print decimal
  `files`, `extensions`, and `words`, then one extension/count per line in sorted
  extension order. For example, `files: 3   extensions: 2   words: 4` followed by
  `html 2` and `jpg 1`. Zero-file completed jobs are valid. Full unsigned word totals
  remain exact, not floating-point. Running, failed, unknown, or corrupt jobs exit
  `1` without printing partial totals. A new CLI can read results immediately when
  done, with no node restart/manual finalization; results remain after nodes exit.

Job IDs are positive canonical decimal integers, local to a Redis database and
namespace. Invalid IDs are rejected before connecting; unknown IDs suggest checking
that configuration. Redis and CLI output errors are nonzero, not success messages.
Only the `node` command creates an HTTP fetcher or executes crawler work. There is
no direct CLI-to-node connection and no job cancellation/reset command.

## How the components fit together

```text
CLI submit/status/stats ──────► one Redis (Docker)
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

The library exposes `jobs::JobStore::{connect, submit, snapshot, stats}` plus the
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
base returns its retained ID, including for a completed or failed job, without
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
- `state`: `running`, `done`, or `failed` with a fixed `fetch`, `statistics`, or
  `protocol` failure category. Running requires outstanding work; done requires
  an empty frontier and no owners. Failed may retain outstanding diagnostic state
  and can never supply final statistics. Failure freezes outstanding diagnostic
  state; future nodes must stop claiming that job and drain their network tasks.

Submission starts at `(discovered, processed, frontier, in_flight, successful_files)
= (1, 0, 1, 0, 0)`. Unknown jobs report an explicit error. Invalid metadata, key
shapes, canonical URLs, state combinations or statistics are rejected with safe
operation/field categories, not raw Redis error messages or stored values.
All stored counters/totals are canonical unsigned decimal strings: no negative,
signed, padded, fractional or exponential forms. Reads preserve the target's full
`usize` and `u64` ranges, including words above `i64::MAX`, and reject overflow.

`stats` returns validated `WebStats` **only for done jobs**; running and failed jobs
return distinct errors rather than partial statistics. Its state/result reads are
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
5. Done/failed states never return to running. Failures retain diagnostic state,
   remove active membership and never expose partial aggregates as final results.
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

Publication returns `Published { done }`, `AlreadyCompleted`, or `Failed(reason)`.
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

Run all of these for relevant changes (CI uses the same commands):

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
bash scripts/redis-smoke.sh
```

Default tests require no external Redis: domain policy and checked-arithmetic
unit tests, library configuration/redaction checks, local silent TCP peers for
the check/job-connection deadlines, and actual CLI processes for help/version,
validation, configuration precedence, ignored legacy environment names, and credential
redaction. Silent peers test timeout behavior only, not Redis compatibility.
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
test-owned keys after success or assertion failure. Seven tests in `tests/redis_jobs.rs` check:
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

Seven tests in `tests/redis_frontier.rs` check 32 concurrent parent discoveries;
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

`tests/cli_jobs.rs` adds eight opt-in cases using independent real user-command
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

## Source organization and conventions

- `src/main.rs`: Clap presentation, shared configuration-source selection, command
  dispatch and exit behavior; no Redis schema or network policy in CLI parsing.
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
- `src/jobs/frontier.rs`, `src/jobs/{worker,protocol,claim,complete}.lua`: fresh
  process identities, typed ownership, scope filtering, preflight/exact decimal
  arithmetic, and atomic claiming,
  publication, failure and finalization.
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

Baseline crash recovery, cancellation, robots/politeness, and JavaScript rendering
remain excluded by the assignment. A working connectivity check alone does not
exercise the library's cluster ownership, completion or final-statistics guarantees.

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

- [ ] Confirm the individual GitHub repository is **private until after the demo**;
  keep descriptive commits and the working branch's history intact.
- [ ] Arrange another person to help on demo day; share [the runbook](demo/README.md),
  terminal roles, expected output, release timing and safe reset instructions.
- [ ] Run the complete quality/real-Redis suite and rehearse from a fresh checkout.
  Reset the disposable demo and repeat; use several live nodes plus the CLI.
- [ ] Select the verified grading commit and create the **exact tag `1.0.0`**. Check
  for an existing local/remote tag first; never overwrite/move a shared tag. Tagging
  is an explicit owner-approved release action, not an automatic normal push.
- [ ] Perform the live demo with the helper and several host-run crawler processes.
- [ ] **After** the live demo, make the repository public and verify graders can
  access source, Cargo files/lockfile, Redis setup and README/runbook.
- [ ] Submit the repository URL on the course LMS and retain confirmation.

No crash-recovery challenge, browser rendering, cancellation or politeness system
is required for the baseline. Demo readiness does not imply that the live demo,
visibility change, grading tag or LMS delivery is finished.
