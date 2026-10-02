# swarmcrawl

A Rust/Tokio distributed crawler coordinated through one Docker Redis. The host-run
binary is named `crawl`; the Cargo package and library remain `swarmcrawl`.

**Current capability:** development foundation, tested URL/HTML/statistics
policies, bounded async HTTP fetching, and library-level Redis submission,
ownership, atomic publication and validated read contracts. `crawl --help`,
`--version`, and `check` work. `check` sends a read-only Redis PING; it does not
create or delete keys. Node orchestration and the `node`, `submit`, `status`, and
`stats` CLI commands are **not implemented yet**. Library consumers can fetch a
URL or claim/publish work, but no application worker connects those components yet.

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
./target/debug/crawl check
```

Expect `PONG` from redis-cli and `Redis connectivity: OK (PONG)` from `crawl`.
If Redis is not ready yet, retry the read-only check; each `crawl check` is bounded
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
| `--redis-timeout-secs` | `CRAWL_REDIS_TIMEOUT_SECS` | `5` | Integer 1–60; one deadline for check connection+PING; library job storage uses it for connection setup and each operation |

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

These are Redis settings, **not HTTP-fetch timeouts**. The library fetcher has a
separate validated deadline described below; HTTP/node settings are not CLI flags
yet. The node-wide maximum of ten requests is a requirement, not an option to
raise. The library job store uses the same validated `RedisConfig`; no unused CLI
namespace settings or placeholder crawl commands are exposed now.

## Crawl domain policies

These contracts are implemented in the library and tested independently of network
access. The fetcher uses them with real local HTTP responses; application worker
scheduling remains future work. Fetching and Redis coordination are described below.

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
Connecting these interfaces into application nodes is the next integration step.

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
  reporting must add safe job/node context when orchestration is implemented.

## Redis job storage and read contracts

The library exposes `jobs::JobStore::{connect, submit, snapshot, stats}` plus the
frontier operations described below. It uses an async multiplexed connection and
the shared `RedisConfig`, with a deadline for
connection setup and each operation. `submit` accepts a validated `CrawlUrl`; it
returns a `Submission { job, created }` without waiting for any worker. Job IDs
are namespace-local positive decimal integers (`1`, `2`, …), capped at Redis's
signed sequence maximum `9223372036854775807`. They are stable, not random secrets
or URL hashes. IDs can have gaps after detected orphan-key corruption.

The default namespace is **`swarmcrawl:v1`**. All future commands/nodes must share
that namespace and Redis database. `connect_in_namespace` allows isolated library
tests/deployments (1–128 ASCII letters, digits, `:`, `_`, `-`); it is not a CLI
option. Use exclusive, nonoverlapping namespaces. Schema v1 is strict, with no
migration or automatic reset of incompatible/corrupt data. Redis Cluster is not
supported; coordination uses the assignment's single Redis instance.

With prefix `P` and job ID `J`:

| Key | Redis type | Meaning |
| --- | --- | --- |
| `P:submissions` | hash | canonical base URL → retained job ID |
| `P:next-job-id` | string | signed Redis `INCR` sequence |
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
  this is the future CLI's **crawled** count, not HTML pages or successful files.
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

The worker protocol extends the schema above; bounded HTTP fetching is a separate
component, and application node orchestration remains future work. Its correctness
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
  IDs are caller-selected 1–128 ASCII letters/digits/underscores/hyphens; nodes
  must choose a fresh identity at startup and share the same Redis/database.
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
a completed crawl. This is a Redis protocol proof/test boundary, not evidence of
an end-to-end distributed crawler. HTTP fetching is separately tested below;
application-node integration still needs process-level verification.

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
validation, configuration precedence, and credential redaction. The silent peer tests
timeout behavior only, not Redis compatibility. Fetcher tests use scripted loopback
HTTP sockets with no external website, Redis or Docker prerequisite.

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
allocated **loopback-only** port, waits for PONG with bounded polling, runs the real
CLI check and ignored connectivity/job/frontier tests, and removes only its own
container/temp metadata on success or failure. Docker startup and Cargo
subprocesses have finite limits. It never calls `FLUSHDB`/`FLUSHALL` or touches
another container. Cleanup failures are reported as failures with the owned
container ID for manual removal.

For an already isolated Redis, the equivalent opt-in invocation is:

```sh
CRAWL_REDIS_URL=redis://127.0.0.1:6379/0 \
  cargo test --locked --test redis_connectivity --test redis_jobs --test redis_frontier -- --ignored
```

The connectivity test only sends PING. `tests/support/mod.rs` shares an isolated
namespace/cleanup harness for both Redis suites, deleting only test-owned keys
after success or assertion failure. Seven tests in `tests/redis_jobs.rs` check:
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
These use **mock page outcomes**, not an HTTP server or application node binary;
cluster-wide HTTP at-most-once retrieval, application-node concurrency and full CLI
evidence remain future integration work. The local fetcher suite is separate.
A default test pass is **not** evidence of Docker/Redis or distributed crawl
verification. CI runs the same gates for new commits.

Optional shell checks:

```sh
bash -n scripts/redis-smoke.sh
shellcheck scripts/redis-smoke.sh
```

## Source organization and conventions

- `src/main.rs`: Clap presentation, configuration-source selection, exit behavior;
  no Redis schema or network policy lives in CLI parsing.
- `src/config.rs`: validated Redis/HTTP deadlines and typed, secret-safe errors.
- `src/fetch.rs`: shared async client/request budget, scope-safe GET classification,
  explicit redirect discoveries, complete HTML decoding and operational errors.
- `src/redis.rs`: async multiplexed Redis connectivity and bounded read-only check.
- `src/jobs.rs`, `src/jobs/submit.lua`: typed job IDs/states, bounded async Redis
  storage, atomic submission/seed initialization and transactional validated reads.
- `src/jobs/frontier.rs`, `src/jobs/{protocol,claim,complete}.lua`: typed ownership
  API, scope filtering, preflight/exact decimal arithmetic, and atomic claiming,
  publication, failure and finalization.
- `src/urls.rs`: canonical HTTP(S) identity, resolution, scope and path extensions.
- `src/html.rs`: MIME classification, full synchronous link/text extraction.
- `src/stats.rs`: required `WebStats` and validated checked aggregation.
- `src/lib.rs`: testable library boundary; `tests/` contains domain fixtures and
  CLI/Redis integration checks; `.github/workflows/checks.yml` runs the local gates
  on Ubuntu.

Use Rust naming conventions and direct domain-specific modules/functions. Add node
orchestration modules only when they implement real contracts.
Libraries expose typed `Result` errors; the CLI reports them once at the boundary.
There is no logging framework yet; worker diagnostics must later add safe job/node/operation context without exposing
URL credentials, sensitive query values, or Redis credentials. Add dependencies
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
