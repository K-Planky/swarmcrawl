# Deterministic live demo

Run from the repository root on Linux/macOS/WSL with **Docker, Rustup, Python 3.10+
(standard library only), curl, Bash and GNU `timeout`**. On macOS, install coreutils
and use its `timeout` command. Only Redis runs in Docker; three `swarmcrawl node`
processes and the demo HTTP server run on the host. No public website is needed.
For build/configuration, coordination and known limitations, see [../README.md](../README.md).

## What this demonstrates

- Submit two independent jobs **before any nodes run**, returning immediately.
- A fragment-variant submission returns the existing ID, not another crawl.
- Three nodes run concurrently across both jobs, each capped at ten requests.
- A held response body keeps a job running; stats cannot appear early.
- Release the bodies, follow to completion, and read exact retained WebStats from
  new CLI processes without restarting or manually finalizing nodes.
- Gracefully stop the nodes and safely discard **only this disposable deployment**.

The demo is a presentation aid, not a replacement for the gated cross-process
correctness suite. `demo/serve.py` binds **127.0.0.1 only**, serves an explicit route
map (never arbitrary filesystem paths), and holds the 24 leaf HTML bodies per job
until an idempotent control POST. Control paths are outside both crawl bases.
The gate has a **300-second limit per request**; release within five minutes of
starting nodes. Expiry closes an incomplete body and fails the crawl, rather than
silently releasing it. Restart the server to re-arm the gate. Do not use this
small development server as an Internet-facing service.

## 1. Build and start fresh Redis (operator terminal A)

Choose unused ports **6380** (Redis) and **8000** (HTTP). Do not reuse a shared
Redis or stop a container you did not start. If a command fails, stop and diagnose;
do not continue with another deployment's state.

```sh
cargo build --locked
# Check for a name collision; this should print no containers:
docker ps -a --filter 'name=^/swarmcrawl-redis-demo$'

# Keep the printed ID in this shell for owned-container cleanup later:
DEMO_CONTAINER=$(docker run --detach --rm --name swarmcrawl-redis-demo \
  --publish 127.0.0.1:6380:6379 \
  redis:7.4-alpine redis-server --save '' --appendonly no)
printf 'Owned demo container: %s\n' "$DEMO_CONTAINER"
```

Copy these **three lines into terminal A, each node terminal, and the observer
terminal**. All clients must share Redis/database/namespace; no credentials are
needed for this loopback-only disposable Redis. Explicit flags override inherited
configuration; no `.env` file is loaded.

```sh
export SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6380/0
export SWARMCRAWL_JOB_NAMESPACE=swarmcrawl:demo:v1
unset SWARMCRAWL_REDIS_TIMEOUT_SECS SWARMCRAWL_FETCH_TIMEOUT_SECS
```

Then, in terminal A:

```sh
docker exec "$DEMO_CONTAINER" redis-cli ping
./target/debug/swarmcrawl check
```

Expect `PONG` and `Redis connectivity: OK (PONG)`. If Docker forwarding is not ready
(occasionally on WSL), retry **only these read-only readiness probes**. Do not
retry failed coordination writes automatically.

## 2. Start the site (terminal B)

```sh
python3 -B demo/serve.py --port 8000
```

Leave this terminal open. Expect `Demo site: http://127.0.0.1:8000/ ... gates armed`.
Use the control endpoint for readiness; **do not open the pages in a browser or
curl the seeds**, which would add non-crawler requests to the request counts.
In terminal A:

```sh
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8000/_demo/state
```

Expect `released: false`, `held: 0`, `get_counts: {}`, `expired: 0` and
`unexpected: 0` in the JSON. If the port is occupied, choose another and change
**all** site/submit/control URLs in this runbook.

## 3. Submit without nodes (terminal A)

```sh
./target/debug/swarmcrawl submit http://127.0.0.1:8000/docs/ http://127.0.0.1:8000/other/
# job 1  input 1  created
# job 2  input 2  created
./target/debug/swarmcrawl submit 'http://127.0.0.1:8000/docs/#again'
# job 1  input 1  existing
./target/debug/swarmcrawl status 1
./target/debug/swarmcrawl status 2
```

A **fresh** namespace has IDs 1 and 2. Otherwise use the printed IDs, or perform
the reset below before repeating. Both snapshots show `crawled 0  frontier 1
in flight 0  files 0  discovered 1  running`. The HTTP request counts remain empty:
submission contacted Redis only. `swarmcrawl stats 1` now exits **1** with a not-done
message and no partial numbers; that failure is expected.

## 4. Start three nodes (terminals C, D and E)

In **each** terminal, from this repository, copy the three configuration lines
above and run:

```sh
./target/debug/swarmcrawl node --fetch-timeout-secs 300
```

Wait for each ready message with a distinct PID. Redis also allocates a fresh
worker identity internally; the ready message does not print that identity.
The enlarged HTTP deadline is for explaining the held-body demo, not a change to
the fixed ten-request node limit. Nodes need no inbound port.

In terminal A, inspect the site and both progress snapshots:

```sh
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8000/_demo/state \
  | python3 -m json.tool
./target/debug/swarmcrawl status 1
./target/debug/swarmcrawl status 2
./target/debug/swarmcrawl stats 1
# Expected nonzero: no final statistics while a body is held.
```

If necessary, repeat the read-only inspection for up to ten seconds. With enough
work ready, the site settles at **held 30**, with nonzero held counts for both
`/docs/` and `/other/`, and `released: false`. Thirty simultaneously held bodies
require all three nodes (at most ten per node); aggregate CLI `in flight` is also
30. Individual job counters/splits are scheduling-dependent: do **not** promise
an exact intermediate split or confuse `crawled` attempts with existing files.

In the helper/observer terminal F, copy the shared configuration and run:

```sh
timeout 240s ./target/debug/swarmcrawl status -f 1
```

The observer prints running progress and waits. It cannot declare done while
bodies are held. This timeout bounds the rehearsal only; normal follow has no
overall deadline. Ctrl-C stops only the observer (exit 130), not the crawl.

## 5. Release, follow and read final statistics (terminal A)

Release **within five minutes**; the helper can operate terminal F while the
owner explains the queue/ownership/completion invariant.

```sh
curl --fail --silent --show-error --max-time 5 --request POST \
  http://127.0.0.1:8000/_demo/release
# released
timeout 30s ./target/debug/swarmcrawl status -f 2
./target/debug/swarmcrawl stats 1
./target/debug/swarmcrawl stats 2
```

Both follow commands exit **0**. Each final snapshot is exactly:

```text
job <ID>  crawled 32  frontier 0  in flight 0  files 30  discovered 32  done
```

Each `stats` invocation, a new Redis-only CLI process, prints:

```text
files: 30   extensions: 4   words: 81
  html 27
  jpeg 1
  jpg 1
  pdf 1
```

Hand calculation, **per job**:

| Successful resources | Files | Words | Extensions |
| --- | ---: | ---: | --- |
| Seed (`index.html` template, served at `/docs/` or `/other/`) | 1 | 31: title 2 + paragraph 2 + 24 Page labels + Alias/Missing/Outside 3 | html |
| Numbered leaf HTML pages | 24 | 48 (2 each) | html |
| Shared converging HTML page | 1 | 2 | html |
| JPG, JPEG, PDF and extensionless binary | 4 | 0 | jpg, jpeg, pdf, html |
| **Total** | **30** | **81** | **4 unique; html 27** |

`alias` redirects to an already-discovered leaf; `missing` returns 404. These two
attempts are not files, giving **32 unique GETs per job**. Cyclic/fragment links
deduplicate. The text of an outside link counts, but its target is not retrieved.
`data` is not HTML by MIME but has no extension, so its extension bucket is `html`.
Numeric/non-ASCII-leading tokens, comments, attributes, script/style text are
excluded. The assets are synthetic tiny bodies; this demo does not test large-file
cancellation (the integration suite does).

Inspect the site again. Expect **64 entries** in `get_counts`, each exactly **1**,
no `/outside.html`, `held: 0`, `peak_held: 30`, `unexpected: 0`, `expired: 0`.
Control requests are not counted. Repeat submission returns the same ID and
already-done `status -f 1` exits immediately. Explain why publication of children,
parent accounting and final stats is one Redis atomic operation, not a local lock.

## 6. Stop, retain, reset and repeat

1. Press **Ctrl-C in C, D and E**, then wait for every node to drain and exit 0.
   After completion there is no owned work, so shutdown is quick. No SIGKILL is
   needed. A mid-crawl stop can take up to the HTTP deadline; release the demo gate
   before stopping all nodes to avoid needless timeouts. Graceful drain is not
   crash recovery.
2. In terminal A, `./target/debug/swarmcrawl stats 1` and repeat `submit` still return
   the same results/ID with **no nodes running**. Redis retains them without TTLs.
3. Press **Ctrl-C in B** to stop the HTTP server. This releases local server waits;
   it is not a crawler recovery mechanism.
4. In terminal A, remove only the container ID captured in step 1:

   ```sh
   docker stop "$DEMO_CONTAINER"
   ```

   `--rm` removes this owned disposable container and all **its** data. Do not use
   `FLUSHDB`, `FLUSHALL`, broad key deletion or another person's Redis/container.
5. Repeat steps 1–5: new Redis resets IDs/results, and a restarted site resets
   gates/counts. Merely changing namespace or resubmitting a retained URL is not a
   full demo reset. Do not reset Redis while any node uses it.

## Troubleshooting and checks

- Missing ready messages: verify Docker daemon, port collisions, shared Redis
  URL/database/namespace and `swarmcrawl check` in each client shell.
- `held` below 30: ensure three live nodes, no stale jobs and no inherited short
  timeout. Read all node errors. Do not remove live ownership or add retries.
- A failed job never returns final stats or restarts on resubmission. If the gate
  expired or infrastructure failed, stop/drain all processes and reset this
  disposable demo. Fix the cause rather than presenting partial totals as success.
- Duplicate request counts: check for browser/curl seed visits, repeat runs without
  site reset, or a real bug. The source tests prove cluster deduplication; this
  manual counter should agree in a fresh run.
- The supplied site is loopback-only. Physical multi-host demos require a reachable
  target and secured Redis networking; see the main README. They have not been
  rehearsed on separate machines. Non-Unix shutdown has not been verified.

Fixture checks (no Docker or public Internet required):

```sh
python3 -B -m unittest discover -s demo -p 'test_*.py'
cargo test --locked --test demo_policies
```

Run the complete quality/real-Redis suite from the main README for technical
verification. Arrange the demo helper, keep GitHub private until after the live
demo, and complete the [owner hand-in checklist](../README.md#owner-demo-and-hand-in-checklist)
separately; this runbook does not perform release actions.
