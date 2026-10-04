# Local demo

A repeatable, offline demonstration of two crawl jobs shared by three nodes. The fixture includes duplicate links, cycles, redirects, a broken link, and out-of-scope links, with hand-checkable results.

Requires the [main README's build prerequisites](../README.md), plus **Python 3 and curl**. Run everything from the repository root, on **one machine**: the demo server binds only to `127.0.0.1`. Only Redis runs in Docker.

## 1. Prepare the terminals

Build once:

```sh
cargo build --release --locked
```

Run this setup in **every terminal** used below:

```sh
export PATH="$PWD/target/release:$PATH"
export SWARMCRAWL_CONFIG_DIR="$HOME/.config/swarmcrawl-demo"
unset SWARMCRAWL_REDIS_URL SWARMCRAWL_JOB_NAMESPACE SWARMCRAWL_REDIS_TIMEOUT_SECS
```

This keeps demo connection settings separate from your normal configuration. Reserve ports **6380** for Redis and **8000** for HTTP. If demo settings already exist, inspect them with `swarmcrawl cluster info`; do not overwrite or remove a deployment you still need.

## 2. Start Redis and the site

In the CLI terminal:

```sh
swarmcrawl cluster init 127.0.0.1 --port 6380 --namespace demo-three
swarmcrawl check
```

Choose a password when prompted. In a separate server terminal:

```sh
python3 demo/serve.py
```

Wait for the server's startup message. It deliberately holds the bodies of 24 leaf pages per job until you release them, making in-flight work visible without relying on a lucky pause. Do not open crawl pages in a browser or curl them: extra requests change the demo counters.

## 3. Submit before starting nodes

In the CLI terminal:

```sh
swarmcrawl submit http://127.0.0.1:8000/docs/ http://127.0.0.1:8000/other/
swarmcrawl submit 'http://127.0.0.1:8000/docs/#again'
swarmcrawl jobs
```

The first command returns two job IDs immediately. The fragment variant returns the first job's existing ID. Each job initially has one frontier URL and no in-flight work.

The commands below assume IDs **1 and 2**; substitute the printed IDs if different.

## 4. Run three nodes and inspect progress

In **three separate node terminals**, run:

```sh
swarmcrawl node --fetch-timeout-secs 300
```

Wait for all three node-ready messages. Back in the CLI terminal:

```sh
swarmcrawl jobs
swarmcrawl stats 1
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8000/_demo/state | python3 -m json.tool
```

- Both jobs remain running while leaf responses are held. `stats` should fail with an unfinished-job message, not return partial totals.
- The server's `held` and `held_by_job` show blocked responses, with up to **30** across three nodes. Scheduling determines their distribution.
- Held pages later reveal `shared.html`: neither an empty queue nor a temporarily idle node means the job is done.

**Release promptly—well before five minutes after starting nodes.** Both the node timeout and server gate are 300 seconds. Expiry fails the crawl; it does not silently release the pages. Aggregate server counts illustrate concurrency, but do not independently prove each node's ten-request cap.

## 5. Release and read results

```sh
curl --fail --silent --show-error --max-time 5 -X POST http://127.0.0.1:8000/_demo/release
swarmcrawl status -f 1
swarmcrawl status -f 2
swarmcrawl stats 1
swarmcrawl stats 2
```

Each job should finish with **32 crawled, 0 frontier, 0 in flight, 30 files, and 32 discovered**. Both `stats` commands should print:

```text
files: 30   extensions: 4   words: 81
  html 27
  jpeg 1
  jpg 1
  pdf 1
```

The totals are small enough to verify by hand:

| Contribution per job | Files | Words |
| --- | --- | --- |
| Index | 1 | 31 |
| 24 leaf pages | 24 | 48 |
| Shared page | 1 | 2 |
| JPG, JPEG, PDF, and extensionless binary | 4 | 0 |
| Redirect and missing URL | 0 | 0 |
| **Total** | **30** | **81** |

The extensionless binary counts under `html` for extension statistics, but contributes no HTML words. The redirect and 404 explain why 32 URLs are processed but only 30 files count.

Inspect `/_demo/state` again: `get_counts` should contain **64 paths, each requested once**, with no `/outside.html`; `held`, `expired`, and `unexpected` should all be zero. Results remain readable from fresh CLI processes without restarting nodes.

## Repeat or compare with one node

Stop all nodes gracefully, then restart the demo server to reset request counts and re-arm the gate. In the CLI and node terminals, set a **new, matching namespace** after the terminal setup above:

```sh
export SWARMCRAWL_JOB_NAMESPACE=demo-single
```

Repeat steps 3–5 with **one node**. Both jobs should produce the same statistics; at most ten responses can be held at once. Use a fresh namespace for every repeat: resubmitting a retained URL returns its existing job, even if it failed. Namespace changes do not reset the HTTP fixture.

## Clean up

Release held responses first if a run is still active. Press **Ctrl-C in every node terminal** and wait for each node to drain and exit, then stop the Python server with Ctrl-C.

From the CLI terminal configured for this demo:

```sh
swarmcrawl cluster remove --yes
```

**This destroys all jobs/results in the demo's managed Redis instance and removes its saved credentials.** It leaves other deployments alone. Never use `FLUSHALL` or stop Redis while nodes still own work.

## Fixture checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s demo -p 'test_*.py'
cargo test --locked --test demo_policies
```

These check the HTTP controls and hand-counted totals, not distributed correctness. For isolated Redis/process tests, run `bash scripts/redis-smoke.sh` as described in the [main README](../README.md#development).
