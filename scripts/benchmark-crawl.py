#!/usr/bin/env python3
"""Release-node benchmarks: immutable HTTPS graphs and owned Docker Redis (Linux).

No pip dependencies. Run --help; binaries must be built with --release --locked.
Results contain aggregates only. Never connects to or clears an existing Redis.
"""
import argparse
import asyncio
from collections import Counter
import contextlib
import copy
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import statistics
import subprocess
import tempfile
import time
from types import MappingProxyType
import uuid


CASES = ("latency", "large", "broad", "deep", "mixed")


def fixtures():
    """Each HTML text unit has four words; empty anchors add none."""
    result = {}
    unit = b"<p>alpha beta gamma delta</p>"
    for name, count, units, latency, graph in [
        ("latency", 240, 1, 0.020, "broad"),
        ("large", 120, 32768, 0.020, "broad"),
        ("broad", 400, 1, 0.002, "broad"),
        ("deep", 80, 1, 0.005, "deep"),
        ("mixed", 200, 1, 0.010, "broad"),
    ]:
        routes = {}
        for i in range(count + 1):
            path = f"/{name}/" if i == 0 else f"/{name}/p{i}.html"
            children = range(1, count + 1) if i == 0 and graph == "broad" else (
                [i + 1] if graph == "deep" and i < count else []
            )
            links = b"".join(f"<a href='p{j}.html'></a>".encode() for j in children)
            # All broad children converge on p1, testing duplicate publication.
            if i and graph == "broad":
                links += b"<a href='p1.html#duplicate'></a>"
            if name == "mixed" and i:
                links += f"<img src='asset{i}.png'>".encode()
                routes[f"/{name}/asset{i}.png"] = (b"x" * 1024, "image/png", latency)
            routes[path] = (unit * (units if i else 1) + links, "text/html", latency)
        files = count + 1 + (count if name == "mixed" else 0)
        extensions = {"html": count + 1}
        if name == "mixed":
            extensions["png"] = count
        digest = hashlib.sha256()
        for path, (body, mime, delay) in sorted(routes.items()):
            digest.update(path.encode() + mime.encode() + str(delay).encode() + body)
        result[name] = (MappingProxyType(routes), {
            "files": files, "words": 4 * (1 + count * units),
            "extensions": extensions, "sha256": digest.hexdigest(),
            "latency_s": latency,
        })
    return MappingProxyType(result)


async def command(*args, env=None, deadline=30, expected_code=0):
    process = await asyncio.create_subprocess_exec(
        *map(str, args), env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        out, err = await asyncio.wait_for(process.communicate(), deadline)
    except BaseException:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        await process.wait()
        raise
    if process.returncode != expected_code:
        # Do not include endpoint arguments or raw CLI diagnostics in reports.
        detail = err.decode() if Path(args[0]).name == "swarmcrawl" else ""
        raise RuntimeError(f"{Path(args[0]).name} failed with exit {process.returncode}: {detail}")
    return out.decode()


class Fixture:
    def __init__(self, routes):
        self.routes = routes
        self.counts = Counter()
        self.handlers = set()
        self.connections = 0
        self.errors = []

    async def serve(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        self.connections += 1
        writer.get_extra_info("socket").setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            while True:
                try:
                    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 30)
                except asyncio.IncompleteReadError:
                    break
                method, path, version = head.split(b"\r\n", 1)[0].decode().split()
                if method != "GET" or version != "HTTP/1.1" or path not in self.routes:
                    self.errors.append("unexpected request")
                    break
                self.counts[path] += 1
                body, mime, delay = self.routes[path]
                await asyncio.sleep(delay)  # controlled transfer latency, not correctness gating
                size = len(body) if mime == "text/html" else 8 * 1024 * 1024
                writer.write((f"HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\n"
                              f"Content-Length: {size}\r\n\r\n").encode() + body)
                await writer.drain()
                if mime != "text/html":
                    # Never send the remaining binary body. Client must abandon it.
                    if await asyncio.wait_for(reader.read(1), 5):
                        self.errors.append("non-HTML body was not abandoned")
                    break
        except (ConnectionResetError, BrokenPipeError, ssl.SSLError):
            pass  # expected unread binary response abandonment
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, ssl.SSLError):
                await writer.wait_closed()
            self.handlers.discard(task)

    async def close(self):
        tasks = list(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def read_log(process, lines):
    while line := await asyncio.to_thread(process.stderr.readline):
        if sum(map(len, lines)) + len(line) > 65536:
            raise RuntimeError("node diagnostics exceeded bound")
        lines.append(line.decode())


async def reap(process, deadline=10):
    """wait4 gives per-node CPU, including drain; RSS is sampled after exec."""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        pid, status, usage = os.wait4(process.pid, os.WNOHANG)
        if pid:
            process.returncode = os.waitstatus_to_exitcode(status)
            return usage
        await asyncio.sleep(0.005)
    raise TimeoutError("node drain deadline")


async def run_case(args, work, redis_url, tls, cert, name, routes, expected, nodes, repeat):
    namespace = "swarmcrawl-bench:" + uuid.uuid4().hex
    env = {key: value for key, value in os.environ.items() if not key.startswith("SWARMCRAWL_")}
    env.update(SWARMCRAWL_REDIS_URL=redis_url, SWARMCRAWL_JOB_NAMESPACE=namespace,
               SWARMCRAWL_CONFIG_DIR=str(work / "absent"))
    fixture = Fixture(routes)
    server = await asyncio.start_server(fixture.serve, "127.0.0.1", 0, ssl=tls)
    port = server.sockets[0].getsockname()[1]
    processes, logs, readers = [], [], []
    peaks = [0] * nodes
    sampler = None

    async def sample_memory():
        while True:
            for index, process in enumerate(processes):
                try:
                    status = Path(f"/proc/{process.pid}/status").read_text()
                    for line in status.splitlines():
                        if line.startswith("VmHWM:"):
                            peaks[index] = max(peaks[index], int(line.split()[1]))
                except (FileNotFoundError, ProcessLookupError):
                    pass
            await asyncio.sleep(0.005)
    try:
        for _ in range(nodes):
            log = []
            process = subprocess.Popen([str(args.node), str(cert)], env=env, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            processes.append(process)
            logs.append(log)
            readers.append(asyncio.create_task(read_log(process, log)))
        async def ready():
            while not all(any("ready; polling" in line for line in log) for log in logs):
                if any(reader.done() for reader in readers):
                    raise RuntimeError("node exited before readiness")
                await asyncio.sleep(0.005)
        await asyncio.wait_for(ready(), 10)
        sampler = asyncio.create_task(sample_memory())
        started = time.monotonic()
        output = await command(args.cli, "submit", f"https://127.0.0.1:{port}/{name}/", env=env)
        job = output.split()[1]
        # Redis snapshot via the actual CLI. Polling is part of elapsed uncertainty.
        while True:
            output = await command(args.cli, "status", job, env=env)
            if "  done" in output:
                break
            if "failed" in output or "aborted" in output:
                raise RuntimeError("benchmark job did not succeed")
            if time.monotonic() - started > args.deadline:
                raise TimeoutError("crawl deadline")
            await asyncio.sleep(0.010)
        elapsed = time.monotonic() - started
        output = await command(args.cli, "stats", job, env=env)
        header, *extensions = output.splitlines()
        wanted = f"files: {expected['files']}   extensions: {len(expected['extensions'])}   words: {expected['words']}"
        assert header == wanted, "hand-checked statistics mismatch"
        assert dict((line.split()[0], int(line.split()[1])) for line in extensions) == expected["extensions"]
        for process in processes:
            process.send_signal(signal.SIGINT)
        usage = [await reap(process) for process in processes]
        await asyncio.gather(*readers)
        assert all(process.returncode == 0 for process in processes), "node failed"
        assert not fixture.errors, "fixture protocol error"
        assert fixture.counts == Counter({path: 1 for path in routes}), "missing or duplicate GET"
        metrics = []
        for log in logs:
            reports = [json.loads(line.removeprefix("swarmcrawl_metrics "))
                       for line in log if line.startswith("swarmcrawl_metrics ")]
            assert len(reports) == 1, "expected one aggregate per drained node"
            metrics.extend(reports)
        for metric in metrics:
            assert metric["http"]["peak"] <= 10 and metric["owned"]["peak"] <= 20, "node budget exceeded"
            assert all(metric[gauge]["active"] == 0 for gauge in ("http", "cpu", "owned")), "node did not drain"
        for stage in ("http_admission", "http_transfer", "publication"):
            assert sum(metric[stage]["count"] for metric in metrics) == expected["files"], "missing timing observations"
        row = {
            "variant": args.label, "case": name, "nodes": nodes, "repeat": repeat,
            "elapsed_s": elapsed, "urls_per_s": expected["files"] / elapsed,
            "http_utilization": sum(m["http"]["active_ns"] for m in metrics)
                                / (10 * sum(m["elapsed_ns"] for m in metrics)),
            "cpu_s": sum(u.ru_utime + u.ru_stime for u in usage),
            "sum_peak_rss_kib": sum(peaks),
            "node_peak_rss_kib": peaks,
            "connections": fixture.connections, "oracle": expected, "metrics": metrics,
        }
        print(json.dumps({key: value for key, value in row.items() if key not in ("oracle", "metrics")}), flush=True)
        return row
    except BaseException:
        for log in logs:
            print("".join(log), flush=True)
        raise
    finally:
        if sampler:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
        for process in processes:
            if process.poll() is None:
                process.kill()
                with contextlib.suppress(ChildProcessError):
                    await reap(process)
        await asyncio.gather(*readers, return_exceptions=True)
        for process in processes:
            process.stderr.close()
        server.close()
        await server.wait_closed()
        await fixture.close()
        # This instance is ours. Delete only this run's namespace, never FLUSHDB.
        keys = (await command("docker", "exec", args.container, "redis-cli", "--scan", "--pattern", namespace + ":*")).splitlines()
        for offset in range(0, len(keys), 100):
            await command("docker", "exec", args.container, "redis-cli", "DEL", *keys[offset:offset + 100])


async def verify_default_tls(args, work, redis_url, tls):
    """The ordinary CLI must reject the temporary CA (never globally trusted)."""
    fixture = Fixture({"/tls/": (b"<p>alpha beta gamma delta</p>", "text/html", 0)})
    server = await asyncio.start_server(fixture.serve, "127.0.0.1", 0, ssl=tls)
    env = {key: value for key, value in os.environ.items() if not key.startswith("SWARMCRAWL_")}
    env.update(SWARMCRAWL_REDIS_URL=redis_url,
               SWARMCRAWL_JOB_NAMESPACE="swarmcrawl-bench:" + uuid.uuid4().hex,
               SWARMCRAWL_CONFIG_DIR=str(work / "absent"))
    try:
        port = server.sockets[0].getsockname()[1]
        output = await command(args.cli, "submit", f"https://127.0.0.1:{port}/tls/", env=env)
        job = output.split()[1]
        await command(args.cli, "node", "--fetch-timeout-secs", "3", env=env, deadline=10, expected_code=1)
        status = await command(args.cli, "status", job, env=env, expected_code=1)
        assert "failed (fetch)" in status and not fixture.counts, "default CLI unexpectedly trusted test CA"
    finally:
        server.close()
        await server.wait_closed()
        await fixture.close()


async def main(args):
    variants = [args]
    if args.baseline_node:
        baseline = copy.copy(args)
        baseline.node, baseline.cli, baseline.label = args.baseline_node, args.baseline_cli, "baseline"
        variants.insert(0, baseline)
    for binary in [path for variant in variants for path in (variant.cli, variant.node)]:
        if not binary.is_file():
            raise RuntimeError(f"build release binary first: {binary}")
    if len(variants) > 1 and variants[0].node.read_bytes() == variants[1].node.read_bytes():
        raise RuntimeError("comparison runners are identical; use separate Cargo target directories")
    with tempfile.TemporaryDirectory(prefix="swarmcrawl-bench-") as directory:
        work = Path(directory)
        args.container = "swarmcrawl-bench-" + uuid.uuid4().hex
        for variant in variants:
            variant.container = args.container
        # Generated keys/certificates never enter the repository or system trust store.
        await command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                      "-subj", "/CN=swarmcrawl-benchmark-CA", "-addext", "basicConstraints=critical,CA:TRUE",
                      "-keyout", work / "ca-key.pem", "-out", work / "ca.pem")
        await command("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
                      "-subj", "/CN=swarmcrawl-benchmark", "-keyout", work / "key.pem", "-out", work / "leaf.csr")
        (work / "extensions").write_text("subjectAltName=IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nextendedKeyUsage=serverAuth\n")
        await command("openssl", "x509", "-req", "-in", work / "leaf.csr", "-CA", work / "ca.pem",
                      "-CAkey", work / "ca-key.pem", "-CAcreateserial", "-days", "1",
                      "-extfile", work / "extensions", "-out", work / "cert.pem")
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(work / "cert.pem", work / "key.pem")
        tls.set_alpn_protocols(["http/1.1"])
        cidfile = work / "container.id"
        try:
            await command("docker", "run", "--detach", "--cidfile", cidfile, "--name", args.container,
                          "--publish", "127.0.0.1::6379", "redis:7.4-alpine",
                          "redis-server", "--save", "", "--appendonly", "no", deadline=120)
            mapping = (await command("docker", "port", args.container, "6379/tcp")).strip()
            assert mapping.startswith("127.0.0.1:") and mapping[10:].isdigit()
            redis_url = f"redis://{mapping}/0"
            async def redis_ready():
                while (await command("docker", "exec", args.container, "redis-cli", "ping")).strip() != "PONG":
                    await asyncio.sleep(0.05)
            await asyncio.wait_for(redis_ready(), 10)
            await verify_default_tls(args, work, redis_url, tls)
            data = fixtures()
            rows = []
            # Interleave node counts and alternate comparison order to reduce drift.
            for repeat in range(1, args.repeats + 1):
                for name in args.cases:
                    routes, expected = data[name]
                    for nodes in args.nodes:
                        # Alternate baseline/candidate order to reduce drift bias.
                        for variant in (variants if repeat % 2 else list(reversed(variants))):
                            rows.append(await run_case(variant, work, redis_url, tls, work / "ca.pem",
                                                       name, routes, expected, nodes, repeat))
            report = {
                "label": args.label, "platform": os.uname()._asdict() if hasattr(os.uname(), "_asdict") else list(os.uname()),
                "cpus": os.cpu_count(),
                "node_sha256": {variant.label: hashlib.sha256(variant.node.read_bytes()).hexdigest() for variant in variants},
                "rows": rows, "summary": [],
            }
            for variant in variants:
                for name in args.cases:
                    for nodes in args.nodes:
                        selected = [row for row in rows if row["variant"] == variant.label and row["case"] == name and row["nodes"] == nodes]
                        summary = {"variant": variant.label, "case": name, "nodes": nodes}
                        for field in ["elapsed_s", "urls_per_s", "http_utilization", "cpu_s", "sum_peak_rss_kib"]:
                            values = [row[field] for row in selected]
                            summary[field] = {"median": statistics.median(values), "min": min(values), "max": max(values),
                                              "stdev": statistics.stdev(values) if len(values) > 1 else 0}
                        report["summary"].append(summary)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        finally:
            if cidfile.exists():
                container_id = cidfile.read_text().strip()
                if len(container_id) != 64 or any(c not in "0123456789abcdef" for c in container_id):
                    raise RuntimeError(f"invalid owned container ID; inspect {args.container}")
                await command("docker", "rm", "--force", container_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, default=Path("target/release/swarmcrawl"))
    parser.add_argument("--node", type=Path, default=Path("target/release/examples/benchmark_node"))
    parser.add_argument("--baseline-node", type=Path, help="optional instrumented baseline release runner")
    parser.add_argument("--baseline-cli", type=Path, help="CLI built with the baseline runner")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="candidate")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--nodes", type=int, nargs="+", default=[1, 3])
    parser.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    parser.add_argument("--deadline", type=float, default=60)
    options = parser.parse_args()
    if options.repeats < 2 or any(n < 1 for n in options.nodes) or options.deadline <= 0:
        parser.error("need at least two repeats, positive nodes/deadline")
    if bool(options.baseline_node) != bool(options.baseline_cli):
        parser.error("provide both --baseline-node and --baseline-cli")
    if options.baseline_node and options.label == "baseline":
        parser.error("candidate label must differ from baseline")
    options.cli = options.cli.resolve()
    options.node = options.node.resolve()
    if options.baseline_node:
        options.baseline_node = options.baseline_node.resolve()
        options.baseline_cli = options.baseline_cli.resolve()
    try:
        asyncio.run(main(options))
    except KeyboardInterrupt:
        raise SystemExit(130)
