#!/usr/bin/env python3
"""Loopback-only, dependency-free demo site; see demo/README.md for exact totals."""

import argparse
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading

PAGE_COUNT = 24
HTML = "text/html; charset=utf-8"


def routes():
    """The two independent bases serve the same small, hand-countable graph."""
    links = "\n".join(
        f'<a href="page-{number:02}.html">Page {number:02}</a>'
        for number in range(1, PAGE_COUNT + 1)
    )
    index = Path(__file__).with_name("index.html").read_text(encoding="utf-8")
    index = index.replace("{{pages}}", links).encode("utf-8")
    result = {}
    for base in ("/docs/", "/other/"):
        result[base] = (200, HTML, index, False, None)
        for number in range(1, PAGE_COUNT + 1):
            body = (
                '<p>Leaf ant</p><a href="shared.html#convergence"></a>'
                '<a href="./#cycle"></a>'
            ).encode("utf-8")
            result[f"{base}page-{number:02}.html"] = (200, HTML, body, True, None)
        result[f"{base}shared.html"] = (
            200, HTML, b'<p>Shared leaf</p><a href="./"></a>', False, None
        )
        # Synthetic non-HTML bodies: only successful headers establish existence.
        for suffix, mime in (
            ("asset.JPG", "image/jpeg"),
            ("asset.JPEG", "image/jpeg"),
            ("manual.PDF", "application/pdf"),
            ("data", "application/octet-stream"),
        ):
            result[base + suffix] = (200, mime, b"demo bytes", False, None)
        result[base + "alias"] = (302, "text/plain", b"", False, "page-01.html#alias")
        result[base + "missing"] = (404, "text/plain", b"missing", False, None)
    result["/outside.html"] = (200, HTML, b"<p>Must not be crawled</p>", False, None)
    return result


class DemoServer(ThreadingHTTPServer):
    # Enough accept capacity for three nodes; no per-request process or dependency.
    request_queue_size = 64
    daemon_threads = True

    def __init__(self, port=8000, hold_secs=300):
        super().__init__(("127.0.0.1", port), Handler)
        self.routes = routes()
        self.hold_secs = hold_secs
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.counts = Counter()
        self.held = Counter()
        self.peak_held = 0
        self.unexpected = 0
        self.expired = 0

    def snapshot(self):
        with self.lock:
            return {
                "released": self.release.is_set(),
                "held": sum(self.held.values()),
                "held_by_job": dict(self.held),
                "peak_held": self.peak_held,
                "get_counts": dict(sorted(self.counts.items())),
                "unexpected": self.unexpected,
                "expired": self.expired,
            }


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        # Never echo arbitrary request URLs/queries or HTTP headers.
        pass

    def respond(self, status, mime, body, location=None, gated=False):
        self.send_response(status)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        if location is not None:
            self.send_header("Location", location)
        self.end_headers()
        self.close_connection = True
        if gated:
            # Hold a real response body, not an artificial delay in the crawler.
            self.wfile.write(body[:3])
            self.wfile.flush()
            if not self.server.release.wait(self.server.hold_secs):
                with self.server.lock:
                    self.server.expired += 1
                print("Demo gate expired; reset and release within five minutes.", flush=True)
                return  # Incomplete body: never silently turn a timeout into success.
            body = body[3:]
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/_demo/state":
            body = json.dumps(self.server.snapshot(), sort_keys=True).encode("utf-8")
            self.respond(200, "application/json", body)
            return
        route = self.server.routes.get(self.path)
        if route is None:
            with self.server.lock:
                self.server.unexpected += 1
            self.respond(404, "text/plain", b"unknown demo route\n")
            return
        status, mime, body, gated, location = route
        base = "/docs/" if self.path.startswith("/docs/") else "/other/"
        with self.server.lock:
            self.server.counts[self.path] += 1
            if gated:
                self.server.held[base] += 1
                self.server.peak_held = max(
                    self.server.peak_held, sum(self.server.held.values())
                )
        try:
            self.respond(status, mime, body, location, gated)
        except (BrokenPipeError, ConnectionResetError):
            # A stopped client is not a server crash; the request remains counted.
            pass
        finally:
            if gated:
                with self.server.lock:
                    self.server.held[base] -= 1

    def do_POST(self):
        if self.path != "/_demo/release":
            self.respond(404, "text/plain", b"unknown demo control\n")
            return
        self.server.release.set()  # Idempotent; restart the site to re-arm the gate.
        self.respond(200, "text/plain", b"released\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8000, help="loopback port (default: 8000)")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be in 1-65535")
    try:
        server = DemoServer(args.port)
    except OSError as error:
        parser.exit(1, f"Cannot start demo site: {error}\n")
    print(f"Demo site: http://127.0.0.1:{args.port}/ (gates armed, 300-second limit)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.release.set()
        server.server_close()


if __name__ == "__main__":
    main()
