"""Standard-library checks for demo controls/body gates, not crawler correctness."""

from http.client import HTTPConnection, IncompleteRead
import json
import threading
import time
import unittest
from unittest.mock import patch

from serve import DemoServer


class DemoSiteTests(unittest.TestCase):
    def setUp(self):
        self.server = DemoServer(port=0, hold_secs=2)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.connections = []

    def tearDown(self):
        self.server.release.set()
        for connection in self.connections:
            connection.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())

    def request(self, method, path):
        connection = HTTPConnection("127.0.0.1", self.server.server_port, timeout=3)
        self.connections.append(connection)
        connection.request(method, path)
        return connection.getresponse()

    def test_body_gate_and_idempotent_release(self):
        response = self.request("GET", "/docs/page-01.html")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(3), b"<p>")
        state_response = self.request("GET", "/_demo/state")
        state = json.loads(state_response.read())
        self.assertEqual(state["held"], 1)
        self.assertEqual(state["held_by_job"], {"/docs/": 1})
        self.assertFalse(state["released"])
        for _ in range(2):
            release = self.request("POST", "/_demo/release")
            self.assertEqual((release.status, release.read()), (200, b"released\n"))
        self.assertEqual(
            b"<p>" + response.read(),
            b'<p>Leaf ant</p><a href="shared.html#convergence"></a><a href="./#cycle"></a>',
        )
        deadline = time.monotonic() + 2
        while self.server.snapshot()["held"] and time.monotonic() < deadline:
            time.sleep(0.005)
        state = self.server.snapshot()
        self.assertEqual((state["held"], state["peak_held"], state["expired"]), (0, 1, 0))
        self.assertEqual(state["get_counts"], {"/docs/page-01.html": 1})

    def test_expired_gate_closes_an_incomplete_body_without_successful_release(self):
        self.server.hold_secs = 0.05
        with patch("builtins.print") as printed:
            response = self.request("GET", "/other/page-24.html")
            with self.assertRaises(IncompleteRead):
                response.read()
            printed.assert_called_once()
        state = self.server.snapshot()
        self.assertFalse(state["released"])
        self.assertEqual((state["held"], state["expired"]), (0, 1))

    def test_two_graphs_redirect_missing_and_mime(self):
        for base in ("/docs/", "/other/"):
            response = self.request("GET", base)
            self.assertEqual(response.status, 200)
            source = response.read().decode("utf-8")
            self.assertNotIn("{{pages}}", source)
            self.assertEqual(source.count(">Page "), 24)
            response = self.request("GET", base + "alias")
            self.assertEqual(response.status, 302)
            self.assertEqual(response.getheader("Location"), "page-01.html#alias")
            response.read()
            response = self.request("GET", base + "missing")
            self.assertEqual(response.status, 404)
            response.read()
            response = self.request("GET", base + "data")
            self.assertEqual(response.getheader("Content-Type"), "application/octet-stream")
            self.assertEqual(response.read(), b"demo bytes")
        # Controls are outside both bases; no filesystem fallback or query aliases.
        response = self.request("GET", "/docs/../../Cargo.toml?private=value")
        self.assertEqual(response.status, 404)
        response.read()
        self.assertEqual(self.server.snapshot()["unexpected"], 1)
        self.assertNotIn("private", json.dumps(self.server.snapshot()))


if __name__ == "__main__":
    unittest.main()
