#!/usr/bin/env python3
"""Bounded Unix PTY checks of real CLI prompts; fake Docker tests failure contracts only."""
import contextlib
import errno
import json
import os
from pathlib import Path
import pty
import select
import signal
import sys
import tempfile
import termios
import time


class Terminal:
    def __init__(self, binary, args, env):
        self.pid, self.fd = pty.fork()
        if self.pid == 0:
            os.execve(binary, [binary, *args], env)
        self.deadline = time.monotonic() + 10
        self.transcript = b""
        self.reaped = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if not self.reaped:
            with contextlib.suppress(ProcessLookupError):
                os.kill(self.pid, signal.SIGKILL)
            os.waitpid(self.pid, 0)
        os.close(self.fd)

    def read(self):
        assert time.monotonic() < self.deadline, "PTY command exceeded 10 seconds"
        if select.select([self.fd], [], [], 0.05)[0]:
            try:
                data = os.read(self.fd, 8192)
            except OSError as error:
                if error.errno != errno.EIO:
                    raise
                return False
            self.transcript += data
            assert len(self.transcript) < 65536, "unexpectedly large terminal output"
            return bool(data)
        return True

    def expect(self, message):
        while message not in self.transcript:
            assert self.read(), "terminal closed before expected prompt (output withheld)"

    def send(self, line, hidden=False):
        if hidden:
            # The prompt write can precede disabling echo. Wait on the actual
            # terminal state, rather than sleeping and hoping it is ready.
            while termios.tcgetattr(self.fd)[3] & termios.ECHO:
                assert time.monotonic() < self.deadline, "password echo was not disabled"
                time.sleep(0.001)
        os.write(self.fd, line + b"\n")

    def finish(self, expected):
        while True:
            self.read()
            pid, status = os.waitpid(self.pid, os.WNOHANG)
            if pid:
                self.reaped = True
                # Drain remaining buffered output without an unbounded wait.
                while select.select([self.fd], [], [], 0)[0] and self.read():
                    pass
                assert os.waitstatus_to_exitcode(status) == expected, "unexpected CLI exit (output withheld)"
                return self.transcript


def main(binary):
    with tempfile.TemporaryDirectory(prefix="swarmcrawl-pty-") as root:
        root = Path(root)
        config = root / "config"
        log = root / "docker-events"
        bin_dir = root / "bin"
        bin_dir.mkdir()
        fake = bin_dir / "docker"
        fake.write_text(f"#!{sys.executable}\n" + '''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if args[0] == "info":
    print("linux")
elif args[:2] == ["container", "ls"]:
    if os.environ.get("FAKE_EXISTS") == "1": print("a" * 64)
elif args[:2] == ["container", "inspect"]:
    if "Labels" in args[3]:
        saved = json.loads((Path(os.environ["SWARMCRAWL_CONFIG_DIR"]) / "config.json").read_text())
        print(json.dumps({"org.swarmcrawl.owner": saved["owned"]["token"], "org.swarmcrawl.managed": "v1"}))
    else: print("true")
elif args[0] == "create":
    sys.exit(1)  # Exercise incomplete setup, not fake provisioning success.
elif args[0] in ["stop", "rm"]:
    with open(os.environ["FAKE_LOG"], "a") as log: log.write(args[0] + "\\n")
else:
    sys.exit(1)
''')
        fake.chmod(0o700)
        env = {key: value for key, value in os.environ.items() if not key.startswith("SWARMCRAWL_")}
        env.update(SWARMCRAWL_CONFIG_DIR=str(config), PATH=str(bin_dir), FAKE_LOG=str(log), FAKE_EXISTS="0")
        password = b"terminal-fixture-:quoted\\password"

        # Required IP is a parser contract even on a real terminal; no menu/default.
        with Terminal(binary, ["cluster", "init"], env) as terminal:
            terminal.finish(2)
        assert not (config / "config.json").exists()

        # Confirmation mismatch must neither save a credential nor create Redis.
        with Terminal(binary, ["cluster", "init", "127.0.0.1"], env) as terminal:
            terminal.expect(b"Redis password (hidden):")
            terminal.send(password, hidden=True)
            terminal.expect(b"Confirm Redis password (hidden):")
            terminal.send(b"different-fixture", hidden=True)
            transcript = terminal.finish(1)
            assert b"passwords do not match" in transcript
            assert password not in transcript and b"different-fixture" not in transcript
        assert not (config / "config.json").exists()

        with Terminal(binary, ["cluster", "init", "127.0.0.1"], env) as terminal:
            terminal.expect(b"Redis password (hidden):")
            terminal.send(password, hidden=True)
            terminal.expect(b"Confirm Redis password (hidden):")
            terminal.send(password, hidden=True)
            transcript = terminal.finish(1)
            assert b"Setup incomplete" in transcript
            assert password not in transcript
        assert (config / "config.json").exists()
        before = (config / "config.json").read_bytes()
        env["FAKE_EXISTS"] = "1"

        # Declining either destructive prompt performs no Docker writes or cleanup.
        for action in ["stop", "remove"]:
            with Terminal(binary, ["cluster", action], env) as terminal:
                terminal.expect(f"Type '{action}'".encode())
                terminal.send(b"no")
                transcript = terminal.finish(1)
                assert f"{action} canceled".encode() in transcript
            assert (config / "config.json").read_bytes() == before
            assert not log.exists()
        with Terminal(binary, ["cluster", "stop"], env) as terminal:
            terminal.expect(b"Type 'stop'")
            terminal.send(b"stop")
            terminal.finish(0)
        assert (config / "config.json").read_bytes() == before
        assert log.read_text() == "stop\nrm\n"
        env["FAKE_EXISTS"] = "0"
        with Terminal(binary, ["cluster", "remove"], env) as terminal:
            terminal.expect(b"Type 'remove'")
            terminal.send(b"remove")
            terminal.finish(0)
        assert sorted(path.name for path in config.iterdir()) == ["deployment.lock"]

        # Hidden join prompt is also checked; failure cannot save a connection.
        with Terminal(binary, ["cluster", "join", "127.0.0.1", "--port", "1"], env) as terminal:
            terminal.expect(b"Redis password (hidden):")
            terminal.send(password, hidden=True)
            transcript = terminal.finish(1)
            assert password not in transcript
            assert b"Redis connect failed" in transcript
        assert not (config / "config.json").exists()
    print("PTY checks passed: explicit IP, hidden passwords, confirmation mismatch, stop/remove confirmation and no-backup cleanup")


if __name__ == "__main__":
    main(str(Path(sys.argv[1]).resolve()))
