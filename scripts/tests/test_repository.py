"""Behavioral checks for contributor automation."""
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]


class DevelopmentTaskTests(unittest.TestCase):
    def test_dev_starts_again_after_a_successful_stop(self):
        with tempfile.TemporaryDirectory(prefix="deck-dev-task-") as directory:
            root = Path(directory)
            (root / "Taskfile.yml").write_bytes((ROOT / "Taskfile.yml").read_bytes())
            (root / "main.go").write_text("package main\n")
            command_dir = root / "commands"
            command_dir.mkdir()
            stand_in = command_dir / "go"
            stand_in.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$DEV_PROBE_BUILDS"\n')
            stand_in.chmod(0o755)
            (root / "bin").mkdir()
            service = root / "bin/deck"
            service.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$DEV_PROBE_LOG"\n')
            service.chmod(0o755)
            log = root / "starts"
            builds = root / "builds"
            environment = dict(os.environ, PATH=str(command_dir) + os.pathsep + os.environ["PATH"],
                               DEV_PROBE_LOG=str(log), DEV_PROBE_BUILDS=str(builds))
            command = [shutil.which("task"), "dev", "CONFIG=service config.yml"]
            for _ in range(2):
                result = subprocess.run(command, cwd=root, env=environment, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout)
            self.assertEqual(log.read_text().splitlines(),
                             ["service start --config service config.yml"] * 2)
            self.assertEqual(len(builds.read_text().splitlines()), 2)
            self.assertTrue(all(line.startswith("build ") for line in builds.read_text().splitlines()))
            self.assertFalse((root / ".task").exists(), "dev must not record an up-to-date cache")

    @unittest.skipUnless(os.name == "posix", "Task watch cancellation uses Unix signals")
    def test_watch_cancellation_stops_the_service_before_restart(self):
        # Exercise the real Taskfile/build with a port-binding service. Apply
        # Task's watch cancellation (SIGINT, then SIGKILL) to its immediate
        # command, without requiring an available inotify watcher in CI.
        with tempfile.TemporaryDirectory(prefix="deck-dev-cancel-") as directory:
            root = Path(directory)
            (root / "Taskfile.yml").write_bytes((ROOT / "Taskfile.yml").read_bytes())
            (root / "go.mod").write_text("module devprobe\n\ngo 1.24.0\n")
            (root / "cmd/deck").mkdir(parents=True)
            (root / "cmd/deck/main.go").write_text(r'''package main
import (
    "encoding/json"
    "net"
    "os"
    "os/signal"
)
func main() {
    interrupts := make(chan os.Signal, 1)
    signal.Notify(interrupts, os.Interrupt)
    listener, err := net.Listen("tcp", os.Getenv("DEV_PROBE_ADDRESS"))
    if err != nil { panic(err) }
    state, _ := json.Marshal(map[string]any{
        "pid": os.Getpid(), "parent": os.Getppid(), "address": listener.Addr().String(),
    })
    ready := os.Getenv("DEV_PROBE_READY")
    if err := os.WriteFile(ready+".tmp", state, 0600); err != nil { panic(err) }
    if err := os.Rename(ready+".tmp", ready); err != nil { panic(err) }
    <-interrupts
    listener.Close()
    if err := os.WriteFile(os.Getenv("DEV_PROBE_STOPPED"), []byte("stopped"), 0600); err != nil { panic(err) }
}
''')
            address = "127.0.0.1:0"
            service_pids = []
            for iteration in range(2):
                ready = root / f"ready-{iteration}"
                stopped = root / f"stopped-{iteration}"
                environment = dict(os.environ, DEV_PROBE_ADDRESS=address,
                                   DEV_PROBE_READY=str(ready), DEV_PROBE_STOPPED=str(stopped))
                with (root / f"output-{iteration}").open("w+") as output:
                    runner = subprocess.Popen([shutil.which("task"), "dev", "CONFIG=probe.yml"],
                                              cwd=root, env=environment, stdout=output,
                                              stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        deadline = time.monotonic() + 30
                        while not ready.exists() and runner.poll() is None and time.monotonic() < deadline:
                            time.sleep(0.02)
                        output.seek(0)
                        self.assertTrue(ready.exists(), output.read())
                        state = json.loads(ready.read_text())
                        service_pids.append(state["pid"])
                        address = state["address"]
                        supervised = state["pid"] if state["parent"] == runner.pid else state["parent"]
                        os.kill(supervised, signal.SIGINT)
                        try:
                            runner.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            os.kill(supervised, signal.SIGKILL)
                            runner.wait(timeout=5)
                        self.assertTrue(stopped.exists(), "watch cancellation left the service running")
                        with self.assertRaises(ProcessLookupError):
                            os.kill(state["pid"], 0)
                        host, port = address.rsplit(":", 1)
                        with socket.socket() as listener:
                            listener.bind((host, int(port)))
                    finally:
                        # A regressed go-run child shares this process group;
                        # clean it up even when an assertion or timeout fails.
                        try:
                            os.killpg(runner.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        runner.wait(timeout=5)
            self.assertNotEqual(*service_pids)


class CommitMessageTests(unittest.TestCase):
    def check_header(self, header):
        with tempfile.TemporaryDirectory(prefix="deck-commit-message-") as directory:
            message = Path(directory) / "message"
            message.write_text(header + "\n\nBody does not affect header validation.\n")
            return subprocess.run([sys.executable, str(ROOT / "scripts/check-commit-message.py"),
                                   str(message)], text=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=10)

    def test_conventional_and_git_generated_headers(self):
        for header in (
            "fix(cli): preserve unread activity", "feat!: update the contract",
            "Merge branch 'topic'", "Merge remote-tracking branch 'origin/topic'",
            "Merge tag '1.2.3'", 'Revert "feat: add a command"',
            "fixup! feat: add a command", "squash! feat: add a command",
            "amend! feat: add a command",
        ):
            with self.subTest(header=header):
                result = self.check_header(header)
                self.assertEqual(result.returncode, 0, result.stdout)

    def test_invalid_headers_are_rejected(self):
        for header in ("", "Update stuff", "fix: ", "Merge ", "fixup! ",
                       "squash! ", "amend! ", 'Revert ""', "Revert a change"):
            with self.subTest(header=header):
                result = self.check_header(header)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Use a Conventional Commit", result.stdout)


if __name__ == "__main__":
    unittest.main()
