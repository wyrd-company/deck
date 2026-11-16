"""Behavioral checks for contributor automation."""
import json
import os
from pathlib import Path
import re
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
                        # This includes a real cold Go build while task check
                        # compiles other tools in parallel on a small machine.
                        # Startup gets its own budget; watch cancellation below
                        # must still stop the service within Task's 2 seconds.
                        deadline = time.monotonic() + 300
                        while not ready.exists() and runner.poll() is None and time.monotonic() < deadline:
                            time.sleep(0.02)
                        output.seek(0)
                        self.assertTrue(ready.exists(),
                                        f"service did not start within 300s (task exit: {runner.poll()}):\n"
                                        + output.read())
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


class CheckTaskTests(unittest.TestCase):
    def test_check_serializes_its_subtasks(self):
        with tempfile.TemporaryDirectory(prefix="deck-check-task-") as directory:
            root = Path(directory)
            # Keep the real check task, replacing each expensive subtask with
            # a probe that fails if another check is already running.
            sections = re.split(r"(?m)(?=^  [a-z][a-z0-9:]*:\n)",
                                (ROOT / "Taskfile.yml").read_text())
            for index, section in enumerate(sections):
                header = re.match(r"  ([a-z][a-z0-9:]*):\n", section)
                if header and header[1] != "check":
                    sections[index] = (f"  {header[1]}:\n    cmds:\n"
                                       f"      - python3 probe.py {header[1]}\n")
            (root / "Taskfile.yml").write_text("".join(sections))
            (root / "probe.py").write_text('''import os
from pathlib import Path
import sys
import time
try:
    os.mkdir("running")
except FileExistsError:
    sys.exit("overlapping check tasks")
try:
    with Path("starts").open("a") as log:
        log.write(sys.argv[1] + "\\n")
    time.sleep(0.05)
finally:
    os.rmdir("running")
''')
            result = subprocess.run([shutil.which("task"), "check"], cwd=root,
                                    text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout)
            self.assertGreater(len((root / "starts").read_text().splitlines()), 1)


class FormatTaskTests(unittest.TestCase):
    def check_format(self, source, *, with_gofmt=True):
        with tempfile.TemporaryDirectory(prefix="deck-format-task-") as directory:
            root = Path(directory)
            (root / "Taskfile.yml").write_bytes((ROOT / "Taskfile.yml").read_bytes())
            (root / "probe.go").write_text(source)
            commands = root / "commands"
            commands.mkdir()
            if with_gofmt:
                (commands / "gofmt").symlink_to(shutil.which("gofmt"))
            return subprocess.run([shutil.which("task"), "format:check"], cwd=root,
                                  env=dict(os.environ, PATH=str(commands)), text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)

    def test_format_check_requires_gofmt(self):
        result = self.check_format("package probe\n", with_gofmt=False)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("not found", result.stdout)

    def test_format_check_rejects_unformatted_sources(self):
        result = self.check_format("package probe\nfunc main(){}\n")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("probe.go", result.stdout)

    def test_format_check_accepts_formatted_sources(self):
        result = self.check_format("package probe\n\nfunc main() {}\n")
        self.assertEqual(result.returncode, 0, result.stdout)


class IntentTaskTests(unittest.TestCase):
    def test_intents_target_the_selected_release_unit(self):
        for arguments, unit, bump in (([], "deck", "patch"),
                                      (["UNIT=deck-docs"], "deck-docs", "patch"),
                                      (["UNIT=deck-docs", "BUMP=minor"], "deck-docs", "minor")):
            with self.subTest(unit=unit, bump=bump), tempfile.TemporaryDirectory(prefix="deck-intent-") as directory:
                root = Path(directory)
                for name in ("Taskfile.yml", "go.mod", ".intentional/config.yml", "docs/user/docs.yml"):
                    target = root / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes((ROOT / name).read_bytes())
                result = subprocess.run([shutil.which("task"), "intent:add", *arguments,
                                         'MESSAGE=Explain the reader\'s "next step"'],
                                        cwd=root, text=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout)
                intents = list((root / ".intentional/intents").glob("*.md"))
                self.assertEqual(len(intents), 1, result.stdout)
                intent = intents[0].read_text()
                self.assertIn(f"\n{unit}: {bump}\n", intent)
                self.assertIn('Explain the reader\'s "next step"', intent)


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
