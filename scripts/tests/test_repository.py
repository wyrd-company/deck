"""Behavioral checks for contributor automation."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
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
            stand_in.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$DEV_PROBE_LOG"\n')
            stand_in.chmod(0o755)
            log = root / "starts"
            environment = dict(os.environ, PATH=str(command_dir) + os.pathsep + os.environ["PATH"],
                               DEV_PROBE_LOG=str(log))
            command = [shutil.which("task"), "dev", "CONFIG=service config.yml"]
            for _ in range(2):
                result = subprocess.run(command, cwd=root, env=environment, text=True,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
                self.assertEqual(result.returncode, 0, result.stdout)
            self.assertEqual(log.read_text().splitlines(),
                             ["run ./cmd/deck service start --config service config.yml"] * 2)
            self.assertFalse((root / ".task").exists(), "dev must not record an up-to-date cache")



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
