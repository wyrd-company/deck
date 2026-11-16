"""Behavioral checks for contributor automation."""
import os
from pathlib import Path
import shutil
import subprocess
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


if __name__ == "__main__":
    unittest.main()
