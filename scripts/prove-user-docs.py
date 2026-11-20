#!/usr/bin/env python3
"""Prove the acceptance checks by breaking one input at a time and restoring it."""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def rejected(path, replacement, command, expected):
    original = path.read_bytes()
    try:
        path.write_bytes(replacement(original))
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        assert result.returncode != 0, f"Mutation survived: {path}"
        assert expected in result.stdout + result.stderr, result.stdout + result.stderr
        print(f"PASS: {path.relative_to(ROOT)} mutation rejected ({expected})")
    finally:
        path.write_bytes(original)


generate = [sys.executable, "scripts/user-docs.py", "--check"]
check = [sys.executable, "scripts/check-user-docs.py"]
rejected(ROOT / "docs/specifications/assets/events/activity.schema.json",
         lambda data: data.replace(b'"title":', b'"description": "drift", "title":', 1),
         generate, "activity-reference.md")
rejected(ROOT / "docs/user/generated-files.json",
         lambda data: data.replace(b"[", b'["obsolete-reference.md",', 1),
         generate, "obsolete-reference.md")
rejected(ROOT / "docs/user/offline-tutorial.md",
         lambda data: data + b"\n[Broken download](examples/absent.json)\n",
         check, "Missing link")
rejected(ROOT / "docs/user/offline-tutorial.md",
         lambda data: data + b"\n[Hidden design](https://github.com/wyrd-company/deck/blob/main/docs/specifications/events.yml)\n",
         check, "Design document exposed")
rejected(ROOT / "docs/user/offline-tutorial.md",
         lambda data: data + b"\n[Broken fragment](events.md#absent-heading)\n",
         check, "Missing fragment")
rejected(ROOT / "docs/user/docs.yml",
         lambda data: data.replace(b"  - offline-tutorial\n", b"", 1),
         check, "Navigation includes every published page")
rejected(ROOT / "docs/user/docs.yml",
         lambda data: data.replace(b"- examples/message.json\n", b"", 1),
         check, "Download not published")
subprocess.run(generate, cwd=ROOT, check=True)
subprocess.run(check, cwd=ROOT, check=True)
print("PASS: 7/7 targeted mutations rejected; restored sources and outputs pass")
