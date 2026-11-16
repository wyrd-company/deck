#!/usr/bin/env python3
"""Break each relay safety guard in a disposable copy; require its test to fail."""

from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


source = Path(__file__).resolve().parents[1] / "tests" / "acceptance"
probes = [
    ("failed resolution cannot wake", "    check=True,\n)", "    check=False,\n)",
     "test_failed_resolve_does_not_record_even_with_session_output"),
    ("identifier must survive the relay", 'output, identifier["value"]', 'output, "wrong-session"',
     "test_identifier_and_payload_reach_only_the_recorder"),
    ("real t3 must never be selected", '[sys.executable, recorder, output, identifier["value"]]',
     '["t3", output, identifier["value"]]', "test_identifier_and_payload_reach_only_the_recorder"),
]

for label, before, after, test in probes:
    with tempfile.TemporaryDirectory(prefix="deck-acceptance-guard-") as temporary:
        root = Path(temporary)
        for filename in ("relay.py", "t3.py", "test_relay.py"):
            shutil.copyfile(source / filename, root / filename)
        relay = root / "relay.py"
        original = relay.read_text()
        if original.count(before) != 1:
            raise SystemExit(f"FAIL: {label}: mutation anchor changed")
        relay.write_text(original.replace(before, after, 1))
        result = subprocess.run(
            [sys.executable, "-m", "unittest", f"test_relay.RelayTest.{test}"],
            cwd=root, capture_output=True, text=True, timeout=20,
        )
        if result.returncode != 1 or "FAIL:" not in result.stderr or "AssertionError" not in result.stderr:
            raise SystemExit(f"FAIL: {label}: expected assertion did not reject mutation\n{result.stderr}")
        print(f"PASS: {label}: mutation rejected by {test}")

print(f"PASS: {len(probes)}/{len(probes)} relay safety mutations rejected; source unchanged")
