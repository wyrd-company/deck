"""Exercise the real relay/recorder subprocess boundary without Discord."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


HERE = Path(__file__).resolve().parent


class RelayTest(unittest.TestCase):
    def run_relay(self, sessions, exit_code=0):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        deck = root / "deck"
        deck.write_text(
            "#!" + sys.executable + "\n"
            "import json, pathlib, sys\n"
            "assert sys.argv[1:] == ['resolve', '--config', 'client.yml', '--event', '-', '--json']\n"
            "pathlib.Path(__file__).with_name('received.json').write_bytes(sys.stdin.buffer.read())\n"
            f"print({json.dumps(json.dumps({'sessions': sessions}))})\n"
            f"sys.exit({exit_code})\n"
        )
        deck.chmod(0o700)
        # A real t3 selected through PATH would fail loudly and leave evidence.
        trap = root / "t3"
        trap.write_text("#!/bin/sh\ntouch '" + str(root / "real-t3-called") + "'\nexit 99\n")
        trap.chmod(0o700)
        event = {"thread_id": "123", "message": {"content": "$(touch /tmp/unsafe); 'quoted'"}}
        records = root / "records.jsonl"
        completed = subprocess.run(
            [sys.executable, str(HERE / "relay.py"), str(deck), "client.yml",
             str(HERE / "t3.py"), str(records)],
            input=json.dumps(event).encode(), capture_output=True,
            env={**os.environ, "PATH": str(root), "RELAY_SUBJECT": "deck.1.2.123"},
        )
        self.assertFalse((root / "real-t3-called").exists())
        self.assertEqual(json.loads((root / "received.json").read_bytes()), event)
        rows = [json.loads(line) for line in records.read_text().splitlines()] if records.exists() else []
        return completed, rows

    def test_identifier_and_payload_reach_only_the_recorder(self):
        identifier = "acceptance identifier; $(not-a-command)"
        session = {"primary": "T3CODE_THREAD_ID", "identifiers": [
            {"name": "T3CODE_THREAD_ID", "value": identifier},
            {"name": "OTHER_HARNESS", "value": "ignore"},
        ]}
        result, rows = self.run_relay([session])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["session_identifier"], identifier)
        self.assertEqual(rows[0]["subject"], "deck.1.2.123")
        self.assertEqual(rows[0]["resolved"], session)
        self.assertEqual(rows[0]["event"]["thread_id"], "123")

    def test_own_or_unaffiliated_event_does_not_wake_a_session(self):
        result, rows = self.run_relay([])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(rows, [])

    def test_failed_resolve_does_not_record_even_with_session_output(self):
        session = {"identifiers": [{"name": "T3CODE_THREAD_ID", "value": "must-not-record"}]}
        result, rows = self.run_relay([session], exit_code=1)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
