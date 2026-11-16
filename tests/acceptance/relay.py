"""Trowel command destination: resolve locally, then invoke only our recorder."""

import json
import os
import subprocess
import sys

deck, config, recorder, output = sys.argv[1:]
event = sys.stdin.buffer.read()
resolved = subprocess.run(
    [deck, "resolve", "--config", config, "--event", "-", "--json"],
    input=event,
    capture_output=True,
    check=True,
)
for session in json.loads(resolved.stdout)["sessions"]:
    for identifier in session["identifiers"]:
        if identifier["name"] == "T3CODE_THREAD_ID":
            # An absolute script path is deliberate: PATH can never select a real t3.
            subprocess.run(
                [sys.executable, recorder, output, identifier["value"]],
                input=json.dumps({
                    "subject": os.environ["RELAY_SUBJECT"],
                    "event": json.loads(event),
                    "resolved": session,
                }).encode(),
                check=True,
            )
