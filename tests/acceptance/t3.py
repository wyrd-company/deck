"""Stand-in for t3. Record the identifier and event; never contact a session."""

import fcntl
import json
import sys

output, identifier = sys.argv[1:]
record = json.load(sys.stdin)
record["session_identifier"] = identifier
with open(output, "a", encoding="utf-8") as stream:
    fcntl.flock(stream, fcntl.LOCK_EX)
    stream.write(json.dumps(record) + "\n")
    stream.flush()
