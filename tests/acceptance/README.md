# Container acceptance

`task test:acceptance EVIDENCE=/absolute/path/to/an/empty/directory` runs the
issue #2 conversation through the repository's Docker image, a local
JWT-authenticated JetStream server, a real `bao server -dev`, the compiled CLI,
and Trowel. It needs Linux Docker with host networking, Go, OpenBao, Python 3
and Trowel on PATH. The named task skips before starting resources if its test
bot fixture is absent; a present fixture with missing tools or a failed Docker
daemon fails rather than skips.

The default fixture is `/workspace/tools/deck/.env`; override it with
`FIXTURE=/absolute/path`. Its fields are `BOT_TOKEN`, `APPLICATION_ID`,
`SERVER_ID`, `TEST_FORUM`, and `TEST_CHANNEL`. The run checks both channels are
inside that server before posting. It keeps the visible test posts as message
links for review. Keep the fixture outside the repository; it is never copied
into the image or passed on a command line. The CLI configuration contains
only references to the local OpenBao secrets and Transit signing key.

Both posts include a mention of the test server's owner, an embed, a button, an attachment and a
poll. An exact nonce repeat must return the same IDs, with one copy in Discord
history. For forums the poll must be a separate, second message. Trowel feeds
events to `relay.py`, which invokes `deck resolve --event - --json` and calls
the absolute `t3.py` recorder path with the resolved synthetic session ID.
Neither script invokes a real `t3` executable or messages a real session.

Use `HUMAN_WAIT=120s` to leave a window for a person to reply, press the button,
and add/remove a vote on each post. Links appear in the live transcript. The
run then expires both polls: Discord's real type 46 poll-result messages stand
in for incoming replies if no person participates. They exercise the gateway
and relay but **do not prove** person message content, button delivery or poll
vote add/remove delivery. `human-observations.json` names those assumptions
separately. Missing human participation does not turn the automated scenario
into a failure, and must be disclosed for operator acceptance.

The CLI reads incoming activity, proves both the reported count and a separate
inbox call are zero, then answers on each thread and reads the answers directly
from Discord. Finally, stopping NATS must turn the running service health into
HTTP 503. The default image healthcheck must have been healthy before the
conversation. Deleting a stream while running is outside this scenario and
retains the design's stop-first residual.

Evidence includes the source commit, commands/output, independent subject and
header captures, exact stand-in calls, CLI post/read results, human observation
flags, and scrubbed service/Trowel/OpenBao logs. Only a fully completed run
whose cleanup passed writes `"status": "passed"` in `result.json`. A missing
result, failed result, crash or skipped run is not acceptance evidence. Use a
new empty evidence directory for each run so old records cannot satisfy it.
Without `EVIDENCE`, evidence is temporary and deleted at test cleanup.

The run removes its image/container and terminates its child processes. In a
T3 thread it records them in `ledger` as they are created. The test bot's posts
remain as evidence; delete them separately only after review if desired.

`task test:acceptance:relay` exercises the real relay/recorder subprocess path
without external services, including failed resolution, no matching session,
literal untrusted values, and a PATH trap that detects use of a real `t3`.
These checks also run in `task ci`.

`task test:acceptance:diagnostic` runs the same conversation with a native
service process when Docker is unavailable. Its final marker is
`diagnostic-passed-container-unverified`; it cannot satisfy the image or Docker
healthcheck criteria. Use this only to debug the live stack before repeating
the full container acceptance.
