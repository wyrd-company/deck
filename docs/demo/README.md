# Record the demo

From the repository root, run `task demo`. It needs Go, Python 3 and Docker. The pinned VHS 0.12.1 container supplies
Chromium, ffmpeg, ttyd and fonts.
`task demo:check` checks the scenario without recording or needing VHS.

The committed tape writes `docs/user/assets/demo.gif`. The adapter in `main.go`
invokes the production CLI with an injected, in-process fixture API. Its command
parser, output, SQLite session affinity and read cursors are the real ones.
It exercises posting, listening, unread reporting, reading, replying and
resolving a session. It does not exercise SSH authentication, Vault, NATS or
Discord. All names, IDs, keys and conversation text are synthetic. The tape
labels the recording as an offline demo.

`run.sh` builds the adapter and copies its public config into a private temporary
directory, verifies JSON results and cursor changes, resets the store, then
records. It deletes that directory on exit, including failures. The adapter has
no network dialer or credential-reading implementation. It cannot use the live
bot fixture, and the recording does not depend on a session's ambient identifiers.

## Acceptance captures

`bash docs/demo/capture-help.sh` builds the real CLI and renders its `--help`
output to `docs/user/assets/cli-help.png` with VHS and DejaVu Sans Mono,
using the committed `help.tape`.
`task demo` regenerates `docs/user/assets/demo.gif`. Both use synthetic inputs.

The acceptance branch can also record without Docker: `bash docs/demo/record-local.sh`
uses installed VHS 0.12.1, ttyd, Chromium and ffmpeg with the same committed tape
and fixture, after checking the fixture through `task demo:check`.
