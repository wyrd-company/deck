#!/usr/bin/env bash
# Run in a private scratch directory; cleanup happens on success or failure.
set -euo pipefail
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo_root"
demo_dir=$(mktemp -d)
demo_container="deck-demo-$$"
demo_mode=${1:-}
cleanup() {
  # --rm covers normal exit; explicit removal covers an interrupted client.
  if [[ "$demo_mode" != '--check' ]]; then
    docker container rm --force "$demo_container" >/dev/null 2>&1 || true
  fi
  rm -rf "$demo_dir"
}
trap cleanup EXIT
mkdir "$demo_dir/bin"
CGO_ENABLED=0 go build -o "$demo_dir/bin/deck" ./docs/demo
cp docs/demo/client.yml "$demo_dir/client.yml"
export DECK_DEMO_CONFIG="$demo_dir/client.yml"
export PATH="$demo_dir/bin:$PATH"

# Check the actual CLI outputs and cursor/store behavior before recording.
# Do not report success just because VHS can encode a terminal session.
deck channels --session demo=release --json > "$demo_dir/channels.json"
deck post --session demo=release --title 'Release timing' --text 'Can we ship today?' --json > "$demo_dir/post.json"
deck listen --session demo=release --timeout 5m --keep-unread --json > "$demo_dir/listen.json"
deck inbox --session demo=release --json > "$demo_dir/inbox.json"
deck read --session demo=release --json > "$demo_dir/read.json"
deck inbox --session demo=release --json > "$demo_dir/empty.json"
deck reply 1571484634081529857 --session demo=release --text 'Thanks, preparing it.' --json > "$demo_dir/reply.json"
deck resolve 1571484634081529857 --json > "$demo_dir/resolve.json"
python3 - "$demo_dir" <<'PY'
import json
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
def read(name):
    return json.loads((root / (name + '.json')).read_text())
assert read('channels')['channels'][0]['name'] == 'questions'
assert read('post')['thread']['thread_id'] == '1571484634081529857'
assert read('listen')['activity'][0]['message']['content'] == 'Yes, ship it this afternoon.'
assert read('inbox')['unread']['total'] == 1
assert read('read')['activity'][0]['author']['username'] == 'reviewer'
assert read('empty')['unread']['total'] == 0
assert read('reply')['message']['message']['content'] == 'Thanks, preparing it.'
assert read('resolve')['sessions'][0]['identifiers'] == [{'name': 'demo', 'value': 'release'}]
print('demo: CLI send, listen, unread cursor and session resolution passed')
PY

if [[ "$demo_mode" == '--check' ]]; then
  exit 0
fi
rm -f "$demo_dir/deck.db"* "$demo_dir/incoming"
# VHS carries its Chromium/ffmpeg/ttyd dependencies in this pinned image.
docker run --rm --name "$demo_container" \
  --volume "$repo_root:/vhs" --workdir /vhs \
  --volume "$demo_dir:/demo" \
  --env DECK_DEMO_CONFIG=/demo/client.yml \
  --entrypoint /bin/sh \
  ghcr.io/charmbracelet/vhs:v0.12.1 \
  -c 'export PATH="/demo/bin:$PATH"; exec vhs docs/demo/demo.tape'
echo 'demo: wrote docs/user/assets/demo.gif'
