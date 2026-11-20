#!/usr/bin/env bash
# Native equivalent of task demo for the acceptance branch; synthetic data only.
set -euo pipefail
cd "$(dirname "$0")/../.."
bash docs/demo/run.sh --check
record_dir=$(mktemp -d /tmp/agent.XXXXXXXX)
trap 'rm -rf "$record_dir"' EXIT
mkdir "$record_dir/bin"
CGO_ENABLED=0 go build -o "$record_dir/bin/deck" ./docs/demo
cp docs/demo/client.yml "$record_dir/client.yml"
export DECK_DEMO_CONFIG="$record_dir/client.yml"
export PATH="$record_dir/bin:$PATH"
vhs docs/demo/demo.tape
