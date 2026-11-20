#!/usr/bin/env bash
# Capture the real CLI's help in a terminal without configuration or secrets.
set -euo pipefail
cd "$(dirname "$0")/../.."
capture_dir=$(mktemp -d /tmp/agent.XXXXXXXX)
trap 'rm -rf "$capture_dir"' EXIT
mkdir "$capture_dir/bin"
go build -o "$capture_dir/bin/deck" ./cmd/deck
export PATH="$capture_dir/bin:$PATH"
vhs --output "$capture_dir/help.gif" docs/demo/help.tape
