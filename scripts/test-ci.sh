#!/usr/bin/env bash
# Normal tests may skip real OpenBao; CI must execute every live address test.
set -euo pipefail
cd "$(dirname "$0")/.."
log=$(mktemp)
trap 'rm -f "$log"' EXIT
go test -count=1 -v ./... | tee "$log"
if grep -q 'bao executable not available' "$log"; then
  echo 'CI requires the OpenBao tests; set DECK_BAO or put bao on PATH.' >&2
  exit 1
fi
