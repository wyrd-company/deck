#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
printf '%s\n' '{"state":"started","pass":false}' > "$here/result.jsonl"
go build -race -o "$work/probe" ./stream-recreation
for iteration in 1 2 3 4 5; do
  "$work/probe" > "$work/run-$iteration.jsonl"
  cmp "$work/run-1.jsonl" "$work/run-$iteration.jsonl"
  printf 'stream-recreation: identical run %s/5\n' "$iteration" >&2
done
cat "$work/run-1.jsonl" > "$here/result.jsonl"
printf '%s\n' '{"identical_runs":5,"state":"comparison-completed","pass":true}' >> "$here/result.jsonl"
cat "$here/result.jsonl"
