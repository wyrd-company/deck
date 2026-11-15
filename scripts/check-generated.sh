#!/usr/bin/env bash
# Compare against a fresh generation, including absent and unexpected output files.
set -euo pipefail
cd "$(dirname "$0")/.."
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
bash scripts/generate.sh "$out"
diff -ru internal/api "$out/internal/api"
