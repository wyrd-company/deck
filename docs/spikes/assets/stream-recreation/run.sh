#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."
go run ./stream-recreation | tee "$here/result.jsonl"
