#!/usr/bin/env bash
# Generate directly from the design's proto, using pinned Go tools (no protoc install).
set -euo pipefail
cd "$(dirname "$0")/.."
out=${1:-.}
mkdir -p "$out"
out=$(cd "$out" && pwd)
template=$(python3 - "$out" <<'PY'
import json, sys
print(json.dumps({"version": "v2", "plugins": [
    {"local": ["go", "run", "google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12"],
     "out": sys.argv[1], "opt": ["module=github.com/wyrd-company/deck"]},
    {"local": ["go", "run", "google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2"],
     "out": sys.argv[1], "opt": ["module=github.com/wyrd-company/deck"]}
]}))
PY
)
go run github.com/bufbuild/buf/cmd/buf@v1.75.0 generate docs/specifications/assets/grpc-api --template "$template"
