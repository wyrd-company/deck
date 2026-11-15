#!/usr/bin/env bash
# Checks deck's design documents: every document's type and relationships,
# the JSON Schemas and their examples, and the gRPC proto. With --env PATH it
# also checks that no value from that env file appears anywhere under docs/,
# without printing the values. Needs uv and go.
set -euo pipefail

docs=$(cd "$(dirname "$0")" && pwd)
env_file=""
if [ "${1:-}" = "--env" ]; then
  env_file=${2:?--env needs a path}
fi

status=0

uv run --quiet --no-project --with pyyaml==6.0.3 --with jsonschema==4.26.0 \
  python3 -I "$docs/check.py" || status=1

buf="go run github.com/bufbuild/buf/cmd/buf@v1.75.0"
if (cd "$docs/specifications/assets/grpc-api" && $buf build . && $buf lint .); then
  echo "ok   grpc-api proto builds and passes buf lint"
else
  echo "FAIL grpc-api proto builds and passes buf lint"
  status=1
fi

if [ -z "$(gofmt -l "$docs/spikes/assets")" ] && (cd "$docs/spikes/assets" && go vet ./...); then
  echo "ok   spike programs are formatted and vet clean"
else
  echo "FAIL spike programs are formatted and vet clean"
  status=1
fi

if [ -n "$env_file" ]; then
  leaked=0
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    name=${line%%=*}
    value=${line#*=}
    value=${value#\"}; value=${value%\"}; value=${value#\'}; value=${value%\'}
    [ -n "$value" ] || continue
    if grep -rqF -- "$value" "$docs"; then
      echo "FAIL a value of $name appears under docs/"
      leaked=1
    fi
  done <"$env_file"
  if [ "$leaked" = 0 ]; then
    echo "ok   no env file value appears under docs/"
  else
    status=1
  fi
fi

if [ "$status" = 0 ]; then
  echo "docs/check.sh: pass"
else
  echo "docs/check.sh: fail"
fi
exit "$status"
