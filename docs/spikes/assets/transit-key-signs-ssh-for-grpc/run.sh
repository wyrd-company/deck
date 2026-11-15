#!/usr/bin/env bash
# Runs the transit-key-signs-ssh-for-grpc spike against a throwaway OpenBao
# dev server that listens on a unix socket. Needs `bao` and `go`.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
bao_pid=""
cleanup() {
  # Signal only the dev server this script started; with no child yet there
  # is nothing to stop.
  if [ -n "$bao_pid" ]; then
    kill "$bao_pid" 2>/dev/null || true
    wait "$bao_pid" 2>/dev/null || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

if ! command -v bao >/dev/null; then
  echo '{"step":"bao","result":"skip","detail":{"missing":"bao"}}'
  exit 0
fi

head -c 24 /dev/urandom | base64 | tr -d '/+=' >"$work/token"
cat >"$work/unix.hcl" <<EOF
listener "unix" {
  address = "$work/bao.sock"
}
EOF

port=$(python3 -I -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
bao server -dev -dev-no-store-token -dev-root-token-id="$(cat "$work/token")" \
  -dev-listen-address="127.0.0.1:$port" -config="$work/unix.hcl" >"$work/bao.log" 2>&1 &
bao_pid=$!

for _ in $(seq 50); do
  [ -S "$work/bao.sock" ] && break
  sleep 0.1
done

# The bao CLI is pointed at the dev server only, never at an ambient address.
bao_dev() { env -u BAO_ADDR -u VAULT_ADDR BAO_ADDR="unix://$work/bao.sock" BAO_TOKEN="$(cat "$work/token")" bao "$@"; }
bao_dev secrets enable transit >/dev/null
bao_dev write -f transit/keys/deck-client type=ed25519 >/dev/null
bao_dev write -f transit/keys/deck-stranger type=ed25519 >/dev/null

cd "$here/.."
go run ./transit-key-signs-ssh-for-grpc -address "unix://$work/bao.sock" -token-file "$work/token" -idle 15s
