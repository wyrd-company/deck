#!/usr/bin/env bash
# Same end-user assertion as prove-config-home.sh; macOS's directory service is
# the independent oracle. A Linux result cannot prove the macOS assumption.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$(uname -s)" == Darwin ]] || { echo 'macOS required' >&2; exit 1; }
proof=$(mktemp -d)
trap 'rm -rf "$proof"' EXIT
uid=$(id -u)
account=$(id -un)
home=$(dscl . -read "/Users/$account" NFSHomeDirectory | sed 's/^NFSHomeDirectory: //')
[[ "$home" = /* && "$home" != / ]] || { echo 'no absolute directory-service home' >&2; exit 1; }
config_dir="$home/.config/deck"
[[ ! -e "$config_dir" ]] || { echo "$config_dir already exists; run on a disposable macOS runner" >&2; exit 1; }
trap 'rm -rf "$proof" "$config_dir"' EXIT
mkdir -p "$config_dir"
cp docs/specifications/assets/configuration/examples/client.yml "$config_dir/config.yml"
printf '\nstore:\n  path: %s/store.db\n' "$proof" >> "$config_dir/config.yml"
CGO_ENABLED=0 go build -trimpath -o "$proof/deck" ./cmd/deck
probe() {
  env HOME=/wrong/home USER=wrong-user LOGNAME=wrong-user XDG_CONFIG_HOME=/wrong/xdg \
    VAULT_ADDR=http://wrong.invalid BAO_ADDR=http://wrong.invalid VAULT_TOKEN=wrong-token \
    HTTPS_PROXY=http://wrong.invalid:1 "$proof/deck" "$@"
}
probe config validate > "$proof/found"
test "$(cat "$proof/found")" = "ok $config_dir/config.yml"
probe skill > "$proof/skill" 2> "$proof/stderr"
cmp internal/skill/SKILL.md "$proof/skill"
test "$(cat "$proof/stderr")" = 'unread: none'
printf 'PASS CGO_ENABLED=0 CLI UID %s uses directory-service home %s despite hostile HOME/USER/LOGNAME/XDG and Vault/proxy env\n' "$uid" "$home"
