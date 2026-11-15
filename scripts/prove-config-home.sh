#!/usr/bin/env bash
# Exercise the actual CLI as a UID with and without a passwd entry. Linux only;
# bubblewrap is a verification tool, never a runtime dependency.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v bwrap >/dev/null || { echo 'bwrap required to prove config-home lookup' >&2; exit 1; }
proof=$(mktemp -d)
trap 'rm -rf "$proof"' EXIT
chmod 755 "$proof"
mkdir -p "$proof/home/.config/deck"
cp docs/specifications/assets/configuration/examples/client.yml "$proof/home/.config/deck/config.yml"
CGO_ENABLED=0 go build -o "$proof/deck" ./cmd/deck
printf 'agent:x:42424:42424::%s/home:/bin/sh\n' "$proof" > "$proof/with-passwd"
printf 'root:x:0:0::/root:/bin/sh\n' > "$proof/without-passwd"
probe() {
  local passwd_file=$1
  shift
  bwrap --ro-bind / / --unshare-user --uid 42424 --gid 42424 \
    --ro-bind "$passwd_file" /etc/passwd \
    --setenv HOME /wrong/home --setenv USER wrong-user \
    --setenv XDG_CONFIG_HOME /wrong/xdg --setenv VAULT_ADDR http://wrong.invalid \
    --setenv BAO_ADDR http://wrong.invalid --setenv VAULT_TOKEN wrong-token \
    --setenv HTTPS_PROXY http://wrong.invalid:1 \
    "$proof/deck" "$@"
}
probe "$proof/with-passwd" config validate > "$proof/found"
test "$(cat "$proof/found")" = "ok $proof/home/.config/deck/config.yml"
set +e
probe "$proof/without-passwd" config validate > "$proof/missing" 2>&1
status=$?
set -e
test "$status" = 2
grep -q 'UID 42424; pass --config PATH' "$proof/missing"
echo 'PASS actual CLI uses passwd home despite hostile env; missing UID exits 2 naming UID and --config'

probe "$proof/without-passwd" skill > "$proof/skill" 2> "$proof/skill-stderr"
cmp internal/skill/SKILL.md "$proof/skill"
test "$(cat "$proof/skill-stderr")" = 'unread: unknown (no configuration)'
echo 'PASS skill prints embedded instructions with no passwd entry or configuration'
