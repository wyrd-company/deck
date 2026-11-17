#!/usr/bin/env bash
# Exercise the actual pinned publisher; do not replace its product allowlist.
set -euo pipefail
if [[ "$#" -ne 5 ]]; then
  echo 'usage: validate-publisher-contract.sh PUBLISHER COMMIT MANIFEST ARTIFACTS OUTPUT' >&2
  exit 2
fi
publisher=$(cd "$1" && pwd)
expected_commit=$2
manifest=$3
artifacts=$4
output=$5
if [[ ! "$expected_commit" =~ ^[0-9a-f]{40}$ ]] || [[ "$(git -C "$publisher" rev-parse HEAD)" != "$expected_commit" ]]; then
  echo 'publisher checkout must match the full pinned commit' >&2
  exit 2
fi
if [[ -n "$(git -C "$publisher" status --porcelain --untracked-files=all)" ]]; then
  echo 'publisher contract checkout must be clean' >&2
  exit 2
fi
repository_script="$publisher/scripts/repository.py"
allowlist="$publisher/config/products.json"
python3 "$repository_script" validate "$manifest"
mkdir -p "$output"
inbox=$(mktemp -d "$output/inbox.XXXXXX")
trap 'rm -rf "$inbox"' EXIT
# These local commits are fixtures required by the publisher's queue contract.
git init --quiet --initial-branch=release-manifests "$inbox"
git -C "$inbox" -c user.name='Package Contract Test' -c user.email='test@example.invalid' commit --quiet --allow-empty -m 'Initialize contract fixture'
submission=$(python3 "$repository_script" submit "$manifest" "$inbox")
IFS=$'\t' read -r state submitted_path <<<"$submission"
if [[ "$state" != created || ! "$submitted_path" =~ ^releases/deck/[0-9]+\.[0-9]+\.[0-9]+\.json$ ]]; then
  echo "unexpected publisher submission: $submission" >&2
  exit 2
fi
git -C "$inbox" add -- "$submitted_path"
git -C "$inbox" -c user.name='Package Contract Test' -c user.email='test@example.invalid' commit --quiet -m 'Add contract fixture manifest'
python3 "$repository_script" validate-queue "$inbox" "$(git -C "$inbox" rev-parse HEAD)" "$submitted_path" "$allowlist"
python3 "$repository_script" stage "$inbox/$submitted_path" "$artifacts" "$output/stage" "$publisher/pubkey.gpg"
python3 "$repository_script" render-aur "$inbox/$submitted_path" "$output/aur" --artifacts "$artifacts"
test -s "$output/stage/.new-rpms"
test -s "$output/aur/PKGBUILD"
grep -Fxq "pkgname='wyrd-deck-bin'" "$output/aur/PKGBUILD"
echo 'PASS pinned publisher queue validation, package staging, and AUR rendering'
