#!/usr/bin/env bash
# Prove the release tag is reachable with the same anonymous access installers use.
set -euo pipefail
if [[ "$#" -ne 1 || ! "$1" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo 'usage: check-release-source.sh BARE_STABLE_VERSION' >&2
  exit 2
fi
probe=$(mktemp -d)
trap 'rm -rf "$probe"' EXIT
# Drop inherited auth/config and leave the checkout so its extraheaders cannot
# authenticate this request. No prompting or credential helper may supply a token.
if ! env -i PATH="$PATH" HOME="$probe" GIT_CONFIG_NOSYSTEM=1 \
  GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 GIT_ASKPASS=/bin/false \
  git -C "$probe" ls-remote --exit-code https://github.com/wyrd-company/deck.git "refs/tags/$1" > /dev/null; then
  echo "release source tag $1 must be publicly accessible without credentials" >&2
  exit 1
fi
echo "PASS release source tag $1 resolves anonymously"
