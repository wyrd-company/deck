#!/usr/bin/env bash
# Runs the config-home-without-environment probe, built with CGO_ENABLED=0 as
# deck's client is, in a bubblewrap sandbox with a passwd file the script
# writes, and with HOME, USER and XDG_CONFIG_HOME set to values that do not
# match it. Needs go and bwrap.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

(cd "$here/.." && CGO_ENABLED=0 go build -o "$work/probe" ./config-home-without-environment)
uid=$(id -u)

run() { # $1: label, $2: passwd file
  printf '{"case":"%s","result":' "$1"
  bwrap --ro-bind / / --tmpfs /tmp --ro-bind "$2" /etc/passwd --ro-bind "$work/probe" /tmp/probe \
    env -i HOME=/tmp/fake-home USER=fake XDG_CONFIG_HOME=/tmp/fake-xdg /tmp/probe | tr -d '\n'
  printf '}\n'
}

printf 'root:x:0:0:root:/root:/bin/sh\n' >"$work/passwd-without-user"
printf 'root:x:0:0:root:/root:/bin/sh\nprobe:x:%s:%s:probe:/home/probe-from-passwd:/bin/sh\n' "$uid" "$uid" >"$work/passwd-with-user"

run no-passwd-entry "$work/passwd-without-user"
run passwd-entry "$work/passwd-with-user"
