# deck

A agent friendly Discord bot client

Story:

An agent running under T3Code where the T3CODE_THREAD_ID is set in the shell environment.

An agent session wants to reach out to a user on Discord. 
It calls the CLI to create a new post in a forum, where it perhaps `@`s the user, includes text, embeds, components, polls or file attachements.
The CLI, when the message was posted, uses the T3CODE_THREAD_ID (correct?) to "log in". Now the post created is associated with that thread id. Stored somewhere (sqlite?)
The CLI didn't go directly to the discord APIs. It went to a central service over gRPC, which did the posting. It holds the bot secrets.
The service is connected via Gateway. It detects the users response and publishes a NATS message.
Trowel, running in the agents space, subscribed to the services notifications, uses the same thread affinity DB to use the T3code CLI to send a message to that thread notifying them there are messages.
The agent, either having the message queued, or being woken, then calls the CLI to retrieve its "messages". Like checking Discord or an email.
It can then do whatever it needs to based on what it recieves.

Preferences:

- Written in Golang with https://github.com/disgoorg/disgo
- Secrets from OpenBao/Vault
- Service (`deck service start`)
  - Runs as a docker container. Published to GHCR
  - Connects to the gateway, sends REST API calls.
  - Secrets loaded from Vault/OpenBao via unix:// socket
  - Is where the bot account actually resides.  
  - Hosts gRPC API for CLI to communicate through - secured via public key from vault
  - Publishes notifications via NATS
  - Healthcheck
- CLI client (`deck`)
  - Published to homebrew (wyrd-company/homebrew-tools), npmjs (@wyrd-company/deck), repo.wyrd.foo (APT/RPM/AUR)
  - Connects to service via gRPC
  - Transit engine signing from vault/openbao via unix:// socket for gRPC
  - Detects ambient environment variables for session affinity (env vars, by preference order, are configurable) OR can
  - Commands run as session
  - Mark messages as "read"
  - Unread messages reported even for unrelated commands
  - Configurable "personas" to use with embed author - name + icon url
  - `deck listen` to block until response is received for specific Discord threads
  - Only interacts with Discord threads it created or joins - no notification of others.
  - `deck skill` embedded skill
  - Config:
    - Channels: default, allow list, deny list
- config.yml NOT env var for any config
- wyrd.foo user docs, a logo (cyberpunk deck theme) - light/dark, social preview, demo.gif (VHS)
- intentional

## Design

The design is under [`docs/`](docs/). Start with
[`docs/concepts/deck.yml`](docs/concepts/deck.yml);
[`docs/AGENTS.md`](docs/AGENTS.md) maps the rest. Run `docs/check.sh` after
changing anything there.

## Install

- Homebrew: `brew install wyrd-company/tools/deck`.
- npm (Node.js 22+): `npm install --global @wyrd-company/deck`.
- APT/RPM: `deck` from [repo.wyrd.foo](https://repo.wyrd.foo); AUR: `deck-bin`.
- Linux/macOS x86-64 and ARM64 archives and `checksums.txt` from
  [GitHub releases](https://github.com/wyrd-company/deck/releases).
- Service image: `ghcr.io/wyrd-company/deck:<version>` (Linux amd64/arm64).

See [installation and service setup](docs/user/install.md) for checksum
verification, the config file and container mounts. The CLI reads
`~/.config/deck/config.yml` using the OS user database's home, or `--config PATH`.

## Development

Install Go (the version in `go.mod`), [Task](https://taskfile.dev/), Node.js 22+,
Python 3 and [intentional](https://intentional.foo/). Run `task check` for the
repository checks, including release configuration and npm installer tests.
`task build` creates `bin/deck`; pass `VERSION=1.2.3` to inject a release version.
Until a baseline release tag exists, the intentional check uses `intentional status`.

`task release:snapshot` builds all four OS/CPU archives, DEB/RPM packages and a
Homebrew formula in `dist/`, without publishing. `task release:compatibility
PUBLISHER_ROOT=/path/to/repo.wyrd.foo` additionally exercises the pinned
publisher's validation, immutable queue, package staging and AUR rendering.
It needs `rpm` and `dpkg-deb`, and a clean publisher checkout at the configured
`PUBLISHER_COMMIT`. The current pin's product allowlist must admit deck before
that gate can pass; it is an external release prerequisite.

`task image:build VERSION=dev` builds and loads the service image locally.
`task image:build:multiarch` writes `dist/deck-image.tar` as a multi-arch OCI
archive using Docker Buildx. Neither task pushes an image. The image's service
health command is implemented by the service/gateway work.

CI builds the release snapshot without publishing and runs
`task config:home:macos:check` on macOS. That probe builds with `CGO_ENABLED=0`,
sets hostile home/user/config/proxy/Vault variables, and compares actual CLI
config lookup to the macOS directory service. Linux has `task config:home:check`.

## Releases

[Intentional](https://intentional.foo/) is the version and release mechanism.
Record a user-visible change with `intentional add --release-unit deck:patch
--message "Describe the change"`. The operator establishes the initial baseline
with intentional before planning a release. For a normal release:

```sh
intentional plan > release-plan.json
intentional apply
# Review and commit the applied changelog/intent changes through your git operator.
intentional tag --plan release-plan.json
# Push the resulting bare SemVer tag (for example 1.2.3) through your git operator.
```

Keep the saved plan outside tracked files. Never create release tags by hand.
A main-branch push publishes nothing. A release-tag push:

1. Validates intentional's changelog notes and proves the pinned repo.wyrd.foo
   publisher contract before any publishing job can run.
2. Creates the GitHub release with Linux/macOS archives, DEB/RPM packages,
   `checksums.txt`, and notes extracted from `CHANGELOG.md`.
3. Publishes the multi-arch GHCR image (`:<version>` and `:latest`), generated
   Homebrew formula to `wyrd-company/homebrew-tools`, and version-matched
   `@wyrd-company/deck` npm wrapper.
4. Submits the retained immutable manifest to repo.wyrd.foo, which publishes
   APT/RPM packages and renders/publishes `deck-bin` for AUR.

The publisher is pinned in `Taskfile.yml`, `release.yml`, and `submit-package.yml`.
Move all three pins together when its approved contract or deck allowlist changes.
Secrets needed (configure them separately; no release task creates secrets):
`HOMEBREW_TAP_TOKEN` with contents write access to `homebrew-tools`, `NPM_TOKEN`
with publish access to `@wyrd-company/deck`, `REPO_WYRD_FOO_PUBLISHER_APP_ID`, and
`REPO_WYRD_FOO_PUBLISHER_PRIVATE_KEY`. GitHub supplies `GITHUB_TOKEN` for release
assets and GHCR; npm provenance uses the workflow's OIDC permission.

Recover a failed Release job with GitHub's **Re-run failed jobs** on the original
tag run. Recover package submission with **Submit package release**, specifying
the successful Release run ID and tag. It verifies the run, source commit and
tag, then reuses that run's digest-checked manifest artifact (retained for 90 days).
A missing retained manifest fails; do not rebuild a handoff from mutable assets.
