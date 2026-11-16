<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/user/assets/icon-dark.svg">
    <img src="docs/user/assets/icon-light.svg" alt="deck cyberdeck icon" width="160">
  </picture>
</p>

# deck

An agent-friendly Discord client. Post questions, read replies and keep each
Discord thread tied to the agent session that started the conversation. The CLI
signs with an OpenBao/Vault Transit key; the service holds bot tokens and
publishes activity to NATS JetStream.

![A synthetic conversation through the real deck CLI](docs/user/assets/demo.gif)

Read the [user documentation](https://wyrd.foo/docs/deck), or start with the
[first-message walkthrough](docs/user/overview.md#send-your-first-message).
The recording uses invented data and an offline fixture API; the commands run
through the real CLI, including its local session store and read cursors.

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
The compatibility task needs `rpm`, `dpkg-deb`, and a clean publisher checkout at
the configured `PUBLISHER_COMMIT`; that pin already admits deck. Its source-tag
provenance check requires the version's tag to be published and anonymously
accessible. An unpublished snapshot stops at `cannot resolve source tag`.
`task release:install:check` proves a packed npm install using local snapshot bytes
in place of the HTTPS download; run that proof on a disposable machine.

`task image:build VERSION=dev` builds and loads the service image locally.
`task image:build:multiarch` writes `dist/deck-image.tar` as a multi-arch OCI
archive using Docker Buildx; install QEMU/binfmt emulation when building for a
foreign CPU. Neither task pushes an image. The image's service
health command is implemented by the service/gateway work.

CI builds the release snapshot without publishing and runs
`task config:home:macos:check` on macOS. That probe builds with `CGO_ENABLED=0`,
sets hostile home/user/config/proxy/Vault variables, and compares actual CLI
config lookup to the macOS directory service. Linux has `task config:home:check`.

## Releases

The repository must be public before the first release tag. The publisher,
Homebrew, npm installer and AUR fetch source tags and release assets without deck
credentials. Repository visibility is the operator's decision. The release gate
resolves the tag anonymously from outside the checkout with inherited credentials
and Git configuration removed, and refuses a private repository or a missing tag
before any publishing job can run. Run `task release:source:check
RELEASE_VERSION=1.2.3` to check an existing tag without publishing anything.

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

1. Checks anonymous source-tag access, validates intentional's changelog notes,
   and proves the pinned repo.wyrd.foo publisher contract before any publishing
   job can run.
2. Creates the GitHub release with Linux/macOS archives, DEB/RPM packages,
   `checksums.txt`, and notes extracted from `CHANGELOG.md`.
3. Publishes the multi-arch GHCR image (`:<version>` and `:latest`), generated
   Homebrew formula to `wyrd-company/homebrew-tools`, and version-matched
   `@wyrd-company/deck` npm wrapper.
4. Submits the retained immutable manifest to repo.wyrd.foo, which publishes
   APT/RPM packages and renders/publishes `deck-bin` for AUR.
5. After Release succeeds, publishes `docs/user/` and product metadata to
   wyrd.foo through its v2 reusable workflow.

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

## Documentation and media

`task demo` records the real CLI with synthetic data from
[`docs/demo/demo.tape`](docs/demo/demo.tape), using the VHS 0.12.1 container
(Go, Python 3 and Docker are required).
`task demo:check` verifies the same scenario without recording. See the
[demo source](docs/demo/README.md) for the fixture boundary. `task brand:render`
regenerates the 1200×630 PNG from its [SVG source](docs/brand/social-preview.svg).

The light and dark marks, social preview and demo are declared in
[`docs/user/docs.yml`](docs/user/docs.yml). Intentional projects the `deck` and
`deck-docs` versions into that file. The operator establishes both baselines
with intentional; this task creates no tags. A docs-only intent targets
`deck-docs:patch`. Publishing runs only after a successful Release triggered by
a release-tag push; docs-unit tags alone do not publish. For a docs-only release,
include it with the next code release. A push to main publishes nothing.
The workflow needs `WYRD_TOOLS_DOCS_PUBLISHER_APP_ID` and
`WYRD_TOOLS_DOCS_PUBLISHER_PRIVATE_KEY`, as Trowel's workflow does.

With a wyrd.foo checkout and its pnpm dependencies installed, run
`task docs:validate WYRD_FOO_DIR=/path/to/wyrd.foo -- --offline` to validate pages,
assets, links and publishing metadata without publishing. Before the initial
release baselines exist, this reports the missing tags/changelog as warnings.
The publisher uses strict checks when a release actually publishes.
