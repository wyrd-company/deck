# Contributing to deck

## Reporting an issue

Report bugs and propose ideas in [GitHub issues](https://github.com/wyrd-company/deck/issues).
Include the deck version, OS and CPU, the command, expected result and diagnostic.
Remove credentials, private keys and bot tokens from configurations and recordings.

## Setting up

Open the repository in its [devcontainer](.devcontainer/devcontainer.json), which
installs the check tools and hooks. For an existing environment, install Go 1.27.3
or newer, Node.js 22+, Python 3 and [Task](https://taskfile.dev/installation/), then run:

```sh
task setup
```

Ensure the Go binary directory and `~/.local/bin` are on PATH before running
setup. Python's `venv` module must be installed (on Debian/Ubuntu, install the
`python3-venv` package). Live Discord and container acceptance tests are opt-in;
their fixtures and requirements live in [tests/acceptance](tests/acceptance/README.md).

## Making a change

Create a branch from the branch the change targets. Use Conventional Commits
(for example, `fix(cli): preserve unread activity on a failed write`). Open a
pull request describing the problem, resulting behavior and checks run. Changes
receive independent review and passing CI before merging.

Read [AGENTS.md](AGENTS.md) and the relevant design documents before changing a
contract. Keep the implementation and its design in the same pull request.
Record user-visible changes with `task intent:add MESSAGE="Describe the change"`.
`UNIT` defaults to `deck`; use `task intent:add UNIT=deck-docs MESSAGE="Describe the docs change"`
for docs-only changes. The default bump is patch; set `BUMP=minor` or `BUMP=major`
when that is the appropriate release bump.

## Checking a change

```sh
task check
```

Pull requests also require the separate non-publishing release snapshot CI job
and the macOS configuration-home job. The snapshot remains separate from
`task check` because it produces distribution artifacts; run `task release:snapshot`
locally when changing packaging. Real OpenBao coverage is mandatory in
`task check`; `task ci` remains a compatibility alias. For a focused Go test, use
`task test:unit -- ./internal/cli -run TestName`; `task test` runs the full Go suite
and permits OpenBao tests to skip when its executable is unavailable.
`task lint` checks without rewriting files; `task format` applies formatting.
Hooks run formatting and lint before a commit, and check its Conventional Commit
message. They do not run the full suite on push.

## Build and distribution checks

`task build` creates `bin/deck`; pass `VERSION=1.2.3` to inject a release version.
Run a configured local service with `task dev CONFIG=/path/to/service.yml --watch`
to rebuild on source changes.

Use `task release:snapshot` to check packaging without publishing; artifacts are
written to `dist/`. For publisher compatibility, run `task release:compatibility
PUBLISHER_ROOT=/path/to/repo.wyrd.foo`.
The compatibility task needs `rpm`, `dpkg-deb`, and a clean publisher checkout at
the configured `PUBLISHER_COMMIT`; that pin already admits deck. Its source-tag
provenance check requires the version's tag to be published and anonymously
accessible. An unpublished snapshot stops at `cannot resolve source tag`.
`task release:install:check` proves a packed npm install using local snapshot bytes
in place of the HTTPS download; run that proof on a disposable machine.

`task image:build VERSION=dev` builds and loads the service image locally.
`task image:build:multiarch` writes `dist/deck-image.tar` as a multi-arch OCI
archive using Docker Buildx; install QEMU/binfmt emulation when building for a
foreign CPU. Neither task pushes an image.

For configuration-home changes, run `task config:home:macos:check` on macOS
or `task config:home:check` on Linux. Use `task --list` for task descriptions;
[Taskfile.yml](Taskfile.yml) defines their implementation.

## Releases

The publisher, Homebrew, npm installer and AUR fetch source tags and release
assets anonymously. The release gate resolves the tag from outside the checkout
with inherited credentials
and Git configuration removed, and refuses a private repository or a missing tag
before any publishing job can run. Run `task release:source:check
RELEASE_VERSION=1.2.3` to check an existing tag without publishing anything.

[Intentional](https://intentional.foo/) is the version and release mechanism.
Record changes through `task intent:add`. Until a baseline release tag exists,
`task intentional:check` uses `intentional status`. The operator establishes the
baseline with intentional before planning a release. For a normal release:

```sh
task release:plan PLAN=/tmp/deck-release-plan.json
task release:apply
# Review and commit the applied changelog/intent changes through your git operator.
task release:tag PLAN=/tmp/deck-release-plan.json
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
   APT/RPM packages and renders/publishes `wyrd-deck-bin` for AUR.
5. After Release succeeds, publishes `docs/user/` and product metadata to
   wyrd.foo through its v2 reusable workflow.

The publisher is pinned in `Taskfile.yml`, `release.yml`, and `submit-package.yml`.
Move all three pins together when its approved contract or deck allowlist changes.
Secrets needed (configure them separately; no release task creates secrets):
`FORMULAE_PUBLISH_KEY`, the organization's SSH deploy key with write access to
`homebrew-tools`, `NPM_TOKEN` with publish access to `@wyrd-company/deck`,
`REPO_WYRD_FOO_PUBLISHER_APP_ID`, `REPO_WYRD_FOO_PUBLISHER_PRIVATE_KEY`,
`WYRD_TOOLS_DOCS_PUBLISHER_APP_ID`, and `WYRD_TOOLS_DOCS_PUBLISHER_PRIVATE_KEY`
(the last two for the wyrd.foo documentation workflow). GitHub supplies `GITHUB_TOKEN` for release assets and GHCR; npm provenance uses
the workflow's OIDC permission.

Recover a failed Release job with GitHub's **Re-run failed jobs** on the original
tag run. Recover package submission with **Submit package release**, specifying
the successful Release run ID and tag. It verifies the run, source commit and
tag, then reuses that run's digest-checked manifest artifact (retained for 90 days).
A missing retained manifest fails; do not rebuild a handoff from mutable assets.

## Documentation and media

Start with [the concept](docs/concepts/deck.yml); [the docs guide](docs/AGENTS.md)
maps the contracts and technical designs. Run `task docs:check` after editing
documentation. The scripts and fixtures are in [docs/demo](docs/demo/README.md)
and [docs/brand](docs/brand/README.md).

`task demo` records the real CLI with synthetic data from
[`docs/demo/demo.tape`](docs/demo/demo.tape), using the VHS 0.12.1 container
(Go, Python 3 and Docker are required).
`task demo:check` verifies the same scenario without recording. See the
[demo source](docs/demo/README.md) for the fixture boundary. `task brand:render`
regenerates the 1200×630 PNG from its [SVG source](docs/brand/social-preview.svg).

The light and dark marks, social preview and demo are declared in
[`docs/user/docs.yml`](docs/user/docs.yml). Intentional projects the `deck` and
`deck-docs` versions into that file. The operator establishes both baselines
with intentional. A docs-only intent targets `deck-docs:patch`. Docs publish
automatically after a successful Release triggered by a release-tag push. To
publish documentation without a code release, record only `deck-docs` intents,
run the normal plan, apply, commit and tag steps (they create only a
`deck-docs@X.Y.Z` tag), push that tag, then run **Publish to wyrd.foo** manually
with that tag as `ref`. Pushing a docs tag alone, or pushing to main, publishes
nothing. The workflow needs `WYRD_TOOLS_DOCS_PUBLISHER_APP_ID` and
`WYRD_TOOLS_DOCS_PUBLISHER_PRIVATE_KEY`, for the reusable documentation workflow.

With a wyrd.foo checkout and its pnpm dependencies installed, run
`task docs:validate WYRD_FOO_DIR=/path/to/wyrd.foo -- --offline` to validate pages,
assets, links and publishing metadata without publishing. Before the initial
release baselines exist, this reports the missing tags/changelog as warnings.
The publisher uses strict checks when a release actually publishes.

## Scratch acceptance for skills issue 27

This branch is an unmerged acceptance exercise, not deck's compliance delivery.
`task docs:user:generate` generates schema, API and parser-help reference, raw
definition copies, and example downloads under `docs/user/`.
`task docs:user:check` detects missing, stale and obsolete generated output,
checks audience/kind navigation and downloadable assets, rejects design links,
and validates the published examples. `task check` reaches it via `docs:check`.
Run the wyrd.foo publisher in offline validation mode separately, with
`task docs:validate WYRD_FOO_DIR=/path/to/wyrd.foo -- --offline`.
The [capture scripts](docs/demo/README.md) reproduce the embedded terminal image
and synthetic conversation recording. The native recorder accepts
`VHS_NO_SANDBOX=true` in a container and `LD_LIBRARY_PATH` when Chromium
needs local libraries. `uv run --no-project --with pyyaml==6.0.3 --with
jsonschema==4.26.0 python3 scripts/prove-user-docs.py` proves seven check guards.
