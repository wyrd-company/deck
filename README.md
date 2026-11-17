[![CI](https://github.com/wyrd-company/deck/actions/workflows/ci.yml/badge.svg)](https://github.com/wyrd-company/deck/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/wyrd-company/deck)](https://github.com/wyrd-company/deck/releases)
[![npm](https://img.shields.io/npm/v/@wyrd-company/deck)](https://www.npmjs.com/package/@wyrd-company/deck)
[Documentation](https://wyrd.foo/docs/deck)

# deck

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/user/assets/icon-dark.svg">
  <img src="docs/user/assets/icon-light.svg" alt="deck cyberdeck icon" width="160">
</picture>

An agent-friendly Discord client.

Post questions, read replies and keep each conversation tied to its local session.

![An offline conversation through the real deck CLI](docs/user/assets/demo.gif)

The [recording script and tape](docs/demo/README.md) use synthetic data with the
real command parser, output, local store and read cursors. The fixture replaces
Discord and the service connection.

## Features

- Post and reply with mentions, files, embeds, buttons, polls and personas.
- Read an inbox, mark activity read and wait for replies with `deck listen`.
- Associate Discord threads with local sessions and resolve them for a relay.
- Keep bot tokens in a separate service; authenticate clients through OpenBao or Vault.
- Grant each client access to specific bots, servers and channels.
- Publish touched-thread activity to NATS JetStream for subscribers.
- Print agent operating instructions with `deck skill` and request JSON output.

## Install

The CLI supports Linux and macOS on x86-64 and ARM64. Distribution channels
receive artifacts when a release is published; an empty release list means no
version is available yet.

Homebrew:

```sh
brew install wyrd-company/tools/deck
```

npm, with Node.js 22+ and `tar`:

```sh
npm install --global @wyrd-company/deck
```

APT, after configuring the signing key and repository from [repo.wyrd.foo](https://repo.wyrd.foo):

```sh
sudo apt install deck
```

RPM, after configuring the repository from [repo.wyrd.foo](https://repo.wyrd.foo):

```sh
sudo dnf install deck
```

Arch Linux, with an AUR helper:

```sh
paru -S wyrd-deck-bin
```

Release archive, after downloading an archive and `checksums.txt` from the same
[GitHub release](https://github.com/wyrd-company/deck/releases) (Linux x86-64 example):

```sh
version=1.2.3
archive="deck_${version}_linux_x86_64.tar.gz"
grep "  ${archive}$" checksums.txt | sha256sum --check
tar -xzf "$archive"
mkdir -p "$HOME/.local/bin"
install -m 0755 deck "$HOME/.local/bin/deck"
```

Replace the example version with the release you downloaded; macOS uses `darwin`
and `shasum -a 256 --check`, and ARM64 uses `aarch64`.
See [installation](docs/user/install.md) for the configuration examples and PATH.

Service container (Linux amd64/arm64), after configuring its read-only mounts:

```sh
docker pull ghcr.io/wyrd-company/deck:1.2.3
```

Pin a published version. The [service guide](docs/user/service.md#container)
shows the run command and configuration, token and socket mounts.

## Prerequisites

- A running deck service with a Discord bot and channel grants for your client.
  Self-hosting also requires [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream).
- Access to an ed25519 Transit key in [OpenBao](https://openbao.org/)
  or [Vault](https://developer.hashicorp.com/vault). The service registers its public key;
  the CLI uses Transit for signing and holds no bot token.
- A client YAML configuration with the service's SSH address and public host key,
  Transit settings and a granted channel, as the
  [first-message walkthrough](docs/user/overview.md#send-your-first-message) describes.

## Quickstart

These offline commands work immediately after installing the CLI:

```sh
deck --version
deck --help
deck skill
```

To send a message, follow the [first-message walkthrough](docs/user/overview.md#send-your-first-message)
to save and validate `client.yml`, exchange public keys and confirm your grants.
With that configuration and a default channel:

```sh
deck post --config ./client.yml --session conversation=first \
  --title 'Hello from deck' --text 'Can you reply in this thread?'
deck listen --config ./client.yml --session conversation=first --timeout 5m
deck inbox --config ./client.yml --session conversation=first
```

Listen prints the reply and marks it read. Keep the same session to continue the
conversation. See [posting](docs/user/posting.md), [reading](docs/user/reading.md)
and [events](docs/user/events.md) for content flags, history and relay integration.

## Credits

- [disgo](https://github.com/disgoorg/disgo) for the Discord API and gateway.
- [Wish](https://github.com/charmbracelet/wish) and [gRPC](https://grpc.io/) for the authenticated service connection.
- [OpenBao](https://openbao.org/) and [Vault](https://developer.hashicorp.com/vault) for key signing and secret storage.
- [NATS](https://nats.io/) for JetStream activity and the thread registry.
- [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite) for the local session store.
- [VHS](https://github.com/charmbracelet/vhs) for the reproducible terminal recording.
