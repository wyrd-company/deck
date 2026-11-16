# deck

An agent-friendly Discord client with a separate service that holds bot tokens.
The [concept](docs/concepts/deck.yml) describes the product;
[docs/AGENTS.md](docs/AGENTS.md) maps its contracts and technical designs.

## Why this exists

- Let a harness session converse with a person on Discord without holding bot secrets.
- Tie replies to the local session that asked, so a relay can wake it.

These are the owner's decisions in [issue #2](https://github.com/wyrd-company/deck/issues/2).

## Outcomes

- Bot tokens and Discord connections stay in the service; clients see only their grants and touched threads.
- Session affinity stays on the client's machine and is a convenience, not a security boundary.
- Configuration comes from YAML; environment variables supply only configured ambient session identifiers.
- deck speaks Discord, OpenBao/Vault, NATS and gRPC without depending on a particular harness or relay.

These invariants come from [issue #2](https://github.com/wyrd-company/deck/issues/2);
changing them requires the owner's decision.

## Glossary

- [Activity](docs/terms/activity.yml): a message, component use or poll vote in a touched thread.
- [Cursor](docs/terms/cursor.yml): a session's read position in a thread's event stream.
- [Persona](docs/terms/persona.yml): the configured author name and icon of a leading embed.
- [Touched thread](docs/terms/touched-thread.yml): a thread a client key posted or replied in through deck.
- [Unread](docs/terms/unread.yml): activity after the session's cursor, excluding its own sends.
- [Session](docs/terms/session.yml): the locally identified program the CLI acts for.
- [Client key](docs/terms/client-key.yml): a Transit ed25519 identity with service-side grants.
- [Grant](docs/terms/grant.yml): a client's permission to use a bot, server and channels.

## Orient

```tree
.
├── cmd/deck/       # executable and service adapters
├── internal/       # client, service, transport and local state
├── .intentional/   # release intents for deck and deck-docs
├── npm/            # native-binary installer, not a separate product
├── docs/demo/      # offline CLI fixture and recording tape
└── docs/brand/     # source and renderer for social preview
```

Use `task check` before pushing; [CONTRIBUTING.md](CONTRIBUTING.md) covers setup
and focused checks. Record a user-visible change with `task intent:add` as described
there. For API changes, edit
`docs/specifications/assets/grpc-api/deck/v1/deck.proto` and run `task generate`;
`internal/api/deckv1/` is generated.
