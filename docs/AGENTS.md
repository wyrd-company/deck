# docs

deck's design and its user documentation. The design is the contract the code is built to: when code and a design document disagree, the change that makes them agree changes both, in the same pull request.

## Layout

Design documents are YAML in the Refinery ontology, one per file, named by slug. Each opens with its `$schema`, and refers to others by slug under `relationships`.

| Folder | Holds |
|---|---|
| `concepts/deck.yml` | What deck is and does. Start here. |
| `domain-models/` | Bots, channels, client keys, threads, activity, sessions, and their rules. |
| `specifications/` | The contracts: `command-line-interface`, `configuration`, `grpc-api`, `events`. Their definitions are under `specifications/assets/`. |
| `technical-designs/` | How each part is built: `service`, `client`, `transport`, with their module maps. |
| `decisions/` | Each real trade-off and why it went the way it did. |
| `assumptions/`, `risks/`, `terms/` | What is not yet proven, what threatens the outcome, and words with one meaning here. |
| `spikes/` | Experiments behind the design. `spikes/assets/` is a standalone Go module of throwaway programs, outside deck's build. |
| `user/` | User documentation, published to wyrd.foo. |

## Definitions are the source

- `specifications/assets/grpc-api/deck/v1/deck.proto` is what the Go API code is generated from; edit the proto, not generated code.
- The JSON Schemas under `specifications/assets/configuration/` and `specifications/assets/events/` are the contracts Go tests validate against, by path from the repository root. The examples there are the repository's annotated example configurations.
- The two configuration schemas each carry the same shared `$defs`; change both together.

## Check

Run `docs/check.sh` after any change under `docs/`. It checks every document's type, that every relationship resolves, that list items are prose, the schemas, their examples and invalid documents, and the proto with `buf build` and `buf lint`. It ends with `docs/check.sh: pass` only when all pass. With `--env PATH` it also confirms that no value from that env file appears under `docs/`; run it that way before committing anything a live test produced.

## Code points at its design

Each Go package begins with a front-matter comment naming the documents it implements or verifies:

```go
// ---
// relationships:
//   implements:
//     - service
//     - events
// ---
```

## User documentation

`user/` holds one Markdown page per topic, with `title`, `product: deck` and `order` front matter: `overview.md` (first-message walkthrough), `install.md`, `configuration.md` (client and service), `authentication.md` (Transit keys, host key, grants), `posting.md` (post, reply, content flags, personas), `reading.md` (unread, inbox, read, history, listen), `events.md` (subjects, payload, a relay that runs `deck resolve`), and `service.md` (running the container, healthcheck, retention, bots and permissions). A page explains and links to the specification for the exact contract; it does not restate it.
