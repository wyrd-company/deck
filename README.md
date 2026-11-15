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

## Development

Install Go (the version in `go.mod`), [Task](https://taskfile.dev/) and
[intentional](https://intentional.foo/). Run `task check` to format-check, vet,
build, test, lint workflows and validate release configuration. `task build`
creates `bin/deck`; pass `VERSION=1.2.3` to inject a release version. Until a
baseline release tag exists, the intentional check uses `intentional status`.
