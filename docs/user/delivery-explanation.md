---
title: Event delivery and trust
product: deck
---

# Event delivery and trust

The service keeps Discord bot tokens and publishes activity from touched
threads to NATS JetStream. Clients authenticate to the `deck` SSH subsystem
with an ed25519 Transit key. The service checks each key's bot, server and
parent-channel grants; the client's channel filters can narrow these grants.

Events are delivered at least once. Deduplicate by activity ID and acknowledge
only after the action triggered by the event has succeeded. Messages recovered
after downtime can be redelivered. Button presses and poll votes still waiting
when the service stops cannot be recovered from Discord history.

`deck resolve` reads the local store without contacting the service or Vault.
It returns sessions for the event's thread, leaving out the sending session for
an own message. No matching session is a successful empty result. The relay
chooses how to wake a harness; event content and session identifiers stay data,
never executable shell text.

The client bounds connecting, the SSH handshake and ordinary calls with
`service.timeout` (default `15s`). `listen` has its own `--timeout`. Each command
opens one SSH connection and requests the `deck` subsystem; shell, exec and port
forwarding requests are refused.

The [activity schema reference](activity-reference.md) describes payloads;
[events and session relays](events.md) shows subscription and acknowledgement.
The [API reference](api-reference.md) describes RPCs and their native types.
