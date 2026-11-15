---
title: Events and session relays
product: deck
order: 6
---

The service publishes activity for touched Discord threads to the configured
JetStream subject prefix, followed by server, channel and thread IDs. The
[events specification](../specifications/events.yml) defines the payload and
delivery guarantees. A relay on the client's machine can pass an event to deck:

```sh
deck resolve --event - --json < activity.json
```

Resolve reads the local affinity store without contacting the service or Vault.
It returns the thread's sessions with their ordered identifiers, most recently
sent to first. A session's own message leaves that session out, using the sending
fingerprint and nonce recorded before the RPC. Sessions without identifiers are
left out. The relay decides how to wake a session; deck does not call a harness.

`deck resolve THREAD` returns affinity without own-event filtering. Both forms
succeed with an empty list when there is no matching session. Invalid event
payloads exit 2. Resolve has no unread report.

Keep the relay's configured `store.path` pointed at the same local store as the
sessions. A thread is associated only once the first successful send result has
been recorded, so activity during that first send can be unread without yet
resolving to a session. The [client design](../technical-designs/client.yml)
explains that window and the local storage contract.
