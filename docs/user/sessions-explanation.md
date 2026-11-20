---
title: Sessions and unread activity
product: deck
---

# Sessions and unread activity

A session identifies the local program that sent a message. A touched thread is
a Discord thread a client key posted or replied in through deck. The local
SQLite store connects that thread to the session's ordered identifiers.

Each session has its own read position. Reading advances it; `--keep-unread`
leaves it where it was. History reads Discord messages without advancing the
cursor. A session's own sends do not count as unread activity for that session.

The association is saved after a successful send result. Activity arriving
during a session's first send may already be unread before that session can be
resolved. The store belongs on the client's machine: the service holds grants
and thread activity, while the client owns session affinity and read cursors.

See [reading](reading.md) to work with unread messages and
[the event delivery model](delivery-explanation.md) to build a relay.
