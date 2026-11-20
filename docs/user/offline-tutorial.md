---
title: Follow a conversation
product: deck
---

# Follow a conversation

We will follow a synthetic release conversation through deck's real CLI.
The recording uses an offline API fixture: no Discord account or credentials
are needed to view it. To run these commands against your service, first
complete [installation](install.md), [configuration](configuration.md) and
[key registration](authentication.md), then choose your granted channel.

![Offline deck conversation: post a question, listen, read, reply and resolve the session](assets/demo.gif)

The recording shows these actions and results:

1. We list channels with `deck channels --session demo=release`. The granted
   `questions` channel appears.
2. We post with an explicit session:

   ```sh
   deck post --session demo=release --title 'Release timing' --text 'Can we ship today?'
   ```

   deck prints a thread ID and a Discord URL.
3. Now `deck listen --session demo=release --timeout 5m --keep-unread` shows
   the synthetic reviewer's reply, “Yes, ship it this afternoon.”
4. `deck inbox --session demo=release` reports one unread message.
5. `deck read --session demo=release` prints the reply and advances the cursor.
6. We reply to the printed thread and resolve its local session:

   ```sh
   deck reply 1571484634081529857 --session demo=release --text 'Thanks, preparing it.'
   deck resolve 1571484634081529857
   ```

   The session resolves as `demo=release`.

We have followed a complete conversation and connected its thread to a local
session. [Send your first message](overview.md#send-your-first-message) to try
your own service. [Sessions and unread activity](sessions-explanation.md)
explains the cursor, and [command reference](cli-reference.md) lists the flags.
