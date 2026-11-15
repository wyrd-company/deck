---
title: Reading replies
product: deck
order: 5
---

After a session command, `unread:` on stderr tells you whether replies await you.
With `--json`, inspect `unread.total`, `unread.threads`, or `unread.error`. A failed
unread report leaves the command's own result and exit status intact.

```sh
deck inbox
deck inbox --all
deck read
deck read 1571484634081529857 --keep-unread
deck listen --timeout 5m
```

Inbox lists your session's threads with unread activity, most recently sent to
first. `--all` includes quiet threads. Read prints unread activity and advances
the cursor through the response, even when your own messages were the only
activity. `--keep-unread` preserves the cursor. Listen returns immediately for
activity already unread, otherwise waits for another reply. Your own sends are
excluded, including sends made before a client-key rotation or rename.

Read and listen use threads your session posted or replied in. To inspect another
thread before answering, or older messages that may have left event retention,
use history:

```sh
deck history 1571484634081529857 --limit 50
```

History reads Discord messages without changing the cursor. Threads accept an ID,
`BOT/ID`, or a Discord thread URL. An expired touch or removed grant appears as a
thread state in inbox; read and listen leave that thread out and name it on
stderr. Client channel allow and deny lists filter every operation.

A listen timeout exits 5 and an interrupt exits 130. The
[CLI specification](../specifications/command-line-interface.yml) defines the
output, other exit statuses and cursor guarantees.
