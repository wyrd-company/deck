---
title: Posting and replying
product: deck
order: 4
---

Use `deck channels` to find a channel your key can use. `channels.default` in the
client configuration supplies the channel when you omit it.

```sh
deck post --title 'Release timing' --text 'Can we release this afternoon?'
deck reply 1571484634081529857 --text 'Thanks, I will prepare it.'
```

A post creates a Discord thread and prints its identifier and URL. Replying also
associates the thread with your current session. The ordered variable names in
`session.variables` determine that session; `--session NAME=VALUE` overrides
detection. With none set, deck uses a shared default session.

Use `--text-file -` to read text from stdin, `--file PATH` for attachments,
`--embed PATH` for Discord embed JSON, and `--components PATH` for component JSON.
Only one flag can read stdin. `--mention USER` and `--mention-role ROLE` accept
configured aliases or Discord IDs and can repeat. A configured default persona
can be overridden with `--persona NAME` or `--no-persona`.

```sh
deck post team --title 'Pick a time' --mention owner \
  --poll 'When?' --answer Today --answer Tomorrow --poll-duration 48h
```

A forum poll is sent as a second message. A poll failure does not undo its post;
check the diagnostic or JSON `poll_error`.

If a send reports `outcome unknown` and exits 4, inspect Discord before repeating
it. The error prints a command with the send's original `--nonce`. Run that only
intentionally, with the original content, target and session. A repeated nonce is
deduplicated while the original send is in flight or its event is retained.
After retention expires, repeating it can duplicate the message. deck never
repeats a send automatically.

Copy the entire printed shell command, including any assignments before `deck`.
Those preserve line breaks and other control characters in the original content.
If a content flag read stdin, supply the same input again when repeating. A
completed post's nonce cannot be reused to post to a different channel.

Each session command reports unread activity on stderr, or in `unread` with
`--json`. Use [reading](reading.md) to handle replies. `deck skill` prints the
embedded agent instructions. The [CLI specification](../specifications/command-line-interface.yml)
defines every flag and output field; `deck post --help` and `deck reply --help`
are available without configuration.
