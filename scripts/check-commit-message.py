#!/usr/bin/env python3
"""Validate the Conventional Commit header passed by the commit-msg hook."""
import pathlib
import re
import sys


def main():
    if len(sys.argv) != 2:
        print("usage: task lint:commit-msg -- MESSAGE_FILE", file=sys.stderr)
        return 1
    message = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
    header = message.splitlines()[0] if message.splitlines() else ""
    # Git writes these headers for merge, revert and autosquash workflows.
    if re.fullmatch(r'(?:Merge \S.*|Revert "\S.*"|(?:fixup|squash|amend)! \S.*)', header):
        return 0
    if not re.fullmatch(r"[a-z][a-z0-9-]*(?:\([^()\r\n]+\))?!?: \S.*", header):
        print("Use a Conventional Commit header, e.g. fix(cli): preserve unread activity", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
