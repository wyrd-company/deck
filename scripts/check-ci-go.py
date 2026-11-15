#!/usr/bin/env python3
"""Reject a pinned CI compiler older than any tracked Go module requires."""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parent.parent
workflow = (root / '.github/workflows/ci.yml').read_text()
selected = re.search(r'^\s+go-version:\s*["\']([0-9.]+)["\']\s*$', workflow, re.M)
if not selected:
    raise SystemExit('CI Go version must be an explicit pinned version')


def version(value):
    parts = tuple(map(int, value.split('.')))
    return parts + (0,) * (3 - len(parts))


modules = subprocess.check_output(['git', 'ls-files', '--', 'go.mod', '**/go.mod'],
                                  cwd=root, text=True).splitlines()
if not modules:
    raise SystemExit('No tracked Go modules found')
for filename in modules:
    directive = re.search(r'^go ([0-9.]+)\s*$', (root / filename).read_text(), re.M)
    if not directive:
        raise SystemExit(f'{filename}: missing go directive')
    if version(selected[1]) < version(directive[1]):
        raise SystemExit(f'CI Go {selected[1]} is older than {filename} requires ({directive[1]})')
print(f'ok CI Go {selected[1]} covers all {len(modules)} module requirements')
