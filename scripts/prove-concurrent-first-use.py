#!/usr/bin/env python3
"""Run six actual first commands against each of 30 brand-new SQLite stores."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import subprocess
import tempfile
import threading

root = Path(__file__).resolve().parent.parent
with tempfile.TemporaryDirectory(prefix='deck-first-use-') as tmp:
    tmp = Path(tmp)
    binary = tmp / 'deck'
    subprocess.run(['go', 'build', '-o', str(binary), './cmd/deck'], cwd=root, check=True)
    example = (root / 'docs/specifications/assets/configuration/examples/client.yml').read_text()
    host_key = next(line.split('host_key: ', 1)[1] for line in example.splitlines() if 'host_key: ' in line)
    expected = (root / 'internal/skill/SKILL.md').read_text()
    for trial in range(30):
        config = tmp / f'config-{trial}.json'
        config.write_text(json.dumps({
            'service': {'address': 'localhost:2222', 'host_key': host_key},
            'vault': {'address': 'unix:///unused/openbao.sock'},
            'key': {'mount': 'transit', 'name': 'unused'},
            'session': {'variables': []},
            'store': {'path': str(tmp / str(trial) / 'deck.db')},
        }))
        start = threading.Barrier(6)
        def command(_):
            start.wait()
            result = subprocess.run([str(binary), '--config', str(config), 'skill'], capture_output=True, text=True, timeout=15)
            if result.returncode != 0 or result.stdout != expected or result.stderr != 'unread: none\n':
                raise AssertionError(f'trial {trial}: exit={result.returncode}, stderr={result.stderr!r}, correct skill={result.stdout == expected}')
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(command, range(6)))
print('PASS real binary: 180 concurrent first commands across 30 fresh stores (6 per store)', flush=True)
