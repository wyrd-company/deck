#!/usr/bin/env python3
"""Mutation probes for task 4. Run alone in an implementation worktree.

Each source mutation is restored in finally, even on a failed check. --integration
also proves task check's generated/docs dependencies and task ci's no-skip rule.
No report is a pass until every requested probe has completed.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def run(command, env=None):
    return subprocess.run(command, cwd=ROOT, env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=180)


def require_pass(command):
    result = run(command)
    if result.returncode:
        raise RuntimeError(f"baseline failed: {command}\n{result.stdout}")


def require_failure(command, marker, env=None):
    result = run(command, env)
    if result.returncode == 0 or marker not in result.stdout or '[build failed]' in result.stdout:
        raise RuntimeError(f"guard not proved: {command}\n{result.stdout}")


def mutate(name, filename, before, after, command, marker='--- FAIL:'):
    path = ROOT / filename
    original = path.read_bytes()
    text = original.decode()
    if text.count(before) != 1:
        raise RuntimeError(f"mutation target changed: {name}")
    try:
        path.write_text(text.replace(before, after, 1))
        require_failure(command, marker)
    finally:
        path.write_bytes(original)
    print(f"PASS guard: {name}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--integration', action='store_true')
    args = parser.parse_args()
    require_pass(['go', 'test', '-count=1', './internal/config', './internal/vault', './internal/transport'])
    probes = [
        ('schema validation', 'internal/config/load.go',
         'if err := compiled[name].Validate(raw); err != nil {',
         'if err := compiled[name].Validate(raw); false {',
         ['go', 'test', '-count=1', '-run', '^TestContractFixtures$', './internal/config']),
        ('grant bot reference', 'internal/config/validate.go', '!bots[bot]', 'false',
         ['go', 'test', '-count=1', '-run', '^TestServiceReportsAllCrossFields$', './internal/config']),
        ('HTTP proxy isolation', 'internal/vault/client.go', 'Proxy: nil,', 'Proxy: http.ProxyFromEnvironment,',
         ['go', 'test', '-count=1', '-run', '^TestHTTPSIgnoresProxyEnvironment$', './internal/vault']),
        ('TLS certificate verification', 'internal/vault/client.go',
         'MinVersion: tls.VersionTLS12, RootCAs: roots',
         'MinVersion: tls.VersionTLS12, RootCAs: roots, InsecureSkipVerify: true',
         ['go', 'test', '-count=1', '-run', '^TestHTTPSRejectsUntrustedCA$', './internal/vault']),
        ('KV reads once per path' , 'internal/vault/kv.go', 'c.cache[path] = data', '// mutation: do not cache',
         ['go', 'test', '-count=1', '-run', '^TestLiveAddressForms/unix-agent$', './internal/vault']),
        ('AppRole token revocation', 'internal/vault/client.go', 'if !c.issued {', 'if true {',
         ['go', 'test', '-count=1', '-run', '^TestLiveAppRoleAndFailures$', './internal/vault']),
        ('missing identity stays empty', 'internal/transport/identity.go', '\n\treturn ""\n}', '\n\treturn "attacker"\n}',
         ['go', 'test', '-count=1', '-run', '^TestCallingKey$', './internal/transport']),
    ]
    for probe in probes:
        mutate(*probe)
    if args.integration:
        require_pass(['task', 'check'])
        mutate('task check rejects stale generation', 'internal/api/deckv1/deck.pb.go',
               '// Code generated', '// stale-generated-guard-probe\n// Code generated',
               ['task', 'check'], 'stale-generated-guard-probe')
        with tempfile.TemporaryDirectory(prefix='deck-guard-') as directory:
            shim = Path(directory) / 'uv'
            shim.write_text('#!/bin/sh\necho forced-docs-check-failure >&2\nexit 1\n')
            shim.chmod(0o755)
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ['PATH'])
            require_failure(['task', 'check'], 'docs/check.sh: fail', env)
        print('PASS guard: task check propagates docs/check.sh failure', flush=True)
        with tempfile.TemporaryDirectory(prefix='deck-no-bao-') as directory:
            # Leave bao off PATH while retaining all other check prerequisites.
            for name in ('go', 'task', 'uv', 'intentional', 'node'):
                executable = shutil.which(name)
                if not executable:
                    raise RuntimeError(f'missing check prerequisite: {name}')
                (Path(directory) / name).symlink_to(executable)
            env = dict(os.environ, PATH=directory + ':/usr/bin:/bin')
            env.pop('DECK_BAO', None)
            require_failure(['task', 'ci'], 'CI requires the OpenBao tests', env)
        print('PASS guard: task ci rejects skipped real OpenBao tests', flush=True)
    print(f'PASS all {len(probes) + (3 if args.integration else 0)} requested guard probes completed')


if __name__ == '__main__':
    main()
