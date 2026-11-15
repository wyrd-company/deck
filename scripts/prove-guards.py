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
    require_pass(['go', 'test', '-count=1', './cmd/deck', './internal/config', './internal/vault', './internal/transport'])
    probes = [
        ('validation diagnostic file prefix', 'cmd/deck/main.go',
         'lines = append(lines, path+": "+field.Error())',
         'lines = append(lines, field.Error())',
         ['go', 'test', '-count=1', '-run', '^TestInvalidConfigCommand$', './cmd/deck']),
        ('validation diagnostic exact lines', 'cmd/deck/main.go',
         'fmt.Fprintln(stderr, "deck: "+line)',
         'fmt.Fprintln(stderr, "deck: "+line); fmt.Fprintln(stderr, "deck: "+line); fmt.Fprintln(stderr, "deck: vault.timeout: bogus")',
         ['go', 'test', '-count=1', '-run', '^TestInvalidConfigCommand$', './cmd/deck']),
        ('validation JSON error category', 'cmd/deck/main.go',
         '"category": "invalid"', '"category": "failed"',
         ['go', 'test', '-count=1', '-run', '^TestInvalidConfigCommand$', './cmd/deck']),
        ('non-key peers refuse test identity', 'internal/transport/identity.go',
         '\n\t\treturn ""\n\t}\n\tif name, ok := ctx.Value',
         '\n\t}\n\tif name, ok := ctx.Value',
         ['go', 'test', '-count=1', '-run', '^TestNonKeyPeerHasNoIdentity$', './internal/transport']),
        ('empty Vault URL query and fragment', 'internal/config/validate.go',
         'strings.ContainsAny(address, "?#")', '(u.RawQuery != "" || u.Fragment != "")',
         ['go', 'test', '-count=1', '-run', '^TestVaultAddressRejectsQueryAndFragment$', './internal/config']),
        ('field paths escape terminal controls', 'internal/config/load.go',
         'return strconv.QuoteToASCII(token)', 'return token',
         ['go', 'test', '-count=1', '-run', '^TestFieldPathsEscapeControls$', './internal/config']),
        ('Transit signature version', 'internal/vault/transit.go',
         ' || parts[1] != "v"+strconv.Itoa(version)', '',
         ['go', 'test', '-count=1', '-run', '^TestSignTransitRejectsWrongVersion$', './internal/vault']),
        ('schema validation', 'internal/config/load.go',
         'if err := compiled[name].Validate(raw); err != nil {',
         'if err := compiled[name].Validate(raw); false {',
         ['go', 'test', '-count=1', '-run', '^TestContractFixtures$', './internal/config']),
        ('unevaluated sibling filtering', 'internal/config/load.go',
         'if !declaresProperty(schema, e.InstanceLocation) {', 'if true {',
         ['go', 'test', '-count=1', '-run', '^TestExactInvalidFieldPaths$', './internal/config']),
        ('default home ignores environment', 'internal/config/path_linux.go',
         'func userHome() (string, error) {', 'func userHome() (string, error) { return os.Getenv("HOME"),nil',
         ['go', 'test', '-count=1', '-run', '^TestDefaultClientPathIgnoresEnvironment$', './internal/config']),
        ('grant bot reference', 'internal/config/validate.go', '!bots[bot]', 'false',
         ['go', 'test', '-count=1', '-run', '^TestServiceReportsAllCrossFields$', './internal/config']),
        ('HTTP proxy isolation', 'internal/vault/client.go', 'Proxy: nil,', 'Proxy: http.ProxyFromEnvironment,',
         ['go', 'test', '-count=1', '-run', '^TestHTTPSIgnoresProxyEnvironment$', './internal/vault']),
        ('TLS certificate verification', 'internal/vault/client.go',
         'MinVersion: tls.VersionTLS12, RootCAs: roots',
         'MinVersion: tls.VersionTLS12, RootCAs: roots, InsecureSkipVerify: true',
         ['go', 'test', '-count=1', '-run', '^TestHTTPSRejectsUntrustedCA$', './internal/vault']),
        ('typed unavailable errors', 'internal/vault/client.go',
         'return nil, fmt.Errorf("%w: check address, CA and timeout", ErrUnavailable)',
         'return nil, errors.New("untyped dial failure")',
         ['go', 'test', '-count=1', '-run', '^TestClassifiedErrors$', './internal/vault']),
        ('Transit signature verification', 'internal/vault/transit.go',
         'if !ed25519.Verify(key.PublicKey, data, signature) {', 'if false {',
         ['go', 'test', '-count=1', '-run', '^TestSignTransitRejectsWrongKey$', './internal/vault']),
        ('KV reads once per path'  , 'internal/vault/kv.go', 'c.cache[path] = data', '// mutation: do not cache',
         ['go', 'test', '-count=1', '-run', '^TestLiveAddressForms/unix-agent$', './internal/vault']),
        ('KV concurrent cold-cache fills', 'internal/vault/kv.go',
         'c.mu.Lock()\n\tdefer c.mu.Unlock()', '// mutation: no serialization',
         ['go', 'test', '-count=1', '-run', '^TestLiveAddressForms/unix-agent$', './internal/vault']),
        ('AppRole token revocation' , 'internal/vault/client.go', 'if !c.issued {', 'if true {',
         ['go', 'test', '-count=1', '-run', '^TestLiveAppRoleAndFailures$', './internal/vault']),
        ('peer identity precedence', 'internal/transport/identity.go',
         'if p, ok := peer.FromContext(ctx); ok {',
         'if name,ok:=ctx.Value(keyContext{}).(string);ok{return name}; if p, ok := peer.FromContext(ctx); ok {',
         ['go', 'test', '-count=1', '-run', '^TestCallingKey$', './internal/transport']),
        ('CI compiler module coverage', '.github/workflows/ci.yml',
         'go-version: "1.27.3"', 'go-version: "1.27.1"',
         ['python3', 'scripts/check-ci-go.py'], 'CI Go 1.27.1 is older'),
        ('missing identity stays empty' , 'internal/transport/identity.go', '\n\treturn ""\n}', '\n\treturn "attacker"\n}',
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
        for name,output in [('empty run', ''), ('arbitrary skip', '--- SKIP: TestNewReason (0.00s)\n--- PASS: TestLiveAddressForms (0.00s)\n')]:
            with tempfile.TemporaryDirectory(prefix='deck-ci-output-') as directory:
                shim = Path(directory) / 'go'
                # A controlled go process reports successful exit but produces
                # no live-test completion, or a skip with an unrelated reason.
                shim.write_text('#!/bin/sh\ncat <<' + "'EOF'" + '\n' + output + 'EOF\n')
                shim.chmod(0o755)
                env = dict(os.environ, PATH=directory + os.pathsep + os.environ['PATH'])
                require_failure(['bash', 'scripts/test-ci.sh'], 'CI requires the OpenBao tests', env)
            print(f'PASS guard: task ci rejects {name}', flush=True)
    print(f'PASS all {len(probes) + (5 if args.integration else 0)} requested guard probes completed')


if __name__ == '__main__':
    main()
