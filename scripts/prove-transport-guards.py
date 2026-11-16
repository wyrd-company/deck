#!/usr/bin/env python3
"""Prove task 5's carried and transport guards with restored source mutations."""
import runpy
import shutil
import subprocess
import tempfile
from pathlib import Path

helpers = runpy.run_path(str(Path(__file__).with_name('prove-guards.py')))
mutate, require_pass = helpers['mutate'], helpers['require_pass']
require_pass(['go', 'test', '-count=1', './internal/config', './internal/transport'])
probes = [
    ('host public key pin', 'internal/transport/client.go',
     'if string(key.Marshal()) != string(host.Marshal()) {',
     'if false && string(key.Marshal()) != string(host.Marshal()) {',
     ['go', 'test', '-count=1', '-run', '^TestLiveSSHIdentities$', './internal/transport']),
    ('keepalive carries quiet Listen past SSH idle', 'internal/transport/client.go',
     'Time: 30 * time.Second, Timeout: 10 * time.Second',
     'Time: 2 * time.Hour, Timeout: 10 * time.Second',
     ['go', 'test', '-count=1', '-run', '^TestKeepaliveHoldsQuietListen$', './internal/transport']),
    ('configured KV mount', 'internal/vault/kv.go',
     'apiPath(ref.Mount, "data", ref.Path)', 'apiPath("secret", "data", ref.Path)',
     ['go', 'test', '-count=1', '-run', '^TestLiveHostKeyAndRotation$', './internal/transport']),
    ('latest rotated public material', 'internal/vault/transit.go',
     'keys[strconv.Itoa(version)]', 'keys["1"]',
     ['go', 'test', '-count=1', '-run', '^TestLiveHostKeyAndRotation$', './internal/transport']),
    ('localhost Vault address', 'internal/config/validate.go',
     'u.Hostname() == "localhost"', 'false',
     ['go', 'test', '-count=1', '-run', '^TestVaultLoopbackForms$', './internal/config']),
    ('IPv6 loopback Vault address', 'internal/config/validate.go',
     'ip != nil && ip.IsLoopback()', 'ip != nil && ip.To4() != nil && ip.IsLoopback()',
     ['go', 'test', '-count=1', '-run', '^TestVaultLoopbackForms$', './internal/config']),
    ('identity comes from successful signed key, not query context', 'internal/transport/server.go',
     'return byFingerprint[ssh.FingerprintSHA256(public)]',
     'name,_:=session.Context().Value("queried-key").(string);return name',
     ['go', 'test', '-count=1', '-run', '^TestSignedSessionIdentity$', './internal/transport']),
    ('close before Serve is final', 'internal/transport/server.go',
     'if s.closed {', 'if false {',
     ['go', 'test', '-count=1', '-timeout', '10s', '-run', '^TestCancellationBeforeServe$', './internal/transport']),
    ('owned listener closes before SSH registration', 'internal/transport/server.go',
     'if listener != nil {\n\t\t\t_ = listener.Close()\n\t\t}', '_ = listener',
     ['go', 'test', '-count=1', '-timeout', '10s', '-run', '^TestCloseBeforeSSHRegistersListener$', './internal/transport']),
    ('SSH handshake lifetime bounded', 'internal/transport/server.go',
     's.HandshakeTimeout = 30 * time.Second', 's.HandshakeTimeout = 0',
     ['go', 'test', '-count=1', '-run', '^TestHandshakeLifetimeBound$', './internal/transport']),
    ('shutdown joins handlers', 'internal/transport/server.go',
     'grpc.WaitForHandlers(true)', 'grpc.WaitForHandlers(false)',
     ['go', 'test', '-count=1', '-run', '^TestShutdownJoinsHandlersAndClientClose$', './internal/transport']),
    ('SSH session channel cap', 'internal/transport/server.go',
     'slots = make(chan struct{}, maxSSHChannels)', 'slots = make(chan struct{}, maxSSHChannels+1)',
     ['go', 'test', '-count=1', '-run', '^TestConnectionResourceLimits$', './internal/transport']),
    ('gRPC concurrent stream cap', 'internal/transport/server.go',
     'grpc.MaxConcurrentStreams(maxConcurrentStreams)', 'grpc.MaxConcurrentStreams(0)',
     ['go', 'test', '-count=1', '-run', '^TestConnectionResourceLimits$', './internal/transport']),
    ('all non-subsystem requests refused', 'internal/transport/server.go',
     'if request.Type != "subsystem" || ssh.Unmarshal(request.Payload, &payload) != nil || payload.Name != subsystem {', 'if false && (request.Type != "subsystem" || ssh.Unmarshal(request.Payload, &payload) != nil || payload.Name != subsystem) {',
     ['go', 'test', '-count=1', '-run', '^TestSSHProtocolBoundary$', './internal/transport']),
]
for probe in probes:
    mutate(*probe)
# Regress x/crypto's cache in an isolated module copy, never the shared cache.
root = Path(__file__).resolve().parent.parent
crypto = subprocess.check_output(['go', 'list', '-m', '-f', '{{.Dir}}', 'golang.org/x/crypto'], cwd=root, text=True).strip()
with tempfile.TemporaryDirectory(prefix='deck-callback-cache-') as directory:
    path = Path(directory)
    copied = path / 'crypto'
    shutil.copytree(crypto, copied)
    server = copied / 'ssh/server.go'
    source = server.read_text()
    if source.count('const maxCachedPubKeys = 1') != 1:
        raise RuntimeError('x/crypto callback cache target changed; re-audit the regression')
    server.chmod(0o644)
    server.write_text(source.replace('const maxCachedPubKeys = 1', 'const maxCachedPubKeys = 8'))
    modfile = path / 'go.mod'
    modfile.write_text((root / 'go.mod').read_text() + f'\nreplace golang.org/x/crypto => {copied}\n')
    shutil.copyfile(root / 'go.sum', path / 'go.sum')
    require_pass(['go', 'test', '-modfile', str(modfile), '-mod=mod', '-count=1', '-run', '^TestLivePublicKeyCallbackCache$', './internal/transport'])
    print('PASS regression: A/B/A signed identity survives x/crypto eight-entry cache', flush=True)
print(f'PASS all {len(probes)} task 5 guard probes and callback-cache regression completed', flush=True)
