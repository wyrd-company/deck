#!/usr/bin/env python3
"""Prove task 5's carried and transport guards with restored source mutations."""
import runpy
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
    ('actual signed key reaches handlers', 'internal/transport/server.go',
     'ctx.SetValue(authenticatedName{}, name)', 'ctx.SetValue(authenticatedName{}, name+"offered")',
     ['go', 'test', '-count=1', '-run', '^TestLiveOfferedAndSignedKeys$', './internal/transport']),
    ('all non-subsystem requests refused', 'internal/transport/server.go',
     'if request.Type != "subsystem" || ssh.Unmarshal(request.Payload, &payload) != nil || payload.Name != subsystem {', 'if false && (request.Type != "subsystem" || ssh.Unmarshal(request.Payload, &payload) != nil || payload.Name != subsystem) {',
     ['go', 'test', '-count=1', '-run', '^TestSSHProtocolBoundary$', './internal/transport']),
]
for probe in probes:
    mutate(*probe)
print(f'PASS all {len(probes)} task 5 guard probes completed', flush=True)
