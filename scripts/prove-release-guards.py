#!/usr/bin/env python3
"""Prove release rejection checks with restored mutations. Run alone in a worktree."""
from pathlib import Path
import runpy

helpers = runpy.run_path(str(Path(__file__).with_name('prove-guards.py')))
mutate = helpers['mutate']
node = ['node', '--test', 'npm/test/distribution.test.js']
python = ['python3', '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_release.py']
helpers['require_pass'](node)
helpers['require_pass'](python)
probes = [
    ('archive checksum verification', 'npm/install.js',
     '    verifyChecksum(archive, expected);', '    // mutation: omit checksum verification', node, 'regular expression /checksum mismatch|exactly one/'),
    ('duplicate checksum rejection', 'npm/install.js',
     'if (matches.length !== 1)', 'if (matches.length === 0)', node, 'regular expression /checksum mismatch|exactly one/'),
    ('HTTPS-only download', 'npm/install.js',
     'if (current.protocol !== "https:")', 'if (false)', node, 'regular expression /HTTPS/'),
    ('handoff commit binding', 'scripts/verify-release-handoff.py',
     'if source.get("commit") != commit:', 'if False:', python, 'HandoffError not raised'),
    ('stable version guard', 'scripts/create-homebrew-formula.py',
     "if not re.fullmatch(r'(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)', version):",
     'if False:', python, 'ERROR: test_invalid_versions_and_commit_fail'),
]
for probe in probes:
    mutate(*probe)
print('PASS all 5 release guard mutations rejected and restored')
