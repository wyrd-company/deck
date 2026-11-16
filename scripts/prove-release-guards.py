#!/usr/bin/env python3
"""Prove release rejection checks with restored mutations. Run alone in a worktree."""
from pathlib import Path
import runpy

helpers = runpy.run_path(str(Path(__file__).with_name('prove-guards.py')))
mutate = helpers['mutate']
node = ['node', '--test', 'npm/test/distribution.test.js']
python = ['python3', '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_release.py']
source = ['python3', '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_release_source.py']
npm_publish = ['python3', '-m', 'unittest', 'discover', '-s', 'scripts/tests', '-p', 'test_release_npm.py']
helpers['require_pass'](node)
helpers['require_pass'](python)
helpers['require_pass'](source)
helpers['require_pass'](npm_publish)
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
    ('anonymous source ignores inherited credentials', 'scripts/check-release-source.sh',
     'env -i PATH=', 'env PATH=', source, 'private source reached the publish sentinel'),
    ('anonymous source ignores checkout credentials', 'scripts/check-release-source.sh',
     'git -C "$probe" ls-remote', 'git ls-remote', source, 'private source reached the publish sentinel'),
    ('compatibility runs anonymous source gate', '.github/workflows/release.yml',
     'run: bash scripts/check-release-source.sh "$RELEASE_TAG"', 'run: echo skipped',
     source, 'compatibility must run the anonymous source gate'),
    ('compatibility checkout cannot persist credentials', '.github/workflows/release.yml',
     '          fetch-depth: 0\n          persist-credentials: false',
     '          fetch-depth: 0\n          persist-credentials: true',
     source, 'checkout must not persist its token'),
    ('npm publish reads the packed tarball as a file', '.github/workflows/release.yml',
     'npm publish ./dist/wyrd-company-deck-*.tgz', 'npm publish dist/wyrd-company-deck-*.tgz',
     npm_publish, 'npm publish must read the packed tarball as a file'),
    ('npm publish guard stays offline', 'scripts/tests/test_release_npm.py',
     "            'npm_config_offline': 'true',\n", '',
     npm_publish, 'npm publish dry run must not reach the network'),
    ('npm guard suppresses the update check', 'scripts/tests/test_release_npm.py',
     "            'npm_config_update_notifier': 'false',\n", '',
     npm_publish, 'npm publish dry run must not reach the network'),
]
for probe in probes:
    mutate(*probe)
print(f'PASS all {len(probes)} release guard mutations rejected and restored')
