"""Use real Git HTTP requests to prove checkout/global auth cannot admit private tags."""
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
GATE = ROOT / 'scripts/check-release-source.sh'
AUTH = 'Basic ' + base64.b64encode(b'fixture:checkout-token').decode()


def packet(data):
    return f'{len(data) + 4:04x}'.encode() + data


class ReleaseSourceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.requests = []
        self.public = False
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                authorization = self.headers.get('Authorization')
                fixture.requests.append(authorization)
                if not fixture.public and authorization != AUTH:
                    self.send_response(401)
                    self.send_header('WWW-Authenticate', 'Basic realm="private fixture"')
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header('Content-Type', 'application/x-git-upload-pack-advertisement')
                self.end_headers()
                self.wfile.write(packet(b'# service=git-upload-pack\n') + b'0000' +
                                 packet(b'a' * 40 + b' refs/tags/1.2.3\x00object-format=sha1\n') + b'0000')

            def log_message(self, *_args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.url = f'http://127.0.0.1:{self.server.server_port}/deck.git'
        self.checkout = self.root / 'checkout'
        self.checkout.mkdir()
        # Read-only Git operations do not need an operator. No release tag or
        # fixture commit is created; HTTP advertises a synthetic tag for ls-remote.
        # Use system Git for read-only fixture operations. A workspace Git shim
        # may rediscover our transport wrapper on PATH and recurse into it.
        self.git = shutil.which('git', path=os.defpath)
        self.assertIsNotNone(self.git, 'system Git is required for the HTTP fixture')
        subprocess.run([self.git, 'init', '--quiet', str(self.checkout)], check=True)
        subprocess.run([self.git, '-C', str(self.checkout), 'config', 'http.extraheader',
                        'Authorization: ' + AUTH], check=True)
        self.home = self.root / 'hostile-home'
        self.home.mkdir()
        (self.home / '.gitconfig').write_text('[http]\n\textraheader = Authorization: ' + AUTH + '\n')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        # Only substitute the transport endpoint. The actual Git executable,
        # arguments, working directory, environment and auth handling stay real.
        wrapper = self.bin / 'git'
        wrapper.write_text('#!/usr/bin/env python3\nimport os,sys\n' +
                           'args = [' + json.dumps(self.url) + ' if x == "https://github.com/wyrd-company/deck.git" else x for x in sys.argv[1:]]\n' +
                           'os.execv(' + json.dumps(self.git) + ', [' + json.dumps(self.git) + '] + args)\n')
        wrapper.chmod(0o755)
        self.environment = os.environ | {
            'PATH': str(self.bin) + os.pathsep + os.environ['PATH'],
            'HOME': str(self.home),
            'GIT_TERMINAL_PROMPT': '0',
            'GIT_CONFIG_COUNT': '1',
            'GIT_CONFIG_KEY_0': 'http.extraheader',
            'GIT_CONFIG_VALUE_0': 'Authorization: ' + AUTH,
        }

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def gate(self, version='1.2.3'):
        sentinel = self.root / 'publish-reached'
        result = subprocess.run(['bash', '-c', 'bash "$1" "$2" && touch "$3"', '_',
                                 str(GATE), version, str(sentinel)], cwd=self.checkout,
                                env=self.environment, text=True, capture_output=True, timeout=15)
        return result, sentinel

    def test_private_source_never_reaches_publish_despite_checkout_and_inherited_auth(self):
        # Control: this private fixture admits ordinary authenticated Git.
        control = subprocess.run([self.git, 'ls-remote', '--exit-code', self.url, 'refs/tags/1.2.3'],
                                 cwd=self.checkout, env=self.environment, capture_output=True, timeout=15)
        self.assertEqual(control.returncode, 0, control.stderr)
        self.assertIn(AUTH, self.requests)
        self.requests.clear()
        result, sentinel = self.gate()
        self.assertFalse(sentinel.exists(), 'private source reached the publish sentinel')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('must be publicly accessible without credentials', result.stderr)
        self.assertTrue(self.requests, 'gate must make a real Git HTTP request')
        self.assertTrue(all(header is None for header in self.requests), self.requests)

    def test_public_tag_reaches_publish_anonymously(self):
        self.public = True
        result, sentinel = self.gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(sentinel.exists())
        self.assertIn('PASS release source tag 1.2.3 resolves anonymously', result.stdout)
        self.assertTrue(self.requests)
        self.assertTrue(all(header is None for header in self.requests), self.requests)

    def test_missing_public_tag_does_not_reach_publish(self):
        self.public = True
        result, sentinel = self.gate('1.2.4')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(sentinel.exists())
        self.assertTrue(self.requests)

    def test_github_release_body_uses_intentional_notes(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        self.assertIn('--release-notes=${{ runner.temp }}/release-notes.md', workflow)
        # GoReleaser loads --release-notes in its changelog pipe, which
        # changelog.disable skips, leaving the release body empty.
        config = (ROOT / '.goreleaser.yaml').read_text()
        changelog = re.search(r'^changelog:\n((?:[ #].*\n|\n)*)', config, re.MULTILINE)
        disabled = changelog and re.search(r'^\s+disable:\s*(?!false\b)\S', changelog.group(1), re.MULTILINE)
        self.assertFalse(disabled, 'changelog.disable drops --release-notes from the GitHub release')

    def test_docs_publish_follows_release_or_a_manual_run_only(self):
        workflow = (ROOT / '.github/workflows/publish-docs.yml').read_text()
        triggers = workflow.split('\non:\n', 1)[1].split('\npermissions:', 1)[0]
        self.assertEqual(re.findall(r'^  ([a-z_]+):', triggers, re.MULTILINE), ['workflow_run', 'workflow_dispatch'],
                         'only a Release run or a manual run may publish docs')
        self.assertIn('      ref:\n', triggers)
        self.assertIn('        required: true\n', triggers)
        guard = workflow.split('    if: >-\n', 1)[1].split('    uses:', 1)[0]
        for condition in ("github.event_name == 'workflow_dispatch'",
                          "github.event.workflow_run.conclusion == 'success'",
                          "github.event.workflow_run.event == 'push'",
                          'github.event.workflow_run.head_repository.full_name == github.repository'):
            self.assertIn(condition, guard)
        self.assertIn('ref: ${{ inputs.ref || github.event.workflow_run.head_branch }}', workflow)

    def test_workflow_gates_every_publishing_job(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        compatibility = workflow.split('  compatibility:\n', 1)[1].split('  release:\n', 1)[0]
        checkout = compatibility.split('      - name: Check anonymous release tag access', 1)[0]
        self.assertIn('persist-credentials: false', checkout, 'checkout must not persist its token')
        self.assertIn('run: bash scripts/check-release-source.sh "$RELEASE_TAG"', compatibility,
                      'compatibility must run the anonymous source gate')
        self.assertLess(compatibility.index('scripts/check-release-source.sh'),
                        compatibility.index('Build and exercise the real pinned publisher'))
        for job, dependency in [('release', 'compatibility'), ('npm', 'release'),
                                ('homebrew', 'release'), ('image', 'release')]:
            section = workflow.split(f'  {job}:\n', 1)[1].split('    steps:', 1)[0]
            self.assertIn(f'    needs: {dependency}\n', section, job)
