"""Run the Release workflow's npm publish command against a packed wrapper, offline."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github/workflows/release.yml'


def publish_command():
    job = WORKFLOW.read_text().split('\n  npm:\n', 1)[1].split('\n  homebrew:\n', 1)[0]
    commands = re.findall(r'^ +- run: (npm publish .*)$', job, re.MULTILINE)
    if len(commands) != 1:
        raise AssertionError(f'npm job must run exactly one npm publish, found {commands}')
    return commands[0]


class ReleaseNpmPublishTest(unittest.TestCase):
    def setUp(self):
        # Every HTTP(S) request npm or git makes goes through this proxy, which
        # records it and refuses it. publishConfig.registry in npm/package.json
        # overrides any registry setting, so only offline mode keeps npm away
        # from the live registry and its published versions.
        self.requests = []
        requests = self.requests

        class Proxy(BaseHTTPRequestHandler):
            def refuse(self):
                requests.append(f'{self.command} {self.path}')
                self.send_response(403)
                self.end_headers()

            do_CONNECT = do_GET = do_HEAD = do_POST = do_PUT = refuse

            def log_message(self, *arguments):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Proxy)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.proxy = f'http://127.0.0.1:{server.server_address[1]}'

    def environment(self, work):
        inherited = {key: value for key, value in os.environ.items()
                     if not key.lower().startswith('npm_config_')
                     and key.lower() not in {'no_proxy', 'node_auth_token'}}
        return {
            **inherited,
            'npm_config_cache': str(work / 'cache'),
            'npm_config_userconfig': os.devnull,
            'npm_config_offline': 'true',
            'npm_config_update_notifier': 'false',
            # npm skips its update check by itself when it detects CI; CI=false
            # turns that detection off, so the setting above is what disables it.
            'CI': 'false',
            'npm_config_proxy': self.proxy,
            'npm_config_https_proxy': self.proxy,
            'HTTP_PROXY': self.proxy,
            'HTTPS_PROXY': self.proxy,
            'GIT_CONFIG_GLOBAL': os.devnull,
            'GIT_CONFIG_NOSYSTEM': '1',
            'GIT_SSH_COMMAND': 'false',
            'GIT_TERMINAL_PROMPT': '0',
        }

    def test_publish_command_resolves_the_packed_tarball(self):
        command = publish_command()
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            env = self.environment(work)
            (work / 'dist').mkdir()
            subprocess.run(['npm', 'pack', '--ignore-scripts', '--pack-destination', 'dist', str(ROOT / 'npm')],
                           cwd=work, env=env, check=True, capture_output=True, text=True, timeout=120)
            result = subprocess.run(['bash', '-c', f'{command} --dry-run'], cwd=work, env=env,
                                    capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0,
                         f'npm publish must read the packed tarball as a file\n{result.stderr}')
        self.assertIn('+ @wyrd-company/deck@', result.stdout)
        self.assertEqual(self.requests, [], 'npm publish dry run must not reach the network')


if __name__ == '__main__':
    unittest.main()
