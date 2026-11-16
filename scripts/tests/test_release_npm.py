"""Run the Release workflow's npm publish command against a packed wrapper, offline."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
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
    def test_publish_command_resolves_the_packed_tarball(self):
        command = publish_command()
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            env = {
                **os.environ,
                'npm_config_cache': str(work / 'cache'),
                'npm_config_userconfig': os.devnull,
                'npm_config_registry': 'http://127.0.0.1:9/',
                'GIT_CONFIG_GLOBAL': os.devnull,
                'GIT_CONFIG_NOSYSTEM': '1',
                'GIT_SSH_COMMAND': 'false',
                'GIT_TERMINAL_PROMPT': '0',
            }
            env.pop('NODE_AUTH_TOKEN', None)
            (work / 'dist').mkdir()
            subprocess.run(['npm', 'pack', '--ignore-scripts', '--pack-destination', 'dist', str(ROOT / 'npm')],
                           cwd=work, env=env, check=True, capture_output=True, text=True, timeout=120)
            result = subprocess.run(['bash', '-c', f'{command} --dry-run'], cwd=work, env=env,
                                    capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0,
                         f'npm publish must read the packed tarball as a file\n{result.stderr}')
        self.assertIn('+ @wyrd-company/deck@', result.stdout)


if __name__ == '__main__':
    unittest.main()
