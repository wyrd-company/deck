"""Exercise the immutable handoff and formula against concrete artifact bytes."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / (name + '.py'))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


manifest = module('create-release-manifest')
formula = module('create-homebrew-formula')
handoff = module('verify-release-handoff')


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for _, _, _, filename in manifest.expected_artifacts('1.2.3'):
            (self.root / filename).write_bytes(filename.encode())
        for arch in ['aarch64', 'x86_64']:
            (self.root / f'deck_1.2.3_darwin_{arch}.tar.gz').write_bytes(arch.encode())

    def test_six_linux_assets_and_exact_formula_hashes(self):
        value = manifest.create_manifest(self.root, '1.2.3', 'a' * 40)
        self.assertEqual(len(value['artifacts']), 6)
        self.assertEqual(value['publish']['aur']['package'], 'deck-bin')
        for artifact in value['artifacts']:
            self.assertEqual(artifact['sha256'], manifest.sha256(self.root / artifact['filename']))
        rendered = formula.render(self.root, '1.2.3')
        for goos in ['linux', 'darwin']:
            for arch in ['aarch64', 'x86_64']:
                file = self.root / f'deck_1.2.3_{goos}_{arch}.tar.gz'
                self.assertIn(file.name, rendered)
                self.assertIn(manifest.sha256(file), rendered)

    def test_missing_and_extra_linux_assets_fail(self):
        extra = self.root / 'deck_1.2.3_linux_other.tar.gz'
        extra.write_bytes(b'bad')
        with self.assertRaises(manifest.ManifestError):
            manifest.create_manifest(self.root, '1.2.3', 'a' * 40)
        extra.unlink()
        (self.root / manifest.expected_artifacts('1.2.3')[0][3]).unlink()
        with self.assertRaises(manifest.ManifestError):
            manifest.create_manifest(self.root, '1.2.3', 'a' * 40)
        with self.assertRaises(FileNotFoundError):
            formula.render(self.root, '1.2.3')

    def test_handoff_binds_source_tag_and_product(self):
        import json
        value = manifest.create_manifest(self.root, '1.2.3', 'a' * 40)
        path = self.root / 'manifest.json'
        path.write_text(json.dumps(value))
        self.assertEqual(handoff.verify(path, 'a' * 40, '1.2.3'), '1.2.3')
        for commit, version in [('b' * 40, '1.2.3'), ('a' * 40, '1.2.4')]:
            with self.assertRaises(handoff.HandoffError):
                handoff.verify(path, commit, version)
        value['product'] = 'other'
        path.write_text(json.dumps(value))
        with self.assertRaises(handoff.HandoffError):
            handoff.verify(path, 'a' * 40, '1.2.3')

    def test_invalid_versions_and_commit_fail(self):
        for version in ['v1.2.3', '01.2.3', '1.2.3-beta', '../1']:
            with self.assertRaises(manifest.ManifestError):
                manifest.create_manifest(self.root, version, 'a' * 40)
            with self.assertRaises(ValueError):
                formula.render(self.root, version)
        with self.assertRaises(manifest.ManifestError):
            manifest.create_manifest(self.root, '1.2.3', 'short')
