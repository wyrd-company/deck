#!/usr/bin/env node
'use strict';
// Install the real npm tarball in an isolated prefix, then feed the production
// installer local snapshot bytes. Only the HTTPS download is substituted: an
// unpublished snapshot cannot be downloaded from GitHub. Run on a disposable
// machine to prove the clean-host native launcher, never as a global install.
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const artifacts = path.resolve(process.argv[2] || path.join(root, 'dist'));
const version = process.argv[3] || '0.0.0';
const proof = fs.mkdtempSync(path.join(os.tmpdir(), 'deck-install-proof-'));

function run(command, argv, options = {}) {
  const result = spawnSync(command, argv, { encoding: 'utf8', ...options });
  if (result.error || result.status !== 0) {
    throw new Error(`${command} failed: ${result.error || result.stderr || result.stdout}`);
  }
  return result.stdout.trim();
}

async function main() {
  try {
    fs.cpSync(path.join(root, 'npm'), path.join(proof, 'package'), { recursive: true });
    const metadataPath = path.join(proof, 'package/package.json');
    const metadata = JSON.parse(fs.readFileSync(metadataPath, 'utf8'));
    metadata.version = version;
    fs.writeFileSync(metadataPath, JSON.stringify(metadata));
    const packed = run('npm', ['pack', '--ignore-scripts', '--pack-destination', proof, path.join(proof, 'package')]);
    const prefix = path.join(proof, 'installed');
    run('npm', ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund', '--cache', path.join(proof, 'cache'),
      '--prefix', prefix, path.join(proof, packed)]);
    const installed = path.join(prefix, 'node_modules/@wyrd-company/deck');
    const installer = require(path.join(installed, 'install.js'));
    const target = installer.selectTarget();
    const urls = installer.releaseUrls(version, target);
    await installer.install({
      packageRoot: installed,
      packageMetadata: { version },
      download: async (url, destination) => {
        assert.ok(url === urls.archive || url === urls.checksums, 'unexpected release URL');
        fs.copyFileSync(path.join(artifacts, path.basename(url)), destination);
      },
    });
    const command = path.join(prefix, 'node_modules/.bin/deck');
    assert.equal(run(command, ['--version']), version);
    const bad = spawnSync(command, ['--not-a-deck-flag'], { encoding: 'utf8' });
    assert.equal(bad.status, 2, 'launcher must preserve native usage exit status');
    console.log(`PASS npm packed/offline install selects checksum-verified ${target.os}/${target.arch}; native version ${version}; usage exit 2`);
  } finally {
    fs.rmSync(proof, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
