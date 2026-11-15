'use strict';
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawnSync } = require('node:child_process');
const { EventEmitter } = require('node:events');
const { Readable } = require('node:stream');
const installer = require('../install');
const launcher = require('../bin/deck');

function fixture(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'deck-npm-test-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  fs.writeFileSync(path.join(dir, 'deck'), '#!/bin/sh\nprintf "%s\\n" "$@"\nexit 7\n', { mode: 0o755 });
  const archive = path.join(dir, 'deck_1.2.3_linux_x86_64.tar.gz');
  assert.equal(spawnSync('tar', ['-czf', archive, '-C', dir, 'deck']).status, 0);
  return { dir, archive, hash: crypto.createHash('sha256').update(fs.readFileSync(archive)).digest('hex') };
}

test('all supported targets select exact versioned release assets; others fail', () => {
  for (const [platform, arch, expected] of [
    ['linux', 'x64', 'linux_x86_64'], ['linux', 'arm64', 'linux_aarch64'],
    ['darwin', 'x64', 'darwin_x86_64'], ['darwin', 'arm64', 'darwin_aarch64']]) {
    assert.equal(installer.releaseUrls('1.2.3', installer.selectTarget(platform, arch)).archive,
      `https://github.com/wyrd-company/deck/releases/download/1.2.3/deck_1.2.3_${expected}.tar.gz`);
  }
  assert.throws(() => installer.selectTarget('win32', 'x64'), /unsupported/);
  assert.throws(() => installer.selectTarget('linux', 'ia32'), /unsupported/);
  for (const value of ['v1.2.3', '1.2.3-rc.1', '01.2.3', '../1']) {
    assert.throws(() => installer.releaseUrls(value, installer.selectTarget('linux', 'x64')), /version/);
  }
});

test('installs a real checksum-verified tar executable and launcher preserves args/exit', async (t) => {
  const f = fixture(t);
  const asset = path.basename(f.archive);
  const destination = await installer.install({
    packageRoot: f.dir, packageMetadata: { version: '1.2.3' },
    target: installer.selectTarget('linux', 'x64'),
    download: async (url, dest) => {
      if (url.endsWith('/checksums.txt')) fs.writeFileSync(dest, `${f.hash}  ${asset}\n`);
      else fs.copyFileSync(f.archive, dest);
    },
  });
  fs.copyFileSync(path.join(__dirname, '../bin/deck.js'), path.join(f.dir, 'bin/deck.js'));
  const result = spawnSync(process.execPath, [path.join(f.dir, 'bin/deck.js'), 'a b', '--json'], { encoding: 'utf8' });
  assert.equal(destination, path.join(f.dir, 'bin/deck-native'));
  assert.equal(result.status, 7);
  assert.equal(result.stdout, 'a b\n--json\n');
});

test('checksum mismatch and ambiguous/missing checksum fail before extraction', async (t) => {
  const f = fixture(t);
  const asset = path.basename(f.archive);
  for (const contents of [`${'0'.repeat(64)}  ${asset}\n`, `${f.hash}  ${asset}\n${f.hash}  ${asset}\n`]) {
    let extracted = false;
    await assert.rejects(installer.install({
      packageRoot: f.dir, packageMetadata: { version: '1.2.3' },
      target: installer.selectTarget('linux', 'x64'),
      download: async (url, dest) => {
        if (url.endsWith('/checksums.txt')) fs.writeFileSync(dest, contents);
        else fs.copyFileSync(f.archive, dest);
      },
      extractExecutable: () => { extracted = true; },
    }), /checksum mismatch|exactly one/);
    assert.equal(extracted, false, 'must reject before extraction');
  }
  assert.throws(() => installer.parseChecksum('', 'deck.tar.gz'), /exactly one/);
  assert.throws(() => installer.parseChecksum(`${f.hash}  deck.tar.gz\n${f.hash}  deck.tar.gz`, 'deck.tar.gz'), /exactly one/);
  assert.equal(fs.existsSync(path.join(f.dir, 'bin/deck-native')), false);
});

test('unsafe archive paths, missing and duplicate executables fail', () => {
  const target = installer.selectTarget('linux', 'x64');
  for (const entries of [['../deck'], ['/deck'], ['C:/deck'], ['x/../deck'], ['README.md'], ['deck', 'deck']]) {
    assert.throws(() => installer.validateArchiveEntries(entries, target));
  }
});

function response(status, location, body = '') {
  const result = Readable.from([body]);
  result.statusCode = status;
  result.headers = location ? { location } : {};
  return result;
}
function fakeGet(responses) {
  return (_url, _options, callback) => {
    const request = new EventEmitter();
    request.setTimeout = () => {};
    queueMicrotask(() => callback(responses.shift()));
    return request;
  };
}
test('downloads enforce HTTPS, HTTP success, redirect cap and clean partial files', async (t) => {
  const f = fixture(t);
  const dest = path.join(f.dir, 'download');
  await installer.download('https://github.com/start', dest, { get: fakeGet([
    response(302, 'https://assets.example/file'), response(200, null, 'archive')]) });
  assert.equal(fs.readFileSync(dest, 'utf8'), 'archive');
  fs.rmSync(dest);
  await assert.rejects(installer.download('http://example.com', dest), /HTTPS/);
  await assert.rejects(installer.download('https://example.com', dest, { get: fakeGet([response(404)]) }), /HTTP 404/);
  await assert.rejects(installer.download('https://example.com', dest, { get: fakeGet([response(302, 'http://example.com')]) }), /HTTPS/);
  await assert.rejects(installer.download('https://example.com', dest, { maxRedirects: 0, get: fakeGet([response(302, '/again')]) }), /redirects/);
  const broken = new Readable({ read() { this.push('partial'); this.destroy(new Error('stream broke')); } });
  broken.statusCode = 200;
  broken.headers = {};
  await assert.rejects(installer.download('https://example.com', dest, { get: fakeGet([broken]) }), /stream broke/);
  assert.equal(fs.existsSync(dest + '.partial'), false);
});

test('launcher forwards termination and returns nonzero for missing executable', () => {
  const proc = new EventEmitter();
  const child = new EventEmitter();
  let killed;
  child.kill = (signal) => { killed = signal; };
  launcher.run([], { processObject: proc, exists: () => true, spawn: () => child, platform: 'linux' });
  proc.emit('SIGTERM');
  assert.equal(killed, 'SIGTERM');
  child.emit('exit', null, 'SIGTERM');
  assert.equal(proc.exitCode, 143);
  assert.equal(proc.listenerCount('SIGTERM'), 0);
  launcher.run([], { processObject: proc, exists: () => false, error: () => {} });
  assert.equal(proc.exitCode, 1);
});
