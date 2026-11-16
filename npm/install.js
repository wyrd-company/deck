#!/usr/bin/env node

"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const https = require("node:https");
const os = require("node:os");
const path = require("node:path");
const { pipeline } = require("node:stream/promises");
const { spawnSync } = require("node:child_process");

const REPOSITORY = "wyrd-company/deck";
const MAX_REDIRECTS = 5;
const DOWNLOAD_TIMEOUT_MS = 30_000;
const MAX_ARCHIVE_METADATA_BYTES = 8 * 1024 * 1024;
const MAX_EXECUTABLE_BYTES = 64 * 1024 * 1024;
const TARGETS = Object.freeze({
  "linux/x64": { os: "linux", arch: "x86_64", executable: "deck" },
  "linux/arm64": { os: "linux", arch: "aarch64", executable: "deck" },
  "darwin/x64": { os: "darwin", arch: "x86_64", executable: "deck" },
  "darwin/arm64": { os: "darwin", arch: "aarch64", executable: "deck" },
});

function selectTarget(platform = process.platform, architecture = process.arch) {
  const key = `${platform}/${architecture}`;
  const target = TARGETS[key];
  if (!target) {
    throw new Error(
      `unsupported platform ${key}; use Linux or macOS on x64/arm64, or build deck from source`,
    );
  }
  return target;
}

function releaseUrls(version, target) {
  if (!/^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$/.test(version)) {
    throw new Error(`package version ${version} is not plain Semantic Versioning`);
  }
  const base = `https://github.com/${REPOSITORY}/releases/download/${version}`;
  return {
    archive: `${base}/deck_${version}_${target.os}_${target.arch}.tar.gz`,
    checksums: `${base}/checksums.txt`,
  };
}

function requestResponse(url, get = https.get, timeout = DOWNLOAD_TIMEOUT_MS) {
  return new Promise((resolve, reject) => {
    const request = get(
      url,
      { headers: { "User-Agent": "deck-npm-installer" } },
      resolve,
    );
    request.once("error", reject);
    if (typeof request.setTimeout === "function") {
      request.setTimeout(timeout, () => {
        request.destroy(new Error(`download timed out after ${timeout}ms`));
      });
    }
  });
}

async function download(url, destination, options = {}) {
  const get = options.get || https.get;
  const maxRedirects = options.maxRedirects ?? MAX_REDIRECTS;
  const timeout = options.timeout ?? DOWNLOAD_TIMEOUT_MS;
  let current = new URL(url);

  for (let redirects = 0; ; redirects += 1) {
    if (current.protocol !== "https:") {
      throw new Error(`refusing non-HTTPS download URL ${current}`);
    }

    const response = await requestResponse(current, get, timeout);
    const location = response.headers.location;
    if (response.statusCode >= 300 && response.statusCode < 400 && location) {
      response.resume();
      if (redirects >= maxRedirects) {
        throw new Error(`download exceeded ${maxRedirects} redirects`);
      }
      current = new URL(location, current);
      continue;
    }
    if (response.statusCode !== 200) {
      response.resume();
      throw new Error(`download failed with HTTP ${response.statusCode} for ${current}`);
    }

    const temporary = `${destination}.partial`;
    try {
      await pipeline(response, fs.createWriteStream(temporary, { flags: "wx" }));
      fs.renameSync(temporary, destination);
      return;
    } catch (error) {
      fs.rmSync(temporary, { force: true });
      throw error;
    }
  }
}

function parseChecksum(contents, asset) {
  const matches = [];
  for (const line of contents.split(/\r?\n/)) {
    const match = /^([a-fA-F0-9]{64})\s+\*?(.+)$/.exec(line.trim());
    if (match && match[2] === asset) {
      matches.push(match[1].toLowerCase());
    }
  }
  if (matches.length !== 1) {
    throw new Error(`checksums.txt must contain exactly one checksum for ${asset}`);
  }
  return matches[0];
}

function sha256(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function verifyChecksum(file, expected) {
  const actual = sha256(file);
  if (!crypto.timingSafeEqual(Buffer.from(actual), Buffer.from(expected))) {
    throw new Error(`checksum mismatch for ${path.basename(file)}`);
  }
}

function runTar(args, options = {}) {
  const encoding = Object.hasOwn(options, "encoding") ? options.encoding : "utf8";
  const command = "tar";
  const maxBuffer = options.maxBuffer || MAX_ARCHIVE_METADATA_BYTES;
  const result = (options.spawnSync || spawnSync)(command, args, {
    encoding,
    maxBuffer,
  });
  if (result.error) {
    if (result.error.code === "ENOBUFS") {
      throw new Error(`archive output exceeds the ${maxBuffer}-byte safety limit`);
    }
    throw new Error(`failed to run tar: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const detail = Buffer.isBuffer(result.stderr)
      ? result.stderr.toString("utf8").trim()
      : String(result.stderr || "").trim();
    throw new Error(`tar failed${detail ? `: ${detail}` : ""}`);
  }
  return result.stdout;
}

function listArchive(archive, options = {}) {
  const output = runTar(["-tzf", archive], options);
  return String(output)
    .split(/\r?\n/)
    .filter(Boolean);
}

function validateArchiveEntries(entries, target) {
  const expected = target.executable;
  let executableCount = 0;

  for (const entry of entries) {
    const normalized = entry.replaceAll("\\", "/").replace(/\/$/, "");
    const parts = normalized.split("/");
    if (
      !normalized ||
      normalized.startsWith("/") ||
      /^[A-Za-z]:/.test(normalized) ||
      parts.includes("..") ||
      parts.includes(".")
    ) {
      throw new Error(`archive contains an unsafe path: ${entry}`);
    }
    if (normalized === expected) {
      executableCount += 1;
    }
  }

  if (executableCount !== 1) {
    throw new Error(`archive must contain exactly one ${expected}`);
  }
  return expected;
}

function extractExecutable(archive, target, destination, options = {}) {
  const entry = validateArchiveEntries(listArchive(archive, options), target);
  const contents = runTar(
    ["-xOzf", archive, entry],
    { ...options, encoding: null, maxBuffer: MAX_EXECUTABLE_BYTES },
  );
  if (!Buffer.isBuffer(contents) || contents.length === 0) {
    throw new Error(`archive member ${entry} is empty`);
  }

  fs.mkdirSync(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.partial`;
  try {
    fs.writeFileSync(temporary, contents, { flag: "wx", mode: 0o755 });
    if (process.platform !== "win32") {
      fs.chmodSync(temporary, 0o755);
    }
    fs.rmSync(destination, { force: true });
    fs.renameSync(temporary, destination);
  } catch (error) {
    fs.rmSync(temporary, { force: true });
    throw error;
  }
}

async function install(options = {}) {
  const packageRoot = options.packageRoot || __dirname;
  const packageMetadata = options.packageMetadata || require("./package.json");
  const target = options.target || selectTarget();
  const urls = releaseUrls(packageMetadata.version, target);
  const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "deck-install-"));
  const checksumFile = path.join(temporaryDirectory, "checksums.txt");
  const archive = path.join(temporaryDirectory, path.basename(urls.archive));
  const nativeName = process.platform === "win32" ? "deck-native.exe" : "deck-native";
  const destination = path.join(packageRoot, "bin", nativeName);

  try {
    console.log(`deck: downloading ${path.basename(urls.archive)}`);
    await (options.download || download)(urls.checksums, checksumFile);
    await (options.download || download)(urls.archive, archive);
    const expected = parseChecksum(fs.readFileSync(checksumFile, "utf8"), path.basename(urls.archive));
    verifyChecksum(archive, expected);
    (options.extractExecutable || extractExecutable)(archive, target, destination);
    if (!fs.existsSync(destination)) {
      throw new Error("installer completed without a deck executable");
    }
    console.log("deck: installed checksum-verified native executable");
    return destination;
  } finally {
    fs.rmSync(temporaryDirectory, { recursive: true, force: true });
  }
}

if (require.main === module) {
  install().catch((error) => {
    console.error(`deck: install failed: ${error.message}`);
    process.exitCode = 1;
  });
}

module.exports = {
  DOWNLOAD_TIMEOUT_MS,
  MAX_EXECUTABLE_BYTES,
  MAX_REDIRECTS,
  TARGETS,
  download,
  extractExecutable,
  install,
  listArchive,
  parseChecksum,
  releaseUrls,
  selectTarget,
  validateArchiveEntries,
  verifyChecksum,
};
