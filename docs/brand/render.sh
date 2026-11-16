#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
brand_dir=$(mktemp -d)
trap 'rm -rf "$brand_dir"' EXIT
npm install --prefix "$brand_dir" --no-audit --no-fund --ignore-scripts @resvg/resvg-js@2.6.2 >/dev/null
node --input-type=commonjs - "$repo_root" "$brand_dir" <<'JS'
const fs = require('node:fs');
const path = require('node:path');
const [repo, scratch] = process.argv.slice(2);
const { Resvg } = require(path.join(scratch, 'node_modules/@resvg/resvg-js'));
const svg = fs.readFileSync(path.join(repo, 'docs/brand/social-preview.svg'));
const image = new Resvg(svg, { font: { loadSystemFonts: true } });
fs.writeFileSync(path.join(repo, 'docs/user/assets/social-preview.png'), image.render().asPng());
console.log('brand: wrote docs/user/assets/social-preview.png');
JS
