// Compute only: inspect the publisher's output without writing or publishing it.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { posix, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const [repo, publisher] = process.argv.slice(2);
assert(repo && publisher, "usage: check-published-assets.mjs <repo> <wyrd.foo snapshot>");
const moduleUrl = (path) => pathToFileURL(resolve(publisher, path)).href;
const { computeRepo } = await import(moduleUrl("publisher/compute.ts"));
const { workingTreeFiles, gitTags } = await import(moduleUrl("publisher/repo-files.ts"));
const { parse } = createRequire(moduleUrl("package.json"))("yaml");
const metadata = parse(readFileSync(resolve(repo, "docs/user/docs.yml"), "utf8"));
const result = await computeRepo({
  ownerRepo: "wyrd-company/deck", files: workingTreeFiles(repo),
  tags: gitTags(repo), strict: false,
});
assert(!result.failed, JSON.stringify(result.findings));
assert(result.products.length > 0, "Publisher computed no products");
for (const asset of metadata.assets) {
  const emitted = result.products.some((product) => {
    const relative = posix.relative(product.tool["docs-path"], `docs/user/${asset}`);
    return !relative.startsWith("../") && product.files.has(`docs/${relative}`);
  });
  assert(emitted, `Listed asset not published: ${asset}`);
}
console.log(JSON.stringify({
  listedAssets: metadata.assets,
  products: result.products.map((product) => ({ key: product.key, files: [...product.files.keys()].sort() })),
  findings: result.findings,
}, null, 2));
console.log(`PASS: computeRepo emits all ${metadata.assets.length} listed assets`);
