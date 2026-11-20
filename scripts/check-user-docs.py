#!/usr/bin/env python3
"""Check the acceptance pages, published assets and example payloads."""

import json
import os
from pathlib import Path
import re
import subprocess

from jsonschema import Draft202012Validator, FormatChecker
import yaml

ROOT = Path(__file__).resolve().parents[1] / "docs/user"
metadata = yaml.safe_load((ROOT / "docs.yml").read_text())
pages = {path.stem: path for path in ROOT.rglob("*.md") if path.name != "CHANGELOG.md"}
nav = metadata["products"]["deck"]["nav"]
assert [section["section"] for section in nav] == ["Users", "Developers"]
listed = []
for section in nav:
    assert [group["group"] for group in section["groups"]] == ["Tutorials", "How-to guides", "Explanation", "Reference"]
    for group in section["groups"]:
        listed.extend(group["pages"])
assert len(listed) == len(set(listed)), "Each page has exactly one home"
assert set(listed) == set(pages), "Navigation includes every published page"
assets = set(metadata["assets"])


def anchors(path):
    result = set()
    counts = {}
    fenced = False
    for line in path.read_text().splitlines():
        if line.startswith("```"):
            fenced = not fenced
        if fenced or not line.startswith("#"):
            continue
        heading = re.sub(r"^#+\s+", "", line).strip().lower()
        heading = re.sub(r"[^\w\- ]", "", heading).replace(" ", "-")
        number = counts.get(heading, 0)
        counts[heading] = number + 1
        result.add(heading + (f"-{number}" if number else ""))
    return result


for asset in assets:
    assert (ROOT / asset).is_file(), f"Missing published asset: {asset}"
design = re.compile(r"(?:specifications|technical-designs|concepts|decisions|domain-models)/", re.I)
for slug, path in pages.items():
    text = path.read_text()
    frontmatter = yaml.safe_load(text.split("---", 2)[1])
    assert frontmatter["product"] == "deck"
    assert frontmatter["title"]
    assert not design.search(text), f"Design document exposed in {slug}"
    for target in re.findall(r"\]\(([^)]+)\)", text):
        if target.startswith(("http://", "https://")):
            continue
        name, separator, fragment = target.partition("#")
        destination = (path.parent / name).resolve() if name else path
        assert destination.is_relative_to(ROOT), f"Link leaves published folder: {target}"
        assert destination.is_file(), f"Missing link: {slug}: {target}"
        if separator and destination.suffix == ".md":
            assert fragment in anchors(destination), f"Missing fragment: {slug}: {target}"
        if destination.suffix != ".md":
            assert destination.relative_to(ROOT).as_posix() in assets, f"Download not published: {target}"
schema = json.loads((ROOT / "definitions/activity.schema.json").read_text())
validator = Draft202012Validator(schema, format_checker=FormatChecker())
for example in sorted((ROOT / "examples").glob("*.json")):
    validator.validate(json.loads(example.read_text()))
for name in ("client", "service"):
    schema = json.loads((ROOT / f"definitions/{name}.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(yaml.safe_load((ROOT / f"examples/{name}.yml").read_text()))
publisher = os.environ.get("WYRD_FOO_DIR")
assert publisher, "Set WYRD_FOO_DIR to a wyrd.foo checkout with installed dependencies"
subprocess.run(["node", str(ROOT.parents[1] / "scripts/check-published-assets.mjs"),
                str(ROOT.parents[1]), str(Path(publisher).resolve())], check=True)
print(f"PASS: {len(pages)} pages in audience/kind navigation; {len(assets)} assets exist; no design links; all published examples validate")
