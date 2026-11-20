#!/usr/bin/env python3
"""Generate acceptance reference and published definitions; --check detects drift."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs/user"
SPEC = ROOT / "docs/specifications/assets"


def page(title, body):
    return f"---\ntitle: {title}\nproduct: deck\n---\n\n# {title}\n\n" + body.rstrip() + "\n"


def expected():
    files = {}
    for name, source in [
        ("client", SPEC / "configuration/client.schema.json"),
        ("service", SPEC / "configuration/service.schema.json"),
        ("activity", SPEC / "events/activity.schema.json"),
    ]:
        schema = json.loads(source.read_text())
        download = f"definitions/{name}.schema.json"
        files[download] = source.read_bytes()
        body = f"Generated from the {name} JSON Schema. [Download the schema]({download}).\n\n"
        body += "The following entries preserve every constraint, description and default from the definition.\n\n"
        for key, value in schema.items():
            if key in ("properties", "$defs"):
                for field, definition in value.items():
                    body += f"## {key}: {field}\n\n```json\n{json.dumps(definition, indent=2)}\n```\n\n"
            else:
                body += f"## {key}\n\n```json\n{json.dumps(value, indent=2)}\n```\n\n"
        files[f"{name}-reference.md"] = page(f"{name.capitalize()} schema reference", body).encode()

    for source in sorted((SPEC / "configuration/examples").glob("*.yml")):
        files[f"examples/{source.name}"] = source.read_bytes()
    for source in sorted((SPEC / "events/examples").glob("*.json")):
        files[f"examples/{source.name}"] = source.read_bytes()

    # Preserve native import paths in the published proto tree. Google's
    # well-known types are resolved by protobuf tooling's standard include path.
    files["definitions/deck/v1/deck.proto"] = (SPEC / "grpc-api/deck/v1/deck.proto").read_bytes()
    proto = (SPEC / "grpc-api/deck/v1/deck.proto").read_text()
    body = "Generated from the service definition. [Download deck.proto](definitions/deck/v1/deck.proto).\n\n"
    body += "Uses the standard protobuf `google/protobuf/struct.proto` and `google/protobuf/timestamp.proto` imports.\n\n"
    # Each top-level declaration retains the exact fields, RPC signatures,
    # comments and nested declarations. Braces delimit native proto syntax.
    lines = proto.splitlines()
    start = 0
    while start < len(lines):
        match = re.match(r"(service|message|enum) (\w+)", lines[start])
        if not match:
            start += 1
            continue
        end = start
        depth = 0
        while end < len(lines):
            depth += lines[end].count("{") - lines[end].count("}")
            end += 1
            if depth == 0:
                break
        comment = start
        while comment and lines[comment - 1].startswith("//"):
            comment -= 1
        declaration = "\n".join(lines[comment:end])
        body += f"## {match[2]}\n\n```protobuf\n{declaration}\n```\n\n"
        start = end
    files["api-reference.md"] = page("gRPC API reference", body).encode()

    # The parser is the source while no raw CLI definition exists. Running its
    # help also keeps the command signatures and examples tied to the real CLI.
    flags = (ROOT / "internal/cli/flags.go").read_text()
    options = flags.split("var options = map[string]string{", 1)[1].split("\n}", 1)[0]
    commands = re.findall(r'"([a-z-]+)":\s*"[^"]*"', options)
    commands += ["config validate", "config key", "service start", "service validate", "service health", "service host-key"]
    body = "Generated from the running CLI's parser and help.\n\n"
    with tempfile.TemporaryDirectory(prefix="agent.", dir="/tmp") as temporary:
        binary = Path(temporary) / "deck"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/deck"], cwd=ROOT, check=True)
        for command in [""] + commands:
            result = subprocess.run([str(binary), *command.split(), "--help"], cwd=ROOT, check=True, capture_output=True, text=True)
            body += f"## {command or 'deck'}\n\n```text\n{result.stdout.rstrip()}\n```\n\n"
    files["cli-reference.md"] = page("Command reference", body).encode()
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    files = expected()
    # The manifest lets the drift check detect obsolete generated pages too.
    manifest = "generated-files.json"
    files[manifest] = (json.dumps(sorted(files), indent=2) + "\n").encode()
    prior = json.loads((DOCS / manifest).read_text()) if (DOCS / manifest).exists() else []
    obsolete = set(prior) - files.keys()
    drift = sorted(path for path, data in files.items() if not (DOCS / path).exists() or (DOCS / path).read_bytes() != data)
    if args.check:
        if obsolete or drift:
            raise SystemExit("Generated user documentation differs: " + ", ".join(sorted(obsolete | set(drift))))
        print(f"PASS: {len(files)} generated pages, definition copies and examples match their sources")
        return
    for path in obsolete:
        (DOCS / path).unlink(missing_ok=True)
    for path, data in files.items():
        target = DOCS / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    print(f"Generated {len(files)} user documentation files")


if __name__ == "__main__":
    main()
