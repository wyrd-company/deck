"""Checks deck's design documents.

Run through docs/check.sh, which supplies PyYAML and jsonschema. Each check
prints one line; the run ends with "docs check: pass" only when every check
passed, and exits non-zero otherwise.
"""

import json
import pathlib
import sys

import jsonschema
import yaml

DOCS = pathlib.Path(__file__).resolve().parent
ONTOLOGY = "https://refinery.systems/ontology/"
FOLDERS = {
    "assumptions": "assumption",
    "concepts": "concept",
    "decisions": "decision-record",
    "domain-models": "domain-model",
    "risks": "risk",
    "specifications": "specification",
    "spikes": "spike",
    "technical-designs": "technical-design",
    "terms": "term",
}
ASSETS = DOCS / "specifications" / "assets"
SCHEMAS = {
    "configuration/client.schema.json": ("configuration/examples/client.yml", "configuration/invalid/client-*.yml"),
    "configuration/service.schema.json": ("configuration/examples/service.yml", "configuration/invalid/service-*.yml"),
    "events/activity.schema.json": ("events/examples/*.json", None),
}
SHARED_DEFS = ["name", "snowflake", "duration", "absolute_path", "authorized_key", "kv_reference", "vault"]

failures = []


def check(name, ok, detail=""):
    print(("ok   " if ok else "FAIL ") + name + (": " + detail if detail and not ok else ""))
    if not ok:
        failures.append(name)


def load(path):
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        return json.loads(text)
    return yaml.safe_load(text)


def documents():
    found = {}
    for folder, kind in FOLDERS.items():
        for path in sorted((DOCS / folder).glob("*.yml")):
            found[path.stem] = (path, kind)
    return found


def relationship_slugs(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from relationship_slugs(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from relationship_slugs(item)


def lists_with_maps(value, path="$"):
    """Yields the path of every list item that is a mapping. A list item in a
    design document is prose; a mapping there is a plain scalar that YAML read
    as a key because it held ": "."""
    if isinstance(value, list):
        for index, item in enumerate(value):
            if isinstance(item, dict):
                yield f"{path}[{index}]"
            yield from lists_with_maps(item, f"{path}[{index}]")
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from lists_with_maps(item, f"{path}.{key}")


def main():
    docs = documents()
    check("design documents found", len(docs) > 0)
    for slug, (path, kind) in docs.items():
        rel = path.relative_to(DOCS)
        try:
            document = load(path)
        except yaml.YAMLError as error:
            check(f"{rel} parses", False, str(error).splitlines()[0])
            continue
        check(f"{rel} is a {kind}", isinstance(document, dict) and document.get("$schema") == ONTOLOGY + kind,
              f"$schema is {document.get('$schema') if isinstance(document, dict) else None!r}")
        if not isinstance(document, dict):
            continue
        accidental = list(lists_with_maps(document))
        check(f"{rel} list items are prose", not accidental, ", ".join(accidental))
        missing = sorted(set(relationship_slugs(document.get("relationships", {}))) - set(docs))
        for key in ("terms",):
            missing += sorted(set(document.get(key, []) or []) - set(docs))
        check(f"{rel} relationships resolve", not missing, ", ".join(missing))
        if kind == "risk":
            check(f"{rel} slug matches its file", document.get("slug") == slug, str(document.get("slug")))

    for schema_name, (valid_glob, invalid_glob) in SCHEMAS.items():
        schema_path = ASSETS / schema_name
        schema = load(schema_path)
        try:
            jsonschema.Draft202012Validator.check_schema(schema)
            check(f"{schema_name} is a valid 2020-12 schema", True)
        except jsonschema.SchemaError as error:
            check(f"{schema_name} is a valid 2020-12 schema", False, error.message)
            continue
        validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
        valid = sorted(ASSETS.glob(valid_glob))
        check(f"{schema_name} has examples", len(valid) > 0)
        for path in valid:
            errors = [f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in validator.iter_errors(load(path))]
            check(f"{path.relative_to(ASSETS)} validates", not errors, "; ".join(errors))
        if invalid_glob:
            invalid = sorted(ASSETS.glob(invalid_glob))
            check(f"{schema_name} has invalid documents", len(invalid) > 0)
            for path in invalid:
                check(f"{path.relative_to(ASSETS)} is rejected", not validator.is_valid(load(path)))

    client = load(ASSETS / "configuration/client.schema.json")["$defs"]
    service = load(ASSETS / "configuration/service.schema.json")["$defs"]
    for name in SHARED_DEFS:
        check(f"$defs/{name} is identical in both configuration schemas", client.get(name) == service.get(name) and name in client)

    if failures:
        print(f"docs check: fail ({len(failures)} failed)")
        return 1
    print("docs check: pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
