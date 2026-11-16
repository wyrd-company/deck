#!/usr/bin/env python3
"""Prove repository lint, commit-message and setup checksum guards.

Every temporary mutation is removed in finally. Run alone in a worktree.
A pass is printed only after every probe has completed.
"""
import io
from pathlib import Path
import subprocess
import shutil
import sys
import tarfile
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent


def run(command, marker=None, cwd=ROOT, timeout=120):
    result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if marker is None:
        if result.returncode:
            raise RuntimeError(result.stdout)
    elif result.returncode == 0 or marker not in result.stdout:
        raise RuntimeError(f"Guard did not reject the probe: {command}\n{result.stdout}")


def fixture_failure(filename, contents, command, marker):
    path = ROOT / filename
    if path.exists():
        raise RuntimeError(f"Probe would overwrite an existing file: {path}")
    try:
        path.write_text(contents)
        run(command, marker)
    finally:
        path.unlink(missing_ok=True)


def ignored_lint_guard():
    with tempfile.TemporaryDirectory(prefix="deck-ignored-lint-") as directory:
        root = Path(directory)
        for name in ("Taskfile.yml", ".gitignore", ".markdownlint-cli2.yaml", ".yamllint.yml"):
            shutil.copyfile(ROOT / name, root / name)
        (root / "checked.md").write_text("# Checked source\n")
        fixtures = {}
        for name in (".venv/lib", "bin", ".release-proof", "npm/node_modules/example"):
            folder = root / name
            folder.mkdir(parents=True)
            for filename, contents in (("probe.md", "# Probe\n\n### Skipped level\n"),
                                       ("probe.yml", "duplicate: one\nduplicate: two\n")):
                path = folder / filename
                path.write_text(contents)
                fixtures[path] = contents
        run(["task", "lint:markdown", "lint:yaml"], cwd=root)
        run(["task", "format"], cwd=root)
        if any(path.read_text() != contents for path, contents in fixtures.items()):
            raise RuntimeError("Formatting rewrote git-ignored content")
        markdown = root / ".markdownlint-cli2.yaml"
        source = markdown.read_text()
        if source.count("gitignore: true") != 1:
            raise RuntimeError("Markdown ignore mutation target changed")
        markdown.write_text(source.replace("gitignore: true", "gitignore: false"))
        run(["task", "lint:markdown"], "MD001", cwd=root)
        yaml = root / ".yamllint.yml"
        source = yaml.read_text()
        if source.count("ignore-from-file: .gitignore") != 1:
            raise RuntimeError("YAML ignore mutation target changed")
        yaml.write_text(source.replace("ignore-from-file: .gitignore", ""))
        run(["task", "lint:yaml"], "key-duplicates", cwd=root)
    print("PASS guard: lints/format leave ignored files alone; removing either exclusion detects fixtures", flush=True)


def checksum_guard(source):
    namespace = {"__name__": "installer_probe"}
    exec(compile(source, "install-openbao.py", "exec"), namespace)
    archive = io.BytesIO()
    payload = b"synthetic OpenBao executable"
    with tarfile.open(fileobj=archive, mode="w:gz") as package:
        entry = tarfile.TarInfo("bao")
        entry.size = len(payload)
        package.addfile(entry, io.BytesIO(payload))
    data = archive.getvalue()
    # A real tar archive that differs from the pinned OpenBao release bytes.
    with tempfile.TemporaryDirectory(prefix="deck-installer-probe-") as directory:
        target = Path(directory) / ".local/bin/bao"
        with patch("platform.system", return_value="Linux"), \
                patch("platform.machine", return_value="x86_64"), \
                patch("pathlib.Path.home", return_value=Path(directory)), \
                patch("urllib.request.urlopen", return_value=io.BytesIO(data)):
            try:
                namespace["main"]()
            except SystemExit as error:
                if str(error) != "OpenBao archive checksum mismatch" or target.exists():
                    raise RuntimeError("Unexpected checksum rejection") from error
                return True
        return False


def development_cancellation_guard():
    # Mutate a disposable copy so the live Taskfile and tests stay untouched.
    with tempfile.TemporaryDirectory(prefix="deck-dev-guard-") as directory:
        root = Path(directory)
        test = root / "scripts/tests/test_repository.py"
        test.parent.mkdir(parents=True)
        shutil.copyfile(ROOT / "scripts/tests/test_repository.py", test)
        taskfile = root / "Taskfile.yml"
        source = (ROOT / "Taskfile.yml").read_text()
        taskfile.write_text(source)
        command = [sys.executable, str(test),
                   "DevelopmentTaskTests.test_watch_cancellation_stops_the_service_before_restart"]
        run(command, cwd=root, timeout=650)
        target = ('      - task: build\n'
                  '      - ./bin/deck service start --config {{shellQuote .CONFIG}}')
        if source.count(target) != 1:
            raise RuntimeError("Development command mutation target changed")
        taskfile.write_text(source.replace(
            target, '      - go run ./cmd/deck service start --config {{shellQuote .CONFIG}}'))
        run(command, "watch cancellation left the service running", cwd=root, timeout=650)
    print("PASS guard: development service stops and releases its port; old go run leaves an orphan and fails",
          flush=True)


def format_check_guard():
    with tempfile.TemporaryDirectory(prefix="deck-format-guard-") as directory:
        root = Path(directory)
        test = root / "scripts/tests/test_repository.py"
        test.parent.mkdir(parents=True)
        shutil.copyfile(ROOT / "scripts/tests/test_repository.py", test)
        taskfile = root / "Taskfile.yml"
        source = (ROOT / "Taskfile.yml").read_text()
        taskfile.write_text(source)
        command = [sys.executable, str(test), "FormatTaskTests"]
        run(command, cwd=root)
        checked_command = ('unformatted=$(gofmt -l .) || exit 1\n'
                           '        if [ -n "$unformatted" ]; then\n'
                           '          printf \'%s\\n\' "$unformatted"\n'
                           '          exit 1\n'
                           '        fi')
        for target, replacement, marker in (
            (checked_command, 'test -z "$(gofmt -l .)" || (gofmt -l . && exit 1)',
             'FAIL: test_format_check_requires_gofmt'),
            ('if [ -n "$unformatted" ]; then', 'if false; then',
             'FAIL: test_format_check_rejects_unformatted_sources'),
        ):
            if source.count(target) != 1:
                raise RuntimeError("Formatting check mutation target changed")
            taskfile.write_text(source.replace(target, replacement))
            run(command, marker, cwd=root)
    print("PASS guard: format check requires gofmt and formatted files; removing either check fails its regression",
          flush=True)


def main():
    run(["task", "lint:markdown"])
    run(["task", "lint:yaml"])
    fixture_failure("repository-guard-probe.md", "# Probe\n\n### Skipped heading\n",
                    ["task", "lint:markdown"], "MD001")
    print("PASS guard: Markdown heading lint rejects a skipped level", flush=True)
    fixture_failure("repository-guard-probe.yml", "duplicate: one\nduplicate: two\n",
                    ["task", "lint:yaml"], "key-duplicates")
    print("PASS guard: YAML lint rejects duplicate keys", flush=True)
    ignored_lint_guard()
    development_cancellation_guard()
    format_check_guard()
    with tempfile.TemporaryDirectory(prefix="deck-commit-probe-") as directory:
        message = Path(directory) / "message"
        for header in ("fix(cli): preserve unread activity", "feat!: update the contract",
                       "Merge branch 'topic'", 'Revert "feat: add a command"',
                       "fixup! feat: add a command", "squash! feat: add a command",
                       "amend! feat: add a command"):
            message.write_text(header + "\n")
            run(["task", "lint:commit-msg", "--", str(message)])
        for header in ("", "Update stuff", "fix: ", "Merge ", "fixup! ",
                       "squash! ", "amend! ", 'Revert ""'):
            message.write_text(header + "\n")
            run(["task", "lint:commit-msg", "--", str(message)], "Use a Conventional Commit")
    print("PASS guard: commit messages accept conventional/Git-generated headers and reject invalid ones", flush=True)
    source = (ROOT / "scripts/install-openbao.py").read_text()
    if not checksum_guard(source):
        raise RuntimeError("The installer accepted a corrupt download")
    target = "if hashlib.sha256(archive).hexdigest() != expected:"
    if source.count(target) != 1:
        raise RuntimeError("Installer mutation target changed")
    if checksum_guard(source.replace(target, "if False:")):
        raise RuntimeError("The checksum mutation was not detected")
    print("PASS guard: a checksum mismatch prevents installation; removing the check defeats the probe", flush=True)
    print("repository guards: pass", flush=True)


if __name__ == "__main__":
    main()
