#!/usr/bin/env python3
"""Install the pinned OpenBao check binary, checking its release checksum."""
import hashlib
import io
import os
import pathlib
import platform
import tarfile
import urllib.request


VERSION = "2.7.2"
# From v2.7.2's checksums.txt; update these together with VERSION and CI's pin.
CHECKSUMS = {
    ("linux", "amd64"): "582b1f3c2fdc5afd443b2d57a225ef654de75adea609fdb66731fd582a9cbc96",
    ("linux", "arm64"): "5b97bf816fa820e5e521abec1db19c6161fcca2f666cf4a84e335089c0e83edd",
    ("darwin", "amd64"): "ea62ce00b81a2170a5daa8589f8420434e72d4d4ebcd2a4cbfa9add507a2602d",
    ("darwin", "arm64"): "2b4fc131910f4b075b2e1c9a9de67c461489491a8f03fbdc73387294d67d9547",
}


def main():
    system = platform.system().lower()
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    if system not in ("linux", "darwin") or arch is None:
        raise SystemExit("OpenBao check tools support Linux/macOS on x86-64/ARM64")
    name = f"openbao_{VERSION}_{system}_{arch}.tar.gz"
    base = f"https://github.com/openbao/openbao/releases/download/v{VERSION}"
    expected = CHECKSUMS[(system, arch)]
    with urllib.request.urlopen(f"{base}/{name}", timeout=60) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != expected:
        raise SystemExit("OpenBao archive checksum mismatch")
    destination = pathlib.Path.home() / ".local/bin/bao"
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Extract only the binary; no archive paths are written to the filesystem.
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as package:
        with package.extractfile("bao") as binary:
            content = binary.read()
    temporary = destination.with_name(f".bao-{os.getpid()}")
    try:
        temporary.write_bytes(content)
        temporary.chmod(0o755)
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Installed OpenBao {VERSION}: {destination}")


if __name__ == "__main__":
    main()
