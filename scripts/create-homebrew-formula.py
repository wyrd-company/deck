#!/usr/bin/env python3
"""Generate a Linux/macOS Homebrew formula from the release's exact archives."""
import argparse
import hashlib
import re
from pathlib import Path


def render(artifacts, version):
    if not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError('version must be a bare stable SemVer')
    lines = ['class Deck < Formula',
             '  desc "Discord conversations for agents through a secret-holding service"',
             '  homepage "https://github.com/wyrd-company/deck"',
             f'  version "{version}"', '  license "Apache-2.0"']
    for system, goos in [('macos', 'darwin'), ('linux', 'linux')]:
        lines.append(f'  on_{system} do')
        for cpu, suffix in [('arm', 'aarch64'), ('intel', 'x86_64')]:
            filename = f'deck_{version}_{goos}_{suffix}.tar.gz'
            checksum = hashlib.sha256((artifacts / filename).read_bytes()).hexdigest()
            lines.extend([f'    on_{cpu} do',
                          f'      url "https://github.com/wyrd-company/deck/releases/download/{version}/{filename}"',
                          f'      sha256 "{checksum}"', '    end'])
        lines.append('  end')
    lines.extend(['  def install', '    bin.install "deck"', '  end',
                  '  test do', '    assert_match version.to_s, shell_output("#{bin}/deck --version")',
                  '  end', 'end', ''])
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts-dir', required=True, type=Path)
    parser.add_argument('--version', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    text = render(args.artifacts_dir, args.version)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text)


if __name__ == '__main__':
    main()
