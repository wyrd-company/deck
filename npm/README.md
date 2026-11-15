# deck

Install with `npm install --global @wyrd-company/deck` (Node.js 22+, Linux or
macOS, x64 or arm64). Installation needs HTTPS access to GitHub releases and
`tar`. The installer downloads the archive for this package's exact version,
verifies its SHA-256 against the release's `checksums.txt`, and installs the
native executable. Installation fails on unsupported platforms or a bad checksum.

Run `deck --version`. Configure `~/.config/deck/config.yml`, or pass `--config`.
The CLI uses a service; bot tokens stay on that service. See the
[installation and configuration guide](https://github.com/wyrd-company/deck#install).

If npm lifecycle scripts are disabled, run `node install.js` in the installed
package directory before using `deck`, or install a GitHub release archive.
