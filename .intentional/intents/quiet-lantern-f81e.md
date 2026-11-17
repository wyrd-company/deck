---
deck: patch
deck-docs: patch
---

Publish the APT and RPM packages as wyrd-deck: install with
`apt install wyrd-deck` or `dnf install wyrd-deck`. Ubuntu and Debian ship a
different program as `deck`, which `apt install deck` would install instead.
The command is still `deck`, and wyrd-deck conflicts with the other `deck`
package because both install /usr/bin/deck. Earlier releases stay in the
repositories under the old name. To upgrade from the `deck` package, run
`sudo dnf swap deck wyrd-deck` on Fedora; `sudo apt install wyrd-deck`
removes `deck` once you confirm.
