# Brand assets

The mark is a portable cyberdeck: a terminal, physical keyboard and a conversation
signal. `docs/user/assets/icon-light.svg` and `icon-dark.svg` are the sources for
the light and dark marks. `favicon.svg` uses the dark mark. Keep the square
viewBox so the same mark works in the README and wyrd.foo catalog.

`social-preview.svg` is the 1200×630 source for
`docs/user/assets/social-preview.png`. Run `task brand:render` with Node.js and
DejaVu Sans fonts installed to render it with resvg 2.6.2. The script installs
its renderer in a temporary directory and deletes it on exit. It needs npm
registry access; it does not change the repository's dependencies.

The PNG is also suitable for the repository's GitHub social preview setting.
Publishing metadata lives in `docs/user/docs.yml`.
