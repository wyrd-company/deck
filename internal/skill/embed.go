/*
---
relationships:

  implements:
    - client
    - command-line-interface

---
*/
// Package skill contains the agent instructions shipped in the executable.
package skill

import _ "embed"

//go:embed SKILL.md
var Markdown string
