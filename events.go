/*
---
relationships:

	implements:
	  - events

---
*/
package deck

import _ "embed"

// ActivitySchema is the published event contract, shared by offline relays.
//
//go:embed docs/specifications/assets/events/activity.schema.json
var ActivitySchema []byte
