// Package deck embeds the configuration contracts shared with the design checks.
package deck

import "embed"

// ConfigurationSchemas holds the original schemas, without a copied runtime version.
//
//go:embed docs/specifications/assets/configuration/*.schema.json
var ConfigurationSchemas embed.FS
