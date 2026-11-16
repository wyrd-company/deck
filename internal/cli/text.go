package cli

import (
	"fmt"
	"strings"
	"unicode"
)

// Preserve layout only where the renderer itself indents message content.
// User-supplied metadata always occupies one terminal line.
func terminalText(value string, multiline bool) string {
	var out strings.Builder
	for _, r := range value {
		switch r {
		case '\n':
			if multiline {
				out.WriteRune(r)
			} else {
				out.WriteString(`\n`)
			}
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				fmt.Fprintf(&out, `\u%04x`, r)
			} else {
				out.WriteRune(r)
			}
		}
	}
	return out.String()
}
func line(value string) string { return terminalText(value, false) }
func channelType(kind uint32) string {
	switch kind {
	case 0:
		return "text"
	case 5:
		return "announcement"
	case 15:
		return "forum"
	case 16:
		return "media"
	}
	return fmt.Sprintf("unknown(%d)", kind)
}
