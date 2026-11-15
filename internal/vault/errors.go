package vault

import (
	"errors"
	"fmt"
)

// ErrUnavailable identifies failed communication with OpenBao or Vault,
// including dial/TLS failures, request timeouts and interrupted body reads.
// It survives config-path wrapping, so CLI callers can map it to exit 4
// without inspecting message text. Caller cancellation also retains ctx.Err().
var ErrUnavailable = errors.New("OpenBao or Vault unavailable")

// StatusError identifies a non-success HTTP response. Code is available to
// callers through errors.As; the API response body is deliberately omitted.
type StatusError struct{ Code int }

func (e *StatusError) Error() string { return fmt.Sprintf("OpenBao or Vault returned HTTP %d", e.Code) }
