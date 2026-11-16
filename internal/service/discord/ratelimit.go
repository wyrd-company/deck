package discord

import (
	"context"
	"github.com/disgoorg/disgo/rest"
)

// A rejected limiter wait is known to precede the HTTP write. Preserve that
// distinction so a failed first reply does not leave a touch behind.
type limiterError struct{ error }

func (e *limiterError) Unwrap() error { return e.error }

type rateLimiter struct{ rest.RateLimiter }

func (l rateLimiter) Wait(ctx context.Context, endpoint *rest.CompiledEndpoint) error {
	if err := l.RateLimiter.Wait(ctx, endpoint); err != nil {
		return &limiterError{err}
	}
	return nil
}
