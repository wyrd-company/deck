package discord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/disgoorg/disgo/rest"
	"github.com/gorilla/websocket"
)

// safeKeys are disgo log attributes that hold only names, numbers and states.
var safeKeys = map[string]bool{"name": true, "shard_id": true, "shard_count": true, "code": true, "reconnect": true, "try": true, "delay": true, "can_resume": true, "event_type": true, "opcode": true, "status": true, "retry_after": true, "global": true, "bucket": true}

// redacted wires disgo's logger to the service's handler at warning level.
// It keeps disgo's message and the attributes above, describes errors by
// kind and code only, and drops everything else, so no token, URL, message
// content or Discord response body reaches the log.
type redacted struct{ next slog.Handler }

// DisgoLogger returns disgo's logger over the service's log handler.
func DisgoLogger(next slog.Handler) *slog.Logger {
	return slog.New(redacted{next: next})
}

func (h redacted) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn && h.next.Enabled(ctx, level)
}

func (h redacted) Handle(ctx context.Context, r slog.Record) error {
	out := slog.NewRecord(r.Time, r.Level, r.Message, 0)
	out.AddAttrs(slog.String("component", "disgo"))
	r.Attrs(func(a slog.Attr) bool {
		if safe, ok := safeAttr(a); ok {
			out.AddAttrs(safe)
		}
		return true
	})
	return h.next.Handle(ctx, out)
}

func (h redacted) WithAttrs(attrs []slog.Attr) slog.Handler {
	kept := []slog.Attr{}
	for _, a := range attrs {
		if safe, ok := safeAttr(a); ok {
			kept = append(kept, safe)
		}
	}
	return redacted{next: h.next.WithAttrs(kept)}
}

func (h redacted) WithGroup(string) slog.Handler { return h }

func safeAttr(a slog.Attr) (slog.Attr, bool) {
	switch {
	case a.Key == "err" || a.Key == "error":
		return slog.String("error", describe(a.Value.Any())), true
	case safeKeys[a.Key]:
		switch a.Value.Kind() {
		case slog.KindString, slog.KindInt64, slog.KindUint64, slog.KindBool, slog.KindDuration, slog.KindFloat64:
			return a, true
		}
	}
	return slog.Attr{}, false
}

// describe names an error's kind and Discord's codes without its text.
func describe(v any) string {
	err, ok := v.(error)
	if !ok {
		return "unknown"
	}
	var r *rest.Error
	if errors.As(err, &r) {
		status := 0
		if r.Response != nil {
			status = r.Response.StatusCode
		}
		return fmt.Sprintf("discord rest error: status %d code %d", status, r.Code)
	}
	var closed *websocket.CloseError
	if errors.As(err, &closed) {
		return fmt.Sprintf("gateway closed: code %d", closed.Code)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled"
	}
	return fmt.Sprintf("%T", err)
}
