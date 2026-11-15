package discord

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/disgoorg/disgo/rest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errNotGuild = errors.New("not a guild channel")

func Code(err error) int {
	var r *rest.Error
	if errors.As(err, &r) {
		return int(r.Code)
	}
	return 0
}

// Unknown is deliberately conservative: without a definitive Discord 4xx
// rejection a write may have reached Discord, including an undecodable answer.
func Unknown(err error) bool {
	var limiter *limiterError
	if errors.As(err, &limiter) {
		return false
	}
	var r *rest.Error
	return !errors.As(err, &r) || r.Response == nil || r.Response.StatusCode >= 500
}
func Error(err error) error {
	if err == nil {
		return nil
	}
	var limiter *limiterError
	if errors.As(err, &limiter) {
		return status.Error(codes.ResourceExhausted, "Discord rate limit exceeds call deadline")
	}
	var r *rest.Error
	if errors.As(err, &r) {
		switch int(r.Code) {
		case 50035:
			var fields map[string]any
			_ = json.Unmarshal(r.Errors, &fields)
			paths := []string{}
			var walk func(map[string]any, string)
			walk = func(m map[string]any, p string) {
				for key, v := range m {
					if key == "_errors" {
						if p != "" {
							paths = append(paths, p)
						}
						continue
					}
					part := strconv.QuoteToASCII(key)
					if key != "" && strings.IndexFunc(key, func(r rune) bool {
						return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_')
					}) < 0 {
						part = key
					}
					if next, ok := v.(map[string]any); ok {
						prefix := part
						if p != "" {
							prefix = p + "." + part
						}
						walk(next, prefix)
					}
				}
			}
			walk(fields, "")
			sort.Strings(paths)
			return status.Error(codes.InvalidArgument, "Discord validation failed: "+strings.Join(paths, ", "))
		case 50013:
			return status.Error(codes.FailedPrecondition, "Discord permission missing for this operation")
		case 10008:
			return status.Error(codes.NotFound, "message not found")
		case 40005:
			return status.Error(codes.InvalidArgument, "files: upload exceeds server limit")
		}
		if r.Response != nil {
			if r.Response.StatusCode == 429 {
				return status.Error(codes.ResourceExhausted, "Discord rate limit exceeds call deadline")
			}
			if r.Response.StatusCode >= 500 {
				return status.Error(codes.Unavailable, "Discord unavailable; send outcome may be unknown")
			}
			if r.Response.StatusCode == 403 || r.Response.StatusCode == 404 {
				return status.Error(codes.PermissionDenied, "target is not granted")
			}
		}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.Error(codes.Unavailable, "Discord unavailable; send outcome may be unknown")
	}
	return status.Error(codes.Unavailable, "Discord unavailable; send outcome may be unknown")
}
