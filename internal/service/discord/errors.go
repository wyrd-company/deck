package discord

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
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
	if errors.Is(err, errNotGuild) {
		return status.Error(codes.PermissionDenied, "target is not granted")
	}
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
			detail := "Discord validation failed"
			if len(paths) > 0 {
				detail += ": " + strings.Join(paths, ", ")
			}
			return status.Error(codes.InvalidArgument, detail)
		case 50013:
			names := []string{}
			for _, name := range []string{"View Channel", "Send Messages in Threads", "Send Messages", "Create Public Threads", "Create Private Threads", "Attach Files", "Embed Links", "Send Polls", "Read Message History"} {
				if strings.Contains(r.Message, name) {
					names = append(names, name)
				}
			}
			detail := "Discord permission missing for this operation"
			if len(names) > 0 {
				detail = "Discord permission missing: " + strings.Join(names, ", ")
			}
			return status.Error(codes.FailedPrecondition, detail)
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
	if r != nil {
		return status.Error(codes.Internal, "Discord operation failed")
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.Error(codes.Unavailable, "Discord unavailable; send outcome may be unknown")
	}
	var network net.Error
	var syntax *json.SyntaxError
	var decode *json.UnmarshalTypeError
	if errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &syntax) || errors.As(err, &decode) {
		return status.Error(codes.Unavailable, "Discord unavailable; send outcome may be unknown")
	}
	return status.Error(codes.Internal, "Discord operation failed")
}
