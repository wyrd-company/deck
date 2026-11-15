package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/wyrd-company/deck/internal/vault"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failure struct {
	code    int
	message string
}

func (e *failure) Error() string               { return e.message }
func invalid(format string, args ...any) error { return &failure{2, fmt.Sprintf(format, args...)} }
func refused(format string, args ...any) error { return &failure{3, fmt.Sprintf(format, args...)} }
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var f *failure
	if errors.As(err, &f) {
		return f.code
	}
	if errors.Is(err, context.Canceled) {
		return 130
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, vault.ErrUnavailable) {
		return 4
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return 2
	case codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition:
		return 3
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return 4
	case codes.Canceled:
		return 130
	}
	return 1
}
func category(code int) string {
	switch code {
	case 2:
		return "invalid"
	case 3:
		return "refused"
	case 4:
		return "unavailable"
	case 5:
		return "timeout"
	case 130:
		return "interrupted"
	}
	return "failed"
}

// INTERNAL/UNKNOWN can follow a partial operation too. A send is considered
// definitely refused only for explicit pre-operation refusals.
func unknownSend(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition, codes.Unauthenticated:
		return false
	}
	return true
}
