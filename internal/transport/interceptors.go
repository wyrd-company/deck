package transport

import (
	"context"
	"log/slog"

	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func identityInterceptors(keys []config.Key) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	byName := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		byName[key.Name] = struct{}{}
	}
	authenticate := func(ctx context.Context) (context.Context, error) {
		_, ok := byName[KeyName(ctx)]
		if !ok || KeyName(ctx) == "" {
			return nil, status.Error(codes.Unauthenticated, "authenticated key required")
		}
		return ctx, nil
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx)
		if err != nil {
			return nil, err
		}
		response, err := handler(ctx, req)
		logRefused(ctx, info.FullMethod, err)
		return response, err
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context())
		if err != nil {
			return err
		}
		err = handler(srv, &authenticatedStream{ServerStream: ss, ctx: ctx})
		logRefused(ctx, info.FullMethod, err)
		return err
	}
	return unary, stream
}

// logRefused logs a call that ended with an error status: the key's name,
// the method and the gRPC code, never the status message or the request.
// A call the client ended itself is not a refusal.
func logRefused(ctx context.Context, method string, err error) {
	if code := status.Code(err); err != nil && code != codes.Canceled {
		slog.Warn("call refused", "key", KeyName(ctx), "method", method, "code", code.String())
	}
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context { return s.ctx }
