package transport

import (
	"context"

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
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &authenticatedStream{ServerStream: ss, ctx: ctx})
	}
	return unary, stream
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context { return s.ctx }
