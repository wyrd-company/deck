package transport

import (
	"context"
	"slices"
	"strconv"

	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grantsContext struct{}

func copyGrants(in []config.Grant) []config.Grant {
	out := slices.Clone(in)
	for i := range out {
		out[i].Channels = slices.Clone(out[i].Channels)
	}
	return out
}

// Grants returns a copy of the authenticated key's immutable grants. It never
// accepts grants supplied in request metadata.
func Grants(ctx context.Context) []config.Grant {
	grants, _ := ctx.Value(grantsContext{}).([]config.Grant)
	return copyGrants(grants)
}

// CheckBot must run before looking up target metadata through a bot.
func CheckBot(ctx context.Context, bot string) error {
	for _, grant := range Grants(ctx) {
		if grant.Bot == bot && bot != "" {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "target is not granted")
}

// CheckTarget checks trusted metadata, never server/channel values inferred
// from a grant. A wildcard is scoped to the grant's bot and server.
func CheckTarget(ctx context.Context, bot string, serverID, channelID uint64) error {
	for _, grant := range Grants(ctx) {
		if grant.Bot == bot && bot != "" && grant.Server == strconv.FormatUint(serverID, 10) && serverID != 0 && channelID != 0 {
			if slices.Contains(grant.Channels, "*") || slices.Contains(grant.Channels, strconv.FormatUint(channelID, 10)) {
				return nil
			}
		}
	}
	return status.Error(codes.PermissionDenied, "target is not granted")
}

func grantInterceptors(keys []config.Key) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	byName := make(map[string][]config.Grant, len(keys))
	for _, key := range keys {
		byName[key.Name] = copyGrants(key.Grants)
	}
	authenticate := func(ctx context.Context) (context.Context, error) {
		grants, ok := byName[KeyName(ctx)]
		if !ok || KeyName(ctx) == "" {
			return nil, status.Error(codes.Unauthenticated, "authenticated key required")
		}
		return context.WithValue(ctx, grantsContext{}, grants), nil
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
