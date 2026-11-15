// Package transport carries the authenticated calling key to API handlers.
package transport

import (
	"context"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type keyContext struct{}

// KeyAuthInfo is the SSH-authenticated identity returned by the transport's
// server handshake. A context value or peer metadata from a client is never
// accepted as an identity by the SSH handshake.
type KeyAuthInfo struct {
	credentials.CommonAuthInfo
	Name string
}

func (KeyAuthInfo) AuthType() string { return "ssh" }

// WithKeyName installs a calling key for interceptors and handler tests. The
// transport must call it only after SSH authentication has resolved the name.
func WithKeyName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, keyContext{}, name)
}

// KeyName returns the calling key's configured name, or empty when no
// authenticated identity is present. Handlers must refuse an empty name.
func KeyName(ctx context.Context) string {
	if name, ok := ctx.Value(keyContext{}).(string); ok {
		return name
	}
	if p, ok := peer.FromContext(ctx); ok {
		switch info := p.AuthInfo.(type) {
		case KeyAuthInfo:
			return info.Name
		case *KeyAuthInfo:
			if info != nil {
				return info.Name
			}
		}
	}
	return ""
}
