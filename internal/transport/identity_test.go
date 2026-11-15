package transport

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestCallingKey(t *testing.T) {
	ctx := context.Background()
	if KeyName(ctx) != "" {
		t.Fatal("unauthenticated context has an identity")
	}
	spoofed := metadata.NewIncomingContext(ctx, metadata.Pairs("key-name", "attacker"))
	if KeyName(spoofed) != "" {
		t.Fatal("client metadata established an identity")
	}
	authenticated := peer.NewContext(ctx, &peer.Peer{AuthInfo: KeyAuthInfo{Name: "agents"}})
	if KeyName(authenticated) != "agents" {
		t.Fatal("authenticated peer identity missing")
	}
	if KeyName(WithKeyName(ctx, "test-key")) != "test-key" {
		t.Fatal("test identity missing")
	}
	for _, override := range []string{"", "attacker"} {
		if KeyName(WithKeyName(authenticated, override)) != "agents" {
			t.Fatal("context value overrode authenticated peer")
		}
	}
	pointerPeer := peer.NewContext(ctx, &peer.Peer{AuthInfo: &KeyAuthInfo{Name: "pointer-peer"}})
	if KeyName(WithKeyName(pointerPeer, "attacker")) != "pointer-peer" {
		t.Fatal("context value overrode pointer peer")
	}
	emptyPeer := peer.NewContext(ctx, &peer.Peer{AuthInfo: KeyAuthInfo{}})
	if KeyName(WithKeyName(emptyPeer, "attacker")) != "" {
		t.Fatal("empty authenticated peer fell back to context")
	}

	if KeyName(ctx) != "" {
		t.Fatal("identity leaked across contexts")
	}
}
