package transport

import (
	"context"
	"net"

	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListenAndServe reads the host key once, binds cfg.Listen.SSH, and serves the
// injected API until ctx is canceled. Call it after constructing the service
// handler, with the same SecretReader, to keep all startup secret reads serial.
// Shutdown closes the listener and cancels outstanding calls before returning.
func ListenAndServe(ctx context.Context, cfg *config.Service, secrets SecretReader, handler deckv1.DeckServiceServer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	host, err := ReadHostKey(ctx, secrets, cfg.HostKey)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := NewServer(cfg.Keys, host, handler)
	if err != nil {
		return err
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Listen.SSH)
	if err != nil {
		_ = server.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return status.Error(codes.Unavailable, "cannot listen on listen.ssh")
	}
	return serveUntilCanceled(ctx, server, listener)
}
func serveUntilCanceled(ctx context.Context, server *Server, listener net.Listener) error {
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = server.Close()
		if err := <-done; err != nil {
			return err
		}
		return nil
	}
}
