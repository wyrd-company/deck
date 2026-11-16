package transport

import (
	"context"
	"errors"
	"net"

	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BeforeBind runs just before the SSH listener binds. It exists for tests,
// which hold a bind there until a stop arrives; production leaves it nil.
var BeforeBind func(context.Context)

// ErrBind is the bind failure of listen.ssh; it wraps no upstream text.
var ErrBind = status.Error(codes.Unavailable, "cannot listen on listen.ssh")

// Bind binds the SSH listener's address. A bind that fails is reported as
// that failure even when a stop arrived meanwhile: only an error the stop
// itself caused reads as cancellation.
func Bind(ctx context.Context, address string) (net.Listener, error) {
	if BeforeBind != nil {
		BeforeBind(ctx)
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, ctx.Err()
		}
		return nil, ErrBind
	}
	return listener, nil
}

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
	listener, err := Bind(ctx, cfg.Listen.SSH)
	if err != nil {
		_ = server.Close()
		return err
	}
	return Serve(ctx, server, listener)
}

// Serve serves listener until ctx is canceled, then closes the server,
// cancelling and joining outstanding calls.
func Serve(ctx context.Context, server *Server, listener net.Listener) error {
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
