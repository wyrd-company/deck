package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/health"
	"github.com/wyrd-company/deck/internal/transport"
)

// Run opens the runtime and every gateway session, reads the host key, binds
// the SSH and health listeners, and serves until ctx is canceled. Startup
// secret reads stay serial. The caller owns signal handling and cancels ctx
// to stop the service. On cancellation the transport closes and joins RPC
// handlers before the runtime cancels process-owned sends and closes its
// shared resources. Every startup failure is a *Failure naming its step.
func Run(ctx context.Context, cfg *config.Service, secrets SecretReader) error {
	return run(ctx, cfg, secrets, func(runtimeCtx context.Context) (*Runtime, pb.DeckServiceServer, error) {
		r, err := Open(runtimeCtx, cfg, secrets)
		if err != nil {
			return nil, nil, err
		}
		return r, r.Handler, nil
	})
}

// prepare lets lifecycle tests wrap the handler while exercising the real
// runtime and transport startup and shutdown below.
func run(ctx context.Context, cfg *config.Service, secrets SecretReader, prepare func(context.Context) (*Runtime, pb.DeckServiceServer, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtimeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stopStartup := context.AfterFunc(ctx, cancel)
	defer stopStartup()
	r, handler, err := prepare(runtimeCtx)
	if err != nil {
		return err
	}
	defer r.Close()
	if err = r.OpenGateways(runtimeCtx); err != nil {
		return err
	}
	stopStartup()
	if err = ctx.Err(); err != nil {
		return err
	}
	host, err := transport.ReadHostKey(ctx, secrets, cfg.HostKey)
	if err != nil {
		return startupFailure("read the host key", "host_key", "cannot read an OpenSSH ed25519 host key", err)
	}
	server, err := transport.NewServer(cfg.Keys, host, handler)
	if err != nil {
		return startupFailure("prepare the SSH listener", "keys", "invalid configured key", err)
	}
	listener, err := transport.Bind(ctx, cfg.Listen.SSH)
	if err != nil {
		_ = server.Close()
		if errors.Is(err, transport.ErrBind) {
			return startupFailure("open the SSH listener", "listen.ssh", "cannot listen", nil)
		}
		return startupFailure("open the SSH listener", "listen.ssh", "stopped during startup", err)
	}
	probe, err := health.Listen(cfg.Listen.Health, r.Checks())
	if err != nil {
		_ = listener.Close()
		_ = server.Close()
		return startupFailure("open the health listener", "listen.health", "cannot listen", nil)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		probe.Close(ctx)
	}()
	slog.Info("service started", "ssh", listener.Addr().String(), "health", probe.Addr().String(), "bots", len(r.Bots))
	err = transport.Serve(ctx, server, listener)
	slog.Info("service stopping")
	return err
}
