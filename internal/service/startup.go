package service

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/transport"
)

// Run opens the runtime, then the SSH listener, sharing serial startup reads.
// SIGINT and SIGTERM cancel the serving context.
// On cancellation the transport closes and joins RPC handlers before the
// runtime cancels process-owned sends and closes its shared resources.
func Run(ctx context.Context, cfg *config.Service, secrets SecretReader) error {
	return run(ctx, cfg, secrets, func(runtimeCtx context.Context) (*Runtime, pb.DeckServiceServer, error) {
		r, err := Open(runtimeCtx, cfg, secrets)
		if err != nil {
			return nil, nil, err
		}
		return r, r.Handler, nil
	})
}

// prepare also lets lifecycle tests install the streaming handler task 7 owns
// while exercising the real runtime and transport startup/shutdown below.
func run(ctx context.Context, cfg *config.Service, secrets SecretReader, prepare func(context.Context) (*Runtime, pb.DeckServiceServer, error)) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	runtimeCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stopStartup := context.AfterFunc(ctx, cancel)
	defer stopStartup()
	r, handler, err := prepare(runtimeCtx)
	stopStartup()
	if err != nil {
		return err
	}
	defer r.Close()
	return transport.ListenAndServe(ctx, cfg, secrets, handler)
}
