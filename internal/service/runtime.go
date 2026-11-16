package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/activity"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/registry"
)

type SecretReader interface {
	ReadSecret(context.Context, string, config.SecretRef) (string, error)
}
type Runtime struct {
	Handler   *Handler
	NATS      *nats.Conn
	JetStream jetstream.JetStream
	Registry  *registry.Registry
	Publisher *activity.Publisher
	Bots      map[string]*discord.Client
	Events    *activity.ConnectionEvents
	Index     *activity.Index
	cancel    context.CancelFunc
	close     sync.Once
}

// Open prepares REST clients, NATS and service handlers. Task 7 installs gateway
// listeners and replaces the bootstrap SendIndex before opening listeners.
// The shared Vault client remains owned by the caller. ctx owns the runtime
// lifetime and must remain live until Close; do not pass a startup-only timeout.
func Open(ctx context.Context, cfg *config.Service, secrets SecretReader) (runtime *Runtime, err error) {
	return open(ctx, cfg, secrets, func(token string) (*discord.Client, error) { return discord.New(token) })
}
func open(ctx context.Context, cfg *config.Service, secrets SecretReader, newBot func(string) (*discord.Client, error)) (runtime *Runtime, err error) {
	ctx, cancel := context.WithCancel(ctx)
	r := &Runtime{Bots: map[string]*discord.Client{}, Events: &activity.ConnectionEvents{}, cancel: cancel}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	credentials, err := secrets.ReadSecret(ctx, "nats.creds", cfg.NATS.Creds)
	if err != nil {
		return nil, startupFailure("nats.creds: cannot read configured secret", err)
	}
	r.NATS, err = activity.Connect(cfg.NATS, credentials, r.Events)
	if err != nil {
		return nil, err
	}
	r.JetStream, err = jetstream.New(r.NATS)
	if err != nil {
		return nil, errors.New("nats: cannot open JetStream")
	}
	if _, err = activity.EnsureStream(ctx, r.JetStream, cfg.NATS); err != nil {
		return nil, startupFailure("nats.stream: cannot create or check stream", err)
	}
	r.Registry, err = registry.Open(ctx, r.JetStream, cfg.NATS.Threads)
	if err != nil {
		return nil, err
	}
	// The index registers its connection callbacks before its replay starts.
	r.Index = activity.NewIndex(ctx, r.JetStream, r.Registry.KV, cfg.NATS, r.Events, slog.Default())
	r.Publisher = activity.New(ctx, r.JetStream, cfg.NATS.SubjectPrefix)
	apis := map[string]discord.API{}
	for i, b := range cfg.Bots {
		where := fmt.Sprintf("bots[%d].token", i)
		token, readErr := secrets.ReadSecret(ctx, where, b.Token)
		if readErr != nil {
			return nil, startupFailure(where+": cannot read configured secret", readErr)
		}
		client, createErr := newBot(token)
		if createErr != nil {
			return nil, fmt.Errorf("%s: cannot initialize Discord client", where)
		}
		r.Bots[b.Name] = client
		apis[b.Name] = client
	}
	r.Handler, err = NewHandler(cfg, Dependencies{Context: ctx, Bots: apis, Registry: r.Registry, Publisher: r.Publisher, Index: streamIndex{r.Index}, Activity: r.Index})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Keep cancellation identifiable without carrying upstream bodies or secrets.
// A concurrent stop must not turn an independent startup failure into success.
func startupFailure(message string, cause error) error {
	if errors.Is(cause, context.Canceled) {
		return fmt.Errorf("%s: %w", message, context.Canceled)
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", message, context.DeadlineExceeded)
	}
	return errors.New(message)
}

func (r *Runtime) Close() {
	r.close.Do(func() {
		r.cancel()
		if r.Handler != nil {
			r.Handler.Wait()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, b := range r.Bots {
			b.Bot.Close(ctx)
		}
		if r.Publisher != nil {
			r.Publisher.Close()
		}
		if r.Index != nil {
			r.Index.Close()
		}
		if r.Registry != nil {
			if pending := r.Registry.Pending(); pending > 0 {
				slog.Warn("thread registry writes lost at shutdown", "entries", pending)
			}
			r.Registry.Close()
		}
		if r.NATS != nil {
			_ = r.NATS.Drain()
			r.NATS.Close()
		}
	})
}
