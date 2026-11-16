package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/activity"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/health"
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
	Gateways  map[string]*discord.Gateway
	names     []string
	cancel    context.CancelFunc
	stopSends context.CancelFunc
	close     sync.Once
}

// Failure is a startup failure. It names the step and the configuration field
// involved, carries a safe message, and keeps only a cancellation or deadline
// identity of its cause, never an upstream body.
type Failure struct {
	Step, Field, Message string
	cause                error
}

func (f *Failure) Error() string {
	text := f.Field + ": " + f.Message
	if f.cause != nil {
		text += ": " + f.cause.Error()
	}
	return text
}
func (f *Failure) Unwrap() error { return f.cause }

// startupFailure keeps cancellation identifiable without carrying upstream
// bodies or secrets: a concurrent stop must not turn an independent startup
// failure into success.
func startupFailure(step, field, message string, cause error) error {
	f := &Failure{Step: step, Field: field, Message: message}
	switch {
	case errors.Is(cause, context.Canceled):
		f.cause = context.Canceled
	case errors.Is(cause, context.DeadlineExceeded):
		f.cause = context.DeadlineExceeded
	}
	return f
}

// fieldFailure turns a "<field>: <message>" error of the packages below
// into a Failure for its step, keeping cancellation's identity.
func fieldFailure(step, field string, err error) error {
	text := err.Error()
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
		text = strings.TrimSuffix(text, ": "+sentinel.Error())
		if text == sentinel.Error() {
			text = "stopped during startup"
		}
	}
	if prefix, message, ok := strings.Cut(text, ": "); ok && !strings.Contains(prefix, " ") {
		field, text = prefix, message
	}
	return startupFailure(step, field, text, err)
}

// BotOptions adds disgo options to every bot client. It exists for tests,
// which point it at fake Discord servers; production leaves it nil.
var BotOptions func(name string) []bot.ConfigOpt

// StartupFailure builds a Failure for a step outside this package, such as
// opening the OpenBao or Vault client.
func StartupFailure(step, field, message string, cause error) error {
	return startupFailure(step, field, message, cause)
}

// gatewayTimeout bounds opening one bot's gateway session at start.
var gatewayTimeout = 30 * time.Second

// Open prepares REST clients, NATS, the registry, the activity index, the
// publisher, the handler and every bot's gateway listeners. The shared Vault
// client remains owned by the caller. ctx owns the runtime lifetime and must
// remain live until Close; do not pass a startup-only timeout.
func Open(ctx context.Context, cfg *config.Service, secrets SecretReader) (runtime *Runtime, err error) {
	return open(ctx, cfg, secrets, func(name, token string) (*discord.Client, error) {
		var opts []bot.ConfigOpt
		if BotOptions != nil {
			opts = BotOptions(name)
		}
		return discord.New(token, opts...)
	})
}
func open(ctx context.Context, cfg *config.Service, secrets SecretReader, newBot func(name, token string) (*discord.Client, error)) (runtime *Runtime, err error) {
	ctx, cancel := context.WithCancel(ctx)
	r := &Runtime{Bots: map[string]*discord.Client{}, Gateways: map[string]*discord.Gateway{}, Events: &activity.ConnectionEvents{}, cancel: cancel}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	logEvents(r.Events)
	credentials, err := secrets.ReadSecret(ctx, "nats.creds", cfg.NATS.Creds)
	if err != nil {
		return nil, startupFailure("read NATS credentials", "nats.creds", "cannot read configured secret", err)
	}
	r.NATS, err = activity.Connect(cfg.NATS, credentials, r.Events)
	if err != nil {
		return nil, fieldFailure("connect to NATS", "nats.servers", err)
	}
	r.JetStream, err = jetstream.New(r.NATS)
	if err != nil {
		return nil, startupFailure("connect to NATS", "nats.servers", "cannot open JetStream", err)
	}
	if _, err = activity.EnsureStream(ctx, r.JetStream, cfg.NATS); err != nil {
		return nil, startupFailure("create or check the stream", "nats.stream", "cannot create or check stream", err)
	}
	r.Registry, err = registry.Open(ctx, r.JetStream, cfg.NATS.Threads)
	if err != nil {
		return nil, fieldFailure("load the thread registry", "nats.threads", err)
	}
	// The index registers its connection callbacks before its replay starts.
	r.Index = activity.NewIndex(ctx, r.JetStream, r.Registry.KV, cfg.NATS, r.Events, slog.Default())
	r.Publisher = activity.New(ctx, r.JetStream, cfg.NATS.SubjectPrefix)
	apis := map[string]discord.API{}
	for i, b := range cfg.Bots {
		where := fmt.Sprintf("bots[%d].token", i)
		token, readErr := secrets.ReadSecret(ctx, where, b.Token)
		if readErr != nil {
			return nil, startupFailure("read bot tokens", where, "cannot read configured secret", readErr)
		}
		client, createErr := newBot(b.Name, token)
		if createErr != nil {
			return nil, startupFailure("prepare bot clients", where, "cannot initialize Discord client", createErr)
		}
		r.Bots[b.Name] = client
		r.names = append(r.names, b.Name)
		apis[b.Name] = client
	}
	sendCtx, stopSends := context.WithCancel(ctx)
	r.stopSends = stopSends
	r.Handler, err = NewHandler(cfg, Dependencies{Context: sendCtx, Bots: apis, Registry: r.Registry, Publisher: r.Publisher, Activity: r.Index})
	if err != nil {
		return nil, startupFailure("prepare the handler", "keys", "invalid configured key", err)
	}
	// Listeners are installed on every bot before any session opens.
	for _, name := range r.names {
		r.Gateways[name] = discord.NewGateway(ctx, name, r.Bots[name], r.Registry, r.Publisher, r.Index, slog.Default())
	}
	return r, nil
}

func logEvents(events *activity.ConnectionEvents) {
	events.OnDisconnect(func() { slog.Warn("NATS connection state", "state", "disconnected") })
	events.OnReconnect(func() { slog.Info("NATS connection state", "state", "connected") })
}

// OpenGateways opens every bot's gateway session, returning once each sent
// READY, within gatewayTimeout per bot.
func (r *Runtime) OpenGateways(ctx context.Context) error {
	for i, name := range r.names {
		open, cancel := context.WithTimeout(ctx, gatewayTimeout)
		err := r.Gateways[name].Open(open)
		cancel()
		if err != nil {
			// Only a failure the stop itself caused is cancellation: an
			// independent failure (an invalid token, Discord refusing the
			// session) stays a failure even when a stop arrived meanwhile.
			if ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
				err = nil // a failure, not a stop: keep no cancellation identity
			}
			return startupFailure("open gateway sessions", fmt.Sprintf("bots[%d]", i), "cannot open the gateway session", err)
		}
	}
	return nil
}

// Checks are the health listener's checks over the runtime's state.
func (r *Runtime) Checks() health.Checks {
	c := health.Checks{Bots: map[string]func() string{}}
	for name, g := range r.Gateways {
		c.Bots[name] = g.Status
	}
	c.NATS = func() string {
		if r.NATS == nil {
			return "disconnected"
		}
		return strings.ToLower(r.NATS.Status().String())
	}
	c.Index = func() string {
		if r.Index == nil {
			return "unavailable"
		}
		return r.Index.State()
	}
	return c
}

// Shutdown budgets, within the service's 10-second stop: each step ends at
// its budget, so an outage cannot hold the process. With the gateway and bot
// closes (half a second each) and the index's consumer stop (a second),
// they add up to 8.5 seconds after the transport has joined its calls.
var (
	sendBudget    = 2 * time.Second
	publishBudget = 1500 * time.Millisecond
	flushBudget   = 1500 * time.Millisecond
	drainBudget   = 1500 * time.Millisecond
)

// Close stops the runtime in order: gateway sessions stop delivering;
// process-owned sends finish (up to sendBudget, then they are canceled);
// waiting activity is published (publishBudget); dirty registry entries are
// written (flushBudget); then the index, bot clients and NATS close, NATS
// draining first (drainBudget). What a budget cuts short is logged.
func (r *Runtime) Close() {
	r.close.Do(func() {
		started := time.Now()
		within := func(d time.Duration) (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), d)
		}
		ctx, cancel := within(500 * time.Millisecond)
		for _, g := range r.Gateways {
			g.Close(ctx)
		}
		cancel()
		if r.Handler != nil {
			done := make(chan struct{})
			go func() { r.Handler.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(sendBudget):
				slog.Warn("shutdown canceled sends still in flight")
				r.stopSends()
				<-done
			}
		}
		if r.stopSends != nil {
			r.stopSends()
		}
		if r.Publisher != nil {
			ctx, cancel := within(publishBudget)
			if !r.Publisher.Drain(ctx) {
				slog.Warn("activity unpublished at shutdown", "waiting", r.Publisher.Pending())
			}
			cancel()
			r.Publisher.Close()
		}
		for _, g := range r.Gateways {
			g.Wait()
		}
		if r.Registry != nil {
			ctx, cancel := within(flushBudget)
			if left := r.Registry.Flush(ctx); left > 0 {
				slog.Warn("thread registry writes lost at shutdown", "entries", left)
			}
			cancel()
		}
		r.cancel()
		if r.Index != nil {
			r.Index.Close()
		}
		if r.Registry != nil {
			r.Registry.Close()
		}
		// DISGO-WORKAROUND(rest-close-deadlock): bot clients close only after sends and gateway work finished; see wyrd-company/deck#16 (upstream disgoorg/disgo#661).
		ctx, cancel = within(500 * time.Millisecond)
		for _, b := range r.Bots {
			b.Bot.Close(ctx)
		}
		cancel()
		if r.NATS != nil {
			if r.NATS.Drain() == nil {
				deadline := time.Now().Add(drainBudget)
				for !r.NATS.IsClosed() && time.Now().Before(deadline) {
					time.Sleep(10 * time.Millisecond)
				}
			}
			r.NATS.Close()
		}
		slog.Debug("runtime closed", "elapsed", time.Since(started).String())
	})
}
