package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
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
		return nil, errors.New("nats.creds: cannot read configured secret")
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
		return nil, errors.New("nats.stream: cannot create or check stream")
	}
	r.Registry, err = registry.Open(ctx, r.JetStream, cfg.NATS.Threads)
	if err != nil {
		return nil, err
	}
	r.Publisher = activity.New(ctx, r.JetStream, cfg.NATS.SubjectPrefix)
	apis := map[string]discord.API{}
	for i, b := range cfg.Bots {
		where := fmt.Sprintf("bots[%d].token", i)
		token, readErr := secrets.ReadSecret(ctx, where, b.Token)
		if readErr != nil {
			return nil, fmt.Errorf("%s: cannot read configured secret", where)
		}
		client, createErr := newBot(token)
		if createErr != nil {
			return nil, fmt.Errorf("%s: cannot initialize Discord client", where)
		}
		r.Bots[b.Name] = client
		apis[b.Name] = client
	}
	r.Handler, err = NewHandler(cfg, Dependencies{Context: ctx, Bots: apis, Registry: r.Registry, Publisher: r.Publisher, Index: &bootstrapIndex{js: r.JetStream, kv: r.Registry.KV, stream: cfg.NATS.Stream.Name}})
	if err != nil {
		return nil, err
	}
	return r, nil
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

// bootstrapIndex supplies a starting position while task 7's replay/index layer
// is absent. Task 7 replaces it before serving: completed-send lookup and replay
// gates belong to that task, alongside the unread APIs.
type bootstrapIndex struct {
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	stream string
}

func (i *bootstrapIndex) Completed(context.Context, string, string) (*CompletedSend, error) {
	return nil, nil
}
func (i *bootstrapIndex) Position(ctx context.Context) (*pb.StreamPosition, error) {
	for {
		stream, err := i.js.Stream(ctx, i.stream)
		if err != nil {
			return nil, err
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return nil, err
		}
		record, err := i.kv.Get(ctx, "stream.generation")
		var generation struct {
			Epoch   uint64    `json:"epoch"`
			Created time.Time `json:"created"`
		}
		var revision uint64
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
		if err == nil {
			revision = record.Revision()
			if err = json.Unmarshal(record.Value(), &generation); err != nil {
				return nil, err
			}
		}
		if generation.Epoch == 0 || !generation.Created.Equal(info.Created) {
			next := uint64(time.Now().UnixNano())
			if next <= generation.Epoch {
				next = generation.Epoch + 1
			}
			generation.Epoch = next
			generation.Created = info.Created
			raw, _ := json.Marshal(generation)
			if revision == 0 {
				_, err = i.kv.Create(ctx, "stream.generation", raw)
			} else {
				_, err = i.kv.Update(ctx, "stream.generation", raw, revision)
			}
			if errors.Is(err, jetstream.ErrKeyExists) {
				continue
			}
			if err != nil {
				return nil, err
			}
		}
		return &pb.StreamPosition{Epoch: generation.Epoch, Sequence: info.State.LastSeq}, nil
	}
}

func (i *bootstrapIndex) Confirm(context.Context, *CompletedSend) error { return nil }
