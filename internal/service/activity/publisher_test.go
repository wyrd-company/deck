package activity

import (
	"context"
	"encoding/json"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"os"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Observe failures returned by the real JetStream client, after its internal retries.
type observedPublisher struct {
	jetstream.JetStream
	failures atomic.Int64
}

func (j *observedPublisher) PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	ack, err := j.JetStream.PublishMsg(ctx, m, opts...)
	if err != nil {
		j.failures.Add(1)
	}
	return ack, err
}

func TestPublishRetryAndEnvelope(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	cfg := config.NATS{SubjectPrefix: "deck", Stream: config.Stream{Name: "DECK", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}}
	observed := &observedPublisher{JetStream: js}
	p := New(ctx, observed, "deck")
	defer p.Close()
	a := &pb.Activity{Id: 123, Time: timestamppb.Now(), Bot: "bot", ServerId: 1, ChannelId: 2, ThreadId: 3, SentBy: "key", SenderKey: "SHA256:key", Nonce: "N1", Sequence: 99, Kind: &pb.Activity_Message{Message: &pb.Message{Content: "hello"}}}
	start := &pb.StreamPosition{Epoch: 42, Sequence: 7}
	wait, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := p.Publish(wait, a, start); err == nil {
		t.Fatal("publish without stream succeeded")
	}
	if p.Pending() != 1 {
		t.Fatal("deadline dropped accepted publish")
	}
	testutil.Until(t, func() bool { return observed.failures.Load() > 0 })
	if p.Pending() != 1 {
		t.Fatal("publisher dropped queue head after its own publish error")
	}
	stream, err := EnsureStream(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Until(t, func() bool { return p.Pending() == 0 })
	// Read the original accepted publication before a second Publish can mask a drop.
	raw, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"Nats-Msg-Id": "message:123", "Deck-Kind": "message", "Deck-Activity-Id": "123", "Deck-Message-Type": "0", "Deck-Sent-By": "key", "Deck-Sender-Key": "SHA256:key", "Deck-Nonce": "N1", "Deck-Send-Start": "42:7", "Deck-Schema": "deck.v1.Activity"} {
		if got := raw.Header.Get(name); got != want {
			t.Errorf("%s=%q, want %q", name, got, want)
		}
	}
	if raw.Subject != "deck.1.2.3" {
		t.Fatal(raw.Subject)
	}
	var body map[string]any
	if err = json.Unmarshal(raw.Data, &body); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["sequence"]; ok {
		t.Fatal("API sequence leaked into event")
	}
	if body["sender_key"] != "SHA256:key" {
		t.Fatal(body)
	}
	seq, err := p.Publish(ctx, a, start)
	if err != nil || seq != 1 {
		t.Fatal(seq, err)
	}
	info, _ := stream.Info(ctx)
	if info.State.Msgs != 1 {
		t.Fatal("dedup failed")
	}
}
func TestConnectionEventsCallsEveryHook(t *testing.T) {
	e := &ConnectionEvents{}
	a, b, d := 0, 0, 0
	e.OnReconnect(func() { a++ })
	e.OnReconnect(func() { b++ })
	e.OnDisconnect(func() { d++ })
	for range 1000 {
		e.fire(false)
		e.fire(true)
	}
	if a != 1000 || b != 1000 || d != 1000 {
		t.Fatal(a, b, d)
	}
}

func TestJWTConnectionAndReconnectHooks(t *testing.T) {
	s, creds := testutil.JWTNATS(t)
	cfg := config.NATS{Servers: []string{s.ClientURL()}, Name: "test", Timeout: time.Second}
	events := &ConnectionEvents{}
	var first, second, disconnect atomic.Int64
	events.OnReconnect(func() { first.Add(1) })
	events.OnReconnect(func() { second.Add(1) })
	events.OnDisconnect(func() { disconnect.Add(1) })
	nc, err := Connect(cfg, creds, events)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_ = nc.StatusChanged(nats.RECONNECTING, nats.CONNECTED)
	for i := int64(1); i <= 15; i++ {
		if err := nc.ForceReconnect(); err != nil {
			t.Fatal(err)
		}
		testutil.Until(t, func() bool { return first.Load() >= i && second.Load() >= i && disconnect.Load() >= i })
	}
	if first.Load() != 15 || second.Load() != 15 {
		t.Fatal("reconnect notification dropped")
	}
	if _, err := Connect(cfg, "invalid creds", events); err == nil {
		t.Fatal("invalid JWT accepted")
	}
}

func TestActivitySchema(t *testing.T) {
	raw, err := os.ReadFile("../../../docs/specifications/assets/events/activity.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc any
	if err = json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	if err = compiler.AddResource("activity.json", doc); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile("activity.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"message", "component", "poll_vote"} {
		t.Run(kind, func(t *testing.T) {
			a := &pb.Activity{Id: 123, Time: timestamppb.Now(), Bot: "test", ServerId: 1, ChannelId: 2, ThreadId: 3}
			switch kind {
			case "message":
				a.Kind = &pb.Activity_Message{Message: &pb.Message{Content: "hello"}}
			case "component":
				a.Kind = &pb.Activity_Component{Component: &pb.ComponentUse{MessageId: 1, CustomId: "button", ComponentType: 2}}
			case "poll_vote":
				a.Kind = &pb.Activity_PollVote{PollVote: &pb.PollVote{MessageId: 1, AnswerId: 1}}
			}
			m, err := Envelope("deck", a, nil)
			if err != nil {
				t.Fatal(err)
			}
			var payload any
			if err = json.Unmarshal(m.Data, &payload); err != nil {
				t.Fatal(err)
			}
			if err = schema.Validate(payload); err != nil {
				t.Fatal(err)
			}
		})
	}
}
