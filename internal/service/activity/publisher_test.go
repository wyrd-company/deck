package activity

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPublishRetryAndEnvelope(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	cfg := config.NATS{SubjectPrefix: "deck", Stream: config.Stream{Name: "DECK", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}}
	p := New(ctx, js, "deck")
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
	stream, err := EnsureStream(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Until(t, func() bool { return p.Pending() == 0 })
	seq, err := p.Publish(ctx, a, start)
	if err != nil || seq != 1 {
		t.Fatal(seq, err)
	}
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
