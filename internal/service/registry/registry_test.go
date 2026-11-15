package registry

import (
	"context"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"testing"
)

func TestReplyStatesAndBoundary(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "threads"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(ctx, kv, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e := Entry{Bot: "bot", ServerID: 1, ChannelID: 2, LastMessageID: 50}
	if err = r.BeginReply(ctx, 100, e, "key"); err != nil {
		t.Fatal(err)
	}
	got, _ := r.Lookup(100)
	if got.LastMessageID != 50 || got.Keys["key"].State != "pending" {
		t.Fatalf("pre-send entry: %+v", got)
	}
	if err = r.BeginReply(ctx, 100, e, "key"); err != nil {
		t.Fatal(err)
	}
	r.FinishReply(100, "key", false, false)
	got, _ = r.Lookup(100)
	if len(got.Keys) != 1 {
		t.Fatal("concurrent attempt removed")
	}
	r.FinishReply(100, "key", true, false)
	testutil.Until(t, func() bool { return r.Pending() == 0 })
	if err = r.BeginReply(ctx, 100, e, "key"); err != nil {
		t.Fatal(err)
	}
	r.FinishReply(100, "key", false, false)
	got, _ = r.Lookup(100)
	if got.Keys["key"].State != "sent" {
		t.Fatal("failure removed successful touch")
	}
	if err = r.BeginReply(ctx, 101, e, "key"); err != nil {
		t.Fatal(err)
	}
	r.FinishReply(101, "key", false, false)
	if _, ok := r.Lookup(101); ok {
		t.Fatal("known failure left first touch")
	}
	if err = r.BeginReply(ctx, 102, e, "key"); err != nil {
		t.Fatal(err)
	}
	if err = r.BeginReply(ctx, 102, e, "key"); err != nil {
		t.Fatal(err)
	}
	r.FinishReply(102, "key", false, true)
	r.FinishReply(102, "key", false, false)
	got, _ = r.Lookup(102)
	if got.Keys["key"].State != "pending" {
		t.Fatal("unknown outcome lost touch")
	}
	e.Bot = "other"
	if err = r.BeginReply(ctx, 100, e, "other"); err != ErrBotConflict {
		t.Fatal("bot ownership changed", err)
	}
	r2, err := New(ctx, kv, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	got, _ = r2.Lookup(100)
	if got.Keys["key"].State != "sent" {
		t.Fatal("touch not durable")
	}
	r.Advance(100, 70)
	r.Advance(100, 60)
	testutil.Until(t, func() bool { v, _ := r2.Lookup(100); return v.LastMessageID == 70 })
	if err = r.Delete(ctx, 100); err != nil {
		t.Fatal(err)
	}
	testutil.Until(t, func() bool { _, ok := r2.Lookup(100); return !ok })
}
