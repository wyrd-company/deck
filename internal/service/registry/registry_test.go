package registry

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"sync/atomic"
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
	// A later known failure must preserve an earlier unknown outcome too.
	if err = r.BeginReply(ctx, 103, e, "key"); err != nil {
		t.Fatal(err)
	}
	r.FinishReply(103, "key", false, true)
	if err = r.BeginReply(ctx, 103, e, "key"); err != nil {
		t.Fatal(err)
	}
	r.FinishReply(103, "key", false, false)
	got, ok := r.Lookup(103)
	if !ok || got.Keys["key"].State != "pending" {
		t.Fatal("later known failure removed unknown touch")
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

type flakyKV struct {
	jetstream.KeyValue
	fail atomic.Bool
}

func (k *flakyKV) Create(ctx context.Context, key string, data []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	if k.fail.Load() {
		return 0, errors.New("injected KV failure")
	}
	return k.KeyValue.Create(ctx, key, data, opts...)
}
func (k *flakyKV) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	if k.fail.Load() {
		return 0, errors.New("injected KV failure")
	}
	return k.KeyValue.Update(ctx, key, data, revision)
}
func (k *flakyKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if k.fail.Load() {
		return errors.New("injected KV failure")
	}
	return k.KeyValue.Delete(ctx, key, opts...)
}
func TestPersistenceRetryAndFailedBegin(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "retry"})
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyKV{KeyValue: kv}
	r, err := New(ctx, flaky, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	e := Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true, LastMessageID: 99}
	flaky.fail.Store(true)
	if err = r.RegisterPost(100, e, "first"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup(100); !ok {
		t.Fatal("post was not registered immediately")
	}
	if r.Pending() != 1 {
		t.Fatal("post write not pending")
	}
	flaky.fail.Store(false)
	testutil.Until(t, func() bool { return r.Pending() == 0 })
	stored, err := kv.Get(ctx, "threads.100")
	if err != nil {
		t.Fatal(err)
	}
	var durable Entry
	_ = json.Unmarshal(stored.Value(), &durable)
	if durable.LastMessageID != 99 || durable.Keys["first"].State != "sent" {
		t.Fatal("post not durable")
	}
	flaky.fail.Store(true)
	if err = r.BeginReply(ctx, 100, e, "second"); err == nil {
		t.Fatal("failed write allowed reply")
	}
	local, _ := r.Lookup(100)
	if _, ok := local.Keys["second"]; ok {
		t.Fatal("failed pre-send write touched existing thread")
	}
	flaky.fail.Store(false)
	e.CreatedByDeck = false
	if err = r.BeginReply(ctx, 101, e, "first"); err != nil {
		t.Fatal(err)
	}
	flaky.fail.Store(true)
	r.FinishReply(101, "first", false, false)
	if _, ok := r.Lookup(101); ok {
		t.Fatal("known failure left local touch")
	}
	if r.Pending() != 1 {
		t.Fatal("failed cleanup not queued")
	}
	flaky.fail.Store(false)
	testutil.Until(t, func() bool { return r.Pending() == 0 })
	if _, err = kv.Get(ctx, "threads.101"); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatal("cleanup did not persist", err)
	}
}
