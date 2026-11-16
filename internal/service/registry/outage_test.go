package registry

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/service/testutil"
)

// stalledKV blocks every bucket request until released or until the caller's
// context ends, standing in for a slow or unreachable NATS server.
type stalledKV struct {
	jetstream.KeyValue
	mu      sync.Mutex
	release chan struct{}
	calls   int
}

func (k *stalledKV) stall() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.release = make(chan struct{})
}
func (k *stalledKV) resume() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.release != nil {
		close(k.release)
		k.release = nil
	}
}
func (k *stalledKV) wait(ctx context.Context) error {
	k.mu.Lock()
	k.calls++
	release := k.release
	k.mu.Unlock()
	if release == nil {
		return nil
	}
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (k *stalledKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if err := k.wait(ctx); err != nil {
		return nil, err
	}
	return k.KeyValue.Get(ctx, key)
}
func (k *stalledKV) Create(ctx context.Context, key string, data []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	if err := k.wait(ctx); err != nil {
		return 0, err
	}
	return k.KeyValue.Create(ctx, key, data, opts...)
}
func (k *stalledKV) Update(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	if err := k.wait(ctx); err != nil {
		return 0, err
	}
	return k.KeyValue.Update(ctx, key, data, revision)
}

func durable(t *testing.T, kv jetstream.KeyValue, id uint64) Entry {
	t.Helper()
	v, err := kv.Get(context.Background(), key(id))
	if err != nil {
		t.Fatal(err)
	}
	var e Entry
	if err = json.Unmarshal(v.Value(), &e); err != nil {
		t.Fatal(err)
	}
	return e
}

// Reads never wait on bucket I/O, and writes made while a write of the same
// entry is in flight are kept and written afterwards.
func TestLookupsStayResponsiveDuringStalledBucket(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "stall"})
	if err != nil {
		t.Fatal(err)
	}
	stalled := &stalledKV{KeyValue: kv}
	r, err := New(ctx, stalled, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	stalled.stall()
	for id := uint64(100); id < 105; id++ {
		if err = r.RegisterPost(id, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true, LastMessageID: id}, "key"); err != nil {
			t.Fatal(err)
		}
	}
	// Let the background writer enter its stalled requests.
	testutil.Until(t, func() bool { stalled.mu.Lock(); defer stalled.mu.Unlock(); return stalled.calls > 0 })
	t.Cleanup(stalled.resume)
	measured := make(chan time.Duration, 1)
	go func() {
		var worst time.Duration
		for range 200 {
			started := time.Now()
			_, _ = r.Lookup(102)
			_ = r.Snapshot()
			_ = r.Pending()
			if d := time.Since(started); d > worst {
				worst = d
			}
		}
		measured <- worst
	}()
	var worst time.Duration
	select {
	case worst = <-measured:
	case <-time.After(3 * time.Second):
		t.Fatal("lookups blocked behind a stalled bucket write")
	}
	if _, ok := r.Lookup(102); !ok {
		t.Fatal("registered thread missing")
	}
	if worst > 50*time.Millisecond {
		t.Fatalf("lookup waited on bucket I/O: worst %s", worst)
	}
	// A blocked pre-send write ends at its own deadline, and does not hold up
	// a reply to another thread's lookups or the caller's cancellation.
	call, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	started := time.Now()
	err = r.BeginReply(call, 200, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, LastMessageID: 10}, "key")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 600*time.Millisecond {
		t.Fatalf("blocked BeginReply ignored its deadline: %v after %s", err, time.Since(started))
	}
	if _, ok := r.Lookup(200); ok {
		t.Fatal("failed pre-send write touched the thread")
	}
	// Concurrent local changes while writes are stalled are not lost.
	r.Advance(101, 500)
	if err = r.RegisterPost(101, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true}, "second"); err != nil {
		t.Fatal(err)
	}
	stalled.resume()
	testutil.Until(t, func() bool { return r.Pending() == 0 })
	got := durable(t, kv, 101)
	if got.LastMessageID != 500 || got.Keys["key"].State != "sent" || got.Keys["second"].State != "sent" {
		t.Fatalf("concurrent change lost: %+v", got)
	}
	for id := uint64(100); id < 105; id++ {
		if durable(t, kv, id).Keys["key"].State != "sent" {
			t.Fatal("dirty entry not written after the outage", id)
		}
	}
}

// A local change made while the entry's write is in flight survives it.
func TestChangeDuringWriteStaysDirty(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "race"})
	if err != nil {
		t.Fatal(err)
	}
	stalled := &stalledKV{KeyValue: kv}
	r := newRegistry(ctx, stalled)
	defer r.cancel()
	if err = r.RegisterPost(7, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true, LastMessageID: 10}, "key"); err != nil {
		t.Fatal(err)
	}
	stalled.stall()
	done := make(chan error, 1)
	go func() { done <- r.flushOne(ctx, 7) }()
	testutil.Until(t, func() bool { stalled.mu.Lock(); defer stalled.mu.Unlock(); return stalled.calls > 0 })
	r.Advance(7, 99)
	stalled.resume()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if local, _ := r.Lookup(7); local.LastMessageID != 99 || r.Pending() != 1 {
		t.Fatalf("in-flight write discarded a newer change: %+v pending %d", local, r.Pending())
	}
	if err = r.flushOne(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if durable(t, kv, 7).LastMessageID != 99 || r.Pending() != 0 {
		t.Fatal("newer change not written")
	}
}

// Shutdown flushes dirty entries within its budget: they reach the real
// bucket when NATS is healthy, and an outage cannot hold shutdown longer.
func TestFlushBudget(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "flush"})
	if err != nil {
		t.Fatal(err)
	}
	stalled := &stalledKV{KeyValue: kv}
	r := newRegistry(ctx, stalled)
	defer r.cancel()
	for id := uint64(1); id <= 3; id++ {
		if err = r.RegisterPost(id, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true}, "key"); err != nil {
			t.Fatal(err)
		}
	}
	r.Advance(2, 77)
	budget, cancel := context.WithTimeout(ctx, 2*time.Second)
	if left := r.Flush(budget); left != 0 {
		t.Fatal("healthy flush left entries", left)
	}
	cancel()
	if durable(t, kv, 2).LastMessageID != 77 {
		t.Fatal("flushed boundary not durable")
	}
	r.Advance(3, 88)
	stalled.stall()
	defer stalled.resume()
	budget, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	if left := r.Flush(budget); left != 1 {
		t.Fatal("outage flush reported", left)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatal("outage flush exceeded its budget", elapsed)
	}
}

// slowWatchKV returns its watcher only after the caller's stop: nats.go then
// closes the update channel, which used to read as a load failure.
type slowWatchKV struct {
	jetstream.KeyValue
	stop func()
}

func (k slowWatchKV) Watch(ctx context.Context, keys string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	w, err := k.KeyValue.Watch(ctx, keys, opts...)
	k.stop()
	time.Sleep(5 * time.Millisecond)
	return w, err
}

func TestStopDuringLoadIsCancellation(t *testing.T) {
	_, _, js := testutil.NATS(t)
	kv, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: "load"})
	if err != nil {
		t.Fatal(err)
	}
	for id := range 50 {
		if _, err = kv.Put(context.Background(), key(uint64(id+1)), []byte(`{"bot":"bot","keys":{"k":{"state":"sent"}}}`)); err != nil {
			t.Fatal(err)
		}
	}
	stopped := 0
	for i := range 60 {
		ctx, cancel := context.WithCancel(context.Background())
		r, err := New(ctx, slowWatchKV{KeyValue: kv, stop: cancel}, 0)
		cancel()
		if r != nil {
			r.Close() // the load finished before the stop was seen
			continue
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run %d: stop during load reported %v", i, err)
		}
		stopped++
	}
	if stopped == 0 {
		t.Fatal("no run stopped during the load")
	}
}

// The hourly sweep deletes entries whose latest touch or publish is older
// than max_age; the delete reaches every watcher, and fresh entries stay.
func TestSweepDeletesStaleEntries(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "sweep"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := New(ctx, kv, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	watcher, err := New(ctx, kv, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	for _, id := range []uint64{1, 2} {
		if err = r.RegisterPost(id, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true}, "key"); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Until(t, func() bool { _, a := watcher.Lookup(1); _, b := watcher.Lookup(2); return a && b && r.Pending() == 0 })
	r.mu.Lock()
	stale := r.entries[1]
	stale.ActiveAt = time.Now().Add(-2 * time.Hour)
	r.entries[1] = stale
	r.mu.Unlock()
	r.Sweep(time.Hour)
	testutil.Until(t, func() bool { _, gone := watcher.Lookup(1); _, kept := watcher.Lookup(2); return !gone && kept })
	if _, ok := r.Lookup(1); ok {
		t.Fatal("swept entry still local")
	}
}
