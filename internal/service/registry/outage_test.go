package registry

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
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

// afterUpdate runs a hook once a revision-checked write has landed, before
// the writer reconciles: another instance's newer write arrives there.
type afterUpdate struct {
	jetstream.KeyValue
	after func()
}

func (k afterUpdate) Update(ctx context.Context, name string, value []byte, revision uint64) (uint64, error) {
	rev, err := k.KeyValue.Update(ctx, name, value, revision)
	if err == nil && k.after != nil {
		k.after()
	}
	return rev, err
}

// newerRemote writes another instance's change on top of the current
// revision and delivers it through the watch path, as the real watch does.
func newerRemote(t *testing.T, r *Registry, kv jetstream.KeyValue, id uint64) {
	t.Helper()
	ctx := context.Background()
	v, err := kv.Get(ctx, key(id))
	if err != nil {
		t.Fatal(err)
	}
	var remote Entry
	if err = json.Unmarshal(v.Value(), &remote); err != nil {
		t.Fatal(err)
	}
	remote.Keys["remote"] = Touch{State: "sent"}
	remote.LastMessageID = 999
	raw, _ := json.Marshal(remote)
	if _, err = kv.Update(ctx, key(id), raw, v.Revision()); err != nil {
		t.Fatal(err)
	}
	if v, err = kv.Get(ctx, key(id)); err != nil {
		t.Fatal(err)
	}
	r.apply(v)
}

// R1-02: a newer bucket revision delivered by the watch while this
// instance's write is in flight survives the write's reconciliation, for a
// pre-send write and for a background write of a dirty entry.
func TestWatchedRevisionDuringWriteSurvives(t *testing.T) {
	for _, path := range []string{"begin-reply", "flush"} {
		t.Run(path, func(t *testing.T) {
			_, _, js := testutil.NATS(t)
			ctx := context.Background()
			kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "watched"})
			if err != nil {
				t.Fatal(err)
			}
			r := newRegistry(ctx, kv)
			defer r.cancel()
			if err = r.RegisterPost(7, Entry{Bot: "bot", ServerID: 1, ChannelID: 2, LastMessageID: 10}, "initial"); err != nil {
				t.Fatal(err)
			}
			if err = r.flushOne(ctx, 7); err != nil {
				t.Fatal(err)
			}
			once := false
			r.KV = afterUpdate{KeyValue: kv, after: func() {
				if !once {
					once = true
					newerRemote(t, r, kv, 7)
				}
			}}
			if path == "begin-reply" {
				err = r.BeginReply(ctx, 7, Entry{Bot: "bot"}, "caller")
			} else {
				r.Advance(7, 20)
				err = r.flushOne(ctx, 7)
			}
			if err != nil {
				t.Fatal(err)
			}
			local, _ := r.Lookup(7)
			if local.Keys["remote"].State != "sent" || local.LastMessageID != 999 {
				t.Fatalf("reconciliation erased the newer watched revision: %+v pending %d", local, r.Pending())
			}
			if path == "begin-reply" && local.Keys["caller"].State != "pending" {
				t.Fatal("the pre-send touch was lost", local)
			}
			if r.Pending() != 0 {
				t.Fatal("entry left dirty although the bucket holds every change")
			}
		})
	}
}

// heldDelete holds the first bucket delete until released.
type heldDelete struct {
	jetstream.KeyValue
	first            atomic.Bool
	entered, release chan struct{}
}

func (k *heldDelete) Delete(ctx context.Context, name string, opts ...jetstream.KVDeleteOpt) error {
	if k.first.CompareAndSwap(false, true) {
		close(k.entered)
		<-k.release
	}
	return k.KeyValue.Delete(ctx, name, opts...)
}

// R2-01: the sweep keeps a thread touched or published after it was chosen
// as stale, both one whose delete is already in flight and one still
// waiting its turn.
func TestSweepKeepsThreadsActiveSinceSelection(t *testing.T) {
	_, _, js := testutil.NATS(t)
	ctx := context.Background()
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "sweeprace"})
	if err != nil {
		t.Fatal(err)
	}
	held := &heldDelete{KeyValue: kv, entered: make(chan struct{}), release: make(chan struct{})}
	r := newRegistry(ctx, held)
	defer r.cancel()
	for _, id := range []uint64{7, 8} {
		stale := Entry{Bot: "bot", ServerID: 1, ChannelID: 2, CreatedByDeck: true, ActiveAt: time.Now().Add(-2 * time.Hour), Keys: map[string]Touch{"caller": {State: "sent"}}}
		raw, _ := json.Marshal(stale)
		if _, err = kv.Put(ctx, key(id), raw); err != nil {
			t.Fatal(err)
		}
		record, err := kv.Get(ctx, key(id))
		if err != nil {
			t.Fatal(err)
		}
		r.apply(record)
	}
	done := make(chan struct{})
	go func() { r.Sweep(time.Hour); close(done) }()
	<-held.entered // both chosen; the first delete is in flight
	r.Advance(7, 999)
	r.Advance(8, 999)
	close(held.release)
	<-done
	for _, id := range []uint64{7, 8} {
		e, ok := r.Lookup(id)
		if !ok || e.LastMessageID != 999 || e.Keys["caller"].State != "sent" {
			t.Fatalf("sweep erased thread %d made active after its selection: %+v %t", id, e, ok)
		}
	}
	// The in-flight delete landed; the active entry is written back.
	for _, id := range []uint64{7, 8} {
		if err = r.flushOne(ctx, id); err != nil {
			t.Fatal(err)
		}
		if got := durable(t, kv, id); got.LastMessageID != 999 || got.Keys["caller"].State != "sent" {
			t.Fatalf("thread %d not durable after the sweep: %+v", id, got)
		}
	}
}
