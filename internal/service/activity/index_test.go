package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func init() {
	auditInterval = 100 * time.Millisecond
	catchUpPoll = 100 * time.Millisecond
}

func natsCfg(manage bool) config.NATS {
	return config.NATS{SubjectPrefix: "deck", Stream: config.Stream{Name: "DECK", Manage: manage, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}}
}

// relay cuts only the service's link to NATS; an admin connection stays up.
type relay struct {
	listener net.Listener
	target   string
	online   atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
}

func newRelay(t *testing.T, target string) *relay {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &relay{listener: ln, target: target}
	r.online.Store(true)
	t.Cleanup(func() { _ = ln.Close(); r.cut() })
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			if !r.online.Load() {
				r.mu.Unlock()
				_ = client.Close()
				continue
			}
			upstream, err := net.DialTimeout("tcp", r.target, time.Second)
			if err != nil {
				r.mu.Unlock()
				_ = client.Close()
				continue
			}
			r.conns = append(r.conns, client, upstream)
			r.mu.Unlock()
			go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close(); _ = client.Close() }()
			go func() { _, _ = io.Copy(client, upstream); _ = upstream.Close(); _ = client.Close() }()
		}
	}()
	return r
}

func (r *relay) cut() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.online.Store(false)
	for _, c := range r.conns {
		_ = c.Close()
	}
	r.conns = nil
}

type fixture struct {
	t       *testing.T
	server  *server.Server
	admin   jetstream.JetStream
	service jetstream.JetStream
	conn    *nats.Conn
	kv      jetstream.KeyValue
	events  *ConnectionEvents
	relay   *relay
	index   *Index
	cfg     config.NATS
}

// newFixture connects the index through a relay, with the connection's
// handlers wired to a ConnectionEvents hub as Connect does.
func newFixture(t *testing.T, manage bool) *fixture {
	t.Helper()
	s, _, admin := testutil.NATS(t)
	f := &fixture{t: t, server: s, admin: admin, events: &ConnectionEvents{}, cfg: natsCfg(manage)}
	f.relay = newRelay(t, s.Addr().String())
	conn, err := nats.Connect("nats://"+f.relay.listener.Addr().String(), nats.MaxReconnects(-1), nats.ReconnectWait(20*time.Millisecond), nats.ReconnectJitter(0, 0),
		nats.ReconnectHandler(func(*nats.Conn) { f.events.fire(true) }), nats.DisconnectErrHandler(func(*nats.Conn, error) { f.events.fire(false) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	f.conn = conn
	if f.service, err = jetstream.New(conn); err != nil {
		t.Fatal(err)
	}
	if _, err = EnsureStream(context.Background(), admin, natsCfg(true)); err != nil {
		t.Fatal(err)
	}
	if f.kv, err = admin.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: "threads"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) start() *Index {
	f.t.Helper()
	kv, err := f.service.KeyValue(context.Background(), "threads")
	if err != nil {
		f.t.Fatal(err)
	}
	f.index = NewIndex(context.Background(), f.service, kv, f.cfg, f.events, nil)
	f.t.Cleanup(f.index.Close)
	return f.index
}

func activity(id, thread uint64) *pb.Activity {
	return &pb.Activity{Id: id, Time: timestamppb.Now(), Bot: "bot", ServerId: 1, ChannelId: 2, ThreadId: thread, Kind: &pb.Activity_Message{Message: &pb.Message{Content: fmt.Sprint(id)}}}
}

func publish(t *testing.T, js jetstream.JetStream, from, through uint64) {
	t.Helper()
	for n := from; n <= through; n++ {
		m, err := Envelope("deck", activity(1000+n, 9), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = js.PublishMsg(ctx, m)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (f *fixture) created() time.Time {
	f.t.Helper()
	s, err := f.admin.Stream(context.Background(), "DECK")
	if err != nil {
		f.t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		f.t.Fatal(err)
	}
	return info.Created
}

func (f *fixture) record() generation {
	f.t.Helper()
	v, err := f.kv.Get(context.Background(), GenerationKey)
	if err != nil {
		f.t.Fatal(err)
	}
	var g generation
	if err = json.Unmarshal(v.Value(), &g); err != nil {
		f.t.Fatal(err)
	}
	return g
}

// sequences waits for the gate and returns the thread's indexed sequences.
func (f *fixture) sequences(want int) []uint64 {
	f.t.Helper()
	var got []uint64
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		u, err := f.index.Unread(ctx, []*pb.Cursor{{ThreadId: 9}}, 0)
		cancel()
		if err == nil {
			got = got[:0]
			for _, e := range u.Listed {
				got = append(got, e.Sequence)
			}
			if len(got) == want {
				return got
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	f.t.Fatalf("index holds %v, want %d entries", got, want)
	return nil
}

func expectSequences(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("indexed %v, want %v", got, want)
	}
}

func (f *fixture) epoch() uint64 {
	f.index.mu.Lock()
	defer f.index.mu.Unlock()
	return f.index.epoch
}

func (f *fixture) recreate(publishNew bool) {
	f.t.Helper()
	ctx := context.Background()
	if err := f.admin.DeleteStream(ctx, "DECK"); err != nil {
		f.t.Fatal(err)
	}
	if _, err := EnsureStream(ctx, f.admin, natsCfg(true)); err != nil {
		f.t.Fatal(err)
	}
	if publishNew {
		publish(f.t, f.admin, 1, 7)
	}
}

// The embedded recreation trace: old 1-5 indexed, the stream deleted and
// recreated while the consumer runs, new 1-7 published. The consumer error
// triggers the check though its consume context stays open.
func TestRecreationTraceReplaysNewGeneration(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 5)
	expectSequences(t, f.sequences(5), 1, 2, 3, 4, 5)
	old := f.epoch()
	if f.record().Epoch != old || !f.record().Created.Equal(f.created()) {
		t.Fatal("generation record does not match the stream")
	}
	f.recreate(true)
	expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
	if f.epoch() == old || f.record().Epoch != f.epoch() || !f.record().Created.Equal(f.created()) {
		t.Fatal("epoch did not follow the recreated stream")
	}
}

// Deleting the stream with management on creates a replacement before the
// generation check; with management off the index waits for the operator.
func TestMissingStreamManagedAndUnmanaged(t *testing.T) {
	for _, manage := range []bool{true, false} {
		t.Run(fmt.Sprint("manage=", manage), func(t *testing.T) {
			f := newFixture(t, manage)
			f.start()
			publish(t, f.admin, 1, 5)
			f.sequences(5)
			old := f.epoch()
			if err := f.admin.DeleteStream(context.Background(), "DECK"); err != nil {
				t.Fatal(err)
			}
			if !manage {
				time.Sleep(300 * time.Millisecond)
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				_, err := f.index.Position(ctx)
				cancel()
				if !errors.Is(err, ErrUnavailable) || f.index.State() == "ready" {
					t.Fatal("unmanaged index answered without a stream", err)
				}
				if _, err = EnsureStream(context.Background(), f.admin, natsCfg(true)); err != nil {
					t.Fatal(err)
				}
			}
			testutil.Until(t, func() bool { _, err := f.admin.Stream(context.Background(), "DECK"); return err == nil })
			publish(t, f.service, 1, 7)
			expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
			if f.epoch() == old {
				t.Fatal("epoch unchanged after recreation")
			}
		})
	}
}

// Deleting only the consumer keeps the epoch and resumes after the last
// indexed sequence.
func TestConsumerOnlyDeletionKeepsGeneration(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 5)
	f.sequences(5)
	old := f.epoch()
	ctx := context.Background()
	s, _ := f.admin.Stream(ctx, "DECK")
	lister := s.ListConsumers(ctx)
	for info := range lister.Info() {
		if err := f.admin.DeleteConsumer(ctx, "DECK", info.Name); err != nil {
			t.Fatal(err)
		}
	}
	publish(t, f.admin, 6, 7)
	expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
	if f.epoch() != old {
		t.Fatal("consumer deletion changed the epoch")
	}
}

// Spike cases (a)-(d), each run five times: the connection's reconnect
// callback marks the check due, and every retained new sequence is indexed.
func TestReconnectCases(t *testing.T) {
	cases := []struct {
		name                     string
		recreate, before, manage bool
	}{
		{"a-published-while-disconnected", true, true, true},
		{"b-service-publishes-after-reconnect", true, false, true},
		{"c-management-recreates-missing-stream", false, false, true},
		{"d-unmanaged-waits-for-operator", false, false, false},
	}
	for _, c := range cases {
		for run := range 5 {
			t.Run(fmt.Sprintf("%s/%d", c.name, run), func(t *testing.T) {
				f := newFixture(t, c.manage)
				f.start()
				publish(t, f.admin, 1, 5)
				f.sequences(5)
				old := f.epoch()
				f.index.mu.Lock()
				due := f.index.due
				f.index.mu.Unlock()
				f.relay.cut()
				testutil.Until(t, func() bool { return f.index.State() != "ready" })
				ctx := context.Background()
				if err := f.admin.DeleteStream(ctx, "DECK"); err != nil {
					t.Fatal(err)
				}
				if c.recreate {
					if _, err := EnsureStream(ctx, f.admin, natsCfg(true)); err != nil {
						t.Fatal(err)
					}
					if c.before {
						publish(t, f.admin, 1, 7)
					}
				}
				f.relay.online.Store(true)
				// The reconnect callback itself marks the check due.
				testutil.Until(t, func() bool { f.index.mu.Lock(); defer f.index.mu.Unlock(); return f.index.due > due })
				if !c.manage {
					time.Sleep(200 * time.Millisecond)
					if f.index.State() == "ready" {
						t.Fatal("unmanaged index opened without a stream")
					}
					if _, err := EnsureStream(ctx, f.admin, natsCfg(true)); err != nil {
						t.Fatal(err)
					}
				}
				testutil.Until(t, func() bool { _, err := f.admin.Stream(ctx, "DECK"); return err == nil })
				if !c.before {
					publish(t, f.service, 1, 7)
				}
				expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
				if f.epoch() == old || !f.record().Created.Equal(f.created()) {
					t.Fatal("generation not changed to the recreated stream")
				}
			})
		}
	}
}

// lossyKV writes the record, then reports the acknowledgement as lost once.
type lossyKV struct {
	jetstream.KeyValue
	lose atomic.Bool
}

func (k *lossyKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	rev, err := k.KeyValue.Create(ctx, key, value, opts...)
	if err == nil && k.lose.CompareAndSwap(true, false) {
		return 0, nats.ErrTimeout
	}
	return rev, err
}
func (k *lossyKV) Update(ctx context.Context, key string, value []byte, last uint64) (uint64, error) {
	rev, err := k.KeyValue.Update(ctx, key, value, last)
	if err == nil && k.lose.CompareAndSwap(true, false) {
		return 0, nats.ErrTimeout
	}
	return rev, err
}

// A record whose created matches the stream but whose epoch differs from the
// index's discards old entries, adopts that epoch and replays: after another
// instance wrote it, and after this instance's write acknowledgement was lost.
func TestRecordEpochAdoption(t *testing.T) {
	t.Run("other-instance", func(t *testing.T) {
		f := newFixture(t, true)
		f.start()
		publish(t, f.admin, 1, 7)
		f.sequences(7)
		g := f.record()
		g.Epoch += 1000
		raw, _ := json.Marshal(g)
		if _, err := f.kv.Put(context.Background(), GenerationKey, raw); err != nil {
			t.Fatal(err)
		}
		f.events.fire(true) // the next reconnect check reads it
		testutil.Until(t, func() bool { return f.epoch() == g.Epoch })
		expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
	})
	t.Run("lost-acknowledgement", func(t *testing.T) {
		f := newFixture(t, true)
		kv, _ := f.service.KeyValue(context.Background(), "threads")
		lossy := &lossyKV{KeyValue: kv}
		f.index = NewIndex(context.Background(), f.service, lossy, f.cfg, f.events, nil)
		t.Cleanup(f.index.Close)
		publish(t, f.admin, 1, 5)
		f.sequences(5)
		old := f.epoch()
		lossy.lose.Store(true)
		f.recreate(true)
		expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
		if lossy.lose.Load() {
			t.Fatal("the lossy write never happened")
		}
		if f.epoch() == old || f.epoch() != f.record().Epoch {
			t.Fatal("index did not adopt the record that landed")
		}
	})
}

// Recreating between the first creation-time read and the fresh consumer's
// creation repeats the check.
func TestRecreationBeforeConsumerRepeatsCheck(t *testing.T) {
	f := newFixture(t, true)
	var once sync.Once
	var armed atomic.Bool
	beforeConsumer = func() {
		if armed.Load() {
			once.Do(func() { f.recreate(true) })
		}
	}
	t.Cleanup(func() { beforeConsumer = nil })
	f.start()
	publish(t, f.admin, 1, 3)
	f.sequences(3)
	armed.Store(true)
	f.events.fire(true)
	expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
	if !f.record().Created.Equal(f.created()) || f.record().Epoch != f.epoch() {
		t.Fatal("index kept the generation it read before the recreation")
	}
}

// An unchanged generation already caught up opens its gate without a new
// message, and an empty stream opens at once.
func TestCaughtUpAndEmptyStreamOpen(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := f.index.Position(ctx)
	if err != nil || p.Sequence != 0 {
		t.Fatal("empty stream did not open", p, err)
	}
	publish(t, f.admin, 1, 2)
	f.sequences(2)
	f.events.fire(true)
	testutil.Until(t, func() bool { return f.index.State() == "ready" })
	if p, err = f.index.Position(ctx); err != nil || p.Sequence != 2 || p.Epoch != f.epoch() {
		t.Fatal(p, err)
	}
}

// R2-01: a stream whose messages all expired opens the gate at start and
// after a check.
func TestAllExpiredStreamOpens(t *testing.T) {
	f := newFixture(t, true)
	f.cfg.Stream.MaxAge = time.Second
	f.cfg.Stream.DuplicateWindow = 500 * time.Millisecond
	if _, err := EnsureStream(context.Background(), f.admin, f.cfg); err != nil {
		t.Fatal(err)
	}
	publish(t, f.admin, 1, 3)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s, _ := f.admin.Stream(context.Background(), "DECK")
		info, _ := s.Info(context.Background())
		if info.State.Msgs == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := f.index.Position(ctx)
	if err != nil || p.Sequence != 3 {
		t.Fatal("expired stream kept the gate shut", p, err)
	}
}

// R2-02: a stream that lost messages the index holds, keeping its creation
// time, gets a new epoch rather than reusing positions.
func TestLostNewestMessagesChangeEpoch(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 3)
	f.sequences(3)
	old := f.epoch()
	g, err := f.index.generation(context.Background(), f.created(), true)
	if err != nil || g.Epoch == old || !g.Created.Equal(f.created()) {
		t.Fatal("lost messages kept the epoch", g, err)
	}
	if g, err = f.index.generation(context.Background(), f.created(), false); err != nil || g.Epoch == old {
		t.Fatal("record not durable", g, err)
	}
}

// The audit catches a recreated stream even when no consumer trigger fires.
func TestAuditCatchesSilentRecreation(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 5)
	f.sequences(5)
	old := f.epoch()
	muteConsumerTriggers.Store(true)
	t.Cleanup(func() { muteConsumerTriggers.Store(false) })
	f.recreate(true)
	// The library's own reset can deliver new sequences 6 and 7 onto the old
	// entries before the audit runs; wait for the audit's new generation.
	deadline := time.Now().Add(5 * time.Second)
	for f.epoch() == old {
		if time.Now().After(deadline) {
			t.Fatal("audit missed the recreation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
}

func sent(id, thread uint64, nonce string) *pb.Activity {
	a := activity(id, thread)
	a.SentBy, a.SenderKey, a.Nonce = "key", "SHA256:A", nonce
	return a
}

// A completed send is found by fingerprint and nonce, with its messages
// read from the stream in order, its start position and poll error, also by
// a fresh index after a restart; Confirm waits until the index holds it.
func TestCompletedSendAndConfirm(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := New(ctx, f.admin, "deck")
	defer p.Close()
	start := &pb.StreamPosition{Epoch: 5, Sequence: 3}
	starter, poll := sent(500, 9, "n1"), sent(501, 9, "n1")
	var err error
	if starter.Sequence, err = p.Publish(ctx, starter, start, SendMetadata{PollError: "poll failed"}); err != nil {
		t.Fatal(err)
	}
	if poll.Sequence, err = p.Publish(ctx, poll, start); err != nil {
		t.Fatal(err)
	}
	if err = f.index.Confirm(ctx, "SHA256:A", "n1", []*pb.Activity{starter, poll}); err != nil {
		t.Fatal(err)
	}
	for _, index := range []*Index{f.index, f.start()} {
		got, err := index.Completed(ctx, "SHA256:A", "n1")
		if err != nil || got == nil || len(got.Messages) != 2 || got.Messages[0].Id != 500 || got.Messages[1].Id != 501 || got.Messages[0].Sequence != starter.Sequence || got.Start.Sequence != 3 || got.PollError != "poll failed" {
			t.Fatal("completed send not reconstructed", got, err)
		}
		if other, err := index.Completed(ctx, "SHA256:B", "n1"); other != nil || err != nil {
			t.Fatal("another key's nonce matched", other, err)
		}
	}
	// A publish whose stream was replaced is never held: Confirm returns
	// once the new generation passed its sequence.
	lost := sent(600, 9, "n2")
	lost.Sequence = 3
	f.recreate(true)
	if err = f.index.Confirm(ctx, "SHA256:A", "n2", []*pb.Activity{lost}); err != nil {
		t.Fatal(err)
	}
}

// A body read by sequence is never returned under another generation.
func TestBodiesNeverCrossGenerations(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 5)
	f.sequences(5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := f.index.Unread(ctx, []*pb.Cursor{{ThreadId: 9}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	muteConsumerTriggers.Store(true)
	t.Cleanup(func() { muteConsumerTriggers.Store(false) })
	if err = f.admin.DeleteStream(ctx, "DECK"); err != nil {
		t.Fatal(err)
	}
	if _, err = EnsureStream(ctx, f.admin, natsCfg(true)); err != nil {
		t.Fatal(err)
	}
	for n := uint64(1); n <= 7; n++ {
		m, _ := Envelope("deck", activity(9000+n, 9), nil)
		if _, err = f.admin.PublishMsg(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.index.List(ctx, u); !IsRetry(err) {
		t.Fatal("old generation's entries returned new bodies", err)
	}
	expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
	u, err = f.index.Unread(ctx, []*pb.Cursor{{ThreadId: 9}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	bodies, err := f.index.List(ctx, u)
	if err != nil || len(bodies) != 7 || bodies[0].Id != 9001 || u.Through.Epoch != f.epoch() {
		t.Fatal("new generation's bodies", bodies, err)
	}
}

// Counts follow the cursor's epoch, list own copies, mark the horizon, and
// drop what the stream no longer holds.
func TestCountsHorizonAndPrune(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := New(ctx, f.admin, "deck")
	defer p.Close()
	for n := uint64(1); n <= 5; n++ {
		a := activity(100+n, 9)
		if n == 2 {
			a = sent(102, 9, "n")
		}
		if n == 3 {
			a.GetMessage().Type = 18 // a system message never counts
		}
		var start *pb.StreamPosition
		if a.SentBy != "" {
			start = &pb.StreamPosition{Epoch: 1, Sequence: 1}
		}
		if _, err := p.Publish(ctx, a, start); err != nil {
			t.Fatal(err)
		}
	}
	expectSequences(t, f.sequences(4), 1, 2, 4, 5) // the system message is not listed
	epoch := f.epoch()
	u, err := f.index.Unread(ctx, []*pb.Cursor{{ThreadId: 9, After: &pb.StreamPosition{Epoch: epoch, Sequence: 1}}, {ThreadId: 9, After: &pb.StreamPosition{Epoch: epoch + 1, Sequence: 99}}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	same, other := u.Counts[0], u.Counts[1]
	if same.Count != 3 || same.Horizon || len(same.Own) != 1 || same.Own[0].Id != 102 || same.Own[0].SenderKey != "SHA256:A" || same.Own[0].Nonce != "n" {
		t.Fatalf("same-epoch count %+v", same)
	}
	if other.Count != 4 || !other.Horizon {
		t.Fatalf("other-epoch count %+v", other)
	}
	if !u.More || len(u.Listed) != 2 || u.Through.Sequence != u.Listed[1].Sequence {
		t.Fatalf("limited list through %+v", u.Through)
	}
	s, _ := f.admin.Stream(ctx, "DECK")
	if err = s.Purge(ctx, jetstream.WithPurgeKeep(2)); err != nil {
		t.Fatal(err)
	}
	testutil.Until(t, func() bool {
		u, err := f.index.Unread(ctx, []*pb.Cursor{{ThreadId: 9, After: &pb.StreamPosition{Epoch: epoch, Sequence: 1}}}, 0)
		return err == nil && u.Counts[0].Horizon && u.Counts[0].Count == 2 && len(u.Counts[0].Own) == 0
	})
	u, _ = f.index.Unread(ctx, []*pb.Cursor{{ThreadId: 9, After: &pb.StreamPosition{Epoch: epoch, Sequence: 3}}}, 0)
	if u.Counts[0].Horizon || f.index.Holds(9, "message", 102) || !f.index.Holds(9, "message", 105) {
		t.Fatal("prune or horizon wrong", u.Counts[0])
	}
}

// The epoch is the current time in Unix nanoseconds, or the recorded epoch
// plus one when the clock is not ahead of it.
func TestEpochRule(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	f.sequences(0)
	ahead := generation{Epoch: uint64(time.Now().Add(time.Hour).UnixNano()), Created: time.Unix(1, 0).UTC()}
	raw, _ := json.Marshal(ahead)
	if _, err := f.kv.Put(context.Background(), GenerationKey, raw); err != nil {
		t.Fatal(err)
	}
	g, err := f.index.generation(context.Background(), f.created(), false)
	if err != nil || g.Epoch != ahead.Epoch+1 || !g.Created.Equal(f.created()) {
		t.Fatal("epoch did not move past a record ahead of the clock", g, err)
	}
	before := uint64(time.Now().UnixNano())
	if err = f.kv.Delete(context.Background(), GenerationKey); err != nil {
		t.Fatal(err)
	}
	if g, err = f.index.generation(context.Background(), f.created(), false); err != nil || g.Epoch < before || g.Epoch > uint64(time.Now().UnixNano()) {
		t.Fatal("epoch is not the current time", g, err)
	}
}

// The connection's reconnect callback alone marks the check due: with the
// consumer's triggers and the audit silenced, a stream recreated while the
// service was cut off is still replayed after the reconnect.
func TestReconnectCallbackAloneTriggersCheck(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 5)
	f.sequences(5)
	old := f.epoch()
	muteConsumerTriggers.Store(true)
	muteAudit.Store(true)
	t.Cleanup(func() { muteConsumerTriggers.Store(false); muteAudit.Store(false) })
	f.relay.cut()
	testutil.Until(t, func() bool { return f.index.State() != "ready" })
	f.recreate(true)
	f.relay.online.Store(true)
	expectSequences(t, f.sequences(7), 1, 2, 3, 4, 5, 6, 7)
	if f.epoch() == old {
		t.Fatal("reconnect did not change the generation")
	}
}

// A stream deleted from another connection is recreated only after its
// deletion has finished. nats-server removes a deleted stream from its
// account before its files, and the consumer-deleted notice that triggers
// the check arrives in between; a stream created in that window shares the
// directory the deletion removes, and every write to it then fails with
// JetStream error 10077. Many consumers widen the deletion's window.
func TestRecreationWaitsForTheDeletion(t *testing.T) {
	f := newFixture(t, true)
	f.start()
	publish(t, f.admin, 1, 3)
	f.sequences(3)
	ctx := context.Background()
	for n := range 300 {
		if _, err := f.admin.CreateOrUpdateConsumer(ctx, "DECK", jetstream.ConsumerConfig{Durable: fmt.Sprint("widen", n)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.admin.DeleteStream(ctx, "DECK"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := f.admin.Stream(ctx, "DECK"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the index did not recreate the stream")
		}
		time.Sleep(20 * time.Millisecond)
	}
	publish(t, f.service, 1, 3) // fails with 10077 if created inside the deletion
	expectSequences(t, f.sequences(3), 1, 2, 3)
}

// stalledManagement holds the check's first stream-management request until
// its context ends, as a request lost across a reconnect would wait out its
// timeout.
type stalledManagement struct {
	jetstream.JetStream
	first   atomic.Bool
	entered chan struct{}
}

func (j *stalledManagement) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	if j.first.CompareAndSwap(false, true) {
		close(j.entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return j.JetStream.UpdateStream(ctx, cfg)
}

// R1-07: a newer trigger cancels the running check's requests, so a stalled
// request does not hold the check; the index opens promptly after it.
func TestNewerTriggerCancelsStalledCheck(t *testing.T) {
	f := newFixture(t, true)
	stalled := &stalledManagement{JetStream: f.service, entered: make(chan struct{})}
	kv, err := f.service.KeyValue(context.Background(), "threads")
	if err != nil {
		t.Fatal(err)
	}
	index := NewIndex(context.Background(), stalled, kv, f.cfg, f.events, nil)
	t.Cleanup(index.Close)
	select {
	case <-stalled.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the check never reached stream management")
	}
	f.events.fire(true) // a reconnect: the newer trigger
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err = index.Position(ctx); err != nil {
		t.Fatal("the newer trigger did not release the stalled check", err)
	}
}
