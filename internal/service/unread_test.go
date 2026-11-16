package service

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/activity"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"github.com/wyrd-company/deck/internal/transport"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// setupIndexed replaces the fixed index with the real activity index over the
// harness's embedded JetStream.
func setupIndexed(t *testing.T) *harness {
	t.Helper()
	s := setup(t)
	idx := activity.NewIndex(context.Background(), s.js, s.reg.KV, s.cfg.NATS, nil, quiet())
	t.Cleanup(idx.Close)
	s.h.Activity = idx
	s.h.Index = streamIndex{idx}
	return s
}

func person(thread, id uint64, kind uint32) *pb.Activity {
	return &pb.Activity{Id: id, Time: timestamppb.Now(), Bot: "a", ServerId: 1, ChannelId: 10, ThreadId: thread, Author: &pb.User{Id: 7, Username: "person"}, Kind: &pb.Activity_Message{Message: &pb.Message{Type: kind, Content: "hi"}}}
}

func (s *harness) publishActivity(t *testing.T, a *pb.Activity) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seq, err := s.h.Publisher.Publish(ctx, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

// position waits until the index has caught up with the stream.
func (s *harness) position(t *testing.T) *pb.StreamPosition {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	testutil.Until(t, func() bool {
		p, err := s.h.Activity.Position(ctx)
		return err == nil && s.lastSeq() == p.Sequence
	})
	p, _ := s.h.Activity.Position(ctx)
	return p
}

func (s *harness) lastSeq() uint64 {
	stream, err := s.js.Stream(context.Background(), "DECK")
	if err != nil {
		return 0
	}
	info, err := stream.Info(context.Background())
	if err != nil {
		return 0
	}
	return info.State.LastSeq
}

func at(thread uint64, p *pb.StreamPosition) *pb.Cursor {
	return &pb.Cursor{ThreadId: thread, After: p}
}

func register(t *testing.T, reg *registry.Registry, id uint64, e registry.Entry, key string) {
	t.Helper()
	e.CreatedByDeck = true
	if err := reg.RegisterPost(id, e, key); err != nil {
		t.Fatal(err)
	}
}

// Thread-state precedence: only a TOUCHED thread has a count; a thread
// another key touched is NOT_TOUCHED; a key in the entry without a grant
// through its bot, or whose bot is no longer configured, is NOT_GRANTED.
// Counts still answer from the index when the bot cannot see the thread.
func TestUnreadStatePrecedence(t *testing.T) {
	s := setupIndexed(t)
	register(t, s.reg, 100, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10}, "key")
	register(t, s.reg, 300, registry.Entry{Bot: "b", ServerID: 1, ChannelID: 20}, "other")
	register(t, s.reg, 400, registry.Entry{Bot: "a", ServerID: 2, ChannelID: 30}, "key")
	register(t, s.reg, 500, registry.Entry{Bot: "gone", ServerID: 1, ChannelID: 10}, "key")
	for _, a := range []*pb.Activity{person(100, 1001, 0), person(100, 1002, 19), person(300, 1003, 0), person(400, 1004, 0), person(500, 1005, 0)} {
		s.publishActivity(t, a)
	}
	s.position(t)
	s.fake.mu.Lock()
	s.fake.metadataStatus, s.fake.metadataCode, s.fake.requests = 403, 50001, nil
	s.fake.mu.Unlock()
	r, err := s.client.CountUnread(s.ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{{ThreadId: 100}, {ThreadId: 300}, {ThreadId: 400}, {ThreadId: 500}, {ThreadId: 600}}})
	if err != nil {
		t.Fatal(err)
	}
	want := []pb.ThreadState{pb.ThreadState_THREAD_STATE_TOUCHED, pb.ThreadState_THREAD_STATE_NOT_TOUCHED, pb.ThreadState_THREAD_STATE_NOT_GRANTED, pb.ThreadState_THREAD_STATE_NOT_GRANTED, pb.ThreadState_THREAD_STATE_NOT_TOUCHED}
	for n, tc := range r.Threads {
		if tc.State != want[n] {
			t.Fatalf("thread %d state %v, want %v", tc.ThreadId, tc.State, want[n])
		}
		if n > 0 && (tc.Count != 0 || tc.HorizonPassed || len(tc.Own) != 0) {
			t.Fatalf("untouched thread %d has a count", tc.ThreadId)
		}
	}
	if r.Threads[0].Count != 2 || !r.Threads[0].HorizonPassed {
		t.Fatal("touched count", r.Threads[0])
	}
	if fakeRequestCount(s.fake) != 0 {
		t.Fatal("counts read Discord")
	}
	l, err := s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: []*pb.Cursor{{ThreadId: 100}, {ThreadId: 300}}})
	if err != nil || len(l.Activity) != 2 || l.Activity[0].Id != 1001 || l.Threads[1].State != pb.ThreadState_THREAD_STATE_NOT_TOUCHED {
		t.Fatal("list leaked another key's thread", l, err)
	}
	if _, err = s.client.CountUnread(s.ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{{}}}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("cursor without thread", err)
	}
}

// rotated returns a handler whose configuration renames "key" or, for the
// same name, rotates its public key, sharing the harness's state.
func rotated(t *testing.T, s *harness, name string) *Handler {
	t.Helper()
	cfg := *s.cfg
	cfg.Keys = append([]config.Key{}, s.cfg.Keys...)
	for n := range cfg.Keys {
		if cfg.Keys[n].Name == "key" {
			cfg.Keys[n].Name = name
			if name == "key" {
				pub, _, _ := ed25519.GenerateKey(rand.Reader)
				key, _ := ssh.NewPublicKey(pub)
				cfg.Keys[n].PublicKey = string(ssh.MarshalAuthorizedKey(key))
			}
		}
	}
	h, err := NewHandler(&cfg, Dependencies{Context: s.h.ctx, Bots: s.h.bots, Registry: s.reg, Publisher: s.h.Publisher, Activity: s.h.Activity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Wait)
	return h
}

// R3-01 and the rename and same-nonce traces: `own` lists every counted copy
// of a deck-sent message with its original sender_key and nonce, selected by
// neither the caller's name nor its fingerprint.
func TestOwnAcrossRotationRenameAndNonce(t *testing.T) {
	s := setupIndexed(t)
	post, err := s.client.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "t", Content: content(), Nonce: "start"})
	if err != nil {
		t.Fatal(err)
	}
	cursor := s.position(t)
	a := s.h.fingerprints["key"]
	reply, err := s.client.Reply(s.ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: post.Thread.ThreadId}, Content: content(), Nonce: "n"})
	if err != nil {
		t.Fatal(err)
	}
	// Rotation: K keeps its name and grants with a new public key B.
	rot := rotated(t, s, "key")
	b := rot.fingerprints["key"]
	if a == b {
		t.Fatal("rotation kept the fingerprint")
	}
	ctx := transport.WithKeyName(context.Background(), "key")
	c, err := rot.CountUnread(ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{at(100, cursor)}})
	if err != nil {
		t.Fatal(err)
	}
	if tc := c.Threads[0]; tc.Count != 1 || len(tc.Own) != 1 || tc.Own[0].Id != reply.Message.Id || tc.Own[0].SenderKey != a || tc.Own[0].Nonce != "n" {
		t.Fatalf("rotation count %+v", tc)
	}
	l, err := rot.ListUnread(ctx, &pb.ListUnreadRequest{Cursors: []*pb.Cursor{at(100, cursor)}})
	if err != nil || len(l.Activity) != 1 || l.Activity[0].SenderKey != a || l.Activity[0].Nonce != "n" {
		t.Fatal("rotation list", l, err)
	}
	// Same nonce under the rotated key: two own entries, told apart by key.
	s.fake.mu.Lock()
	s.fake.replyID = "102"
	s.fake.mu.Unlock()
	if _, err = rot.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "n"}); err != nil {
		t.Fatal(err)
	}
	c, _ = rot.CountUnread(ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{at(100, cursor)}})
	if tc := c.Threads[0]; tc.Count != 2 || len(tc.Own) != 2 || tc.Own[0].SenderKey != a || tc.Own[1].SenderKey != b || tc.Own[1].Nonce != "n" {
		t.Fatalf("same-nonce count %+v", tc)
	}
	// Rename: K becomes K2 with public key A unchanged; K2's send touches the
	// thread for K2, and own lists the copies of both names.
	ren := rotated(t, s, "key2")
	ctx2 := transport.WithKeyName(context.Background(), "key2")
	s.fake.mu.Lock()
	s.fake.replyID = "103"
	s.fake.mu.Unlock()
	if _, err = ren.Reply(ctx2, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "n2"}); err != nil {
		t.Fatal(err)
	}
	c, err = ren.CountUnread(ctx2, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{at(100, cursor)}})
	if err != nil {
		t.Fatal(err)
	}
	if tc := c.Threads[0]; tc.Count != 3 || len(tc.Own) != 3 || tc.Own[2].SenderKey != a || tc.Own[2].Nonce != "n2" {
		t.Fatalf("rename count %+v", tc)
	}
}

// `through`: the last listed position when `more` is set, otherwise the
// stream's last position when the list was taken, also with nothing listed.
func TestUnreadThrough(t *testing.T) {
	s := setupIndexed(t)
	register(t, s.reg, 100, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10}, "key")
	register(t, s.reg, 300, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10}, "key")
	start := s.position(t)
	seqs := []uint64{}
	for n, thread := range []uint64{100, 300, 100, 999} {
		seqs = append(seqs, s.publishActivity(t, person(thread, uint64(2000+n), 0)))
	}
	last := s.position(t)
	cursors := []*pb.Cursor{at(100, start), at(300, start)}
	l, err := s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: cursors, Limit: 2})
	if err != nil || !l.More || len(l.Activity) != 2 || l.Through.Sequence != seqs[1] || l.Activity[0].Sequence != seqs[0] || l.Activity[1].ThreadId != 300 {
		t.Fatal("limited list", l, err)
	}
	l, err = s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: cursors})
	if err != nil || l.More || len(l.Activity) != 3 || l.Through.Sequence != last.Sequence || l.Through.Epoch != last.Epoch {
		t.Fatal("full list", l, err)
	}
	l, err = s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: []*pb.Cursor{at(100, last)}})
	if err != nil || len(l.Activity) != 0 || l.Through.Sequence != last.Sequence || l.Threads[0].HorizonPassed {
		t.Fatal("empty list", l, err)
	}
}

func recv(t *testing.T, stream pb.DeckService_ListenClient) *pb.ListenResponse {
	t.Helper()
	got := make(chan *pb.ListenResponse, 1)
	failed := make(chan error, 1)
	go func() {
		r, err := stream.Recv()
		if err != nil {
			failed <- err
			return
		}
		got <- r
	}()
	select {
	case r := <-got:
		return r
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("listen sent nothing")
	}
	return nil
}

func (s *harness) recreate(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if err := s.js.DeleteStream(ctx, "DECK"); err != nil {
		t.Fatal(err)
	}
	if _, err := activity.EnsureStream(ctx, s.js, s.cfg.NATS); err != nil {
		t.Fatal(err)
	}
}

// Listen answers at once, streams each new counted activity with `through`
// at its position, and after the stream is recreated while it is open sends
// the new generation's activity: R3-02, nothing of the new stream is missed.
func TestListenAndRecreatedStream(t *testing.T) {
	s := setupIndexed(t)
	register(t, s.reg, 100, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10}, "key")
	s.publishActivity(t, person(100, 3000, 0))
	start := s.position(t)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	stream, err := s.client.Listen(ctx, &pb.ListenRequest{Cursors: []*pb.Cursor{at(100, start), {ThreadId: 600}}})
	if err != nil {
		t.Fatal(err)
	}
	first := recv(t, stream)
	if len(first.Threads) != 2 || first.Threads[0].State != pb.ThreadState_THREAD_STATE_TOUCHED || first.Threads[1].State != pb.ThreadState_THREAD_STATE_NOT_TOUCHED || len(first.Activity) != 0 || first.Through.Sequence != start.Sequence {
		t.Fatal("first response", first)
	}
	s.publishActivity(t, person(100, 3001, 18)) // system message: never sent
	s.publishActivity(t, person(999, 3002, 0))  // another thread: never sent
	seq := s.publishActivity(t, person(100, 3003, 0))
	next := recv(t, stream)
	if len(next.Activity) != 1 || next.Activity[0].Id != 3003 || next.Through.Sequence != seq || next.Threads != nil {
		t.Fatal("live response", next)
	}
	// Recreate the stream and publish into it before the index notices.
	s.recreate(t)
	s.publishActivity(t, person(100, 4001, 0))
	s.publishActivity(t, person(100, 4002, 0))
	a, b := recv(t, stream), recv(t, stream)
	if a.Activity[0].Id != 4001 || b.Activity[0].Id != 4002 || a.Through.Epoch == start.Epoch || b.Through.Sequence != 2 {
		t.Fatal("new generation missed", a, b)
	}
	// A count from a cursor of the old generation sees everything retained.
	old := at(100, &pb.StreamPosition{Epoch: start.Epoch, Sequence: seq})
	c, err := s.client.CountUnread(s.ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{old}})
	if err != nil || c.Threads[0].Count != 2 || !c.Threads[0].HorizonPassed {
		t.Fatal("count after recreation", c, err)
	}
	l, err := s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: []*pb.Cursor{old}})
	if err != nil || len(l.Activity) != 2 || l.Through.Epoch != b.Through.Epoch {
		t.Fatal("list after recreation", l, err)
	}
}

func attached(thread, id uint64, link string) *pb.Activity {
	a := person(thread, id, 0)
	a.GetMessage().Attachments = []*pb.Attachment{{Id: 1, Filename: "f.txt", Url: link}}
	return a
}

// Listed attachment URLs are refreshed from Discord, including a text channel
// post's starter read from the parent channel; a failed read keeps the
// published URL.
func TestAttachmentURLsRefreshed(t *testing.T) {
	s := setupIndexed(t)
	register(t, s.reg, 100, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10}, "key")
	register(t, s.reg, 200, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 20}, "key")
	start := s.position(t)
	s.publishActivity(t, attached(100, 700, "https://cdn/old-700"))
	starter := attached(200, 200, "https://cdn/old-200")
	starter.ChannelId = 20
	s.publishActivity(t, starter)
	s.position(t)
	fail := false
	s.fake.mu.Lock()
	s.fake.history = func(path string, q url.Values) (int, any) {
		if fail {
			return 500, map[string]any{"message": "down"}
		}
		message := func(id, channel string) map[string]any {
			m := messageJSON(id, channel, map[string]any{"content": "x"})
			m["attachments"] = []any{map[string]any{"id": "1", "filename": "f.txt", "size": 1, "url": "https://cdn/new-" + id, "proxy_url": "https://cdn/new-" + id}}
			return m
		}
		switch {
		case path == "/channels/100/messages" && q.Get("after") == "699":
			return 200, []any{message("700", "100")}
		case path == "/channels/20/messages" && q.Get("after") == "199":
			return 200, []any{message("200", "20")}
		}
		return 200, []any{}
	}
	s.fake.mu.Unlock()
	l, err := s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: []*pb.Cursor{at(100, start), at(200, start)}})
	if err != nil || len(l.Activity) != 2 {
		t.Fatal(l, err)
	}
	if l.Activity[0].GetMessage().Attachments[0].Url != "https://cdn/new-700" || l.Activity[1].GetMessage().Attachments[0].Url != "https://cdn/new-200" {
		t.Fatal("attachment URLs not refreshed", l.Activity)
	}
	s.fake.mu.Lock()
	fail = true
	s.fake.mu.Unlock()
	l, err = s.client.ListUnread(s.ctx, &pb.ListUnreadRequest{Cursors: []*pb.Cursor{at(100, start)}})
	if err != nil || l.Activity[0].GetMessage().Attachments[0].Url != "https://cdn/old-700" {
		t.Fatal("failed refresh lost the published URL", l, err)
	}
}

// Calls that read the index wait for its replay up to their deadline and
// then answer UNAVAILABLE.
func TestUnreadWaitsForIndex(t *testing.T) {
	s := setup(t)
	cfg := s.cfg.NATS
	cfg.Stream.Name, cfg.Stream.Manage = "ABSENT", false
	kv, err := s.js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{Bucket: "absent"})
	if err != nil {
		t.Fatal(err)
	}
	idx := activity.NewIndex(context.Background(), s.js, kv, cfg, nil, quiet())
	t.Cleanup(idx.Close)
	s.h.Activity = idx
	register(t, s.reg, 100, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10}, "key")
	ctx, cancel := context.WithTimeout(s.ctx, 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = s.client.CountUnread(ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{{ThreadId: 100}}})
	if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded || time.Since(started) > 2*time.Second {
		t.Fatal("count during replay", err, time.Since(started))
	}
	if r, err := s.client.CountUnread(s.ctx, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{{ThreadId: 600}}}); err != nil || r.Threads[0].State != pb.ThreadState_THREAD_STATE_NOT_TOUCHED {
		t.Fatal("untouched threads need no index", r, err)
	}
}
