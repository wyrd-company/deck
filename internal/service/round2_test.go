package service

import (
	"context"
	"errors"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/url"
	"sync"
	"testing"
	"time"
)

func invokeTarget(h *Handler, ctx context.Context, op, bot string, id uint64) error {
	var err error
	switch op {
	case "Post":
		_, err = h.Post(ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: bot, ChannelId: id}, Title: "test", Content: content(), Nonce: "target"})
	case "Reply":
		_, err = h.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: bot, ThreadId: id}, Content: content(), Nonce: "target"})
	case "GetThread":
		_, err = h.GetThread(ctx, &pb.GetThreadRequest{Thread: &pb.ThreadRef{Bot: bot, ThreadId: id}})
	case "ListHistory":
		_, err = h.ListHistory(ctx, &pb.ListHistoryRequest{Thread: &pb.ThreadRef{Bot: bot, ThreadId: id}})
	case "ListChannels":
		_, err = h.ListChannels(ctx, &pb.ListChannelsRequest{})
	}
	return err
}
func TestMetadataReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		http int
		code int
		id   uint64
		want codes.Code
	}{{"forbidden", 403, 0, 101, codes.PermissionDenied}, {"missing", 404, 0, 101, codes.PermissionDenied}, {"outage", 503, 0, 101, codes.Unavailable}, {"non-guild", 0, 0, 50, codes.PermissionDenied}, {"permission-code", 403, 50013, 101, codes.PermissionDenied}, {"notfound-code", 404, 10008, 101, codes.PermissionDenied}} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"Post", "Reply", "GetThread", "ListHistory", "ListChannels"} {
				if (tc.http == 0 || tc.code != 0) && op == "ListChannels" {
					continue
				}
				t.Run(op, func(t *testing.T) {
					s := setup(t)
					ungranted := invokeTarget(s.h, s.ctx, "GetThread", "a", 30)
					s.fake.metadataStatus = tc.http
					s.fake.metadataCode = tc.code
					err := invokeTarget(s.h, s.ctx, op, "a", tc.id)
					if status.Code(err) != tc.want {
						t.Fatalf("got %v want %v", err, tc.want)
					}
					if tc.want == codes.PermissionDenied && status.Convert(err).Message() != status.Convert(ungranted).Message() {
						t.Fatal("metadata refusal disclosed target visibility", err, ungranted)
					}
				})
			}
		})
	}
}
func TestEmptyBotTouchedAndSingleBot(t *testing.T) {
	s := setup(t)
	if err := s.reg.RegisterPost(101, registry.Entry{Bot: "b", ServerID: 1, ChannelID: 10}, "other"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{101, 102} {
		_, err := s.h.GetThread(s.ctx, &pb.GetThreadRequest{Thread: &pb.ThreadRef{ThreadId: id}})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("target %d: %v", id, err)
		}
	}
	for _, id := range []uint64{101, 102} {
		got, err := s.h.GetThread(transport.WithKeyName(s.ctx, "other"), &pb.GetThreadRequest{Thread: &pb.ThreadRef{ThreadId: id}})
		if err != nil || got.Thread.Bot != "b" {
			t.Fatal("single-bot empty reference failed", got, err)
		}
	}
}

func TestFlightsSeparateKeyFingerprints(t *testing.T) {
	s := setup(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	firstEntered, secondEntered := make(chan struct{}, 1), make(chan struct{}, 1)
	s.fake.block = release
	s.fake.entered = firstEntered
	s.fakes["b"].block = release
	s.fakes["b"].entered = secondEntered
	s.fakes["b"].replyID = "202"
	type result struct {
		r   *pb.ReplyResponse
		err error
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go func() {
		r, err := s.h.Reply(s.ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 101}, Content: content(), Nonce: "shared"})
		first <- result{r, err}
	}()
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first send did not enter Discord")
	}
	go func() {
		r, err := s.h.Reply(transport.WithKeyName(s.ctx, "other"), &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "b", ThreadId: 201}, Content: content(), Nonce: "shared"})
		second <- result{r, err}
	}()
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Error("second key joined first key's flight")
	}
	unblock()
	a, b := <-first, <-second
	if a.err != nil || b.err != nil {
		t.Fatal(a.err, b.err)
	}
	if a.r.Message.Id != 101 || b.r.Message.Id != 202 || a.r.Thread.ThreadId != 101 || a.r.Message.SentBy != "key" || b.r.Thread.ThreadId != 201 || b.r.Message.SentBy != "other" || a.r.Message.SenderKey == b.r.Message.SenderKey {
		t.Fatal("keys shared a flight result", a.r, b.r)
	}
	if len(s.fake.posts()) != 1 || len(s.fakes["b"].posts()) != 1 {
		t.Fatal("one key's message was not sent")
	}
}
func TestFlightRejectsDifferentOperation(t *testing.T) {
	s := setup(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	entered := make(chan struct{}, 1)
	s.fake.block = release
	s.fake.entered = entered
	done := make(chan error, 1)
	go func() {
		_, err := s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "test", Content: content(), Nonce: "operation"})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("post did not enter Discord")
	}
	ctx, cancel := context.WithTimeout(s.ctx, 200*time.Millisecond)
	defer cancel()
	_, err := s.h.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 101}, Content: content(), Nonce: "operation"})
	if status.Code(err) != codes.InvalidArgument {
		t.Error("different operation joined flight", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(s.fake.posts()) != 1 {
		t.Fatal("different operation sent")
	}
}
func TestFailedFlightCanRetry(t *testing.T) {
	s := setup(t)
	s.fake.reject = 50035
	request := &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 101}, Content: content(), Nonce: "retry"}
	if _, err := s.h.Reply(s.ctx, request); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	s.fake.mu.Lock()
	s.fake.reject = 0
	s.fake.mu.Unlock()
	if _, err := s.h.Reply(s.ctx, request); err != nil {
		t.Fatal("failed flight retained stale failure", err)
	}
	if len(s.fake.posts()) != 2 {
		t.Fatal("known failure was not retried")
	}
}
func TestHistoryMoreAndBeforeCursor(t *testing.T) {
	for _, cursor := range []string{"before", "after"} {
		t.Run(cursor, func(t *testing.T) {
			s := setup(t)
			s.fake.historyMore = true
			request := &pb.ListHistoryRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Limit: 2}
			boundary := "103"
			if cursor == "before" {
				request.BeforeId = 110
				boundary = "102"
			} else {
				request.AfterId = 100
			}
			got, err := s.client.ListHistory(s.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if !got.More || len(got.Activity) != 2 || got.Activity[0].Id != 102 || got.Activity[1].Id != 103 {
				t.Fatal("full page omitted more or order", got)
			}
			s.fake.mu.Lock()
			defer s.fake.mu.Unlock()
			foundPage, foundProbe := false, false
			for _, req := range s.fake.requests {
				u, err := url.Parse(req.path)
				if err != nil {
					t.Fatal(err)
				}
				if u.Path != "/channels/100/messages" {
					continue
				}
				q := u.Query()
				if q.Get("limit") == "1" {
					foundProbe = true
					if q.Get(cursor) != boundary {
						t.Fatal("probe cursor", q)
					}
				} else {
					foundPage = true
					want := "100"
					if cursor == "before" {
						want = "110"
					}
					if q.Get(cursor) != want {
						t.Fatal("page cursor", q)
					}
				}
			}
			if !foundPage || !foundProbe {
				t.Fatal("missing page/probe request")
			}
		})
	}
}
func TestStarterCleanupAfterThreadFailure(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		name := "known"
		if unknown {
			name = "unknown"
		}
		t.Run(name, func(t *testing.T) {
			s := setup(t)
			if unknown {
				s.fake.startLose = true
			} else {
				s.fake.startReject = 50035
			}
			_, err := s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 20}, Title: "test", Content: content(), Nonce: "starter"})
			want := codes.InvalidArgument
			if unknown {
				want = codes.Unavailable
			}
			if status.Code(err) != want {
				t.Fatal(err)
			}
			if len(s.fake.posts()) != 2 {
				t.Fatal("starter or StartThread request missing")
			}
			s.fake.mu.Lock()
			defer s.fake.mu.Unlock()
			deletes := 0
			for _, req := range s.fake.requests {
				if req.method == "DELETE" {
					deletes++
					if req.path != "/channels/20/messages/200" {
						t.Fatal("wrong starter deleted", req.path)
					}
				}
			}
			expected := 1
			if unknown {
				expected = 0
			}
			if deletes != expected {
				t.Fatalf("deletes=%d want %d", deletes, expected)
			}
			if len(s.fake.requests) == 0 {
				t.Fatal("no Discord requests")
			}
		})
	}
}

type positionFailureIndex struct{ fixedIndex }

func (positionFailureIndex) Position(context.Context) (*pb.StreamPosition, error) {
	return nil, errors.New("index down")
}
func TestReplyPositionFailureCleansTouch(t *testing.T) {
	s := setup(t)
	s.h.Index = positionFailureIndex{}
	_, err := s.h.Reply(s.ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 101}, Content: content(), Nonce: "position"})
	if status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if len(s.fake.posts()) != 0 {
		t.Fatal("position failure reached Discord send")
	}
	if _, ok := s.reg.Lookup(101); ok {
		t.Fatal("position failure retained first pending touch")
	}
}
