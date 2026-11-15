package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// retainedIndex models task 7's replay seam by reading the real retained events.
// It owns no send-result cache, so a fresh instance proves restart reconstruction.
type retainedIndex struct {
	js      jetstream.JetStream
	gate    <-chan struct{}
	entered chan struct{}
}

func (i retainedIndex) Position(context.Context) (*pb.StreamPosition, error) {
	return &pb.StreamPosition{Epoch: 42, Sequence: 7}, nil
}
func (i retainedIndex) Completed(ctx context.Context, fingerprint, nonce string) (*CompletedSend, error) {
	stream, err := i.js.Stream(ctx, "DECK")
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, err
	}
	var out *CompletedSend
	for seq := info.State.FirstSeq; seq > 0 && seq <= info.State.LastSeq; seq++ {
		m, err := stream.GetMsg(ctx, seq)
		if err != nil {
			return nil, err
		}
		if m.Header.Get("Deck-Sender-Key") != fingerprint || m.Header.Get("Deck-Nonce") != nonce {
			continue
		}
		a := &pb.Activity{}
		if err = protojson.Unmarshal(m.Data, a); err != nil {
			return nil, err
		}
		a.Sequence = seq
		if out == nil {
			start := strings.Split(m.Header.Get("Deck-Send-Start"), ":")
			epoch, _ := strconv.ParseUint(start[0], 10, 64)
			sequence, _ := strconv.ParseUint(start[1], 10, 64)
			out = &CompletedSend{Thread: &pb.Thread{Bot: a.Bot, ServerId: a.ServerId, ChannelId: a.ChannelId, ThreadId: a.ThreadId}, Position: &pb.StreamPosition{Epoch: epoch, Sequence: sequence}, PollError: m.Header.Get("Deck-Poll-Error")}
		}
		out.Messages = append(out.Messages, a)
	}
	return out, nil
}
func (i retainedIndex) Confirm(ctx context.Context, send *CompletedSend) error {
	if i.entered != nil {
		select {
		case i.entered <- struct{}{}:
		default:
		}
	}
	if i.gate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-i.gate:
		}
	}
	first := send.Messages[0]
	found, err := i.Completed(ctx, first.SenderKey, first.Nonce)
	if err != nil {
		return err
	}
	if found == nil || len(found.Messages) != len(send.Messages) || found.PollError != send.PollError {
		return fmt.Errorf("published result is not discoverable")
	}
	return nil
}
func TestRepeatWaitsForIndexAndRestoresPollFailure(t *testing.T) {
	s := setup(t)
	s.fake.pollFail = true
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	s.h.Index = retainedIndex{js: s.js, gate: gate, entered: entered}
	req := &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "original name", Content: &pb.Content{Text: "body", Poll: &pb.PollCreate{Question: "?", Answers: []string{"yes"}}}, Nonce: "replay"}
	results := make(chan *pb.PostResponse, 2)
	errs := make(chan error, 2)
	go func() { r, e := s.h.Post(s.ctx, req); results <- r; errs <- e }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("send skipped the index visibility barrier")
	}
	go func() { r, e := s.h.Post(s.ctx, req); results <- r; errs <- e }()
	time.Sleep(40 * time.Millisecond)
	if len(s.fake.posts()) != 2 {
		t.Fatal("repeat sent while the index lagged")
	}
	close(gate)
	a, b := <-results, <-results
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if !proto.Equal(a, b) || a.PollError == "" {
		t.Fatal("in-flight partial result changed")
	}
	restarted, err := NewHandler(s.cfg, Dependencies{Context: s.h.ctx, Bots: s.h.bots, Registry: s.reg, Publisher: s.h.Publisher, Index: retainedIndex{js: s.js}})
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.Post(s.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if again.PollError != a.PollError || !proto.Equal(again.Position, a.Position) || again.Message.Id != a.Message.Id || len(s.fake.posts()) != 2 {
		t.Fatal("retained partial result did not survive a fresh handler/index")
	}
	if again.Thread.Name != "thread" {
		t.Fatal("completed result did not refresh thread metadata")
	}
	cfg := *s.cfg
	cfg.Keys = append([]config.Key(nil), s.cfg.Keys...)
	key := cfg.Keys[0]
	key.Grants = []config.Grant{{Bot: "a", Server: "1", Channels: []string{"20"}}}
	cfg.Keys[0] = key
	restricted, err := NewHandler(&cfg, Dependencies{Context: s.h.ctx, Bots: s.h.bots, Registry: s.reg, Publisher: s.h.Publisher, Index: retainedIndex{js: s.js}})
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(req).(*pb.PostRequest)
	changed.Channel.ChannelId = 20
	_, err = restricted.Post(s.ctx, changed)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("completed result bypassed its thread's current grants", err)
	}
}

func TestCompletedReplyReplay(t *testing.T) {
	s := setup(t)
	s.h.Index = retainedIndex{js: s.js}
	request := &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "replyreplay"}
	first, err := s.h.Reply(s.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	s.fake.mu.Lock()
	s.fake.threadName = "fresh thread name"
	s.fake.archived = true
	s.fake.mu.Unlock()
	restarted, err := NewHandler(s.cfg, Dependencies{Context: s.h.ctx, Bots: s.h.bots, Registry: s.reg, Publisher: s.h.Publisher, Index: retainedIndex{js: s.js}})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []*Handler{s.h, restarted} {
		repeated, err := h.Reply(s.ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(repeated.Message, first.Message) || !proto.Equal(repeated.Position, first.Position) || repeated.Thread.ThreadId != 100 || repeated.Thread.Name != "fresh thread name" || !repeated.Thread.Archived || !repeated.Thread.Touched || repeated.Thread.CreatedByDeck {
			t.Fatal("reply replay lost stored result or refreshed metadata", repeated)
		}
	}
	if len(s.fake.posts()) != 1 {
		t.Fatal("completed reply sent again")
	}
	cfg := *s.cfg
	cfg.Keys = append([]config.Key(nil), cfg.Keys...)
	cfg.Keys[0].Grants = []config.Grant{{Bot: "a", Server: "1", Channels: []string{"20"}}}
	restricted, err := NewHandler(&cfg, Dependencies{Context: s.h.ctx, Bots: s.h.bots, Registry: s.reg, Publisher: s.h.Publisher, Index: retainedIndex{js: s.js}})
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(request).(*pb.ReplyRequest)
	changed.Thread.ThreadId = 200
	_, err = restricted.Reply(s.ctx, changed)
	if status.Code(err) != codes.PermissionDenied || len(s.fake.posts()) != 1 {
		t.Fatal("completed reply bypassed actual thread grants", err)
	}
}
