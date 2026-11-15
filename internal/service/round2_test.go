package service

import (
	"context"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/transport"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
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
		id   uint64
		want codes.Code
	}{{"forbidden", 403, 101, codes.PermissionDenied}, {"missing", 404, 101, codes.PermissionDenied}, {"outage", 503, 101, codes.Unavailable}, {"non-guild", 0, 50, codes.PermissionDenied}} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"Post", "Reply", "GetThread", "ListHistory", "ListChannels"} {
				if tc.http == 0 && op == "ListChannels" {
					continue
				}
				t.Run(op, func(t *testing.T) {
					s := setup(t)
					ungranted := invokeTarget(s.h, s.ctx, "GetThread", "a", 30)
					s.fake.metadataStatus = tc.http
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
