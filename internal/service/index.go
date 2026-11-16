package service

import (
	"context"
	"errors"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/activity"
)

// streamIndex serves the send path's SendIndex from the activity index.
type streamIndex struct{ index *activity.Index }

func (s streamIndex) Position(ctx context.Context) (*pb.StreamPosition, error) {
	return s.index.Position(ctx)
}
func (s streamIndex) Completed(ctx context.Context, fingerprint, nonce string) (*CompletedSend, error) {
	send, err := s.index.Completed(ctx, fingerprint, nonce)
	if err != nil || send == nil {
		return nil, err
	}
	first := send.Messages[0]
	return &CompletedSend{Thread: &pb.Thread{Bot: first.Bot, ServerId: first.ServerId, ChannelId: first.ChannelId, ThreadId: first.ThreadId}, Messages: send.Messages, Position: send.Start, PollError: send.PollError}, nil
}
func (s streamIndex) Confirm(ctx context.Context, send *CompletedSend) error {
	if send == nil || len(send.Messages) == 0 {
		return errors.New("confirm: send has no messages")
	}
	first := send.Messages[0]
	return s.index.Confirm(ctx, first.SenderKey, first.Nonce, send.Messages)
}
