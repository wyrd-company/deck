package service

import (
	"context"
	"errors"
	"time"

	d "github.com/disgoorg/disgo/discord"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/activity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

const defaultUnreadLimit = 500

func indexUnavailable() error {
	return status.Error(codes.Unavailable, "activity index unavailable; retry shortly")
}

// cursorStates applies thread-state precedence for the calling key: only a
// TOUCHED thread is read from the index.
func (h *Handler) cursorStates(ctx context.Context, cursors []*pb.Cursor) ([]pb.ThreadState, []*pb.Cursor, error) {
	states := make([]pb.ThreadState, len(cursors))
	touched := []*pb.Cursor{}
	for n, c := range cursors {
		if c.GetThreadId() == 0 {
			return nil, nil, status.Error(codes.InvalidArgument, "cursor thread_id is required")
		}
		states[n] = h.State(ctx, c.ThreadId)
		if states[n] == pb.ThreadState_THREAD_STATE_TOUCHED {
			touched = append(touched, c)
		}
	}
	return states, touched, nil
}

// unread takes a consistent view and its bodies, retrying when the index
// changed generation or ingestion under the read.
func (h *Handler) unread(ctx context.Context, cursors []*pb.Cursor, limit int, bodies bool) (*activity.Unread, []*pb.Activity, error) {
	if h.Activity == nil {
		return nil, nil, indexUnavailable()
	}
	for {
		u, err := h.Activity.Unread(ctx, cursors, limit)
		if err == nil && bodies {
			var listed []*pb.Activity
			if listed, err = h.Activity.List(ctx, u); err == nil {
				return u, listed, nil
			}
		} else if err == nil {
			return u, nil, nil
		}
		if !activity.IsRetry(err) {
			if errors.Is(err, activity.ErrUnavailable) || ctx.Err() != nil {
				return nil, nil, indexUnavailable()
			}
			return nil, nil, status.Error(codes.Internal, "cannot read activity")
		}
		select {
		case <-ctx.Done():
			return nil, nil, indexUnavailable()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func threadCounts(cursors []*pb.Cursor, states []pb.ThreadState, u *activity.Unread) []*pb.ThreadCount {
	out := make([]*pb.ThreadCount, len(cursors))
	j := 0
	for n, c := range cursors {
		tc := &pb.ThreadCount{ThreadId: c.ThreadId, State: states[n]}
		if states[n] == pb.ThreadState_THREAD_STATE_TOUCHED && u != nil {
			count := u.Counts[j]
			j++
			tc.Count, tc.Own, tc.HorizonPassed = count.Count, count.Own, count.Horizon
		}
		out[n] = tc
	}
	return out
}

func (h *Handler) CountUnread(ctx context.Context, r *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
	if _, err := h.calling(ctx); err != nil {
		return nil, err
	}
	states, touched, err := h.cursorStates(ctx, r.Cursors)
	if err != nil {
		return nil, err
	}
	var u *activity.Unread
	if len(touched) > 0 {
		if u, _, err = h.unread(ctx, touched, 0, false); err != nil {
			return nil, err
		}
	}
	return &pb.CountUnreadResponse{Threads: threadCounts(r.Cursors, states, u)}, nil
}

func (h *Handler) ListUnread(ctx context.Context, r *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
	if _, err := h.calling(ctx); err != nil {
		return nil, err
	}
	states, touched, err := h.cursorStates(ctx, r.Cursors)
	if err != nil {
		return nil, err
	}
	out := &pb.ListUnreadResponse{}
	var u *activity.Unread
	if len(touched) > 0 {
		limit := int(r.Limit)
		if limit == 0 {
			limit = defaultUnreadLimit
		}
		if u, out.Activity, err = h.unread(ctx, touched, limit, true); err != nil {
			return nil, err
		}
		out.More, out.Through = u.More, u.Through
		h.refreshAttachments(ctx, out.Activity)
	}
	out.Threads = threadCounts(r.Cursors, states, u)
	return out, nil
}

// Listen answers at once like ListUnread, then sends each further activity
// that counts in the key's touched threads, with `through` at its position.
// A generation change sends the new generation's retained activity again.
func (h *Handler) Listen(r *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
	ctx := stream.Context()
	if _, err := h.calling(ctx); err != nil {
		return err
	}
	states, touched, err := h.cursorStates(ctx, r.Cursors)
	if err != nil {
		return err
	}
	first := &pb.ListenResponse{}
	var u *activity.Unread
	if len(touched) > 0 {
		if u, first.Activity, err = h.unread(ctx, touched, defaultUnreadLimit, true); err != nil {
			return err
		}
		first.Through = u.Through
		h.refreshAttachments(ctx, first.Activity)
	}
	first.Threads = threadCounts(r.Cursors, states, u)
	if err = stream.Send(first); err != nil {
		return err
	}
	if len(touched) == 0 {
		return nil
	}
	positions := map[uint64]*pb.StreamPosition{}
	for _, c := range touched {
		positions[c.ThreadId] = later(c.After, u.Through)
	}
	for {
		changed := h.Activity.Changed()
		cursors := make([]*pb.Cursor, 0, len(positions))
		for thread, after := range positions {
			if h.State(ctx, thread) == pb.ThreadState_THREAD_STATE_TOUCHED {
				cursors = append(cursors, &pb.Cursor{ThreadId: thread, After: after})
			}
		}
		if len(cursors) > 0 {
			next, listed, err := h.unread(ctx, cursors, 0, true)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			// Later responses carry current attachment URLs too: a retained
			// backlog or a replayed generation can hold expired ones.
			h.refreshAttachments(ctx, listed)
			for _, a := range listed {
				if err = stream.Send(&pb.ListenResponse{Activity: []*pb.Activity{a}, Through: &pb.StreamPosition{Epoch: next.Through.Epoch, Sequence: a.Sequence}}); err != nil {
					return err
				}
			}
			for thread := range positions {
				// A cursor ahead of the index in the same epoch stays where
				// it is: activity is what lies strictly after it.
				positions[thread] = later(positions[thread], next.Through)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-h.ctx.Done():
			return status.Error(codes.Unavailable, "service stopping")
		case <-changed:
		}
	}
}

// later keeps a cursor that is ahead of `through` in the same epoch; a
// position of another epoch is replaced.
func later(cursor, through *pb.StreamPosition) *pb.StreamPosition {
	if cursor != nil && through != nil && cursor.Epoch == through.Epoch && cursor.Sequence > through.Sequence {
		return cursor
	}
	return through
}

// refreshTimeout bounds the Discord reads that refresh attachment URLs.
const refreshTimeout = 5 * time.Second

// refreshAttachments replaces the published attachment URLs, which Discord's
// CDN expires, with current ones read through each thread's bot. A message
// it cannot read within the deadline keeps its published URLs.
func (h *Handler) refreshAttachments(ctx context.Context, listed []*pb.Activity) {
	byThread := map[uint64][]*pb.Activity{}
	for _, a := range listed {
		if len(a.GetMessage().GetAttachments()) > 0 {
			byThread[a.ThreadId] = append(byThread[a.ThreadId], a)
		}
	}
	if len(byThread) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > time.Second {
		var shorter context.CancelFunc
		ctx, shorter = context.WithDeadline(ctx, deadline.Add(-500*time.Millisecond))
		defer shorter()
	}
	for thread, messages := range byThread {
		e, ok := h.Registry.Lookup(thread)
		client := h.bots[e.Bot]
		if !ok || client == nil {
			continue
		}
		want := map[uint64]*pb.Activity{}
		oldest := messages[0].Id
		for _, m := range messages {
			want[m.Id] = m
			oldest = min(oldest, m.Id)
		}
		apply := func(found []d.Message) {
			for _, m := range found {
				if a := want[uint64(m.ID)]; a != nil {
					current := map[uint64]string{}
					for _, f := range m.Attachments {
						current[uint64(f.ID)] = f.URL
					}
					for _, f := range a.GetMessage().Attachments {
						if url := current[f.Id]; url != "" {
							f.Url = url
						}
					}
					delete(want, uint64(m.ID))
				}
			}
		}
		after := oldest - 1
		for len(want) > 0 && ctx.Err() == nil {
			page, err := client.History(ctx, thread, after, 0, 100)
			if err != nil || len(page) == 0 {
				break
			}
			apply(page)
			for _, m := range page {
				after = max(after, uint64(m.ID))
			}
			if len(page) < 100 {
				break
			}
		}
		// A text channel post's starter lives in the parent channel under the
		// thread's snowflake.
		if starter := want[thread]; starter != nil && ctx.Err() == nil {
			if page, err := client.History(ctx, e.ChannelID, thread-1, 0, 1); err == nil {
				apply(page)
			}
		}
	}
}
