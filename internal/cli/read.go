package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/client"
	"github.com/wyrd-company/deck/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (i *invocation) selected() ([]store.Thread, error) {
	all, err := i.client.Store.Threads(i.client.Session)
	if err != nil {
		return nil, err
	}
	held := map[uint64]store.Thread{}
	for _, t := range all {
		held[t.Info.ThreadId] = t
	}
	chosen := all
	if len(i.flags.args) > 0 {
		chosen = []store.Thread{}
		seen := map[uint64]bool{}
		for _, value := range i.flags.args {
			bot, id, err := client.Reference(value)
			if err != nil {
				return nil, invalid("%v", err)
			}
			t, ok := held[id]
			if !ok {
				return nil, invalid("thread %d is not held by this session; use deck history %d to read it", id, id)
			}
			if bot != "" && bot != t.Info.Bot {
				return nil, invalid("thread %d belongs to bot %s", id, t.Info.Bot)
			}
			if err = i.check(t.Info.Bot, t.Info.ChannelId); err != nil {
				return nil, err
			}
			if !seen[id] {
				chosen = append(chosen, t)
				seen[id] = true
			}
		}
	}
	result := []store.Thread{}
	for _, t := range chosen {
		if client.Allowed(i.client.Config, t.Info.Bot, t.Info.ChannelId) {
			result = append(result, t)
		}
	}
	if len(all) == 0 {
		return nil, invalid("this session holds no threads; post or reply first, or use deck history THREAD")
	}
	return result, nil
}
func (i *invocation) recordStates(states []*pb.ThreadCount) {
	for _, s := range states {
		i.states[s.ThreadId] = s.State
		if s.State != pb.ThreadState_THREAD_STATE_TOUCHED {
			fmt.Fprintf(i.app.Stderr, "deck: thread %d: %s; leaving it out\n", s.ThreadId, s.State)
		}
	}
}
func (i *invocation) finishRead(ts []store.Thread, activity []*pb.Activity, through *pb.StreamPosition, more bool) error {
	visible, err := i.client.Visible(activity)
	if err != nil {
		return err
	}
	metadata := map[uint64]*pb.Thread{}
	ids := []uint64{}
	allowed := map[uint64]bool{}
	for _, t := range ts {
		metadata[t.Info.ThreadId] = t.Info
		if i.states[t.Info.ThreadId] == pb.ThreadState_THREAD_STATE_TOUCHED {
			ids = append(ids, t.Info.ThreadId)
			allowed[t.Info.ThreadId] = true
		}
	}
	kept := []*pb.Activity{}
	for _, a := range visible {
		if allowed[a.ThreadId] {
			kept = append(kept, a)
		}
	}
	i.result["activity"] = apiActivities(kept)
	i.result["more"] = more
	i.renderActivities(kept, metadata)
	if !i.flags.has("keep-unread") && len(ids) > 0 {
		if through == nil {
			return errors.New("service returned no through position")
		}
		i.readIDs, i.readThrough = ids, through
	}
	return nil
}
func (i *invocation) read(ctx context.Context, listen bool) error {
	limit, err := i.flags.limit(500, 1000000)
	if err != nil {
		return err
	}
	timeout, err := i.flags.duration("timeout", 0)
	if err != nil {
		return err
	}
	ts, err := i.selected()
	if err != nil {
		return err
	}
	if len(ts) == 0 {
		i.result["activity"] = []any{}
		i.result["more"] = false
		return nil
	}
	dialCtx, cancel := i.callContext(ctx)
	defer cancel()
	if err = i.connect(dialCtx); err != nil {
		return err
	}
	if !listen {
		call, done := i.callContext(ctx)
		defer done()
		r, err := i.client.API.ListUnread(call, &pb.ListUnreadRequest{Cursors: client.Cursors(ts), Limit: limit})
		if err != nil {
			return err
		}
		i.recordStates(r.Threads)
		return i.finishRead(ts, r.Activity, r.Through, r.More)
	}
	listenCtx := ctx
	done := func() {}
	if timeout > 0 {
		listenCtx, done = context.WithTimeout(ctx, timeout)
	} else {
		listenCtx, done = context.WithCancel(ctx)
	}
	defer done()
	fmt.Fprintln(i.app.Stderr, "deck: waiting for unread activity (Ctrl-C stops)")
	stream, err := i.client.API.Listen(listenCtx, &pb.ListenRequest{Cursors: client.Cursors(ts)})
	if err != nil {
		return listenError(ctx, listenCtx, timeout > 0, err)
	}
	first := true
	for {
		response, err := stream.Recv()
		if err != nil {
			return listenError(ctx, listenCtx, timeout > 0, err)
		}
		if first {
			i.recordStates(response.Threads)
			touched := false
			for _, t := range ts {
				if i.states[t.Info.ThreadId] == pb.ThreadState_THREAD_STATE_TOUCHED {
					touched = true
				}
			}
			if !touched {
				return refused("no thread is still touched by this key; post or reply to touch a thread")
			}
			first = false
		}
		visible, err := i.client.Visible(response.Activity)
		if err != nil {
			return err
		}
		if len(visible) == 0 {
			continue
		}
		return i.finishRead(ts, response.Activity, response.Through, false)
	}
}
func (i *invocation) renderActivities(activity []*pb.Activity, metadata map[uint64]*pb.Thread) {
	order := []uint64{}
	groups := map[uint64][]*pb.Activity{}
	for _, a := range activity {
		if _, ok := groups[a.ThreadId]; !ok {
			order = append(order, a.ThreadId)
		}
		groups[a.ThreadId] = append(groups[a.ThreadId], a)
	}
	for _, id := range order {
		title := ""
		if t := metadata[id]; t != nil {
			title = t.Name
		}
		fmt.Fprintf(&i.text, "## %s (%d)\n", line(title), id)
		for _, a := range groups[id] {
			author := a.GetAuthor().GetDisplayName()
			if author == "" {
				author = a.GetAuthor().GetUsername()
			}
			when := ""
			if a.Time != nil {
				when = a.Time.AsTime().UTC().Format("2006-01-02T15:04:05Z")
			}
			kind := "message"
			if a.GetComponent() != nil {
				kind = "component"
			}
			if a.GetPollVote() != nil {
				kind = "poll_vote"
			}
			fmt.Fprintf(&i.text, "%s %s %s %d\n", when, line(author), kind, a.Id)
			if m := a.GetMessage(); m != nil {
				if m.Content != "" {
					i.text.WriteString("  " + strings.ReplaceAll(terminalText(m.Content, true), "\n", "\n  ") + "\n")
				}
				for _, embed := range m.Embeds {
					fields := embed.GetFields()
					label := fields["title"].GetStringValue()
					if label == "" {
						label = fields["author"].GetStructValue().GetFields()["name"].GetStringValue()
					}
					fmt.Fprintf(&i.text, "  embed: %s\n", line(label))
				}
				for _, f := range m.Attachments {
					fmt.Fprintf(&i.text, "  attachment: %s %d %s\n", line(f.Filename), f.Size, line(f.Url))
				}
				if p := m.Poll; p != nil {
					fmt.Fprintf(&i.text, "  poll: %s\n", line(p.Question))
					for _, answer := range p.Answers {
						fmt.Fprintf(&i.text, "  answer: %d %s\n", answer.AnswerId, line(answer.Text))
					}
				}
			}
			if c := a.GetComponent(); c != nil {
				if len(c.Values) > 0 {
					fmt.Fprintf(&i.text, "  chose %s in %s on %d\n", line(strings.Join(c.Values, ", ")), line(c.CustomId), c.MessageId)
				} else {
					fmt.Fprintf(&i.text, "  pressed %s on %d\n", line(c.CustomId), c.MessageId)
				}
			}
			if p := a.GetPollVote(); p != nil {
				verb := "voted"
				if p.Removed {
					verb = "removed vote"
				}
				fmt.Fprintf(&i.text, "  %s %d on %d\n", verb, p.AnswerId, p.MessageId)
			}
		}
	}
}

// The remote gRPC deadline may arrive before the local context timer fires.
func listenError(ctx, listenCtx context.Context, hasTimeout bool, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	if hasTimeout && (status.Code(err) == codes.DeadlineExceeded || errors.Is(listenCtx.Err(), context.DeadlineExceeded)) {
		return &failure{5, "listen timeout passed with no activity; use deck inbox or listen again"}
	}
	if errors.Is(err, io.EOF) {
		return status.Error(codes.Unavailable, "listen stream ended before a reply; listen again")
	}
	return err
}
