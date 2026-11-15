package service

import (
	"context"
	"fmt"
	"unicode/utf16"

	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/activity"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (h *Handler) send(ctx context.Context, k config.Key, nonce, operation string, work func(context.Context) (proto.Message, error)) (proto.Message, error) {
	if !discord.ValidNonce(nonce) {
		return nil, status.Error(codes.InvalidArgument, "nonce: 1 to 25 letters and digits required")
	}
	id := h.fingerprints[k.Name] + ":" + nonce
	h.mu.Lock()
	f, exists := h.flights[id]
	if !exists {
		f = &flight{done: make(chan struct{}), operation: operation}
		h.flights[id] = f
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			f.response, f.err = work(h.ctx)
			h.mu.Lock()
			close(f.done)
			delete(h.flights, id)
			h.mu.Unlock()
		}()
	}
	h.mu.Unlock()
	if f.operation != operation {
		return nil, status.Error(codes.InvalidArgument, "nonce is in use by another operation")
	}
	select {
	case <-ctx.Done():
		return nil, unavailable()
	case <-f.done:
		if f.err != nil {
			return nil, f.err
		}
		return proto.Clone(f.response), nil
	}
}
func (h *Handler) publish(ctx context.Context, k config.Key, nonce string, a *pb.Activity, start *pb.StreamPosition, metadata ...activity.SendMetadata) error {
	a.SentBy = k.Name
	a.SenderKey = h.fingerprints[k.Name]
	a.Nonce = nonce
	seq, err := h.Publisher.Publish(ctx, a, start, metadata...)
	if err != nil {
		return unavailable()
	}
	a.Sequence = seq
	return nil
}
func (h *Handler) Post(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
	k, err := h.calling(ctx)
	if err != nil {
		return nil, err
	}
	if r.GetChannel() == nil {
		return nil, status.Error(codes.InvalidArgument, "channel is required")
	}
	client, ch, e, err := h.target(ctx, k, r.Channel.Bot, r.Channel.ChannelId, false)
	if err != nil {
		return nil, err
	}
	if ch.Type() != 0 && ch.Type() != 5 && ch.Type() != 15 && ch.Type() != 16 {
		return nil, status.Error(codes.FailedPrecondition, "channel type cannot take posts")
	}
	titleLength := len(utf16.Encode([]rune(r.Title)))
	if titleLength < 1 || titleLength > 100 {
		return nil, status.Error(codes.InvalidArgument, "title: 1 to 100 characters required")
	}
	message, err := discord.Build(r.Content)
	if err != nil {
		return nil, err
	}
	forum := ch.Type() == 15 || ch.Type() == 16
	var tags []snowflake.ID
	if forum {
		tags, err = resolveTags(ch, r.Tags)
		if err != nil {
			return nil, err
		}
	} else if len(r.Tags) > 0 {
		return nil, status.Error(codes.InvalidArgument, "tags require a forum or media channel")
	}
	response, err := h.send(ctx, k, r.Nonce, "post", func(run context.Context) (proto.Message, error) {
		restCtx, restCancel := detachedREST(run, ctx)
		defer restCancel()
		completed, err := h.Index.Completed(run, h.fingerprints[k.Name], r.Nonce)
		if err != nil {
			return nil, unavailable()
		}
		if completed != nil {
			thread, err := h.completedThread(run, k, completed)
			if err != nil {
				return nil, err
			}
			out := &pb.PostResponse{Thread: thread, Position: completed.Position, PollError: completed.PollError}
			if len(completed.Messages) > 0 {
				out.Message = completed.Messages[0]
			}
			if len(completed.Messages) > 1 {
				out.PollMessage = completed.Messages[1]
			}
			return out, nil
		}
		if h.Registry.Check(run) != nil {
			return nil, unavailable()
		}
		position, err := h.Index.Position(run)
		if err != nil {
			return nil, unavailable()
		}
		var thread *d.GuildThread
		var sent *d.Message
		poll := message.Poll
		if forum {
			message.Poll = nil
			if poll != nil && message.Content == "" && len(message.Embeds) == 0 && len(message.Components) == 0 && len(message.Files) == 0 {
				message.Content = r.Title
			}
			post, err := client.Post(restCtx, r.Channel.ChannelId, d.ThreadChannelPostCreate{Name: r.Title, Message: message, AppliedTags: tags})
			if err != nil {
				return nil, discord.Error(err)
			}
			thread = &post.GuildThread
			sent = &post.Message
		} else {
			message.Nonce = discord.Nonce(h.fingerprints[k.Name], r.Nonce)
			message.EnforceNonce = true
			sent, err = client.Send(restCtx, r.Channel.ChannelId, message)
			if err != nil {
				return nil, discord.Error(err)
			}
			thread, err = client.StartThread(restCtx, r.Channel.ChannelId, uint64(sent.ID), r.Title)
			if err != nil {
				if discord.Code(err) == 160004 {
					existing, readErr := client.Channel(restCtx, uint64(sent.ID))
					if readErr != nil {
						return nil, discord.Error(readErr)
					}
					value, ok := existing.(d.GuildThread)
					if !ok {
						return nil, status.Error(codes.Internal, "existing thread has unexpected type")
					}
					thread = &value
				} else {
					if !discord.Unknown(err) {
						_ = client.DeleteMessage(restCtx, r.Channel.ChannelId, uint64(sent.ID))
					}
					return nil, discord.Error(err)
				}
			}
		}
		id := uint64(thread.ID())
		e.CreatedByDeck = true
		e.LastMessageID = uint64(sent.ID)
		if thread.LastMessageID() != nil {
			e.LastMessageID = uint64(*thread.LastMessageID())
		}
		if err = h.Registry.RegisterPost(id, e, k.Name); err != nil {
			return nil, denied()
		}
		e, _ = h.Registry.Lookup(id)
		out := &pb.PostResponse{Thread: threadValue(*thread, e, k.Name), Position: position, Message: discord.Activity(sent, e, id)}
		if forum && poll != nil {
			pollMessage := d.MessageCreate{Poll: poll, Nonce: discord.Nonce(h.fingerprints[k.Name], r.Nonce+":poll"), EnforceNonce: true, AllowedMentions: message.AllowedMentions}
			sentPoll, err := client.Send(restCtx, id, pollMessage)
			if err != nil {
				out.PollError = status.Convert(discord.Error(err)).Message()
			} else {
				out.PollMessage = discord.Activity(sentPoll, e, id)
				out.Thread.LastMessageId = out.PollMessage.Id
			}
		}
		if err = h.publish(run, k, r.Nonce, out.Message, position, activity.SendMetadata{PollError: out.PollError}); err != nil {
			return nil, err
		}
		messages := []*pb.Activity{out.Message}
		if out.PollMessage != nil {
			if err = h.publish(run, k, r.Nonce, out.PollMessage, position); err != nil {
				return nil, err
			}
			messages = append(messages, out.PollMessage)
		}
		if err = h.Index.Confirm(run, &CompletedSend{Thread: out.Thread, Messages: messages, Position: position, PollError: out.PollError}); err != nil {
			return nil, unavailable()
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return response.(*pb.PostResponse), nil
}
func (h *Handler) Reply(ctx context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
	k, err := h.calling(ctx)
	if err != nil {
		return nil, err
	}
	if r.GetThread() == nil {
		return nil, status.Error(codes.InvalidArgument, "thread is required")
	}
	client, ch, e, err := h.target(ctx, k, r.Thread.Bot, r.Thread.ThreadId, true)
	if err != nil {
		return nil, err
	}
	if ch.Type() != 10 && ch.Type() != 11 && ch.Type() != 12 {
		return nil, status.Error(codes.FailedPrecondition, "target must be a thread")
	}
	if th, ok := ch.(d.GuildThread); ok && th.ThreadMetadata.Locked {
		return nil, status.Error(codes.FailedPrecondition, "thread is locked")
	}
	message, err := discord.Build(r.Content)
	if err != nil {
		return nil, err
	}
	id := r.Thread.ThreadId
	response, err := h.send(ctx, k, r.Nonce, "reply", func(run context.Context) (proto.Message, error) {
		restCtx, restCancel := detachedREST(run, ctx)
		defer restCancel()
		completed, err := h.Index.Completed(run, h.fingerprints[k.Name], r.Nonce)
		if err != nil {
			return nil, unavailable()
		}
		if completed != nil && len(completed.Messages) > 0 {
			thread, err := h.completedThread(run, k, completed)
			if err != nil {
				return nil, err
			}
			return &pb.ReplyResponse{Thread: thread, Message: completed.Messages[0], Position: completed.Position}, nil
		}
		if err = h.Registry.BeginReply(run, id, e, k.Name); err != nil {
			return nil, unavailable()
		}
		position, err := h.Index.Position(run)
		if err != nil {
			h.Registry.FinishReply(id, k.Name, false, false)
			return nil, unavailable()
		}
		message.Nonce = discord.Nonce(h.fingerprints[k.Name], r.Nonce)
		message.EnforceNonce = true
		if r.ReplyToMessageId != 0 {
			ref := snowflake.ID(r.ReplyToMessageId)
			channel := snowflake.ID(id)
			message.MessageReference = &d.MessageReference{MessageID: &ref, ChannelID: &channel}
		}
		sent, err := client.Send(restCtx, id, message)
		if err != nil {
			h.Registry.FinishReply(id, k.Name, false, discord.Unknown(err))
			return nil, discord.Error(err)
		}
		h.Registry.FinishReply(id, k.Name, true, false)
		e, _ = h.Registry.Lookup(id)
		out := &pb.ReplyResponse{Thread: threadValue(ch, e, k.Name), Message: discord.Activity(sent, e, id), Position: position}
		out.Thread.LastMessageId = uint64(sent.ID)
		out.Thread.Archived = false
		if err = h.publish(run, k, r.Nonce, out.Message, position); err != nil {
			return nil, err
		}
		if err = h.Index.Confirm(run, &CompletedSend{Thread: out.Thread, Messages: []*pb.Activity{out.Message}, Position: position}); err != nil {
			return nil, unavailable()
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return response.(*pb.ReplyResponse), nil
}

// Wait finishes process-owned sends after listeners stop. Cancel the runtime
// context first to bound shutdown when NATS is unavailable.
func (h *Handler) Wait()          { h.wg.Wait() }
func (h *Handler) String() string { return fmt.Sprintf("deck service (%d bots)", len(h.bots)) }

func detachedREST(run, call context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := call.Deadline(); ok {
		return context.WithDeadline(run, deadline)
	}
	return context.WithCancel(run)
}
