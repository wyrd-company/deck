// ---
// relationships:
//
//	implements:
//	  - service
//	  - grpc-api
//
// ---
// Package service implements the Discord-facing DeckService operations.
package service

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"

	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/activity"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/transport"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// SendIndex is implemented by task 7's activity index. Completed returns nil
// when the retained stream has no completed send with this fingerprint/nonce.
type SendIndex interface {
	Position(context.Context) (*pb.StreamPosition, error)
	Completed(context.Context, string, string) (*CompletedSend, error)
	// Confirm waits for every message in send to be discoverable by Completed.
	Confirm(context.Context, *CompletedSend) error
}
type CompletedSend struct {
	PollError string
	Thread    *pb.Thread
	Messages  []*pb.Activity
	Position  *pb.StreamPosition
}
type Dependencies struct {
	Context   context.Context
	Bots      map[string]discord.API
	Registry  *registry.Registry
	Publisher *activity.Publisher
	Index     SendIndex
}
type flight struct {
	done      chan struct{}
	response  proto.Message
	err       error
	operation string
}
type Handler struct {
	pb.UnimplementedDeckServiceServer
	ctx          context.Context
	bots         map[string]discord.API
	keys         map[string]config.Key
	fingerprints map[string]string
	Registry     *registry.Registry
	Publisher    *activity.Publisher
	Index        SendIndex
	mu           sync.Mutex
	flights      map[string]*flight
	wg           sync.WaitGroup
}

func NewHandler(cfg *config.Service, deps Dependencies) (*Handler, error) {
	if deps.Context == nil {
		deps.Context = context.Background()
	}
	if deps.Registry == nil || deps.Publisher == nil || deps.Index == nil {
		return nil, errors.New("service: registry, publisher and index are required")
	}
	h := &Handler{ctx: deps.Context, bots: deps.Bots, keys: map[string]config.Key{}, fingerprints: map[string]string{}, Registry: deps.Registry, Publisher: deps.Publisher, Index: deps.Index, flights: map[string]*flight{}}
	for _, k := range cfg.Keys {
		pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(k.PublicKey))
		if err != nil {
			return nil, fmt.Errorf("keys: invalid public key for %s", k.Name)
		}
		k.Grants = slices.Clone(k.Grants)
		for i := range k.Grants {
			k.Grants[i].Channels = slices.Clone(k.Grants[i].Channels)
		}
		h.keys[k.Name] = k
		h.fingerprints[k.Name] = ssh.FingerprintSHA256(pub)
	}
	return h, nil
}
func denied() error { return status.Error(codes.PermissionDenied, "target is not granted") }
func unavailable() error {
	return status.Error(codes.Unavailable, "service unavailable; send outcome may be unknown")
}
func (h *Handler) calling(ctx context.Context) (config.Key, error) {
	name := transport.KeyName(ctx)
	k, ok := h.keys[name]
	if name == "" || !ok {
		return config.Key{}, status.Error(codes.Unauthenticated, "authenticated key required")
	}
	return k, nil
}
func botGranted(k config.Key, bot string) bool {
	for _, g := range k.Grants {
		if g.Bot == bot {
			return true
		}
	}
	return false
}
func granted(k config.Key, e registry.Entry) bool {
	for _, g := range k.Grants {
		if g.Bot == e.Bot && g.Server == strconv.FormatUint(e.ServerID, 10) {
			for _, ch := range g.Channels {
				if ch == "*" || ch == strconv.FormatUint(e.ChannelID, 10) {
					return true
				}
			}
		}
	}
	return false
}
func (h *Handler) choose(k config.Key, bot string) (string, error) {
	if bot != "" {
		if !botGranted(k, bot) || h.bots[bot] == nil {
			return "", denied()
		}
		return bot, nil
	}
	bots := map[string]bool{}
	for _, g := range k.Grants {
		bots[g.Bot] = true
	}
	if len(bots) != 1 {
		return "", status.Error(codes.InvalidArgument, "bot is required when grants name several bots")
	}
	for b := range bots {
		if h.bots[b] == nil {
			return "", denied()
		}
		return b, nil
	}
	return "", denied()
}
func channelEntry(bot string, ch d.GuildChannel) registry.Entry {
	e := registry.Entry{Bot: bot, ServerID: uint64(ch.GuildID()), ChannelID: uint64(ch.ID())}
	if parent := ch.ParentID(); parent != nil && (ch.Type() == 10 || ch.Type() == 11 || ch.Type() == 12) {
		e.ChannelID = uint64(*parent)
	}
	if message, ok := ch.(d.GuildMessageChannel); ok && message.LastMessageID() != nil {
		e.LastMessageID = uint64(*message.LastMessageID())
	}
	return e
}
func (h *Handler) target(ctx context.Context, k config.Key, bot string, id uint64, thread bool) (discord.API, d.GuildChannel, registry.Entry, error) {
	if id == 0 {
		return nil, nil, registry.Entry{}, status.Error(codes.InvalidArgument, "target id is required")
	}
	var existing registry.Entry
	var touched bool
	if thread {
		existing, touched = h.Registry.Lookup(id)
	}
	var err error
	if touched {
		reachable := granted(k, existing) && h.bots[existing.Bot] != nil
		if bot == "" && !reachable {
			if _, err = h.choose(k, bot); err != nil {
				return nil, nil, existing, err
			}
		}
		if !reachable {
			return nil, nil, existing, denied()
		}
		if bot != "" && bot != existing.Bot {
			return nil, nil, existing, status.Error(codes.InvalidArgument, "thread uses bot "+existing.Bot)
		}
		bot = existing.Bot
	} else {
		bot, err = h.choose(k, bot)
		if err != nil {
			return nil, nil, existing, err
		}
	}
	client := h.bots[bot]
	ch, err := client.Channel(ctx, id)
	if err != nil {
		return nil, nil, existing, denied()
	}
	e := channelEntry(bot, ch)
	if !granted(k, e) {
		return nil, nil, e, denied()
	}
	if touched {
		e = existing
	}
	return client, ch, e, nil
}

// State hides other keys' touches before testing grants, for task 7's unread API.
func (h *Handler) State(ctx context.Context, id uint64) pb.ThreadState {
	k, err := h.calling(ctx)
	if err != nil {
		return pb.ThreadState_THREAD_STATE_NOT_TOUCHED
	}
	e, ok := h.Registry.Lookup(id)
	if !ok {
		return pb.ThreadState_THREAD_STATE_NOT_TOUCHED
	}
	if _, ok = e.Keys[k.Name]; !ok {
		return pb.ThreadState_THREAD_STATE_NOT_TOUCHED
	}
	if !granted(k, e) || h.bots[e.Bot] == nil {
		return pb.ThreadState_THREAD_STATE_NOT_GRANTED
	}
	return pb.ThreadState_THREAD_STATE_TOUCHED
}
func threadValue(ch d.GuildChannel, e registry.Entry, key string) *pb.Thread {
	t := &pb.Thread{Bot: e.Bot, ServerId: e.ServerID, ChannelId: e.ChannelID, ThreadId: uint64(ch.ID()), Name: ch.Name(), CreatedByDeck: e.CreatedByDeck, LastMessageId: e.LastMessageID, Url: fmt.Sprintf("https://discord.com/channels/%d/%d", e.ServerID, ch.ID())}
	if th, ok := ch.(d.GuildThread); ok {
		t.Archived = th.ThreadMetadata.Archived
		t.Locked = th.ThreadMetadata.Locked
		if th.LastMessageID() != nil {
			t.LastMessageId = uint64(*th.LastMessageID())
		}
	}
	_, t.Touched = e.Keys[key]
	return t
}
func (h *Handler) GetThread(ctx context.Context, r *pb.GetThreadRequest) (*pb.GetThreadResponse, error) {
	k, err := h.calling(ctx)
	if err != nil {
		return nil, err
	}
	if r.GetThread() == nil {
		return nil, status.Error(codes.InvalidArgument, "thread is required")
	}
	_, ch, e, err := h.target(ctx, k, r.Thread.Bot, r.Thread.ThreadId, true)
	if err != nil {
		return nil, err
	}
	if ch.Type() != 10 && ch.Type() != 11 && ch.Type() != 12 {
		return nil, status.Error(codes.FailedPrecondition, "target must be a thread")
	}
	return &pb.GetThreadResponse{Thread: threadValue(ch, e, k.Name)}, nil
}
func (h *Handler) ListChannels(ctx context.Context, r *pb.ListChannelsRequest) (*pb.ListChannelsResponse, error) {
	k, err := h.calling(ctx)
	if err != nil {
		return nil, err
	}
	out := &pb.ListChannelsResponse{}
	seen := map[string]bool{}
	for _, grant := range k.Grants {
		client := h.bots[grant.Bot]
		if client == nil {
			continue
		}
		id, err := strconv.ParseUint(grant.Server, 10, 64)
		if err != nil {
			return nil, status.Error(codes.Internal, "invalid configured grant")
		}
		group := grant.Bot + ":" + grant.Server
		if seen[group] {
			continue
		}
		seen[group] = true
		channels, err := client.Channels(ctx, id)
		if err != nil {
			return nil, discord.Error(err)
		}
		serverName, err := client.ServerName(ctx, id)
		if err != nil {
			return nil, discord.Error(err)
		}
		for _, ch := range channels {
			e := channelEntry(grant.Bot, ch)
			if e.ServerID != id || !granted(k, e) {
				continue
			}
			if ch.Type() != 0 && ch.Type() != 5 && ch.Type() != 15 && ch.Type() != 16 {
				continue
			}
			c := &pb.Channel{Bot: e.Bot, ServerId: e.ServerID, ServerName: serverName, ChannelId: uint64(ch.ID()), Name: ch.Name(), Type: uint32(ch.Type())}
			tags, requires := forumTags(ch)
			c.RequiresTag = requires
			for _, tag := range tags {
				c.Tags = append(c.Tags, &pb.ForumTag{Id: uint64(tag.ID), Name: tag.Name})
			}
			out.Channels = append(out.Channels, c)
		}
	}
	slices.SortFunc(out.Channels, func(a, b *pb.Channel) int {
		if a.Bot < b.Bot {
			return -1
		}
		if a.Bot > b.Bot {
			return 1
		}
		if a.ChannelId < b.ChannelId {
			return -1
		}
		if a.ChannelId > b.ChannelId {
			return 1
		}
		return 0
	})
	return out, nil
}
func (h *Handler) ListHistory(ctx context.Context, r *pb.ListHistoryRequest) (*pb.ListHistoryResponse, error) {
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
	if r.AfterId != 0 && r.BeforeId != 0 || r.Limit > 100 {
		return nil, status.Error(codes.InvalidArgument, "history: specify one cursor and limit at most 100")
	}
	limit := int(r.Limit)
	if limit == 0 {
		limit = 50
	}
	messages, err := client.History(ctx, r.Thread.ThreadId, r.AfterId, r.BeforeId, limit)
	if err != nil {
		return nil, discord.Error(err)
	}
	slices.SortFunc(messages, func(a, b d.Message) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	out := &pb.ListHistoryResponse{}
	for i := range messages {
		out.Activity = append(out.Activity, discord.Activity(&messages[i], e, r.Thread.ThreadId))
	}
	if len(messages) == limit {
		after, before := uint64(0), uint64(0)
		if r.AfterId != 0 {
			after = uint64(messages[len(messages)-1].ID)
		} else {
			before = uint64(messages[0].ID)
		}
		more, err := client.History(ctx, r.Thread.ThreadId, after, before, 1)
		if err != nil {
			return nil, discord.Error(err)
		}
		out.More = len(more) > 0
	}
	return out, nil
}
func forumTags(ch d.GuildChannel) ([]d.ChannelTag, bool) {
	switch c := ch.(type) {
	case d.GuildForumChannel:
		return c.AvailableTags, c.Flags().Has(d.ChannelFlagRequireTag)
	case d.GuildMediaChannel:
		return c.AvailableTags, c.Flags().Has(d.ChannelFlagRequireTag)
	}
	return nil, false
}
func resolveTags(ch d.GuildChannel, names []string) ([]snowflake.ID, error) {
	available, required := forumTags(ch)
	if len(names) > 5 {
		return nil, status.Error(codes.InvalidArgument, "tags: maximum 5")
	}
	ids := []snowflake.ID{}
	for _, name := range names {
		found := false
		for _, tag := range available {
			if name == tag.Name || name == tag.ID.String() {
				ids = append(ids, tag.ID)
				found = true
				break
			}
		}
		if !found {
			return nil, status.Error(codes.InvalidArgument, "tags: unknown forum tag")
		}
	}
	if required && len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tags: forum requires at least one tag")
	}
	return ids, nil
}

func (h *Handler) completedThread(ctx context.Context, k config.Key, c *CompletedSend) (*pb.Thread, error) {
	if c.Thread == nil {
		return nil, status.Error(codes.Internal, "completed send has no thread")
	}
	_, ch, e, err := h.target(ctx, k, c.Thread.Bot, c.Thread.ThreadId, true)
	if err != nil {
		return nil, err
	}
	t := threadValue(ch, e, k.Name)
	t.CreatedByDeck = t.CreatedByDeck || c.Thread.CreatedByDeck
	return t, nil
}
