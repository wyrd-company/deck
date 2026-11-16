package discord

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/disgoorg/disgo/bot"
	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/snowflake/v2"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/activity"
	"github.com/wyrd-company/deck/internal/service/registry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Bounds and timings of the gateway path, recorded in the service design.
var (
	// SettleWindow is how long an event in an unregistered thread younger
	// than this is held for the thread to register.
	SettleWindow = 10 * time.Second
	// MaxHeld bounds the settling buffer per bot.
	MaxHeld = 1000
	// MaxWaiting bounds the activity waiting for JetStream that the gateway
	// may add to; beyond it a message is dropped and read back by recovery.
	MaxWaiting = 10000
	// statePoll is how often a session's state is sampled for the log.
	statePoll = time.Second
	// GuildsReadyWait bounds the wait for every server of a session to be
	// ready before recovery starts anyway.
	GuildsReadyWait = 30 * time.Second
)

// voteCounter makes poll-vote snowflakes unique within the process.
var voteCounter atomic.Uint64

// VoteID is a poll vote's snowflake: milliseconds since the Discord epoch
// shifted left 22 bits, plus a per-process counter modulo 2^22.
func VoteID(at time.Time) uint64 {
	ms := uint64(at.UnixMilli() - snowflake.Epoch)
	return ms<<22 | voteCounter.Add(1)%(1<<22)
}

type held struct {
	thread uint64
	until  time.Time
	run    func()
}

// Gateway publishes one bot's gateway activity in touched threads and
// recovers missed messages at the bot's READY.
type Gateway struct {
	Name      string
	client    *Client
	registry  *registry.Registry
	publisher *activity.Publisher
	index     *activity.Index
	log       *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	mu         sync.Mutex
	held       []held
	overflowed map[uint64]time.Time
	session    uint64
	boundaries map[uint64]uint64
	pending    map[uint64]bool
	recovered  uint64
	closed     bool
}

// NewGateway installs the bot's listeners. Install every gateway before
// opening any session, and before serving, so no event is missed.
func NewGateway(ctx context.Context, name string, client *Client, reg *registry.Registry, publisher *activity.Publisher, index *activity.Index, log *slog.Logger) *Gateway {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(ctx)
	g := &Gateway{Name: name, client: client, registry: reg, publisher: publisher, index: index, log: log.With("bot", name), ctx: ctx, cancel: cancel, overflowed: map[uint64]time.Time{}, boundaries: map[uint64]uint64{}, pending: map[uint64]bool{}}
	client.Bot.AddEventListeners(
		bot.NewListenerFunc(g.onReady),
		bot.NewListenerFunc(g.onGuildsReady),
		bot.NewListenerFunc(g.onMessage),
		bot.NewListenerFunc(g.onComponent),
		bot.NewListenerFunc(g.onRaw),
	)
	reg.OnRegister(g.registered)
	g.spawn(g.janitor)
	return g
}

// spawn starts background work unless the gateway is closed, so no work is
// added while Wait runs.
func (g *Gateway) spawn(f func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
	return true
}

// Open opens the bot's gateway session, returning once Discord sent READY.
func (g *Gateway) Open(ctx context.Context) error {
	if err := g.client.Bot.OpenGateway(ctx); err != nil {
		return err
	}
	g.spawn(g.watchState)
	return nil
}

// Status is the session's state for the healthcheck: "ready", or
// "disconnected" while it connects, reconnects or resumes.
func (g *Gateway) Status() string {
	if g.client.Bot.Gateway != nil && g.client.Bot.Gateway.Status() == gateway.StatusReady {
		return "ready"
	}
	return "disconnected"
}

// Close closes the bot's session and stops recovery and new work. Publishes
// already submitted still raise boundaries as their acknowledgements arrive;
// Wait returns once the publisher has answered them all.
func (g *Gateway) Close(ctx context.Context) {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.cancel()
	if g.client.Bot.Gateway != nil {
		g.client.Bot.Gateway.Close(ctx)
	}
}

// Wait waits for the gateway's background work after Close.
func (g *Gateway) Wait() { g.wg.Wait() }

func (g *Gateway) watchState() {
	last := g.Status()
	g.log.Info("gateway session state", "state", last)
	t := time.NewTicker(statePoll)
	defer t.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-t.C:
			if now := g.Status(); now != last {
				last = now
				g.log.Info("gateway session state", "state", now)
			}
		}
	}
}

func (g *Gateway) janitor() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case now := <-t.C:
			g.mu.Lock()
			g.held = slices.DeleteFunc(g.held, func(h held) bool { return now.After(h.until) })
			for thread, until := range g.overflowed {
				if now.After(until) {
					delete(g.overflowed, thread)
				}
			}
			g.mu.Unlock()
		}
	}
}

func (g *Gateway) self() uint64 {
	if id := g.client.Bot.ID(); id != 0 {
		return uint64(id)
	}
	return uint64(g.client.Bot.ApplicationID)
}

// route finds the registered thread an event belongs to: the channel itself,
// or for an event in a channel that is not a thread, the thread started from
// the message, whose snowflake it shares and whose parent is that channel.
func (g *Gateway) route(channel, message uint64) (uint64, registry.Entry, bool) {
	if e, ok := g.registry.Lookup(channel); ok {
		return channel, e, true
	}
	if message != 0 {
		if e, ok := g.registry.Lookup(message); ok && e.ChannelID == channel {
			return message, e, true
		}
	}
	return 0, registry.Entry{}, false
}

// hold keeps an event in an unregistered thread younger than SettleWindow
// until the thread registers or the window passes. Events in channels the
// bot's cache knows are not threads are never held.
func (g *Gateway) hold(channel uint64, run func()) {
	created := snowflake.ID(channel).Time()
	until := created.Add(SettleWindow)
	if time.Now().After(until) {
		return
	}
	if ch, ok := g.client.Bot.Caches.Channel(snowflake.ID(channel)); ok {
		if t := ch.Type(); t != d.ChannelTypeGuildNewsThread && t != d.ChannelTypeGuildPublicThread && t != d.ChannelTypeGuildPrivateThread {
			return
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.held) >= MaxHeld {
		dropped := g.held[0]
		g.held = g.held[1:]
		g.overflowed[dropped.thread] = dropped.until
		g.log.Warn("settling buffer overflow; event dropped", "thread", dropped.thread)
	}
	g.held = append(g.held, held{thread: channel, until: until, run: run})
}

// registered releases a newly registered thread's held events into
// publishing, and schedules recovery when the buffer dropped one of them.
func (g *Gateway) registered(id uint64, e registry.Entry) {
	g.mu.Lock()
	release := []func(){}
	g.held = slices.DeleteFunc(g.held, func(h held) bool {
		if h.thread == id {
			release = append(release, h.run)
			return true
		}
		return false
	})
	_, lost := g.overflowed[id]
	delete(g.overflowed, id)
	if lost && e.Bot == g.Name {
		g.pending[id] = true
	}
	g.mu.Unlock()
	for _, run := range release {
		run()
	}
	if lost && e.Bot == g.Name {
		g.recoverLater([]uint64{id})
	}
}

// releaseHeld runs a thread's held events before a newer one is published.
func (g *Gateway) releaseHeld(id uint64) {
	g.mu.Lock()
	release := []func(){}
	g.held = slices.DeleteFunc(g.held, func(h held) bool {
		if h.thread == id {
			release = append(release, h.run)
			return true
		}
		return false
	})
	g.mu.Unlock()
	for _, run := range release {
		run()
	}
}

// publish submits an activity and, for a message, raises the thread's
// boundary once JetStream acknowledged it, unless the thread's recovery is
// pending. Beyond MaxWaiting a message is dropped and the thread is left to
// recovery, so its boundary never passes the dropped message.
func (g *Gateway) publish(thread uint64, a *pb.Activity, message bool) {
	if g.publisher.Pending() >= MaxWaiting {
		g.log.Warn("publish buffer overflow; activity dropped", "thread", thread, "kind", kind(a))
		if message {
			g.mu.Lock()
			g.pending[thread] = true
			g.mu.Unlock()
			g.recoverLater([]uint64{thread})
		}
		return
	}
	done, err := g.publisher.Submit(a, nil)
	if err != nil {
		return
	}
	id := a.Id
	g.spawn(func() {
		if r := <-done; r.Err == nil {
			g.advance(thread, id, message)
		}
	})
}

func kind(a *pb.Activity) string {
	switch a.Kind.(type) {
	case *pb.Activity_Component:
		return "component"
	case *pb.Activity_PollVote:
		return "poll_vote"
	}
	return "message"
}

// advance records a publish: active_at always, the boundary only for a
// message in a thread whose recovery is not pending.
func (g *Gateway) advance(thread, id uint64, message bool) {
	g.mu.Lock()
	frozen := g.pending[thread]
	g.mu.Unlock()
	if !message || frozen {
		id = 0
	}
	g.registry.Advance(thread, id)
}

func (g *Gateway) onMessage(e *events.GuildMessageCreate) {
	m := e.Message
	var reference uint64
	if m.Type == d.MessageTypePollResult && m.MessageReference != nil && m.MessageReference.MessageID != nil {
		reference = uint64(*m.MessageReference.MessageID)
	}
	g.message(uint64(e.ChannelID), reference, m)
}

func (g *Gateway) message(channel, reference uint64, m d.Message) {
	thread, entry, ok := g.route(channel, reference)
	if !ok {
		g.hold(channel, func() { g.message(channel, reference, m) })
		return
	}
	g.releaseHeld(thread)
	if entry.Bot != g.Name {
		return // only the thread's bot publishes its messages
	}
	if uint64(m.Author.ID) == g.self() && (m.Type == d.MessageTypeDefault || m.Type == d.MessageTypeReply) {
		return // deck's own messages are published by their send alone
	}
	g.publish(thread, Activity(&m, entry, thread), true)
}

func (g *Gateway) onComponent(e *events.ComponentInteractionCreate) {
	// Acknowledge first, within Discord's deadline, touched thread or not.
	if err := e.DeferUpdateMessage(); err != nil {
		g.log.Warn("component interaction not acknowledged", "interaction", uint64(e.ID()))
	}
	channel, message := uint64(e.Message.ChannelID), uint64(e.Message.ID)
	user := e.User()
	a := &pb.Activity{Id: uint64(e.ID()), Time: timestamppb.New(e.CreatedAt()), Author: &pb.User{Id: uint64(user.ID), Username: user.Username, Bot: user.Bot}}
	if user.GlobalName != nil {
		a.Author.DisplayName = *user.GlobalName
	}
	use := &pb.ComponentUse{MessageId: message, CustomId: e.Data.CustomID(), ComponentType: uint32(e.Data.Type())}
	switch data := e.Data.(type) {
	case d.StringSelectMenuInteractionData:
		use.Values = data.Values
	case d.UserSelectMenuInteractionData:
		use.Values = ids(data.Values)
	case d.RoleSelectMenuInteractionData:
		use.Values = ids(data.Values)
	case d.MentionableSelectMenuInteractionData:
		use.Values = ids(data.Values)
	case d.ChannelSelectMenuInteractionData:
		use.Values = ids(data.Values)
	}
	a.Kind = &pb.Activity_Component{Component: use}
	g.component(channel, message, a)
}

func ids(values []snowflake.ID) []string {
	out := make([]string, len(values))
	for n, v := range values {
		out[n] = v.String()
	}
	return out
}

// component publishes a component use on any registered thread: Discord
// delivers it only to the bot that sent the message, so it has one receiver.
func (g *Gateway) component(channel, message uint64, a *pb.Activity) {
	thread, entry, ok := g.route(channel, message)
	if !ok {
		g.hold(channel, func() { g.component(channel, message, a) })
		return
	}
	g.releaseHeld(thread)
	a.Bot, a.ServerId, a.ChannelId, a.ThreadId = entry.Bot, entry.ServerID, entry.ChannelID, thread
	g.publish(thread, a, false)
}

type rawVote struct {
	UserID    snowflake.ID  `json:"user_id"`
	ChannelID snowflake.ID  `json:"channel_id"`
	MessageID snowflake.ID  `json:"message_id"`
	GuildID   *snowflake.ID `json:"guild_id"`
	AnswerID  int           `json:"answer_id"`
}

// DISGO-WORKAROUND(poll-votes): votes are read from raw gateway events because disgo never decodes them; see wyrd-company/deck issue (pending).
// onRaw reads poll votes from the raw gateway events: disgo v0.20.0 does not
// decode MESSAGE_POLL_VOTE_ADD or _REMOVE (gateway.UnmarshalEventData has no
// case for them), so its typed vote events never fire. The raw event is
// dispatched in order with the session's other events.
func (g *Gateway) onRaw(e *events.Raw) {
	removed := false
	switch e.EventType {
	case gateway.EventTypeMessagePollVoteAdd:
	case gateway.EventTypeMessagePollVoteRemove:
		removed = true
	default:
		return
	}
	var v rawVote
	if err := json.NewDecoder(e.Payload).Decode(&v); err != nil || v.GuildID == nil {
		return
	}
	g.vote(v, removed)
}

func (g *Gateway) vote(v rawVote, removed bool) {
	a := &pb.Activity{Id: VoteID(time.Now()), Time: timestamppb.Now(), Author: &pb.User{Id: uint64(v.UserID)}}
	if member, ok := g.client.Bot.Caches.Member(*v.GuildID, v.UserID); ok {
		a.Author.Username, a.Author.Bot = member.User.Username, member.User.Bot
	}
	a.Kind = &pb.Activity_PollVote{PollVote: &pb.PollVote{MessageId: uint64(v.MessageID), AnswerId: uint32(v.AnswerID), Removed: removed}}
	g.pollVote(uint64(v.ChannelID), uint64(v.MessageID), a)
}

func (g *Gateway) pollVote(channel, message uint64, a *pb.Activity) {
	thread, entry, ok := g.route(channel, message)
	if !ok {
		g.hold(channel, func() { g.pollVote(channel, message, a) })
		return
	}
	g.releaseHeld(thread)
	if entry.Bot != g.Name {
		return
	}
	a.Bot, a.ServerId, a.ChannelId, a.ThreadId = entry.Bot, entry.ServerID, entry.ChannelID, thread
	g.publish(thread, a, false)
}

// onReady copies the boundary of each of the bot's registered threads and
// marks their recovery pending, before any later event of the session.
func (g *Gateway) onReady(e *events.Ready) {
	g.mu.Lock()
	g.session++
	session := g.session
	for id, entry := range g.registry.Snapshot() {
		if entry.Bot == g.Name {
			g.boundaries[id] = entry.LastMessageID
			g.pending[id] = true
		}
	}
	g.mu.Unlock()
	g.log.Info("gateway session ready; recovery pending", "threads", g.pendingCount())
	if len(e.Guilds) == 0 {
		g.startRecovery(session)
		return
	}
	// A server that stays unavailable keeps GuildsReady from arriving; recover
	// anyway after GuildsReadyWait rather than leave boundaries frozen.
	g.spawn(func() {
		select {
		case <-g.ctx.Done():
		case <-time.After(GuildsReadyWait):
			g.startRecovery(session)
		}
	})
}

func (g *Gateway) onGuildsReady(*events.GuildsReady) {
	g.mu.Lock()
	session := g.session
	g.mu.Unlock()
	g.startRecovery(session)
}

func (g *Gateway) pendingCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.pending)
}

// startRecovery runs a session's recovery once.
func (g *Gateway) startRecovery(session uint64) {
	g.mu.Lock()
	if g.recovered == session {
		g.mu.Unlock()
		return
	}
	g.recovered = session
	g.mu.Unlock()
	g.spawn(func() { g.recover(session, nil) })
}

// recoverLater reads the given threads back once the publisher has room.
func (g *Gateway) recoverLater(threads []uint64) {
	g.mu.Lock()
	session := g.session
	for _, id := range threads {
		if _, ok := g.boundaries[id]; !ok {
			if e, ok := g.registry.Lookup(id); ok {
				g.boundaries[id] = e.LastMessageID
			}
		}
	}
	g.mu.Unlock()
	g.spawn(func() {
		for g.publisher.Pending() >= MaxWaiting/2 {
			select {
			case <-g.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		g.recover(session, threads)
	})
}

// recover reads each pending thread's messages after its copied boundary,
// oldest first, once the index has replayed, and publishes those the index
// does not hold, except the bot's own type 0 and 19 messages. A thread
// stays pending until a read of it succeeds; Discord's 10003 removes it.
func (g *Gateway) recover(session uint64, only []uint64) {
	if _, err := g.index.Position(g.ctx); err != nil {
		return
	}
	g.mu.Lock()
	threads := []uint64{}
	for id := range g.pending {
		if only == nil || slices.Contains(only, id) {
			threads = append(threads, id)
		}
	}
	g.mu.Unlock()
	slices.Sort(threads)
	for _, id := range threads {
		if g.ctx.Err() != nil {
			return
		}
		g.mu.Lock()
		boundary, stale := g.boundaries[id], g.session != session
		g.mu.Unlock()
		if stale {
			return // a newer READY recovers from its own copy
		}
		newest, err := g.recoverThread(id, boundary)
		if err != nil {
			if Code(err) == 10003 {
				ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
				if g.registry.Delete(ctx, id) == nil {
					g.done(id, session)
				}
				cancel()
				g.log.Info("recovery: thread deleted; left the registry", "thread", id)
				continue
			}
			g.log.Warn("recovery read failed; retried at the next READY", "thread", id)
			continue
		}
		if newest > 0 {
			g.registry.Advance(id, newest)
		}
		g.done(id, session)
	}
}

func (g *Gateway) done(id, session uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.session == session {
		delete(g.pending, id)
		delete(g.boundaries, id)
	}
}

func (g *Gateway) recoverThread(id, boundary uint64) (uint64, error) {
	ctx, cancel := context.WithTimeout(g.ctx, 30*time.Second)
	defer cancel()
	ch, err := g.client.Channel(ctx, id)
	if err != nil {
		return 0, err
	}
	var last uint64
	if mc, ok := ch.(d.GuildMessageChannel); ok && mc.LastMessageID() != nil {
		last = uint64(*mc.LastMessageID())
	}
	if last <= boundary {
		return 0, nil
	}
	entry, ok := g.registry.Lookup(id)
	if !ok {
		return 0, nil
	}
	after, newest := boundary, uint64(0)
	for {
		page, err := g.client.History(ctx, id, after, 0, 100)
		if err != nil {
			return 0, err
		}
		slices.SortFunc(page, func(a, b d.Message) int { return compare(uint64(a.ID), uint64(b.ID)) })
		for n := range page {
			m := &page[n]
			after, newest = max(after, uint64(m.ID)), max(newest, uint64(m.ID))
			if uint64(m.Author.ID) == g.self() && (m.Type == d.MessageTypeDefault || m.Type == d.MessageTypeReply) {
				continue
			}
			if g.index.Holds(id, "message", uint64(m.ID)) {
				continue
			}
			if _, err := g.publisher.Publish(ctx, Activity(m, entry, id), nil); err != nil {
				return 0, err
			}
		}
		if len(page) < 100 {
			return newest, nil
		}
	}
}

func compare(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Pending reports whether a thread's recovery is pending, for tests and logs.
func (g *Gateway) Pending(id uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.pending[id]
}
