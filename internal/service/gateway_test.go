//go:build !race

// The race detector reports a data race inside disgo v0.20.0 itself:
// gateway.CloseWithCode reads heartbeatCancel (gateway/gateway.go:360)
// without the lock heartbeat() would need when it writes it (:491-492), so
// any test that opens and closes a real session fails under -race. These
// tests run in every ordinary `go test`, including task ci.

package service

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/snowflake/v2"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/activity"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/service/testutil"
)

type gatewayHarness struct {
	*harness
	fakeGW  map[string]*testutil.Gateway
	gateway map[string]*discord.Gateway
}

// setupGateways opens bot a's and bot b's sessions against fake gateways
// (bot user ids 123 and 456) over the real index, registry and publisher.
func setupGateways(t *testing.T) *gatewayHarness {
	t.Helper()
	fakes := map[string]*testutil.Gateway{"a": testutil.NewGateway(t, "123", testutil.Guild()), "b": testutil.NewGateway(t, "456", testutil.Guild())}
	s := setupWith(t, func(name string) []bot.ConfigOpt {
		return []bot.ConfigOpt{bot.WithGatewayConfigOpts(gateway.WithURL(fakes[name].URL), gateway.WithCompression(gateway.CompressionNone))}
	})
	idx := activity.NewIndex(context.Background(), s.js, s.reg.KV, s.cfg.NATS, nil, quiet())
	t.Cleanup(idx.Close)
	s.h.Activity = idx
	s.h.Index = streamIndex{idx}
	g := &gatewayHarness{harness: s, fakeGW: fakes, gateway: map[string]*discord.Gateway{}}
	for _, name := range []string{"a", "b"} {
		gw := discord.NewGateway(context.Background(), name, s.h.bots[name].(*discord.Client), s.reg, s.h.Publisher, idx, quiet())
		g.gateway[name] = gw
		t.Cleanup(func() { gw.Close(context.Background()); gw.Wait() })
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err := gw.Open(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		fakes[name].WaitReady(t)
	}
	s.position(t)
	return g
}

func messageEvent(id, channel uint64, author string, kind int) map[string]any {
	return map[string]any{"id": strconv.FormatUint(id, 10), "channel_id": strconv.FormatUint(channel, 10), "guild_id": "1", "author": map[string]any{"id": author, "username": "u" + author}, "content": "hello", "timestamp": "2026-11-16T00:00:00Z", "type": kind, "mentions": []any{}, "attachments": []any{}, "embeds": []any{}}
}

func voteEvent(channel, message uint64, answer int) map[string]any {
	return map[string]any{"user_id": "7", "channel_id": strconv.FormatUint(channel, 10), "message_id": strconv.FormatUint(message, 10), "guild_id": "1", "answer_id": answer}
}

func componentEvent(id, channel, message uint64, application string) map[string]any {
	return map[string]any{"id": strconv.FormatUint(id, 10), "application_id": application, "type": 3, "token": "interaction-token", "version": 1, "guild_id": "1", "channel_id": strconv.FormatUint(channel, 10),
		"channel": map[string]any{"id": strconv.FormatUint(channel, 10), "type": 11, "guild_id": "1", "parent_id": "10", "name": "t"},
		"member":  map[string]any{"user": map[string]any{"id": "7", "username": "person"}, "roles": []any{}, "joined_at": "2026-01-01T00:00:00Z", "permissions": "0"},
		"data":    map[string]any{"custom_id": "approve", "component_type": 2},
		"message": messageEvent(message, channel, application, 0), "entitlements": []any{}, "authorizing_integration_owners": map[string]any{}, "context": 0}
}

func registerBot(t *testing.T, reg *registry.Registry, id, channel uint64, botName string, boundary uint64) {
	t.Helper()
	if err := reg.RegisterPost(id, registry.Entry{Bot: botName, ServerID: 1, ChannelID: channel, CreatedByDeck: true, LastMessageID: boundary}, "key"); err != nil {
		t.Fatal(err)
	}
}

// A touched thread's activity is published with its subject and headers; a
// thread nobody touched publishes nothing; the bot's own type 0 and 19 copies
// are dropped and its type 46 poll result published; two bots that see one
// touched thread publish each message and vote once, by the thread's bot;
// and the boundary follows acknowledged publishes.
func TestGatewayPublishesTouchedThreads(t *testing.T) {
	g := setupGateways(t)
	registerBot(t, g.reg, 100, 10, "a", 100)
	for _, name := range []string{"a", "b"} { // both bots see the thread
		gw := g.fakeGW[name]
		gw.Dispatch("MESSAGE_CREATE", messageEvent(1001, 100, "7", 0))
		gw.Dispatch("MESSAGE_CREATE", messageEvent(1002, 777, "7", 0)) // untouched
		gw.Dispatch("MESSAGE_CREATE", messageEvent(1003, 100, "123", 0))
		gw.Dispatch("MESSAGE_CREATE", messageEvent(1004, 100, "123", 19))
		gw.Dispatch("MESSAGE_CREATE", messageEvent(1005, 100, "123", 46))
		gw.Dispatch("MESSAGE_POLL_VOTE_ADD", voteEvent(100, 1006, 1))
		gw.Dispatch("MESSAGE_POLL_VOTE_REMOVE", voteEvent(100, 1006, 1))
		gw.Dispatch("MESSAGE_POLL_VOTE_ADD", voteEvent(100, 1006, 1))
	}
	got := g.eventually(t, 5)
	if got[0].a.Id != 1001 || got[1].a.Id != 1005 || got[0].subject != "deck.1.10.100" {
		t.Fatal("published messages", got)
	}
	h := got[0].header
	if h["Nats-Msg-Id"] != "message:1001" || h["Deck-Kind"] != "message" || h["Deck-Activity-Id"] != "1001" || h["Deck-Message-Type"] != "0" || h["Deck-Schema"] != "deck.v1.Activity" || h["Deck-Sent-By"] != "" {
		t.Fatal("headers", h)
	}
	votes := got[2:]
	ids := map[uint64]bool{}
	for n, v := range votes {
		vote := v.a.GetPollVote()
		if vote == nil || vote.MessageId != 1006 || vote.Removed != (n == 1) || v.a.Bot != "a" || v.header["Deck-Kind"] != "poll_vote" || v.a.Author.Username != "person" {
			t.Fatal("vote", v)
		}
		ids[v.a.Id] = true
	}
	if len(ids) != 3 {
		t.Fatal("votes share a synthetic id")
	}
	testutil.Until(t, func() bool { e, _ := g.reg.Lookup(100); return e.LastMessageID == 1005 })
}

// Component uses are acknowledged with a deferred update at once, touched
// thread or not, and published by the bot Discord delivered them to, under
// the thread's bot; activity on a text channel post's starter belongs to its
// thread.
func TestComponentsAndStarterRouting(t *testing.T) {
	g := setupGateways(t)
	registerBot(t, g.reg, 100, 10, "a", 100)
	registerBot(t, g.reg, 200, 20, "a", 200)
	started := time.Now()
	g.fakeGW["b"].Dispatch("INTERACTION_CREATE", componentEvent(5001, 100, 1000, "456"))
	testutil.Until(t, func() bool {
		return slices.ContainsFunc(g.fakes["b"].posts(), func(r request) bool { return strings.HasPrefix(r.path, "/interactions/5001/") })
	})
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatal("acknowledgement missed Discord's deadline", elapsed)
	}
	g.fakeGW["a"].Dispatch("INTERACTION_CREATE", componentEvent(5002, 777, 1000, "123")) // untouched
	g.fakeGW["a"].Dispatch("MESSAGE_POLL_VOTE_ADD", voteEvent(20, 200, 2))
	g.fakeGW["a"].Dispatch("INTERACTION_CREATE", componentEvent(5003, 20, 200, "123"))
	result := messageEvent(2001, 20, "123", 46)
	result["message_reference"] = map[string]any{"message_id": "200", "channel_id": "20", "guild_id": "1"}
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", result)
	testutil.Until(t, func() bool {
		return slices.ContainsFunc(g.fakes["a"].posts(), func(r request) bool { return strings.HasPrefix(r.path, "/interactions/5002/") })
	})
	got := g.eventually(t, 4)
	use := got[0].a.GetComponent()
	if use == nil || got[0].a.Id != 5001 || got[0].a.Bot != "a" || use.CustomId != "approve" || use.ComponentType != 2 || use.MessageId != 1000 || got[0].subject != "deck.1.10.100" {
		t.Fatal("component use", got[0])
	}
	for _, e := range got[1:] {
		if e.subject != "deck.1.20.200" || e.a.ThreadId != 200 {
			t.Fatal("starter activity not on its thread", e)
		}
	}
}

// A person's reply held while its thread's post was in flight is published
// once the thread registers; one whose thread never registers is dropped.
func TestHeldEventsReleasedOnRegistration(t *testing.T) {
	window := discord.SettleWindow
	discord.SettleWindow = time.Second
	t.Cleanup(func() { discord.SettleWindow = window })
	g := setupGateways(t)
	fresh := uint64(snowflake.New(time.Now()))
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(fresh+1, fresh, "7", 0))
	never := uint64(snowflake.New(time.Now())) + 1<<22
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(never+1, never, "7", 0))
	time.Sleep(200 * time.Millisecond)
	if len(g.events(t)) != 0 {
		t.Fatal("unregistered thread published")
	}
	registerBot(t, g.reg, fresh, 10, "a", fresh)
	got := g.eventually(t, 1)
	if got[0].a.Id != fresh+1 || got[0].a.ThreadId != fresh {
		t.Fatal("held reply", got)
	}
	time.Sleep(discord.SettleWindow + 1500*time.Millisecond)
	registerBot(t, g.reg, never, 10, "a", never)
	g.eventually(t, 1)
}

// A deck send is published once, by the send, with sent_by, sender_key and
// nonce, whether the gateway copy reaches the service first or later, and a
// forum post's poll is the thread's second message.
func TestOwnMessagesPublishedBySendOnly(t *testing.T) {
	g := setupGateways(t)
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(100, 100, "123", 0)) // gateway copy first
	c := content()
	c.Poll = &pb.PollCreate{Question: "?", Answers: []string{"y", "n"}}
	post, err := g.client.Post(g.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "t", Content: c, Nonce: "own"})
	if err != nil {
		t.Fatal(err)
	}
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(100, 100, "123", 0)) // and later
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(101, 100, "123", 0))
	got := g.eventually(t, 2)
	if got[0].a.Id != post.Message.Id || got[1].a.Id != post.PollMessage.Id || got[0].header["Deck-Sent-By"] != "key" || got[0].header["Deck-Nonce"] != "own" || got[0].header["Deck-Sender-Key"] != g.h.fingerprints["key"] || got[0].header["Deck-Send-Start"] == "" {
		t.Fatal("send publication", got)
	}
}

// recoveryFake serves a thread's channel object and its messages after a
// boundary, oldest-last as Discord returns them.
type recoveryFake struct {
	mu       sync.Mutex
	last     map[string]uint64
	messages map[string][]map[string]any
	fail     map[string]int
	block    chan struct{}
	reads    map[string]int
}

func (r *recoveryFake) install(f *fakeDiscord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channel = func(id string) (int, any) {
		r.mu.Lock()
		defer r.mu.Unlock()
		last, ok := r.last[id]
		if !ok {
			return 0, nil
		}
		r.reads[id]++
		if code := r.fail[id]; code != 0 {
			return 404, map[string]any{"code": code, "message": "fails"}
		}
		thread := threadJSON(id, "10")
		thread["last_message_id"] = strconv.FormatUint(last, 10)
		thread["thread_metadata"] = map[string]any{"archived": true, "locked": false}
		thread["type"] = 12
		return 200, thread
	}
	f.history = func(path string, q url.Values) (int, any) {
		id := strings.Split(path, "/")[2]
		if r.block != nil {
			<-r.block
		}
		after, _ := strconv.ParseUint(q.Get("after"), 10, 64)
		r.mu.Lock()
		defer r.mu.Unlock()
		out := []any{}
		for _, m := range r.messages[id] {
			n, _ := strconv.ParseUint(m["id"].(string), 10, 64)
			if n > after {
				out = append([]any{m}, out...)
			}
		}
		return 200, out
	}
}

// Recovery at READY publishes the messages sent while the session was down,
// in an archived private thread, dropping the bot's own copies and those the
// index holds; live publishes during a pending recovery leave the boundary
// unraised; a thread whose read fails is read again at the next READY; a
// deleted thread leaves the registry.
func TestRecoveryAtReady(t *testing.T) {
	g := setupGateways(t)
	registerBot(t, g.reg, 100, 10, "a", 1000)
	registerBot(t, g.reg, 300, 10, "a", 3000)
	registerBot(t, g.reg, 400, 10, "a", 4000)
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(1001, 100, "7", 0))
	g.eventually(t, 1)
	testutil.Until(t, func() bool { e, _ := g.reg.Lookup(100); return e.LastMessageID == 1001 })
	g.reg.Advance(100, 0) // the index holds 1001; recovery must not publish it again
	rf := &recoveryFake{last: map[string]uint64{"100": 1004, "300": 3001, "400": 4001}, fail: map[string]int{"300": 50001, "400": 10003}, reads: map[string]int{}, block: make(chan struct{}),
		messages: map[string][]map[string]any{"100": {messageEvent(1001, 100, "7", 0), messageEvent(1002, 100, "7", 0), messageEvent(1003, 100, "123", 0), messageEvent(1004, 100, "7", 19)}, "300": {messageEvent(3001, 300, "7", 0)}}}
	// The boundary recovery copies at READY is 1001, so 1001 is not read again.
	rf.install(g.fakes["a"])
	g.fakeGW["a"].Expire()
	g.fakeGW["a"].WaitReady(t)
	testutil.Until(t, func() bool { return g.gateway["a"].Pending(100) })
	// While recovery is pending, a live publish does not raise the boundary.
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(1010, 100, "7", 0))
	g.eventually(t, 2)
	time.Sleep(300 * time.Millisecond)
	if e, _ := g.reg.Lookup(100); e.LastMessageID != 1001 {
		t.Fatal("live publish raised a pending boundary", e.LastMessageID)
	}
	close(rf.block)
	got := g.eventually(t, 4)
	if got[2].a.Id != 1002 || got[3].a.Id != 1004 {
		t.Fatal("recovered", got)
	}
	testutil.Until(t, func() bool { e, _ := g.reg.Lookup(100); return e.LastMessageID == 1004 && !g.gateway["a"].Pending(100) })
	testutil.Until(t, func() bool { _, ok := g.reg.Lookup(400); return !ok })
	if !g.gateway["a"].Pending(300) {
		t.Fatal("failed read left recovery")
	}
	if e, _ := g.reg.Lookup(300); e.LastMessageID != 3000 {
		t.Fatal("failed read moved the boundary")
	}
	// The next READY reads the failed thread again.
	rf.mu.Lock()
	rf.fail = map[string]int{}
	rf.mu.Unlock()
	g.fakeGW["a"].Expire()
	g.fakeGW["a"].WaitReady(t)
	got = g.eventually(t, 5)
	if got[4].a.Id != 3001 {
		t.Fatal("failed thread not read again", got[4])
	}
	testutil.Until(t, func() bool { return !g.gateway["a"].Pending(300) })
}

// A component use, a vote and a message for a thread registered to another
// bot: only the thread's bot publishes messages and votes.
func TestOtherBotsIgnoreMessagesAndVotes(t *testing.T) {
	g := setupGateways(t)
	registerBot(t, g.reg, 100, 10, "b", 100)
	g.fakeGW["a"].Dispatch("MESSAGE_CREATE", messageEvent(1001, 100, "7", 0))
	g.fakeGW["a"].Dispatch("MESSAGE_POLL_VOTE_ADD", voteEvent(100, 1002, 1))
	g.fakeGW["b"].Dispatch("MESSAGE_CREATE", messageEvent(1003, 100, "7", 0))
	got := g.eventually(t, 1)
	if got[0].a.Id != 1003 || got[0].a.Bot != "b" {
		t.Fatal(fmt.Sprint(got))
	}
}
