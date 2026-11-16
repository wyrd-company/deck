//go:build discordlive

package service

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	discord "github.com/wyrd-company/deck/internal/service/discord"
)

// liveFixture reads the authorized test-server fixture; nothing from it is
// printed.
func liveFixture(t *testing.T) map[string]string {
	t.Helper()
	file, err := os.Open("/workspace/tools/deck/.env")
	if os.IsNotExist(err) {
		t.Skip("Discord test fixture is absent")
	}
	if err != nil {
		t.Fatal("cannot open Discord test fixture")
	}
	defer file.Close()
	values := map[string]string{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if name, value, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(line, "#") {
			values[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), "\"'")
		}
	}
	if scanner.Err() != nil {
		t.Fatal("cannot read Discord test fixture")
	}
	return values
}

// TestDiscordLiveGateway runs the test bot's real gateway session against an
// embedded JetStream server. A message in a thread deck touched is published
// on deck.<server>.<forum>.<thread>; a message in a thread deck did not touch
// publishes nothing. Discord's type 46 poll result, posted when a poll ends,
// stands in for a person's reply: the bot's own type 0 and 19 messages are
// published by their send alone, and the test bot cannot create webhooks.
func TestDiscordLiveGateway(t *testing.T) {
	values := liveFixture(t)
	id := func(name string) uint64 {
		n, err := strconv.ParseUint(values[name], 10, 64)
		if err != nil || n == 0 {
			t.Fatal("invalid Discord test fixture field", name)
		}
		return n
	}
	serverID, forumID := id("SERVER_ID"), id("TEST_FORUM")
	s := setupIndexed(t)
	live, err := discord.New(values["BOT_TOKEN"])
	if err != nil {
		t.Fatal("cannot initialize test Discord client")
	}
	gw := discord.NewGateway(context.Background(), "a", live, s.reg, s.h.Publisher, s.h.Activity, quiet())
	t.Cleanup(func() { gw.Close(context.Background()); gw.Wait(); live.Bot.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if err = gw.Open(ctx); err != nil {
		t.Fatal("gateway session did not open")
	}
	if gw.Status() != "ready" {
		t.Fatal("gateway session not ready")
	}
	t.Log("PASS: live gateway session READY")
	s.h.bots["a"] = live
	k := s.h.keys["key"]
	k.Grants = []config.Grant{{Bot: "a", Server: strconv.FormatUint(serverID, 10), Channels: []string{strconv.FormatUint(forumID, 10)}}}
	s.h.keys["key"] = k
	forum, err := live.Channel(ctx, forumID)
	if err != nil || uint64(forum.GuildID()) != serverID {
		t.Fatal("test forum is not in the configured test server")
	}
	tags := []snowflake.ID{}
	if available, required := forumTags(forum); required && len(available) > 0 {
		tags = append(tags, available[0].ID)
	}
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	tagNames := []string{}
	for _, tag := range tags {
		tagNames = append(tagNames, tag.String())
	}
	poll := &pb.PollCreate{Question: "Deck gateway live poll", Answers: []string{"Yes", "No"}, DurationHours: 1}
	post, err := s.client.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: forumID}, Title: "Deck gateway test " + nonce, Tags: tagNames, Content: &pb.Content{Text: "Deck gateway live verification (touched)", Poll: poll}, Nonce: "G" + nonce})
	if err != nil || post.PollMessage == nil {
		t.Fatal("touching forum post with its poll failed")
	}
	touched := post.Thread.ThreadId
	untouched, err := live.Post(ctx, forumID, d.ThreadChannelPostCreate{Name: "Deck gateway test untouched " + nonce, AppliedTags: tags, Message: d.MessageCreate{Content: "Deck gateway live verification (not touched through deck)"}})
	if err != nil {
		t.Fatal("untouched forum post failed")
	}
	untouchedPoll, err := live.Send(ctx, uint64(untouched.ID()), d.MessageCreate{Poll: &d.PollCreate{Question: d.PollMedia{Text: &poll.Question}, Answers: []d.PollMedia{{Text: &poll.Answers[0]}, {Text: &poll.Answers[1]}}, Duration: 1}})
	if err != nil {
		t.Fatal("untouched poll failed")
	}
	// Ending a poll makes Discord post a type 46 poll result as the bot: a new
	// message in the thread that is not deck's own send, published like any
	// other. Past the settling window, an untouched thread's event is not held.
	time.Sleep(discord.SettleWindow + time.Second)
	if _, err = live.Bot.Rest.ExpirePoll(snowflake.ID(touched), snowflake.ID(post.PollMessage.Id), rest.WithCtx(ctx)); err != nil {
		t.Fatal("cannot end the touched thread's poll")
	}
	if _, err = live.Bot.Rest.ExpirePoll(untouched.ID(), untouchedPoll.ID, rest.WithCtx(ctx)); err != nil {
		t.Fatal("cannot end the untouched thread's poll")
	}
	resultIn := func(thread uint64) uint64 {
		for range 30 {
			messages, err := live.History(ctx, thread, 0, 0, 10)
			if err == nil {
				for _, m := range messages {
					if m.Type == d.MessageTypePollResult {
						return uint64(m.ID)
					}
				}
			}
			time.Sleep(time.Second)
		}
		t.Fatal("Discord posted no poll result")
		return 0
	}
	inTouched, inUntouched := resultIn(touched), resultIn(uint64(untouched.ID()))
	deadline := time.Now().Add(30 * time.Second)
	var found *event
	for found == nil && time.Now().Before(deadline) {
		for _, e := range s.events(t) {
			if e.a.Id == inTouched {
				found = &e
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if found == nil {
		t.Fatal("the touched thread's message was not published")
	}
	want := "deck." + strconv.FormatUint(serverID, 10) + "." + strconv.FormatUint(forumID, 10) + "." + strconv.FormatUint(touched, 10)
	if found.subject != want || found.header["Nats-Msg-Id"] != "message:"+strconv.FormatUint(inTouched, 10) || found.header["Deck-Kind"] != "message" || found.header["Deck-Message-Type"] != "46" || found.a.SentBy != "" {
		t.Fatal("published event has the wrong subject or headers")
	}
	t.Log("PASS: the poll result in the touched thread was published on deck.<server>.<forum>.<thread> with its headers")
	time.Sleep(3 * time.Second)
	for _, e := range s.events(t) {
		if e.a.Id == inUntouched || e.a.ThreadId == uint64(untouched.ID()) {
			t.Fatal("a thread deck did not touch published activity")
		}
	}
	t.Log("PASS: the poll result in a thread deck did not touch published nothing")
	// The send's own copies were dropped: the post's starter is published once,
	// by the send, with sent_by.
	starters := 0
	for _, e := range s.events(t) {
		if e.a.Id == post.Message.Id {
			starters++
			if e.a.SentBy != "key" {
				t.Fatal("deck's own message published without sent_by")
			}
		}
	}
	if starters != 1 {
		t.Fatal("deck's own starter published", starters, "times")
	}
	t.Log("PASS: the gateway copy of deck's own starter was dropped; the send published it once")
}
