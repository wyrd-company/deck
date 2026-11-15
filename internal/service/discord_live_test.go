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

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestDiscordLive reads only the explicitly authorized test-server fixture.
// Nothing from the fixture, a request, or an upstream error is printed.
func TestDiscordLive(t *testing.T) {
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
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if ok {
			values[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), "\"'")
		}
	}
	if scanner.Err() != nil {
		t.Fatal("cannot read Discord test fixture")
	}
	id := func(name string) uint64 {
		n, e := strconv.ParseUint(values[name], 10, 64)
		if e != nil || n == 0 {
			t.Fatal("invalid Discord test fixture field", name)
		}
		return n
	}
	serverID, forumID, channelID, botID := id("SERVER_ID"), id("TEST_FORUM"), id("TEST_CHANNEL"), id("APPLICATION_ID")
	s := setup(t)
	live, err := discord.New(values["BOT_TOKEN"])
	if err != nil {
		t.Fatal("cannot initialize test Discord client")
	}
	defer live.Bot.Close(context.Background())
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	defer cancel()
	forum, err := live.Channel(ctx, forumID)
	if err != nil || uint64(forum.GuildID()) != serverID {
		t.Fatal("test forum is not in the configured test server")
	}
	channel, err := live.Channel(ctx, channelID)
	if err != nil || uint64(channel.GuildID()) != serverID {
		t.Fatal("test channel is not in the configured test server")
	}
	if forum.Type() != 15 && forum.Type() != 16 || channel.Type() != 0 && channel.Type() != 5 {
		t.Fatal("test channels have unexpected types")
	}
	s.h.bots["a"], s.h.bots["b"] = live, live
	k := s.h.keys["key"]
	k.Grants = []config.Grant{{Bot: "a", Server: strconv.FormatUint(serverID, 10), Channels: []string{strconv.FormatUint(forumID, 10)}}, {Bot: "b", Server: strconv.FormatUint(serverID, 10), Channels: []string{strconv.FormatUint(channelID, 10)}}}
	s.h.keys["key"] = k
	embed, _ := structpb.NewStruct(map[string]any{"title": "Deck service live test", "description": "Embeds returned by Discord are checked."})
	component, _ := structpb.NewStruct(map[string]any{"type": 1, "components": []any{map[string]any{"type": 2, "style": 1, "label": "Deck test", "custom_id": "deck-test"}}})
	c := &pb.Content{Text: "Deck service content verification", MentionUserIds: []uint64{botID}, Embeds: []*structpb.Struct{embed}, Components: []*structpb.Struct{component}, Persona: &pb.Persona{Name: "Deck test persona", IconUrl: "https://cdn.discordapp.com/embed/avatars/0.png"}, Files: []*pb.File{{Name: "deck-test.txt", Data: []byte("deck service live verification\n"), Description: "Deck test attachment"}}, Poll: &pb.PollCreate{Question: "Does the poll arrive?", Answers: []string{"Yes", "No"}, DurationHours: 1}}
	tags := []string{}
	available, required := forumTags(forum)
	if required {
		if len(available) == 0 {
			t.Fatal("test forum requires a tag but supplies none")
		}
		tags = append(tags, available[0].ID.String())
	}
	nonce := strconv.FormatInt(time.Now().UnixNano(), 36)
	post, err := s.client.Post(ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: forumID}, Title: "Deck service test " + nonce, Tags: tags, Content: c, Nonce: "P" + nonce})
	if err != nil {
		t.Fatal("forum post failed; upstream details suppressed")
	}
	if post.PollError != "" || post.PollMessage == nil || post.PollMessage.GetMessage().Poll == nil {
		t.Fatal("forum poll was not returned as the second message")
	}
	assertLiveContent(t, post.Message)
	t.Log("PASS: forum persona, mentions, embeds, components, attachment and follow-up poll returned by Discord")
	reply := &pb.ReplyRequest{Thread: &pb.ThreadRef{ThreadId: post.Thread.ThreadId}, Content: &pb.Content{Text: "Deck nonce verification " + nonce}, Nonce: "R" + nonce}
	first, err := s.client.Reply(ctx, reply)
	if err != nil {
		t.Fatal("first live reply failed")
	}
	second, err := s.client.Reply(ctx, reply)
	if err != nil || second.Message.Id != first.Message.Id {
		t.Fatal("Discord did not deduplicate the repeated reply nonce")
	}
	history, err := s.client.ListHistory(ctx, &pb.ListHistoryRequest{Thread: reply.Thread, AfterId: first.Message.Id - 1, Limit: 100})
	if err != nil {
		t.Fatal("live history failed")
	}
	copies := 0
	for _, a := range history.Activity {
		if a.Id == first.Message.Id {
			copies++
		}
	}
	if copies != 1 {
		t.Fatal("repeated reply did not leave exactly one message")
	}
	t.Log("PASS: repeated reply nonce returned one Discord message, confirmed by history")
	textContent := proto.Clone(c).(*pb.Content)
	textContent.Poll = nil
	textReq := &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "b", ChannelId: channelID}, Title: "Deck text test " + nonce, Content: textContent, Nonce: "T" + nonce}
	textPost, err := s.client.Post(ctx, textReq)
	if err != nil {
		t.Fatal("text-channel post failed:", status.Code(err), status.Convert(err).Message())
	}
	assertLiveContent(t, textPost.Message)
	repeated, err := s.client.Post(ctx, textReq)
	if err != nil || repeated.Thread.ThreadId != textPost.Thread.ThreadId {
		t.Fatal("repeated text post did not take the existing thread")
	}
	t.Log("PASS: second configured bot route sent rich content and reused its existing text-channel thread")
	pollReply, err := s.client.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{ThreadId: textPost.Thread.ThreadId}, Content: &pb.Content{Poll: c.Poll}, Nonce: "Q" + nonce})
	if err != nil || pollReply.Message.GetMessage().Poll == nil {
		t.Fatal("text-thread poll reply failed")
	}
	t.Log("PASS: poll-only reply in the text-channel thread returned by Discord")
	// Keep the visible test posts as live evidence; only test-server targets were used.
}
func assertLiveContent(t *testing.T, a *pb.Activity) {
	t.Helper()
	m := a.GetMessage()
	if m == nil || len(m.Embeds) != 2 || len(m.Components) != 1 || len(m.Attachments) != 1 || len(m.MentionUserIds) != 1 {
		t.Fatal("Discord returned incomplete rich content")
	}
	author := m.Embeds[0].GetFields()["author"].GetStructValue()
	if author.GetFields()["name"].GetStringValue() != "Deck test persona" {
		t.Fatal("Discord returned an unexpected persona")
	}
	row := m.Components[0].GetFields()["components"].GetListValue()
	if row == nil || len(row.Values) != 1 || row.Values[0].GetStructValue().GetFields()["custom_id"].GetStringValue() != "deck-test" {
		t.Fatal("Discord returned an unexpected component")
	}
}
