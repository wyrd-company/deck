package service

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/rest"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/activity"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"github.com/wyrd-company/deck/internal/transport"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type request struct {
	method, path string
	body         map[string]any
}
type fakeDiscord struct {
	mu       sync.Mutex
	requests []request
	reject   int
	lose     bool
	pollFail bool
	already  bool
	block    <-chan struct{}
	entered  chan struct{}
	onSend   func()
}

func threadJSON(id, parent string) map[string]any {
	return map[string]any{"id": id, "guild_id": "1", "parent_id": parent, "type": 11, "name": "thread", "last_message_id": "90", "thread_metadata": map[string]any{"archived": false, "locked": false}}
}
func messageJSON(id, channel string, body map[string]any) map[string]any {
	out := map[string]any{"id": id, "channel_id": channel, "timestamp": "2026-11-15T12:00:00Z", "author": map[string]any{"id": "123", "username": "bot", "bot": true}, "type": 0}
	for _, name := range []string{"content", "embeds", "components", "poll", "allowed_mentions", "nonce"} {
		if v, ok := body[name]; ok {
			out[name] = v
		}
	}
	return out
}
func (f *fakeDiscord) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	f.mu.Lock()
	f.requests = append(f.requests, request{r.Method, r.URL.RequestURI(), body})
	reject, lose, pollFail, already, block, entered, onSend := f.reject, f.lose, f.pollFail, f.already, f.block, f.entered, f.onSend
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	respond := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	path := r.URL.Path
	if r.Method == "GET" {
		switch path {
		case "/channels/10":
			respond(map[string]any{"id": "10", "guild_id": "1", "type": 15, "name": "forum", "available_tags": []any{map[string]any{"id": "5", "name": "tag"}}})
		case "/channels/20":
			respond(map[string]any{"id": "20", "guild_id": "1", "type": 0, "name": "text"})
		case "/channels/30":
			respond(map[string]any{"id": "30", "guild_id": "2", "type": 0, "name": "outside"})
		case "/guilds/1/channels":
			respond([]any{map[string]any{"id": "10", "guild_id": "1", "type": 15, "name": "forum"}, map[string]any{"id": "20", "guild_id": "1", "type": 0, "name": "text"}, map[string]any{"id": "30", "guild_id": "2", "type": 0, "name": "outside"}})
		default:
			if strings.HasSuffix(path, "/messages") {
				if r.URL.Query().Get("limit") == "1" {
					respond([]any{})
					return
				}
				respond([]any{messageJSON("103", "100", map[string]any{"content": "newer"}), messageJSON("102", "100", map[string]any{"content": "older"})})
				return
			}
			id := strings.TrimPrefix(path, "/channels/")
			parent := "10"
			if id == "200" {
				parent = "20"
			}
			respond(threadJSON(id, parent))
		}
		return
	}
	if r.Method == "DELETE" {
		w.WriteHeader(204)
		return
	}
	if onSend != nil {
		onSend()
	}
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}
	if lose {
		conn, _, _ := w.(http.Hijacker).Hijack()
		_ = conn.Close()
		return
	}
	if reject != 0 {
		w.WriteHeader(400)
		respond(map[string]any{"code": reject, "message": "sensitive upstream text fake-bot-token", "errors": map[string]any{"content": map[string]any{"_errors": []any{map[string]any{"message": "fake-bot-token", "code": "BAD"}}}}})
		return
	}
	switch path {
	case "/channels/10/threads":
		respond(map[string]any{"id": "100", "guild_id": "1", "parent_id": "10", "type": 11, "name": "post", "last_message_id": "100", "thread_metadata": map[string]any{"archived": false, "locked": false}, "message": messageJSON("100", "100", body["message"].(map[string]any))})
	case "/channels/20/messages":
		respond(messageJSON("200", "20", body))
	case "/channels/20/messages/200/threads":
		if already {
			w.WriteHeader(400)
			respond(map[string]any{"code": 160004, "message": "already has thread"})
			return
		}
		respond(threadJSON("200", "20"))
	default:
		if pollFail && body["poll"] != nil {
			w.WriteHeader(400)
			respond(map[string]any{"code": 50035, "message": "no poll"})
			return
		}
		respond(messageJSON("101", strings.Split(path, "/")[2], body))
	}
}
func (f *fakeDiscord) posts() []request {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []request{}
	for _, r := range f.requests {
		if r.method == "POST" {
			out = append(out, r)
		}
	}
	return out
}

type fixedIndex struct{}

func (fixedIndex) Position(context.Context) (*pb.StreamPosition, error) {
	return &pb.StreamPosition{Epoch: 42, Sequence: 7}, nil
}
func (fixedIndex) Completed(context.Context, string, string) (*CompletedSend, error) { return nil, nil }

type harness struct {
	h      *Handler
	ctx    context.Context
	cfg    *config.Service
	fake   *fakeDiscord
	js     jetstream.JetStream
	reg    *registry.Registry
	client pb.DeckServiceClient
}

func setup(t *testing.T) *harness {
	t.Helper()
	_, _, js := testutil.NATS(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := &config.Service{NATS: config.NATS{SubjectPrefix: "deck", Stream: config.Stream{Name: "DECK", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}}}
	if _, err := activity.EnsureStream(ctx, js, cfg.NATS); err != nil {
		t.Fatal(err)
	}
	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "threads"})
	if err != nil {
		t.Fatal(err)
	}
	reg, err := registry.New(ctx, kv, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reg.Close)
	publisher := activity.New(ctx, js, "deck")
	t.Cleanup(publisher.Close)
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	key, _ := ssh.NewPublicKey(pub)
	cfg.Keys = []config.Key{{Name: "key", PublicKey: string(ssh.MarshalAuthorizedKey(key)), Grants: []config.Grant{{Bot: "a", Server: "1", Channels: []string{"*"}}, {Bot: "b", Server: "1", Channels: []string{"20"}}}}, {Name: "other", PublicKey: string(ssh.MarshalAuthorizedKey(key)), Grants: []config.Grant{{Bot: "b", Server: "1", Channels: []string{"*"}}}}}
	fake := &fakeDiscord{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	client, err := discord.New("MTIz.fake-bot-token.signature", bot.WithRestClientConfigOpts(rest.WithURL(server.URL)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Bot.Close(context.Background()) })
	fake.mu.Lock()
	fake.requests = nil
	fake.mu.Unlock()
	h, err := NewHandler(cfg, Dependencies{Context: ctx, Bots: map[string]discord.API{"a": client, "b": client}, Registry: reg, Publisher: publisher, Index: fixedIndex{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); h.Wait() })
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(peer.NewContext(ctx, &peer.Peer{AuthInfo: transport.KeyAuthInfo{Name: "key"}}), req)
	}))
	pb.RegisterDeckServiceServer(grpcServer, h)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &harness{h, transport.WithKeyName(ctx, "key"), cfg, fake, js, reg, pb.NewDeckServiceClient(conn)}
}
func content() *pb.Content { return &pb.Content{Text: "hello"} }
func TestForumPollAndHeaders(t *testing.T) {
	s := setup(t)
	c := content()
	c.Poll = &pb.PollCreate{Question: "Choose", Answers: []string{"A", "B"}}
	r, err := s.client.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "post", Content: c, Nonce: "N1"})
	if err != nil {
		t.Fatal(err)
	}
	if r.PollMessage == nil || r.Message.Sequence != 1 || r.PollMessage.Sequence != 2 || r.Position.Epoch != 42 {
		t.Fatal(r)
	}
	posts := s.fake.posts()
	if len(posts) != 2 || posts[0].body["message"].(map[string]any)["poll"] != nil || posts[1].body["poll"] == nil {
		t.Fatal("poll was not sent second")
	}
	stream, _ := s.js.Stream(s.ctx, "DECK")
	m, _ := stream.GetMsg(s.ctx, 1)
	if m.Header.Get("Deck-Send-Start") != "42:7" || m.Header.Get("Deck-Sent-By") != "key" || m.Header.Get("Deck-Sender-Key") != s.h.fingerprints["key"] {
		t.Fatal(m.Header)
	}
	if r.Thread.Touched != true || !r.Thread.CreatedByDeck {
		t.Fatal(r.Thread)
	}
	s.fake.mu.Lock()
	s.fake.pollFail = true
	s.fake.mu.Unlock()
	r, err = s.client.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "post", Content: c, Nonce: "N2"})
	if err != nil || r.PollError == "" || r.PollMessage != nil {
		t.Fatal("missing partial poll failure", r, err)
	}
}
func TestReplyInFlightAndBoundary(t *testing.T) {
	s := setup(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	s.fake.block = release
	s.fake.entered = entered
	s.fake.onSend = func() {
		e, ok := s.reg.Lookup(100)
		if !ok || e.LastMessageID != 90 || e.Keys["key"].State != "pending" {
			t.Errorf("boundary was not persisted before send: %+v", e)
		}
	}
	req := &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "same"}
	results := make(chan *pb.ReplyResponse, 2)
	errs := make(chan error, 2)
	go func() { r, e := s.h.Reply(s.ctx, req); results <- r; errs <- e }()
	<-entered
	go func() { r, e := s.h.Reply(s.ctx, req); results <- r; errs <- e }()
	time.Sleep(40 * time.Millisecond)
	close(release)
	first, second := <-results, <-results
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if !proto.Equal(first, second) || len(s.fake.posts()) != 1 {
		t.Fatal("inflight send repeated")
	}
	posts := s.fake.posts()
	if posts[0].body["nonce"] != discord.Nonce(s.h.fingerprints["key"], "same") || posts[0].body["enforce_nonce"] != true {
		t.Fatal("Discord nonce was not enforced")
	}
}
func TestLostAnswersKeepReplyTouch(t *testing.T) {
	for _, kind := range []string{"reply", "forum", "text"} {
		t.Run(kind, func(t *testing.T) {
			s := setup(t)
			s.fake.lose = true
			var err error
			if kind == "reply" {
				_, err = s.h.Reply(s.ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "lost"})
			} else {
				id := uint64(10)
				if kind == "text" {
					id = 20
				}
				_, err = s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: id}, Title: "lost", Content: content(), Nonce: "lost"})
			}
			if status.Code(err) != codes.Unavailable {
				t.Fatal(err)
			}
			if strings.Contains(err.Error(), "fake-bot-token") {
				t.Fatal("secret leaked")
			}
			if kind == "reply" {
				e, ok := s.reg.Lookup(100)
				if !ok || e.Keys["key"].State != "pending" {
					t.Fatal("unknown outcome removed touch")
				}
			}
			if len(s.fake.posts()) != 1 {
				t.Fatal("lost answer caused automatic repeat")
			}
			s.fake.mu.Lock()
			defer s.fake.mu.Unlock()
			for _, r := range s.fake.requests {
				if kind == "forum" && r.method == "GET" && r.path != "/channels/10" {
					t.Fatal("lost forum searched Discord", r.path)
				}
			}
		})
	}
}
func TestExistingTextThread(t *testing.T) {
	s := setup(t)
	s.fake.already = true
	r, err := s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 20}, Title: "title", Content: content(), Nonce: "existing"})
	if err != nil || r.Thread.ThreadId != 200 {
		t.Fatal(r, err)
	}
	s.fake.mu.Lock()
	defer s.fake.mu.Unlock()
	for _, r := range s.fake.requests {
		if r.method == "DELETE" {
			t.Fatal("existing message deleted")
		}
	}
}
func TestRegistryUnavailablePreventsPost(t *testing.T) {
	s := setup(t)
	if err := s.js.DeleteKeyValue(s.ctx, "threads"); err != nil {
		t.Fatal(err)
	}
	_, err := s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "title", Content: content(), Nonce: "bucket"})
	if status.Code(err) != codes.Unavailable || len(s.fake.posts()) != 0 {
		t.Fatal("posted without registry", err)
	}
}
func TestGrantOrderAndThreadState(t *testing.T) {
	s := setup(t)
	e := registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10, LastMessageID: 90}
	if err := s.reg.RegisterPost(100, e, "key"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		key, bot string
		id       uint64
		code     codes.Code
	}{{"key", "b", 100, codes.InvalidArgument}, {"other", "b", 100, codes.PermissionDenied}, {"key", "", 100, codes.OK}, {"key", "", 101, codes.InvalidArgument}, {"key", "", 30, codes.InvalidArgument}, {"key", "a", 30, codes.PermissionDenied}, {"key", "forbidden", 100, codes.InvalidArgument}, {"key", "forbidden", 101, codes.PermissionDenied}, {"", "a", 100, codes.Unauthenticated}}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%s/%d", tc.key, tc.bot, tc.id), func(t *testing.T) {
			_, err := s.h.GetThread(transport.WithKeyName(context.Background(), tc.key), &pb.GetThreadRequest{Thread: &pb.ThreadRef{Bot: tc.bot, ThreadId: tc.id}})
			if status.Code(err) != tc.code {
				t.Fatalf("got %v want %v", err, tc.code)
			}
		})
	}
	if s.h.State(transport.WithKeyName(context.Background(), "other"), 100) != pb.ThreadState_THREAD_STATE_NOT_TOUCHED {
		t.Fatal("other key's thread disclosed")
	}
	e, _ = s.reg.Lookup(100)
	if err := s.reg.RegisterPost(100, e, "other"); err != nil {
		t.Fatal(err)
	}
	if s.h.State(transport.WithKeyName(context.Background(), "other"), 100) != pb.ThreadState_THREAD_STATE_NOT_GRANTED {
		t.Fatal("missing grant state")
	}
	before := len(s.fake.requests)
	_, err := s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "forbidden", ChannelId: 10}})
	if status.Code(err) != codes.PermissionDenied || len(s.fake.requests) != before {
		t.Fatal("ungranted bot reached Discord")
	}
}
func TestHistoryAndChannels(t *testing.T) {
	s := setup(t)
	channels, err := s.client.ListChannels(s.ctx, &pb.ListChannelsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(channels.Channels) != 3 {
		t.Fatal(channels)
	}
	for _, ch := range channels.Channels {
		if ch.ServerId != 1 {
			t.Fatal("cross-server wildcard leaked")
		}
	}
	r, err := s.client.ListHistory(s.ctx, &pb.ListHistoryRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, AfterId: 100, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if r.More || len(r.Activity) != 2 || r.Activity[0].Id != 102 || r.Activity[1].Id != 103 {
		t.Fatal(r)
	}
	s.fake.mu.Lock()
	defer s.fake.mu.Unlock()
	found := false
	for _, r := range s.fake.requests {
		if strings.Contains(r.path, "after=100") {
			found = true
		}
	}
	if !found {
		t.Fatal("after cursor not sent to Discord")
	}
}
func TestKnownFailureRemovesFirstTouch(t *testing.T) {
	s := setup(t)
	s.fake.reject = 50035
	_, err := s.client.Reply(s.ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "fail"})
	if status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "fake-bot-token") || !strings.Contains(err.Error(), "content") {
		t.Fatal(err)
	}
	if _, ok := s.reg.Lookup(100); ok {
		t.Fatal("failed first reply touched thread")
	}
}
func TestContentValidationAndConversion(t *testing.T) {
	embed, _ := structpb.NewStruct(map[string]any{"title": "extra"})
	component, _ := structpb.NewStruct(map[string]any{"type": 1, "components": []any{map[string]any{"type": 2, "style": 1, "label": "Press", "custom_id": "button"}}})
	c := &pb.Content{Text: "persona text", MentionUserIds: []uint64{7}, MentionRoleIds: []uint64{8}, Persona: &pb.Persona{Name: "Persona", IconUrl: "https://example.com/icon.png"}, Embeds: []*structpb.Struct{embed}, Components: []*structpb.Struct{component}, Files: []*pb.File{{Name: "test.txt", Data: []byte("data"), Description: "file"}}}
	m, err := discord.Build(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.Content != "<@7> <@&8>" || m.Embeds[0].Description != "persona text" || m.Embeds[0].Author.Name != "Persona" || len(m.Files) != 1 || len(m.Components) != 1 || len(m.AllowedMentions.Parse) != 0 || m.AllowedMentions.RepliedUser {
		t.Fatal("conversion lost content or mention controls")
	}
	for _, bad := range []*pb.Content{nil, {Text: strings.Repeat("x", 2001)}, {Text: "hi", Poll: &pb.PollCreate{Question: "?"}}, {Text: "hi", Files: []*pb.File{{Name: ""}}}} {
		if _, err := discord.Build(bad); status.Code(err) != codes.InvalidArgument {
			t.Fatal("invalid content accepted", err)
		}
	}
}

func TestPollOnlyForumStarter(t *testing.T) {
	s := setup(t)
	r, err := s.h.Post(s.ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "Poll title", Content: &pb.Content{Poll: &pb.PollCreate{Question: "?", Answers: []string{"yes"}}}, Nonce: "pollonly"})
	if err != nil || r.PollMessage == nil {
		t.Fatal(r, err)
	}
	if got := s.fake.posts()[0].body["message"].(map[string]any)["content"]; got != "Poll title" {
		t.Fatal("poll-only forum starter was empty")
	}
}
func TestCallerCancellationDoesNotCancelSend(t *testing.T) {
	s := setup(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	s.fake.block = release
	s.fake.entered = entered
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := s.h.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "cancel"})
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	close(release)
	testutil.Until(t, func() bool {
		stream, err := s.js.Stream(s.ctx, "DECK")
		if err != nil {
			return false
		}
		info, err := stream.Info(s.ctx)
		return err == nil && info.State.Msgs == 1
	})
	e, _ := s.reg.Lookup(100)
	if e.Keys["key"].State != "sent" {
		t.Fatal("client cancellation cancelled the send")
	}
}
