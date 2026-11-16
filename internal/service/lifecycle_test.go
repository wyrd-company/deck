//go:build !race

// DISGO-WORKAROUND(gateway-close-race): excluded from -race builds for disgo's gateway Close data race; see wyrd-company/deck issue (pending).
// The race detector reports a data race inside disgo v0.20.0 itself when a
// gateway session closes (gateway/gateway.go:360 against :491-492), and these
// tests open and close real sessions against a fake gateway. They run in
// every ordinary `go test`, including task ci.

package service

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/health"
	"github.com/wyrd-company/deck/internal/service/registry"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"github.com/wyrd-company/deck/internal/transport"
	"golang.org/x/crypto/ssh"
)

type lifecycle struct {
	server   *server.Server
	runtime  *Runtime
	admin    jetstream.JetStream
	fake     *fakeDiscord
	gateways map[string]*testutil.Gateway
	cfg      *config.Service
}

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// openRuntime opens the production runtime and its gateway sessions against
// JWT NATS, a fake Discord REST API and a fake gateway.
func openRuntime(t *testing.T) *lifecycle {
	t.Helper()
	natsServer, credentials := testutil.JWTNATS(t)
	l := &lifecycle{server: natsServer, fake: &fakeDiscord{}}
	upstream := httptest.NewServer(l.fake)
	t.Cleanup(upstream.Close)
	options, gateways := fakeBotGateways(t, upstream.URL)
	l.gateways = gateways
	l.cfg = &config.Service{
		Listen: config.Listen{SSH: freeAddress(t), Health: freeAddress(t)},
		NATS:   config.NATS{Servers: []string{natsServer.ClientURL()}, Timeout: time.Second, SubjectPrefix: "deck", Stream: config.Stream{Name: "DECK", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}, Threads: config.Threads{Bucket: "threads", Manage: true, Replicas: 1}},
		Bots:   []config.Bot{{Name: "a"}},
		Keys:   []config.Key{{Name: "key", PublicKey: newAuthorizedKey(t), Grants: []config.Grant{{Bot: "a", Server: "1", Channels: []string{"*"}}}}},
	}
	secrets := &startupSecrets{credentials: credentials}
	r, err := open(context.Background(), l.cfg, secrets, func(name, token string) (*discord.Client, error) {
		return discord.New(token, options(name)...)
	})
	if err != nil {
		t.Fatal(err)
	}
	l.runtime = r
	t.Cleanup(r.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err = r.OpenGateways(ctx); err != nil {
		t.Fatal(err)
	}
	admin, err := nats.Connect(natsServer.ClientURL(), nats.UserCredentialBytes([]byte(credentials)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if l.admin, err = jetstream.New(admin); err != nil {
		t.Fatal(err)
	}
	return l
}

func newAuthorizedKey(t *testing.T) string {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	return string(ssh.MarshalAuthorizedKey(key))
}

func (l *lifecycle) streamIDs(t *testing.T) map[uint64]bool {
	t.Helper()
	ctx := context.Background()
	s, err := l.admin.Stream(ctx, "DECK")
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[uint64]bool{}
	for seq := info.State.FirstSeq; seq > 0 && seq <= info.State.LastSeq; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		if n, err := strconv.ParseUint(m.Header.Get("Deck-Activity-Id"), 10, 64); err == nil {
			ids[n] = true
		}
	}
	return ids
}

// Shutdown lets a process-owned send finish before closing the publisher,
// registry and NATS: removing Handler.Wait from Close fails this test.
func TestCloseFinishesSendsBeforeResources(t *testing.T) {
	l := openRuntime(t)
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	l.fake.mu.Lock()
	l.fake.block, l.fake.entered = release, entered
	l.fake.mu.Unlock()
	ctx := transport.WithKeyName(context.Background(), "key")
	replied := make(chan error, 1)
	go func() {
		_, err := l.runtime.Handler.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 201}, Content: content(), Nonce: "closing"})
		replied <- err
	}()
	<-entered
	closed := make(chan struct{})
	go func() { l.runtime.Close(); close(closed) }()
	time.Sleep(200 * time.Millisecond) // Close is under way
	close(release)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close exceeded its budget")
	}
	select {
	case err := <-replied:
		if err != nil {
			t.Fatal("send canceled by shutdown", err)
		}
	case <-time.After(5 * time.Second):
		// disgo's REST client deadlocks a request still in flight when it is
		// closed, so a send outliving the bot clients never returns.
		// DISGO-WORKAROUND(rest-close-deadlock): bounds the wait on a request that closing the REST client deadlocks; see wyrd-company/deck issue (pending).
		t.Fatal("the send never finished: resources closed under it")
	}
	if !l.streamIDs(t)[101] {
		t.Fatal("the send's message never reached the stream")
	}
	if e := durableEntry(t, l.admin, 201); e.Keys["key"].State != "sent" {
		t.Fatal("the send's touch never reached the bucket", e)
	}
}

func durableEntry(t *testing.T, js jetstream.JetStream, id uint64) registry.Entry {
	t.Helper()
	kv, err := js.KeyValue(context.Background(), "threads")
	if err != nil {
		t.Fatal(err)
	}
	v, err := kv.Get(context.Background(), "threads."+strconv.FormatUint(id, 10))
	if err != nil {
		t.Fatal(err)
	}
	var e registry.Entry
	if err = json.Unmarshal(v.Value(), &e); err != nil {
		t.Fatal(err)
	}
	return e
}

// Shutdown publishes waiting activity and writes dirty registry entries
// before it closes NATS, when NATS is healthy.
func TestClosePublishesAndFlushes(t *testing.T) {
	l := openRuntime(t)
	r := l.runtime
	if err := r.Registry.RegisterPost(300, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10, CreatedByDeck: true, LastMessageID: 300}, "key"); err != nil {
		t.Fatal(err)
	}
	for n := range 300 {
		if _, err := r.Publisher.Submit(person(300, uint64(5000+n), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	r.Registry.Advance(300, 777)
	r.Close()
	if !r.NATS.IsClosed() {
		t.Fatal("NATS not closed")
	}
	ids := l.streamIDs(t)
	for n := range 300 {
		if !ids[uint64(5000+n)] {
			t.Fatalf("activity %d lost at shutdown", 5000+n)
		}
	}
	if e := durableEntry(t, l.admin, 300); e.LastMessageID != 777 {
		t.Fatal("dirty boundary lost at shutdown", e.LastMessageID)
	}
}

// An outage cannot hold shutdown: with NATS gone, waiting activity and dirty
// entries are abandoned at their budgets and Close ends within 10 seconds.
func TestCloseBoundedDuringOutage(t *testing.T) {
	l := openRuntime(t)
	r := l.runtime
	_ = r.Registry.RegisterPost(300, registry.Entry{Bot: "a", ServerID: 1, ChannelID: 10, CreatedByDeck: true}, "key")
	l.server.Shutdown()
	l.server.WaitForShutdown()
	r.Registry.Advance(300, 900)
	_, _ = r.Publisher.Submit(person(300, 9000, 0), nil)
	started := time.Now()
	r.Close()
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatal("shutdown during an outage took", elapsed)
	}
}

// The healthcheck fails while a gateway session or NATS is disconnected and
// recovers with them; `deck service health` reads the same listener.
func TestHealthFollowsGatewayAndNATS(t *testing.T) {
	l := openRuntime(t)
	probe, err := health.Listen(l.cfg.Listen.Health, l.runtime.Checks())
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close(context.Background())
	state := func() (int, map[string]any) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		code, raw, err := health.Probe(ctx, probe.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		return code, body
	}
	testutil.Until(t, func() bool { code, _ := state(); return code == http.StatusOK })
	gw := l.gateways["a"]
	gw.Refuse(true)
	gw.Drop()
	testutil.Until(t, func() bool {
		code, body := state()
		return code == http.StatusServiceUnavailable && body["bots"].(map[string]any)["a"] == "disconnected" && body["nats"] == "connected"
	})
	gw.Refuse(false)
	deadline := time.Now().Add(15 * time.Second)
	for code, _ := state(); code != http.StatusOK; code, _ = state() {
		if time.Now().After(deadline) {
			t.Fatal("health did not recover with the gateway")
		}
		time.Sleep(100 * time.Millisecond)
	}
	l.server.Shutdown()
	l.server.WaitForShutdown()
	testutil.Until(t, func() bool {
		code, body := state()
		return code == http.StatusServiceUnavailable && body["nats"] != "connected" && body["index"] != "ready"
	})
}
