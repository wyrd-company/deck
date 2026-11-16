//go:build !race

// DISGO-WORKAROUND(gateway-close-race): excluded from -race builds for disgo's gateway Close data race; see wyrd-company/deck#15 (fixed upstream by disgoorg/disgo#559, unreleased).
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
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lifecycle struct {
	server   *server.Server
	runtime  *Runtime
	admin    jetstream.JetStream
	fake     *fakeDiscord
	gateways map[string]*testutil.Gateway
	cfg      *config.Service
	open     func() (*Runtime, error)
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
	// open runs the production startup steps once more: a restart.
	l.open = func() (*Runtime, error) {
		r, err := open(context.Background(), l.cfg, secrets, func(name, token string) (*discord.Client, error) {
			return discord.New(token, options(name)...)
		})
		if err != nil {
			return nil, err
		}
		t.Cleanup(r.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return r, r.OpenGateways(ctx)
	}
	r, err := l.open()
	if err != nil {
		t.Fatal(err)
	}
	l.runtime = r
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
		// DISGO-WORKAROUND(rest-close-deadlock): bounds the wait on a request that closing the REST client deadlocks; see wyrd-company/deck#16 (upstream disgoorg/disgo#661).
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

// stopThenFail cancels the caller's context, then fails Open for a reason of
// its own, as an authentication failure coinciding with a stop would.
type stopThenFail struct {
	gateway.Gateway
	stop    context.CancelFunc
	failure error
}

func (g stopThenFail) Open(ctx context.Context) error {
	g.stop()
	if g.failure == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return g.failure
}

// R1-03: a gateway failure that coincides with a stop stays a failure (exit
// 1), and only a failure the stop caused reads as cancellation (exit 0).
func TestGatewayFailureDuringStop(t *testing.T) {
	for _, independent := range []bool{true, false} {
		t.Run(fmt.Sprint("independent=", independent), func(t *testing.T) {
			s := setupIndexed(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := s.h.bots["a"].(*discord.Client)
			previous := client.Bot.Gateway
			var failure error
			if independent {
				failure = errors.New("independent gateway authentication failure")
			}
			client.Bot.Gateway = stopThenFail{Gateway: previous, stop: cancel, failure: failure}
			defer func() { client.Bot.Gateway = previous }()
			g := discord.NewGateway(context.Background(), "a", client, s.reg, s.h.Publisher, s.h.Activity, quiet())
			defer func() { g.Close(context.Background()); g.Wait() }()
			r := &Runtime{names: []string{"a"}, Gateways: map[string]*discord.Gateway{"a": g}}
			err := r.OpenGateways(ctx)
			var f *Failure
			if !errors.As(err, &f) || f.Field != "bots[0]" || f.Step != "open gateway sessions" {
				t.Fatalf("gateway failure not reported as a startup failure: %v", err)
			}
			if errors.Is(err, context.Canceled) == independent {
				t.Fatalf("independent=%t, cancellation identity=%t: %v", independent, errors.Is(err, context.Canceled), err)
			}
		})
	}
}

// heldAck holds the component acknowledgement until released, ignoring its
// context, and records whether the REST client was closed while it was
// still in flight.
type heldAck struct {
	rest.Rest
	entered, release chan struct{}
	active           atomic.Bool
	closedActive     atomic.Bool
}

func (h *heldAck) CreateInteractionResponse(snowflake.ID, string, d.InteractionResponse, ...rest.RequestOpt) error {
	h.active.Store(true)
	defer h.active.Store(false)
	close(h.entered)
	<-h.release
	return nil
}
func (h *heldAck) Close(ctx context.Context) {
	h.closedActive.Store(h.active.Load())
	h.Rest.Close(ctx)
}

// R1-04: shutdown joins a component acknowledgement in flight before it
// closes the publisher and the bot's REST client. One that ends within the
// budget is published; a real request that does not end is canceled at the
// budget and its use still published; one that ignores cancellation leaves
// its bot's REST client open rather than closed under it.
func TestCloseJoinsComponentAcknowledgements(t *testing.T) {
	for _, mode := range []string{"released", "canceled", "stubborn"} {
		t.Run(mode, func(t *testing.T) {
			l := openRuntime(t)
			r := l.runtime
			registerBot(t, r.Registry, 100, 10, "a", 100)
			client := r.Bots["a"].Bot
			entered := make(chan struct{})
			var ack *heldAck
			if mode == "canceled" {
				block := make(chan struct{})
				t.Cleanup(func() { close(block) })
				l.fake.mu.Lock()
				l.fake.interactionBlock, l.fake.interactionEntered = block, entered
				l.fake.mu.Unlock()
			} else {
				ack = &heldAck{Rest: client.Rest, entered: entered, release: make(chan struct{})}
				client.Rest = ack
				t.Cleanup(func() {
					select {
					case <-ack.release:
					default:
						close(ack.release)
					}
				})
			}
			l.gateways["a"].Dispatch("INTERACTION_CREATE", componentEvent(5001, 100, 1000, "123"))
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the acknowledgement never started")
			}
			closed := make(chan struct{})
			go func() { r.Close(); close(closed) }()
			if mode == "released" {
				time.Sleep(300 * time.Millisecond) // Close is waiting for the callback
				close(ack.release)
			}
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				t.Fatal("Close exceeded its budget")
			}
			if ack != nil && ack.closedActive.Load() {
				t.Fatal("the REST client closed while the acknowledgement was in flight")
			}
			if mode != "stubborn" && !l.streamIDs(t)[5001] {
				t.Fatal("the component use was not published before the publisher closed")
			}
		})
	}
}

// A stream deleted while the service runs leaves it unavailable, with the
// healthcheck naming the missing stream and calls answering UNAVAILABLE,
// until a restart creates the stream at startup (R1-01 ruling).
func TestDeletedStreamUntilRestart(t *testing.T) {
	l := openRuntime(t)
	r := l.runtime
	probe, err := health.Listen(l.cfg.Listen.Health, r.Checks())
	if err != nil {
		t.Fatal(err)
	}
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
	registerBot(t, r.Registry, 100, 10, "a", 100)
	if err = l.admin.DeleteStream(context.Background(), "DECK"); err != nil {
		t.Fatal(err)
	}
	testutil.Until(t, func() bool {
		code, body := state()
		return code == http.StatusServiceUnavailable && body["index"] == "missing"
	})
	call, cancel := context.WithTimeout(transport.WithKeyName(context.Background(), "key"), 300*time.Millisecond)
	_, err = r.Handler.CountUnread(call, &pb.CountUnreadRequest{Cursors: []*pb.Cursor{{ThreadId: 100}}})
	cancel()
	if status.Code(err) != codes.Unavailable {
		t.Fatal("a call answered without the stream", err)
	}
	if _, err = l.admin.Stream(context.Background(), "DECK"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal("the running service recreated the stream", err)
	}
	probe.Close(context.Background())
	r.Close()
	restarted, err := l.open()
	if err != nil {
		t.Fatal("restart failed", err)
	}
	if _, err = l.admin.Stream(context.Background(), "DECK"); err != nil {
		t.Fatal("startup did not create the missing stream", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = restarted.Index.Position(ctx); err != nil {
		t.Fatal("the restarted index did not open", err)
	}
}
