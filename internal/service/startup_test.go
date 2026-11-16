//go:build !race

// DISGO-WORKAROUND(gateway-close-race): excluded from -race builds for disgo's gateway Close data race; see wyrd-company/deck#15 (fixed upstream by disgoorg/disgo#559, unreleased).
// The race detector reports a data race inside disgo v0.20.0 itself when a
// gateway session closes (gateway/gateway.go:360 against :491-492), and these
// tests open and close real sessions against a fake gateway. They run in
// every ordinary `go test`, including task ci.

package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
)

type shutdownFixture struct{ Address, HostPEM, KeyPEM string }
type startupSecrets struct {
	credentials, host string
	reads             []string
}

func (s *startupSecrets) ReadSecret(_ context.Context, where string, _ config.SecretRef) (string, error) {
	s.reads = append(s.reads, where)
	switch where {
	case "nats.creds":
		return s.credentials, nil
	case "bots[0].token":
		return "MTIz.test-only.signature", nil
	case "host_key":
		fmt.Println("HOSTREAD")
		return s.host, nil
	default:
		return "", fmt.Errorf("unexpected test secret reference")
	}
}

// shutdownHandler runs the production Listen and checks, as it returns, that
// the transport ended the call before the runtime was canceled.
type shutdownHandler struct {
	*Handler
	t *testing.T
}

func (h *shutdownHandler) Listen(r *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
	err := h.Handler.Listen(r, stream)
	if stream.Context().Err() == nil {
		h.t.Error("Listen returned before its call ended")
	}
	select {
	case <-h.ctx.Done():
		h.t.Error("runtime canceled before transport joined Listen")
	case <-time.After(25 * time.Millisecond):
	}
	return err
}

// Run in a subprocess: an absent SIGTERM handler must kill that process rather
// than the parent runner. Without the marker this helper simply returns.
func TestServiceSIGTERMHelper(t *testing.T) {
	path := os.Getenv("DECK_SIGTERM_FIXTURE")
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture shutdownFixture
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	key, err := ssh.ParsePrivateKey([]byte(fixture.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	natsServer, credentials := testutil.JWTNATS(t)
	secrets := &startupSecrets{credentials: credentials, host: fixture.HostPEM}
	restCanceled := make(chan struct{})
	fake := &fakeDiscord{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/messages") {
			_, _ = io.Copy(io.Discard, r.Body)
			fmt.Println("INFLIGHT")
			<-r.Context().Done()
			close(restCanceled)
			return
		}
		fake.ServeHTTP(w, r)
	}))
	defer upstream.Close()
	cfg := &config.Service{
		Listen: config.Listen{SSH: fixture.Address},
		NATS:   config.NATS{Servers: []string{natsServer.ClientURL()}, Timeout: time.Second, SubjectPrefix: "deck", Stream: config.Stream{Name: "activity", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}, Threads: config.Threads{Bucket: "threads", Manage: true, Replicas: 1}},
		Bots:   []config.Bot{{Name: "a"}},
		Keys:   []config.Key{{Name: "test", PublicKey: string(ssh.MarshalAuthorizedKey(key.PublicKey())), Grants: []config.Grant{{Bot: "a", Server: "1", Channels: []string{"10"}}}}},
	}
	var runtime *Runtime
	defer func() {
		if runtime != nil {
			runtime.Close()
		}
	}() // release resources even under adapter mutations
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err = run(ctx, cfg, secrets, func(ctx context.Context) (*Runtime, pb.DeckServiceServer, error) {
		options := fakeBots(t, upstream.URL)
		r, err := open(ctx, cfg, secrets, func(name, token string) (*discord.Client, error) {
			return discord.New(token, options(name)...)
		})
		if err != nil {
			return nil, nil, err
		}
		runtime = r
		return r, &shutdownHandler{Handler: r.Handler, t: t}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || !runtime.NATS.IsClosed() || runtime.Handler.ctx.Err() == nil {
		t.Fatal("adapter returned without closing Runtime")
	}
	select {
	case <-restCanceled:
	case <-time.After(time.Second):
		t.Fatal("detached REST send not canceled by Runtime.Close")
	}
	if !slices.Equal(secrets.reads, []string{"nats.creds", "bots[0].token", "host_key"}) {
		t.Fatal("startup reads not serial/in order", secrets.reads)
	}
}

func TestServiceSIGTERM(t *testing.T) {
	host, hostPEM := sshMaterial(t)
	key, keyPEM := sshMaterial(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	fixture := filepath.Join(t.TempDir(), "startup.json")
	raw, err := json.Marshal(shutdownFixture{address, hostPEM, keyPEM})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(fixture, raw, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestServiceSIGTERMHelper$", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "DECK_SIGTERM_FIXTURE="+fixture)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 2)
	var transcript bytes.Buffer
	scanned := make(chan struct{})
	go func() {
		defer close(scanned)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(&transcript, line)
			if line == "HOSTREAD" || line == "INFLIGHT" {
				lines <- line
			}
		}
		close(lines)
	}()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	reaped := false
	t.Cleanup(func() {
		if !reaped {
			_ = cmd.Process.Kill()
			<-done
		}
		<-scanned
	})
	waitMarker := func(want string) {
		t.Helper()
		deadline := time.NewTimer(8 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatalf("child closed before %s", want)
				}
				if line == want {
					return
				}
			case <-deadline.C:
				t.Fatalf("child did not reach %s", want)
			}
		}
	}
	waitMarker("HOSTREAD")
	testutil.Until(t, func() bool {
		c, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	})
	client := dialSSHHandler(t, address, host.PublicKey(), key)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	reply := make(chan error, 1)
	go func() {
		_, err := client.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "shutdown"})
		reply <- err
	}()
	waitMarker("INFLIGHT")
	// The in-flight reply touched thread 100, so the production Listen keeps
	// its stream open on it.
	stream, err := client.Listen(ctx, &pb.ListenRequest{Cursors: []*pb.Cursor{{ThreadId: 100}}})
	if err != nil {
		t.Fatal(err)
	}
	if first, err := stream.Recv(); err != nil || first.Threads[0].State != pb.ThreadState_THREAD_STATE_TOUCHED {
		t.Fatal("Listen did not open on the touched thread", first, err)
	}
	started := time.Now()
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		reaped = true
		if err != nil {
			<-scanned
			t.Fatalf("SIGTERM shutdown: %v; %s; %s", err, transcript.String(), stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SIGTERM shutdown exceeded 10 seconds")
	}
	if _, err = stream.Recv(); err == nil {
		t.Fatal("Listen remained open after shutdown")
	}
	if err = <-reply; err == nil {
		t.Fatal("in-flight Reply did not terminate")
	}
	t.Logf("SIGTERM joined open Listen and in-flight Reply, closed runtime in %s", time.Since(started))
}

// Startup reads must retain cancellation even though the serving runtime is
// detached later so transport can join handlers first.
type blockingStartupSecret struct {
	entered     chan struct{}
	block       string
	credentials string
}

func (s blockingStartupSecret) ReadSecret(ctx context.Context, where string, _ config.SecretRef) (string, error) {
	if where != s.block {
		return s.credentials, nil
	}
	close(s.entered)
	<-ctx.Done()
	return "", ctx.Err()
}

// fakeBots points every bot client at a fake REST server and a fake gateway
// per bot, so startup can open sessions without Discord.
func fakeBots(t *testing.T, restURL string) func(name string) []bot.ConfigOpt {
	options, _ := fakeBotGateways(t, restURL)
	return options
}

// fakeBotGateways also returns the fake gateways, created on first use.
func fakeBotGateways(t *testing.T, restURL string) (func(name string) []bot.ConfigOpt, map[string]*testutil.Gateway) {
	t.Helper()
	gateways := map[string]*testutil.Gateway{}
	return func(name string) []bot.ConfigOpt {
		if gateways[name] == nil {
			gateways[name] = testutil.NewGateway(t, "123", testutil.Guild())
		}
		return []bot.ConfigOpt{bot.WithRestClientConfigOpts(rest.WithURL(restURL)), bot.WithGatewayConfigOpts(gateway.WithURL(gateways[name].URL), gateway.WithCompression(gateway.CompressionNone))}
	}, gateways
}

// useFakeBots installs fakeBots as the production bot options for a test.
func useFakeBots(t *testing.T) {
	t.Helper()
	upstream := httptest.NewServer(&fakeDiscord{})
	t.Cleanup(upstream.Close)
	BotOptions = fakeBots(t, upstream.URL)
	t.Cleanup(func() { BotOptions = nil })
}

func TestServiceStartupCancellation(t *testing.T) {
	useFakeBots(t)
	natsServer, credentials := testutil.JWTNATS(t)
	for _, where := range []string{"nats.creds", "bots[0].token", "host_key"} {
		t.Run(where, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// Each secret has a distinct ref in the CLI test; this reader targets the
			// logical step so dropping a context on any single call is observable.
			secrets := blockingStartupSecret{entered: make(chan struct{}), block: where, credentials: credentials}
			cfg := &config.Service{Listen: config.Listen{SSH: "127.0.0.1:0"}, NATS: config.NATS{Servers: []string{natsServer.ClientURL()}, Timeout: time.Second, SubjectPrefix: "deck", Stream: config.Stream{Name: "cancel", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}, Threads: config.Threads{Bucket: "cancel", Manage: true, Replicas: 1}}, Bots: []config.Bot{{Name: "a"}}}
			// Host-key cancellation needs a valid bot token to finish Open.
			reader := startupSecretFunc(func(ctx context.Context, step string, ref config.SecretRef) (string, error) {
				if step == "bots[0].token" && where == "host_key" {
					return "MTIz.test-only.signature", nil
				}
				return secrets.ReadSecret(ctx, step, ref)
			})
			done := make(chan error, 1)
			go func() { done <- Run(ctx, cfg, reader) }()
			select {
			case <-secrets.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("startup secret read did not start")
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("canceled startup succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("startup read ignored cancellation")
			}
		})
	}
}

type startupSecretFunc func(context.Context, string, config.SecretRef) (string, error)

func (f startupSecretFunc) ReadSecret(ctx context.Context, where string, ref config.SecretRef) (string, error) {
	return f(ctx, where, ref)
}

// Exercise Run -> Open -> startupFailure, not vault.New. The secret reader
// returns a genuine failure only after observing the stop; that failure must
// stay distinct from cancellation and must not expose its upstream body.
func TestServiceStartupIndependentFailure(t *testing.T) {
	useFakeBots(t)
	natsServer, credentials := testutil.JWTNATS(t)
	for _, where := range []string{"nats.creds", "bots[0].token"} {
		for _, stop := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stop=%t", where, stop), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				entered := make(chan struct{})
				reader := startupSecretFunc(func(ctx context.Context, step string, _ config.SecretRef) (string, error) {
					if step != where {
						return credentials, nil
					}
					close(entered)
					if stop {
						<-ctx.Done()
					}
					return "", errors.New("private upstream secret body")
				})
				cfg := &config.Service{Listen: config.Listen{SSH: "127.0.0.1:0"}, NATS: config.NATS{Servers: []string{natsServer.ClientURL()}, Timeout: time.Second, SubjectPrefix: "deck", Stream: config.Stream{Name: "failure", Manage: true, MaxAge: time.Hour, DuplicateWindow: time.Minute, Replicas: 1}, Threads: config.Threads{Bucket: "failure", Manage: true, Replicas: 1}}, Bots: []config.Bot{{Name: "a"}}}
				done := make(chan error, 1)
				go func() { done <- Run(ctx, cfg, reader) }()
				select {
				case <-entered:
				case <-time.After(2 * time.Second):
					t.Fatal("startup did not reach failing read")
				}
				if stop {
					cancel()
				}
				select {
				case err := <-done:
					if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("independent failure misclassified: %v", err)
					}
					if want := where + ": cannot read configured secret"; err.Error() != want {
						t.Fatalf("unsafe/wrong startup message: %q; want %q", err, want)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("startup failed to return its real error")
				}
			})
		}
	}
}
