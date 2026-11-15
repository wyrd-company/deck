package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/disgoorg/disgo/bot"
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

// Task 7 owns Listen. This wrapper supplies an open stream while all unary
// calls, SSH authentication, startup and runtime teardown use production code.
type shutdownHandler struct {
	*Handler
	t *testing.T
}

func (h *shutdownHandler) Listen(_ *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
	if err := stream.Send(&pb.ListenResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	select {
	case <-h.ctx.Done():
		h.t.Error("runtime canceled before transport joined Listen")
	case <-time.After(25 * time.Millisecond):
	}
	return stream.Context().Err()
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
	err = run(context.Background(), cfg, secrets, func(ctx context.Context) (*Runtime, pb.DeckServiceServer, error) {
		r, err := open(ctx, cfg, secrets, func(token string) (*discord.Client, error) {
			return discord.New(token, bot.WithRestClientConfigOpts(rest.WithURL(upstream.URL)))
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
	stream, err := client.Listen(ctx, &pb.ListenRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal("Listen did not open", err)
	}
	reply := make(chan error, 1)
	go func() {
		_, err := client.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: "a", ThreadId: 100}, Content: content(), Nonce: "shutdown"})
		reply <- err
	}()
	waitMarker("INFLIGHT")
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
type blockingStartupSecret struct{ entered chan struct{} }

func (s blockingStartupSecret) ReadSecret(ctx context.Context, _ string, _ config.SecretRef) (string, error) {
	close(s.entered)
	<-ctx.Done()
	return "", ctx.Err()
}
func TestServiceStartupCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	secrets := blockingStartupSecret{entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, &config.Service{}, secrets) }()
	select {
	case <-secrets.entered:
	case <-time.After(time.Second):
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
}
