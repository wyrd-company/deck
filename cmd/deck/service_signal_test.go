//go:build !race

// DISGO-WORKAROUND(gateway-close-race): excluded from -race builds for disgo's gateway Close data race; see wyrd-company/deck issue (pending).
// The race detector reports a data race inside disgo v0.20.0 itself when a
// gateway session closes (gateway/gateway.go:360 against :491-492), and these
// tests open and close real sessions against a fake gateway. They run in
// every ordinary `go test`, including task ci.

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/rest"
	"github.com/wyrd-company/deck/internal/service"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"github.com/wyrd-company/deck/internal/transport"
	"golang.org/x/crypto/ssh"
)

// Real CLI dispatch, Vault HTTP/KV and JWT NATS; only generated test material
// is used. A subprocess contains native signals if signal handling regresses.
func TestServiceStartSignalHelper(t *testing.T) {
	stage := os.Getenv("DECK_START_SIGNAL_STAGE")
	if stage == "" {
		return
	}
	natsServer, credentials := testutil.JWTNATS(t)
	_, hostPrivate, keyErr := ed25519.GenerateKey(rand.Reader)
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	hostBlock, keyErr := ssh.MarshalPrivateKey(hostPrivate, "")
	if keyErr != nil {
		t.Fatal(keyErr)
	}
	hostKey := string(pem.EncodeToMemory(hostBlock))
	target := map[string]string{"login": "/v1/auth/approle/login", "nats": "/v1/kv/data/deck/nats", "bot": "/v1/kv/data/deck/bots/helper", "host": "/v1/kv/data/deck/service", "second": "/v1/kv/data/deck/nats", "failure": "/unused", "registry": "/unused", "stream": "/unused", "bind": "/unused"}[stage]
	if target == "" {
		t.Fatal("invalid stage")
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == target {
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Error(err)
			}
			<-r.Context().Done()
			return
		}
		if stage == "second" && r.URL.Path == "/v1/auth/token/revoke-self" {
			fmt.Println("REVOKING")
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/auth/approle/login" {
			_ = json.NewEncoder(w).Encode(map[string]any{"auth": map[string]any{"client_token": "generated-test-login"}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"creds": credentials, "token": "MTIz.test-only.signature", "host_key": hostKey}}})
	}))
	defer upstream.Close()
	// Bot clients reach a fake Discord REST API and gateway, not Discord.
	discordREST := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer discordREST.Close()
	fakeGateway := testutil.NewGateway(t, "123", testutil.Guild())
	service.BotOptions = func(string) []bot.ConfigOpt {
		return []bot.ConfigOpt{bot.WithRestClientConfigOpts(rest.WithURL(discordREST.URL)), bot.WithGatewayConfigOpts(gateway.WithURL(fakeGateway.URL), gateway.WithCompression(gateway.CompressionNone))}
	}
	dir := t.TempDir()
	credential := filepath.Join(dir, "credential")
	if err := os.WriteFile(credential, []byte("generated-test-credential"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../docs/specifications/assets/configuration/examples/service.yml")
	if err != nil {
		t.Fatal(err)
	}
	auth := "  auth:\n    token_file: " + credential
	if stage == "login" || stage == "second" {
		auth = "  auth:\n    approle:\n      role_id: generated-test-role\n      secret_id_file: " + credential
	}
	cfg := strings.Replace(string(raw), "address: unix:///run/openbao/agent.sock", "address: "+upstream.URL+"\n"+auth, 1)
	natsURL := natsServer.ClientURL()
	var bucketSignal <-chan struct{}
	switch stage {
	case "registry":
		natsURL, bucketSignal = signalAt(t, natsURL, "KV_deck-threads")
	case "stream":
		natsURL, bucketSignal = signalAt(t, natsURL, "$JS.API.STREAM.")
	case "bind":
		// Hold the SSH bind until the stop arrives; the address stays occupied
		// by this helper's listener, so the bind itself fails.
		transport.BeforeBind = func(ctx context.Context) {
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Error(err)
			}
			<-ctx.Done()
		}
	}
	cfg = strings.Replace(cfg, "nats://nats.example.internal:4222", natsURL, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cfg = strings.Replace(cfg, `ssh: ":2222"`, `ssh: "`+listener.Addr().String()+`"`, 1)
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = free.Close()
	cfg = strings.Replace(cfg, `health: ":8080"`, `health: "`+free.Addr().String()+`"`, 1)
	path := filepath.Join(dir, "config.yml")
	if err = os.WriteFile(path, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errs bytes.Buffer
	ctx := context.Background()
	var writerDone chan error
	if stage == "failure" {
		// Mirror main's incoming signal context. Hold a real credential read on a
		// FIFO until SIGTERM has canceled it, then produce an independent empty-file
		// error rather than an HTTP cancellation. This pins the race deterministically.
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err = os.Remove(credential); err != nil {
			t.Fatal(err)
		}
		if err = syscall.Mkfifo(credential, 0600); err != nil {
			t.Fatal(err)
		}
		writerDone = make(chan error, 1)
		go func() {
			f, err := os.OpenFile(credential, os.O_WRONLY, 0)
			if err != nil {
				writerDone <- err
				return
			}
			if err = syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				_ = f.Close()
				writerDone <- err
				return
			}
			<-ctx.Done()
			writerDone <- f.Close()
		}()
	}
	code := runContext(ctx, []string{"service", "start", "--config", path}, &out, &errs)
	if stage == "failure" {
		select {
		case err := <-writerDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("credential writer did not finish")
		}
		if ctx.Err() == nil || code != 1 || out.Len() != 0 || !strings.Contains(errs.String(), "vault.auth.token_file: credential file is empty") {
			t.Fatalf("real startup failure hidden after signal: ctx=%v code=%d stdout=%q stderr=%q", ctx.Err(), code, out.String(), errs.String())
		}
		return
	}
	if stage == "bind" {
		for _, field := range []string{`"level":"ERROR"`, `"step":"open the SSH listener"`, `"field":"listen.ssh"`, `"config":"` + path + `"`, `"msg":"service starting"`, `"msg":"gateway session state"`} {
			if code != 1 || out.Len() != 0 || !strings.Contains(errs.String(), field) {
				t.Fatalf("bind failure during stop: code=%d stdout=%q stderr=%q", code, out.String(), errs.String())
			}
		}
		// The log holds no secret the service read: token, NATS credentials,
		// host key or the Vault credential.
		for _, secret := range []string{"test-only.signature", credentials, strings.Split(hostKey, "\n")[1], "generated-test-credential", "generated-test-login"} {
			if strings.Contains(errs.String(), secret) {
				t.Fatalf("log holds a secret: %q", errs.String())
			}
		}
		return
	}
	if stage == "registry" || stage == "stream" {
		select {
		case <-bucketSignal:
		default:
			t.Fatal("startup did not reach the held NATS request")
		}
	}
	if code != 0 || out.Len() != 0 || !onlyLogLines(errs.String()) {
		t.Fatalf("signal during %s: code=%d stdout=%q stderr=%q", stage, code, out.String(), errs.String())
	}
}

// onlyLogLines reports whether stderr holds only JSON log lines, none of
// them an error: a clean stop logs its start and stop and nothing failed.
func onlyLogLines(stderr string) bool {
	for _, line := range strings.Split(strings.TrimSpace(stderr), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil || record["level"] == "ERROR" {
			return false
		}
	}
	return true
}

func TestServiceStartSignal(t *testing.T) {
	for _, stage := range []string{"login", "nats", "stream", "registry", "bot", "host", "bind"} {
		t.Run(stage, func(t *testing.T) {
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(executable, "-test.run=^TestServiceStartSignalHelper$", "-test.timeout=8s")
			cmd.Env = append(os.Environ(), "DECK_START_SIGNAL_STAGE="+stage)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("startup signal: %v\n%s", err, output)
			}
		})
	}
}

// Hold the AppRole revoke request open during teardown: a second SIGTERM
// must regain its default action instead of waiting for the close timeout.
func TestServiceStartSecondSignal(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestServiceStartSignalHelper$", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), "DECK_START_SIGNAL_STAGE=second")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var errs bytes.Buffer
	cmd.Stderr = &errs
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	reaped := false
	t.Cleanup(func() {
		if !reaped {
			_ = cmd.Process.Kill()
			<-done
		}
	})
	revoking := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "REVOKING" {
				close(revoking)
				return
			}
		}
	}()
	select {
	case <-revoking:
	case <-time.After(3 * time.Second):
		t.Fatal("teardown did not reach revoke")
	}
	// Allow the signal cancellation's AfterFunc to restore default handlers.
	time.Sleep(100 * time.Millisecond)
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		reaped = true
		exit, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("second signal did not terminate process: %v", err)
		}
		state, ok := exit.Sys().(syscall.WaitStatus)
		if !ok || !state.Signaled() || state.Signal() != syscall.SIGTERM {
			t.Fatalf("wrong second-signal result: %v; %s", err, errs.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second signal swallowed during teardown")
	}
}

func TestServiceStartRealFailureAfterSignal(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestServiceStartSignalHelper$", "-test.timeout=8s")
	cmd.Env = append(os.Environ(), "DECK_START_SIGNAL_STAGE=failure")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("real failure after signal: %v\n%s", err, output)
	}
}

// Forward the real NATS protocol until a request naming marker (the thread
// bucket or the stream API), then signal the child and withhold the request.
// This pins cancellation during that startup step rather than a secret read.
// The subprocess bounds failures of native signal handling.
func signalAt(t *testing.T, upstream, marker string) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	signalled := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client, err := listener.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		server, err := net.Dial("tcp", strings.TrimPrefix(upstream, "nats://"))
		if err != nil {
			t.Error(err)
			return
		}
		defer server.Close()
		forwarded := make(chan struct{})
		go func() { defer close(forwarded); _, _ = io.Copy(client, server) }()
		defer func() { server.Close(); <-forwarded }()
		reader := bufio.NewReader(client)
		for {
			line, err := reader.ReadString('\n')
			if strings.Contains(line, marker) {
				if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
					t.Error(err)
				}
				close(signalled)
				<-release
				return
			}
			if _, writeErr := io.WriteString(server, line); writeErr != nil {
				return
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { close(release); listener.Close(); wg.Wait() })
	return "nats://" + listener.Addr().String(), signalled
}
