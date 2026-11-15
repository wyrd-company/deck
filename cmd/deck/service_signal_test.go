package main

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
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wyrd-company/deck/internal/service/testutil"
)

// Real CLI dispatch, Vault HTTP/KV and JWT NATS; only generated test material
// is used. A subprocess contains native signals if signal handling regresses.
func TestServiceStartSignalHelper(t *testing.T) {
	stage := os.Getenv("DECK_START_SIGNAL_STAGE")
	if stage == "" {
		return
	}
	natsServer, credentials := testutil.JWTNATS(t)
	target := map[string]string{"login": "/v1/auth/approle/login", "nats": "/v1/kv/data/deck/nats", "bot": "/v1/kv/data/deck/bots/helper", "host": "/v1/kv/data/deck/service", "second": "/v1/kv/data/deck/nats", "failure": "/unused", "registry": "/unused"}[stage]
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
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"creds": credentials, "token": "MTIz.test-only.signature"}}})
	}))
	defer upstream.Close()
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
	if stage == "registry" {
		natsURL, bucketSignal = signalAtBucket(t, natsURL)
	}
	cfg = strings.Replace(cfg, "nats://nats.example.internal:4222", natsURL, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	cfg = strings.Replace(cfg, `ssh: ":2222"`, `ssh: "`+listener.Addr().String()+`"`, 1)
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
	if stage == "registry" {
		select {
		case <-bucketSignal:
		default:
			t.Fatal("startup did not reach thread-bucket request")
		}
	}
	if code != 0 || out.Len() != 0 || errs.Len() != 0 {
		t.Fatalf("signal during %s: code=%d stdout=%q stderr=%q", stage, code, out.String(), errs.String())
	}
}

func TestServiceStartSignal(t *testing.T) {
	for _, stage := range []string{"login", "nats", "registry", "bot", "host"} {
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

// Forward the real NATS protocol until bucket startup, then signal the child
// and withhold the request. This pins cancellation during registry.Open rather
// than a secret read. The subprocess bounds failures of native signal handling.
func signalAtBucket(t *testing.T, upstream string) (string, <-chan struct{}) {
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
			if strings.Contains(line, "KV_deck-threads") {
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
