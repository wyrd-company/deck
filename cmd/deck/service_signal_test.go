package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	target := map[string]string{"login": "/v1/auth/approle/login", "nats": "/v1/kv/data/deck/nats", "bot": "/v1/kv/data/deck/bots/helper", "host": "/v1/kv/data/deck/service", "second": "/v1/kv/data/deck/nats"}[stage]
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
	cfg = strings.Replace(cfg, "nats://nats.example.internal:4222", natsServer.ClientURL(), 1)
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
	code := run([]string{"service", "start", "--config", path}, &out, &errs)
	if code != 0 || out.Len() != 0 || errs.Len() != 0 {
		t.Fatalf("signal during %s: code=%d stdout=%q stderr=%q", stage, code, out.String(), errs.String())
	}
}

func TestServiceStartSignal(t *testing.T) {
	for _, stage := range []string{"login", "nats", "bot", "host"} {
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
