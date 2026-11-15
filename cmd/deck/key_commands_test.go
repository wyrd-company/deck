package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wyrd-company/deck/internal/transport"
	"github.com/wyrd-company/deck/internal/vault"
	"go.yaml.in/yaml/v3"
	"golang.org/x/crypto/ssh"
)

func TestKeyCommands(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	rawPrivate := string(pem.EncodeToMemory(block))
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("key commands made %s request", r.Method)
			w.WriteHeader(500)
			return
		}
		switch r.URL.Path {
		case "/v1/identities/keys/agent":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"type": "ed25519", "latest_version": 2, "keys": map[string]any{"2": map[string]any{"public_key": base64.StdEncoding.EncodeToString(public)}}}})
		case "/v1/hosts/data/deck/host":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]any{"private": rawPrivate}}})
		default:
			t.Errorf("unexpected secret path %q", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	for _, kind := range []string{"config", "service"} {
		name, command, comment := "client", "key", "deck:agent"
		if kind == "service" {
			name, command, comment = "service", "host-key", ""
		}
		path := keyConfig(t, name, server.URL)
		expected := transport.DescribeKey(sshPublic, comment)
		for _, jsonMode := range []bool{false, true} {
			var stdout, stderr bytes.Buffer
			args := []string{"--config", path, kind, command}
			if jsonMode {
				args = append(args, "--json")
			}
			code, handled := keyCommand(context.Background(), args, &stdout, &stderr)
			if code != 0 || !handled || stderr.Len() != 0 {
				t.Fatalf("%s: %d %v %s", command, code, handled, stderr.String())
			}
			if strings.Contains(stdout.String(), "PRIVATE") || strings.Contains(stdout.String(), rawPrivate) {
				t.Fatal("printed private key")
			}
			if jsonMode {
				var got transport.PublicIdentity
				decoder := json.NewDecoder(&stdout)
				if err := decoder.Decode(&got); err != nil || got != expected {
					t.Fatalf("JSON identity: %v %v", got, err)
				}
				if err := decoder.Decode(&got); !errors.Is(err, io.EOF) {
					t.Fatalf("extra JSON output: %v", err)
				}
			} else if stdout.String() != expected.PublicKey+"\n"+expected.Fingerprint+"\n" {
				t.Fatalf("text identity: %q", stdout.String())
			}
		}
		var stdout, stderr bytes.Buffer
		if code, handled := keyCommand(context.Background(), []string{kind, command, "--help"}, &stdout, &stderr); code != 0 || !handled || !strings.Contains(stdout.String(), "Example:") || stderr.Len() != 0 {
			t.Fatalf("help: %d %v %q %q", code, handled, stdout.String(), stderr.String())
		}
		stdout.Reset()
		stderr.Reset()
		if code, _ := keyCommand(context.Background(), []string{kind, command, "extra", "--json"}, &stdout, &stderr); code != 2 || !strings.Contains(stdout.String(), `"category":"invalid"`) || stderr.Len() != 0 {
			t.Fatalf("usage: %d %q %q", code, stdout.String(), stderr.String())
		}
	}
}
func keyConfig(t *testing.T, kind, address string) string {
	t.Helper()
	raw, err := os.ReadFile("../../docs/specifications/assets/configuration/examples/" + kind + ".yml")
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["vault"] = map[string]any{"address": address, "timeout": "1s"}
	if kind == "client" {
		document["key"] = map[string]any{"mount": "identities", "name": "agent"}
	} else {
		document["host_key"] = map[string]any{"mount": "hosts", "path": "deck/host", "field": "private"}
	}
	raw, err = yaml.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestKeyCommandFailures(t *testing.T) {
	upstreamSecret := "DO-NOT-PRINT-SECRET-OR-RESPONSE"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, upstreamSecret)
	}))
	path := keyConfig(t, "client", server.URL)
	var stdout, stderr bytes.Buffer
	code, _ := keyCommand(context.Background(), []string{"config", "key", "--config", path, "--json"}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stdout.String(), `"category":"failed"`) || strings.Contains(stdout.String()+stderr.String(), upstreamSecret) {
		t.Fatalf("upstream error: %d %s %s", code, stdout.String(), stderr.String())
	}
	server.Close()
	stdout.Reset()
	stderr.Reset()
	code, _ = keyCommand(context.Background(), []string{"config", "key", "--config", path, "--json"}, &stdout, &stderr)
	if code != 4 || !strings.Contains(stdout.String(), `"category":"unavailable"`) {
		t.Fatalf("unavailable: %d %q", code, stdout.String())
	}
	for _, test := range []struct {
		err  error
		code int
	}{{context.Canceled, 130}, {context.DeadlineExceeded, 4}, {vault.ErrUnavailable, 4}, {transport.ErrHostKeyMismatch, 1}} {
		if got := serviceExitCode(test.err); got != test.code {
			t.Fatalf("exit code %v: %d", test.err, got)
		}
	}
}
