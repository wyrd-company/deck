package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
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
			code := run(args, &stdout, &stderr)
			if code != 0 || stderr.Len() != 0 {
				t.Fatalf("%s: %d %s", command, code, stderr.String())
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
	const secret = "DO-NOT-PRINT-SECRET-OR-RESPONSE"
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := ssh.MarshalPrivateKeyWithPassphrase(private, "", []byte("test-passphrase"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherBlock, err := ssh.MarshalPrivateKey(other, "")
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	invalidHosts := map[string]string{
		"malformed-host":  secret,
		"encrypted-host":  string(pem.EncodeToMemory(encrypted)),
		"wrong-algorithm": string(pem.EncodeToMemory(otherBlock)),
		"non-OpenSSH":     string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})),
	}
	for _, kind := range []string{"client", "service"} {
		words := []string{"config", "key"}
		if kind == "service" {
			words = []string{"service", "host-key"}
		}
		cases := []string{"forbidden", "unreachable", "invalid-config", "invalid-key"}
		if kind == "service" {
			for name := range invalidHosts {
				cases = append(cases, name)
			}
			cases = append(cases, "missing-host-field")
		}
		for _, name := range cases {
			t.Run(kind+"/"+name, func(t *testing.T) {
				body := invalidHosts[name]
				if body == "" {
					body = secret
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if name == "forbidden" {
						w.WriteHeader(http.StatusForbidden)
						_, _ = io.WriteString(w, secret)
						return
					}
					if kind == "client" {
						_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"type": secret}})
						return
					}
					raw := body
					data := map[string]any{"private": raw}
					if name == "missing-host-field" {
						data = map[string]any{"other": secret}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": data}})
				}))
				defer server.Close()
				path := keyConfig(t, kind, server.URL)
				code, category := 1, "failed"
				if name == "unreachable" {
					server.Close()
					code, category = 4, "unavailable"
				}
				if name == "invalid-config" {
					if err := os.WriteFile(path, []byte("unknown: "+secret+"\n"), 0600); err != nil {
						t.Fatal(err)
					}
					code, category = 2, "invalid"
				}
				for _, jsonMode := range []bool{false, true} {
					var stdout, stderr bytes.Buffer
					args := append(append([]string{}, words...), "--config", path)
					if jsonMode {
						args = append(args, "--json")
					}
					got := run(args, &stdout, &stderr)
					if got != code {
						t.Fatalf("status %d want %d: %s%s", got, code, stdout.String(), stderr.String())
					}
					combined := stdout.String() + stderr.String()
					for _, forbidden := range []string{secret, body, "PRIVATE KEY", "test-passphrase"} {
						if forbidden != "" && strings.Contains(combined, forbidden) {
							t.Fatal("failure output leaked upstream or key material")
						}
					}
					if jsonMode {
						if stderr.Len() != 0 {
							t.Fatalf("JSON diagnostics on stderr: %q", stderr.String())
						}
						var object struct {
							Error struct{ Category, Message string }
						}
						decoder := json.NewDecoder(&stdout)
						if err := decoder.Decode(&object); err != nil || object.Error.Category != category || object.Error.Message == "" {
							t.Fatalf("JSON failure: %+v %v", object, err)
						}
						if err := decoder.Decode(&object); !errors.Is(err, io.EOF) {
							t.Fatal("more than one JSON object")
						}
					} else {
						if stdout.Len() != 0 || stderr.Len() == 0 || !strings.HasSuffix(stderr.String(), "\n") {
							t.Fatalf("text output streams: %q %q", stdout.String(), stderr.String())
						}
						for _, line := range strings.Split(strings.TrimSuffix(stderr.String(), "\n"), "\n") {
							if !strings.HasPrefix(line, "deck: ") {
								t.Fatalf("unprefixed diagnostic %q", line)
							}
						}
					}
				}
			})
		}
	}
	for _, test := range []struct {
		err  error
		code int
	}{{context.Canceled, 130}, {context.DeadlineExceeded, 4}, {vault.ErrUnavailable, 4}, {transport.ErrHostKeyMismatch, 1}} {
		if got := serviceExitCode(test.err); got != test.code {
			t.Fatalf("exit code %v: %d", test.err, got)
		}
	}
	var stdout, stderr bytes.Buffer
	if _, handled := keyCommand(context.Background(), []string{"unknown"}, &stdout, &stderr); handled {
		t.Fatal("key hook swallowed an unrelated command")
	}
}
