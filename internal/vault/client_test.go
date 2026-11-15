package vault

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyrd-company/deck/internal/config"
)

const testToken = "deck-test-root-token"

type devServer struct {
	cfg  config.Vault
	root *Client
}

func writeFile(t *testing.T, name, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func startBao(t *testing.T, tls bool) *devServer {
	t.Helper()
	bin := os.Getenv("DECK_BAO")
	if bin == "" {
		var err error
		bin, err = exec.LookPath("bao")
		if err != nil {
			t.Skip("bao executable not available; set DECK_BAO")
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	args := []string{"server", "-dev", "-dev-root-token-id=" + testToken, "-dev-listen-address=" + addr}
	certDir := t.TempDir()
	scheme := "http"
	if tls {
		scheme = "https"
		args = append(args, "-dev-tls", "-dev-tls-cert-dir="+certDir)
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = []string{"HOME=" + t.TempDir(), "PATH=" + os.Getenv("PATH")}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	cfg := config.Vault{Address: scheme + "://" + addr, Timeout: 2 * time.Second, Auth: config.Auth{TokenFile: writeFile(t, "token", testToken+"\n")}}
	if tls {
		cfg.CAFile = filepath.Join(certDir, "vault-ca.pem")
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		c, err := New(context.Background(), cfg)
		if err == nil {
			if _, err = c.request(context.Background(), http.MethodGet, "sys/health", nil); err == nil {
				s := &devServer{cfg, c}
				t.Cleanup(func() { _ = c.Close(context.Background()) })
				return s
			}
			c.transport.CloseIdleConnections()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("bao dev server did not become healthy")
	return nil
}
func admin(t *testing.T, c *Client, method, path string, data any) map[string]any {
	t.Helper()
	out, err := c.request(context.Background(), method, path, data)
	if err != nil {
		t.Fatalf("admin %s %s: %v", method, path, err)
	}
	return out
}
func prepare(t *testing.T, s *devServer) {
	admin(t, s.root, "POST", "secret/data/deck/shared", map[string]any{"data": map[string]any{"token": "test-bot-token", "creds": "test-nats-creds", "number": 42}})
	admin(t, s.root, "POST", "sys/mounts/custom/transit", map[string]any{"type": "transit"})
	admin(t, s.root, "POST", "custom/transit/keys/agent", map[string]any{"type": "ed25519"})
}
func verifyOperations(t *testing.T, c *Client) {
	t.Helper()
	ctx := context.Background()
	refs := map[string]config.SecretRef{"bots[0].token": {Mount: "secret", Path: "deck/shared", Field: "token"}, "nats.creds": {Mount: "secret", Path: "deck/shared", Field: "creds"}}
	values, err := c.Resolve(ctx, refs)
	if err != nil {
		t.Fatal(err)
	}
	if values["bots[0].token"] != "test-bot-token" || values["nats.creds"] != "test-nats-creds" {
		t.Fatal("wrong KV fields")
	}
	key, err := c.ReadTransitKey(ctx, "custom/transit", "agent")
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("an SSH handshake transcript")
	signature, err := c.SignTransit(ctx, "custom/transit", "agent", key.Version, data)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(key.PublicKey, data, signature) {
		t.Fatal("Transit signature does not verify with its public key")
	}
}
func TestLiveAddressForms(t *testing.T) {
	for _, tls := range []bool{false, true} {
		name := "http-loopback"
		if tls {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			s := startBao(t, tls)
			prepare(t, s)
			for _, key := range []string{"BAO_ADDR", "VAULT_ADDR", "VAULT_TOKEN", "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
				t.Setenv(key, "http://do-not-use.invalid:1")
			}
			c, err := New(context.Background(), s.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			verifyOperations(t, c)
			// A read version still signs with that public identity after a rotation.
			key, err := c.ReadTransitKey(context.Background(), "custom/transit", "agent")
			if err != nil {
				t.Fatal(err)
			}
			admin(t, s.root, "POST", "custom/transit/keys/agent/rotate", nil)
			signature, err := c.SignTransit(context.Background(), "custom/transit", "agent", key.Version, []byte("pinned"))
			if err != nil || !ed25519.Verify(key.PublicKey, []byte("pinned"), signature) {
				t.Fatalf("pinned signature after rotation: %v", err)
			}
			latest, err := c.ReadTransitKey(context.Background(), "custom/transit", "agent")
			if err != nil || latest.Version != key.Version+1 {
				t.Fatalf("latest version: %v", err)
			}
		})
	}
	t.Run("unix-agent", func(t *testing.T) {
		s := startBao(t, true)
		prepare(t, s)
		var reads atomic.Int64
		var incomingToken atomic.Bool
		socket := unixProxy(t, s, func(r *http.Request) {
			if r.Header.Get("X-Vault-Token") != "" {
				incomingToken.Store(true)
			}
			if strings.HasSuffix(r.URL.Path, "/secret/data/deck/shared") {
				reads.Add(1)
			}
		})
		c, err := New(context.Background(), config.Vault{Address: "unix://" + socket})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		verifyOperations(t, c)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				_, err := c.ReadSecret(context.Background(), "host_key", config.SecretRef{Mount: "secret", Path: "deck/shared", Field: "token"})
				if err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		if reads.Load() != 1 || incomingToken.Load() {
			t.Fatalf("KV GETs=%d, client sent token=%v", reads.Load(), incomingToken.Load())
		}
	})
}
func unixProxy(t *testing.T, s *devServer, observe func(*http.Request)) string {
	t.Helper()
	target, _ := url.Parse(s.cfg.Address)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = s.root.transport
	director := proxy.Director
	proxy.Director = func(r *http.Request) { observe(r); director(r); r.Header.Set("X-Vault-Token", testToken) }
	dir, err := os.MkdirTemp("", "deck-vault-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: time.Second}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return socket
}
func TestLiveAppRoleAndFailures(t *testing.T) {
	s := startBao(t, true)
	prepare(t, s)
	ctx := context.Background()
	admin(t, s.root, "POST", "sys/auth/role-auth", map[string]any{"type": "approle"})
	admin(t, s.root, "PUT", "sys/policies/acl/deck", map[string]any{"policy": `path "secret/data/deck/*" { capabilities = ["read"] }
path "custom/transit/keys/agent" { capabilities = ["read"] }
path "custom/transit/sign/agent" { capabilities = ["update"] }`})
	admin(t, s.root, "POST", "auth/role-auth/role/deck", map[string]any{"token_policies": []string{"deck"}, "token_ttl": "5m"})
	role := admin(t, s.root, "GET", "auth/role-auth/role/deck/role-id", nil)["data"].(map[string]any)["role_id"].(string)
	secret := admin(t, s.root, "POST", "auth/role-auth/role/deck/secret-id", nil)["data"].(map[string]any)["secret_id"].(string)
	cfg := s.cfg
	cfg.Auth = config.Auth{AppRole: &config.AppRole{Mount: "role-auth", RoleID: role, SecretIDFile: writeFile(t, "secret-id", secret+"\n")}}
	c, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(ctx) })
	verifyOperations(t, c)
	tests := []struct{ path, field, reason string }{{"deck/missing", "token", "HTTP 404"}, {"deck/shared", "missing", "field is missing"}, {"deck/shared", "number", "must be a string"}, {"not-granted", "token", "HTTP 403"}}
	for _, tt := range tests {
		_, err := c.ReadSecret(ctx, "bots[0].token", config.SecretRef{Mount: "secret", Path: tt.path, Field: tt.field})
		if err == nil || !strings.HasPrefix(err.Error(), "bots[0].token:") || !strings.Contains(err.Error(), tt.reason) {
			t.Errorf("%s/%s: %v", tt.path, tt.field, err)
		}
	}
	issued := c.token
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	probe, err := New(ctx, s.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close(ctx)
	probe.token = issued
	if _, err := probe.request(ctx, "GET", "auth/token/lookup-self", nil); err == nil {
		t.Fatal("AppRole token was not revoked")
	}
	// A file token is owned by the operator and survives Close.
	if err := s.root.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.root.request(ctx, "GET", "auth/token/lookup-self", nil); err != nil {
		t.Fatal("file token was revoked")
	}
	admin(t, s.root, "POST", "custom/transit/keys/rsa", map[string]any{"type": "rsa-2048"})
	if _, err := s.root.ReadTransitKey(ctx, "custom/transit", "rsa"); err == nil {
		t.Fatal("accepted a non-ed25519 key")
	}
}
func TestRequestGuardsAndRedaction(t *testing.T) {
	var targetHits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetHits.Add(1) }))
	defer target.Close()
	cases := []struct {
		name      string
		handler   http.HandlerFunc
		operation func(*Client) error
	}{
		{"HTTP error body", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(403)
			fmt.Fprint(w, "private-server-value")
		}, func(c *Client) error {
			_, err := c.request(context.Background(), "GET", "secret/data/x", nil)
			return err
		}},
		{"redirect", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }, func(c *Client) error { _, err := c.request(context.Background(), "GET", "x", nil); return err }},
		{"malformed response", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "private-server-value") }, func(c *Client) error { _, err := c.request(context.Background(), "GET", "x", nil); return err }},
		{"signature version", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"data":{"signature":"vault:v2:private-server-value"}}`)
		}, func(c *Client) error {
			_, err := c.SignTransit(context.Background(), "transit", "agent", 1, []byte("x"))
			return err
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()
			c, err := New(context.Background(), config.Vault{Address: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			if err = tt.operation(c); err == nil || strings.Contains(err.Error(), "private-server-value") {
				t.Fatalf("missing/redaction error: %v", err)
			}
		})
	}
	if targetHits.Load() != 0 {
		t.Fatal("followed a credential-bearing redirect")
	}
}
func TestAuthenticationAndNamespace(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("X-Vault-Namespace") != "team/deck" || r.Header.Get("X-Vault-Token") != "file-token" {
			t.Error("configured auth/namespace not applied")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]any{"token": "value"}}})
	}))
	defer server.Close()
	cfg := config.Vault{Address: server.URL, Namespace: "team/deck", Auth: config.Auth{TokenFile: writeFile(t, "token", " file-token\n")}}
	c, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if _, err := c.ReadSecret(context.Background(), "host_key", config.SecretRef{Mount: "kv", Path: "a space/x", Field: "token"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../x", "a/../x", "/x", "x/"} {
		if _, err := c.ReadSecret(context.Background(), "host_key", config.SecretRef{Mount: "kv", Path: path, Field: "token"}); err == nil {
			t.Errorf("accepted path traversal %s", path)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("unexpected requests %d", hits.Load())
	}
	for _, tokenFile := range []string{writeFile(t, "empty", "\n"), filepath.Join(t.TempDir(), "missing")} {
		cfg.Auth.TokenFile = tokenFile
		if _, err := New(context.Background(), cfg); err == nil {
			t.Fatal("accepted unusable token file")
		}
	}
	for _, address := range []string{"http://example.com:8200", "https://token@host:8200", "unix://relative/path"} {
		if _, err := New(context.Background(), config.Vault{Address: address}); err == nil {
			t.Errorf("accepted %s", address)
		}
	}
}

// Use a non-loopback request host: Go itself bypasses environment proxies for
// loopback hosts, so the real bao loopback tests alone cannot prove this guard.
func TestHTTPSIgnoresProxyEnvironment(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"data":{"data":{"value":"direct"}}}`) }))
	defer server.Close()
	t.Setenv("HTTPS_PROXY", "http://proxy.invalid:1")
	t.Setenv("NO_PROXY", "")
	c, err := New(context.Background(), config.Vault{Address: "https://vault.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	c.transport.TLSClientConfig.RootCAs.AddCert(server.Certificate())
	c.transport.TLSClientConfig.ServerName = "127.0.0.1"
	target := strings.TrimPrefix(server.URL, "https://")
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != "vault.example.test:443" {
			return nil, fmt.Errorf("unexpected proxy dial %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}
	value, err := c.ReadSecret(context.Background(), "host_key", config.SecretRef{Mount: "secret", Path: "x", Field: "value"})
	if err != nil || value != "direct" {
		t.Fatalf("direct request failed: %v", err)
	}
}
