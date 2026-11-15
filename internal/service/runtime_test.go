package service

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/rest"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	discord "github.com/wyrd-company/deck/internal/service/discord"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"github.com/wyrd-company/deck/internal/transport"
	"github.com/wyrd-company/deck/internal/vault"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRuntimeVaultPathsAndSecretIsolation(t *testing.T) {
	s := setup(t)
	natsServer, creds := testutil.JWTNATS(t)
	var reads atomic.Int64
	var paths []string
	var factoryTokens []string
	tokens := []string{"MTIz.fake-bot-token.signature", "MTI0.second-private-bot.signature"}
	vaultServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !slices.Contains([]string{"/v1/private/data/deck/runtime", "/v1/private/data/deck/first", "/v1/private/data/deck/second"}, r.URL.Path) || r.Header.Get("X-Vault-Token") != "vault-test-secret" {
			t.Error("unexpected configured Vault request")
			w.WriteHeader(403)
			return
		}
		reads.Add(1)
		paths = append(paths, r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"nats": creds, "first": tokens[0], "second": tokens[1]}}})
	}))
	defer vaultServer.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	_ = os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: vaultServer.Certificate().Raw}), 0600)
	tokenFile := filepath.Join(dir, "token")
	_ = os.WriteFile(tokenFile, []byte("vault-test-secret"), 0600)
	vc, err := vault.New(s.ctx, config.Vault{Address: vaultServer.URL, CAFile: ca, Timeout: time.Second, Auth: config.Auth{TokenFile: tokenFile}})
	if err != nil {
		t.Fatal(err)
	}
	defer vc.Close(context.Background())
	ref := func(field string) config.SecretRef {
		return config.SecretRef{Mount: "private", Path: "deck/runtime", Field: field}
	}
	cfg := *s.cfg
	cfg.NATS.Servers = []string{natsServer.ClientURL()}
	cfg.NATS.Timeout = time.Second
	cfg.NATS.Creds = ref("nats")
	cfg.NATS.Threads = config.Threads{Bucket: "runtime", Manage: true, Replicas: 1}
	first, second := ref("first"), ref("second")
	first.Path = "deck/first"
	second.Path = "deck/second"
	cfg.Bots = []config.Bot{{Name: "a", Token: first}, {Name: "b", Token: second}}
	server := httptest.NewServer(s.fake)
	defer server.Close()
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)
	r, err := open(s.ctx, &cfg, vc, func(token string) (*discord.Client, error) {
		factoryTokens = append(factoryTokens, token)
		if token != tokens[0] && token != tokens[1] {
			return nil, errors.New("unexpected token")
		}
		return discord.New(token, bot.WithRestClientConfigOpts(rest.WithURL(server.URL)))
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := r.Handler.Post(transport.WithKeyName(s.ctx, "key"), &pb.PostRequest{Channel: &pb.ChannelRef{Bot: "a", ChannelId: 10}, Title: "test", Content: content(), Nonce: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := protojson.Marshal(result)
	r.Close()
	if reads.Load() != 3 || !slices.Equal(paths, []string{"/v1/private/data/deck/runtime", "/v1/private/data/deck/first", "/v1/private/data/deck/second"}) {
		t.Fatal("wrong secret paths or caching", paths)
	}
	if !slices.Equal(factoryTokens, tokens) {
		t.Fatal("per-bot token mapping changed")
	}
	visible := logs.String() + string(raw) + fmt.Sprint(r.Handler)
	for _, secret := range append(tokens, creds, "vault-test-secret") {
		if strings.Contains(visible, secret) {
			t.Fatal("secret present in logs or API output")
		}
	}
	_, err = open(s.ctx, &cfg, vc, func(token string) (*discord.Client, error) { return nil, errors.New(token) })
	if err == nil {
		t.Fatal("factory failure ignored")
	}
	for _, secret := range tokens {
		if strings.Contains(err.Error(), secret) {
			t.Fatal("startup error leaked token")
		}
	}
}
