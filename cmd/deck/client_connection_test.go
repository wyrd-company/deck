package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/cli"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/session"
	"github.com/wyrd-company/deck/internal/store"
	"go.yaml.in/yaml/v3"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
)

// A protocol peer independently records the key only after NewServerConn has
// verified its signature. The dialer, signer and command dispatch are production.
type adapterChannel struct {
	ssh.Channel
	local, remote net.Addr
}

func (c adapterChannel) LocalAddr() net.Addr            { return c.local }
func (c adapterChannel) RemoteAddr() net.Addr           { return c.remote }
func (adapterChannel) SetDeadline(time.Time) error      { return nil }
func (adapterChannel) SetReadDeadline(time.Time) error  { return nil }
func (adapterChannel) SetWriteDeadline(time.Time) error { return nil }

type adapterListener struct {
	conns chan net.Conn
	done  chan struct{}
}

func (l *adapterListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (*adapterListener) Close() error   { return nil }
func (*adapterListener) Addr() net.Addr { return &net.TCPAddr{} }
func adapterSSHPeer(t *testing.T, public ssh.PublicKey, handler pb.DeckServiceServer, refuse bool) (string, string, func() string) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	host, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	incoming := &adapterListener{conns: make(chan net.Conn), done: make(chan struct{})}
	server := grpc.NewServer()
	pb.RegisterDeckServiceServer(server, handler)
	go server.Serve(incoming)
	var authenticated atomic.Value
	cfg := &ssh.ServerConfig{PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if meta.User() != "deck" || !bytes.Equal(key.Marshal(), public.Marshal()) {
			return nil, errors.New("unknown offered key")
		}
		return &ssh.Permissions{Extensions: map[string]string{"fingerprint": ssh.FingerprintSHA256(key)}}, nil
	}}
	cfg.AddHostKey(host)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		defer raw.Close()
		_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
		conn, channels, requests, err := ssh.NewServerConn(raw, cfg)
		if err != nil {
			return
		}
		defer conn.Close()
		authenticated.Store(conn.Permissions.Extensions["fingerprint"])
		go ssh.DiscardRequests(requests)
		for next := range channels {
			if next.ChannelType() != "session" {
				_ = next.Reject(ssh.UnknownChannelType, "session required")
				continue
			}
			ch, reqs, err := next.Accept()
			if err != nil {
				return
			}
			req, ok := <-reqs
			var subsystem struct{ Name string }
			if !ok || req.Type != "subsystem" || ssh.Unmarshal(req.Payload, &subsystem) != nil || subsystem.Name != "deck" {
				ch.Close()
				return
			}
			if refuse {
				_ = req.Reply(false, nil)
				ch.Close()
				return
			}
			_ = req.Reply(true, nil)
			go ssh.DiscardRequests(reqs)
			select {
			case incoming.conns <- adapterChannel{Channel: ch, local: raw.LocalAddr(), remote: raw.RemoteAddr()}:
			case <-incoming.done:
				ch.Close()
				return
			}
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		close(incoming.done)
		server.Stop()
		select {
		case <-finished:
		case <-time.After(6 * time.Second):
			t.Error("SSH peer did not stop")
		}
	})
	return listener.Addr().String(), strings.TrimSpace(string(ssh.MarshalAuthorizedKey(host.PublicKey()))), func() string { key, _ := authenticated.Load().(string); return key }
}

type adapterAPI struct {
	pb.UnimplementedDeckServiceServer
	post func(*pb.PostRequest) (*pb.PostResponse, error)
}

func (a *adapterAPI) Post(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
	return a.post(r)
}
func (*adapterAPI) CountUnread(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
	return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_TOUCHED}}}, nil
}

type adapterTransit struct {
	public       ssh.PublicKey
	address      string
	signatures   atomic.Value
	reads, signs atomic.Int32
}

func adapterVault(t *testing.T, fail bool) *adapterTransit {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		t.Fatal(err)
	}
	b := &adapterTransit{public: sshPublic}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != keyCommandToken {
			t.Error("dial did not use configured token")
		}
		switch r.URL.Path {
		case "/v1/identities/keys/agent":
			b.reads.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"type": "ed25519", "latest_version": 7, "keys": map[string]any{"7": map[string]any{"public_key": base64.StdEncoding.EncodeToString(public)}}}})
		case "/v1/identities/sign/agent":
			b.signs.Add(1)
			var request struct {
				Input   string `json:"input"`
				Version int    `json:"key_version"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			data, err := base64.StdEncoding.DecodeString(request.Input)
			if err != nil || request.Version != 7 {
				t.Error("dial did not pin read key version")
				w.WriteHeader(400)
				return
			}
			signature := "vault:v7:" + base64.StdEncoding.EncodeToString(ed25519.Sign(private, data))
			b.signatures.Store(signature)
			if fail {
				w.WriteHeader(500)
				_, _ = io.WriteString(w, keyCommandToken+" "+signature)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"signature": signature}})
		default:
			t.Errorf("unexpected Vault dial path %q", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	b.address = server.URL
	return b
}
func adapterConfig(t *testing.T, b *adapterTransit, address, host string) (string, string) {
	t.Helper()
	path := keyConfig(t, "client", b.address)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err = yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(filepath.Dir(path), "deck.db")
	doc["service"] = map[string]any{"address": address, "host_key": host, "timeout": "2s"}
	doc["store"] = map[string]any{"path": db}
	doc["channels"] = map[string]any{"default": "helper/2"}
	doc["session"] = map[string]any{"variables": []string{}}
	raw, err = yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path, db
}

func TestDefaultDialRecordsServerAuthenticatedKey(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		t.Run(fmt.Sprint(jsonMode), func(t *testing.T) {
			b := adapterVault(t, false)
			api := &adapterAPI{}
			address, host, authenticated := adapterSSHPeer(t, b.public, api, false)
			path, db := adapterConfig(t, b, address, host)
			api.post = func(r *pb.PostRequest) (*pb.PostResponse, error) {
				serverKey := authenticated()
				if serverKey == "" {
					t.Error("Post before successful server authentication")
				}
				s, err := store.OpenReadOnly(db)
				if err != nil {
					t.Error(err)
					return nil, err
				}
				defer s.Close()
				own, err := s.Own(session.Session{}, serverKey, r.Nonce)
				record, recordErr := s.SendRecord(serverKey, r.Nonce)
				if err != nil || recordErr != nil || !own || record.State != "pending" {
					t.Errorf("pre-RPC identity differs from server-authenticated key: own=%v record=%v errors=%v %v", own, record, err, recordErr)
				}
				return &pb.PostResponse{Thread: &pb.Thread{Bot: "helper", ServerId: 1, ChannelId: 2, ThreadId: 10}, Message: &pb.Activity{Id: 11}, Position: &pb.StreamPosition{Epoch: 1, Sequence: 5}}, nil
			}
			args := []string{"--config", path, "post", "--title", "Question", "--text", "hello", "--nonce", "adapterNonce"}
			if jsonMode {
				args = append(args, "--json")
			}
			var out, stderr bytes.Buffer
			if code := run(args, &out, &stderr); code != 0 {
				t.Fatalf("production dial exit=%d out=%s err=%s", code, &out, &stderr)
			}
			s, err := store.OpenReadOnly(db)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			record, err := s.SendRecord(authenticated(), "adapterNonce")
			if err != nil || record.State != "sent" || record.MessageID != 11 {
				t.Fatalf("record=%v err=%v", record, err)
			}
			if authenticated() != ssh.FingerprintSHA256(b.public) || b.reads.Load() != 1 || b.signs.Load() != 1 {
				t.Fatal("identity reread or signing mismatch")
			}
		})
	}
}

func TestDefaultDialErrorsDoNotExposeCredentials(t *testing.T) {
	for _, which := range []string{"transit sign failure", "subsystem refusal"} {
		for _, jsonMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", which, jsonMode), func(t *testing.T) {
				b := adapterVault(t, which == "transit sign failure")
				address, host, _ := adapterSSHPeer(t, b.public, &adapterAPI{}, true)
				path, _ := adapterConfig(t, b, address, host)
				cfg, err := config.LoadClient(path)
				if err != nil {
					t.Fatal(err)
				}
				conn, dialErr := cli.DefaultDial(context.Background(), cfg)
				if conn.API != nil || dialErr == nil {
					t.Fatal("expected real DialClient error")
				}
				assertSafe := func(output string) {
					t.Helper()
					signature, _ := b.signatures.Load().(string)
					if signature == "" || b.signs.Load() == 0 {
						t.Fatal("test did not reach Transit signing")
					}
					for _, secret := range []string{keyCommandToken, signature, strings.TrimPrefix(signature, "vault:v7:")} {
						if strings.Contains(output, secret) {
							t.Fatal("real dial error/output exposed token or signature")
						}
					}
				}
				assertSafe(dialErr.Error())
				// A separate peer is required: each CLI invocation owns a fresh SSH session.
				address, host, _ = adapterSSHPeer(t, b.public, &adapterAPI{}, true)
				path, _ = adapterConfig(t, b, address, host)
				var out, stderr bytes.Buffer
				args := []string{"--config", path, "post", "--title", "Q", "--text", "hello"}
				if jsonMode {
					args = append(args, "--json")
				}
				code := run(args, &out, &stderr)
				if code != cli.ExitCode(dialErr) || code == 0 {
					t.Fatal("dial error category changed", code, dialErr)
				}
				assertSafe(out.String() + stderr.String())
			})
		}
	}
}
