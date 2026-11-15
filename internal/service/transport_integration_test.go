package service

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/transport"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func sshMaterial(t *testing.T) (ssh.Signer, string) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	return signer, string(pem.EncodeToMemory(block))
}

type sshRPCConn struct {
	ssh.Channel
	local, remote net.Addr
}

func (c *sshRPCConn) LocalAddr() net.Addr              { return c.local }
func (c *sshRPCConn) RemoteAddr() net.Addr             { return c.remote }
func (c *sshRPCConn) SetDeadline(time.Time) error      { return nil }
func (c *sshRPCConn) SetReadDeadline(time.Time) error  { return nil }
func (c *sshRPCConn) SetWriteDeadline(time.Time) error { return nil }

// Use the real deck SSH subsystem and server identity interceptors. Local
// signers isolate handler integration from the Transit signer tests in transport.
func dialSSHHandler(t *testing.T, address string, host ssh.PublicKey, key ssh.Signer) pb.DeckServiceClient {
	t.Helper()
	client, err := ssh.Dial("tcp", address, &ssh.ClientConfig{User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(key)}, HostKeyCallback: ssh.FixedHostKey(host), Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	channel, requests, err := client.OpenChannel("session", nil)
	if err != nil {
		t.Fatal(err)
	}
	go ssh.DiscardRequests(requests)
	ok, err := channel.SendRequest("subsystem", true, ssh.Marshal(struct{ Name string }{"deck"}))
	if err != nil || !ok {
		t.Fatal("deck subsystem", ok, err)
	}
	conn := &sshRPCConn{channel, client.LocalAddr(), client.RemoteAddr()}
	var mu sync.Mutex
	used := false
	grpcConn, err := grpc.NewClient("passthrough:///deck-integration", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if used {
			return nil, net.ErrClosed
		}
		used = true
		return conn, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = grpcConn.Close() })
	return pb.NewDeckServiceClient(grpcConn)
}
func fakeRequestCount(f *fakeDiscord) int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.requests) }

func TestSSHHandlerGrants(t *testing.T) {
	s := setup(t)
	listed, _ := sshMaterial(t)
	wild, _ := sshMaterial(t)
	host, _ := sshMaterial(t)
	cfg := *s.cfg
	cfg.Keys = []config.Key{
		{Name: "listed", PublicKey: string(ssh.MarshalAuthorizedKey(listed.PublicKey())), Grants: []config.Grant{{Bot: "a", Server: "1", Channels: []string{"10"}}}},
		{Name: "wild", PublicKey: string(ssh.MarshalAuthorizedKey(wild.PublicKey())), Grants: []config.Grant{{Bot: "b", Server: "1", Channels: []string{"*"}}}},
	}
	h, err := NewHandler(&cfg, Dependencies{Context: s.h.ctx, Bots: s.h.bots, Registry: s.reg, Publisher: s.h.Publisher, Index: fixedIndex{}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.NewServer(cfg.Keys, host, h)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
		h.Wait()
	})
	a := dialSSHHandler(t, listener.Addr().String(), host.PublicKey(), listed)
	b := dialSSHHandler(t, listener.Addr().String(), host.PublicKey(), wild)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, tc := range []struct {
		client   pb.DeckServiceClient
		bot, key string
		channel  uint64
	}{{a, "a", "listed", 10}, {b, "b", "wild", 20}} {
		out, err := tc.client.Post(ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: tc.bot, ChannelId: tc.channel}, Title: "integration", Content: content(), Nonce: "grant"})
		if err != nil || out.Message.SentBy != tc.key || out.Thread.Bot != tc.bot {
			t.Fatal("authenticated own-channel send", out, err)
		}
		for _, op := range []string{"Reply", "GetThread", "ListHistory"} {
			if err := invokeSSH(tc.client, ctx, op, tc.bot, tc.channel, out.Thread.ThreadId); err != nil {
				t.Fatal("own-channel", op, err)
			}
		}
	}
	for _, tc := range []struct {
		client pb.DeckServiceClient
		want   map[string]bool
	}{{a, map[string]bool{"a:10": true}}, {b, map[string]bool{"b:10": true, "b:20": true}}} {
		out, err := tc.client.ListChannels(ctx, &pb.ListChannelsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for _, ch := range out.Channels {
			got[fmt.Sprintf("%s:%d", ch.Bot, ch.ChannelId)] = true
		}
		if len(got) != len(tc.want) {
			t.Fatal("listed channels crossed grants", got)
		}
		for id := range tc.want {
			if !got[id] {
				t.Fatal("missing own channel", id)
			}
		}
	}
	for _, op := range []string{"Post", "Reply", "GetThread", "ListHistory"} {
		t.Run("unlisted/"+op, func(t *testing.T) {
			if err := invokeSSH(a, ctx, op, "a", 20, 201); status.Code(err) != codes.PermissionDenied {
				t.Fatal(err)
			}
		})
		t.Run("server-boundary/"+op, func(t *testing.T) {
			if err := invokeSSH(b, ctx, op, "b", 30, 300); status.Code(err) != codes.PermissionDenied {
				t.Fatal(err)
			}
		})
		t.Run("ungranted-bot/"+op, func(t *testing.T) {
			beforeA, beforeB := fakeRequestCount(s.fakes["a"]), fakeRequestCount(s.fakes["b"])
			err := invokeSSH(a, ctx, op, "b", 20, 501)
			if status.Code(err) != codes.PermissionDenied || fakeRequestCount(s.fakes["a"]) != beforeA || fakeRequestCount(s.fakes["b"]) != beforeB {
				t.Fatal("ungranted bot reached Discord", err)
			}
		})
	}
}
func invokeSSH(client pb.DeckServiceClient, ctx context.Context, op, bot string, channel, thread uint64) error {
	var err error
	switch op {
	case "Post":
		_, err = client.Post(ctx, &pb.PostRequest{Channel: &pb.ChannelRef{Bot: bot, ChannelId: channel}, Title: "refused", Content: content(), Nonce: "deny"})
	case "Reply":
		_, err = client.Reply(ctx, &pb.ReplyRequest{Thread: &pb.ThreadRef{Bot: bot, ThreadId: thread}, Content: content(), Nonce: "deny"})
	case "GetThread":
		_, err = client.GetThread(ctx, &pb.GetThreadRequest{Thread: &pb.ThreadRef{Bot: bot, ThreadId: thread}})
	case "ListHistory":
		_, err = client.ListHistory(ctx, &pb.ListHistoryRequest{Thread: &pb.ThreadRef{Bot: bot, ThreadId: thread}})
	}
	return err
}
