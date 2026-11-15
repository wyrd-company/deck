package cli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// This test peer exercises the CLI over an authenticated SSH deck subsystem.
// Transit and production transport security are task 5's own integration tests.
type channelConn struct {
	ssh.Channel
	local, remote net.Addr
}

func (c channelConn) LocalAddr() net.Addr            { return c.local }
func (c channelConn) RemoteAddr() net.Addr           { return c.remote }
func (channelConn) SetDeadline(time.Time) error      { return nil }
func (channelConn) SetReadDeadline(time.Time) error  { return nil }
func (channelConn) SetWriteDeadline(time.Time) error { return nil }

type channelListener struct {
	connections chan net.Conn
	done        chan struct{}
}

func (l *channelListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.connections:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *channelListener) Close() error   { return nil }
func (l *channelListener) Addr() net.Addr { return &net.TCPAddr{} }
func sshFakeDial(t *testing.T, f *fake) (Dial, string) {
	t.Helper()
	_, clientPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientSigner, err := ssh.NewSignerFromKey(clientPrivate)
	if err != nil {
		t.Fatal(err)
	}
	_, hostPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostPrivate)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcListener := &channelListener{connections: make(chan net.Conn), done: make(chan struct{})}
	server := grpc.NewServer()
	pb.RegisterDeckServiceServer(server, f)
	go server.Serve(grpcListener)
	t.Cleanup(func() { listener.Close(); close(grpcListener.done); server.Stop() })
	serverConfig := &ssh.ServerConfig{PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if meta.User() != "deck" || string(key.Marshal()) != string(clientSigner.PublicKey().Marshal()) {
			return nil, errors.New("unknown key")
		}
		return &ssh.Permissions{}, nil
	}}
	serverConfig.AddHostKey(hostSigner)
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				conn, channels, requests, err := ssh.NewServerConn(raw, serverConfig)
				if err != nil {
					raw.Close()
					return
				}
				defer conn.Close()
				go ssh.DiscardRequests(requests)
				for next := range channels {
					if next.ChannelType() != "session" {
						next.Reject(ssh.UnknownChannelType, "session required")
						continue
					}
					ch, reqs, err := next.Accept()
					if err != nil {
						continue
					}
					go func() {
						req, ok := <-reqs
						var subsystem struct{ Name string }
						if !ok || req.Type != "subsystem" || ssh.Unmarshal(req.Payload, &subsystem) != nil || subsystem.Name != "deck" {
							if ok {
								req.Reply(false, nil)
							}
							ch.Close()
							return
						}
						req.Reply(true, nil)
						go ssh.DiscardRequests(reqs)
						wrapped := channelConn{Channel: ch, local: raw.LocalAddr(), remote: raw.RemoteAddr()}
						select {
						case grpcListener.connections <- wrapped:
						case <-grpcListener.done:
							ch.Close()
						}
					}()
				}
			}()
		}
	}()
	fingerprint := ssh.FingerprintSHA256(clientSigner.PublicKey())
	return func(ctx context.Context, _ *config.Client) (Connection, error) {
		client, err := ssh.Dial("tcp", listener.Addr().String(), &ssh.ClientConfig{User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(clientSigner)}, HostKeyCallback: ssh.FixedHostKey(hostSigner.PublicKey()), Timeout: time.Second})
		if err != nil {
			return Connection{}, err
		}
		ch, reqs, err := client.OpenChannel("session", nil)
		if err != nil {
			client.Close()
			return Connection{}, err
		}
		go ssh.DiscardRequests(reqs)
		ok, err := ch.SendRequest("subsystem", true, ssh.Marshal(struct{ Name string }{"deck"}))
		if err != nil || !ok {
			client.Close()
			return Connection{}, errors.New("subsystem refused")
		}
		raw := channelConn{Channel: ch, local: client.LocalAddr(), remote: client.RemoteAddr()}
		var used atomic.Bool
		conn, err := grpc.NewClient("passthrough:///ssh-fake", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			if !used.CompareAndSwap(false, true) {
				return nil, errors.New("no repeat connection")
			}
			return raw, nil
		}))
		if err != nil {
			client.Close()
			return Connection{}, err
		}
		return Connection{API: pb.NewDeckServiceClient(conn), Fingerprint: fingerprint, Close: func() error { conn.Close(); return client.Close() }}, nil
	}, fingerprint
}
func TestCommandsOverSSH(t *testing.T) {
	f := setup(t)
	dial, key := sshFakeDial(t, f.fake)
	f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		own, err := f.open().Own(ss(), key, r.Nonce)
		if err != nil || !own {
			t.Errorf("authenticated fingerprint not pre-recorded: %v %v", own, err)
		}
		return &pb.PostResponse{Thread: baseThread, Message: ownActivity(key, r.Nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	for _, args := range [][]string{{"post", "--title", "Question", "--text", "hello"}, {"reply", "10", "--text", "answer"}, {"inbox", "--all"}, {"read"}, {"history", "10"}, {"listen", "--timeout", "1s"}, {"channels"}, {"skill"}, {"resolve", "10"}} {
		for _, jsonMode := range []bool{false, true} {
			var out, err bytes.Buffer
			a := f.app(&out, &err)
			a.Dial = dial
			command := append([]string{"--config", f.path}, args...)
			if jsonMode {
				command = append(command, "--json")
			}
			if code := a.Run(context.Background(), command); code != 0 {
				t.Fatalf("%v: %d %s %s", command, code, out.String(), err.String())
			}
			if jsonMode {
				var object map[string]any
				if e := json.Unmarshal(out.Bytes(), &object); e != nil {
					t.Fatal(e, out.String())
				}
				if args[0] != "resolve" && object["unread"] == nil {
					t.Fatal(object)
				}
			}
		}
	}
}
