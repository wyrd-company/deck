package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type startupSecret struct {
	raw    string
	read   atomic.Int32
	cancel context.CancelFunc
}

func (s *startupSecret) ReadSecret(context.Context, string, config.SecretRef) (string, error) {
	s.read.Add(1)
	if s.cancel != nil {
		s.cancel()
	}
	return s.raw, nil
}
func startupMaterial(t *testing.T) (ssh.Signer, string) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	host, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	return host, string(pem.EncodeToMemory(block))
}
func unusedAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	_ = l.Close()
	return address
}
func requireClosedPort(t *testing.T, address string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("listener still accepts after shutdown")
	}
}
func TestCancellationBeforeServe(t *testing.T) {
	for range 20 {
		server, err := NewServer(nil, testHost(t), &probeHandler{})
		if err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := make(chan error, 1)
		go func() { done <- serveUntilCanceled(ctx, server, listener) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			_ = listener.Close()
			t.Fatal("pre-canceled Serve hung")
		}
		requireClosedPort(t, listener.Addr().String())
	}
	server, err := NewServer(nil, testHost(t), &probeHandler{})
	if err != nil {
		t.Fatal(err)
	}
	_ = server.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = listener.Close()
		<-stopped
		t.Fatal("Close before Serve was not final")
	}
	requireClosedPort(t, listener.Addr().String())
}

// Close after Serve releases its lock but before Wish tracks the listener.
// Only the direct listener close can end Accept once Serve resumes here.
func TestCloseBeforeSSHRegistersListener(t *testing.T) {
	server, err := NewServer(nil, testHost(t), &probeHandler{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	defer server.Close()
	closed := make(chan error, 1)
	server.beforeSSHServe = func() { closed <- server.Close() }
	stopped := make(chan error, 1)
	go func() { stopped <- server.Serve(listener) }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish in the unregistered-listener window")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		// Join the mutant's blocked Serve before reporting an ordinary test failure.
		_ = listener.Close()
		<-stopped
		t.Fatal("Serve blocked after Close before SSH listener registration")
	}
	requireClosedPort(t, listener.Addr().String())
}

func TestListenAndServeCancellation(t *testing.T) {
	host, raw := startupMaterial(t)
	signer := testHost(t)
	for _, duringRead := range []bool{true, false} {
		t.Run(map[bool]string{true: "during-host-read", false: "after-serving"}[duringRead], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			secret := &startupSecret{raw: raw}
			if duringRead {
				secret.cancel = cancel
			}
			address := unusedAddress(t)
			cfg := &config.Service{Listen: config.Listen{SSH: address}, Keys: []config.Key{{Name: "client", PublicKey: DescribeKey(signer.PublicKey(), "").PublicKey}}}
			done := make(chan error, 1)
			go func() { done <- ListenAndServe(ctx, cfg, secret, &probeHandler{}) }()
			if !duringRead {
				var client *ssh.Client
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					var err error
					client, err = ssh.Dial("tcp", address, &ssh.ClientConfig{User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: 100 * time.Millisecond})
					if err == nil {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if client == nil {
					cancel()
					t.Fatal("ListenAndServe never started")
				}
				defer client.Close()
				cancel()
			}
			select {
			case err := <-done:
				if err != nil && !(duringRead && errors.Is(err, context.Canceled)) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("ListenAndServe did not return after cancellation")
			}
			if secret.read.Load() != 1 {
				t.Fatalf("host read %d times", secret.read.Load())
			}
			requireClosedPort(t, address)
		})
	}
}
func TestHandshakeLifetimeBound(t *testing.T) {
	server, err := NewServer(nil, testHost(t), &probeHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if server.ssh.HandshakeTimeout != 30*time.Second {
		t.Fatalf("handshake timeout %s", server.ssh.HandshakeTimeout)
	}
	server.ssh.HandshakeTimeout = 60 * time.Millisecond
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	raw, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadAll(raw); err != nil {
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			t.Fatal("pre-auth connection outlived handshake deadline")
		}
	}
}

type unwindingHandler struct {
	deckv1.UnimplementedDeckServiceServer
	entered, release, exited chan struct{}
}

func (h *unwindingHandler) Listen(_ *deckv1.ListenRequest, stream grpc.ServerStreamingServer[deckv1.ListenResponse]) error {
	if err := stream.Send(&deckv1.ListenResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	close(h.entered)
	<-h.release
	close(h.exited)
	return stream.Context().Err()
}
func TestShutdownJoinsHandlersAndClientClose(t *testing.T) {
	host, signer := testHost(t), testHost(t)
	handler := &unwindingHandler{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	server, err := NewServer([]config.Key{{Name: "client", PublicKey: DescribeKey(signer.PublicKey(), "").PublicKey}}, host, handler)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := dialSigner(ctx, &config.Client{Service: config.ServiceEndpoint{Address: listener.Addr().String(), HostKey: DescribeKey(host.PublicKey(), "").PublicKey}}, signer, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := conn.API.Listen(ctx, &deckv1.ListenRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	<-handler.entered
	select {
	case <-closed:
		close(handler.release)
		t.Fatal("Close returned before handler finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(handler.release)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	select {
	case <-handler.exited:
	default:
		t.Fatal("handler still unwinding")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = conn.ssh.Wait()
	if err := conn.Close(); err != nil {
		t.Fatalf("Close after server drop: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConnectionResourceLimits(t *testing.T) {
	host, signer := testHost(t), testHost(t)
	address := serveTest(t, []config.Key{{Name: "client", PublicKey: DescribeKey(signer.PublicKey(), "").PublicKey}}, host, &probeHandler{})
	client, err := ssh.Dial("tcp", address, &ssh.ClientConfig{User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for range maxSSHChannels {
		channel, requests, err := client.OpenChannel("session", nil)
		if err != nil {
			t.Fatal(err)
		}
		go ssh.DiscardRequests(requests)
		defer channel.Close()
	}
	channel, _, err := client.OpenChannel("session", nil)
	if err == nil {
		_ = channel.Close()
		t.Fatal("extra SSH session was accepted")
	}
	var rejected *ssh.OpenChannelError
	if !errors.As(err, &rejected) || rejected.Reason != ssh.ResourceShortage {
		t.Fatalf("SSH cap error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := dialSigner(ctx, &config.Client{Service: config.ServiceEndpoint{Address: address, HostKey: DescribeKey(host.PublicKey(), "").PublicKey}}, signer, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range maxConcurrentStreams {
		stream, err := conn.API.Listen(ctx, &deckv1.ListenRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}
	overflow, overflowCancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer overflowCancel()
	stream, err := conn.API.Listen(overflow, &deckv1.ListenRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	requireCode(t, err, codes.DeadlineExceeded)
}
