package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
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

	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/vault"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const transportTestToken = "deck-transport-test-root"

type baoFixture struct {
	cfg     config.Vault
	address string
	signs   atomic.Int32
}

func transportBao(t *testing.T) *baoFixture {
	t.Helper()
	bin := os.Getenv("DECK_BAO")
	if bin == "" {
		var err error
		bin, err = exec.LookPath("bao")
		if err != nil {
			t.Skip("bao executable not available; set DECK_BAO")
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	cmd := exec.Command(bin, "server", "-dev", "-dev-root-token-id="+transportTestToken, "-dev-listen-address="+addr)
	cmd.Env = []string{"HOME=" + t.TempDir(), "PATH=" + os.Getenv("PATH")}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	address := "http://" + addr
	ready := false
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		response, err := http.Get(address + "/v1/sys/health")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		t.Fatal("OpenBao did not become ready")
	}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte(transportTestToken), 0600); err != nil {
		t.Fatal(err)
	}
	b := &baoFixture{address: address, cfg: config.Vault{Address: address, Timeout: 3 * time.Second, Auth: config.Auth{TokenFile: token}}}
	target, _ := url.Parse(address)
	proxy := httputil.NewSingleHostReverseProxy(target)
	socket := filepath.Join(t.TempDir(), "bao.sock")
	unix, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/sign/") {
			b.signs.Add(1)
		}
		proxy.ServeHTTP(w, r)
	})}
	go func() { _ = server.Serve(unix) }()
	t.Cleanup(func() { _ = server.Close() })
	b.cfg.Address = "unix://" + socket
	b.admin(t, "sys/mounts/identities", map[string]any{"type": "transit"})
	b.admin(t, "sys/mounts/hosts", map[string]any{"type": "kv", "options": map[string]string{"version": "2"}})
	return b
}
func (b *baoFixture) admin(t *testing.T, path string, body any) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest("POST", b.address+"/v1/"+path, strings.NewReader(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Vault-Token", transportTestToken)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		t.Fatalf("fixture setup %s: HTTP %d", path, response.StatusCode)
	}
}
func (b *baoFixture) signer(t *testing.T, name string) *TransitSigner {
	t.Helper()
	b.admin(t, "identities/keys/"+name, map[string]any{"type": "ed25519"})
	c, err := vault.New(context.Background(), b.cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	signer, err := NewTransitSigner(context.Background(), c, config.TransitKey{Mount: "identities", Name: name})
	if err != nil {
		t.Fatal(err)
	}
	return signer
}
func testHost(t *testing.T) ssh.Signer {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	host, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	return host
}
func serveTest(t *testing.T, keys []config.Key, host ssh.Signer, handler deckv1.DeckServiceServer) string {
	t.Helper()
	server, err := NewServer(keys, host, handler)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(l) }()
	t.Cleanup(func() {
		_ = server.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("SSH server did not stop")
		}
	})
	return l.Addr().String()
}

type grantHandler struct {
	deckv1.UnimplementedDeckServiceServer
	metadataReads atomic.Int32
}

func (h *grantHandler) ListChannels(ctx context.Context, _ *deckv1.ListChannelsRequest) (*deckv1.ListChannelsResponse, error) {
	out := &deckv1.ListChannelsResponse{}
	for _, channel := range []*deckv1.Channel{{Bot: "helper", ServerId: 10, ChannelId: 11, Name: KeyName(ctx)}, {Bot: "helper", ServerId: 20, ChannelId: 21, Name: KeyName(ctx)}} {
		if CheckTarget(ctx, channel.Bot, channel.ServerId, channel.ChannelId) == nil {
			out.Channels = append(out.Channels, channel)
		}
	}
	return out, nil
}
func (h *grantHandler) GetThread(ctx context.Context, req *deckv1.GetThreadRequest) (*deckv1.GetThreadResponse, error) {
	ref := req.GetThread()
	if err := CheckBot(ctx, ref.GetBot()); err != nil {
		return nil, err
	}
	h.metadataReads.Add(1)
	server, channel := uint64(10), uint64(11)
	if ref.GetThreadId() == 22 {
		server, channel = 20, 21
	}
	if err := CheckTarget(ctx, ref.Bot, server, channel); err != nil {
		return nil, err
	}
	return &deckv1.GetThreadResponse{Thread: &deckv1.Thread{Bot: ref.Bot, ThreadId: ref.ThreadId, ServerId: server, ChannelId: channel, Name: KeyName(ctx)}}, nil
}
func (h *grantHandler) Listen(req *deckv1.ListenRequest, stream grpc.ServerStreamingServer[deckv1.ListenResponse]) error {
	if len(Grants(stream.Context())) != 1 || KeyName(stream.Context()) == "" {
		return status.Error(codes.Internal, "missing stream identity")
	}
	if err := stream.Send(&deckv1.ListenResponse{Through: &deckv1.StreamPosition{Epoch: uint64(KeyName(stream.Context())[0])}}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("code = %v, want %v", err, code)
	}
}

func TestLiveSSHGrants(t *testing.T) {
	b := transportBao(t)
	a := b.signer(t, "a")
	bb := b.signer(t, "b")
	_ = b.signer(t, "unknown")
	host := testHost(t)
	handler := &grantHandler{}
	keys := []config.Key{{Name: "A", PublicKey: DescribeKey(a.PublicKey(), "").PublicKey, Grants: []config.Grant{{Bot: "helper", Server: "10", Channels: []string{"*"}}}}, {Name: "B", PublicKey: DescribeKey(bb.PublicKey(), "").PublicKey, Grants: []config.Grant{{Bot: "helper", Server: "20", Channels: []string{"21"}}}}}
	address := serveTest(t, keys, host, handler)
	// Mutating the original config after startup must not widen grants.
	keys[0].Grants[0].Channels[0] = "99"
	cfg := &config.Client{Vault: b.cfg, Service: config.ServiceEndpoint{Address: address, HostKey: DescribeKey(host.PublicKey(), "").PublicKey, Timeout: 3 * time.Second}, Key: config.TransitKey{Mount: "identities", Name: "a"}}
	for _, test := range []struct {
		name, key string
		channel   uint64
	}{{"A", "a", 11}, {"B", "b", 21}} {
		t.Run(test.name, func(t *testing.T) {
			cfg.Key.Name = test.key
			before := b.signs.Load()
			conn, err := DialClient(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if b.signs.Load() != before+1 {
				t.Fatal("accepted connection must sign once")
			}
			expected := a.PublicKey()
			if test.key == "b" {
				expected = bb.PublicKey()
			}
			if conn.Fingerprint != ssh.FingerprintSHA256(expected) {
				t.Fatal("wrong actual identity fingerprint")
			}
			ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("key", "forged", "key-name", "forged"))
			var wg sync.WaitGroup
			for range 5 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					result, err := conn.API.ListChannels(ctx, &deckv1.ListChannelsRequest{})
					if err != nil {
						t.Error(err)
						return
					}
					if len(result.Channels) != 1 || result.Channels[0].ChannelId != test.channel || result.Channels[0].Name != test.name {
						t.Errorf("wrong grant or identity: %v", result)
					}
				}()
			}
			wg.Wait()
			streamCtx, cancel := context.WithCancel(context.Background())
			stream, err := conn.API.Listen(streamCtx, &deckv1.ListenRequest{})
			if err != nil {
				t.Fatal(err)
			}
			first, err := stream.Recv()
			if err != nil || first.GetThrough().GetEpoch() != uint64(test.name[0]) {
				t.Fatalf("stream identity: %v %v", first, err)
			}
			cancel()
			_, err = stream.Recv()
			requireCode(t, err, codes.Canceled)
			if test.key == "a" {
				_, err = conn.API.GetThread(ctx, &deckv1.GetThreadRequest{Thread: &deckv1.ThreadRef{Bot: "helper", ThreadId: 22}})
				requireCode(t, err, codes.PermissionDenied)
				before := handler.metadataReads.Load()
				_, err = conn.API.GetThread(ctx, &deckv1.GetThreadRequest{Thread: &deckv1.ThreadRef{Bot: "not-granted", ThreadId: 12}})
				requireCode(t, err, codes.PermissionDenied)
				if handler.metadataReads.Load() != before {
					t.Fatal("bot authorization followed metadata read")
				}
			}
			if b.signs.Load() != before+1 {
				t.Fatal("RPCs unexpectedly signed or reconnected")
			}
		})
	}
	cfg.Key.Name = "unknown"
	before := b.signs.Load()
	conn, err := DialClient(context.Background(), cfg)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("unknown identity authenticated")
	}
	if b.signs.Load() != before {
		t.Fatal("unknown offered key caused a Transit signature")
	}
	cfg.Key.Name = "a"
	cfg.Service.HostKey = DescribeKey(testHost(t).PublicKey(), "").PublicKey
	_, err = DialClient(context.Background(), cfg)
	if !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatalf("host mismatch: %v", err)
	}
}

// An offered key can differ from the subsequently signed key: the first two
// marshals are the unsigned query and its acknowledgement, then the signature
// packet contains B. This reproduces the callback-cache identity confusion.
type offeredKey struct {
	ssh.PublicKey
	signed   ssh.PublicKey
	marshals atomic.Int32
}

func (k *offeredKey) Marshal() []byte {
	if k.marshals.Add(1) <= 2 {
		return k.PublicKey.Marshal()
	}
	return k.signed.Marshal()
}

type alternateSigner struct {
	ssh.Signer
	offered ssh.PublicKey
}

func (s alternateSigner) PublicKey() ssh.PublicKey { return s.offered }

func TestLiveOfferedAndSignedKeys(t *testing.T) {
	b := transportBao(t)
	a := b.signer(t, "offer")
	bb := b.signer(t, "sign")
	host := testHost(t)
	keys := []config.Key{{Name: "offered", PublicKey: DescribeKey(a.PublicKey(), "").PublicKey, Grants: []config.Grant{{Bot: "helper", Server: "10", Channels: []string{"*"}}}}, {Name: "signed", PublicKey: DescribeKey(bb.PublicKey(), "").PublicKey, Grants: []config.Grant{{Bot: "helper", Server: "20", Channels: []string{"*"}}}}}
	address := serveTest(t, keys, host, &grantHandler{})
	cfg := &config.Client{Service: config.ServiceEndpoint{Address: address, HostKey: DescribeKey(host.PublicKey(), "").PublicKey}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	switching := &offeredKey{PublicKey: a.PublicKey(), signed: bb.PublicKey()}
	conn, err := dialSigner(ctx, cfg, alternateSigner{Signer: bb, offered: switching}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	result, err := conn.API.ListChannels(ctx, &deckv1.ListChannelsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if switching.marshals.Load() < 3 || len(result.Channels) != 1 || result.Channels[0].Name != "signed" || result.Channels[0].ChannelId != 21 {
		t.Fatalf("offered key became handler identity: %v (%d marshals)", result, switching.marshals.Load())
	}
	// Actually signing A's packet with B is a forgery, and is always refused.
	_, err = dialSigner(ctx, cfg, alternateSigner{Signer: bb, offered: a.PublicKey()}, time.Second)
	if err == nil {
		t.Fatal("signature for a different public key authenticated")
	}
	// When only the offered identity is configured, signing as B is refused.
	cfg.Service.Address = serveTest(t, keys[:1], host, &grantHandler{})
	_, err = dialSigner(ctx, cfg, alternateSigner{Signer: bb, offered: &offeredKey{PublicKey: a.PublicKey(), signed: bb.PublicKey()}}, time.Second)
	if err == nil {
		t.Fatal("unconfigured signed key authenticated")
	}
}

func TestLiveHostKeyAndRotation(t *testing.T) {
	b := transportBao(t)
	signer := b.signer(t, "rotating")
	// Read again after rotation and compare the public material, not only version.
	b.admin(t, "identities/keys/rotating/rotate", nil)
	latest, err := NewTransitSigner(context.Background(), signer.client, signer.ref)
	if err != nil {
		t.Fatal(err)
	}
	if latest.key.Version != 2 || string(latest.PublicKey().Marshal()) == string(signer.PublicKey().Marshal()) {
		t.Fatal("latest Transit public key did not rotate")
	}
	if _, err := latest.Sign(rand.Reader, []byte("latest")); err != nil {
		t.Fatal(err)
	}
	if _, err := signer.Sign(rand.Reader, []byte("pinned")); err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pem, err := ssh.MarshalPrivateKey(private, "")
	if err != nil {
		t.Fatal(err)
	}
	b.storeHost(t, pemEncode(pem))
}

// Keep ordinary HTTP fixture servers from being mistaken for production SSH.
func TestCredentialsRejectPlainConnection(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if _, _, err := (sshCredentials{}).ServerHandshake(a); err == nil {
		t.Fatal("plain connection authenticated")
	}
	if _, _, err := (sshCredentials{}).ClientHandshake(context.Background(), "", a); err == nil {
		t.Fatal("plain connection accepted by client")
	}
}
func pemEncode(block *pem.Block) []byte { return pem.EncodeToMemory(block) }
func (b *baoFixture) storeHost(t *testing.T, raw []byte) {
	b.admin(t, "hosts/data/deck/host", map[string]any{"data": map[string]any{"private": string(raw)}})
	c, err := vault.New(context.Background(), b.cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	ref := config.SecretRef{Mount: "hosts", Path: "deck/host", Field: "private"}
	signer, err := ReadHostKey(context.Background(), c, ref)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := ssh.ParsePrivateKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(signer.PublicKey().Marshal()) != string(expected.PublicKey().Marshal()) {
		t.Fatal("wrong host public key")
	}
	// Cached host material continues to work after the KV value changes.
	b.admin(t, "hosts/data/deck/host", map[string]any{"data": map[string]any{"private": "invalid replacement"}})
	cached, err := ReadHostKey(context.Background(), c, ref)
	if err != nil || string(cached.PublicKey().Marshal()) != string(signer.PublicKey().Marshal()) {
		t.Fatal("host key was read more than once")
	}
}

type probeHandler struct {
	deckv1.UnimplementedDeckServiceServer
	wait bool
}

func (h *probeHandler) ListChannels(ctx context.Context, _ *deckv1.ListChannelsRequest) (*deckv1.ListChannelsResponse, error) {
	if h.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &deckv1.ListChannelsResponse{Channels: []*deckv1.Channel{{Name: strings.Repeat("x", 5<<20)}}}, nil
}
func (h *probeHandler) Listen(_ *deckv1.ListenRequest, stream grpc.ServerStreamingServer[deckv1.ListenResponse]) error {
	if err := stream.Send(&deckv1.ListenResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func TestSSHProtocolBoundary(t *testing.T) {
	signer, host := testHost(t), testHost(t)
	address := serveTest(t, []config.Key{{Name: "client", PublicKey: DescribeKey(signer.PublicKey(), "").PublicKey}}, host, &probeHandler{})
	sshConfig := &ssh.ClientConfig{User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: time.Second}
	client, err := ssh.Dial("tcp", address, sshConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, request := range []struct {
		name    string
		payload []byte
	}{
		{"shell", nil}, {"exec", ssh.Marshal(struct{ Command string }{"uname"})},
		{"env", ssh.Marshal(struct{ Key, Value string }{"HOME", "/tmp"})},
		{"auth-agent-req@openssh.com", nil},
		{"pty-req", ssh.Marshal(struct {
			Term                         string
			Columns, Rows, Width, Height uint32
			Modes                        string
		}{Term: "xterm"})},
		{"subsystem", ssh.Marshal(struct{ Name string }{"sftp"})},
	} {
		channel, requests, err := client.OpenChannel("session", nil)
		if err != nil {
			t.Fatal(err)
		}
		go ssh.DiscardRequests(requests)
		ok, err := channel.SendRequest(request.name, true, request.payload)
		_ = channel.Close()
		if err != nil || ok {
			t.Fatalf("request %s: accepted=%v err=%v", request.name, ok, err)
		}
	}
	if conn, err := client.Dial("tcp", "127.0.0.1:1"); err == nil {
		_ = conn.Close()
		t.Fatal("port forwarding accepted")
	}
	if ok, _, err := client.SendRequest("tcpip-forward", true, ssh.Marshal(struct {
		Host string
		Port uint32
	}{"127.0.0.1", 0})); err != nil || ok {
		t.Fatalf("reverse forwarding: %v %v", ok, err)
	}
	sshConfig.User = "somebody"
	if conn, err := ssh.Dial("tcp", address, sshConfig); err == nil {
		_ = conn.Close()
		t.Fatal("wrong username accepted")
	}
	sshConfig.User = "deck"
	sshConfig.Auth = []ssh.AuthMethod{ssh.Password("anything")}
	if conn, err := ssh.Dial("tcp", address, sshConfig); err == nil {
		_ = conn.Close()
		t.Fatal("password accepted")
	}
}
func TestCallLifetimeAndSize(t *testing.T) {
	signer, host := testHost(t), testHost(t)
	keys := []config.Key{{Name: "client", PublicKey: DescribeKey(signer.PublicKey(), "").PublicKey}}
	cfg := &config.Client{Service: config.ServiceEndpoint{HostKey: DescribeKey(host.PublicKey(), "").PublicKey}}
	cfg.Service.Address = serveTest(t, keys, host, &probeHandler{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := dialSigner(ctx, cfg, signer, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := conn.API.ListChannels(ctx, &deckv1.ListChannelsRequest{})
	if err != nil || len(result.GetChannels()) != 1 || len(result.Channels[0].Name) != 5<<20 {
		t.Fatalf("large response: %v", err)
	}
	_ = conn.Close()
	conn, err = dialSigner(ctx, cfg, signer, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := conn.API.Listen(streamCtx, &deckv1.ListenRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	// More than one unary timeout can pass without ending a Listen.
	time.Sleep(110 * time.Millisecond)
	if err := stream.Context().Err(); err != nil {
		t.Fatalf("Listen inherited unary deadline: %v", err)
	}
	_ = conn.Close()
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("close left stream running")
	}
	cfg.Service.Address = serveTest(t, keys, host, &probeHandler{wait: true})
	conn, err = dialSigner(ctx, cfg, signer, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Now()
	_, err = conn.API.ListChannels(context.Background(), &deckv1.ListChannelsRequest{})
	requireCode(t, err, codes.DeadlineExceeded)
	if time.Since(start) > time.Second {
		t.Fatal("unary timeout was not applied")
	}
}
