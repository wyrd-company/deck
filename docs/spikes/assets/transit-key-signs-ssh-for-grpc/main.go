// Command transit-key-signs-ssh-for-grpc is the throwaway program behind the
// transit-key-signs-ssh-for-grpc spike. It reads two ed25519 Transit keys
// from an OpenBao server, serves the gRPC health service over an SSH
// subsystem with Wish that accepts only the first key, and connects as each
// key with an SSH signer whose signatures come from the Transit sign API.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"charm.land/ssh"
	"charm.land/wish/v2"
	"github.com/openbao/openbao/api/v2"
	gossh "golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"
)

const subsystem = "deck"

type report struct {
	Step   string         `json:"step"`
	Result string         `json:"result"`
	Detail map[string]any `json:"detail,omitempty"`
}

var failed atomic.Bool

func emit(step, result string, detail map[string]any) {
	if result == "fail" {
		failed.Store(true)
	}
	line, _ := json.Marshal(report{Step: step, Result: result, Detail: detail})
	fmt.Println(string(line))
}

// transitSigner is an SSH signer whose private key stays in OpenBao.
type transitSigner struct {
	client *api.Client
	mount  string
	name   string
	public gossh.PublicKey
	signs  atomic.Int32
}

func newTransitSigner(ctx context.Context, client *api.Client, mount, name string) (*transitSigner, error) {
	secret, err := client.Logical().ReadWithContext(ctx, mount+"/keys/"+name)
	if err != nil {
		return nil, err
	}
	if secret == nil {
		return nil, errors.New("transit key not found")
	}
	if kind, _ := secret.Data["type"].(string); kind != "ed25519" {
		return nil, fmt.Errorf("transit key type %q, want ed25519", kind)
	}
	latest, _ := secret.Data["latest_version"].(json.Number)
	keys, _ := secret.Data["keys"].(map[string]any)
	version, _ := keys[latest.String()].(map[string]any)
	encoded, _ := version["public_key"].(string)
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("transit public key is not an ed25519 key")
	}
	public, err := gossh.NewPublicKey(ed25519.PublicKey(raw))
	if err != nil {
		return nil, err
	}
	return &transitSigner{client: client, mount: mount, name: name, public: public}, nil
}

func (s *transitSigner) PublicKey() gossh.PublicKey { return s.public }

// Sign asks Transit to sign the SSH session data. Transit's ed25519
// signature is the raw 64-byte signature the ssh-ed25519 format carries.
func (s *transitSigner) Sign(_ io.Reader, data []byte) (*gossh.Signature, error) {
	s.signs.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	secret, err := s.client.Logical().WriteWithContext(ctx, s.mount+"/sign/"+s.name, map[string]any{
		"input": base64.StdEncoding.EncodeToString(data),
	})
	if err != nil {
		return nil, err
	}
	encoded, _ := secret.Data["signature"].(string)
	parts := strings.Split(encoded, ":")
	if len(parts) != 3 || parts[0] != "vault" {
		return nil, errors.New("unexpected transit signature format")
	}
	blob, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, err
	}
	return &gossh.Signature{Format: gossh.KeyAlgoED25519, Blob: blob}, nil
}

// channelConn presents one SSH channel as a net.Conn for HTTP/2.
type channelConn struct {
	io.ReadWriteCloser
	local, remote net.Addr
	fingerprint   string
	once          sync.Once
	done          chan struct{}
}

func (c *channelConn) Close() error {
	err := c.ReadWriteCloser.Close()
	c.once.Do(func() { close(c.done) })
	return err
}
func (c *channelConn) LocalAddr() net.Addr              { return c.local }
func (c *channelConn) RemoteAddr() net.Addr             { return c.remote }
func (c *channelConn) SetDeadline(time.Time) error      { return nil }
func (c *channelConn) SetReadDeadline(time.Time) error  { return nil }
func (c *channelConn) SetWriteDeadline(time.Time) error { return nil }

// channelListener hands subsystem channels to grpc.Server.Serve.
type channelListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
	addr   net.Addr
}

func (l *channelListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *channelListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *channelListener) Addr() net.Addr { return l.addr }

// keyInfo is the gRPC AuthInfo that carries the SSH key a call came in on.
type keyInfo struct {
	credentials.CommonAuthInfo
	Fingerprint string
}

func (keyInfo) AuthType() string { return "ssh" }

// sshCredentials does no handshake of its own: SSH has authenticated the
// channel, and the server side lifts the key's fingerprint into AuthInfo.
type sshCredentials struct{}

func (sshCredentials) ClientHandshake(_ context.Context, _ string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return conn, keyInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}, nil
}
func (sshCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	channel, ok := conn.(*channelConn)
	if !ok {
		return nil, nil, errors.New("connection did not come from the SSH subsystem")
	}
	return conn, keyInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}, Fingerprint: channel.fingerprint}, nil
}
func (sshCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "ssh"}
}
func (c sshCredentials) Clone() credentials.TransportCredentials { return c }
func (sshCredentials) OverrideServerName(string) error           { return nil }

type fingerprintKey struct{}

func callerFingerprint(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	info, _ := p.AuthInfo.(keyInfo)
	return info.Fingerprint
}

func main() {
	address := flag.String("address", "", "OpenBao address: unix:///path or http://127.0.0.1:port")
	tokenFile := flag.String("token-file", "", "file holding the OpenBao token")
	mount := flag.String("mount", "transit", "Transit mount")
	allowedName := flag.String("key", "deck-client", "Transit key the service accepts")
	strangerName := flag.String("unknown-key", "deck-stranger", "Transit key the service does not know")
	flag.Parse()

	config := api.DefaultConfig()
	if err := config.Error; err != nil {
		emit("openbao-client", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	config.Address = *address
	client, err := api.NewClient(config)
	if err != nil {
		emit("openbao-client", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	// The client reads no BAO_* or VAULT_* variable: address and token come
	// from flags only, as deck's configuration would supply them.
	client.ClearToken()
	if *tokenFile != "" {
		token, err := os.ReadFile(*tokenFile)
		if err != nil {
			emit("openbao-client", "fail", map[string]any{"error": "cannot read token file"})
			os.Exit(1)
		}
		client.SetToken(strings.TrimSpace(string(token)))
	}
	ctx := context.Background()
	allowed, err := newTransitSigner(ctx, client, *mount, *allowedName)
	if err != nil {
		emit("transit-key", "fail", map[string]any{"key": *allowedName, "error": err.Error()})
		os.Exit(1)
	}
	stranger, err := newTransitSigner(ctx, client, *mount, *strangerName)
	if err != nil {
		emit("transit-key", "fail", map[string]any{"key": *strangerName, "error": err.Error()})
		os.Exit(1)
	}
	allowedFingerprint := gossh.FingerprintSHA256(allowed.PublicKey())
	emit("transit-key", "pass", map[string]any{"address_scheme": strings.SplitN(*address, ":", 2)[0], "allowed": allowedFingerprint, "stranger": gossh.FingerprintSHA256(stranger.PublicKey())})

	// The service: Wish accepts the allowed key only and serves gRPC on the
	// deck subsystem.
	_, hostPrivate, _ := ed25519.GenerateKey(rand.Reader)
	hostSigner, _ := gossh.NewSignerFromKey(hostPrivate)
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		emit("listen", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	grpcListener := &channelListener{conns: make(chan net.Conn), closed: make(chan struct{}), addr: tcp.Addr()}
	var refused atomic.Int32
	server, err := wish.NewServer(
		wish.WithVersion("SSH-2.0-deck-spike"),
		wish.WithPublicKeyAuth(func(ctx ssh.Context, key ssh.PublicKey) bool {
			fingerprint := gossh.FingerprintSHA256(key)
			if fingerprint != allowedFingerprint {
				refused.Add(1)
				return false
			}
			ctx.SetValue(fingerprintKey{}, fingerprint)
			return true
		}),
		wish.WithSubsystem(subsystem, func(session ssh.Session) {
			fingerprint, _ := session.Context().Value(fingerprintKey{}).(string)
			conn := &channelConn{ReadWriteCloser: session, local: session.LocalAddr(), remote: session.RemoteAddr(), fingerprint: fingerprint, done: make(chan struct{})}
			select {
			case grpcListener.conns <- conn:
			case <-grpcListener.closed:
				return
			}
			select {
			case <-conn.done:
			case <-session.Context().Done():
			}
		}),
		func(server *ssh.Server) error {
			server.AddHostKey(hostSigner)
			server.Handler = func(session ssh.Session) { _ = session.Exit(1) }
			return nil
		},
	)
	if err != nil {
		emit("wish-server", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	go func() { _ = server.Serve(tcp) }()
	defer server.Close()

	var seen sync.Map
	healthServer := health.NewServer()
	healthServer.SetServingStatus("deck", healthpb.HealthCheckResponse_SERVING)
	grpcServer := grpc.NewServer(
		grpc.Creds(sshCredentials{}),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			seen.Store(info.FullMethod, callerFingerprint(ctx))
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			seen.Store(info.FullMethod, callerFingerprint(stream.Context()))
			return handler(srv, stream)
		}),
	)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	go func() { _ = grpcServer.Serve(grpcListener) }()
	defer grpcServer.Stop()

	hostKey := hostSigner.PublicKey()
	dial := func(signer gossh.Signer) (*gossh.Client, time.Duration, error) {
		started := time.Now()
		client, err := gossh.Dial("tcp", tcp.Addr().String(), &gossh.ClientConfig{
			User:            "deck",
			Auth:            []gossh.AuthMethod{gossh.PublicKeys(signer)},
			HostKeyCallback: gossh.FixedHostKey(hostKey),
			Timeout:         10 * time.Second,
		})
		return client, time.Since(started), err
	}

	// The unknown key is refused at the SSH handshake.
	if strangerClient, _, err := dial(stranger); err == nil {
		strangerClient.Close()
		emit("unknown-key-refused", "fail", map[string]any{"error": "handshake succeeded"})
	} else {
		emit("unknown-key-refused", "pass", map[string]any{"error": err.Error(), "refusals": refused.Load(), "transit_signs": stranger.signs.Load()})
	}

	// The allowed key completes the handshake with a Transit signature.
	sshClient, elapsed, err := dial(allowed)
	if err != nil {
		emit("allowed-key-handshake", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	defer sshClient.Close()
	emit("allowed-key-handshake", "pass", map[string]any{"handshake_ms": elapsed.Milliseconds(), "transit_signs": allowed.signs.Load()})

	// One SSH channel carries one HTTP/2 connection for every call.
	grpcClient, err := grpc.NewClient("passthrough:///deck",
		grpc.WithTransportCredentials(sshCredentials{}),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			channel, requests, err := sshClient.OpenChannel("session", nil)
			if err != nil {
				return nil, err
			}
			go gossh.DiscardRequests(requests)
			ok, err := channel.SendRequest("subsystem", true, gossh.Marshal(struct{ Name string }{subsystem}))
			if err != nil || !ok {
				channel.Close()
				return nil, fmt.Errorf("subsystem %s refused", subsystem)
			}
			return &channelConn{ReadWriteCloser: channel, local: sshClient.LocalAddr(), remote: sshClient.RemoteAddr(), done: make(chan struct{})}, nil
		}),
	)
	if err != nil {
		emit("grpc-client", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	defer grpcClient.Close()
	health := healthpb.NewHealthClient(grpcClient)

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	started := time.Now()
	response, err := health.Check(callCtx, &healthpb.HealthCheckRequest{Service: "deck"})
	if err != nil {
		emit("grpc-unary", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	fingerprint, _ := seen.Load("/grpc.health.v1.Health/Check")
	emit("grpc-unary", "pass", map[string]any{"status": response.Status.String(), "call_ms": time.Since(started).Milliseconds(), "server_saw_key": fingerprint == allowedFingerprint})

	// A server stream carries updates as they happen, as deck listen needs.
	watch, err := health.Watch(callCtx, &healthpb.HealthCheckRequest{Service: "deck"})
	if err != nil {
		emit("grpc-stream", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	first, err := watch.Recv()
	if err != nil {
		emit("grpc-stream", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	changed := time.Now()
	healthServer.SetServingStatus("deck", healthpb.HealthCheckResponse_NOT_SERVING)
	second, err := watch.Recv()
	if err != nil {
		emit("grpc-stream", "fail", map[string]any{"error": err.Error()})
		os.Exit(1)
	}
	fingerprint, _ = seen.Load("/grpc.health.v1.Health/Watch")
	emit("grpc-stream", "pass", map[string]any{"first": first.Status.String(), "second": second.Status.String(), "update_ms": time.Since(changed).Milliseconds(), "server_saw_key": fingerprint == allowedFingerprint})

	// Concurrent calls share the one channel.
	var wg sync.WaitGroup
	var concurrentFailures atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := health.Check(callCtx, &healthpb.HealthCheckRequest{Service: "deck"}); err != nil {
				concurrentFailures.Add(1)
			}
		}()
	}
	wg.Wait()
	if concurrentFailures.Load() == 0 {
		emit("grpc-concurrent", "pass", map[string]any{"calls": 8, "transit_signs": allowed.signs.Load()})
	} else {
		emit("grpc-concurrent", "fail", map[string]any{"failures": concurrentFailures.Load()})
	}

	if failed.Load() {
		emit("done", "fail", nil)
		os.Exit(1)
	}
	emit("done", "pass", nil)
}
