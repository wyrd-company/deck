// ---
// relationships:
//
//	implements:
//	  - transport
//	  - carry-grpc-in-an-ssh-subsystem-channel
//
// ---
package transport

import (
	"errors"
	"fmt"
	"net"
	"time"

	wishssh "charm.land/ssh"
	"charm.land/wish/v2"
	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const maxMessageSize = 64 << 20
const subsystem = "deck"

type authenticatedName struct{}

// Server serves only the deck subsystem. Close cancels all outstanding calls.
type Server struct {
	ssh      *wishssh.Server
	grpc     *grpc.Server
	channels *channelListener
}

// NewServer snapshots keys and grants; callers register the service handler
// without giving it access to the SSH listener or the host private key.
func NewServer(keys []config.Key, host ssh.Signer, handler deckv1.DeckServiceServer) (*Server, error) {
	if host == nil || host.PublicKey().Type() != ssh.KeyAlgoED25519 {
		return nil, errors.New("host_key: expected ed25519 signer")
	}
	byFingerprint := map[string]string{}
	names := map[string]bool{}
	for i, key := range keys {
		public, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(key.PublicKey))
		if err != nil || len(options) > 0 || len(rest) > 0 || public.Type() != ssh.KeyAlgoED25519 || key.Name == "" {
			return nil, fmt.Errorf("keys[%d]: invalid ed25519 key or name", i)
		}
		fingerprint := ssh.FingerprintSHA256(public)
		if _, exists := byFingerprint[fingerprint]; exists || names[key.Name] {
			return nil, fmt.Errorf("keys[%d]: duplicate identity", i)
		}
		byFingerprint[fingerprint] = key.Name
		names[key.Name] = true
	}
	channels := &channelListener{conns: make(chan net.Conn), closed: make(chan struct{})}
	unary, stream := grantInterceptors(keys)
	grpcServer := grpc.NewServer(grpc.Creds(sshCredentials{}), grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream), grpc.MaxRecvMsgSize(maxMessageSize), grpc.MaxSendMsgSize(maxMessageSize), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 15 * time.Second, PermitWithoutStream: true}))
	deckv1.RegisterDeckServiceServer(grpcServer, handler)
	sshServer, err := wish.NewServer(
		wish.WithIdleTimeout(10*time.Minute),
		wish.WithPublicKeyAuth(func(ctx wishssh.Context, key wishssh.PublicKey) bool {
			ctx.SetValue(authenticatedName{}, "")
			name, ok := byFingerprint[ssh.FingerprintSHA256(key)]
			if !ok || ctx.User() != "deck" {
				return false
			}
			ctx.SetValue(authenticatedName{}, name)
			return true
		}),
		wish.WithSubsystem(subsystem, func(session wishssh.Session) {
			name, _ := session.Context().Value(authenticatedName{}).(string)
			// Bind the callback identity to the key in the signed connection too.
			public := session.PublicKey()
			if public == nil || name == "" || byFingerprint[ssh.FingerprintSHA256(public)] != name {
				return
			}
			conn := &channelConn{ReadWriteCloser: session, local: session.LocalAddr(), remote: session.RemoteAddr(), name: name, done: make(chan struct{})}
			defer conn.Close()
			select {
			case channels.conns <- conn:
			case <-channels.closed:
				return
			case <-session.Context().Done():
				return
			}
			select {
			case <-conn.done:
			case <-session.Context().Done():
			case <-channels.closed:
			}
		}),
		func(s *wishssh.Server) error {
			s.AddHostKey(host)
			s.Handler = nil
			s.SessionRequestCallback = func(session wishssh.Session, request string) bool {
				return request == "subsystem" && session.Subsystem() == subsystem
			}
			s.PtyCallback = func(wishssh.Context, wishssh.Pty) bool { return false }
			s.ChannelHandlers = map[string]wishssh.ChannelHandler{"session": wishssh.DefaultSessionHandler}
			s.RequestHandlers = map[string]wishssh.RequestHandler{}
			return nil
		},
	)
	if err != nil {
		grpcServer.Stop()
		return nil, errors.New("cannot configure SSH server")
	}
	return &Server{ssh: sshServer, grpc: grpcServer, channels: channels}, nil
}

// Serve takes ownership of listener and blocks until it closes or fails.
func (s *Server) Serve(listener net.Listener) error {
	s.channels.addr = listener.Addr()
	grpcDone := make(chan error, 1)
	go func() { grpcDone <- s.grpc.Serve(s.channels) }()
	err := s.ssh.Serve(listener)
	_ = s.Close()
	grpcErr := <-grpcDone
	if err != nil && !errors.Is(err, wishssh.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		return err
	}
	if grpcErr != nil && !errors.Is(grpcErr, grpc.ErrServerStopped) {
		return grpcErr
	}
	return nil
}
func (s *Server) Close() error { _ = s.channels.Close(); s.grpc.Stop(); return s.ssh.Close() }
