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
	"sync"
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
const maxSSHChannels = 4
const maxConcurrentStreams = 32

type authenticatedName struct{}

// Server serves only the deck subsystem. Close cancels all outstanding calls.
type Server struct {
	ssh       *wishssh.Server
	grpc      *grpc.Server
	channels  *channelListener
	mu        sync.Mutex
	listener  net.Listener
	closed    bool
	closeOnce sync.Once
	closeErr  error
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
	grpcServer := grpc.NewServer(grpc.WaitForHandlers(true), grpc.MaxConcurrentStreams(maxConcurrentStreams), grpc.Creds(sshCredentials{}), grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream), grpc.MaxRecvMsgSize(maxMessageSize), grpc.MaxSendMsgSize(maxMessageSize), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 15 * time.Second, PermitWithoutStream: true}))
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
			s.HandshakeTimeout = 30 * time.Second
			s.Handler = nil
			s.SessionRequestCallback = func(session wishssh.Session, request string) bool {
				return request == "subsystem" && session.Subsystem() == subsystem
			}
			s.PtyCallback = func(wishssh.Context, wishssh.Pty) bool { return false }
			s.ChannelHandlers = map[string]wishssh.ChannelHandler{"session": deckSessionHandler}
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
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = listener.Close()
		return nil
	}
	if s.listener != nil {
		s.mu.Unlock()
		_ = listener.Close()
		return errors.New("SSH server is already serving")
	}
	s.listener = listener
	s.channels.addr = listener.Addr()
	s.mu.Unlock()
	grpcDone := make(chan error, 1)
	go func() { grpcDone <- s.grpc.Serve(s.channels) }()
	err := s.ssh.Serve(listener)
	_ = s.Close()
	grpcErr := <-grpcDone
	if err != nil && !errors.Is(err, wishssh.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		return err
	}
	if grpcErr != nil && !errors.Is(grpcErr, grpc.ErrServerStopped) && !errors.Is(grpcErr, net.ErrClosed) {
		return grpcErr
	}
	return nil
}

// Close is final even before Serve starts. Close the owned listener directly:
// Wish may not have registered it yet, so ssh.Close alone is insufficient.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		listener := s.listener
		s.mu.Unlock()
		if listener != nil {
			_ = listener.Close()
		}
		s.grpc.Stop() // WaitForHandlers joins canceled handlers before shared resources close.
		_ = s.channels.Close()
		s.closeErr = s.ssh.Close()
		if errors.Is(s.closeErr, net.ErrClosed) || errors.Is(s.closeErr, wishssh.ErrServerClosed) {
			s.closeErr = nil
		}
	})
	return s.closeErr
}

// Wish's default session accepts env and agent-forwarding requests. Filter the
// request stream before Wish sees it so the only accepted request is deck.
func deckSessionHandler(server *wishssh.Server, conn *ssh.ServerConn, channel ssh.NewChannel, ctx wishssh.Context) {
	ctx.Lock()
	slots, _ := ctx.Value(channelSlots{}).(chan struct{})
	if slots == nil {
		slots = make(chan struct{}, maxSSHChannels)
		ctx.SetValue(channelSlots{}, slots)
	}
	ctx.Unlock()
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		_ = channel.Reject(ssh.ResourceShortage, "SSH session limit reached")
		return
	}
	wishssh.DefaultSessionHandler(server, conn, &subsystemChannel{NewChannel: channel, ctx: ctx}, ctx)
}

type channelSlots struct{}

type subsystemChannel struct {
	ssh.NewChannel
	ctx wishssh.Context
}

func (c *subsystemChannel) Accept() (ssh.Channel, <-chan *ssh.Request, error) {
	channel, requests, err := c.NewChannel.Accept()
	if err != nil {
		return nil, nil, err
	}
	filtered := make(chan *ssh.Request)
	go func() {
		defer close(filtered)
		for request := range requests {
			var payload struct{ Name string }
			if request.Type != "subsystem" || ssh.Unmarshal(request.Payload, &payload) != nil || payload.Name != subsystem {
				_ = request.Reply(false, nil)
				continue
			}
			select {
			case filtered <- request:
			case <-c.ctx.Done():
				return
			}
		}
	}()
	return channel, filtered, nil
}
