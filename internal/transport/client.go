package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/vault"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// ErrHostKeyMismatch is distinct from unavailable transport errors (CLI exit 1).
var ErrHostKeyMismatch = errors.New("service host key differs from service.host_key")
var ErrAuthentication = errors.New("SSH authentication failed")

// ClientConnection owns one SSH connection and one gRPC channel. Fingerprint is
// the actual pinned signing identity, not a second read of the Transit key.
type ClientConnection struct {
	API         deckv1.DeckServiceClient
	Fingerprint string
	grpc        *grpc.ClientConn
	ssh         *ssh.Client
	once        sync.Once
	err         error
}

func (c *ClientConnection) Close() error {
	c.once.Do(func() {
		grpcErr, sshErr := c.grpc.Close(), c.ssh.Close()
		if errors.Is(sshErr, net.ErrClosed) {
			sshErr = nil
		}
		c.err = errors.Join(grpcErr, sshErr)
	})
	return c.err
}

// DialClient bounds connecting and the SSH/subsystem handshakes by the service
// timeout. Calls share one channel; unary calls get that timeout independently.
// Listen keeps only its caller's deadline, so a quiet listen may run for hours.
func DialClient(ctx context.Context, cfg *config.Client) (*ClientConnection, error) {
	timeout := cfg.Service.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := vault.New(dialCtx, cfg.Vault)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeTimeout := cfg.Vault.Timeout
		if closeTimeout <= 0 {
			closeTimeout = 10 * time.Second
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), closeTimeout)
		defer closeCancel()
		_ = client.Close(closeCtx)
	}()
	signer, err := NewTransitSigner(dialCtx, client, cfg.Key)
	if err != nil {
		return nil, err
	}
	return dialSigner(dialCtx, cfg, signer, timeout)
}

func dialSigner(ctx context.Context, cfg *config.Client, signer ssh.Signer, timeout time.Duration) (*ClientConnection, error) {
	host, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(cfg.Service.HostKey))
	if err != nil || len(options) > 0 || len(rest) > 0 || host.Type() != ssh.KeyAlgoED25519 {
		return nil, errors.New("service.host_key: expected one ed25519 public key")
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", cfg.Service.Address)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "cannot connect to SSH service")
	}
	stopCancel := context.AfterFunc(ctx, func() { _ = raw.Close() })
	success := false
	defer func() {
		stopCancel()
		if !success {
			_ = raw.Close()
		}
	}()
	deadline, _ := ctx.Deadline()
	_ = raw.SetDeadline(deadline)
	mismatch := false
	recorder := &signingRecorder{Signer: signer}
	sshConn, chans, reqs, err := ssh.NewClientConn(raw, cfg.Service.Address, &ssh.ClientConfig{
		User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(recorder)},
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			if string(key.Marshal()) != string(host.Marshal()) {
				mismatch = true
				return ErrHostKeyMismatch
			}
			return nil
		},
	})
	if err != nil {
		if mismatch {
			return nil, ErrHostKeyMismatch
		}
		if recorder.err != nil {
			return nil, recorder.err
		}
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		var networkError net.Error
		if errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, status.Error(codes.Unavailable, "SSH service disconnected during handshake")
		}
		// Client-side SSH does not export a typed authentication refusal.
		// Never expose the raw handshake error, which may carry upstream text.
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, fmt.Errorf("%w: connection or credentials refused", ErrAuthentication)
	}
	sshClient := ssh.NewClient(sshConn, chans, reqs)
	channel, requests, err := sshClient.OpenChannel("session", nil)
	if err != nil {
		_ = sshClient.Close()
		return nil, status.Error(codes.Unavailable, "cannot open SSH session")
	}
	go ssh.DiscardRequests(requests)
	ok, err := channel.SendRequest("subsystem", true, ssh.Marshal(struct{ Name string }{subsystem}))
	if err != nil || !ok {
		_ = sshClient.Close()
		return nil, status.Error(codes.Unavailable, "deck SSH subsystem refused")
	}
	_ = raw.SetDeadline(time.Time{})
	conn := &channelConn{ReadWriteCloser: channel, local: raw.LocalAddr(), remote: raw.RemoteAddr(), done: make(chan struct{})}
	var dialMu sync.Mutex
	used := false
	grpcConn, err := grpc.NewClient("passthrough:///deck",
		grpc.WithTransportCredentials(sshCredentials{}),
		grpc.WithDisableRetry(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			dialMu.Lock()
			defer dialMu.Unlock()
			if used {
				return nil, errors.New("SSH channel is closed; open a new command connection")
			}
			used = true
			return conn, nil
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageSize), grpc.MaxCallSendMsgSize(maxMessageSize)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			callCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			return invoker(callCtx, method, req, reply, cc, opts...)
		}),
	)
	if err != nil {
		_ = sshClient.Close()
		return nil, errors.New("cannot configure gRPC connection")
	}
	grpcConn.Connect()
	for {
		state := grpcConn.GetState()
		if state == connectivity.Ready {
			break
		}
		if state == connectivity.Shutdown || !grpcConn.WaitForStateChange(ctx, state) {
			_ = grpcConn.Close()
			_ = sshClient.Close()
			return nil, status.Error(codes.Unavailable, "gRPC over SSH did not become ready")
		}
	}
	// Cancellation of the short dial context must not close a successfully
	// established channel. Its owner closes it after all command calls finish.
	if !stopCancel() && ctx.Err() != nil {
		_ = grpcConn.Close()
		_ = sshClient.Close()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	success = true
	return &ClientConnection{API: deckv1.NewDeckServiceClient(grpcConn), Fingerprint: ssh.FingerprintSHA256(signer.PublicKey()), grpc: grpcConn, ssh: sshClient}, nil
}

// Preserve a sanitized Vault error's category through SSH's handshake wrapping.
type signingRecorder struct {
	ssh.Signer
	err error
}

func (s *signingRecorder) Sign(random io.Reader, data []byte) (*ssh.Signature, error) {
	signature, err := s.Signer.Sign(random, data)
	s.err = err
	return signature, err
}
