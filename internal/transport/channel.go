package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
)

type channelConn struct {
	io.ReadWriteCloser
	local, remote net.Addr
	name          string
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

type channelListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
	addr   net.Addr
}

func (l *channelListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}
func (l *channelListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *channelListener) Addr() net.Addr { return l.addr }

// SSH supplies encryption and authentication. Reject every server connection
// that did not originate in our authenticated subsystem.
type sshCredentials struct{}

func (sshCredentials) ClientHandshake(_ context.Context, _ string, c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if _, ok := c.(*channelConn); !ok {
		return nil, nil, errors.New("SSH channel required")
	}
	return c, KeyAuthInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}, nil
}
func (sshCredentials) ServerHandshake(c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	ch, ok := c.(*channelConn)
	if !ok || ch.name == "" {
		return nil, nil, errors.New("authenticated SSH channel required")
	}
	return c, KeyAuthInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}, Name: ch.name}, nil
}
func (sshCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "ssh"}
}
func (c sshCredentials) Clone() credentials.TransportCredentials { return c }
func (sshCredentials) OverrideServerName(string) error           { return nil }
