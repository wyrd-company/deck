package activity

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/wyrd-company/deck/internal/config"
)

// ConnectionEvents is the sole owner of the NATS handlers. Callbacks run in
// NATS's queued callback dispatcher and must invalidate/signal without blocking.
// Register before starting index replay. There is no lossy status channel.
type ConnectionEvents struct {
	mu                    sync.RWMutex
	reconnect, disconnect []func()
}

func (e *ConnectionEvents) OnReconnect(f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reconnect = append(e.reconnect, f)
}
func (e *ConnectionEvents) OnDisconnect(f func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.disconnect = append(e.disconnect, f)
}
func (e *ConnectionEvents) fire(reconnected bool) {
	e.mu.RLock()
	fs := append([]func(){}, e.disconnect...)
	if reconnected {
		fs = append([]func(){}, e.reconnect...)
	}
	e.mu.RUnlock()
	for _, f := range fs {
		f()
	}
}
func Connect(cfg config.NATS, creds string, events *ConnectionEvents) (*nats.Conn, error) {
	opts := []nats.Option{nats.Name(cfg.Name), nats.Timeout(cfg.Timeout), nats.MaxReconnects(-1), nats.UserCredentialBytes([]byte(creds)), nats.ReconnectHandler(func(*nats.Conn) { events.fire(true) }), nats.DisconnectErrHandler(func(*nats.Conn, error) { events.fire(false) })}
	if cfg.CAFile != "" {
		raw, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, errors.New("nats.ca_file: cannot read CA bundle")
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New("nats.ca_file: cannot load CA roots")
		}
		if !roots.AppendCertsFromPEM(raw) {
			return nil, errors.New("nats.ca_file: no CA certificates")
		}
		opts = append(opts, nats.Secure(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}))
	}
	conn, err := nats.Connect(strings.Join(cfg.Servers, ","), opts...)
	if err != nil {
		return nil, errors.New("nats: cannot connect (check servers, credentials and CA)")
	}
	return conn, nil
}
