// Package testutil starts real, isolated JetStream servers for service tests.
package testutil

import (
	"context"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"testing"
	"time"
)

func NATS(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return s, nc, js
}
func Until(t *testing.T, f func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !f() {
		select {
		case <-ctx.Done():
			t.Fatal("condition did not become true")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// JWTNATS uses an operator/account/user trust chain, with a JetStream-enabled
// account. The generated seed/credentials are process-local test fixtures.
func JWTNATS(t *testing.T) (*server.Server, string) {
	t.Helper()
	op, _ := nkeys.CreateOperator()
	opPub, _ := op.PublicKey()
	account, _ := nkeys.CreateAccount()
	accountPub, _ := account.PublicKey()
	claims := jwt.NewAccountClaims(accountPub)
	claims.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: jwt.NoLimit, DiskStorage: jwt.NoLimit, Streams: jwt.NoLimit, Consumer: jwt.NoLimit}
	accountJWT, err := claims.Encode(op)
	if err != nil {
		t.Fatal(err)
	}
	user, _ := nkeys.CreateUser()
	userPub, _ := user.PublicKey()
	userJWT, err := jwt.NewUserClaims(userPub).Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := user.Seed()
	credentials, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &server.MemAccResolver{}
	_ = resolver.Store(accountPub, accountJWT)
	system, _ := nkeys.CreateAccount()
	systemPub, _ := system.PublicKey()
	systemJWT, _ := jwt.NewAccountClaims(systemPub).Encode(op)
	_ = resolver.Store(systemPub, systemJWT)
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true, TrustedKeys: []string{opPub}, AccountResolver: resolver, SystemAccount: systemPub})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("JWT NATS not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s, string(credentials)
}
