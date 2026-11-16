package transport

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	wishssh "charm.land/ssh"
	"github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
)

// A valid ed25519 blob with the wrong format lets authentication continue to
// the next signer, leaving accepted query A and B in a multi-entry cache.
// A bad ed25519 Verify instead ends the connection, missing the vulnerable path.
type badFormatSigner struct {
	ssh.Signer
	attempts int
}

func (s *badFormatSigner) Sign(random io.Reader, data []byte) (*ssh.Signature, error) {
	s.attempts++
	signature, err := s.Signer.Sign(random, data)
	if err != nil {
		return nil, err
	}
	signature.Format = ssh.KeyAlgoRSASHA256
	return signature, nil
}
func TestLivePublicKeyCallbackCache(t *testing.T) {
	b := transportBao(t)
	a, bb := b.signer(t, "cache-a"), b.signer(t, "cache-b")
	host := testHost(t)
	keys := []config.Key{{Name: "A", PublicKey: DescribeKey(a.PublicKey(), "").PublicKey}, {Name: "B", PublicKey: DescribeKey(bb.PublicKey(), "").PublicKey}}
	address := serveTest(t, keys, host, &identityHandler{})
	badA, badB := &badFormatSigner{Signer: a}, &badFormatSigner{Signer: bb}
	raw, err := ssh.Dial("tcp", address, &ssh.ClientConfig{User: "deck", Auth: []ssh.AuthMethod{ssh.PublicKeys(badA, badB, a)}, HostKeyCallback: ssh.FixedHostKey(host.PublicKey()), Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if badA.attempts != 1 || badB.attempts != 1 {
		t.Fatalf("A/B queries were not exercised: %d/%d", badA.attempts, badB.attempts)
	}
	channel, requests, err := raw.OpenChannel("session", nil)
	if err != nil {
		t.Fatal(err)
	}
	go ssh.DiscardRequests(requests)
	ok, err := channel.SendRequest("subsystem", true, ssh.Marshal(struct{ Name string }{subsystem}))
	if err != nil || !ok {
		t.Fatalf("subsystem: %v %v", ok, err)
	}
	conn := &channelConn{ReadWriteCloser: channel, local: raw.LocalAddr(), remote: raw.RemoteAddr(), done: make(chan struct{})}
	client, err := grpc.NewClient("passthrough:///deck", grpc.WithTransportCredentials(sshCredentials{}), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := deckv1.NewDeckServiceClient(client).ListChannels(ctx, &deckv1.ListChannelsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Channels) != 1 || result.Channels[0].Name != "A" {
		t.Fatalf("queried B replaced signed A: %v", result)
	}
	if b.signs.Load() != 3 {
		t.Fatalf("wanted bad-A, bad-B, valid-A signatures; got %d", b.signs.Load())
	}
}

// Context deliberately holds an offered identity that differs from PublicKey.
// The production identity accessor must use only the successful signed key.
type queriedContext struct{ wishssh.Context }

func (queriedContext) Value(any) any { return "B" }

type signedSession struct {
	wishssh.Session
	key ssh.PublicKey
}

func (s signedSession) PublicKey() wishssh.PublicKey { return s.key }
func (s signedSession) Context() wishssh.Context     { return queriedContext{} }
func TestSignedSessionIdentity(t *testing.T) {
	a, bb := testHost(t), testHost(t)
	keys := map[string]string{ssh.FingerprintSHA256(a.PublicKey()): "A", ssh.FingerprintSHA256(bb.PublicKey()): "B"}
	if got := signedKeyName(signedSession{key: a.PublicKey()}, keys); got != "A" {
		t.Fatalf("queried context became identity: %q", got)
	}
	if got := signedKeyName(signedSession{key: bb.PublicKey()}, keys); got != "B" {
		t.Fatalf("signed B: %q", got)
	}
	if got := signedKeyName(signedSession{}, keys); got != "" {
		t.Fatalf("nil signed key: %q", got)
	}
	if got := signedKeyName(signedSession{key: testHost(t).PublicKey()}, keys); got != "" {
		t.Fatalf("unknown signed key: %q", got)
	}
}
