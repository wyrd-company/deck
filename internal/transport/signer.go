package transport

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/vault"
	"golang.org/x/crypto/ssh"
)

// TransitSigner holds only a public key. Its pinned Transit version is used
// throughout the handshake, including when the key rotates concurrently.
type TransitSigner struct {
	client *vault.Client
	ctx    context.Context
	ref    config.TransitKey
	key    vault.TransitKey
	public ssh.PublicKey
}

func NewTransitSigner(ctx context.Context, client *vault.Client, ref config.TransitKey) (*TransitSigner, error) {
	key, err := client.ReadTransitKey(ctx, ref.Mount, ref.Name)
	if err != nil {
		return nil, err
	}
	public, err := ssh.NewPublicKey(key.PublicKey)
	if err != nil {
		return nil, errors.New("key: invalid Transit public key")
	}
	return &TransitSigner{client: client, ctx: ctx, ref: ref, key: key, public: public}, nil
}
func (s *TransitSigner) PublicKey() ssh.PublicKey { return s.public }
func (s *TransitSigner) Sign(_ io.Reader, data []byte) (*ssh.Signature, error) {
	blob, err := s.client.SignTransit(s.ctx, s.ref.Mount, s.ref.Name, s.key, data)
	if err != nil {
		return nil, err
	}
	return &ssh.Signature{Format: ssh.KeyAlgoED25519, Blob: blob}, nil
}

// SecretReader is shared by host-key loading and service startup. Reads at
// startup are serial, so no request-time KV traffic or concurrent fills occur.
type SecretReader interface {
	ReadSecret(context.Context, string, config.SecretRef) (string, error)
}

func ReadHostKey(ctx context.Context, reader SecretReader, ref config.SecretRef) (ssh.Signer, error) {
	raw, err := reader.ReadSecret(ctx, "host_key", ref)
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey([]byte(raw))
	if err != nil || signer.PublicKey().Type() != ssh.KeyAlgoED25519 {
		return nil, errors.New("host_key: expected an unencrypted OpenSSH ed25519 private key")
	}
	if !strings.HasPrefix(raw, "-----BEGIN OPENSSH PRIVATE KEY-----") {
		return nil, errors.New("host_key: expected an OpenSSH private key")
	}
	return signer, nil
}

// PublicIdentity is safe to print or copy into configuration.
type PublicIdentity struct {
	PublicKey   string `json:"public_key"`
	Fingerprint string `json:"fingerprint"`
}

func DescribeKey(key ssh.PublicKey, comment string) PublicIdentity {
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key)))
	if comment != "" {
		line += " " + comment
	}
	return PublicIdentity{PublicKey: line, Fingerprint: ssh.FingerprintSHA256(key)}
}
