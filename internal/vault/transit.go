package vault

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// TransitKey is the latest public version read from an ed25519 Transit key.
// Pin this Version when signing, so rotation between read and sign cannot
// produce a signature from a different SSH identity.
type TransitKey struct {
	PublicKey ed25519.PublicKey
	Version   int
}

func (c *Client) ReadTransitKey(ctx context.Context, mount, name string) (TransitKey, error) {
	path, err := apiPath(mount, "keys", name)
	if err != nil {
		return TransitKey{}, fmt.Errorf("key: invalid Transit key path")
	}
	response, err := c.request(ctx, http.MethodGet, path, nil)
	if err != nil {
		return TransitKey{}, fmt.Errorf("key: read Transit key: %w", err)
	}
	data, _ := response["data"].(map[string]any)
	if data["type"] != "ed25519" {
		return TransitKey{}, errors.New("key: Transit key must be ed25519")
	}
	version, err := number(data["latest_version"])
	if err != nil || version < 1 {
		return TransitKey{}, errors.New("key: Transit key has no latest version")
	}
	keys, _ := data["keys"].(map[string]any)
	latest, _ := keys[strconv.Itoa(version)].(map[string]any)
	encoded, _ := latest["public_key"].(string)
	publicKey, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		return TransitKey{}, errors.New("key: invalid ed25519 public key in Transit response")
	}
	return TransitKey{PublicKey: publicKey, Version: version}, nil
}
func number(v any) (int, error) {
	if n, ok := v.(json.Number); ok {
		return strconv.Atoi(string(n))
	}
	return 0, errors.New("not a number")
}

// SignTransit asks Transit to sign data at the version returned by
// ReadTransitKey, verifies the signature against that public key, and returns
// the verified ed25519 signature bytes.
func (c *Client) SignTransit(ctx context.Context, mount, name string, key TransitKey, data []byte) ([]byte, error) {
	version := key.Version
	if version < 1 {
		return nil, errors.New("key: a positive Transit key version is required")
	}
	if len(key.PublicKey) != ed25519.PublicKeySize {
		return nil, errors.New("key: a valid ed25519 public key is required")
	}
	path, err := apiPath(mount, "sign", name)
	if err != nil {
		return nil, errors.New("key: invalid Transit key path")
	}
	response, err := c.request(ctx, http.MethodPost, path, map[string]any{"input": base64.StdEncoding.EncodeToString(data), "key_version": version})
	if err != nil {
		return nil, fmt.Errorf("key: sign with Transit: %w", err)
	}
	result, _ := response["data"].(map[string]any)
	encoded, _ := result["signature"].(string)
	parts := strings.Split(encoded, ":")
	if len(parts) != 3 || parts[0] != "vault" || parts[1] != "v"+strconv.Itoa(version) {
		return nil, errors.New("key: invalid signature version in Transit response")
	}
	signature, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil || len(signature) != ed25519.SignatureSize {
		return nil, errors.New("key: invalid ed25519 signature in Transit response")
	}
	if !ed25519.Verify(key.PublicKey, data, signature) {
		return nil, errors.New("key: Transit signature does not verify with the read public key")
	}
	return signature, nil
}
