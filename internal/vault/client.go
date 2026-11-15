// Package vault implements the KV v2 and Transit operations shared by the
// service and SSH client. It reads configuration only from config.Vault.
package vault

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/wyrd-company/deck/internal/config"
)

// Client owns its HTTP transport, cached KV reads and (for AppRole) login token.
// Close must be called at shutdown. Neither errors nor logs contain API bodies.
type Client struct {
	address   string
	http      *http.Client
	transport *http.Transport
	namespace string
	token     string
	issued    bool
	mu        sync.Mutex
	cache     map[string]map[string]any
}

// New builds a client without consulting BAO_*, VAULT_*, proxy or TLS environment
// variables. Only AppRole authentication makes a network request here.
func New(ctx context.Context, cfg config.Vault) (*Client, error) {
	if err := config.ValidateVaultAddress(cfg.Address); err != nil {
		return nil, err
	}
	if cfg.Auth.TokenFile != "" && cfg.Auth.AppRole != nil {
		return nil, errors.New("vault.auth: choose one authentication method")
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		return nil, errors.New("vault.timeout: must not be negative")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: timeout, ResponseHeaderTimeout: timeout}
	c := &Client{address: strings.TrimRight(cfg.Address, "/"), http: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}, transport: transport, namespace: cfg.Namespace, cache: make(map[string]map[string]any)}
	u, _ := url.Parse(cfg.Address)
	if u.Scheme == "unix" {
		socket := u.Path
		transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}
		c.address = "http://vault.local"
	}
	if u.Scheme == "https" {
		roots, err := systemRoots()
		if err != nil {
			return nil, fmt.Errorf("vault.ca_file: %w", err)
		}
		if cfg.CAFile != "" {
			raw, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, errors.New("vault.ca_file: cannot read CA bundle")
			}
			if !roots.AppendCertsFromPEM(raw) {
				return nil, errors.New("vault.ca_file: no certificates in CA bundle")
			}
		}
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	}
	var err error
	if cfg.Auth.TokenFile != "" {
		c.token, err = readCredential(cfg.Auth.TokenFile, "vault.auth.token_file")
	} else if a := cfg.Auth.AppRole; a != nil {
		var secret string
		secret, err = readCredential(a.SecretIDFile, "vault.auth.approle.secret_id_file")
		if err == nil {
			mount := a.Mount
			if mount == "" {
				mount = "approle"
			}
			var path string
			path, err = apiPath("auth", mount, "login")
			if err == nil {
				var response map[string]any
				response, err = c.request(ctx, http.MethodPost, path, map[string]any{"role_id": a.RoleID, "secret_id": secret})
				if err == nil {
					auth, _ := response["auth"].(map[string]any)
					c.token, _ = auth["client_token"].(string)
					if c.token == "" {
						err = errors.New("login response has no token")
					} else {
						c.issued = true
					}
				}
			}
		}
		if err != nil {
			err = fmt.Errorf("vault.auth.approle: %w", err)
		}
	}
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return c, nil
}
func readCredential(path, where string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("%s: cannot read credential file", where)
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("%s: credential file is empty", where)
	}
	return value, nil
}

// Close revokes only an AppRole-issued token. A token loaded from a file belongs
// to its operator and is never revoked by this client. Call after requests stop.
func (c *Client) Close(ctx context.Context) error {
	defer c.transport.CloseIdleConnections()
	if !c.issued {
		return nil
	}
	_, err := c.request(ctx, http.MethodPost, "auth/token/revoke-self", nil)
	if err != nil {
		return fmt.Errorf("vault.auth.approle: revoke login: %w", err)
	}
	c.issued = false
	c.token = ""
	return nil
}
func apiPath(parts ...string) (string, error) {
	var out []string
	for _, part := range parts {
		for _, segment := range strings.Split(part, "/") {
			if segment == "" || segment == "." || segment == ".." {
				return "", errors.New("API path must contain non-empty segments without dot traversal")
			}
			out = append(out, url.PathEscape(segment))
		}
	}
	return strings.Join(out, "/"), nil
}

func (c *Client) request(ctx context.Context, method, path string, body any) (map[string]any, error) {
	var input io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, errors.New("cannot encode API request")
		}
		input = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.address+"/v1/"+path, input)
	if err != nil {
		return nil, errors.New("cannot build API request")
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-Vault-Token", c.token)
	}
	if c.namespace != "" {
		req.Header.Set("X-Vault-Namespace", c.namespace)
	}
	response, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%w: request cancelled: %w", ErrUnavailable, ctx.Err())
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: request timed out: %w", ErrUnavailable, context.DeadlineExceeded)
		}
		return nil, fmt.Errorf("%w: check address, CA and timeout", ErrUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, &StatusError{Code: response.StatusCode}
	}
	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	const maxResponse = 16 << 20
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponse+1))
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read API response", ErrUnavailable)
	}
	if len(raw) > maxResponse {
		return nil, errors.New("API response exceeds size limit")
	}
	var result map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&result); err != nil {
		return nil, errors.New("invalid API response")
	}
	return result, nil
}

// systemRoots loads fixed OS trust paths rather than x509.SystemCertPool on
// Unix, whose SSL_CERT_FILE/SSL_CERT_DIR overrides would configure deck via env.
func unixRoots() (*x509.CertPool, error) {
	roots := x509.NewCertPool()
	for _, path := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt", "/etc/ssl/ca-bundle.pem", "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", "/etc/ssl/cert.pem"} {
		if raw, err := os.ReadFile(path); err == nil {
			roots.AppendCertsFromPEM(raw)
		}
	}
	return roots, nil
}
