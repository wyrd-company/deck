package vault

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/wyrd-company/deck/internal/config"
)

// ReadSecret resolves one configured KV v2 field. where is its configuration
// path (for example bots[0].token), so every failure points to the reference.
// Each mount/path is read once on this client, including concurrent callers.
func (c *Client) ReadSecret(ctx context.Context, where string, ref config.SecretRef) (string, error) {
	if ref.Mount == "" {
		return "", fmt.Errorf("%s: KV mount is required (apply configuration defaults first)", where)
	}
	path, err := apiPath(ref.Mount, "data", ref.Path)
	if err != nil {
		return "", fmt.Errorf("%s: invalid KV path", where)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	data, found := c.cache[path]
	if !found {
		response, err := c.request(ctx, http.MethodGet, path, nil)
		if err != nil {
			return "", fmt.Errorf("%s: read KV secret: %w", where, err)
		}
		envelope, _ := response["data"].(map[string]any)
		data, _ = envelope["data"].(map[string]any)
		if data == nil {
			return "", fmt.Errorf("%s: KV response has no secret data", where)
		}
		c.cache[path] = data
	}
	value, ok := data[ref.Field]
	if !ok {
		return "", fmt.Errorf("%s: secret field is missing", where)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s: secret field must be a string", where)
	}
	return text, nil
}

// Resolve reads the service's references in stable order. Values remain in
// memory and are returned keyed by configuration path; errors hold no secrets.
func (c *Client) Resolve(ctx context.Context, refs map[string]config.SecretRef) (map[string]string, error) {
	values := make(map[string]string, len(refs))
	paths := make([]string, 0, len(refs))
	for path := range refs {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var problems []error
	for _, path := range paths {
		value, err := c.ReadSecret(ctx, path, refs[path])
		if err != nil {
			problems = append(problems, err)
		} else {
			values[path] = value
		}
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}
	return values, nil
}
