package discord

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/disgoorg/disgo/rest"
)

// disgo's records keep their message, safe attributes and an error's kind and
// codes; URLs, bodies, tokens and content never reach the service's log, and
// records below warning are dropped.
func TestDisgoLoggerRedacts(t *testing.T) {
	var out bytes.Buffer
	logger := DisgoLogger(slog.NewJSONHandler(&out, nil)).With("name", "gateway", "token", "Bot secret-token-value")
	logger.Info("debug detail", "body", "hidden")
	upstream := &rest.Error{Code: 50013, Message: "Missing Permissions: secret detail", Response: &http.Response{StatusCode: 403}, RqBody: []byte("request body content")}
	logger.Error("error connecting to the gateway", "err", upstream, "url", "wss://gateway/?token=secret-token-value", "body", "response body content", "code", 4004, "data", "message content")
	logger.Warn("failed", "err", errors.New("wrapped secret-token-value"))
	text := out.String()
	for _, hidden := range []string{"secret-token-value", "body content", "message content", "secret detail", "debug detail", "wss://"} {
		if strings.Contains(text, hidden) {
			t.Fatalf("log holds %q: %s", hidden, text)
		}
	}
	for _, kept := range []string{`"msg":"error connecting to the gateway"`, `"code":4004`, `"error":"discord rest error: status 403 code 50013"`, `"name":"gateway"`, `"component":"disgo"`, `"error":"*errors.errorString"`} {
		if !strings.Contains(text, kept) {
			t.Fatalf("log lacks %s: %s", kept, text)
		}
	}
}
