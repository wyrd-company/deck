package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wyrd-company/deck/internal/service/health"
)

func healthConfig(t *testing.T, address string) string {
	t.Helper()
	raw, err := os.ReadFile("../../docs/specifications/assets/configuration/examples/service.yml")
	if err != nil {
		t.Fatal(err)
	}
	cfg := strings.Replace(string(raw), `health: ":8080"`, `health: "`+address+`"`, 1)
	path := filepath.Join(t.TempDir(), "config.yml")
	if err = os.WriteFile(path, []byte(cfg), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// `deck service health` prints the listener's body and exits 0 when healthy,
// 1 when the service answers unhealthy, 4 when it cannot be reached, and 2
// for a configuration error, as task 10's image HEALTHCHECK relies on.
func TestServiceHealthCommand(t *testing.T) {
	healthy := true
	state := func() string {
		if healthy {
			return "ready"
		}
		return "disconnected"
	}
	listener, err := health.Listen("127.0.0.1:0", health.Checks{Bots: map[string]func() string{"helper": state}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close(context.Background())
	path := healthConfig(t, listener.Addr().String())
	for _, c := range []struct {
		healthy bool
		code    int
		status  string
	}{{true, 0, "ok"}, {false, 1, "fail"}} {
		healthy = c.healthy
		for _, args := range [][]string{{"service", "health", "--config", path}, {"service", "health", "--config", path, "--json"}} {
			var out, errs bytes.Buffer
			code := run(args, &out, &errs)
			var body map[string]any
			if code != c.code || json.Unmarshal(out.Bytes(), &body) != nil || body["status"] != c.status {
				t.Fatalf("%v healthy=%t: code=%d stdout=%q stderr=%q", args, c.healthy, code, out.String(), errs.String())
			}
		}
	}
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := closed.Addr().String()
	_ = closed.Close()
	var out, errs bytes.Buffer
	if code := run([]string{"service", "health", "--config", healthConfig(t, address)}, &out, &errs); code != 4 || out.Len() != 0 || !strings.Contains(errs.String(), "deck: health: cannot reach "+address) {
		t.Fatalf("unreachable: code=%d stdout=%q stderr=%q", code, out.String(), errs.String())
	}
	out.Reset()
	errs.Reset()
	if code := run([]string{"service", "health", "--config", filepath.Join(t.TempDir(), "missing.yml")}, &out, &errs); code != 2 {
		t.Fatalf("invalid configuration: code=%d stderr=%q", code, errs.String())
	}
	out.Reset()
	if code := run([]string{"service", "health", "--help"}, &out, &errs); code != 0 || !strings.Contains(out.String(), "/healthz") {
		t.Fatal("help", code, out.String())
	}
}
