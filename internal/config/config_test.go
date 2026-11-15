package config

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

const fixtures = "../../docs/specifications/assets/configuration/"

func example(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(fixtures + "examples/" + name + ".yml")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestContractFixtures(t *testing.T) {
	c, err := ParseClient(example(t, "client"), "/tmp/deck/config.yml")
	if err != nil {
		t.Fatal(err)
	}
	if c.Store.Path != "/tmp/deck/deck.db" || c.Service.Timeout != 15*time.Second || c.Vault.Timeout != 10*time.Second || c.Key.Mount != "transit" || c.Vault.KVMount != "secret" {
		t.Fatalf("client defaults: %+v", c)
	}
	s, err := ParseService(example(t, "service"))
	if err != nil {
		t.Fatal(err)
	}
	if s.HostKey.Mount != "kv" || s.Bots[0].Token.Mount != "kv" || s.NATS.Creds.Mount != "kv" || s.NATS.Name != "deck" || s.NATS.Timeout != 10*time.Second || !s.NATS.Threads.Manage || s.NATS.Threads.MaxAge != 0 {
		t.Fatalf("service defaults: %+v", s)
	}
	paths, err := filepath.Glob(fixtures + "invalid/*.yml")
	if err != nil || len(paths) == 0 {
		t.Fatal("missing invalid fixtures")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(filepath.Base(path), "client-") {
				_, err = ParseClient(raw, "/tmp/config.yml")
			} else {
				_, err = ParseService(raw)
			}
			if err == nil {
				t.Fatal("invalid design fixture accepted")
			}
		})
	}
}
func mutate(t *testing.T, name string, f func(map[string]any)) []byte {
	t.Helper()
	var m map[string]any
	if err := yaml.Unmarshal(example(t, name), &m); err != nil {
		t.Fatal(err)
	}
	f(m)
	raw, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func assertPaths(t *testing.T, err error, paths ...string) {
	t.Helper()
	var fields Errors
	if !errors.As(err, &fields) {
		t.Fatalf("want field errors, got %v", err)
	}
	for _, path := range paths {
		found := false
		for _, field := range fields {
			if field.Path == path {
				found = true
			}
		}
		if !found {
			t.Errorf("missing path %s in %v", path, fields)
		}
	}
}
func TestClientReportsAllFields(t *testing.T) {
	raw := mutate(t, "client", func(m map[string]any) {
		m["default_persona"] = "missing"
		ch := object(m["channels"])
		ch["default"] = "missing"
		ch["allow"] = []any{"missing-allow"}
		ch["deny"] = []any{"missing-deny"}
		object(m["service"])["unexpected"] = "private-value"
		object(m["vault"])["address"] = "http://external.invalid:8200"
	})
	_, err := ParseClient(raw, "/tmp/config.yml")
	assertPaths(t, err, "default_persona", "channels.default", "channels.allow[0]", "channels.deny[0]", "service.unexpected", "vault.address")
	if strings.Contains(err.Error(), "private-value") {
		t.Fatal("rejected value leaked")
	}
}
func TestServiceReportsAllCrossFields(t *testing.T) {
	raw := mutate(t, "service", func(m map[string]any) {
		bots := array(m["bots"])
		m["bots"] = append(bots, bots[0])
		keys := array(m["keys"])
		second := object(keys[1])
		first := object(keys[0])
		second["name"] = first["name"]
		second["public_key"] = first["public_key"].(string) + " changed-comment"
		object(array(second["grants"])[0])["bot"] = "missing"
	})
	_, err := ParseService(raw)
	assertPaths(t, err, "bots[1].name", "keys[1].name", "keys[1].public_key", "keys[1].grants[0].bot")
}
func TestSchemaAndSemanticValidation(t *testing.T) {
	tests := []struct {
		name, path string
		change     func(map[string]any)
	}{
		{"unknown vault field", "vault.token", func(m map[string]any) { object(m["vault"])["token"] = "do-not-print" }},
		{"missing field", "service.host_key", func(m map[string]any) { delete(object(m["service"]), "host_key") }},
		{"relative path", "store.path", func(m map[string]any) { m["store"] = map[string]any{"path": "~/db"} }},
		{"overflow duration", "vault.timeout", func(m map[string]any) { object(m["vault"])["timeout"] = "999999999999999h" }},
		{"invalid public key", "service.host_key", func(m map[string]any) { object(m["service"])["host_key"] = "ssh-ed25519 do-not-print" }},
		{"invalid port", "service.address", func(m map[string]any) { object(m["service"])["address"] = "host:99999" }},
		{"unknown top field", "token", func(m map[string]any) { m["token"] = "do-not-print" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseClient(mutate(t, "client", tt.change), "/tmp/config.yml")
			assertPaths(t, err, tt.path)
			if strings.Contains(err.Error(), "do-not-print") {
				t.Fatal("value leaked")
			}
		})
	}
}
func TestDefaultValuesAndExplicitFalse(t *testing.T) {
	raw := mutate(t, "service", func(m map[string]any) {
		delete(m, "listen")
		delete(object(m["vault"]), "kv_mount")
		n := object(m["nats"])
		n["stream"] = map[string]any{"manage": false}
		n["threads"] = map[string]any{"manage": false}
		delete(n, "subject_prefix")
	})
	s, err := ParseService(raw)
	if err != nil {
		t.Fatal(err)
	}
	if s.Listen.SSH != ":2222" || s.Listen.Health != ":8080" || s.Vault.KVMount != "secret" || s.NATS.SubjectPrefix != "deck" || s.NATS.Stream.MaxAge != 168*time.Hour || s.NATS.Stream.DuplicateWindow != 2*time.Minute || s.NATS.Stream.Replicas != 1 || s.NATS.Threads.Replicas != 1 || s.NATS.Stream.Manage || s.NATS.Threads.Manage {
		t.Fatalf("defaults/false: %+v", s)
	}
}
func TestMalformedDocuments(t *testing.T) {
	for _, raw := range []string{"", "[]", "vault: [", "a: 1\na: 2", "---\n{}\n---\n{}", strings.Repeat(" ", MaxSize+1), "vault: &v {address: *v}"} {
		if _, err := ParseClient([]byte(raw), "config.yml"); err == nil {
			t.Errorf("accepted malformed document of %d bytes", len(raw))
		}
	}
}
func TestPasswdParsingAndExplicitConfigPath(t *testing.T) {
	home, err := homeFromPasswd(strings.NewReader("agent:x:123:123::/actual/home:/bin/sh\n"), "123")
	if err != nil || home != "/actual/home" {
		t.Fatalf("home %q: %v", home, err)
	}
	_, err = homeFromPasswd(strings.NewReader("root:x:0:0::/root:/bin/sh\n"), "123")
	if err == nil || !strings.Contains(err.Error(), "UID 123") || !strings.Contains(err.Error(), "--config") {
		t.Fatalf("missing UID: %v", err)
	}
	p, err := ClientPath("/explicit/config.yml")
	if err != nil || p != "/explicit/config.yml" {
		t.Fatalf("explicit path %q: %v", p, err)
	}
	if _, err := ParseClient(example(t, "client"), "/actual/config.yml"); err != nil {
		t.Fatal(err)
	}
}

func TestAliasNamedTimeout(t *testing.T) {
	raw := mutate(t, "client", func(m map[string]any) {
		object(object(m["aliases"])["channels"])["timeout"] = "helper/123"
		object(object(m["aliases"])["users"])["timeout"] = "123"
		object(m["channels"])["default"] = "timeout"
	})
	if _, err := ParseClient(raw, "/tmp/config.yml"); err != nil {
		t.Fatal(err)
	}
}

func TestExactInvalidFieldPaths(t *testing.T) {
	tests := []struct {
		name, kind string
		change     func(map[string]any)
		want       []string
	}{
		{"vault sibling stays valid", "client", func(m map[string]any) { object(m["vault"])["timeout"] = "ten" }, []string{"vault.timeout"}},
		{"unknown vault field", "client", func(m map[string]any) { object(m["vault"])["bogus"] = "private" }, []string{"vault.bogus"}},
		{"service vault sibling stays valid", "service", func(m map[string]any) { object(m["vault"])["timeout"] = "ten" }, []string{"vault.timeout"}},
		{"service wrapper property stays valid", "service", func(m map[string]any) { object(m["vault"])["bogus"] = "private" }, []string{"vault.bogus"}},
		{"one line per bad field", "client", func(m map[string]any) {
			object(m["service"])["address"] = "bad"
			object(m["vault"])["address"] = "http://bad.invalid"
		}, []string{"service.address", "vault.address"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := mutate(t, tt.kind, tt.change)
			var err error
			if tt.kind == "client" {
				_, err = ParseClient(raw, "/tmp/config.yml")
			} else {
				_, err = ParseService(raw)
			}
			var fields Errors
			if !errors.As(err, &fields) {
				t.Fatal(err)
			}
			var got []string
			for _, field := range fields {
				got = append(got, field.Path)
				if strings.Contains(field.Reason, "satisfy  ") {
					t.Errorf("empty reason: %v", field)
				}
				if strings.HasSuffix(field.Path, "bogus") && field.Reason != "unknown field" {
					t.Errorf("unknown field reason: %v", field)
				}
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("paths %v, want exactly %v", got, tt.want)
			}
		})
	}
}

func TestVaultAddressRejectsQueryAndFragment(t *testing.T) {
	for _, base := range []string{"https://vault.example", "http://127.0.0.1:8200", "unix:///run/bao.sock"} {
		if err := ValidateVaultAddress(base); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{"?", "#", "?q=1", "#fragment"} {
			address := base + suffix
			if err := ValidateVaultAddress(address); err == nil {
				t.Errorf("accepted query/fragment address %q", address)
			}
		}
	}
	for _, kind := range []string{"client", "service"} {
		for _, suffix := range []string{"?", "#"} {
			raw := mutate(t, kind, func(m map[string]any) {
				object(m["vault"])["address"] = "https://vault.example" + suffix
			})
			var err error
			if kind == "client" {
				_, err = ParseClient(raw, "/tmp/config.yml")
			} else {
				_, err = ParseService(raw)
			}
			assertPaths(t, err, "vault.address")
		}
	}
}
