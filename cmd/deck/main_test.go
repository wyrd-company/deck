package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestRun(t *testing.T) {
	originalVersion := Version
	Version = "1.2.3"
	t.Cleanup(func() { Version = originalVersion })
	tests := []struct {
		name           string
		args           []string
		code           int
		stdout, stderr string
	}{
		{name: "version", args: []string{"--version"}, code: 0, stdout: "1.2.3\n"},
		{name: "service start help", args: []string{"service", "start", "--help"}, code: 1, stderr: "deck service start: not implemented\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			if code != tt.code || stdout.String() != tt.stdout || stderr.String() != tt.stderr {
				t.Errorf("run(%q) = code %d, stdout %q, stderr %q; want code %d, stdout %q, stderr %q", strings.Join(tt.args, " "), code, stdout.String(), stderr.String(), tt.code, tt.stdout, tt.stderr)
			}
		})
	}
}

func TestValidateCommands(t *testing.T) {
	for _, kind := range []string{"config", "service"} {
		t.Run(kind, func(t *testing.T) {
			name := "client"
			if kind == "service" {
				name = "service"
			}
			path := "../../docs/specifications/assets/configuration/examples/" + name + ".yml"
			for _, args := range [][]string{{kind, "validate", "--config", path}, {"--config", path, kind, "validate"}} {
				var stdout, stderr bytes.Buffer
				if code := run(args, &stdout, &stderr); code != 0 || stdout.String() != "ok "+path+"\n" || stderr.Len() != 0 {
					t.Fatalf("validate: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
				}
			}
			var stdout, stderr bytes.Buffer
			code := run([]string{kind, "validate", "--config", path, "--json"}, &stdout, &stderr)
			var result map[string]any
			err := json.Unmarshal(stdout.Bytes(), &result)
			if code != 0 || err != nil || !reflect.DeepEqual(result, map[string]any{"ok": true, "path": path}) || stderr.Len() != 0 {
				t.Fatalf("JSON validate: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestInvalidConfigCommand(t *testing.T) {
	for _, kind := range []string{"config", "service"} {
		t.Run(kind, func(t *testing.T) {
			name := "client"
			if kind == "service" {
				name = "service"
			}
			raw, err := os.ReadFile("../../docs/specifications/assets/configuration/examples/" + name + ".yml")
			if err != nil {
				t.Fatal(err)
			}
			var doc map[string]any
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			vault := doc["vault"].(map[string]any)
			vault["bogus"] = "do-not-print"
			vault["timeout"] = "ten"
			raw, err = yaml.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.yml")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			// These expectations are the CLI contract, not output obtained from
			// the loader: exact paths, reasons, ordering and one line per failure.
			first := path + ": vault.bogus: unknown field"
			second := path + ": vault.timeout: does not satisfy pattern in the configuration schema"
			for _, jsonMode := range []bool{false, true} {
				args := []string{kind, "validate", "--config", path}
				if jsonMode {
					args = append(args, "--json")
				}
				var stdout, stderr bytes.Buffer
				if code := run(args, &stdout, &stderr); code != 2 {
					t.Fatalf("invalid config status %d", code)
				}
				if jsonMode {
					var got map[string]any
					err := json.Unmarshal(stdout.Bytes(), &got)
					want := map[string]any{"error": map[string]any{
						"category": "invalid", "message": first + "\n" + second,
					}}
					if err != nil || !reflect.DeepEqual(got, want) || stderr.Len() != 0 {
						t.Fatalf("JSON: stdout=%q stderr=%q decode=%v; want exactly %#v", stdout.String(), stderr.String(), err, want)
					}
				} else {
					want := "deck: " + first + "\ndeck: " + second + "\n"
					if stderr.String() != want || stdout.Len() != 0 {
						t.Fatalf("text: stdout=%q stderr=%q; want empty stdout and stderr=%q", stdout.String(), stderr.String(), want)
					}
				}
			}
		})
	}
}

func TestValidationUsageJSON(t *testing.T) {
	for _, args := range [][]string{{"config", "validate", "--config", "--json"}, {"config", "validate", "--json", "--config"}, {"config", "validate", "--config=", "--json"}, {"config", "validate", "extra", "--json"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Fatalf("usage status %d for %v", code, args)
		}
		var result map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || stderr.Len() != 0 {
			t.Fatalf("JSON usage for %v: stdout=%s stderr=%s error=%v", args, stdout.String(), stderr.String(), err)
		}
	}
}

func TestClientDispatch(t *testing.T) {
	for _, command := range []string{"post", "reply", "inbox", "read", "history", "listen", "resolve", "channels", "skill"} {
		var out, err bytes.Buffer
		if code := run([]string{command, "--help"}, &out, &err); code != 0 || !strings.Contains(out.String(), "usage: deck "+command) || !strings.Contains(out.String(), "Example:") || err.Len() != 0 {
			t.Fatalf("%s: %d %q %q", command, code, out.String(), err.String())
		}
	}
	var out, err bytes.Buffer
	if code := run(nil, &out, &err); code != 2 || !strings.Contains(err.String(), "usage: deck") {
		t.Fatalf("usage: %d %q", code, err.String())
	}
}
