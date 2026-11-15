package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		{name: "usage", code: 1, stderr: "usage: deck --version | deck service start [--help]\n"},
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
			if code := run([]string{kind, "validate", "--config", path, "--json"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), `"ok":true`) || stderr.Len() != 0 {
				t.Fatalf("JSON validate: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestInvalidConfigCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("service: {}\nvault: {}\nkey: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, jsonMode := range []bool{false, true} {
		args := []string{"config", "validate", "--config", path}
		if jsonMode {
			args = append(args, "--json")
		}
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != 2 {
			t.Fatalf("invalid config status %d", code)
		}
		output := stderr.String()
		if jsonMode {
			output = stdout.String()
			var result map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || stderr.Len() != 0 {
				t.Fatalf("JSON: %q %v", output, err)
			}
		} else if stdout.Len() != 0 {
			t.Fatal("text errors went to stdout")
		}
		for _, field := range []string{"service.address", "service.host_key", "vault.address", "key.name"} {
			if !strings.Contains(output, field) {
				t.Errorf("missing field %s: %s", field, output)
			}
		}
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
