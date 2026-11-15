package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestDefaultClientPathIgnoresEnvironment(t *testing.T) {
	for _, name := range []string{"BAO_ADDR", "VAULT_ADDR", "VAULT_TOKEN", "HTTPS_PROXY", "HOME", "USER", "XDG_CONFIG_HOME"} {
		t.Setenv(name, "/do-not-use")
	}
	// Derive the expected home independently from the real passwd file; call
	// production ClientPath with no explicit override, so userHome is exercised.
	uid := strconv.Itoa(os.Getuid())
	raw, err := os.ReadFile("/etc/passwd")
	if err != nil {
		t.Fatal(err)
	}
	var home string
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) >= 7 && fields[2] == uid && filepath.IsAbs(fields[5]) {
			home = fields[5]
			break
		}
	}
	got, err := ClientPath("")
	if home == "" {
		if err == nil || !strings.Contains(err.Error(), "UID "+uid) || !strings.Contains(err.Error(), "--config") {
			t.Fatalf("missing passwd entry: path %q, error %v", got, err)
		}
		return
	}
	want := filepath.Join(home, ".config", "deck", "config.yml")
	if err != nil || got != want {
		t.Fatalf("default path %q, %v; want passwd path %q", got, err, want)
	}
}
