package main

import (
	"bytes"
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
