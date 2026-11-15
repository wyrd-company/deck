package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestServiceStartDispatch(t *testing.T) {
	for _, args := range [][]string{{"service", "start", "--help"}, {"--config", "missing", "service", "start", "--help"}} {
		var out, errs bytes.Buffer
		if code := run(args, &out, &errs); code != 0 || !strings.Contains(out.String(), "SIGTERM") || errs.Len() != 0 {
			t.Fatal(code, out.String(), errs.String())
		}
	}
	for _, args := range [][]string{{"service", "start", "--config"}, {"service", "start", "extra"}, {"service", "start", "--config", "missing"}} {
		var out, errs bytes.Buffer
		if code := run(args, &out, &errs); code != 2 || errs.Len() == 0 {
			t.Fatal(code, out.String(), errs.String())
		}
	}
	// Startup must not replace the existing key-command hook.
	var out, errs bytes.Buffer
	if code := run([]string{"service", "host-key", "--help"}, &out, &errs); code != 0 || !strings.Contains(out.String(), "fingerprint") {
		t.Fatal(code, out.String(), errs.String())
	}
}
