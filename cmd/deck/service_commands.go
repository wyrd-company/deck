package main

import (
	"context"
	"io"
)

// Task 8 preserves this hook when it installs cli.App dispatch. Keep the public
// key commands chained ahead of service startup.
func init() {
	serviceCommand = func(ctx context.Context, args []string, stdout, stderr io.Writer) (int, bool) {
		if code, handled := keyCommand(ctx, args, stdout, stderr); handled {
			return code, true
		}
		return startCommand(ctx, args, stdout, stderr)
	}
}
