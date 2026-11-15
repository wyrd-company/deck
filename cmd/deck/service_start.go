package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/wyrd-company/deck/internal/cli"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service"
	"github.com/wyrd-company/deck/internal/vault"
)

func startCommand(ctx context.Context, args []string, stdout, stderr io.Writer) (int, bool) {
	options, err := parseServiceFlags(args)
	if len(options.words) < 2 || options.words[0] != "service" || options.words[1] != "start" {
		return 0, false
	}
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 2, err), true
	}
	if options.help {
		fmt.Fprintln(stdout, "usage: deck service start [--config PATH]\nRun the configured service until SIGINT or SIGTERM.\nExample: deck service start --config /etc/deck/config.yml")
		return 0, true
	}
	if len(options.words) != 2 {
		return serviceFailure(stdout, stderr, options.json, 2, errors.New("unexpected argument; use --help")), true
	}
	// Keep signal ownership here: cancellation must reach the exit-status check,
	// including config loading, Vault login and every startup secret read.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop) // restore default handling for a second signal
	path := options.path
	if path == "" {
		path = config.ServicePath
	}
	cfg, err := config.LoadService(path)
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 2, err), true
	}
	secrets, err := vault.New(ctx, cfg.Vault)
	if err == nil {
		defer closeVault(secrets)
		err = service.Run(ctx, cfg, secrets)
	}
	if ctx.Err() != nil {
		return 0, true
	}
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, cli.ExitCode(err), err), true
	}
	return 0, true
}
