package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	// The service logs JSON lines to stderr; disgo's logger shares the handler.
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	previous := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(previous)
	path := options.path
	if path == "" {
		path = config.ServicePath
	}
	cfg, err := config.LoadService(path)
	if err != nil {
		for _, line := range strings.Split(err.Error(), "\n") {
			logger.Error("service start failed", "step", "load the configuration", "config", path, "error", line)
		}
		return startFailure(stdout, options.json, 2, err), true
	}
	logger.Info("service starting", "config", path)
	secrets, err := vault.New(ctx, cfg.Vault)
	if err == nil {
		defer closeVault(secrets)
		err = service.Run(ctx, cfg, secrets)
	} else {
		err = service.StartupFailure("open the OpenBao or Vault client", "vault", safeVaultMessage(err), err)
	}
	if ctx.Err() != nil && (err == nil || errors.Is(err, ctx.Err())) {
		logger.Info("service stopped", "config", path)
		return 0, true
	}
	if err != nil {
		step, field := "serve", ""
		var failure *service.Failure
		if errors.As(err, &failure) {
			step, field = failure.Step, failure.Field
		}
		logger.Error("service start failed", "step", step, "field", field, "config", path, "error", err.Error())
		return startFailure(stdout, options.json, 1, err), true
	}
	logger.Info("service stopped", "config", path)
	return 0, true
}

// safeVaultMessage keeps the vault package's own safe text, which names a
// configuration field and never carries a token or response body.
func safeVaultMessage(err error) string {
	text := err.Error()
	for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
		text = strings.TrimSuffix(text, ": "+sentinel.Error())
	}
	return text
}

// startFailure writes the JSON error object in JSON mode; the log line on
// stderr is the diagnostic either way.
func startFailure(stdout io.Writer, jsonMode bool, code int, err error) int {
	if jsonMode {
		category := map[int]string{1: "failed", 2: "invalid"}[code]
		_ = json.NewEncoder(stdout).Encode(map[string]any{"error": map[string]string{"category": category, "message": err.Error()}})
	}
	return code
}
