package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/service/health"
)

// healthTimeout stays under the image's 3-second HEALTHCHECK timeout.
const healthTimeout = 2 * time.Second

// healthCommand requests /healthz from listen.health on the loopback address,
// prints the body and exits 0 when healthy: 1 when the service answers
// unhealthy, 4 when it cannot be reached, 2 for a usage or configuration error.
func healthCommand(ctx context.Context, args []string, stdout, stderr io.Writer) (int, bool) {
	options, err := parseServiceFlags(args)
	if len(options.words) < 2 || options.words[0] != "service" || options.words[1] != "health" {
		return 0, false
	}
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 2, err), true
	}
	if options.help {
		fmt.Fprintln(stdout, "usage: deck service health [--config PATH] [--json]\nRequest the running service's /healthz on the loopback address and print it.\nExample: deck service health --config /etc/deck/config.yml")
		return 0, true
	}
	if len(options.words) != 2 {
		return serviceFailure(stdout, stderr, options.json, 2, errors.New("unexpected argument; use --help")), true
	}
	path := options.path
	if path == "" {
		path = config.ServicePath
	}
	cfg, err := config.LoadService(path)
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 2, err), true
	}
	address, err := health.LoopbackAddress(cfg.Listen.Health)
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 2, errors.New("listen.health: invalid address")), true
	}
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	code, body, err := health.Probe(ctx, address)
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 4, fmt.Errorf("health: cannot reach %s", address)), true
	}
	fmt.Fprintln(stdout, string(body))
	if code != 200 {
		if !options.json {
			fmt.Fprintln(stderr, "deck: service unhealthy")
		}
		return 1, true
	}
	return 0, true
}
