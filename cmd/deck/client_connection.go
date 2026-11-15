package main

import (
	"context"

	"github.com/wyrd-company/deck/internal/cli"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/transport"
)

func init() {
	cli.DefaultDial = func(ctx context.Context, cfg *config.Client) (cli.Connection, error) {
		c, err := transport.DialClient(ctx, cfg)
		if err != nil {
			return cli.Connection{}, err
		}
		return cli.Connection{API: c.API, Fingerprint: c.Fingerprint, Close: c.Close}, nil
	}
}
