package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/wyrd-company/deck/internal/cli"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/transport"
	"github.com/wyrd-company/deck/internal/vault"
)

type commandOptions struct {
	words      []string
	path       string
	json, help bool
}

func parseServiceFlags(args []string) (commandOptions, error) {
	var result commandOptions
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--config":
			i++
			if i >= len(args) || args[i] == "" {
				return result, errors.New("--config requires a path")
			}
			result.path = args[i]
		case strings.HasPrefix(arg, "--config="):
			result.path = strings.TrimPrefix(arg, "--config=")
			if result.path == "" {
				return result, errors.New("--config requires a path")
			}
		case arg == "--json":
			result.json = true
		case arg == "--help" || arg == "-h":
			result.help = true
		default:
			result.words = append(result.words, arg)
		}
	}
	return result, nil
}

// keyCommand handles the public-key commands without opening a service
// connection or a local affinity store.
func keyCommand(ctx context.Context, args []string, stdout, stderr io.Writer) (int, bool) {
	options, err := parseServiceFlags(args)
	if len(options.words) < 2 || (options.words[0] != "config" || options.words[1] != "key") && (options.words[0] != "service" || options.words[1] != "host-key") {
		return 0, false
	}
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, 2, err), true
	}
	if options.help {
		fmt.Fprintf(stdout, "usage: deck %s %s [--config PATH] [--json]\nPrint the public SSH key and its SHA256 fingerprint.\nExample: deck %s %s --config /etc/deck/config.yml\n", options.words[0], options.words[1], options.words[0], options.words[1])
		return 0, true
	}
	if len(options.words) != 2 {
		return serviceFailure(stdout, stderr, options.json, 2, errors.New("unexpected argument; use --help")), true
	}
	var identity transport.PublicIdentity
	if options.words[0] == "config" {
		path, pathErr := config.ClientPath(options.path)
		if pathErr != nil {
			return serviceFailure(stdout, stderr, options.json, 2, pathErr), true
		}
		cfg, loadErr := config.LoadClient(path)
		if loadErr != nil {
			return serviceFailure(stdout, stderr, options.json, 2, loadErr), true
		}
		identity, err = clientPublicKey(ctx, cfg)
	} else {
		path := options.path
		if path == "" {
			path = config.ServicePath
		}
		cfg, loadErr := config.LoadService(path)
		if loadErr != nil {
			return serviceFailure(stdout, stderr, options.json, 2, loadErr), true
		}
		identity, err = hostPublicKey(ctx, cfg)
	}
	if err != nil {
		return serviceFailure(stdout, stderr, options.json, cli.ExitCode(err), err), true
	}
	if options.json {
		_ = json.NewEncoder(stdout).Encode(identity)
	} else {
		fmt.Fprintln(stdout, identity.PublicKey)
		fmt.Fprintln(stdout, identity.Fingerprint)
	}
	return 0, true
}
func clientPublicKey(ctx context.Context, cfg *config.Client) (transport.PublicIdentity, error) {
	client, err := vault.New(ctx, cfg.Vault)
	if err != nil {
		return transport.PublicIdentity{}, err
	}
	defer closeVault(client)
	signer, err := transport.NewTransitSigner(ctx, client, cfg.Key)
	if err != nil {
		return transport.PublicIdentity{}, err
	}
	return transport.DescribeKey(signer.PublicKey(), "deck:"+cfg.Key.Name), nil
}
func hostPublicKey(ctx context.Context, cfg *config.Service) (transport.PublicIdentity, error) {
	client, err := vault.New(ctx, cfg.Vault)
	if err != nil {
		return transport.PublicIdentity{}, err
	}
	defer closeVault(client)
	signer, err := transport.ReadHostKey(ctx, client, cfg.HostKey)
	if err != nil {
		return transport.PublicIdentity{}, err
	}
	return transport.DescribeKey(signer.PublicKey(), ""), nil
}
func closeVault(client *vault.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = client.Close(ctx)
}
func serviceFailure(stdout, stderr io.Writer, jsonMode bool, code int, err error) int {
	category := map[int]string{1: "failed", 2: "invalid", 3: "refused", 4: "unavailable", 130: "interrupted"}[code]
	if jsonMode {
		_ = json.NewEncoder(stdout).Encode(map[string]any{"error": map[string]string{"category": category, "message": err.Error()}})
	} else {
		for _, line := range strings.Split(err.Error(), "\n") {
			fmt.Fprintln(stderr, "deck: "+line)
		}
	}
	return code
}
