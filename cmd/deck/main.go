// Command deck provides the deck command-line interface.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/wyrd-company/deck/internal/cli"
	"github.com/wyrd-company/deck/internal/config"
)

// Version is supplied at build time from the release version.
var Version = "dev"

// serviceCommand is wired by the service/transport command adapter.
var serviceCommand = func(context.Context, []string, io.Writer, io.Writer) (int, bool) { return 0, false }

func run(args []string, stdout, stderr io.Writer) int {
	return runContext(context.Background(), args, stdout, stderr)
}
func runContext(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if code, handled := validateCommand(args, stdout, stderr); handled {
		return code
	}
	if code, handled := serviceCommand(ctx, args, stdout, stderr); handled {
		return code
	}
	if len(args) == 1 && args[0] == "--version" {
		fmt.Fprintln(stdout, Version)
		return 0
	}
	if len(args) >= 2 && args[0] == "service" && args[1] == "start" {
		fmt.Fprintln(stderr, "deck service start: not implemented")
		return 1
	}
	return (cli.App{Stdout: stdout, Stderr: stderr}).Run(ctx, args)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runContext(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// validateCommand owns only offline validation; the other commands are built by
// the service and CLI tasks.
func validateCommand(args []string, stdout, stderr io.Writer) (int, bool) {
	var words []string
	var path string
	jsonMode, help := slices.Contains(args, "--json"), false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--json":
			jsonMode = true
		case args[i] == "--help" || args[i] == "-h":
			help = true
		case args[i] == "--config":
			if i+1 >= len(args) {
				return validationFailure(stdout, stderr, jsonMode, "--config requires a path"), true
			}
			i++
			path = args[i]
			if path == "" {
				return validationFailure(stdout, stderr, jsonMode, "--config requires a non-empty path"), true
			}
		case strings.HasPrefix(args[i], "--config="):
			path = strings.TrimPrefix(args[i], "--config=")
			if path == "" {
				return validationFailure(stdout, stderr, jsonMode, "--config requires a non-empty path"), true
			}
		default:
			words = append(words, args[i])
		}
	}
	if len(words) < 2 || words[1] != "validate" || (words[0] != "config" && words[0] != "service") {
		return 0, false
	}
	if help {
		fmt.Fprintf(stdout, "usage: deck %s validate [--config PATH] [--json]\nValidate configuration without contacting a server.\nExample: deck %s validate --config /etc/deck/config.yml\n", words[0], words[0])
		return 0, true
	}
	if len(words) != 2 {
		return validationFailure(stdout, stderr, jsonMode, "unexpected argument; use --help"), true
	}
	var err error
	if words[0] == "config" {
		path, err = config.ClientPath(path)
	} else if path == "" {
		path = config.ServicePath
	}
	if err == nil {
		if words[0] == "config" {
			_, err = config.LoadClient(path)
		} else {
			_, err = config.LoadService(path)
		}
	}
	if err != nil {
		var fields config.Errors
		var message string
		if errors.As(err, &fields) {
			var lines []string
			for _, field := range fields {
				lines = append(lines, path+": "+field.Error())
			}
			message = strings.Join(lines, "\n")
		} else {
			message = err.Error()
		}
		return validationFailure(stdout, stderr, jsonMode, message), true
	}
	if jsonMode {
		_ = json.NewEncoder(stdout).Encode(map[string]any{"ok": true, "path": path})
	} else {
		fmt.Fprintln(stdout, "ok "+path)
	}
	return 0, true
}

func validationFailure(stdout, stderr io.Writer, jsonMode bool, message string) int {
	if jsonMode {
		_ = json.NewEncoder(stdout).Encode(map[string]any{"error": map[string]string{"category": "invalid", "message": message}})
	} else {
		for _, line := range strings.Split(message, "\n") {
			fmt.Fprintln(stderr, "deck: "+line)
		}
	}
	return 2
}
