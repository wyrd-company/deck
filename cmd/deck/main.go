// Command deck provides the deck command-line interface.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wyrd-company/deck/internal/config"
	"io"
	"os"
)

// Version is supplied at build time from the release version.
var Version = "dev"

func run(args []string, stdout, stderr io.Writer) int {
	if code, handled := validateCommand(args, stdout, stderr); handled {
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
	fmt.Fprintln(stderr, "usage: deck --version | deck service start [--help]")
	return 1
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// validateCommand owns only offline validation; the other commands are built by
// the service and CLI tasks.
func validateCommand(args []string, stdout, stderr io.Writer) (int, bool) {
	var words []string
	var path string
	jsonMode, help := false, false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--json":
			jsonMode = true
		case args[i] == "--help" || args[i] == "-h":
			help = true
		case args[i] == "--config":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "deck: --config requires a path")
				return 2, true
			}
			i++
			path = args[i]
		case strings.HasPrefix(args[i], "--config="):
			path = strings.TrimPrefix(args[i], "--config=")
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
		fmt.Fprintln(stderr, "deck: unexpected argument; use --help")
		return 2, true
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
		if jsonMode {
			_ = json.NewEncoder(stdout).Encode(map[string]any{"error": map[string]string{"category": "invalid", "message": message}})
		} else {
			for _, line := range strings.Split(message, "\n") {
				fmt.Fprintln(stderr, "deck: "+line)
			}
		}
		return 2, true
	}
	if jsonMode {
		_ = json.NewEncoder(stdout).Encode(map[string]any{"ok": true, "path": path})
	} else {
		fmt.Fprintln(stdout, "ok "+path)
	}
	return 0, true
}
