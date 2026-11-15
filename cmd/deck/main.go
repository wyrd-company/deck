// Command deck provides the deck command-line interface.
package main

import (
	"fmt"
	"io"
	"os"
)

// Version is supplied at build time from the release version.
var Version = "dev"

func run(args []string, stdout, stderr io.Writer) int {
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
