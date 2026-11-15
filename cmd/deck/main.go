// Command deck provides the deck command-line interface.
package main

import (
	"fmt"
	"os"
)

// Version is supplied at build time from the release version.
var Version = "dev"

func run(args []string) int {
	if len(args) == 1 && args[0] == "--version" {
		fmt.Println(Version)
		return 0
	}
	if len(args) >= 2 && args[0] == "service" && args[1] == "start" {
		fmt.Fprintln(os.Stderr, "deck service start: not implemented")
		return 1
	}
	fmt.Fprintln(os.Stderr, "usage: deck --version | deck service start [--help]")
	return 1
}

func main() { os.Exit(run(os.Args[1:])) }
