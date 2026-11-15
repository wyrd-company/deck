package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestSecondSignalTerminates(t *testing.T) {
	if os.Getenv("DECK_TEST_SECOND_SIGNAL") == "1" {
		// Use main's actual signal handling and dispatch, with cleanup that is
		// deliberately stuck after observing the first cancellation.
		serviceCommand = func(ctx context.Context, _ []string, stdout, _ io.Writer) (int, bool) {
			fmt.Fprintln(stdout, "ready")
			<-ctx.Done()
			fmt.Fprintln(stdout, "canceled")
			select {}
		}
		os.Args = []string{"deck", "signal-probe"}
		main()
		return
	}
	for _, first := range []os.Signal{os.Interrupt, syscall.SIGTERM} {
		t.Run(first.String(), func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestSecondSignalTerminates$")
			cmd.Env = append(os.Environ(), "DECK_TEST_SECOND_SIGNAL=1")
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			lines := make(chan string, 2)
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					lines <- scanner.Text()
				}
				close(lines)
			}()
			waitLine := func(want string) {
				t.Helper()
				select {
				case line := <-lines:
					if line != want {
						t.Fatalf("signal helper output=%q want=%q", line, want)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("signal helper did not reach", want)
				}
			}
			waitLine("ready")
			if err := cmd.Process.Signal(first); err != nil {
				t.Fatal(err)
			}
			waitLine("canceled")
			// A human's second Ctrl-C arrives after the cancellation callback
			// has had a turn to restore default signal handling.
			time.Sleep(100 * time.Millisecond)
			select {
			case err := <-done:
				t.Fatal("first signal skipped graceful cleanup", err)
			default:
			}
			if err := cmd.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				exit, ok := err.(*exec.ExitError)
				if !ok || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGINT {
					t.Fatal("second signal did not use default handling", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("second Ctrl-C was swallowed during blocked cleanup")
			}
		})
	}
}
