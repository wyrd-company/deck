/*
---
relationships:

  implements:
    - client
    - command-line-interface

---
*/
// Package cli implements the session commands independently of the SSH dialer.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/client"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/session"
	"github.com/wyrd-company/deck/internal/skill"
	"github.com/wyrd-company/deck/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Connection.Fingerprint is the identity that signed this connection, pinned
// by the dialer. The CLI must never fetch the key separately to record sends.
type Connection struct {
	API         pb.DeckServiceClient
	Fingerprint string
	Close       func() error
}

// Dial bounds only connection establishment with its context. A successful
// connection must remain usable after that context is cancelled, until Close.
// Per-RPC deadlines and listen cancellation are managed by the CLI.
type Dial func(context.Context, *config.Client) (Connection, error)

// DefaultDial is wired by the transport command adapter (task 5).
var DefaultDial Dial = func(context.Context, *config.Client) (Connection, error) {
	return Connection{}, status.Error(codes.Unavailable, "SSH client transport is not configured")
}

type App struct {
	Dial           Dial
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}
type invocation struct {
	app         App
	flags       flags
	client      *client.Client
	connection  *Connection
	dialed      bool
	dialErr     error
	result      map[string]any
	text        strings.Builder
	count       *client.Unread
	states      map[uint64]pb.ThreadState
	readIDs     []uint64
	readThrough *pb.StreamPosition
}

func (a App) Run(ctx context.Context, args []string) int {
	if a.Stdin == nil {
		a.Stdin = os.Stdin
	}
	if a.Stdout == nil {
		a.Stdout = os.Stdout
	}
	if a.Stderr == nil {
		a.Stderr = os.Stderr
	}
	if a.Dial == nil {
		a.Dial = DefaultDial
	}
	f, err := parse(args)
	jsonMode := slices.Contains(args, "--json") || f.has("json")
	if err != nil {
		return a.error(err, jsonMode)
	}
	if f.has("help") || f.has("h") {
		fmt.Fprint(a.Stdout, help(f))
		return 0
	}
	if f.command == "" {
		return a.error(invalid("%s", help(f)), jsonMode)
	}
	if err = validateFlags(f); err != nil {
		return a.error(err, jsonMode)
	}
	path, err := config.ClientPath(f.one("config"))
	if err != nil && f.command != "skill" {
		return a.error(invalid("%v", err), jsonMode)
	}
	var cfg *config.Client
	if err == nil {
		cfg, err = config.LoadClient(path)
	}
	if err != nil && f.command == "skill" && !f.has("config") && (errors.Is(err, os.ErrNotExist) || cfg == nil && path == "") {
		return a.skillWithoutConfig(f)
	}
	if err != nil {
		return a.error(configurationError(path, err), jsonMode)
	}
	if f.command == "resolve" {
		return a.runResolve(f, cfg)
	}
	db, err := store.Open(cfg.Store.Path)
	if err != nil {
		return a.error(fmt.Errorf("open local store: %w", err), jsonMode)
	}
	defer db.Close()
	ss, err := session.Detect(cfg.Session.Variables, f.one("session"))
	if err != nil {
		return a.error(invalid("%v", err), jsonMode)
	}
	inv := invocation{app: a, flags: f, client: &client.Client{Config: cfg, Store: db, Session: ss}, result: map[string]any{}, states: map[uint64]pb.ThreadState{}}
	if err = db.Session(ss); err != nil {
		return a.error(fmt.Errorf("record session: %w", err), jsonMode)
	}
	defer func() {
		if inv.connection != nil && inv.connection.Close != nil {
			inv.connection.Close()
		}
	}()
	err = inv.execute(ctx)
	inv.reportUnread(ctx)
	if err != nil {
		// A successful Discord send whose local commit failed still prints its IDs.
		if len(inv.result) > 0 && f.has("json") {
			inv.result["error"] = map[string]string{"category": category(ExitCode(err)), "message": err.Error()}
			json.NewEncoder(a.Stdout).Encode(inv.result)
			return ExitCode(err)
		}
		if inv.text.Len() > 0 && !f.has("json") {
			fmt.Fprint(a.Stdout, inv.text.String())
		}
		return a.error(err, jsonMode)
	}
	if err = inv.writeOutput(); err != nil {
		return a.error(fmt.Errorf("write output: %w", err), false)
	}
	// A failed or partial stdout write must leave the activity unread. JSON has
	// already been emitted, so a subsequent store error is a stderr diagnostic.
	if len(inv.readIDs) > 0 {
		if err = inv.client.Store.Advance(ss, inv.readIDs, inv.readThrough); err != nil {
			return a.error(fmt.Errorf("mark read in local store: %w", err), false)
		}
	}
	return 0
}

func (i *invocation) writeOutput() error {
	payload := []byte(i.text.String())
	if i.flags.has("json") {
		var err error
		payload, err = json.Marshal(i.result)
		if err != nil {
			return err
		}
		payload = append(payload, '\n')
	}
	n, err := i.app.Stdout.Write(payload)
	if err == nil && n != len(payload) {
		return io.ErrShortWrite
	}
	return err
}
func (a App) error(err error, jsonMode bool) int {
	code := ExitCode(err)
	if jsonMode {
		json.NewEncoder(a.Stdout).Encode(map[string]any{"error": map[string]string{"category": category(code), "message": err.Error()}})
	} else {
		for _, line := range strings.Split(err.Error(), "\n") {
			fmt.Fprintln(a.Stderr, "deck: "+terminalText(line, false))
		}
	}
	return code
}
func (a App) skillWithoutConfig(f flags) int {
	if f.has("json") {
		json.NewEncoder(a.Stdout).Encode(map[string]any{"skill": skill.Markdown, "unread": map[string]string{"error": "no configuration"}})
	} else {
		fmt.Fprint(a.Stdout, skill.Markdown)
		fmt.Fprintln(a.Stderr, "unread: unknown (no configuration)")
	}
	return 0
}
func (i *invocation) connect(ctx context.Context) error {
	if i.dialed {
		return i.dialErr
	}
	i.dialed = true
	conn, err := i.app.Dial(ctx, i.client.Config)
	if err != nil {
		i.dialErr = err
		return err
	}
	if conn.API == nil {
		if conn.Close != nil {
			conn.Close()
		}
		i.dialErr = errors.New("transport returned no API client")
		return i.dialErr
	}
	i.connection = &conn
	i.client.API = conn.API
	i.client.Fingerprint = conn.Fingerprint
	return nil
}
func (i *invocation) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, i.client.Config.Service.Timeout)
}
func (i *invocation) reportUnread(ctx context.Context) {
	var report *client.Unread
	var err error
	if i.count != nil {
		report = i.count
	} else {
		ts, e := i.client.Threads()
		err = e
		if err == nil && len(ts) == 0 {
			report = &client.Unread{Threads: []client.Count{}}
		} else if err == nil {
			call, cancel := i.callContext(ctx)
			defer cancel()
			err = i.connect(call)
			if err == nil {
				// Count from the positions this invocation will commit only after
				// output. Apply the same epoch/sequence guard as Store.Advance.
				for n, t := range ts {
					if slices.Contains(i.readIDs, t.Info.ThreadId) && (t.After.Epoch != i.readThrough.Epoch || t.After.Sequence < i.readThrough.Sequence) {
						ts[n].After = i.readThrough
					}
				}
				var response *pb.CountUnreadResponse
				response, err = i.client.API.CountUnread(call, &pb.CountUnreadRequest{Cursors: client.Cursors(ts)})
				if err == nil {
					report, err = i.client.Counts(response.Threads)
				}
			}
		}
	}
	if err != nil {
		i.result["unread"] = map[string]string{"error": err.Error()}
		if !i.flags.has("json") {
			fmt.Fprintf(i.app.Stderr, "unread: unknown (%s)\n", line(err.Error()))
		}
		return
	}
	i.result["unread"] = report
	if !i.flags.has("json") {
		if report.Total == 0 {
			fmt.Fprint(i.app.Stderr, "unread: none")
		} else {
			n := 0
			for _, t := range report.Threads {
				if t.Count > 0 {
					n++
				}
			}
			fmt.Fprintf(i.app.Stderr, "unread: %d in %d threads (deck read)", report.Total, n)
		}
		for _, t := range report.Threads {
			if t.HorizonPassed {
				fmt.Fprintf(i.app.Stderr, " (older activity may have expired; deck history %s)", t.ThreadID)
			}
		}
		fmt.Fprintln(i.app.Stderr)
	}
}
func apiJSON(m proto.Message) json.RawMessage {
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(m)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return raw
}
func apiActivities(as []*pb.Activity) []json.RawMessage {
	result := make([]json.RawMessage, 0, len(as))
	for _, a := range as {
		result = append(result, apiJSON(a))
	}
	return result
}
func validateFlags(f flags) error {
	for _, name := range []string{"config", "session", "event"} {
		if f.has(name) && f.one(name) == "" {
			return invalid("--%s requires a non-empty value", name)
		}
	}
	switch f.command {
	case "post":
		if err := f.arity(0, 1); err != nil {
			return err
		}
		if f.one("title") == "" {
			return invalid("usage: %s\npost requires --title TITLE; use deck post --help", usage("post"))
		}
	case "reply", "history":
		if err := f.arity(1, 1); err != nil {
			return err
		}
	case "inbox", "channels", "skill":
		if err := f.arity(0, 0); err != nil {
			return err
		}
	case "resolve":
		if f.has("event") {
			return f.arity(0, 0)
		}
		return f.arity(1, 1)
	}
	if f.command == "history" && f.has("after") && f.has("before") {
		return invalid("use only one of --after and --before")
	}
	return nil
}

func configurationError(path string, err error) error {
	var fields config.Errors
	if errors.As(err, &fields) {
		lines := make([]string, 0, len(fields))
		for _, field := range fields {
			lines = append(lines, path+": "+field.Error())
		}
		return invalid("%s\nRun deck config validate --config %s to inspect configuration.", strings.Join(lines, "\n"), path)
	}
	return invalid("%s: %v; check the path and run deck config validate --config %s", path, err, path)
}
