package cli

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/store"
	"math/big"
	"os"
	"regexp"
	"slices"
	"strings"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/client"
	"github.com/wyrd-company/deck/internal/skill"
)

func (i *invocation) execute(ctx context.Context) error {
	switch i.flags.command {
	case "post", "reply":
		return i.send(ctx)
	case "inbox":
		return i.inbox(ctx)
	case "read":
		return i.read(ctx, false)
	case "listen":
		return i.read(ctx, true)
	case "history":
		return i.history(ctx)
	case "resolve":
		return i.resolve()
	case "channels":
		return i.channels(ctx)
	case "skill":
		i.result["skill"] = skill.Markdown
		i.text.WriteString(skill.Markdown)
		return nil
	}
	return invalid("unknown command")
}
func (i *invocation) check(bot string, id uint64) error {
	if !client.Allowed(i.client.Config, bot, id) {
		return refused("channel %s/%d is excluded by channels.allow or channels.deny; update the client configuration", bot, id)
	}
	return nil
}
func (i *invocation) channel(value string) (*pb.ChannelRef, error) {
	value = client.ResolveAlias(value, i.client.Config.Aliases.Channels)
	bot, id, err := client.Reference(value)
	if err != nil {
		return nil, invalid("%v", err)
	}
	if err = i.check(bot, id); err != nil {
		return nil, err
	}
	return &pb.ChannelRef{Bot: bot, ChannelId: id}, nil
}
func (i *invocation) thread(ctx context.Context, value string) (*pb.ThreadRef, *pb.Thread, error) {
	bot, id, err := client.Reference(value)
	if err != nil {
		return nil, nil, invalid("%v", err)
	}
	ref := &pb.ThreadRef{Bot: bot, ThreadId: id}
	t, err := i.client.Store.Thread(id)
	if err != nil {
		return nil, nil, err
	}
	if t != nil {
		if bot != "" && bot != t.Bot {
			return nil, nil, invalid("thread %d belongs to bot %s", id, t.Bot)
		}
		ref.Bot = t.Bot
	} else {
		if err = i.connect(ctx); err != nil {
			return nil, nil, err
		}
		r, e := i.client.API.GetThread(ctx, &pb.GetThreadRequest{Thread: ref})
		if e != nil {
			return nil, nil, e
		}
		t = r.Thread
		if t == nil {
			return nil, nil, errors.New("service returned no thread metadata")
		}
		ref.Bot = t.Bot
	}
	if err = i.check(t.Bot, t.ChannelId); err != nil {
		return nil, nil, err
	}
	return ref, t, nil
}

var noncePattern = regexp.MustCompile(`^[A-Za-z0-9]{1,25}$`)

func (i *invocation) send(ctx context.Context) error {
	content, err := i.content()
	if err != nil {
		return err
	}
	nonce := i.flags.one("nonce")
	if !i.flags.has("nonce") {
		const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		raw := make([]byte, 20)
		for n := range raw {
			v, e := rand.Int(rand.Reader, nonceAlphabetSize)
			if e != nil {
				return e
			}
			raw[n] = alphabet[v.Int64()]
		}
		nonce = string(raw)
	}
	if !noncePattern.MatchString(nonce) {
		return invalid("--nonce must contain 1 to 25 letters or digits")
	}
	call, cancel := i.callContext(ctx)
	defer cancel()
	var channel *pb.ChannelRef
	var thread *pb.ThreadRef
	var replyTo uint64
	if i.flags.command == "post" {
		value := i.client.Config.Channels.Default
		if len(i.flags.args) > 0 {
			value = i.flags.args[0]
		}
		if value == "" {
			return invalid("post needs CHANNEL or channels.default in config.yml")
		}
		channel, err = i.channel(value)
	} else {
		if i.flags.has("reply-to") {
			replyTo, err = parseID(i.flags.one("reply-to"))
			if err != nil {
				return invalid("--reply-to requires a message snowflake")
			}
		}
		thread, _, err = i.thread(call, i.flags.args[0])
	}
	if err != nil {
		return err
	}
	if err = i.connect(call); err != nil {
		return err
	}
	if i.client.Fingerprint == "" {
		return errors.New("transport returned no signing-key fingerprint; refusing to send without an own-message record")
	}
	threadID := uint64(0)
	if thread != nil {
		threadID = thread.ThreadId
	}
	if err = i.client.Store.Pending(i.client.Session, i.client.Fingerprint, nonce, i.flags.command, threadID); err != nil {
		return fmt.Errorf("record pending send: %w", err)
	}
	var t *pb.Thread
	var message, poll *pb.Activity
	var position *pb.StreamPosition
	var pollError string
	if channel != nil {
		r, e := i.client.API.Post(call, &pb.PostRequest{Channel: channel, Title: i.flags.one("title"), Tags: i.flags.values["tag"], Content: content, Nonce: nonce})
		err = e
		if e == nil {
			t, message, position, poll, pollError = r.Thread, r.Message, r.Position, r.PollMessage, r.PollError
		}
	} else {
		r, e := i.client.API.Reply(call, &pb.ReplyRequest{Thread: thread, Content: content, ReplyToMessageId: replyTo, Nonce: nonce})
		err = e
		if e == nil {
			t, message, position = r.Thread, r.Message, r.Position
		}
	}
	if err != nil {
		state := "failed"
		if unknownSend(err) {
			state = "unknown"
		}
		recordErr := i.client.Store.SendState(i.client.Fingerprint, nonce, state)
		if state == "unknown" {
			repeat := repeatCommand(i.flags, nonce)
			msg := fmt.Sprintf("outcome unknown; inspect Discord before repeating: %s", repeat)
			if recordErr != nil {
				msg += "; could not record send outcome: " + recordErr.Error()
			}
			return &failure{4, msg}
		}
		if recordErr != nil {
			return fmt.Errorf("record refused send: %w", recordErr)
		}
		return err
	}
	if t == nil || message == nil || position == nil {
		i.client.Store.SendState(i.client.Fingerprint, nonce, "unknown")
		return &failure{4, "outcome unknown: incomplete service result; inspect Discord before repeating: " + repeatCommand(i.flags, nonce)}
	}
	i.result["thread"] = apiJSON(t)
	i.result["message"] = apiJSON(message)
	if poll != nil {
		i.result["poll_message"] = apiJSON(poll)
	}
	if pollError != "" {
		i.result["poll_error"] = pollError
		fmt.Fprintln(i.app.Stderr, "deck: poll: "+line(pollError))
	}
	role := "created"
	if channel != nil {
		fmt.Fprintf(&i.text, "posted %d in %s/%d\n%s\n", t.ThreadId, line(t.Bot), t.ChannelId, line(threadURL(t)))
	} else {
		role = "replied"
		fmt.Fprintf(&i.text, "replied %d in %d\n%s/%d\n", message.Id, t.ThreadId, line(threadURL(t)), message.Id)
	}
	if err = i.client.Store.Sent(i.client.Session, i.client.Fingerprint, nonce, role, t, message.Id, position); err != nil {
		return fmt.Errorf("message sent, but could not record local store result: %w", err)
	}
	return nil
}
func threadURL(t *pb.Thread) string {
	if t.Url != "" {
		return t.Url
	}
	return fmt.Sprintf("https://discord.com/channels/%d/%d", t.ServerId, t.ThreadId)
}
func repeatCommand(f flags, nonce string) string {
	parts := []string{"deck", f.command}
	for _, arg := range f.args {
		parts = append(parts, shellQuote(arg))
	}
	keys := []string{}
	for key := range f.values {
		if key != "nonce" && key != "help" && key != "h" {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	for _, key := range keys {
		for _, value := range f.values[key] {
			parts = append(parts, "--"+key)
			if !bools[key] {
				parts = append(parts, shellQuote(value))
			}
		}
	}
	parts = append(parts, "--nonce", nonce)
	return strings.Join(parts, " ")
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func (i *invocation) inbox(ctx context.Context) error {
	ts, err := i.client.Threads()
	if err != nil {
		return err
	}
	var report *client.Unread
	if len(ts) == 0 {
		report = &client.Unread{Threads: []client.Count{}}
	} else {
		call, cancel := i.callContext(ctx)
		defer cancel()
		if err = i.connect(call); err != nil {
			return err
		}
		report, err = i.client.Count(call)
		if err != nil {
			return err
		}
	}
	i.count = report
	byID := map[string]client.Count{}
	for _, c := range report.Threads {
		byID[c.ThreadID] = c
	}
	rows := []map[string]any{}
	for _, t := range ts {
		c := byID[fmt.Sprint(t.Info.ThreadId)]
		if !i.flags.has("all") && c.Count == 0 && c.State == pb.ThreadState_THREAD_STATE_TOUCHED.String() {
			continue
		}
		rows = append(rows, map[string]any{"thread_id": fmt.Sprint(t.Info.ThreadId), "bot": t.Info.Bot, "channel_id": fmt.Sprint(t.Info.ChannelId), "title": t.Info.Name, "count": c.Count, "state": c.State})
		count := fmt.Sprint(c.Count)
		if c.State != pb.ThreadState_THREAD_STATE_TOUCHED.String() {
			count = c.State
		}
		fmt.Fprintf(&i.text, "%s %d %s/%d %s\n", count, t.Info.ThreadId, line(t.Info.Bot), t.Info.ChannelId, line(t.Info.Name))
	}
	i.result["threads"] = rows
	return nil
}
func (i *invocation) history(ctx context.Context) error {
	limit, err := i.flags.limit(50, 100)
	if err != nil {
		return err
	}
	var after, before uint64
	for _, pair := range []struct {
		name   string
		target *uint64
	}{{"after", &after}, {"before", &before}} {
		if i.flags.has(pair.name) {
			n, err := parseID(i.flags.one(pair.name))
			if err != nil {
				return invalid("--%s requires a message snowflake", pair.name)
			}
			*pair.target = n
		}
	}
	call, cancel := i.callContext(ctx)
	defer cancel()
	ref, t, err := i.thread(call, i.flags.args[0])
	if err != nil {
		return err
	}
	if err = i.connect(call); err != nil {
		return err
	}
	r, err := i.client.API.ListHistory(call, &pb.ListHistoryRequest{Thread: ref, AfterId: after, BeforeId: before, Limit: limit})
	if err != nil {
		return err
	}
	i.result["activity"] = apiActivities(r.Activity)
	i.result["more"] = r.More
	i.renderActivities(r.Activity, map[uint64]*pb.Thread{t.ThreadId: t})
	return nil
}
func (i *invocation) channels(ctx context.Context) error {
	call, cancel := i.callContext(ctx)
	defer cancel()
	if err := i.connect(call); err != nil {
		return err
	}
	r, err := i.client.API.ListChannels(call, &pb.ListChannelsRequest{})
	if err != nil {
		return err
	}
	channels := []json.RawMessage{}
	for _, c := range r.Channels {
		if !client.Allowed(i.client.Config, c.Bot, c.ChannelId) {
			continue
		}
		channels = append(channels, apiJSON(c))
		alias := ""
		names := []string{}
		for name := range i.client.Config.Aliases.Channels {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			ref, e := i.channel(name)
			if e == nil && ref.ChannelId == c.ChannelId && (ref.Bot == "" || ref.Bot == c.Bot) {
				alias = " [" + name + "]"
				break
			}
		}
		fmt.Fprintf(&i.text, "%s/%d %s %s / %s%s\n", line(c.Bot), c.ChannelId, channelType(c.Type), line(c.ServerName), line(c.Name), line(alias))
	}
	i.result["channels"] = channels
	return nil
}
func (i *invocation) resolve() error {
	var event *pb.Activity
	var id uint64
	if i.flags.has("event") {
		r := contentReader{input: i.app.Stdin}
		raw, err := r.read(i.flags.one("event"))
		if err != nil {
			return err
		}
		event = &pb.Activity{}
		if err = validateEvent(raw, event); err != nil || event.ThreadId == 0 || event.Id == 0 || event.Kind == nil || event.Time == nil || event.Time.CheckValid() != nil || event.Bot == "" || event.ServerId == 0 || event.ChannelId == 0 {
			return invalid("invalid event: expected a deck Activity with id, time, bot, server_id, channel_id, thread_id and kind")
		}
		id = event.ThreadId
	} else {
		_, n, err := client.Reference(i.flags.args[0])
		if err != nil {
			return invalid("%v", err)
		}
		id = n
	}
	db, err := store.OpenReadOnly(i.client.Config.Store.Path)
	if errors.Is(err, os.ErrNotExist) {
		i.result["sessions"] = []store.Resolved{}
		return nil
	}
	if err != nil {
		return fmt.Errorf("open local store: %w", err)
	}
	defer db.Close()
	sessions, err := db.Resolve(id, event)
	if err != nil {
		return err
	}
	i.result["sessions"] = sessions
	for _, s := range sessions {
		values := []string{}
		for _, identifier := range s.Identifiers {
			values = append(values, line(identifier.Name)+"="+line(identifier.Value))
		}
		i.text.WriteString(strings.Join(values, " ") + "\n")
	}
	return nil
}

var nonceAlphabetSize = big.NewInt(62)

func (a App) runResolve(f flags, cfg *config.Client) int {
	inv := invocation{app: a, flags: f, client: &client.Client{Config: cfg}, result: map[string]any{}}
	if err := inv.resolve(); err != nil {
		return a.error(err, f.has("json"))
	}
	if f.has("json") {
		if err := json.NewEncoder(a.Stdout).Encode(inv.result); err != nil {
			return 1
		}
	} else {
		if _, err := fmt.Fprint(a.Stdout, inv.text.String()); err != nil {
			return 1
		}
	}
	return 0
}
