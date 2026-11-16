package activity

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/protobuf/encoding/protojson"
)

// GenerationKey is the registry bucket key of the stream's generation record.
const GenerationKey = "stream.generation"

// Tuning of the index's background work. Tests shorten them.
var (
	auditInterval = 10 * time.Second
	catchUpPoll   = time.Second
	closeWait     = 5 * time.Second
)

// Test hooks: muteConsumerTriggers silences the consumer's own triggers so a
// test can exercise the audit alone; beforeConsumer runs between the
// generation read and the fresh consumer's creation.
var (
	muteConsumerTriggers atomic.Bool
	beforeConsumer       func()
)

// ErrUnavailable is returned when the index cannot answer before the
// caller's deadline: it is replaying, or NATS is disconnected.
var ErrUnavailable = errors.New("activity index unavailable")

// Entry is what the index records of one event, from its subject, headers
// and metadata alone.
type Entry struct {
	Sequence  uint64
	Time      time.Time
	Thread    uint64
	ID        uint64
	Kind      string
	Type      uint32
	SentBy    string
	SenderKey string
	Nonce     string
	SendStart *pb.StreamPosition
	PollError string
}

// Counts reports whether the entry counts as unread: every activity except
// messages of a type other than 0, 19 or 46.
func (e *Entry) Counts() bool {
	return e.Kind != "message" || e.Type == 0 || e.Type == 19 || e.Type == 46
}

type generation struct {
	Epoch   uint64    `json:"epoch"`
	Created time.Time `json:"created"`
}

// Index is the activity index: an in-memory mirror of the event stream's
// headers, fed by an ordered headers-only consumer, guarded by a gate that is
// open only while the mirror is caught up with the current stream generation.
type Index struct {
	js     jetstream.JetStream
	kv     jetstream.KeyValue
	cfg    config.NATS
	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	wake   chan struct{}

	mu            sync.Mutex
	epoch         uint64
	created       time.Time
	first         uint64
	last          uint64
	threads       map[uint64][]*Entry
	held          map[uint64]map[string]bool
	sends         map[string][]*Entry
	open          bool
	token         uint64
	tokens        uint64
	due, checked  uint64
	version       uint64
	invalidations uint64
	changed       chan struct{}
	stream        jetstream.Stream
	consumer      jetstream.Consumer
	cc            jetstream.ConsumeContext
	audited       uint64
	state         string
}

// NewIndex registers the index's connection callbacks and starts its check
// worker. Register before anything else reads or publishes the stream.
func NewIndex(ctx context.Context, js jetstream.JetStream, kv jetstream.KeyValue, cfg config.NATS, events *ConnectionEvents, log *slog.Logger) *Index {
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(ctx)
	i := &Index{js: js, kv: kv, cfg: cfg, log: log, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1), threads: map[uint64][]*Entry{}, held: map[uint64]map[string]bool{}, sends: map[string][]*Entry{}, changed: make(chan struct{}), state: "replaying"}
	if events != nil {
		events.OnDisconnect(i.disconnected)
		events.OnReconnect(func() { i.markDue("reconnect") })
	}
	i.due = 1
	i.wg.Add(1)
	go i.run()
	return i
}

// broadcast wakes every waiter; call with mu held.
func (i *Index) broadcast() {
	close(i.changed)
	i.changed = make(chan struct{})
}

func (i *Index) invalidate() {
	i.open = false
	i.token = 0
	i.invalidations++
	i.broadcast()
}

// disconnected marks the index unavailable and invalidates ingestion. The
// reconnect callback marks the check due.
func (i *Index) disconnected() {
	i.mu.Lock()
	i.invalidate()
	i.state = "unavailable"
	i.mu.Unlock()
}

// markDue invalidates ingestion and asks the worker for a generation check,
// without waiting for it. It is safe to call from nats.go's callbacks.
func (i *Index) markDue(reason string) {
	i.mu.Lock()
	i.invalidate()
	i.due++
	i.state = "replaying"
	i.mu.Unlock()
	i.log.Debug("activity index check due", "reason", reason)
	select {
	case i.wake <- struct{}{}:
	default:
	}
}

// State is "ready", "replaying" or "unavailable", for the healthcheck.
func (i *Index) State() string {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.open {
		return "ready"
	}
	return i.state
}

func (i *Index) run() {
	defer i.wg.Done()
	audit := time.NewTicker(auditInterval)
	defer audit.Stop()
	defer i.stopConsumer()
	for i.ctx.Err() == nil {
		i.mu.Lock()
		pending := i.due > i.checked
		i.mu.Unlock()
		if pending {
			i.check()
			continue
		}
		select {
		case <-i.ctx.Done():
			return
		case <-i.wake:
		case <-audit.C:
			i.audit()
		}
	}
}

func (i *Index) stopConsumer() {
	if i.cc == nil {
		return
	}
	i.cc.Stop()
	select {
	case <-i.cc.Closed():
	case <-time.After(closeWait):
	}
	i.cc = nil
	i.consumer = nil
}

// pause waits before a retry, ending early on a new due check or shutdown.
func (i *Index) pause(delay time.Duration) bool {
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-i.ctx.Done():
		return false
	case <-i.wake:
	case <-t.C:
	}
	return true
}

// check runs the generation check until the gate opens for the due count it
// started with, as the service design's activity-index describes.
func (i *Index) check() {
	delay := 50 * time.Millisecond
	retry := func() bool {
		ok := i.pause(delay)
		if delay < 2*time.Second {
			delay *= 2
		}
		return ok
	}
	for i.ctx.Err() == nil {
		i.mu.Lock()
		target := i.due
		i.invalidate()
		i.mu.Unlock()
		i.stopConsumer()
		stream, err := EnsureStream(i.ctx, i.js, i.cfg)
		if err != nil {
			i.log.Warn("activity stream unavailable", "stream", i.cfg.Stream.Name, "manage", i.cfg.Stream.Manage)
			if !retry() {
				return
			}
			continue
		}
		info, err := stream.Info(i.ctx)
		if err != nil {
			if !retry() {
				return
			}
			continue
		}
		captured := info.State.LastSeq
		i.mu.Lock()
		lost := i.epoch != 0 && i.created.Equal(info.Created) && info.State.LastSeq < i.last
		i.mu.Unlock()
		gen, err := i.generation(info.Created, lost)
		if err != nil {
			i.log.Warn("stream generation record unavailable")
			if !retry() {
				return
			}
			continue
		}
		i.mu.Lock()
		if !gen.Created.Equal(info.Created) || gen.Epoch != i.epoch {
			if i.epoch != 0 {
				i.log.Info("activity stream generation changed; replaying", "epoch", gen.Epoch)
			}
			i.epoch, i.created = gen.Epoch, info.Created
			i.last, i.audited = 0, 0
			i.threads = map[uint64][]*Entry{}
			i.held = map[uint64]map[string]bool{}
			i.sends = map[string][]*Entry{}
			i.version++
		}
		i.first = info.State.FirstSeq
		i.skipExpired(info.State.FirstSeq)
		start := i.last + 1
		i.tokens++
		token := i.tokens
		i.token = token
		i.stream = stream
		i.mu.Unlock()
		if beforeConsumer != nil {
			beforeConsumer()
		}
		config := jetstream.OrderedConsumerConfig{FilterSubjects: []string{i.cfg.SubjectPrefix + ".>"}, HeadersOnly: true, DeliverPolicy: jetstream.DeliverAllPolicy}
		if start > 1 {
			config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			config.OptStartSeq = start
		}
		consumer, err := i.js.OrderedConsumer(i.ctx, i.cfg.Stream.Name, config)
		if err != nil {
			if !retry() {
				return
			}
			continue
		}
		cc, err := consumer.Consume(i.ingest(token), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			// nats.go calls this holding its consumer lock: only mark due.
			if i.current(token) && !muteConsumerTriggers.Load() {
				i.markDue("consumer error")
			}
		}))
		if err != nil {
			if !retry() {
				return
			}
			continue
		}
		i.cc, i.consumer = cc, consumer
		go func() {
			<-cc.Closed()
			if i.current(token) && !muteConsumerTriggers.Load() {
				i.markDue("consume context ended")
			}
		}()
		after, err := stream.Info(i.ctx)
		if err != nil || !after.Created.Equal(info.Created) {
			continue // recreated while the consumer was made: check again
		}
		if i.catchUp(target, token, captured, stream, consumer) {
			return
		}
	}
}

// skipExpired treats sequences below the stream's first as indexed: they can
// never be delivered, so a stream whose messages all expired opens its gate.
func (i *Index) skipExpired(first uint64) {
	if first > 0 && i.last < first-1 {
		i.last = first - 1
	}
}

// catchUp waits for the index to reach the captured last sequence, then opens
// the gate if no newer check became due.
func (i *Index) catchUp(target, token, captured uint64, stream jetstream.Stream, consumer jetstream.Consumer) bool {
	for {
		i.mu.Lock()
		if i.due != target || i.token != token {
			i.mu.Unlock()
			return false
		}
		if i.last >= captured {
			i.open = true
			i.checked = target
			i.state = "ready"
			i.audited = i.last
			i.broadcast()
			i.mu.Unlock()
			return true
		}
		changed := i.changed
		i.mu.Unlock()
		select {
		case <-i.ctx.Done():
			return false
		case <-changed:
			continue
		case <-time.After(catchUpPoll):
		}
		// Messages can expire or be deleted after the capture: a sequence
		// the consumer can no longer deliver must not hold the gate shut.
		info, err := stream.Info(i.ctx)
		if err != nil {
			continue
		}
		ci, err := consumer.Info(i.ctx)
		i.mu.Lock()
		if i.token == token {
			i.skipExpired(info.State.FirstSeq)
			if err == nil && ci.NumPending == 0 && ci.Delivered.Stream <= i.last && i.last < captured {
				i.last = captured
			}
			i.broadcast()
		}
		i.mu.Unlock()
	}
}

func (i *Index) current(token uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return token != 0 && i.token == token
}

// generation reads the stream's generation record, writing a new epoch when
// it is missing, its created differs from the stream's, or the stream lost
// messages the index holds while keeping its creation time.
func (i *Index) generation(created time.Time, lost bool) (generation, error) {
	i.mu.Lock()
	epoch := i.epoch
	i.mu.Unlock()
	for {
		var g generation
		var rev uint64
		record, err := i.kv.Get(i.ctx, GenerationKey)
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return g, err
		}
		if err == nil {
			rev = record.Revision()
			if err = json.Unmarshal(record.Value(), &g); err != nil {
				g = generation{}
			}
		}
		if rev != 0 && g.Epoch != 0 && g.Created.Equal(created) && !(lost && g.Epoch == epoch) {
			return g, nil
		}
		next := uint64(time.Now().UnixNano())
		if next <= g.Epoch {
			next = g.Epoch + 1
		}
		fresh := generation{Epoch: next, Created: created}
		raw, err := json.Marshal(fresh)
		if err != nil {
			return g, err
		}
		if rev == 0 {
			_, err = i.kv.Create(i.ctx, GenerationKey, raw)
		} else {
			_, err = i.kv.Update(i.ctx, GenerationKey, raw, rev)
		}
		if errors.Is(err, jetstream.ErrKeyExists) {
			continue
		}
		if err != nil {
			return g, err // a lost acknowledgement is adopted on the next read
		}
		return fresh, nil
	}
}

func (i *Index) ingest(token uint64) jetstream.MessageHandler {
	return func(m jetstream.Msg) {
		meta, err := m.Metadata()
		if err != nil {
			return
		}
		e := parseEntry(m.Subject(), m.Headers(), meta)
		i.mu.Lock()
		defer i.mu.Unlock()
		if token == 0 || i.token != token || meta.Sequence.Stream <= i.last {
			return
		}
		i.last = meta.Sequence.Stream
		if e != nil {
			i.add(e)
		}
		i.broadcast()
	}
}

func (i *Index) add(e *Entry) {
	i.threads[e.Thread] = append(i.threads[e.Thread], e)
	if i.held[e.Thread] == nil {
		i.held[e.Thread] = map[string]bool{}
	}
	i.held[e.Thread][e.Kind+":"+strconv.FormatUint(e.ID, 10)] = true
	if e.SenderKey != "" && e.Nonce != "" {
		k := e.SenderKey + ":" + e.Nonce
		i.sends[k] = append(i.sends[k], e)
	}
}

func parseEntry(subject string, h map[string][]string, meta *jetstream.MsgMetadata) *Entry {
	get := func(name string) string {
		if v := h[name]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	tokens := strings.Split(subject, ".")
	if len(tokens) < 4 {
		return nil
	}
	thread, err := strconv.ParseUint(tokens[len(tokens)-1], 10, 64)
	if err != nil {
		return nil
	}
	id, err := strconv.ParseUint(get("Deck-Activity-Id"), 10, 64)
	if err != nil {
		return nil
	}
	e := &Entry{Sequence: meta.Sequence.Stream, Time: meta.Timestamp, Thread: thread, ID: id, Kind: get("Deck-Kind"), SentBy: get("Deck-Sent-By"), SenderKey: get("Deck-Sender-Key"), Nonce: get("Deck-Nonce"), PollError: get("Deck-Poll-Error")}
	if t, err := strconv.ParseUint(get("Deck-Message-Type"), 10, 32); err == nil {
		e.Type = uint32(t)
	}
	if start := strings.Split(get("Deck-Send-Start"), ":"); len(start) == 2 {
		epoch, err1 := strconv.ParseUint(start[0], 10, 64)
		seq, err2 := strconv.ParseUint(start[1], 10, 64)
		if err1 == nil && err2 == nil {
			e.SendStart = &pb.StreamPosition{Epoch: epoch, Sequence: seq}
		}
	}
	return e
}

// audit is a backstop for triggers that never fire: a recreated stream whose
// consumer-deleted notice was lost, a stream that lost its newest messages,
// and a consumer whose library-internal reset keeps failing silently. It
// also refreshes the stream's first sequence and prunes expired entries.
func (i *Index) audit() {
	i.mu.Lock()
	open, stream, consumer, created, last, audited := i.open, i.stream, i.consumer, i.created, i.last, i.audited
	i.audited = last
	i.mu.Unlock()
	if !open || stream == nil {
		return
	}
	info, err := stream.Info(i.ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			i.markDue("stream missing")
		}
		return
	}
	if !info.Created.Equal(created) || info.State.LastSeq < last {
		i.markDue("stream changed")
		return
	}
	if consumer != nil {
		ci, err := consumer.Info(i.ctx)
		if errors.Is(err, jetstream.ErrConsumerNotFound) || err == nil && ci.NumPending > 0 && last == audited {
			i.markDue("consumer stalled")
			return
		}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.epoch == 0 || !i.created.Equal(info.Created) {
		return
	}
	i.first = info.State.FirstSeq
	i.prune(time.Now())
}

// prune drops entries the stream no longer holds: below its first sequence
// or older than its maximum age.
func (i *Index) prune(now time.Time) {
	for thread, entries := range i.threads {
		keep := entries[:0]
		for _, e := range entries {
			if e.Sequence < i.first || i.cfg.Stream.MaxAge > 0 && now.Sub(e.Time) > i.cfg.Stream.MaxAge {
				i.forget(e)
				continue
			}
			keep = append(keep, e)
		}
		if len(keep) == 0 {
			delete(i.threads, thread)
		} else {
			i.threads[thread] = keep
		}
	}
}

func (i *Index) forget(e *Entry) {
	delete(i.held[e.Thread], e.Kind+":"+strconv.FormatUint(e.ID, 10))
	if len(i.held[e.Thread]) == 0 {
		delete(i.held, e.Thread)
	}
	if e.SenderKey != "" && e.Nonce != "" {
		k := e.SenderKey + ":" + e.Nonce
		i.sends[k] = slices.DeleteFunc(i.sends[k], func(x *Entry) bool { return x == e })
		if len(i.sends[k]) == 0 {
			delete(i.sends, k)
		}
	}
}

// wait blocks until the gate is open or ctx ends.
func (i *Index) wait(ctx context.Context) error {
	for {
		i.mu.Lock()
		if i.open {
			i.mu.Unlock()
			return nil
		}
		changed := i.changed
		i.mu.Unlock()
		select {
		case <-ctx.Done():
			return ErrUnavailable
		case <-i.ctx.Done():
			return ErrUnavailable
		case <-changed:
		}
	}
}

// Position is the newest position the index holds, once replay has caught up.
func (i *Index) Position(ctx context.Context) (*pb.StreamPosition, error) {
	if err := i.wait(ctx); err != nil {
		return nil, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return &pb.StreamPosition{Epoch: i.epoch, Sequence: i.last}, nil
}

// Holds reports whether the index holds the thread's activity of that kind
// and snowflake. Recovery uses it to skip messages already published.
func (i *Index) Holds(thread uint64, kind string, id uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.held[thread][kind+":"+strconv.FormatUint(id, 10)]
}

// snapshot identifies the generation and ingestion a result was taken from.
type snapshot struct {
	version, invalidations uint64
	created                time.Time
	stream                 jetstream.Stream
}

func (i *Index) snap() snapshot {
	return snapshot{version: i.version, invalidations: i.invalidations, created: i.created, stream: i.stream}
}

// Bodies reads the events at the entries' sequences and verifies that the
// stream is still the generation the entries came from. It returns
// errRetry when the caller must wait for the gate and take its entries
// again; entries whose message the unchanged stream no longer holds are
// dropped from the index and left out.
var errRetry = errors.New("activity index changed during read")

func (i *Index) bodies(ctx context.Context, s snapshot, entries []*Entry) ([]*pb.Activity, error) {
	out := make([]*pb.Activity, 0, len(entries))
	missing := []*Entry{}
	for _, e := range entries {
		m, err := s.stream.GetMsg(ctx, e.Sequence)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			missing = append(missing, e)
			continue
		}
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			i.markDue("stream missing during read")
			return nil, errRetry
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ErrUnavailable
			}
			return nil, errRetry
		}
		a := &pb.Activity{}
		if err = protojson.Unmarshal(m.Data, a); err != nil {
			return nil, err
		}
		a.Sequence = e.Sequence
		out = append(out, a)
	}
	info, err := s.stream.Info(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			i.markDue("stream missing during read")
		}
		if ctx.Err() != nil {
			return nil, ErrUnavailable
		}
		return nil, errRetry
	}
	if !info.Created.Equal(s.created) {
		i.markDue("stream recreated during read")
		return nil, errRetry
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.version != s.version || i.invalidations != s.invalidations {
		return nil, errRetry
	}
	for _, e := range missing {
		i.drop(e)
	}
	return out, nil
}

func (i *Index) drop(e *Entry) {
	entries := i.threads[e.Thread]
	i.threads[e.Thread] = slices.DeleteFunc(entries, func(x *Entry) bool { return x == e })
	if len(i.threads[e.Thread]) == 0 {
		delete(i.threads, e.Thread)
	}
	i.forget(e)
}

// retry runs read until it succeeds without the index changing under it,
// waiting for the gate before each attempt.
func retryRead[T any](ctx context.Context, i *Index, read func() (T, error)) (T, error) {
	for {
		var zero T
		if err := i.wait(ctx); err != nil {
			return zero, err
		}
		v, err := read()
		if errors.Is(err, errRetry) {
			select {
			case <-ctx.Done():
				return zero, ErrUnavailable
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		return v, err
	}
}

// Send is a completed send the stream holds.
type Send struct {
	Messages  []*pb.Activity
	Start     *pb.StreamPosition
	PollError string
}

// Completed returns the send with this fingerprint and nonce while the stream
// holds a message it published, with the messages read from the stream.
func (i *Index) Completed(ctx context.Context, fingerprint, nonce string) (*Send, error) {
	return retryRead(ctx, i, func() (*Send, error) {
		i.mu.Lock()
		found := slices.Clone(i.sends[fingerprint+":"+nonce])
		s := i.snap()
		i.mu.Unlock()
		if len(found) == 0 {
			return nil, nil
		}
		slices.SortFunc(found, func(a, b *Entry) int { return compare(a.Sequence, b.Sequence) })
		unique := []*Entry{}
		seen := map[uint64]bool{}
		for _, e := range found {
			if !seen[e.ID] {
				seen[e.ID] = true
				unique = append(unique, e)
			}
		}
		messages, err := i.bodies(ctx, s, unique)
		if err != nil || len(messages) == 0 {
			return nil, err
		}
		out := &Send{Messages: messages, Start: unique[0].SendStart}
		for _, e := range unique {
			if e.PollError != "" {
				out.PollError = e.PollError
			}
		}
		return out, nil
	})
}

// Confirm returns once every message of a send is discoverable by Completed,
// or once the index has passed the message's sequence in a generation that
// does not hold it (a publish whose stream was replaced); a repeat then sends
// again, as the repeated-sends guarantee allows.
func (i *Index) Confirm(ctx context.Context, fingerprint, nonce string, messages []*pb.Activity) error {
	for {
		i.mu.Lock()
		pending := []*pb.Activity{}
		if i.open {
			for _, m := range messages {
				if !slices.ContainsFunc(i.sends[fingerprint+":"+nonce], func(e *Entry) bool { return e.ID == m.Id }) {
					pending = append(pending, m)
				}
			}
			if len(pending) == 0 {
				i.mu.Unlock()
				return nil
			}
		}
		passed := i.open && len(pending) > 0 && !slices.ContainsFunc(pending, func(m *pb.Activity) bool { return m.Sequence > i.last })
		s, changed := i.snap(), i.changed
		i.mu.Unlock()
		if passed {
			// The stream may have been replaced since the index read it:
			// confirm the generation before giving up on the message.
			info, err := s.stream.Info(ctx)
			if err == nil && info.Created.Equal(s.created) {
				i.mu.Lock()
				same := i.version == s.version && i.invalidations == s.invalidations
				i.mu.Unlock()
				if same {
					return nil
				}
			} else if err == nil || errors.Is(err, jetstream.ErrStreamNotFound) {
				i.markDue("stream changed during confirm")
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-i.ctx.Done():
			return ErrUnavailable
		case <-changed:
		case <-time.After(catchUpPoll):
		}
	}
}

func compare(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// Count is one cursor's unread count from the index.
type Count struct {
	Thread  uint64
	Count   uint32
	Own     []*pb.OwnMessage
	Horizon bool
	entries []*Entry
}

// Unread is a consistent view of the index for a set of cursors.
type Unread struct {
	Counts  []Count
	Listed  []*Entry
	More    bool
	Through *pb.StreamPosition
	snap    snapshot
}

// after selects a thread's entries after a cursor: a greater sequence in the
// stream's epoch, and everything for a cursor of another epoch. Call with mu.
func (i *Index) after(thread uint64, cursor *pb.StreamPosition) []*Entry {
	out := []*Entry{}
	for _, e := range i.threads[thread] {
		if cursor == nil || cursor.Epoch != i.epoch || e.Sequence > cursor.Sequence {
			out = append(out, e)
		}
	}
	return out
}

func (i *Index) count(thread uint64, cursor *pb.StreamPosition) Count {
	c := Count{Thread: thread}
	c.Horizon = cursor == nil || cursor.Epoch != i.epoch || i.first > 0 && cursor.Sequence+1 < i.first
	for _, e := range i.after(thread, cursor) {
		if !e.Counts() {
			continue
		}
		c.Count++
		c.entries = append(c.entries, e)
		if e.SenderKey != "" && e.Nonce != "" {
			c.Own = append(c.Own, &pb.OwnMessage{Id: e.ID, Nonce: e.Nonce, SenderKey: e.SenderKey})
		}
	}
	return c
}

// Unread waits for the gate and takes, in one view, each cursor's count and
// the counted entries in stream order up to limit (zero lists nothing).
func (i *Index) Unread(ctx context.Context, cursors []*pb.Cursor, limit int) (*Unread, error) {
	if err := i.wait(ctx); err != nil {
		return nil, err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.open {
		return nil, errRetry
	}
	return i.unread(cursors, limit), nil
}

func (i *Index) unread(cursors []*pb.Cursor, limit int) *Unread {
	u := &Unread{snap: i.snap()}
	all := []*Entry{}
	for _, c := range cursors {
		count := i.count(c.GetThreadId(), c.GetAfter())
		all = append(all, count.entries...)
		u.Counts = append(u.Counts, count)
	}
	slices.SortFunc(all, func(a, b *Entry) int { return compare(a.Sequence, b.Sequence) })
	all = slices.CompactFunc(all, func(a, b *Entry) bool { return a == b })
	if limit > 0 && len(all) > limit {
		u.Listed, u.More = all[:limit], true
		u.Through = &pb.StreamPosition{Epoch: i.epoch, Sequence: all[limit-1].Sequence}
	} else {
		u.Listed = all
		u.Through = &pb.StreamPosition{Epoch: i.epoch, Sequence: i.last}
	}
	return u
}

// List reads the bodies of an Unread's listed entries, verified against the
// generation they came from. errRetry means: take the Unread again.
func (i *Index) List(ctx context.Context, u *Unread) ([]*pb.Activity, error) {
	if len(u.Listed) == 0 {
		return nil, nil
	}
	return i.bodies(ctx, u.snap, u.Listed)
}

// IsRetry reports whether a read must be taken again after the gate opens.
func IsRetry(err error) bool { return errors.Is(err, errRetry) }

// Changed returns a channel closed at the index's next change.
func (i *Index) Changed() <-chan struct{} {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.changed
}

// Close stops the worker and its consumer.
func (i *Index) Close() {
	i.cancel()
	i.wg.Wait()
}
