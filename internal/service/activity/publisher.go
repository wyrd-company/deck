// ---
// relationships:
//
//	implements:
//	  - service
//	  - events
//
// ---
// Package activity owns publication of deck's activity envelopes.
package activity

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// EnsureStream is also called by task 7's generation-check worker.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg config.NATS) (jetstream.Stream, error) {
	if cfg.Stream.Manage {
		return js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: cfg.Stream.Name, Subjects: []string{cfg.SubjectPrefix + ".>"}, Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardOld, MaxAge: cfg.Stream.MaxAge, Duplicates: cfg.Stream.DuplicateWindow, Replicas: cfg.Stream.Replicas})
	}
	s, err := js.Stream(ctx, cfg.Stream.Name)
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return nil, err
	}
	for _, subject := range info.Config.Subjects {
		if subject == cfg.SubjectPrefix+".>" || subject == ">" {
			return s, nil
		}
	}
	return nil, errors.New("nats.stream: does not cover activity subject")
}

// Envelope excludes API-only sequence and keeps the replay index in headers.
func Envelope(prefix string, a *pb.Activity, start *pb.StreamPosition) (*nats.Msg, error) {
	if a == nil || a.Id == 0 || a.Time == nil || a.Bot == "" || a.ServerId == 0 || a.ChannelId == 0 || a.ThreadId == 0 {
		return nil, errors.New("invalid activity")
	}
	kind := ""
	switch a.Kind.(type) {
	case *pb.Activity_Message:
		kind = "message"
	case *pb.Activity_Component:
		kind = "component"
	case *pb.Activity_PollVote:
		kind = "poll_vote"
	default:
		return nil, errors.New("missing activity kind")
	}
	copy := proto.Clone(a).(*pb.Activity)
	copy.Sequence = 0
	payload, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(copy)
	if err != nil {
		return nil, err
	}
	m := nats.NewMsg(fmt.Sprintf("%s.%d.%d.%d", prefix, a.ServerId, a.ChannelId, a.ThreadId))
	m.Data = payload
	m.Header.Set("Nats-Msg-Id", fmt.Sprintf("%s:%d", kind, a.Id))
	m.Header.Set("Deck-Kind", kind)
	m.Header.Set("Deck-Activity-Id", strconv.FormatUint(a.Id, 10))
	m.Header.Set("Deck-Schema", "deck.v1.Activity")
	if kind == "message" {
		m.Header.Set("Deck-Message-Type", strconv.FormatUint(uint64(a.GetMessage().Type), 10))
	}
	if a.SentBy != "" {
		if a.SenderKey == "" || a.Nonce == "" || start == nil {
			return nil, errors.New("missing send identity")
		}
		m.Header.Set("Deck-Sent-By", a.SentBy)
		m.Header.Set("Deck-Sender-Key", a.SenderKey)
		m.Header.Set("Deck-Nonce", a.Nonce)
		m.Header.Set("Deck-Send-Start", fmt.Sprintf("%d:%d", start.Epoch, start.Sequence))
	}
	return m, nil
}

type Result struct {
	Sequence uint64
	Err      error
}
type queued struct {
	m    *nats.Msg
	done chan Result
}

// Publisher serializes accepted work. A caller's deadline never cancels retries.
// Task 7 can apply admission limits before Submit; accepted work is never dropped.
type Publisher struct {
	js     jetstream.JetStream
	prefix string
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	queue  []queued
	wake   chan struct{}
	wg     sync.WaitGroup
}

func New(ctx context.Context, js jetstream.JetStream, prefix string) *Publisher {
	ctx, cancel := context.WithCancel(ctx)
	p := &Publisher{js: js, prefix: prefix, ctx: ctx, cancel: cancel, wake: make(chan struct{}, 1)}
	p.wg.Add(1)
	go p.run()
	return p
}
func (p *Publisher) Submit(a *pb.Activity, start *pb.StreamPosition) (<-chan Result, error) {
	m, err := Envelope(p.prefix, a, start)
	if err != nil {
		return nil, err
	}
	q := queued{m: m, done: make(chan Result, 1)}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return nil, p.ctx.Err()
	}
	p.queue = append(p.queue, q)
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return q.done, nil
}
func (p *Publisher) Publish(ctx context.Context, a *pb.Activity, start *pb.StreamPosition) (uint64, error) {
	done, err := p.Submit(a, start)
	if err != nil {
		return 0, err
	}
	select {
	case r := <-done:
		return r.Sequence, r.Err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
func (p *Publisher) Pending() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.queue) }
func (p *Publisher) run() {
	defer p.wg.Done()
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, q := range p.queue {
			q.done <- Result{Err: context.Canceled}
			close(q.done)
		}
		p.queue = nil
	}()
	for {
		p.mu.Lock()
		if len(p.queue) == 0 {
			p.mu.Unlock()
			select {
			case <-p.ctx.Done():
				return
			case <-p.wake:
				continue
			}
		}
		q := p.queue[0]
		p.mu.Unlock()
		delay := 50 * time.Millisecond
		for {
			ctx, cancel := context.WithTimeout(p.ctx, time.Second)
			ack, err := p.js.PublishMsg(ctx, q.m)
			cancel()
			if err == nil {
				q.done <- Result{Sequence: ack.Sequence}
				close(q.done)
				p.mu.Lock()
				p.queue = p.queue[1:]
				p.mu.Unlock()
				break
			}
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(delay):
			}
			if delay < 2*time.Second {
				delay *= 2
			}
		}
	}
}
func (p *Publisher) Close() { p.cancel(); p.wg.Wait() }
