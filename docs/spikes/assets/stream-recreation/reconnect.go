package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// relay leaves the admin connection live while cutting the service's link.
type relay struct {
	listener net.Listener
	target   string
	online   atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
}

func newRelay(target string) *relay {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	r := &relay{listener: ln, target: target}
	r.online.Store(true)
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			if !r.online.Load() {
				r.mu.Unlock()
				client.Close()
				continue
			}
			upstream, err := net.DialTimeout("tcp", r.target, time.Second)
			if err != nil {
				r.mu.Unlock()
				client.Close()
				continue
			}
			r.conns = append(r.conns, client, upstream)
			r.mu.Unlock()
			go func() { _, _ = io.Copy(upstream, client); upstream.Close(); client.Close() }()
			go func() { _, _ = io.Copy(client, upstream); upstream.Close(); client.Close() }()
		}
	}()
	return r
}

func (r *relay) cut() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.online.Store(false)
	for _, conn := range r.conns {
		conn.Close()
	}
	r.conns = nil
}

func (r *relay) close() { r.listener.Close(); r.cut() }

type reconnectCase struct {
	label         string
	recreate      bool
	publishBefore bool
	manage        bool
}

func runReconnect(test reconnectCase) {
	dir, err := os.MkdirTemp("", "deck-reconnect-spike-")
	must(err)
	defer os.RemoveAll(dir)
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true})
	must(err)
	s.Start()
	defer s.WaitForShutdown()
	defer s.Shutdown()
	if !s.ReadyForConnections(5 * time.Second) {
		panic("embedded server not ready")
	}
	admin, err := nats.Connect(s.ClientURL())
	must(err)
	defer admin.Close()
	ajs, err := jetstream.New(admin)
	must(err)
	r := newRelay(s.Addr().String())
	defer r.close()
	var invalid, ready atomic.Bool
	var due atomic.Uint64
	wake := make(chan struct{}, 1)
	disconnected := make(chan struct{}, 1)
	service, err := nats.Connect("nats://"+r.listener.Addr().String(),
		nats.MaxReconnects(-1), nats.ReconnectWait(50*time.Millisecond), nats.ReconnectJitter(0, 0),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				invalid.Store(true)
				ready.Store(false)
				disconnected <- struct{}{}
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			invalid.Store(true)
			ready.Store(false)
			due.Add(1)
			select {
			case wake <- struct{}{}:
			default: // The monotonic due counter preserves a coalesced check.
			}
		}))
	must(err)
	defer service.Close()
	sjs, err := jetstream.New(service)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := jetstream.StreamConfig{Name: "DECK", Subjects: []string{"deck.>"}, Storage: jetstream.FileStorage}
	stream, err := ajs.CreateStream(ctx, cfg)
	must(err)
	oldInfo, err := stream.Info(ctx)
	must(err)
	seen := make(chan uint64, 64)
	start := func() jetstream.ConsumeContext {
		consumer, err := sjs.OrderedConsumer(ctx, "DECK", jetstream.OrderedConsumerConfig{FilterSubjects: []string{"deck.>"}, HeadersOnly: true, DeliverPolicy: jetstream.DeliverAllPolicy})
		must(err)
		cc, err := consumer.Consume(func(msg jetstream.Msg) {
			if invalid.Load() {
				return
			}
			meta, err := msg.Metadata()
			must(err)
			seen <- meta.Sequence.Stream
		})
		must(err)
		return cc
	}
	publish := func(js jetstream.JetStream, through int) {
		for n := 1; n <= through; n++ {
			_, err := js.Publish(ctx, "deck.1.2.3", []byte(fmt.Sprint(n)))
			must(err)
		}
	}
	cc := start()
	defer func() { cc.Stop() }()
	publish(ajs, 5)
	for want := uint64(1); want <= 5; want++ {
		if got := receive(seen); got != want {
			panic("old stream not indexed in order")
		}
	}
	ready.Store(true)
	r.cut()
	receive(disconnected)
	if !invalid.Load() || ready.Load() {
		panic("disconnect did not invalidate ingestion and reads")
	}
	must(ajs.DeleteStream(ctx, "DECK"))
	if test.recreate {
		_, err := ajs.CreateStream(ctx, cfg)
		must(err)
		if test.publishBefore {
			publish(ajs, 7)
		}
	}
	r.online.Store(true)
	receive(wake) // The actual reconnect callback wakes this check worker.
	checkDue := due.Load()
	if checkDue != 1 || !invalid.Load() || ready.Load() {
		panic("reconnect callback did not mark exactly one gated check due")
	}
	cc.Stop()
	select {
	case <-cc.Closed():
	case <-ctx.Done():
		panic("old consume context did not stop")
	}
	for len(seen) > 0 {
		<-seen
	}
	waitedForOperator := false
	stream, err = sjs.Stream(ctx, "DECK")
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if test.manage {
			stream, err = sjs.CreateStream(ctx, cfg)
		} else {
			// Exercise the failed lookup with no management: the gate stays
			// closed until the independent admin (operator) supplies a stream.
			if ready.Load() || !invalid.Load() {
				panic("missing unmanaged stream did not remain gated")
			}
			waitedForOperator = true
			_, err = ajs.CreateStream(ctx, cfg)
			must(err)
			stream, err = sjs.Stream(ctx, "DECK")
		}
	}
	must(err)
	info, err := stream.Info(ctx)
	must(err)
	changed := !info.Created.Equal(oldInfo.Created)
	if !changed {
		panic("recreated stream's creation time did not change")
	}
	invalid.Store(false)
	cc = start()
	after, err := stream.Info(ctx)
	must(err)
	if !after.Created.Equal(info.Created) || due.Load() != checkDue {
		panic("generation or due counter changed while replacing consumer")
	}
	if !test.publishBefore {
		publish(sjs, 7) // The service publishes after reconnecting in (b)-(d).
	}
	want := []uint64{1, 2, 3, 4, 5, 6, 7}
	got := make([]uint64, 0, len(want))
	for range want {
		got = append(got, receive(seen))
	}
	if !reflect.DeepEqual(got, want) {
		panic(fmt.Sprintf("%s: got %v, want %v", test.label, got, want))
	}
	ready.Store(true)
	emit(map[string]any{"case": test.label, "trigger": "reconnect-callback", "checks_due": checkDue, "creation_changed": changed, "waited_for_operator": waitedForOperator, "new_sequences": got, "pass": true})
}
