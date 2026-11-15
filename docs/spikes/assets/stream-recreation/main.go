// ---
// relationships:
//
//	verifies:
//	  - service
//	  - events
//
// ---
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func emit(v any) { must(json.NewEncoder(os.Stdout).Encode(v)) }

func receive[T any](ch <-chan T) T {
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		panic("timed out waiting for consumer observation")
	}
}

func run(label string, repair, recreate, managed bool) {
	dir, err := os.MkdirTemp("", "deck-stream-spike-")
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
	nc, err := nats.Connect(s.ClientURL())
	must(err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := jetstream.StreamConfig{Name: "DECK", Subjects: []string{"deck.>"}, Storage: jetstream.FileStorage}
	stream, err := js.CreateStream(ctx, cfg)
	must(err)
	oldInfo, err := stream.Info(ctx)
	must(err)
	publish := func(from, through int) {
		for n := from; n <= through; n++ {
			_, err := js.Publish(ctx, "deck.1.2.3", []byte(fmt.Sprint(n)))
			must(err)
		}
	}
	seen := make(chan uint64, 64)
	errors := make(chan string, 64)
	var invalid atomic.Bool
	start := func(first uint64) jetstream.ConsumeContext {
		config := jetstream.OrderedConsumerConfig{FilterSubjects: []string{"deck.>"}, HeadersOnly: true, DeliverPolicy: jetstream.DeliverAllPolicy}
		if first > 1 {
			config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			config.OptStartSeq = first
		}
		consumer, err := js.OrderedConsumer(ctx, "DECK", config)
		must(err)
		cc, err := consumer.Consume(func(msg jetstream.Msg) {
			if repair && invalid.Load() {
				return
			}
			meta, err := msg.Metadata()
			must(err)
			seen <- meta.Sequence.Stream
		}, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
			if repair {
				invalid.Store(true)
			}
			errors <- err.Error()
		}))
		must(err)
		return cc
	}
	cc := start(1)
	defer func() { cc.Stop() }()
	publish(1, 5)
	for want := uint64(1); want <= 5; want++ {
		if got := receive(seen); got != want {
			panic(fmt.Sprintf("old stream: got %d, want %d", got, want))
		}
	}
	if recreate {
		must(js.DeleteStream(ctx, "DECK"))
	} else {
		consumers := stream.ListConsumers(ctx)
		for info := range consumers.Info() {
			must(js.DeleteConsumer(ctx, "DECK", info.Name))
		}
		must(consumers.Err())
	}
	if recreate && !managed {
		stream, err = js.CreateStream(ctx, cfg)
		must(err)
		publish(1, 7)
	}
	trigger := receive(errors)
	if trigger != jetstream.ErrConsumerDeleted.Error() {
		panic(fmt.Sprintf("unexpected error trigger: %q", trigger))
	}
	closed := false
	select {
	case <-cc.Closed():
		closed = true
	default:
	}
	if closed {
		panic("consume context ended before the observed error trigger")
	}
	if managed {
		// The error arrives while the stream is absent. Management must run
		// before reading creation time, just as it does at process startup.
		_, err := js.Stream(ctx, "DECK")
		if err != jetstream.ErrStreamNotFound {
			panic(fmt.Sprintf("expected missing stream before management: %v", err))
		}
	}
	if repair {
		cc.Stop()
		select {
		case <-cc.Closed():
		case <-ctx.Done():
			panic("consumer did not stop")
		}
		// Ignore callbacks queued before invalidation; they belong to the old
		// consume context. A new consumer owns the rebuilt index.
		for len(seen) > 0 {
			<-seen
		}
		if managed {
			stream, err = js.CreateStream(ctx, cfg)
			must(err)
			publish(1, 7)
		}
		info, err := stream.Info(ctx)
		must(err)
		changed := !info.Created.Equal(oldInfo.Created)
		if changed != recreate {
			panic("unexpected stream creation-time comparison")
		}
		first := uint64(6)
		if changed {
			first = 1
		}
		invalid.Store(false)
		cc = start(first)
		after, err := stream.Info(ctx)
		must(err)
		if !after.Created.Equal(info.Created) {
			panic("generation changed while creating fresh consumer")
		}
	}
	info, err := stream.Info(ctx)
	must(err)
	changed := !info.Created.Equal(oldInfo.Created)
	if changed != recreate {
		panic("unexpected measured creation-time comparison")
	}
	if !recreate {
		publish(6, 7)
	}
	want := []uint64{6, 7}
	if repair && recreate {
		want = []uint64{1, 2, 3, 4, 5, 6, 7}
	}
	got := make([]uint64, 0, len(want))
	for range want {
		got = append(got, receive(seen))
	}
	if !reflect.DeepEqual(got, want) {
		panic(fmt.Sprintf("%s: got %v, want %v", label, got, want))
	}
	emit(map[string]any{"case": label, "trigger": trigger, "closed_at_trigger": closed, "creation_changed": changed, "new_sequences": got, "pass": true})
}

func main() {
	emit(map[string]any{"nats_server": server.VERSION, "nats_go": nats.Version, "pass": false, "state": "started"})
	run("unrepaired-recreation", false, true, false)
	run("error-check-and-replay", true, true, false)
	run("missing-stream-managed-before-check", true, true, true)
	run("same-generation-consumer-deletion", true, false, false)
	for _, test := range []reconnectCase{
		{"reconnect-a-published-while-disconnected", true, true, true},
		{"reconnect-b-service-publishes-after-reconnect", true, false, true},
		{"reconnect-c-management-recreates-missing-stream", false, false, true},
		{"reconnect-d-unmanaged-waits-for-operator", false, false, false},
	} {
		runReconnect(test)
	}
	emit(map[string]any{"state": "completed", "pass": true})
}
