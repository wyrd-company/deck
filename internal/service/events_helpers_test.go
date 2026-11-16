package service

import (
	"context"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/service/testutil"
	"google.golang.org/protobuf/encoding/protojson"
)

type event struct {
	seq     uint64
	subject string
	header  map[string]string
	a       *pb.Activity
}

// events reads every event the stream holds.
func (s *harness) events(t *testing.T) []event {
	t.Helper()
	ctx := context.Background()
	stream, err := s.js.Stream(ctx, "DECK")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	out := []event{}
	for seq := info.State.FirstSeq; seq > 0 && seq <= info.State.LastSeq; seq++ {
		m, err := stream.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		e := event{seq: seq, subject: m.Subject, header: map[string]string{}, a: &pb.Activity{}}
		for k := range m.Header {
			e.header[k] = m.Header.Get(k)
		}
		if err = protojson.Unmarshal(m.Data, e.a); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

// eventually waits until the stream holds n events and returns them.
func (s *harness) eventually(t *testing.T, n int) []event {
	t.Helper()
	var got []event
	testutil.Until(t, func() bool { got = s.events(t); return len(got) >= n })
	time.Sleep(150 * time.Millisecond) // nothing more arrives
	got = s.events(t)
	if len(got) != n {
		t.Fatalf("stream holds %d events, want %d: %v", len(got), n, got)
	}
	return got
}
