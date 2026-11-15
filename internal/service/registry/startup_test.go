package registry

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/config"
)

// Only the startup boundaries are stubbed: no registry worker can start when
// these calls fail. The CLI test also exercises cancellation over real NATS.
type startupJS struct {
	jetstream.JetStream
	kv  jetstream.KeyValue
	err error
}

func (j startupJS) CreateOrUpdateKeyValue(context.Context, jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return j.kv, j.err
}
func (j startupJS) KeyValue(context.Context, string) (jetstream.KeyValue, error) { return j.kv, j.err }

type startupKV struct {
	jetstream.KeyValue
	err error
}

func (k startupKV) Status(context.Context) (jetstream.KeyValueStatus, error) { return nil, k.err }

func TestOpenStartupErrors(t *testing.T) {
	for _, step := range []string{"create", "open", "status"} {
		for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("private upstream body")} {
			t.Run(step+"/"+cause.Error(), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // concurrent stop must not hide an independent error
				upstream := fmt.Errorf("private upstream body: %w", cause)
				js := startupJS{err: upstream}
				message := "nats.threads: cannot open bucket"
				if step == "status" {
					js.err = nil
					js.kv = startupKV{err: upstream}
					message = "nats.threads: cannot check bucket"
				}
				r, err := Open(ctx, js, config.Threads{Bucket: "test", Manage: step != "open"})
				if r != nil || err == nil {
					t.Fatalf("startup result: %v %v", r, err)
				}
				for _, sentinel := range []error{context.Canceled, context.DeadlineExceeded} {
					if errors.Is(err, sentinel) != errors.Is(cause, sentinel) {
						t.Fatalf("wrong error identity: %v, cause %v", err, cause)
					}
				}
				if cause == context.Canceled || cause == context.DeadlineExceeded {
					message += ": " + cause.Error()
				}
				if err.Error() != message {
					t.Fatalf("unsafe/wrong message: %q; want %q", err, message)
				}
			})
		}
	}
}
