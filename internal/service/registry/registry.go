// ---
// relationships:
//
//	implements:
//	  - service
//	  - record-touched-threads-in-jetstream-key-value
//
// ---
// Package registry maintains the durable, one-bot-per-thread routing registry.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wyrd-company/deck/internal/config"
)

type Touch struct {
	TouchedAt time.Time `json:"touched_at"`
	State     string    `json:"state"`
}
type Entry struct {
	Bot           string           `json:"bot"`
	ServerID      uint64           `json:"server_id"`
	ChannelID     uint64           `json:"channel_id"`
	CreatedByDeck bool             `json:"created_by_deck"`
	Keys          map[string]Touch `json:"keys"`
	LastMessageID uint64           `json:"last_message_id"`
	ActiveAt      time.Time        `json:"active_at"`
}

var ErrBotConflict = errors.New("thread belongs to another bot")

type attempt struct {
	count              int
	removable, unknown bool
}
type Registry struct {
	KV        jetstream.KeyValue
	mu        sync.RWMutex
	entries   map[uint64]Entry
	revisions map[uint64]uint64
	dirty     map[uint64]bool
	attempts  map[string]*attempt
	cleanup   map[uint64]map[string]bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	watcher   jetstream.KeyWatcher
}

func Open(ctx context.Context, js jetstream.JetStream, cfg config.Threads) (*Registry, error) {
	var kv jetstream.KeyValue
	var err error
	if cfg.Manage {
		kv, err = js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: cfg.Bucket, History: 1, Replicas: cfg.Replicas})
	} else {
		kv, err = js.KeyValue(ctx, cfg.Bucket)
	}
	if err != nil {
		return nil, errors.New("nats.threads: cannot open bucket")
	}
	st, err := kv.Status(ctx)
	if err != nil || st.History() != 1 || st.TTL() != 0 {
		return nil, errors.New("nats.threads: bucket requires history 1 and no TTL")
	}
	return New(ctx, kv, cfg.MaxAge)
}
func New(ctx context.Context, kv jetstream.KeyValue, maxAge time.Duration) (*Registry, error) {
	ctx, cancel := context.WithCancel(ctx)
	r := &Registry{KV: kv, entries: map[uint64]Entry{}, revisions: map[uint64]uint64{}, dirty: map[uint64]bool{}, attempts: map[string]*attempt{}, cleanup: map[uint64]map[string]bool{}, ctx: ctx, cancel: cancel}
	w, err := kv.Watch(ctx, "threads.*")
	if err != nil {
		cancel()
		return nil, err
	}
	r.watcher = w
	for {
		select {
		case <-ctx.Done():
			w.Stop()
			cancel()
			return nil, ctx.Err()
		case e, ok := <-w.Updates():
			if !ok {
				w.Stop()
				cancel()
				return nil, errors.New("registry watch ended during load")
			}
			if e == nil {
				r.wg.Add(2)
				go r.watch()
				go r.retry(maxAge)
				return r, nil
			}
			r.apply(e)
		}
	}
}
func clone(e Entry) Entry {
	k := map[string]Touch{}
	for n, t := range e.Keys {
		k[n] = t
	}
	e.Keys = k
	return e
}
func (r *Registry) Lookup(id uint64) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[id]
	return clone(e), ok && (e.CreatedByDeck || len(e.Keys) > 0)
}
func (r *Registry) Snapshot() map[uint64]Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[uint64]Entry{}
	for id, e := range r.entries {
		if e.CreatedByDeck || len(e.Keys) > 0 {
			out[id] = clone(e)
		}
	}
	return out
}
func (r *Registry) Check(ctx context.Context) error { _, err := r.KV.Status(ctx); return err }
func (r *Registry) apply(v jetstream.KeyValueEntry) {
	id, err := strconv.ParseUint(strings.TrimPrefix(v.Key(), "threads."), 10, 64)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dirty[id] || v.Revision() <= r.revisions[id] {
		return
	}
	if v.Operation() != jetstream.KeyValuePut {
		delete(r.entries, id)
	} else {
		var e Entry
		if json.Unmarshal(v.Value(), &e) != nil {
			return
		}
		r.entries[id] = e
	}
	r.revisions[id] = v.Revision()
}
func (r *Registry) watch() {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case e, ok := <-r.watcher.Updates():
			if !ok {
				return
			}
			if e != nil {
				r.apply(e)
			}
		}
	}
}
func key(id uint64) string { return "threads." + strconv.FormatUint(id, 10) }

// writeLocked merges using the bucket revision; it never overwrites another bot.
func (r *Registry) writeLocked(ctx context.Context, id uint64, e Entry) error {
	for {
		old, err := r.KV.Get(ctx, key(id))
		var rev uint64
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
		if err == nil {
			var previous Entry
			if json.Unmarshal(old.Value(), &previous) != nil {
				return errors.New("invalid registry entry")
			}
			if previous.Bot != e.Bot {
				return ErrBotConflict
			}
			rev = old.Revision()
			for k, t := range previous.Keys {
				if r.cleanup[id][k] {
					continue
				}
				if current, ok := e.Keys[k]; !ok || t.State == "sent" && current.State != "sent" {
					e.Keys[k] = t
				}
			}
			if previous.LastMessageID > e.LastMessageID {
				e.LastMessageID = previous.LastMessageID
			}
		}
		if len(e.Keys) == 0 && !e.CreatedByDeck {
			if rev != 0 {
				if err := r.KV.Delete(ctx, key(id), jetstream.LastRevision(rev)); err != nil {
					return err
				}
			}
			delete(r.entries, id)
			delete(r.dirty, id)
			delete(r.cleanup, id)
			return nil
		}
		raw, err := json.Marshal(e)
		if err != nil {
			return err
		}
		var next uint64
		if rev == 0 {
			next, err = r.KV.Create(ctx, key(id), raw)
		} else {
			next, err = r.KV.Update(ctx, key(id), raw, rev)
		}
		if errors.Is(err, jetstream.ErrKeyExists) {
			continue
		}
		if err != nil {
			return err
		}
		r.entries[id] = clone(e)
		r.revisions[id] = next
		delete(r.dirty, id)
		delete(r.cleanup, id)
		return nil
	}
}

// RegisterPost makes routing visible immediately and retries persistence until stop.
func (r *Registry) RegisterPost(id uint64, e Entry, keyName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.entries[id]; ok {
		if old.Bot != e.Bot {
			return ErrBotConflict
		}
		e = old
	}
	if e.Keys == nil {
		e.Keys = map[string]Touch{}
	}
	e.Keys[keyName] = Touch{time.Now().UTC(), "sent"}
	e.ActiveAt = time.Now().UTC()
	if e.LastMessageID == 0 {
		e.LastMessageID = id
	}
	r.entries[id] = e
	r.dirty[id] = true
	return nil
}

// BeginReply persists pending before Discord is called, using the pre-send boundary.
func (r *Registry) BeginReply(ctx context.Context, id uint64, e Entry, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.entries[id]; ok {
		if old.Bot != e.Bot {
			return ErrBotConflict
		}
		e = old
	}
	if e.Keys == nil {
		e.Keys = map[string]Touch{}
	}
	t, exists := e.Keys[name]
	if !exists {
		t = Touch{time.Now().UTC(), "pending"}
	}
	e.Keys[name] = t
	e.ActiveAt = time.Now().UTC()
	if e.LastMessageID == 0 {
		e.LastMessageID = id
	}
	if err := r.writeLocked(ctx, id, e); err != nil {
		return err
	}
	k := fmt.Sprintf("%d:%s", id, name)
	a := r.attempts[k]
	if a == nil {
		a = &attempt{removable: !exists}
		r.attempts[k] = a
	}
	a.count++
	return nil
}

// FinishReply distinguishes a known rejection from an unknown REST outcome.
func (r *Registry) FinishReply(id uint64, name string, success, unknown bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	if !ok {
		return
	}
	k := fmt.Sprintf("%d:%s", id, name)
	a := r.attempts[k]
	if a == nil {
		return
	}
	a.count--
	a.unknown = a.unknown || unknown
	if success {
		t := e.Keys[name]
		t.State = "sent"
		e.Keys[name] = t
		e.ActiveAt = time.Now().UTC()
		r.entries[id] = e
		r.dirty[id] = true
	}
	if a.count > 0 {
		return
	}
	delete(r.attempts, k)
	if !success && !a.unknown && a.removable && e.Keys[name].State == "pending" {
		delete(e.Keys, name)
		if r.cleanup[id] == nil {
			r.cleanup[id] = map[string]bool{}
		}
		r.cleanup[id][name] = true
		r.entries[id] = e
		r.dirty[id] = true
		ctx, cancel := context.WithTimeout(r.ctx, time.Second)
		defer cancel()
		_ = r.writeLocked(ctx, id, e)
	}

}

// Advance is monotonic. Task 7 calls it only after its recovery/pending gate allows it.
func (r *Registry) Advance(id, message uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	if !ok {
		return
	}
	if message > e.LastMessageID {
		e.LastMessageID = message
	}
	e.ActiveAt = time.Now().UTC()
	r.entries[id] = e
	r.dirty[id] = true
}
func (r *Registry) Delete(ctx context.Context, id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.KV.Delete(ctx, key(id), jetstream.LastRevision(r.revisions[id])); err != nil {
		return err
	}
	delete(r.entries, id)
	delete(r.dirty, id)
	return nil
}
func (r *Registry) retry(maxAge time.Duration) {
	defer r.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	sweep := time.Now()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-t.C:
			r.mu.Lock()
			for id := range r.dirty {
				ctx, cancel := context.WithTimeout(r.ctx, time.Second)
				_ = r.writeLocked(ctx, id, r.entries[id])
				cancel()
			}
			r.mu.Unlock()
			if maxAge > 0 && time.Since(sweep) >= time.Hour {
				sweep = time.Now()
				for id, e := range r.Snapshot() {
					if time.Since(e.ActiveAt) > maxAge {
						ctx, cancel := context.WithTimeout(r.ctx, time.Second)
						_ = r.Delete(ctx, id)
						cancel()
					}
				}
			}
		}
	}
}
func (r *Registry) Pending() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.dirty) }
func (r *Registry) Close()       { r.cancel(); _ = r.watcher.Stop(); r.wg.Wait() }
