// ---
// relationships:
//
//	implements:
//	  - service
//	  - record-touched-threads-in-jetstream-key-value
//
// ---
// Package registry maintains the durable, one-bot-per-thread routing registry.
//
// The in-memory map is guarded by mu, which is never held across a NATS
// request: lookups stay answerable while the bucket is slow or unreachable.
// Writes to one thread are serialised by a per-thread write lock, read their
// input from a snapshot, perform the revision-checked key-value I/O without
// mu, and reconcile the result only when the local entry has not changed in
// the meantime; otherwise the entry stays dirty and is written again.
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

// BotConflictError carries the winning route from the local lookup or KV CAS.
// The handler must check grants before exposing any part of Owner.
type BotConflictError struct{ Owner Entry }

func (e *BotConflictError) Error() string { return ErrBotConflict.Error() }
func (e *BotConflictError) Unwrap() error { return ErrBotConflict }

// writeTimeout bounds one background write attempt of one entry.
const writeTimeout = time.Second

// watchedState is a bucket revision the watch delivered while the entry had
// local changes or a write in flight; the write's reconciliation, or the
// entry's settling afterwards, applies it so it is never lost.
type watchedState struct {
	rev     uint64
	entry   Entry
	deleted bool
}

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
	versions  map[uint64]uint64
	attempts  map[string]*attempt
	cleanup   map[uint64]map[string]bool
	writers   map[uint64]chan struct{}
	writing   map[uint64]int
	watched   map[uint64]watchedState
	hooks     []func(uint64, Entry)
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
		return nil, bucketFailure("nats.threads: cannot open bucket", err)
	}
	st, err := kv.Status(ctx)
	if err != nil {
		return nil, bucketFailure("nats.threads: cannot check bucket", err)
	}
	if st.History() != 1 || st.TTL() != 0 {
		return nil, errors.New("nats.threads: bucket requires history 1 and no TTL")
	}
	return New(ctx, kv, cfg.MaxAge)
}

// Preserve only safe cancellation identities, never upstream error bodies.
// A concurrent stop must not hide an independent bucket failure.
func bucketFailure(message string, cause error) error {
	if errors.Is(cause, context.Canceled) {
		return fmt.Errorf("%s: %w", message, context.Canceled)
	}
	if errors.Is(cause, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", message, context.DeadlineExceeded)
	}
	return errors.New(message)
}

func newRegistry(ctx context.Context, kv jetstream.KeyValue) *Registry {
	ctx, cancel := context.WithCancel(ctx)
	return &Registry{KV: kv, entries: map[uint64]Entry{}, revisions: map[uint64]uint64{}, dirty: map[uint64]bool{}, versions: map[uint64]uint64{}, attempts: map[string]*attempt{}, cleanup: map[uint64]map[string]bool{}, writers: map[uint64]chan struct{}{}, writing: map[uint64]int{}, watched: map[uint64]watchedState{}, ctx: ctx, cancel: cancel}
}

func New(ctx context.Context, kv jetstream.KeyValue, maxAge time.Duration) (*Registry, error) {
	r := newRegistry(ctx, kv)
	w, err := kv.Watch(r.ctx, "threads.*")
	if err != nil {
		r.cancel()
		return nil, bucketFailure("nats.threads: cannot watch bucket", err)
	}
	r.watcher = w
	for {
		select {
		case <-r.ctx.Done():
			err := ctx.Err()
			_ = w.Stop()
			r.cancel()
			return nil, err
		case e, ok := <-w.Updates():
			if !ok {
				// nats.go closes the updates channel when the load is canceled,
				// and select may pick that before Done: report the stop, not a
				// failure, whenever the caller's context has ended.
				err := ctx.Err()
				_ = w.Stop()
				r.cancel()
				if err != nil {
					return nil, err
				}
				return nil, errors.New("nats.threads: registry watch ended during load")
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
func visible(e Entry) bool { return e.CreatedByDeck || len(e.Keys) > 0 }

func (r *Registry) Lookup(id uint64) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[id]
	return clone(e), ok && visible(e)
}
func (r *Registry) Snapshot() map[uint64]Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[uint64]Entry{}
	for id, e := range r.entries {
		if visible(e) {
			out[id] = clone(e)
		}
	}
	return out
}

// OnRegister calls f, outside the registry's locks, each time a thread
// becomes registered in memory: by a local post or reply, or by a watched
// write of another instance. Register hooks before serving.
func (r *Registry) OnRegister(f func(id uint64, e Entry)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks = append(r.hooks, f)
}

// notify runs registration hooks; call it without holding mu.
func (r *Registry) notify(id uint64, e Entry, before bool) {
	if before || !visible(e) {
		return
	}
	r.mu.RLock()
	hooks := append([]func(uint64, Entry){}, r.hooks...)
	r.mu.RUnlock()
	for _, f := range hooks {
		f(id, clone(e))
	}
}

func (r *Registry) Check(ctx context.Context) error { _, err := r.KV.Status(ctx); return err }
func (r *Registry) apply(v jetstream.KeyValueEntry) {
	id, err := strconv.ParseUint(strings.TrimPrefix(v.Key(), "threads."), 10, 64)
	if err != nil {
		return
	}
	var now Entry
	deleted := v.Operation() != jetstream.KeyValuePut
	if !deleted && json.Unmarshal(v.Value(), &now) != nil {
		return
	}
	r.mu.Lock()
	if v.Revision() <= r.revisions[id] {
		r.mu.Unlock()
		return
	}
	if r.dirty[id] || r.writing[id] > 0 {
		// Local changes or a write are pending: keep the newer revision for
		// the write's reconciliation, or for settle once the entry is clean.
		if w, ok := r.watched[id]; !ok || v.Revision() > w.rev {
			r.watched[id] = watchedState{rev: v.Revision(), entry: now, deleted: deleted}
		}
		r.mu.Unlock()
		return
	}
	old, existed := r.entries[id]
	before := existed && visible(old)
	if deleted {
		delete(r.entries, id)
	} else {
		r.entries[id] = now
	}
	r.revisions[id] = v.Revision()
	r.mu.Unlock()
	r.notify(id, now, before)
}

// settle applies a revision the watch delivered during a write once the
// entry has no write in flight and no local change; call with mu held.
func (r *Registry) settle(id uint64) {
	w, ok := r.watched[id]
	if !ok || r.writing[id] > 0 || r.dirty[id] {
		return
	}
	delete(r.watched, id)
	if w.rev <= r.revisions[id] {
		return
	}
	if w.deleted {
		delete(r.entries, id)
	} else {
		r.entries[id] = clone(w.entry)
	}
	r.revisions[id] = w.rev
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

// lock serialises writers of one thread. It never holds mu while waiting, and
// a waiter gives up when its context ends.
func (r *Registry) lock(ctx context.Context, id uint64) (func(), error) {
	r.mu.Lock()
	ch := r.writers[id]
	if ch == nil {
		ch = make(chan struct{}, 1)
		r.writers[id] = ch
	}
	r.mu.Unlock()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// write merges e into the durable entry with revision-checked writes. It
// never overwrites another bot's entry, keeps `sent` over `pending`, keeps
// keys only the bucket holds unless cleanup removed them, and never lowers
// the boundary. It must be called without mu.
func (r *Registry) write(ctx context.Context, id uint64, e Entry, cleanup map[string]bool) (Entry, uint64, bool, error) {
	e = clone(e)
	for {
		old, err := r.KV.Get(ctx, key(id))
		var rev uint64
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return Entry{}, 0, false, err
		}
		if err == nil {
			var previous Entry
			if json.Unmarshal(old.Value(), &previous) != nil {
				return Entry{}, 0, false, errors.New("invalid registry entry")
			}
			if previous.Bot != e.Bot {
				return Entry{}, 0, false, &BotConflictError{Owner: clone(previous)}
			}
			rev = old.Revision()
			for k, t := range previous.Keys {
				if cleanup[k] {
					continue
				}
				if current, ok := e.Keys[k]; !ok || t.State == "sent" && current.State != "sent" {
					e.Keys[k] = t
				}
			}
			if previous.LastMessageID > e.LastMessageID {
				e.LastMessageID = previous.LastMessageID
			}
			if previous.ActiveAt.After(e.ActiveAt) {
				e.ActiveAt = previous.ActiveAt
			}
			e.CreatedByDeck = e.CreatedByDeck || previous.CreatedByDeck
		}
		if !visible(e) {
			if rev != 0 {
				if err := r.KV.Delete(ctx, key(id), jetstream.LastRevision(rev)); err != nil {
					return Entry{}, 0, false, err
				}
			}
			return e, 0, true, nil
		}
		raw, err := json.Marshal(e)
		if err != nil {
			return Entry{}, 0, false, err
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
			return Entry{}, 0, false, err
		}
		return e, next, false, nil
	}
}

// fold adds durable state to a local entry that changed while it was written.
func fold(local, durable Entry, cleanup map[string]bool) Entry {
	local = clone(local)
	for k, t := range durable.Keys {
		if cleanup[k] {
			continue
		}
		if current, ok := local.Keys[k]; !ok || t.State == "sent" && current.State != "sent" {
			local.Keys[k] = t
		}
	}
	if durable.LastMessageID > local.LastMessageID {
		local.LastMessageID = durable.LastMessageID
	}
	local.CreatedByDeck = local.CreatedByDeck || durable.CreatedByDeck
	return local
}

// reconcile applies a finished write under mu. When the local entry changed
// during the I/O, its newer state is kept, the durable state folded in, and
// the entry stays dirty for the next write.
func (r *Registry) reconcile(id, version uint64, written Entry, rev uint64, deleted bool, cleanup map[string]bool) {
	if rev > r.revisions[id] {
		r.revisions[id] = rev
	}
	if w, ok := r.watched[id]; ok && w.rev > rev {
		// A later revision, built on this write by another writer, arrived
		// through the watch before this reconciliation: it is the durable
		// state, and must not be overwritten with this older write.
		delete(r.watched, id)
		r.revisions[id] = w.rev
		if r.versions[id] != version {
			if local, ok := r.entries[id]; ok && !w.deleted {
				r.entries[id] = fold(local, w.entry, cleanup)
			}
			return
		}
		written, deleted = w.entry, w.deleted
	} else if ok {
		delete(r.watched, id)
	}
	if r.versions[id] != version {
		if local, ok := r.entries[id]; ok && !deleted {
			r.entries[id] = fold(local, written, cleanup)
		}
		return
	}
	if deleted {
		delete(r.entries, id)
	} else {
		r.entries[id] = clone(written)
	}
	delete(r.dirty, id)
	for k := range cleanup {
		delete(r.cleanup[id], k)
	}
	if len(r.cleanup[id]) == 0 {
		delete(r.cleanup, id)
	}
}

func copySet(s map[string]bool) map[string]bool {
	out := make(map[string]bool, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// mark records a local change under mu.
func (r *Registry) mark(id uint64, e Entry) {
	r.entries[id] = e
	r.versions[id]++
	r.dirty[id] = true
}

// RegisterPost makes routing visible immediately and retries persistence until stop.
func (r *Registry) RegisterPost(id uint64, e Entry, keyName string) error {
	r.mu.Lock()
	old, existed := r.entries[id]
	before := existed && visible(old)
	if existed {
		if old.Bot != e.Bot {
			r.mu.Unlock()
			return &BotConflictError{Owner: clone(old)}
		}
		created := e.CreatedByDeck
		e = clone(old)
		e.CreatedByDeck = e.CreatedByDeck || created
	}
	if e.Keys == nil {
		e.Keys = map[string]Touch{}
	}
	e.Keys[keyName] = Touch{time.Now().UTC(), "sent"}
	e.ActiveAt = time.Now().UTC()
	if e.LastMessageID == 0 {
		e.LastMessageID = id
	}
	r.mark(id, e)
	r.mu.Unlock()
	r.notify(id, e, before)
	return nil
}

// BeginReply persists pending before Discord is called, using the pre-send
// boundary. The in-memory entry changes only after the write succeeds, and
// the write runs without mu.
func (r *Registry) BeginReply(ctx context.Context, id uint64, e Entry, name string) error {
	unlock, err := r.lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	r.mu.Lock()
	old, existed := r.entries[id]
	before := existed && visible(old)
	if existed {
		if old.Bot != e.Bot {
			r.mu.Unlock()
			return &BotConflictError{Owner: clone(old)}
		}
		e = clone(old)
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
	version, cleanup := r.versions[id], copySet(r.cleanup[id])
	delete(cleanup, name)
	r.writing[id]++
	r.mu.Unlock()
	written, rev, _, err := r.write(ctx, id, e, cleanup)
	r.mu.Lock()
	r.writing[id]--
	if err != nil {
		r.settle(id)
		r.mu.Unlock()
		return err
	}
	r.reconcile(id, version, written, rev, false, cleanup)
	if local, ok := r.entries[id]; ok && r.versions[id] != version {
		// A concurrent local change kept its state; the touch written above
		// must be in memory as well.
		if _, has := local.Keys[name]; !has {
			local.Keys[name] = written.Keys[name]
		}
		r.entries[id] = local
	}
	delete(r.cleanup[id], name)
	k := fmt.Sprintf("%d:%s", id, name)
	a := r.attempts[k]
	if a == nil {
		a = &attempt{removable: !exists}
		r.attempts[k] = a
	}
	a.count++
	r.settle(id)
	now := r.entries[id]
	r.mu.Unlock()
	r.notify(id, now, before)
	return nil
}

// FinishReply distinguishes a known rejection from an unknown REST outcome.
func (r *Registry) FinishReply(id uint64, name string, success, unknown bool) {
	r.mu.Lock()
	e, ok := r.entries[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	k := fmt.Sprintf("%d:%s", id, name)
	a := r.attempts[k]
	if a == nil {
		r.mu.Unlock()
		return
	}
	a.count--
	a.unknown = a.unknown || unknown
	if success {
		e = clone(e)
		t := e.Keys[name]
		t.State = "sent"
		e.Keys[name] = t
		e.ActiveAt = time.Now().UTC()
		r.mark(id, e)
	}
	if a.count > 0 {
		r.mu.Unlock()
		return
	}
	delete(r.attempts, k)
	cleanup := !success && !a.unknown && a.removable && e.Keys[name].State == "pending"
	if cleanup {
		e = clone(e)
		delete(e.Keys, name)
		if r.cleanup[id] == nil {
			r.cleanup[id] = map[string]bool{}
		}
		r.cleanup[id][name] = true
		r.mark(id, e)
	}
	r.mu.Unlock()
	if cleanup {
		ctx, cancel := context.WithTimeout(r.ctx, writeTimeout)
		defer cancel()
		_ = r.flushOne(ctx, id)
	}
}

// Advance is monotonic. The gateway calls it only after its recovery/pending
// gate allows the boundary to move.
func (r *Registry) Advance(id, message uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	if !ok {
		return
	}
	e = clone(e)
	if message > e.LastMessageID {
		e.LastMessageID = message
	}
	e.ActiveAt = time.Now().UTC()
	r.mark(id, e)
}

// Delete removes a thread's entry from the bucket and memory, unless the
// thread changed locally while the delete was in flight.
func (r *Registry) Delete(ctx context.Context, id uint64) error {
	unlock, err := r.lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	r.mu.Lock()
	rev, version := r.revisions[id], r.versions[id]
	r.writing[id]++
	r.mu.Unlock()
	err = r.KV.Delete(ctx, key(id), jetstream.LastRevision(rev))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writing[id]--
	if err == nil && r.versions[id] == version {
		delete(r.entries, id)
		delete(r.dirty, id)
		delete(r.cleanup, id)
	}
	r.settle(id)
	return err
}

// flushOne writes one dirty entry; it returns nil when the entry is clean.
func (r *Registry) flushOne(ctx context.Context, id uint64) error {
	unlock, err := r.lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()
	r.mu.Lock()
	e, ok := r.entries[id]
	dirty := r.dirty[id]
	version, cleanup := r.versions[id], copySet(r.cleanup[id])
	if !dirty {
		r.mu.Unlock()
		return nil
	}
	if !ok {
		delete(r.dirty, id)
		r.settle(id)
		r.mu.Unlock()
		return nil
	}
	r.writing[id]++
	r.mu.Unlock()
	written, rev, deleted, err := r.write(ctx, id, e, cleanup)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.writing[id]--
	if err != nil {
		return err
	}
	r.reconcile(id, version, written, rev, deleted, cleanup)
	r.settle(id)
	return nil
}

func (r *Registry) dirtyIDs() []uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]uint64, 0, len(r.dirty))
	for id := range r.dirty {
		ids = append(ids, id)
	}
	return ids
}

// Flush writes every dirty entry within ctx. Shutdown calls it with a budget
// before cancelling the registry, so writes made while serving reach the
// bucket when NATS is reachable; it returns how many entries stay unwritten.
func (r *Registry) Flush(ctx context.Context) int {
	for _, id := range r.dirtyIDs() {
		if ctx.Err() != nil {
			break
		}
		_ = r.flushOne(ctx, id)
	}
	return r.Pending()
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
			for _, id := range r.dirtyIDs() {
				ctx, cancel := context.WithTimeout(r.ctx, writeTimeout)
				_ = r.flushOne(ctx, id)
				cancel()
			}
			if maxAge > 0 && time.Since(sweep) >= time.Hour {
				sweep = time.Now()
				r.Sweep(maxAge)
			}
		}
	}
}

// Sweep deletes every clean entry whose latest touch or publish is older
// than maxAge. A delete, unlike an expiry, reaches every watcher.
func (r *Registry) Sweep(maxAge time.Duration) {
	r.mu.RLock()
	stale := []uint64{}
	for id, e := range r.entries {
		if !r.dirty[id] && time.Since(e.ActiveAt) > maxAge {
			stale = append(stale, id)
		}
	}
	r.mu.RUnlock()
	for _, id := range stale {
		ctx, cancel := context.WithTimeout(r.ctx, writeTimeout)
		_ = r.Delete(ctx, id)
		cancel()
	}
}
func (r *Registry) Pending() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.dirty) }
func (r *Registry) Close() {
	r.cancel()
	if r.watcher != nil {
		_ = r.watcher.Stop()
	}
	r.wg.Wait()
}
