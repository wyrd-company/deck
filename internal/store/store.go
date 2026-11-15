/*
---
relationships:

  implements:
    - client
    - store-session-affinity-in-sqlite
    - keep-read-cursors-on-the-client

---
*/
// Package store owns the local SQLite session affinity, sends and read cursors.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/session"
	"modernc.org/sqlite"
)

type Store struct {
	db    *sql.DB
	empty bool
}
type Thread struct {
	Info      *pb.Thread
	After     *pb.StreamPosition
	Role      string
	TouchedAt string
}
type Resolved struct {
	Primary     string               `json:"primary"`
	Identifiers []session.Identifier `json:"identifiers"`
	TouchedAt   string               `json:"touched_at"`
}

const schema = `
CREATE TABLE sessions(key TEXT PRIMARY KEY, identifiers TEXT NOT NULL, seen_at TEXT NOT NULL);
CREATE TABLE threads(thread_id INTEGER PRIMARY KEY, bot TEXT NOT NULL, server_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, title TEXT NOT NULL);
CREATE TABLE session_threads(session_key TEXT NOT NULL REFERENCES sessions(key), thread_id INTEGER NOT NULL REFERENCES threads(thread_id), role TEXT NOT NULL CHECK(role IN ('created','replied')), cursor_epoch INTEGER NOT NULL, cursor_sequence INTEGER NOT NULL, touched_at TEXT NOT NULL, PRIMARY KEY(session_key,thread_id));
CREATE TABLE sends(key_fingerprint TEXT NOT NULL, nonce TEXT NOT NULL, session_key TEXT NOT NULL REFERENCES sessions(key), kind TEXT NOT NULL CHECK(kind IN ('post','reply')), thread_id INTEGER, message_id INTEGER, state TEXT NOT NULL CHECK(state IN ('pending','sent','failed','unknown')), sent_at TEXT NOT NULL, PRIMARY KEY(key_fingerprint,nonce));
CREATE INDEX sends_session_thread ON sends(session_key,thread_id);
CREATE INDEX sends_message ON sends(message_id);
PRAGMA user_version=1;`

// OpenReadOnly never creates or migrates the database, for offline relays.
func OpenReadOnly(path string) (*Store, error) { return open(path, true) }
func Open(path string) (*Store, error)         { return open(path, false) }
func open(path string, readOnly bool) (*Store, error) {
	if readOnly {
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
	}
	if !readOnly {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		if err = f.Chmod(0600); err != nil {
			f.Close()
			return nil, err
		}
		if err = f.Close(); err != nil {
			return nil, err
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: abs}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	if readOnly {
		q.Set("mode", "ro")
	}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	fail := func(e error) (*Store, error) { db.Close(); return nil, e }
	// An immediate write lock serializes migration with other processes opening
	// a brand new store. Ordinary transactions remain short and deferred.
	conn, err := db.Conn(context.Background())
	if err != nil {
		return fail(err)
	}
	defer conn.Close()
	if !readOnly {
		if err = enableWAL(conn); err != nil {
			return fail(err)
		}
		if _, err = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
			return fail(err)
		}
		defer conn.ExecContext(context.Background(), "ROLLBACK")
	}
	var version int
	if err = conn.QueryRowContext(context.Background(), "PRAGMA user_version").Scan(&version); err != nil {
		return fail(err)
	}
	if version > 1 {
		return fail(fmt.Errorf("store schema %d is newer than supported schema 1; upgrade deck", version))
	}
	if version < 1 {
		if readOnly {
			s.empty = true
			return s, nil
		}
		if _, err = conn.ExecContext(context.Background(), schema); err != nil {
			return fail(err)
		}
	}
	if !readOnly {
		if _, err = conn.ExecContext(context.Background(), "COMMIT"); err != nil {
			return fail(err)
		}
	}
	return s, nil
}

// Changing journal mode can return SQLITE_BUSY without invoking SQLite's busy
// handler. Keep it out of the connection pragmas so a concurrent creator can
// finish, and retry only that lock error within the same five-second bound.
func enableWAL(conn *sql.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var mode string
		err := conn.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode)
		if err == nil {
			if mode != "wal" {
				return fmt.Errorf("local store journal mode is %q, expected wal", mode)
			}
			return nil
		}
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) || sqliteErr.Code()&0xff != 5 {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("enable local store WAL: %w", ctx.Err())
		case <-timer.C:
		}
	}
}
func (s *Store) Close() error { return s.db.Close() }
func now() string             { return time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z") }
func upsertSession(tx *sql.Tx, ss session.Session) error {
	raw, err := json.Marshal(ss.Identifiers)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO sessions VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET identifiers=excluded.identifiers,seen_at=excluded.seen_at`, ss.Key(), string(raw), now())
	return err
}
func (s *Store) Session(ss session.Session) error {
	return s.write(func(tx *sql.Tx) error { return upsertSession(tx, ss) })
}
func (s *Store) write(fn func(*sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// ErrNonceConflict means an explicit repeat would change its session or target.
var ErrNonceConflict = errors.New("nonce already belongs to another session or target; use the original session and command")

// Pending is committed before the RPC, so relays can suppress an event even
// while its sending command is still waiting for a response. A nonce belongs
// to its original session and kind; repeats cannot steal another send.
func (s *Store) Pending(ss session.Session, key, nonce, kind string, thread uint64, channel ...*pb.ChannelRef) error {
	return s.write(func(tx *sql.Tx) error {
		if err := upsertSession(tx, ss); err != nil {
			return err
		}
		var owner, previousKind string
		var previousThread sql.NullInt64
		err := tx.QueryRow(`SELECT session_key,kind,thread_id FROM sends WHERE key_fingerprint=? AND nonce=?`, key, nonce).Scan(&owner, &previousKind, &previousThread)
		if err == nil {
			if owner != ss.Key() || kind != previousKind || (kind == "reply" && uint64(previousThread.Int64) != thread) {
				return ErrNonceConflict
			}
			// A completed post already identifies its original channel through
			// the stored thread. Refuse retargeting before another RPC can send.
			if kind == "post" && previousThread.Valid && len(channel) > 0 && channel[0] != nil {
				var bot string
				var id uint64
				if err := tx.QueryRow(`SELECT bot,channel_id FROM threads WHERE thread_id=?`, previousThread.Int64).Scan(&bot, &id); err != nil {
					return err
				}
				if channel[0].ChannelId != id || channel[0].Bot != "" && channel[0].Bot != bot {
					return ErrNonceConflict
				}
			}
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}
		var target any
		if thread != 0 {
			target = thread
		}
		_, err = tx.Exec(`INSERT INTO sends VALUES(?,?,?,?,?,NULL,'pending',?)`, key, nonce, ss.Key(), kind, target, now())
		return err
	})
}
func (s *Store) SendState(key, nonce, state string) error {
	_, err := s.db.Exec(`UPDATE sends SET state=? WHERE key_fingerprint=? AND nonce=? AND state <> 'sent'`, state, key, nonce)
	return err
}
func (s *Store) Sent(ss session.Session, key, nonce, role string, thread *pb.Thread, message uint64, position *pb.StreamPosition) error {
	if thread == nil || position == nil {
		return errors.New("service returned no thread or cursor position")
	}
	return s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO threads VALUES(?,?,?,?,?) ON CONFLICT(thread_id) DO UPDATE SET bot=excluded.bot,server_id=excluded.server_id,channel_id=excluded.channel_id,title=excluded.title`, thread.ThreadId, thread.Bot, thread.ServerId, thread.ChannelId, thread.Name); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO session_threads VALUES(?,?,?,?,?,?) ON CONFLICT(session_key,thread_id) DO UPDATE SET touched_at=excluded.touched_at`, ss.Key(), thread.ThreadId, role, position.Epoch, position.Sequence, now()); err != nil {
			return err
		}
		_, err := tx.Exec(`UPDATE sends SET state='sent',thread_id=?,message_id=? WHERE key_fingerprint=? AND nonce=? AND session_key=?`, thread.ThreadId, message, key, nonce, ss.Key())
		return err
	})
}
func (s *Store) Threads(ss session.Session) ([]Thread, error) {
	rows, err := s.db.Query(`SELECT t.thread_id,t.bot,t.server_id,t.channel_id,t.title,st.cursor_epoch,st.cursor_sequence,st.role,st.touched_at FROM session_threads st JOIN threads t USING(thread_id) WHERE session_key=? ORDER BY touched_at DESC,thread_id`, ss.Key())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Thread{}
	for rows.Next() {
		t := Thread{Info: &pb.Thread{}, After: &pb.StreamPosition{}}
		if err = rows.Scan(&t.Info.ThreadId, &t.Info.Bot, &t.Info.ServerId, &t.Info.ChannelId, &t.Info.Name, &t.After.Epoch, &t.After.Sequence, &t.Role, &t.TouchedAt); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}
func (s *Store) Thread(id uint64) (*pb.Thread, error) {
	t := &pb.Thread{}
	err := s.db.QueryRow(`SELECT thread_id,bot,server_id,channel_id,title FROM threads WHERE thread_id=?`, id).Scan(&t.ThreadId, &t.Bot, &t.ServerId, &t.ChannelId, &t.Name)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return t, err
}
func (s *Store) Own(ss session.Session, key, nonce string) (bool, error) {
	if key == "" || nonce == "" {
		return false, nil
	}
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM sends WHERE session_key=? AND key_fingerprint=? AND nonce=?`, ss.Key(), key, nonce).Scan(&n)
	return n > 0, err
}
func (s *Store) Advance(ss session.Session, ids []uint64, through *pb.StreamPosition) error {
	if through == nil {
		return errors.New("service returned no through position")
	}
	return s.write(func(tx *sql.Tx) error {
		for _, id := range ids {
			if _, err := tx.Exec(`UPDATE session_threads SET cursor_epoch=?,cursor_sequence=? WHERE session_key=? AND thread_id=? AND (cursor_epoch <> ? OR cursor_sequence < ?)`, through.Epoch, through.Sequence, ss.Key(), id, through.Epoch, through.Sequence); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) Resolve(thread uint64, event *pb.Activity) ([]Resolved, error) {
	if s.empty {
		return []Resolved{}, nil
	}
	key, nonce := "", ""
	if event != nil {
		key, nonce = event.SenderKey, event.Nonce
	}
	rows, err := s.db.Query(`SELECT s.identifiers,st.touched_at FROM session_threads st JOIN sessions s ON s.key=st.session_key WHERE st.thread_id=? AND NOT EXISTS(SELECT 1 FROM sends x WHERE x.session_key=s.key AND x.key_fingerprint=? AND x.nonce=?) ORDER BY st.touched_at DESC,s.key`, thread, key, nonce)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Resolved{}
	for rows.Next() {
		var raw string
		var r Resolved
		if err = rows.Scan(&raw, &r.TouchedAt); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &r.Identifiers); err != nil {
			return nil, err
		}
		if len(r.Identifiers) == 0 {
			continue
		}
		r.Primary = r.Identifiers[0].Name
		result = append(result, r)
	}
	return result, rows.Err()
}

// SendRecord returns durable outcome metadata without exposing the database.
// It also makes it possible to inspect a send whose command was interrupted.
type SendRecord struct {
	State               string
	ThreadID, MessageID uint64
}

func (s *Store) SendRecord(key, nonce string) (SendRecord, error) {
	var r SendRecord
	var thread, message sql.NullInt64
	err := s.db.QueryRow(`SELECT state,thread_id,message_id FROM sends WHERE key_fingerprint=? AND nonce=?`, key, nonce).Scan(&r.State, &thread, &message)
	r.ThreadID = uint64(thread.Int64)
	r.MessageID = uint64(message.Int64)
	return r, err
}
