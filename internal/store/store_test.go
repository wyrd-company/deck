package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/session"
)

func testSession() session.Session {
	return session.Session{Identifiers: []session.Identifier{{Name: "HARNESS", Value: "one"}}}
}
func seed(t *testing.T, s *Store) {
	t.Helper()
	ss := testSession()
	if err := s.Pending(ss, "A", "n", "post", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Sent(ss, "A", "n", "created", &pb.Thread{ThreadId: 10, Bot: "helper", ServerId: 1, ChannelId: 2, Name: "subject"}, 11, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
		t.Fatal(err)
	}
}
func TestPersistenceAndResolve(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "deck.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, s)
	if err = s.Pending(testSession(), "B", "pending", "reply", 10); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.Resolve(10, nil)
	if err != nil || len(rows) != 1 || rows[0].Primary != "HARNESS" {
		t.Fatalf("%+v %v", rows, err)
	}
	rows, err = s.Resolve(10, &pb.Activity{SenderKey: "B", Nonce: "pending"})
	if err != nil || len(rows) != 0 {
		t.Fatalf("pending not suppressed: %+v %v", rows, err)
	}
	for _, pair := range [][2]string{{"A", "n"}, {"B", "pending"}} {
		own, err := s.Own(testSession(), pair[0], pair[1])
		if err != nil || !own {
			t.Fatalf("%v %v", own, err)
		}
	}
	own, err := s.Own(testSession(), "other", "n")
	if err != nil || own {
		t.Fatalf("nonce alone matched: %v %v", own, err)
	}
	for _, p := range []string{path, filepath.Dir(path)} {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0600)
		if info.IsDir() {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Fatalf("permissions %s: %v", p, info.Mode())
		}
	}
}
func TestCursorEpochAndConcurrentReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deck.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seed(t, s)
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	var wg sync.WaitGroup
	for i := uint64(6); i < 30; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			db := s
			if seq%2 == 0 {
				db = s2
			}
			if err := db.Advance(testSession(), []uint64{10}, &pb.StreamPosition{Epoch: 100, Sequence: seq}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := s.Advance(testSession(), []uint64{10}, &pb.StreamPosition{Epoch: 100, Sequence: 6}); err != nil {
		t.Fatal(err)
	}
	ts, err := s.Threads(testSession())
	if err != nil || ts[0].After.Sequence != 29 {
		t.Fatalf("%+v %v", ts, err)
	}
	// A replacement stream can have either a higher or lower epoch.
	if err = s.Advance(testSession(), []uint64{10}, &pb.StreamPosition{Epoch: 200, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	ts, err = s.Threads(testSession())
	if err != nil || ts[0].After.Epoch != 200 || ts[0].After.Sequence != 1 {
		t.Fatalf("higher epoch cursor: %+v %v", ts, err)
	}
	// <> is required regardless of which epoch is larger.
	if err = s.Advance(testSession(), []uint64{10}, &pb.StreamPosition{Epoch: 50, Sequence: 1}); err != nil {
		t.Fatal(err)
	}
	ts, err = s.Threads(testSession())
	if err != nil || ts[0].After.Epoch != 50 || ts[0].After.Sequence != 1 {
		t.Fatalf("cursor: %+v %v", ts, err)
	}
	// Repeated send responses never reset an existing read cursor.
	if err = s.Sent(testSession(), "A", "n", "created", &pb.Thread{ThreadId: 10}, 11, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
		t.Fatal(err)
	}
	ts, _ = s.Threads(testSession())
	if ts[0].After.Epoch != 50 {
		t.Fatal("repeat reset cursor")
	}
}
func TestSchemaAndNonceOwnership(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "deck.db"))
	if err != nil {
		t.Fatal(err)
	}
	seed(t, s)
	other := session.Session{Identifiers: []session.Identifier{{Name: "HARNESS", Value: "other"}}}
	if err = s.Pending(other, "A", "n", "post", 0); err == nil {
		t.Fatal("stole nonce")
	}
	if _, err = s.db.Exec("PRAGMA user_version=2"); err != nil {
		t.Fatal(err)
	}
	path := ""
	s.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path)
	s.Close()
	if next, err := Open(path); err == nil {
		next.Close()
		t.Fatal("accepted future schema")
	}
}
func TestConcurrentCreation(t *testing.T) {
	for trial := range 50 {
		path := filepath.Join(t.TempDir(), "deck.db")
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				s, err := Open(path)
				if err != nil {
					t.Errorf("trial %d open: %v", trial, err)
					return
				}
				defer s.Close()
				if err = s.Session(testSession()); err != nil {
					t.Errorf("trial %d session: %v", trial, err)
				}
				var mode string
				var version int
				if err = s.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
					t.Errorf("mode %q: %v", mode, err)
				}
				if err = s.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 1 {
					t.Errorf("version %d: %v", version, err)
				}
			}()
		}
		close(start)
		wg.Wait()
		if t.Failed() {
			return
		}
	}
}

func TestOpenWaitsForCreationLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deck.db")
	blocker, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	conn, err := blocker.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	result := make(chan error, 1)
	go func() {
		s, err := Open(path)
		if err == nil {
			err = s.Session(testSession())
			s.Close()
		}
		result <- err
	}()
	select {
	case err = <-result:
		t.Fatalf("open returned before creator released lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err = conn.ExecContext(context.Background(), "COMMIT"); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal(fmt.Errorf("open did not complete after creator committed"))
	}
}

func TestResolveWhileStoreIsBeingCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deck.db")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := reader.Resolve(10, nil)
	if err != nil || len(rows) != 0 {
		t.Fatalf("initial store: %+v %v", rows, err)
	}
	reader.Close()
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, writer)
	writer.Close()
	reader, err = OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	rows, err = reader.Resolve(10, nil)
	if err != nil || len(rows) != 1 {
		t.Fatalf("migrated store: %+v %v", rows, err)
	}
}

func TestResolvePrimaryUsesFirstIdentifier(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "deck.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ss := session.Session{Identifiers: []session.Identifier{{Name: "FIRST", Value: "one"}, {Name: "SECOND", Value: "two"}}}
	if err = s.Pending(ss, "K", "n", "post", 0); err != nil {
		t.Fatal(err)
	}
	if err = s.Sent(ss, "K", "n", "created", &pb.Thread{ThreadId: 10}, 11, &pb.StreamPosition{Epoch: 1, Sequence: 5}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.Resolve(10, nil)
	if err != nil || len(rows) != 1 || rows[0].Primary != "FIRST" || len(rows[0].Identifiers) != 2 || rows[0].Identifiers[1].Name != "SECOND" {
		t.Fatalf("resolve primary/order: %v %v", rows, err)
	}
}

func TestEnableWALRejectsNonBusyError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(path, []byte("not a SQLite database"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Now()
	err = enableWAL(conn)
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("non-busy error retried: elapsed=%v err=%v", time.Since(start), err)
	}
}
