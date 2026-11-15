package store

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

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
	ts, err := s.Threads(testSession())
	if err != nil || ts[0].After.Sequence != 29 {
		t.Fatalf("%+v %v", ts, err)
	}
	// A replacement stream can have a lower epoch and sequence. <> is required.
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
	path := filepath.Join(t.TempDir(), "deck.db")
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := Open(path)
			if err != nil {
				t.Error(err)
				return
			}
			defer s.Close()
			if err = s.Session(testSession()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
