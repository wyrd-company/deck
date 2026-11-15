package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/config"
	"github.com/wyrd-company/deck/internal/session"
	"github.com/wyrd-company/deck/internal/store"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const keyA = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
const keyB = "SHA256:BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"

var baseThread = &pb.Thread{Bot: "helper", ServerId: 1, ChannelId: 2, ThreadId: 10, Name: "Question", Url: "https://discord.com/channels/1/10"}

func person(id, seq uint64, text string) *pb.Activity {
	return &pb.Activity{Id: id, Time: timestamppb.New(time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)), Bot: "helper", ServerId: 1, ChannelId: 2, ThreadId: 10, Author: &pb.User{Id: 3, Username: "person"}, Sequence: seq, Kind: &pb.Activity_Message{Message: &pb.Message{Content: text}}}
}
func ownActivity(key, nonce string, seq uint64) *pb.Activity {
	a := person(seq+100, seq, "own")
	a.SenderKey = key
	a.Nonce = nonce
	return a
}
func touched(count uint32, own ...*pb.OwnMessage) *pb.ThreadCount {
	return &pb.ThreadCount{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_TOUCHED, Count: count, Own: own}
}

type fake struct {
	pb.UnimplementedDeckServiceServer
	post     func(context.Context, *pb.PostRequest) (*pb.PostResponse, error)
	reply    func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error)
	get      func(context.Context, *pb.GetThreadRequest) (*pb.GetThreadResponse, error)
	count    func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error)
	list     func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error)
	history  func(context.Context, *pb.ListHistoryRequest) (*pb.ListHistoryResponse, error)
	listen   func(*pb.ListenRequest, grpc.ServerStreamingServer[pb.ListenResponse]) error
	channels func(context.Context, *pb.ListChannelsRequest) (*pb.ListChannelsResponse, error)
}

func (f *fake) Post(c context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
	if f.post != nil {
		return f.post(c, r)
	}
	return &pb.PostResponse{Thread: baseThread, Message: ownActivity(keyA, r.Nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
}
func (f *fake) Reply(c context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
	if f.reply != nil {
		return f.reply(c, r)
	}
	return &pb.ReplyResponse{Thread: baseThread, Message: ownActivity(keyA, r.Nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
}
func (f *fake) GetThread(c context.Context, r *pb.GetThreadRequest) (*pb.GetThreadResponse, error) {
	if f.get != nil {
		return f.get(c, r)
	}
	return &pb.GetThreadResponse{Thread: baseThread}, nil
}
func (f *fake) CountUnread(c context.Context, r *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
	if f.count != nil {
		return f.count(c, r)
	}
	return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{touched(0)}}, nil
}
func (f *fake) ListUnread(c context.Context, r *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
	if f.list != nil {
		return f.list(c, r)
	}
	return &pb.ListUnreadResponse{Threads: []*pb.ThreadCount{touched(0)}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
}
func (f *fake) ListHistory(c context.Context, r *pb.ListHistoryRequest) (*pb.ListHistoryResponse, error) {
	if f.history != nil {
		return f.history(c, r)
	}
	return &pb.ListHistoryResponse{Activity: []*pb.Activity{person(11, 0, "history")}, More: true}, nil
}
func (f *fake) Listen(r *pb.ListenRequest, s grpc.ServerStreamingServer[pb.ListenResponse]) error {
	if f.listen != nil {
		return f.listen(r, s)
	}
	return s.Send(&pb.ListenResponse{Threads: []*pb.ThreadCount{touched(1)}, Activity: []*pb.Activity{person(12, 9, "hello")}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}})
}
func (f *fake) ListChannels(c context.Context, r *pb.ListChannelsRequest) (*pb.ListChannelsResponse, error) {
	if f.channels != nil {
		return f.channels(c, r)
	}
	return &pb.ListChannelsResponse{Channels: []*pb.Channel{{Bot: "helper", ServerId: 1, ServerName: "Team", ChannelId: 2, Name: "questions", Type: 15}, {Bot: "helper", ChannelId: 3, Name: "private", Type: 0}}}, nil
}

type fixture struct {
	t                    *testing.T
	fake                 *fake
	path, db, key, input string
	dialN                int
	doc                  map[string]any
	api                  pb.DeckServiceClient
}

func setup(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	f := &fixture{t: t, fake: &fake{}, path: filepath.Join(dir, "config.yml"), db: filepath.Join(dir, "deck.db"), key: keyA}
	raw, err := os.ReadFile("../../docs/specifications/assets/configuration/examples/client.yml")
	if err != nil {
		t.Fatal(err)
	}
	if err = yaml.Unmarshal(raw, &f.doc); err != nil {
		t.Fatal(err)
	}
	f.doc["store"] = map[string]any{"path": f.db}
	f.doc["session"] = map[string]any{"variables": []string{"DECK_TEST_SESSION", "DECK_TEST_OTHER"}}
	f.doc["channels"] = map[string]any{"default": "helper/2"}
	delete(f.doc, "default_persona")
	f.doc["aliases"] = map[string]any{"channels": map[string]string{"team": "helper/2"}, "users": map[string]string{"owner": "8"}, "roles": map[string]string{"editors": "9"}}
	f.save()
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterDeckServiceServer(server, f.fake)
	go server.Serve(lis)
	t.Cleanup(func() { server.Stop(); lis.Close() })
	conn, err := grpc.NewClient("passthrough:///fake", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	f.api = pb.NewDeckServiceClient(conn)
	t.Setenv("DECK_TEST_SESSION", "one")
	t.Setenv("DECK_TEST_OTHER", "")
	return f
}
func (f *fixture) save() {
	f.t.Helper()
	raw, err := yaml.Marshal(f.doc)
	if err != nil {
		f.t.Fatal(err)
	}
	if err = os.WriteFile(f.path, raw, 0600); err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) app(out, err io.Writer) App {
	return App{Stdout: out, Stderr: err, Stdin: strings.NewReader(f.input), Dial: func(context.Context, *config.Client) (Connection, error) {
		f.dialN++
		return Connection{API: f.api, Fingerprint: f.key}, nil
	}}
}
func (f *fixture) run(args ...string) (int, string, string) {
	f.t.Helper()
	var out, err bytes.Buffer
	code := f.app(&out, &err).Run(context.Background(), append([]string{"--config", f.path}, args...))
	return code, out.String(), err.String()
}
func (f *fixture) result(args ...string) map[string]any {
	f.t.Helper()
	code, out, err := f.run(append(args, "--json")...)
	if code != 0 {
		f.t.Fatalf("%v code %d out=%s err=%s", args, code, out, err)
	}
	var value map[string]any
	if e := json.Unmarshal([]byte(out), &value); e != nil {
		f.t.Fatal(e, out)
	}
	return value
}
func (f *fixture) open() *store.Store {
	f.t.Helper()
	s, err := store.Open(f.db)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { s.Close() })
	return s
}
func ss() session.Session {
	return session.Session{Identifiers: []session.Identifier{{Name: "DECK_TEST_SESSION", Value: "one"}}}
}
func (f *fixture) seed() {
	f.t.Helper()
	s := f.open()
	if err := s.Pending(ss(), keyA, "original", "post", 0); err != nil {
		f.t.Fatal(err)
	}
	if err := s.Sent(ss(), keyA, "original", "created", baseThread, 11, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
		f.t.Fatal(err)
	}
}
func assertUnread(t *testing.T, result map[string]any, total float64) {
	t.Helper()
	if result["unread"].(map[string]any)["total"] != total {
		t.Fatalf("unread %+v want %v", result, total)
	}
}

func TestPostReplyPreRecordAndCursor(t *testing.T) {
	f := setup(t)
	s := f.open()
	calls := 0
	f.fake.post = func(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		calls++
		if r.Channel.Bot != "helper" || r.Channel.ChannelId != 2 || r.Title != "Question" || len(r.Nonce) != 20 {
			t.Errorf("post %+v", r)
		}
		own, err := s.Own(ss(), f.key, r.Nonce)
		if err != nil || !own {
			t.Errorf("send not recorded before RPC: %v %v", own, err)
		}
		return &pb.PostResponse{Thread: baseThread, Message: ownActivity(f.key, r.Nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	code, out, errout := f.run("post", "--title", "Question", "--text", "hello")
	if code != 0 || out != "posted 10 in helper/2\nhttps://discord.com/channels/1/10\n" || errout != "unread: none\n" || calls != 1 {
		t.Fatalf("%d %q %q calls %d", code, out, errout, calls)
	}
	ts, err := s.Threads(ss())
	if err != nil || len(ts) != 1 || ts[0].After.Sequence != 5 {
		t.Fatalf("cursor %+v %v", ts, err)
	}
	f.fake.reply = func(ctx context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
		own, err := s.Own(ss(), f.key, r.Nonce)
		if err != nil || !own {
			t.Error("reply not pre-recorded")
		}
		if r.Thread.ThreadId != 10 || r.ReplyToMessageId != 11 {
			t.Errorf("reply %+v", r)
		}
		return &pb.ReplyResponse{Thread: baseThread, Message: ownActivity(f.key, r.Nonce, 15), Position: &pb.StreamPosition{Epoch: 100, Sequence: 13}}, nil
	}
	result := f.result("reply", "https://discord.com/channels/1/10/11", "--reply-to", "11", "--text", "reply")
	assertUnread(t, result, 0)
	if result["message"].(map[string]any)["id"] != "115" {
		t.Fatal(result)
	}
	ts, _ = s.Threads(ss())
	if ts[0].After.Sequence != 5 {
		t.Fatal("reply advanced existing cursor")
	}
}
func TestFirstReplyResponsePosition(t *testing.T) {
	f := setup(t)
	f.result("reply", "helper/10", "--text", "answer")
	ts, err := f.open().Threads(ss())
	if err != nil || len(ts) != 1 || ts[0].After.Epoch != 100 || ts[0].After.Sequence != 5 {
		t.Fatalf("%+v %v", ts, err)
	}
}
func TestUnknownSendNoRetryAndExplicitNonce(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Unknown, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			f := setup(t)
			calls := 0
			var nonce string
			f.fake.post = func(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
				calls++
				nonce = r.Nonce
				return nil, status.Error(code, "answer lost")
			}
			exit, out, errout := f.run("post", "--title", "Question", "--text", "hello")
			if exit != 4 || calls != 1 || !strings.Contains(errout, "outcome unknown") || !strings.Contains(errout, "--nonce "+nonce) || out != "" {
				t.Fatalf("%d %q %q calls=%d", exit, out, errout, calls)
			}
			var got string
			f.fake.post = func(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
				calls++
				got = r.Nonce
				return &pb.PostResponse{Thread: baseThread, Message: ownActivity(keyA, got, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
			}
			f.result("post", "--title", "Question", "--text", "hello", "--nonce", nonce)
			if calls != 2 || got != nonce {
				t.Fatal("explicit repeat", calls, got, nonce)
			}
		})
	}
}
func TestDeadlineAfterSendLeft(t *testing.T) {
	f := setup(t)
	f.doc["service"].(map[string]any)["timeout"] = "25ms"
	f.save()
	var calls atomic.Int32
	f.fake.post = func(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	code, _, err := f.run("post", "--title", "Q", "--text", "body")
	if code != 4 || calls.Load() != 1 || !strings.Contains(err, "outcome unknown") {
		t.Fatalf("%d %q %d", code, err, calls.Load())
	}
}
func TestRotationRenameAndSameNonce(t *testing.T) {
	for _, name := range []string{"rotation", "rename", "rotation-and-rename", "same-nonce", "duplicate-copies", "late-own"} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			f.seed()
			s := f.open()
			activities := []*pb.Activity{ownActivity(keyA, "original", 7)}
			own := []*pb.OwnMessage{{Id: 107, SenderKey: keyA, Nonce: "original"}}
			want := float64(0)
			if name == "rotation" || name == "rotation-and-rename" {
				f.key = keyB
			}
			if strings.Contains(name, "rename") {
				if err := s.Pending(ss(), f.key, "renamed", "reply", 10); err != nil {
					t.Fatal(err)
				}
				a := ownActivity(f.key, "renamed", 8)
				a.SentBy = "new-name"
				activities = append(activities, a)
				own = append(own, &pb.OwnMessage{Id: 108, SenderKey: f.key, Nonce: "renamed"})
			}
			if name == "same-nonce" {
				activities = append(activities, ownActivity(keyB, "original", 8))
				own = append(own, &pb.OwnMessage{Id: 108, SenderKey: keyB, Nonce: "original"})
				want = 1
			}
			if name == "duplicate-copies" {
				activities = append(activities, ownActivity(keyA, "original", 8))
				own = append(own, &pb.OwnMessage{Id: 107, SenderKey: keyA, Nonce: "original"})
			}
			if name == "late-own" {
				if err := s.Advance(ss(), []uint64{10}, &pb.StreamPosition{Epoch: 100, Sequence: 6}); err != nil {
					t.Fatal(err)
				}
			}
			state := touched(uint32(len(activities)), own...)
			f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
				return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{state}}, nil
			}
			f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
				return &pb.ListUnreadResponse{Activity: activities, Threads: []*pb.ThreadCount{state}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
			}
			result := f.result("inbox", "--all")
			assertUnread(t, result, want)
			result = f.result("read", "--keep-unread")
			if len(result["activity"].([]any)) != int(want) {
				t.Fatalf("read %+v", result)
			}
			activities[0].Sequence = 0
			raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(activities[0])
			if err != nil {
				t.Fatal(err)
			}
			f.input = string(raw)
			result = f.result("resolve", "--event", "-")
			if len(result["sessions"].([]any)) != 0 {
				t.Fatal("own event resolved", result)
			}
			if name == "same-nonce" {
				activities[1].Sequence = 0
				raw, _ = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(activities[1])
				f.input = string(raw)
				result = f.result("resolve", "--event", "-")
				if len(result["sessions"].([]any)) != 1 {
					t.Fatal("other fingerprint suppressed", result)
				}
			}
		})
	}
}
func TestReadThroughEmptyAndKeepUnread(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprint(keep), func(t *testing.T) {
			f := setup(t)
			f.seed()
			s := f.open()
			f.fake.list = func(ctx context.Context, r *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
				if r.Cursors[0].After.Sequence != 5 {
					t.Errorf("cursor %+v", r)
				}
				return &pb.ListUnreadResponse{Threads: []*pb.ThreadCount{{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_TOUCHED, HorizonPassed: true}}, Through: &pb.StreamPosition{Epoch: 50, Sequence: 1}}, nil
			}
			f.fake.count = func(ctx context.Context, r *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
				return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_TOUCHED, HorizonPassed: r.Cursors[0].After.Epoch != 50}}}, nil
			}
			args := []string{"read"}
			if keep {
				args = append(args, "--keep-unread")
			}
			result := f.result(args...)
			if len(result["activity"].([]any)) != 0 {
				t.Fatal(result)
			}
			ts, _ := s.Threads(ss())
			if keep && ts[0].After.Epoch != 100 || !keep && ts[0].After.Epoch != 50 {
				t.Fatal(ts)
			}
			horizon := result["unread"].(map[string]any)["threads"].([]any)[0].(map[string]any)["horizon_passed"]
			if keep && horizon != true || !keep && horizon != nil {
				t.Fatalf("horizon %v", horizon)
			}
		})
	}
}
func TestConcurrentSendSuppressedDuringListen(t *testing.T) {
	f := setup(t)
	f.seed()
	s := f.open()
	f.fake.listen = func(r *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
		if err := stream.Send(&pb.ListenResponse{Threads: []*pb.ThreadCount{touched(0)}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 5}}); err != nil {
			return err
		}
		if err := s.Pending(ss(), keyB, "concurrent", "reply", 10); err != nil {
			return err
		}
		if err := stream.Send(&pb.ListenResponse{Activity: []*pb.Activity{ownActivity(keyB, "concurrent", 8)}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 8}}); err != nil {
			return err
		}
		return stream.Send(&pb.ListenResponse{Activity: []*pb.Activity{person(12, 9, "hello")}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}})
	}
	result := f.result("listen", "--timeout", "1s")
	items := result["activity"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != "12" {
		t.Fatal(result)
	}
	ts, _ := s.Threads(ss())
	if ts[0].After.Sequence != 9 {
		t.Fatal(ts)
	}
}
func TestListenKeepTimeoutInterruptAndRefused(t *testing.T) {
	for _, name := range []string{"keep", "timeout", "interrupt", "refused", "eof"} {
		t.Run(name, func(t *testing.T) {
			f := setup(t)
			f.seed()
			switch name {
			case "timeout", "interrupt":
				f.fake.listen = func(r *pb.ListenRequest, s grpc.ServerStreamingServer[pb.ListenResponse]) error {
					if err := s.Send(&pb.ListenResponse{Threads: []*pb.ThreadCount{touched(0)}}); err != nil {
						return err
					}
					<-s.Context().Done()
					return s.Context().Err()
				}
			case "refused":
				f.fake.listen = func(r *pb.ListenRequest, s grpc.ServerStreamingServer[pb.ListenResponse]) error {
					return s.Send(&pb.ListenResponse{Threads: []*pb.ThreadCount{{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_NOT_TOUCHED}}})
				}
			case "eof":
				f.fake.listen = func(r *pb.ListenRequest, s grpc.ServerStreamingServer[pb.ListenResponse]) error { return nil }
			}
			args := []string{"listen"}
			want := 0
			if name == "keep" {
				args = append(args, "--keep-unread")
			}
			if name == "timeout" {
				args = append(args, "--timeout", "25ms")
				want = 5
			}
			if name == "refused" {
				want = 3
			}
			if name == "eof" {
				want = 4
			}
			var code int
			var out, errout string
			if name == "interrupt" {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(25*time.Millisecond, cancel)
				var outbuf, errbuf bytes.Buffer
				code = f.app(&outbuf, &errbuf).Run(ctx, append([]string{"--config", f.path}, args...))
				out, errout = outbuf.String(), errbuf.String()
				want = 130
			} else {
				code, out, errout = f.run(args...)
			}
			if code != want {
				t.Fatalf("%s: code %d output %s err %s", name, code, out, errout)
			}
			ts, _ := f.open().Threads(ss())
			if ts[0].After.Sequence != 5 {
				t.Fatalf("unexpected cursor %+v", ts)
			}
		})
	}
}
func TestAllowDenyBeforeCalls(t *testing.T) {
	for _, command := range []string{"post", "reply", "history", "read", "listen", "inbox"} {
		t.Run(command, func(t *testing.T) {
			f := setup(t)
			f.seed()
			f.doc["channels"] = map[string]any{"default": "team", "allow": []string{"2"}, "deny": []string{"helper/2"}}
			f.save()
			args := map[string][]string{"post": {"post", "--title", "Q", "--text", "hello"}, "reply": {"reply", "10", "--text", "hello"}, "history": {"history", "10"}, "read": {"read", "10"}, "listen": {"listen", "10"}, "inbox": {"inbox"}}[command]
			code, _, err := f.run(args...)
			want := 3
			if command == "inbox" {
				want = 0
			}
			if code != want || f.dialN != 0 {
				t.Fatalf("%d %s dials %d", code, err, f.dialN)
			}
		})
	}
}
func TestUnknownThreadParentCheckedBeforeReplyHistory(t *testing.T) {
	for _, command := range []string{"reply", "history"} {
		t.Run(command, func(t *testing.T) {
			f := setup(t)
			f.doc["channels"] = map[string]any{"deny": []string{"2"}}
			f.save()
			calls := 0
			f.fake.get = func(context.Context, *pb.GetThreadRequest) (*pb.GetThreadResponse, error) {
				calls++
				return &pb.GetThreadResponse{Thread: baseThread}, nil
			}
			f.fake.reply = func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error) {
				t.Error("reply reached service")
				return nil, errors.New("unexpected")
			}
			f.fake.history = func(context.Context, *pb.ListHistoryRequest) (*pb.ListHistoryResponse, error) {
				t.Error("history reached service")
				return nil, errors.New("unexpected")
			}
			args := []string{command, "10"}
			if command == "reply" {
				args = append(args, "--text", "hello")
			}
			code, _, _ := f.run(args...)
			if code != 3 || calls != 1 {
				t.Fatalf("code %d metadata %d", code, calls)
			}
		})
	}
}
func TestHistoryChannelsSkillResolveAndText(t *testing.T) {
	f := setup(t)
	f.seed()
	result := f.result("history", "10")
	if result["more"] != true || len(result["activity"].([]any)) != 1 {
		t.Fatal(result)
	}
	ts, _ := f.open().Threads(ss())
	if ts[0].After.Sequence != 5 {
		t.Fatal("history marked read")
	}
	f.doc["channels"] = map[string]any{"allow": []string{"team"}}
	f.save()
	result = f.result("channels")
	if len(result["channels"].([]any)) != 1 {
		t.Fatal(result)
	}
	code, out, err := f.run("channels")
	if code != 0 || out != "helper/2 15 Team / questions [team]\n" || err != "unread: none\n" {
		t.Fatalf("%d %q %q", code, out, err)
	}
	result = f.result("skill")
	if !strings.HasPrefix(result["skill"].(string), "---\nname: deck\n") {
		t.Fatal(result)
	}
	before := f.dialN
	result = f.result("resolve", "10")
	if len(result["sessions"].([]any)) != 1 || result["unread"] != nil || f.dialN != before {
		t.Fatal(result)
	}
	code, out, err = f.run("resolve", "10")
	if code != 0 || out != "DECK_TEST_SESSION=one\n" || err != "" {
		t.Fatalf("%d %q %q", code, out, err)
	}
	code, out, err = f.run("read", "--keep-unread")
	if code != 0 || out != "" || err != "unread: none\n" {
		t.Fatalf("%d %q %q", code, out, err)
	}
}
func TestSessionOverrideAndDefault(t *testing.T) {
	f := setup(t)
	t.Setenv("DECK_TEST_OTHER", "secondary")
	f.result("post", "--title", "Q", "--text", "hello")
	result := f.result("resolve", "10")
	identifiers := result["sessions"].([]any)[0].(map[string]any)["identifiers"].([]any)
	if len(identifiers) != 2 || identifiers[0].(map[string]any)["name"] != "DECK_TEST_SESSION" {
		t.Fatal(result)
	}
	f.result("reply", "10", "--text", "hello", "--session", "manual=value")
	result = f.result("resolve", "10")
	if len(result["sessions"].([]any)) != 2 || result["sessions"].([]any)[0].(map[string]any)["primary"] != "manual" {
		t.Fatal(result)
	}
	t.Setenv("DECK_TEST_SESSION", "")
	t.Setenv("DECK_TEST_OTHER", "")
	f.result("reply", "10", "--text", "hello")
	result = f.result("resolve", "10")
	if len(result["sessions"].([]any)) != 2 {
		t.Fatal("default session exposed", result)
	}
}
func TestUnknownUnreadPreservesResult(t *testing.T) {
	f := setup(t)
	f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
		return nil, status.Error(codes.Unavailable, "index offline")
	}
	result := f.result("post", "--title", "Q", "--text", "hello")
	if result["thread"] == nil || !strings.Contains(result["unread"].(map[string]any)["error"].(string), "index offline") {
		t.Fatal(result)
	}
	code, _, err := f.run("skill")
	if code != 0 || !strings.Contains(err, "unread: unknown") {
		t.Fatal(code, err)
	}
}
func TestStatesHorizonAndTextActivity(t *testing.T) {
	f := setup(t)
	f.seed()
	f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
		return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_NOT_TOUCHED}}}, nil
	}
	code, out, err := f.run("inbox")
	if code != 0 || !strings.HasPrefix(out, "THREAD_STATE_NOT_TOUCHED 10") || err != "unread: none\n" {
		t.Fatal(code, out, err)
	}
	f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
		return &pb.ListUnreadResponse{Activity: []*pb.Activity{person(12, 9, "hello\nworld")}, Threads: []*pb.ThreadCount{touched(1)}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
	}
	f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
		c := touched(3)
		c.HorizonPassed = true
		return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{c}}, nil
	}
	code, out, err = f.run("read", "--keep-unread")
	if code != 0 || out != "## Question (10)\n2026-11-15T12:00:00Z person message 12\n  hello\n  world\n" || err != "unread: 3 in 1 threads (deck read) (older activity may have expired; deck history 10)\n" {
		t.Fatalf("%d %q %q", code, out, err)
	}
}
func TestInvalidCommandsAndEvents(t *testing.T) {
	f := setup(t)
	for _, args := range [][]string{{"post"}, {"reply"}, {"read"}, {"listen"}, {"read", "999"}, {"history", "10", "--after", "1", "--before", "2"}, {"post", "--title", "Q", "--text", "x", "--text-file", "-"}, {"post", "--title", "Q", "--text", "x", "--nonce", "!"}, {"post", "--title", "Q", "--text", "x", "--persona", "missing"}, {"post", "--title", "Q", "--poll", "Q"}, {"history", "10", "--limit", "101"}, {"read", "--limit", "0"}, {"listen", "--timeout", "oops"}, {"reply", "10", "--text", "x", "--reply-to", "oops"}, {"channels", "--unknown"}, {"resolve", "--event", "-"}} {
		f.input = `{"thread_id":"10"}`
		code, _, err := f.run(args...)
		if code != 2 {
			t.Fatalf("%v code %d %s", args, code, err)
		}
	}
	if f.dialN != 0 {
		t.Fatalf("invalid called service %d", f.dialN)
	}
}
func TestContentFlagsAndPollError(t *testing.T) {
	f := setup(t)
	dir := t.TempDir()
	embed := filepath.Join(dir, "embeds.json")
	components := filepath.Join(dir, "components.json")
	file := filepath.Join(dir, "note.txt")
	os.WriteFile(embed, []byte(`[{"title":"Title"},{"author":{"name":"Author"}}]`), 0600)
	os.WriteFile(components, []byte(`[{"type":1,"components":[]}]`), 0600)
	os.WriteFile(file, []byte("attachment"), 0600)
	f.doc["personas"] = map[string]any{"writer": map[string]any{"name": "Writer", "icon_url": "https://example.com/icon.png"}}
	f.doc["default_persona"] = "writer"
	f.save()
	f.input = "hello"
	f.fake.post = func(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		c := r.Content
		if c.Text != "hello" || len(c.Embeds) != 2 || len(c.Components) != 1 || !reflect.DeepEqual(c.MentionUserIds, []uint64{8}) || !reflect.DeepEqual(c.MentionRoleIds, []uint64{9}) || c.Poll.DurationHours != 2 || !c.Poll.AllowMultiselect || c.Persona.Name != "Writer" || string(c.Files[0].Data) != "attachment" || c.Files[0].Name != "note.txt" || len(r.Tags) != 1 {
			t.Errorf("content %+v request %+v", c, r)
		}
		return &pb.PostResponse{Thread: baseThread, Message: ownActivity(keyA, r.Nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}, PollError: "poll refused"}, nil
	}
	result := f.result("post", "team", "--title", "Q", "--text-file", "-", "--mention", "owner", "--mention-role", "editors", "--embed", embed, "--components", components, "--file", file, "--poll", "When?", "--answer", "Now", "--poll-duration", "61m", "--poll-multiselect", "--tag", "answer")
	if result["poll_error"] != "poll refused" {
		t.Fatal(result)
	}
}
func TestSkillWithoutConfigAndHelp(t *testing.T) {
	var out, err bytes.Buffer
	a := App{Stdout: &out, Stderr: &err}
	code := a.Run(context.Background(), []string{"skill", "--help"})
	if code != 0 || !strings.Contains(out.String(), "Example: deck skill") || err.Len() != 0 {
		t.Fatal(code, out.String(), err.String())
	}
	f := setup(t)
	code, _, _ = f.run("skill", "--config", filepath.Join(t.TempDir(), "missing"))
	if code != 2 {
		t.Fatal(code)
	}
}
func TestErrorMapping(t *testing.T) {
	cases := map[codes.Code]int{codes.Internal: 1, codes.Unauthenticated: 1, codes.Unknown: 1, codes.InvalidArgument: 2, codes.PermissionDenied: 3, codes.NotFound: 3, codes.FailedPrecondition: 3, codes.Unavailable: 4, codes.ResourceExhausted: 4, codes.DeadlineExceeded: 4, codes.Canceled: 130}
	for c, want := range cases {
		if got := exitCode(status.Error(c, "failure")); got != want {
			t.Errorf("%s=%d want %d", c, got, want)
		}
	}
	if exitCode(context.Canceled) != 130 || exitCode(context.DeadlineExceeded) != 4 {
		t.Fatal("context mapping")
	}
}

func TestRefusedSendAndUnreadStatus(t *testing.T) {
	for _, c := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition, codes.Unauthenticated} {
		t.Run(c.String(), func(t *testing.T) {
			f := setup(t)
			f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) {
				return nil, status.Error(c, "request refused")
			}
			code, out, err := f.run("post", "--title", "Q", "--text", "body", "--json")
			if code != exitCode(status.Error(c, "refused")) || strings.Contains(out, "outcome unknown") || err != "" {
				t.Fatalf("%d %s %s", code, out, err)
			}
			var result map[string]any
			if e := json.Unmarshal([]byte(out), &result); e != nil || result["unread"] == nil || result["error"].(map[string]any)["category"] != category(code) {
				t.Fatal(result, e)
			}
		})
	}
}
func TestIncompleteSendAndStoreResultFailure(t *testing.T) {
	for _, which := range []string{"incomplete", "store"} {
		t.Run(which, func(t *testing.T) {
			f := setup(t)
			f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
				result := &pb.PostResponse{Thread: baseThread, Message: ownActivity(keyA, r.Nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}
				if which == "incomplete" {
					result.Position = nil
				} else {
					result.Position.Epoch = 1 << 63
				}
				return result, nil
			}
			code, out, err := f.run("post", "--title", "Q", "--text", "hello")
			if which == "incomplete" {
				if code != 4 || !strings.Contains(err, "outcome unknown") {
					t.Fatal(code, out, err)
				}
			} else {
				if code != 1 || !strings.Contains(out, "posted 10") || !strings.Contains(err, "message sent, but could not record local store result") {
					t.Fatal(code, out, err)
				}
			}
		})
	}
}
func TestOfflineResolveMissingStoreAndInvalidEvent(t *testing.T) {
	f := setup(t)
	result := f.result("resolve", "10")
	if len(result["sessions"].([]any)) != 0 || f.dialN != 0 {
		t.Fatal(result)
	}
	if _, err := os.Stat(f.db); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("resolve created store", err)
	}
	for _, payload := range []string{`{}`, `{"thread_id":"10"}`, `{"id":"1","time":"2026-11-15T12:00:00Z","bot":"helper","server_id":"1","channel_id":"2","thread_id":"10","component":{"custom_id":"oops"}}`, `{"id":1,"time":"2026-11-15T12:00:00Z","bot":"helper","server_id":"1","channel_id":"2","thread_id":"10","message":{}}`} {
		f.input = payload
		code, _, err := f.run("resolve", "--event", "-")
		if code != 2 || !strings.Contains(err, "invalid event") {
			t.Fatal(code, err)
		}
	}
}
func TestResolvePendingSuppressesExistingSession(t *testing.T) {
	f := setup(t)
	f.seed()
	f.fake.reply = func(_ context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
		event := ownActivity(keyA, r.Nonce, 0)
		raw, _ := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(event)
		f.input = string(raw)
		result := f.result("resolve", "--event", "-")
		if len(result["sessions"].([]any)) != 0 {
			t.Error("pending own event wakes session", result)
		}
		return &pb.ReplyResponse{Thread: baseThread, Message: event, Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	f.result("reply", "10", "--text", "pending")
}
func TestReadFirstSendInterleaving(t *testing.T) {
	f := setup(t)
	nonce := ""
	f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		nonce = r.Nonce
		return &pb.PostResponse{Thread: baseThread, Message: ownActivity(keyA, nonce, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	f.result("post", "--title", "Q", "--text", "hello")
	f.fake.list = func(_ context.Context, r *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
		if r.Cursors[0].After.Sequence != 5 {
			t.Errorf("started too late: %v", r)
		}
		return &pb.ListUnreadResponse{Activity: []*pb.Activity{person(12, 6, "reply before starter publication"), ownActivity(keyA, nonce, 7)}, Threads: []*pb.ThreadCount{touched(2)}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 8}}, nil
	}
	result := f.result("read")
	if len(result["activity"].([]any)) != 1 || result["activity"].([]any)[0].(map[string]any)["id"] != "12" {
		t.Fatal(result)
	}
}
func TestNoSigningFingerprintNoSend(t *testing.T) {
	f := setup(t)
	f.key = ""
	f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) {
		t.Error("sent without fingerprint")
		return nil, nil
	}
	code, _, err := f.run("post", "--title", "Q", "--text", "body")
	if code != 1 || !strings.Contains(err, "no signing-key fingerprint") {
		t.Fatal(code, err)
	}
}
func TestContentInvalidBeforeService(t *testing.T) {
	f := setup(t)
	for _, args := range [][]string{{"post", "--title", "Q", "--text-file", "-", "--embed", "-"}, {"post", "--title", "Q", "--text", "body", "--persona", "missing"}, {"post", "--title", "Q", "--text", "body", "--persona", "x", "--no-persona"}, {"post", "--title", "Q", "--text", "body", "--poll-duration", "2h"}, {"post", "--title", "Q", "--poll", "Q", "--answer", "A", "--poll-duration", "769h"}, {"post", "--title", "Q", "--embed", "-"}, {"post", "--title", "Q", "--components", "-"}, {"post", "--title", "Q", "--text", "body", "--session", ""}, {"post", "--title", "Q", "--text", "body", "--mention", "missing"}} {
		f.input = `"not an object"`
		code, _, err := f.run(args...)
		if code != 2 {
			t.Fatalf("%v: %d %s", args, code, err)
		}
	}
	if f.dialN != 0 {
		t.Fatal(f.dialN)
	}
}
func TestReadLeavesUntouchedCursor(t *testing.T) {
	f := setup(t)
	f.seed()
	f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
		return &pb.ListUnreadResponse{Threads: []*pb.ThreadCount{{ThreadId: 10, State: pb.ThreadState_THREAD_STATE_NOT_GRANTED}}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 100}}, nil
	}
	code, out, err := f.run("read")
	if code != 0 || out != "" || !strings.Contains(err, "THREAD_STATE_NOT_GRANTED; leaving it out") {
		t.Fatal(code, out, err)
	}
	ts, _ := f.open().Threads(ss())
	if ts[0].After.Sequence != 5 {
		t.Fatal(ts)
	}
}
func TestOutputContainsNoConfigurationSecrets(t *testing.T) {
	f := setup(t)
	f.seed()
	secret := "do-not-print-token-123"
	f.doc["vault"].(map[string]any)["auth"] = map[string]any{"token_file": filepath.Join(t.TempDir(), secret)}
	f.save()
	t.Setenv("VAULT_TOKEN", secret)
	t.Setenv("DECK_UNCONFIGURED", secret)
	for _, args := range [][]string{{"channels"}, {"channels", "--json"}, {"inbox", "--json"}, {"resolve", "10", "--json"}, {"skill", "--json"}} {
		code, out, err := f.run(args...)
		if code != 0 || strings.Contains(out+err, secret) || strings.Contains(out+err, "PRIVATE KEY") || strings.Contains(out+err, "vault:v1:") {
			t.Fatal(code, out, err)
		}
	}
}

func TestDialFailureNotRepeatedForUnread(t *testing.T) {
	f := setup(t)
	f.seed()
	var out, err bytes.Buffer
	a := f.app(&out, &err)
	calls := 0
	a.Dial = func(context.Context, *config.Client) (Connection, error) {
		calls++
		return Connection{}, status.Error(codes.Unavailable, "connection failed")
	}
	code := a.Run(context.Background(), []string{"--config", f.path, "reply", "10", "--text", "hello"})
	if code != 4 || calls != 1 || !strings.Contains(err.String(), "unread: unknown") {
		t.Fatal(code, calls, err.String())
	}
}

func TestHelpFitsTerminal(t *testing.T) {
	for command := range options {
		for _, line := range strings.Split(help(flags{command: command}), "\n") {
			if len(line) > 80 {
				t.Errorf("%s help line %d columns: %s", command, len(line), line)
			}
		}
	}
}

func TestListenRemoteDeadlineBeforeLocalTimer(t *testing.T) {
	for _, hasTimeout := range []bool{true, false} {
		t.Run(fmt.Sprint(hasTimeout), func(t *testing.T) {
			f := setup(t)
			f.seed()
			f.fake.listen = func(_ *pb.ListenRequest, s grpc.ServerStreamingServer[pb.ListenResponse]) error {
				if s.Context().Err() != nil {
					t.Error("local timer already expired")
				}
				return status.Error(codes.DeadlineExceeded, "remote deadline arrived first")
			}
			args := []string{"listen"}
			want := 4
			if hasTimeout {
				args = append(args, "--timeout", "1h")
				want = 5
			}
			code, _, err := f.run(args...)
			if code != want {
				t.Fatalf("%d want %d: %s", code, want, err)
			}
		})
	}
}
func TestBotQualifiedDenyOnUnqualifiedPost(t *testing.T) {
	for _, target := range []string{"2", "bare-alias", "default"} {
		t.Run(target, func(t *testing.T) {
			f := setup(t)
			f.doc["channels"] = map[string]any{"default": "2", "deny": []string{"helper/2"}}
			f.doc["aliases"].(map[string]any)["channels"].(map[string]string)["bare-alias"] = "2"
			f.save()
			args := []string{"post"}
			if target != "default" {
				args = append(args, target)
			}
			args = append(args, "--title", "Q", "--text", "hello")
			code, _, err := f.run(args...)
			if code != 3 || f.dialN != 0 {
				t.Fatalf("%d dials %d: %s", code, f.dialN, err)
			}
		})
	}
}
func TestPresentEmptyAllowRefusesBeforeDial(t *testing.T) {
	f := setup(t)
	f.doc["channels"] = map[string]any{"default": "helper/2", "allow": []string{}}
	f.save()
	code, _, err := f.run("post", "--title", "Q", "--text", "hello")
	if code != 3 || f.dialN != 0 {
		t.Fatalf("%d dials %d: %s", code, f.dialN, err)
	}
}
