package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
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
	"google.golang.org/protobuf/proto"
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
	for _, kind := range []string{"post", "reply"} {
		t.Run(kind, func(t *testing.T) {
			for _, grpcCode := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Unknown, codes.Internal} {
				t.Run(grpcCode.String(), func(t *testing.T) {
					f := setup(t)
					if kind == "reply" {
						f.seed()
					}
					s := f.open()
					calls := 0
					nonce := ""
					observe := func(n string) {
						calls++
						nonce = n
						record, err := s.SendRecord(keyA, n)
						if err != nil || record.State != "pending" && record.State != "unknown" {
							t.Errorf("before call: %+v %v", record, err)
						}
					}
					f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
						observe(r.Nonce)
						return nil, status.Error(grpcCode, "answer lost")
					}
					f.fake.reply = func(_ context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
						observe(r.Nonce)
						return nil, status.Error(grpcCode, "answer lost")
					}
					args := []string{kind}
					if kind == "post" {
						args = append(args, "--title", "Question")
					} else {
						args = append(args, "10")
					}
					args = append(args, "--text", "hello")
					code, out, errout := f.run(args...)
					if code != 4 || calls != 1 || !strings.Contains(errout, "outcome unknown") || !strings.Contains(errout, "--nonce "+nonce) || out != "" {
						t.Fatalf("%d %q %q calls=%d", code, out, errout, calls)
					}
					record, err := s.SendRecord(keyA, nonce)
					if err != nil || record.State != "unknown" {
						t.Fatalf("after unknown: %+v %v", record, err)
					}
					got := ""
					f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
						calls++
						got = r.Nonce
						return &pb.PostResponse{Thread: baseThread, Message: ownActivity(keyA, got, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
					}
					f.fake.reply = func(_ context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
						calls++
						got = r.Nonce
						return &pb.ReplyResponse{Thread: baseThread, Message: ownActivity(keyA, got, 7), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
					}
					repeat, ok := strings.CutPrefix(strings.Split(strings.Split(errout, "before repeating: ")[1], "\n")[0], "deck ")
					if !ok {
						t.Fatal("missing deck repeat", errout)
					}
					want := fmt.Sprintf("post --config='%s' --text='hello' --title='Question' --nonce %s", f.path, nonce)
					if kind == "reply" {
						want = fmt.Sprintf("reply '10' --config='%s' --text='hello' --nonce %s", f.path, nonce)
					}
					if repeat != want {
						t.Fatalf("repeat command %q want %q", repeat, want)
					}
					runPrintedRepeat(t, f, "deck "+repeat)
					if calls != 2 || got != nonce {
						t.Fatalf("explicit repeat: calls=%d got=%s want=%s", calls, got, nonce)
					}
					record, err = s.SendRecord(keyA, nonce)
					if err != nil || record.State != "sent" || record.ThreadID != 10 || record.MessageID != 107 {
						t.Fatalf("after success: %+v %v", record, err)
					}
				})
			}
		})
	}
}
func TestDeadlineAfterSendLeft(t *testing.T) {
	for _, kind := range []string{"post", "reply"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t)
			if kind == "reply" {
				f.seed()
			}
			f.doc["service"].(map[string]any)["timeout"] = "25ms"
			f.save()
			var calls atomic.Int32
			nonce := make(chan string, 4)
			wait := func(ctx context.Context, n string) error { calls.Add(1); nonce <- n; <-ctx.Done(); return ctx.Err() }
			f.fake.post = func(ctx context.Context, r *pb.PostRequest) (*pb.PostResponse, error) { return nil, wait(ctx, r.Nonce) }
			f.fake.reply = func(ctx context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
				return nil, wait(ctx, r.Nonce)
			}
			args := []string{kind}
			if kind == "post" {
				args = append(args, "--title", "Q")
			} else {
				args = append(args, "10")
			}
			args = append(args, "--text", "body")
			code, _, errout := f.run(args...)
			if code != 4 || calls.Load() != 1 || !strings.Contains(errout, "outcome unknown") {
				t.Fatalf("%d %q calls=%d", code, errout, calls.Load())
			}
			record, err := f.open().SendRecord(keyA, <-nonce)
			if err != nil || record.State != "unknown" {
				t.Fatalf("after deadline: %+v %v", record, err)
			}
		})
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
	if code != 0 || out != "helper/2 forum Team / questions [team]\n" || err != "unread: none\n" {
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
		if got := ExitCode(status.Error(c, "failure")); got != want {
			t.Errorf("%s=%d want %d", c, got, want)
		}
	}
	if ExitCode(context.Canceled) != 130 || ExitCode(context.DeadlineExceeded) != 4 {
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
			if code != ExitCode(status.Error(c, "refused")) || strings.Contains(out, "outcome unknown") || err != "" {
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
	for _, failureCode := range []codes.Code{codes.OK, codes.Unavailable, codes.PermissionDenied, codes.InvalidArgument, codes.Internal} {
		t.Run(failureCode.String(), func(t *testing.T) {
			f := setup(t)
			f.seed()
			token := "do-not-print-token-123"
			signature := "vault:v42:ZmFrZS1zaWduYXR1cmUtZm9yLXJlZGFjdGlvbi10ZXN0"
			tokenFile := filepath.Join(t.TempDir(), "vault-token")
			if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
				t.Fatal(err)
			}
			f.doc["vault"].(map[string]any)["auth"] = map[string]any{"token_file": tokenFile}
			f.save()
			t.Setenv("VAULT_TOKEN", token)
			t.Setenv("DECK_UNCONFIGURED", signature)
			if failureCode != codes.OK {
				fail := status.Error(failureCode, "service failed; inspect configuration")
				f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) { return nil, fail }
				f.fake.reply = func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error) { return nil, fail }
				f.fake.channels = func(context.Context, *pb.ListChannelsRequest) (*pb.ListChannelsResponse, error) { return nil, fail }
				f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) { return nil, fail }
				f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) { return nil, fail }
				f.fake.history = func(context.Context, *pb.ListHistoryRequest) (*pb.ListHistoryResponse, error) { return nil, fail }
				f.fake.listen = func(*pb.ListenRequest, grpc.ServerStreamingServer[pb.ListenResponse]) error { return fail }
			}
			for _, command := range [][]string{{"post", "--title", "Q", "--text", "hello"}, {"reply", "10", "--text", "hello"}, {"inbox"}, {"read", "--keep-unread"}, {"history", "10"}, {"listen", "--keep-unread", "--timeout", "1s"}, {"channels"}, {"resolve", "10"}, {"skill"}} {
				for _, jsonMode := range []bool{false, true} {
					args := append([]string{}, command...)
					if jsonMode {
						args = append(args, "--json")
					}
					code, out, errout := f.run(args...)
					want := 0
					if failureCode != codes.OK && command[0] != "resolve" && command[0] != "skill" {
						want = ExitCode(status.Error(failureCode, "failure"))
						if (command[0] == "post" || command[0] == "reply") && unknownSend(status.Error(failureCode, "failure")) {
							want = 4
						}
					}
					if code != want {
						t.Fatalf("%v code %d want %d: %s %s", args, code, want, out, errout)
					}
					for _, secret := range []string{token, signature, "vault:v42:", "PRIVATE KEY", "ZmFrZS1zaWduYXR1cmUtZm9yLXJlZGFjdGlvbi10ZXN0"} {
						if strings.Contains(out+errout, secret) {
							t.Fatalf("credential material reached %v output", args)
						}
					}
					if jsonMode {
						var value map[string]any
						if err := json.Unmarshal([]byte(out), &value); err != nil {
							t.Fatal(err, out)
						}
					}
				}
			}
			for _, args := range [][]string{{"post", "--title", "Q"}, {"reply", "10"}, {"read", "missing"}, {"listen", "--timeout", "bad"}, {"inbox", "--config", filepath.Join(t.TempDir(), "missing")}, {"resolve", "--event", "-"}} {
				f.input = `{}`
				code, out, errout := f.run(args...)
				if code != 2 || strings.Contains(out+errout, token) || strings.Contains(out+errout, signature) {
					t.Fatalf("invalid %v: %d %s %s", args, code, out, errout)
				}
			}
		})
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

func TestSameKeyDifferentSessionsSeeEachOthersSends(t *testing.T) {
	f := setup(t)
	f.seed()
	s := f.open()
	other := session.Session{Identifiers: []session.Identifier{{Name: "DECK_TEST_SESSION", Value: "two"}}}
	if err := s.Pending(other, keyA, "otherSession", "reply", 10); err != nil {
		t.Fatal(err)
	}
	if err := s.Sent(other, keyA, "otherSession", "replied", baseThread, 108, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
		t.Fatal(err)
	}
	activities := []*pb.Activity{ownActivity(keyA, "original", 7), ownActivity(keyA, "otherSession", 8)}
	state := touched(2, &pb.OwnMessage{Id: 107, SenderKey: keyA, Nonce: "original"}, &pb.OwnMessage{Id: 108, SenderKey: keyA, Nonce: "otherSession"})
	f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
		return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{state}}, nil
	}
	f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
		return &pb.ListUnreadResponse{Threads: []*pb.ThreadCount{state}, Activity: activities, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
	}
	f.fake.listen = func(_ *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
		return stream.Send(&pb.ListenResponse{Threads: []*pb.ThreadCount{state}, Activity: activities, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}})
	}
	for _, which := range []struct{ session, id string }{{"one", "108"}, {"two", "107"}} {
		t.Setenv("DECK_TEST_SESSION", which.session)
		assertUnread(t, f.result("inbox", "--all"), 1)
		for _, command := range []string{"read", "listen"} {
			result := f.result(command, "--keep-unread")
			items := result["activity"].([]any)
			if len(items) != 1 || items[0].(map[string]any)["id"] != which.id {
				t.Fatalf("%s/%s: %+v", which.session, command, result)
			}
		}
	}
	for _, a := range activities {
		a.Sequence = 0
		raw, _ := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(a)
		f.input = string(raw)
		result := f.result("resolve", "--event", "-")
		sessions := result["sessions"].([]any)
		if len(sessions) != 1 {
			t.Fatal(result)
		}
		want := "two"
		if a.Nonce == "otherSession" {
			want = "one"
		}
		if sessions[0].(map[string]any)["identifiers"].([]any)[0].(map[string]any)["value"] != want {
			t.Fatal(result)
		}
	}
}
func TestSendStatesAndFailedRepeat(t *testing.T) {
	for _, kind := range []string{"post", "reply"} {
		for _, outcome := range []string{"sent", "failed", "unknown", "store-failure"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				f := setup(t)
				if kind == "reply" {
					f.seed()
				}
				s := f.open()
				nonce := "StateProbe"
				observe := func() {
					record, err := s.SendRecord(keyA, nonce)
					if err != nil || record.State != "pending" {
						t.Errorf("pending: %+v %v", record, err)
					}
				}
				result := func() (*pb.Thread, *pb.Activity, *pb.StreamPosition, error) {
					observe()
					if outcome == "failed" {
						return nil, nil, nil, status.Error(codes.PermissionDenied, "refused")
					}
					if outcome == "unknown" {
						return nil, nil, nil, status.Error(codes.Unavailable, "lost answer")
					}
					position := &pb.StreamPosition{Epoch: 100, Sequence: 5}
					if outcome == "store-failure" {
						position.Epoch = 1 << 63
					}
					return baseThread, ownActivity(keyA, nonce, 7), position, nil
				}
				f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) {
					thread, a, p, e := result()
					return &pb.PostResponse{Thread: thread, Message: a, Position: p}, e
				}
				f.fake.reply = func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error) {
					thread, a, p, e := result()
					return &pb.ReplyResponse{Thread: thread, Message: a, Position: p}, e
				}
				args := []string{kind}
				if kind == "post" {
					args = append(args, "--title", "Q")
				} else {
					args = append(args, "10")
				}
				args = append(args, "--text", "hello", "--nonce", nonce)
				code, _, errout := f.run(args...)
				wantCode := map[string]int{"sent": 0, "failed": 3, "unknown": 4, "store-failure": 1}[outcome]
				record, err := s.SendRecord(keyA, nonce)
				wantState := outcome
				if outcome == "store-failure" {
					wantState = "pending"
				}
				if code != wantCode || err != nil || record.State != wantState {
					t.Fatalf("code %d record %+v err %v output %s", code, record, err, errout)
				}
				if outcome == "sent" {
					for _, c := range []codes.Code{codes.Unavailable, codes.PermissionDenied} {
						f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) {
							return nil, status.Error(c, "repeat failed")
						}
						f.fake.reply = func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error) {
							return nil, status.Error(c, "repeat failed")
						}
						f.run(args...)
						record, err = s.SendRecord(keyA, nonce)
						if err != nil || record.State != "sent" || record.MessageID != 107 {
							t.Fatalf("downgraded sent repeat: %+v %v", record, err)
						}
					}
				}
			})
		}
	}
}

func TestTextEscapesUntrustedControls(t *testing.T) {
	f := setup(t)
	f.seed()
	bad := "name\nforged\r\x1b[31m"
	thread := proto.Clone(baseThread).(*pb.Thread)
	thread.Name = bad
	if err := f.open().Sent(ss(), keyA, "original", "created", thread, 107, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
		t.Fatal(err)
	}
	f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
		return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{touched(1)}}, nil
	}
	a := person(12, 9, "first\nsecond\r\x1b[31m")
	a.Author.Username = bad
	a.GetMessage().Attachments = []*pb.Attachment{{Filename: bad, Url: "https://example.com/\rfile", Size: 1}}
	f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
		return &pb.ListUnreadResponse{Activity: []*pb.Activity{a}, Threads: []*pb.ThreadCount{touched(1)}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
	}
	for _, args := range [][]string{{"inbox"}, {"read", "--keep-unread"}} {
		code, out, err := f.run(args...)
		if code != 0 || strings.ContainsAny(out+err, "\r\x1b") || strings.Contains(out, "name\nforged") || !strings.Contains(out, `name\nforged\r\u001b[31m`) {
			t.Fatalf("%v: %d %q %q", args, code, out, err)
		}
	}
	code, out, err := f.run("read", "--keep-unread")
	if code != 0 || !strings.Contains(out, "  first\n  second\\r\\u001b[31m") {
		t.Fatal(code, out, err)
	}
	result := f.result("read", "--keep-unread")
	if result["activity"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"] != a.GetMessage().Content {
		t.Fatal("JSON content changed", result)
	}
}
func TestSessionConfigurationErrorsNameFileAndFix(t *testing.T) {
	f := setup(t)
	delete(f.doc, "key")
	f.save()
	for _, jsonMode := range []bool{false, true} {
		args := []string{"inbox"}
		if jsonMode {
			args = append(args, "--json")
		}
		code, out, err := f.run(args...)
		if code != 2 || !strings.Contains(out+err, f.path+": key: required field is missing") || !strings.Contains(out+err, "deck config validate --config ") {
			t.Fatal(code, out, err)
		}
	}
}

// Let a real POSIX shell interpret exactly the printed command, then run those
// arguments through the application rather than reconstructing a repeat ourselves.
func runPrintedRepeat(t *testing.T, f *fixture, command string) {
	t.Helper()
	raw, err := exec.Command("sh", "-c", `deck() { printf '%s\0' deck "$@"; }; `+command).Output()
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
	if len(args) == 0 || args[0] != "deck" {
		t.Fatalf("repeat argv %q", args)
	}
	var out, stderr bytes.Buffer
	if code := f.app(&out, &stderr).Run(context.Background(), args[1:]); code != 0 {
		t.Fatalf("repeat exit %d: %s %s", code, &out, &stderr)
	}
}

func TestNumericReferencesOverrideAliases(t *testing.T) {
	for _, target := range []string{"123", "helper/123", "default"} {
		t.Run("deny/"+target, func(t *testing.T) {
			f := setup(t)
			f.doc["aliases"] = map[string]any{"channels": map[string]string{"123": "helper/999"}}
			f.doc["channels"] = map[string]any{"default": "123", "deny": []string{"123"}}
			f.save()
			args := []string{"post", "--title", "Q", "--text", "hello"}
			if target != "default" {
				args = append(args, target)
			}
			code, _, stderr := f.run(args...)
			if code != 3 || f.dialN != 0 {
				t.Fatalf("deny bypass: %d dial=%d %s", code, f.dialN, stderr)
			}
		})
	}
	f := setup(t)
	f.doc["aliases"] = map[string]any{"channels": map[string]string{"123": "helper/999"}, "users": map[string]string{"123": "999", "owner": "8"}, "roles": map[string]string{"123": "999", "editors": "9"}}
	f.doc["channels"] = map[string]any{"allow": []string{"123"}}
	f.save()
	f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		if r.Channel.ChannelId != 123 || !reflect.DeepEqual(r.Content.MentionUserIds, []uint64{123, 8}) || !reflect.DeepEqual(r.Content.MentionRoleIds, []uint64{123, 9}) {
			t.Errorf("numeric reference redirected: %v", r)
		}
		return &pb.PostResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	f.result("post", "123", "--title", "Q", "--text", "hello", "--mention", "123", "--mention", "owner", "--mention-role", "123", "--mention-role", "editors")
	code, _, _ := f.run("post", "999", "--title", "Q", "--text", "no")
	if code != 3 {
		t.Fatal("numeric allow redirected", code)
	}
}

func TestRepeatCommandPreservesShellArguments(t *testing.T) {
	f := setup(t)
	text := "hello 'operator'\n$(echo wrong) `echo wrong`"
	f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		return nil, status.Error(codes.Unavailable, "lost")
	}
	code, out, stderr := f.run("post", "team", "--title", "A 'title'", "--text", text, "--nonce", "fixedNonce", "--json")
	if code != 4 {
		t.Fatal(code, out, stderr)
	}
	var result struct{ Error struct{ Message string } }
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatal(err, out)
	}
	_, repeat, ok := strings.Cut(result.Error.Message, "before repeating: ")
	if !ok {
		t.Fatal(result)
	}
	f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		if r.Nonce != "fixedNonce" || r.Channel.ChannelId != 2 || r.Title != "A 'title'" || r.Content.Text != text {
			t.Errorf("repeat changed request: %v", r)
		}
		return &pb.PostResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	runPrintedRepeat(t, f, repeat)
}

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }

func TestReadOutputBeforeCursorCommit(t *testing.T) {
	for _, command := range []string{"read", "listen"} {
		for _, jsonMode := range []bool{false, true} {
			for _, outcome := range []string{"success", "write error", "short write"} {
				t.Run(fmt.Sprintf("%s/json=%v/%s", command, jsonMode, outcome), func(t *testing.T) {
					f := setup(t)
					f.seed()
					s := f.open()
					f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
						return &pb.ListUnreadResponse{Threads: []*pb.ThreadCount{touched(1)}, Activity: []*pb.Activity{person(12, 9, "hello")}, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
					}
					assertOld := func() {
						t.Helper()
						ts, err := s.Threads(ss())
						if err != nil || ts[0].After.Sequence != 5 {
							t.Errorf("cursor moved before output completed: %v %v", ts, err)
						}
					}
					var output bytes.Buffer
					counted := false
					f.fake.count = func(_ context.Context, r *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
						assertOld()
						if output.Len() != 0 {
							t.Error("count should precede result output")
						}
						if r.Cursors[0].After.Sequence != 9 {
							t.Errorf("count did not use prospective cursor: %v", r)
						}
						counted = true
						return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{touched(0)}}, nil
					}
					var stderr bytes.Buffer
					out := writeFunc(func(p []byte) (int, error) {
						assertOld()
						if !counted {
							t.Error("missing unread report")
						}
						if outcome == "write error" {
							return 0, errors.New("broken output")
						}
						if outcome == "short write" {
							return len(p) / 2, nil
						}
						return output.Write(p)
					})
					args := []string{"--config", f.path, command}
					if jsonMode {
						args = append(args, "--json")
					}
					code := f.app(out, &stderr).Run(context.Background(), args)
					ts, err := s.Threads(ss())
					if err != nil {
						t.Fatal(err)
					}
					if outcome == "success" {
						if code != 0 || ts[0].After.Sequence != 9 || !strings.Contains(output.String(), "hello") {
							t.Fatalf("exit=%d after=%v out=%s err=%s", code, ts[0].After, &output, &stderr)
						}
						if jsonMode {
							var result map[string]any
							if err = json.Unmarshal(output.Bytes(), &result); err != nil {
								t.Fatal(err)
							}
							assertUnread(t, result, 0)
						}
					} else if code != 1 || ts[0].After.Sequence != 5 || !strings.Contains(stderr.String(), "write output") {
						t.Fatalf("failed write: exit=%d after=%v err=%s", code, ts[0].After, &stderr)
					}
				})
			}
		}
	}
}

func TestReadCommitFailureAfterOutput(t *testing.T) {
	for _, command := range []string{"read", "listen"} {
		for _, jsonMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", command, jsonMode), func(t *testing.T) {
				f := setup(t)
				f.seed()
				s := f.open()
				through := &pb.StreamPosition{Epoch: 1 << 63, Sequence: 9}
				f.fake.list = func(context.Context, *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
					return &pb.ListUnreadResponse{Threads: []*pb.ThreadCount{touched(1)}, Activity: []*pb.Activity{person(12, 9, "hello")}, Through: through}, nil
				}
				f.fake.listen = func(_ *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
					return stream.Send(&pb.ListenResponse{Threads: []*pb.ThreadCount{touched(1)}, Activity: []*pb.Activity{person(12, 9, "hello")}, Through: through})
				}
				args := []string{command}
				if jsonMode {
					args = append(args, "--json")
				}
				code, out, stderr := f.run(args...)
				if code != 1 || !strings.Contains(out, "hello") || !strings.Contains(stderr, "mark read in local store") {
					t.Fatal(code, out, stderr)
				}
				ts, err := s.Threads(ss())
				if err != nil || ts[0].After.Sequence != 5 {
					t.Fatalf("cursor after failed commit: %v %v", ts, err)
				}
				if jsonMode {
					var result map[string]any
					if err = json.Unmarshal([]byte(out), &result); err != nil {
						t.Fatal("expected single valid JSON object", err, out)
					}
				}
			})
		}
	}
}

// Fault injection only: production packages use Store rather than SQL directly.
func installStoreFailure(t *testing.T, path, trigger string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(trigger); err != nil {
		t.Fatal(err)
	}
}

func TestNonceConflictIsUsageError(t *testing.T) {
	for _, scenario := range []string{"session", "kind", "target", "store failure"} {
		for _, jsonMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%v", scenario, jsonMode), func(t *testing.T) {
				f := setup(t)
				f.result("reply", "10", "--text", "original", "--nonce", "owned")
				args := []string{"reply", "10", "--text", "repeat", "--nonce", "owned"}
				want := 2
				switch scenario {
				case "session":
					args = append(args, "--session", "DECK_TEST_SESSION=two")
				case "kind":
					args = []string{"post", "--title", "Q", "--text", "repeat", "--nonce", "owned"}
				case "target":
					args[1] = "11"
					f.fake.get = func(context.Context, *pb.GetThreadRequest) (*pb.GetThreadResponse, error) {
						th := proto.Clone(baseThread).(*pb.Thread)
						th.ThreadId = 11
						return &pb.GetThreadResponse{Thread: th}, nil
					}
				case "store failure":
					installStoreFailure(t, f.db, `CREATE TRIGGER fail_pending BEFORE INSERT ON sends BEGIN SELECT RAISE(ABORT, 'injected pending failure'); END`)
					args[len(args)-1] = "newNonce"
					want = 1
				}
				if jsonMode {
					args = append(args, "--json")
				}
				calls := 0
				f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) {
					calls++
					return nil, status.Error(codes.Internal, "should not call")
				}
				f.fake.reply = func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error) {
					calls++
					return nil, status.Error(codes.Internal, "should not call")
				}
				code, out, stderr := f.run(args...)
				if code != want || calls != 0 {
					t.Fatalf("exit=%d want=%d calls=%d out=%s err=%s", code, want, calls, out, stderr)
				}
				needle := "nonce already belongs"
				if want == 1 {
					needle = "record pending send"
				}
				if !strings.Contains(out+stderr, needle) {
					t.Fatal(code, out, stderr)
				}
			})
		}
	}
}

func TestIncompleteResultReportsOutcomeWriteFailure(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		t.Run(fmt.Sprint(jsonMode), func(t *testing.T) {
			f := setup(t)
			s := f.open()
			f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
				installStoreFailure(t, f.db, `CREATE TRIGGER fail_outcome BEFORE UPDATE OF state ON sends BEGIN SELECT RAISE(ABORT, 'injected outcome failure'); END`)
				return &pb.PostResponse{}, nil
			}
			args := []string{"post", "--title", "Q", "--text", "hello", "--nonce", "broken"}
			if jsonMode {
				args = append(args, "--json")
			}
			code, out, stderr := f.run(args...)
			if code != 4 || !strings.Contains(out+stderr, "outcome unknown") || !strings.Contains(out+stderr, "could not record send outcome") || !strings.Contains(out+stderr, "injected outcome failure") {
				t.Fatal(code, out, stderr)
			}
			record, err := s.SendRecord(keyA, "broken")
			if err != nil || record.State != "pending" {
				t.Fatalf("failed state update: %v %v", record, err)
			}
		})
	}
}

func TestTextRepeatPreservesControlsAndOptionValues(t *testing.T) {
	for _, kind := range []string{"post", "reply"} {
		for _, value := range []string{"line one\nline two\tcol\n\n", "-- signed off", "quotes ' $(printf bad) `echo bad`\r\x1b\u202e"} {
			t.Run(kind+"/"+fmt.Sprintf("%q", value), func(t *testing.T) {
				f := setup(t)
				if kind == "reply" {
					f.seed()
				}
				var original *pb.Content
				remember := func(c *pb.Content) { original = proto.Clone(c).(*pb.Content) }
				f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
					remember(r.Content)
					return nil, status.Error(codes.Unavailable, "lost")
				}
				f.fake.reply = func(_ context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
					remember(r.Content)
					return nil, status.Error(codes.Unavailable, "lost")
				}
				args := []string{kind}
				if kind == "post" {
					args = append(args, "team", "--title=--title")
				} else {
					args = append(args, "10")
				}
				args = append(args, "--text="+value, "--nonce=fixedNonce")
				code, _, stderr := f.run(args...)
				if code != 4 {
					t.Fatal(code, stderr)
				}
				_, repeat, ok := strings.Cut(stderr, "before repeating: ")
				if !ok {
					t.Fatal(stderr)
				}
				repeat = strings.TrimSuffix(repeat, "\n")
				if strings.ContainsAny(repeat, "\n\t\r\x1b\u202e") {
					t.Fatalf("repeat not printable single line: %q", repeat)
				}
				check := func(c *pb.Content, nonce string) {
					if !proto.Equal(c, original) || nonce != "fixedNonce" {
						t.Errorf("repeat changed content/nonce: %v %s", c, nonce)
					}
				}
				f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
					check(r.Content, r.Nonce)
					if r.Title != "--title" || r.Channel.ChannelId != 2 {
						t.Errorf("post target/title changed: %v", r)
					}
					return &pb.PostResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
				}
				f.fake.reply = func(_ context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
					check(r.Content, r.Nonce)
					if r.Thread.ThreadId != 10 {
						t.Errorf("reply target changed: %v", r)
					}
					return &pb.ReplyResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
				}
				runPrintedRepeat(t, f, repeat)
			})
		}
	}
}

func TestRepeatStdinReminder(t *testing.T) {
	for _, flag := range []string{"text-file", "file", "embed", "components"} {
		t.Run(flag, func(t *testing.T) {
			f := setup(t)
			f.input = "original stdin\n"
			if flag == "embed" {
				f.input = `{"title":"original"}`
			} else if flag == "components" {
				f.input = `[{"type":1}]`
			}
			var original *pb.Content
			f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
				original = proto.Clone(r.Content).(*pb.Content)
				return nil, status.Error(codes.Unavailable, "lost")
			}
			code, _, stderr := f.run("post", "team", "--title", "Q", "--"+flag, "-", "--nonce", "stdinNonce")
			if code != 4 || !strings.Contains(stderr, "supply the same input on stdin") {
				t.Fatal(code, stderr)
			}
			_, repeat, ok := strings.Cut(stderr, "before repeating: ")
			if !ok {
				t.Fatal(stderr)
			}
			f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
				if !proto.Equal(r.Content, original) || r.Nonce != "stdinNonce" {
					t.Errorf("stdin repeat changed: %v", r)
				}
				return &pb.PostResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
			}
			runPrintedRepeat(t, f, strings.TrimSuffix(repeat, "\n"))
		})
	}
}

func TestBareReadListenFilterChannels(t *testing.T) {
	for _, command := range []string{"read", "listen"} {
		for _, jsonMode := range []bool{false, true} {
			for _, none := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/json=%v/none=%v", command, jsonMode, none), func(t *testing.T) {
					f := setup(t)
					s := f.open()
					for n, id := range []uint64{10, 20, 30} {
						th := proto.Clone(baseThread).(*pb.Thread)
						th.ThreadId = id
						th.ChannelId = uint64(n + 2)
						nonce := fmt.Sprintf("seed%d", id)
						if err := s.Pending(ss(), keyA, nonce, "post", 0); err != nil {
							t.Fatal(err)
						}
						if err := s.Sent(ss(), keyA, nonce, "created", th, id+1, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
							t.Fatal(err)
						}
					}
					allow := []string{"2", "3"}
					if none {
						allow = []string{}
					}
					f.doc["channels"] = map[string]any{"allow": allow, "deny": []string{"3"}}
					f.save()
					check := func(c []*pb.Cursor) {
						if len(c) != 1 || c[0].ThreadId != 10 {
							t.Errorf("unfiltered cursors: %v", c)
						}
					}
					activity := []*pb.Activity{person(12, 9, "allowed content"), person(22, 9, "DENIED content"), person(32, 9, "EXCLUDED content")}
					activity[1].ThreadId = 20
					activity[2].ThreadId = 30
					states := []*pb.ThreadCount{touched(1), {ThreadId: 20, State: pb.ThreadState_THREAD_STATE_TOUCHED}, {ThreadId: 30, State: pb.ThreadState_THREAD_STATE_TOUCHED}}
					f.fake.list = func(_ context.Context, r *pb.ListUnreadRequest) (*pb.ListUnreadResponse, error) {
						check(r.Cursors)
						return &pb.ListUnreadResponse{Activity: activity, Threads: states, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}}, nil
					}
					f.fake.listen = func(r *pb.ListenRequest, stream grpc.ServerStreamingServer[pb.ListenResponse]) error {
						check(r.Cursors)
						return stream.Send(&pb.ListenResponse{Activity: activity, Threads: states, Through: &pb.StreamPosition{Epoch: 100, Sequence: 9}})
					}
					f.fake.count = func(_ context.Context, r *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
						check(r.Cursors)
						return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{touched(0)}}, nil
					}
					args := []string{command}
					if jsonMode {
						args = append(args, "--json")
					}
					code, out, stderr := f.run(args...)
					if code != 0 || strings.Contains(out, "DENIED") || strings.Contains(out, "EXCLUDED") || (!none && !strings.Contains(out, "allowed content")) {
						t.Fatal(code, out, stderr)
					}
					if none && f.dialN != 0 {
						t.Fatal("all filtered command dialed", f.dialN)
					}
					ts, err := s.Threads(ss())
					if err != nil {
						t.Fatal(err)
					}
					for _, th := range ts {
						want := uint64(5)
						if !none && th.Info.ThreadId == 10 {
							want = 9
						}
						if th.After.Sequence != want {
							t.Fatalf("filtered cursor moved: %v", th)
						}
					}
				})
			}
		}
	}
}

func TestInboxAllAndMostRecentSendOrder(t *testing.T) {
	f := setup(t)
	s := f.open()
	for _, id := range []uint64{10, 20, 30} {
		th := proto.Clone(baseThread).(*pb.Thread)
		th.ThreadId = id
		nonce := fmt.Sprintf("seed%d", id)
		if err := s.Pending(ss(), keyA, nonce, "post", 0); err != nil {
			t.Fatal(err)
		}
		if err := s.Sent(ss(), keyA, nonce, "created", th, id+1, &pb.StreamPosition{Epoch: 100, Sequence: 5}); err != nil {
			t.Fatal(err)
		}
	}
	f.fake.count = func(context.Context, *pb.CountUnreadRequest) (*pb.CountUnreadResponse, error) {
		return &pb.CountUnreadResponse{Threads: []*pb.ThreadCount{touched(1), {ThreadId: 20, State: pb.ThreadState_THREAD_STATE_TOUCHED, Count: 2}, {ThreadId: 30, State: pb.ThreadState_THREAD_STATE_TOUCHED}}}, nil
	}
	for _, all := range []bool{false, true} {
		args := []string{"inbox"}
		want := []string{"20", "10"}
		if all {
			args = append(args, "--all")
			want = []string{"30", "20", "10"}
		}
		result := f.result(args...)
		got := []string{}
		for _, row := range result["threads"].([]any) {
			got = append(got, row.(map[string]any)["thread_id"].(string))
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("all=%v threads=%v want=%v", all, got, want)
		}
		assertUnread(t, result, 3)
		code, out, stderr := f.run(args...)
		if code != 0 {
			t.Fatal(code, out, stderr)
		}
		got = []string{}
		for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				t.Fatal(out)
			}
			got = append(got, fields[1])
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("text all=%v threads=%v want=%v", all, got, want)
		}
	}
}

func TestNoPersonaOverridesDefault(t *testing.T) {
	for _, kind := range []string{"post", "reply"} {
		for _, choice := range []string{"default", "none", "explicit"} {
			t.Run(kind+"/"+choice, func(t *testing.T) {
				f := setup(t)
				f.doc["personas"] = map[string]any{"default": map[string]string{"name": "Default agent", "icon_url": "https://example.com/default.png"}, "alternate": map[string]string{"name": "Other agent", "icon_url": "https://example.com/other.png"}}
				f.doc["default_persona"] = "default"
				f.save()
				want := &pb.Persona{Name: "Default agent", IconUrl: "https://example.com/default.png"}
				if choice == "none" {
					want = nil
				} else if choice == "explicit" {
					want = &pb.Persona{Name: "Other agent", IconUrl: "https://example.com/other.png"}
				}
				check := func(c *pb.Content) {
					if !proto.Equal(c.Persona, want) {
						t.Errorf("persona=%v want=%v", c.Persona, want)
					}
				}
				f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
					check(r.Content)
					return &pb.PostResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
				}
				f.fake.reply = func(_ context.Context, r *pb.ReplyRequest) (*pb.ReplyResponse, error) {
					check(r.Content)
					return &pb.ReplyResponse{Thread: baseThread, Message: person(11, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
				}
				args := []string{kind}
				if kind == "post" {
					args = append(args, "--title", "Q")
				} else {
					args = append(args, "10")
				}
				args = append(args, "--text", "hello")
				if choice == "none" {
					args = append(args, "--no-persona")
				} else if choice == "explicit" {
					args = append(args, "--persona", "alternate")
				}
				f.result(args...)
			})
		}
	}
}

func TestCompletedPostNonceCannotChangeChannel(t *testing.T) {
	f := setup(t)
	s := f.open()
	f.result("post", "helper/2", "--title", "original", "--text", "hello", "--nonce", "boundPost")
	original, err := s.SendRecord(keyA, "boundPost")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	f.fake.post = func(_ context.Context, r *pb.PostRequest) (*pb.PostResponse, error) {
		calls++
		if r.Channel.ChannelId != 2 {
			t.Errorf("post retargeted: %v", r)
		}
		return &pb.PostResponse{Thread: baseThread, Message: person(107, 7, "sent"), Position: &pb.StreamPosition{Epoch: 100, Sequence: 5}}, nil
	}
	for _, target := range []string{"helper/3", "other/2"} {
		for _, jsonMode := range []bool{false, true} {
			args := []string{"post", target, "--title", "repeat", "--text", "hello", "--nonce", "boundPost"}
			if jsonMode {
				args = append(args, "--json")
			}
			code, out, stderr := f.run(args...)
			if code != 2 || calls != 0 {
				t.Fatal(code, calls, out, stderr)
			}
			got, err := s.SendRecord(keyA, "boundPost")
			if err != nil || got != original {
				t.Fatalf("retarget rewrote original: %v want %v %v", got, original, err)
			}
		}
	}
	f.result("post", "2", "--title", "repeat", "--text", "hello", "--nonce", "boundPost")
	if calls != 1 {
		t.Fatal("same channel repeat refused", calls)
	}
}
