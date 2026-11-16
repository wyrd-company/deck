// The demo runs the real CLI against a synthetic API. It never opens a socket,
// reads a credential or contacts Discord. Keep it outside cmd/: it is not a
// product executable or an alternate authentication path.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"github.com/wyrd-company/deck/internal/cli"
	"github.com/wyrd-company/deck/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const threadID = 1571484634081529857
const fingerprint = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

var thread = &pb.Thread{Bot: "helper", ServerId: 1287000000000000001, ChannelId: 1287000000000000002, ThreadId: threadID, Name: "Release timing", Url: "https://discord.com/channels/1287000000000000001/1571484634081529857"}

// Embedding the client makes unused operations fail loudly if the tape grows
// beyond the implemented fixture. The CLI, parsing and SQLite store are real.
type fixture struct {
	pb.DeckServiceClient
	marker string
}

func position(sequence uint64) *pb.StreamPosition {
	return &pb.StreamPosition{Epoch: 100, Sequence: sequence}
}
func activity(id, sequence uint64, text, author string) *pb.Activity {
	return &pb.Activity{Id: id, Time: timestamppb.New(time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)), Bot: thread.Bot, ServerId: thread.ServerId, ChannelId: thread.ChannelId, ThreadId: threadID, Author: &pb.User{Id: 1100000000000000003, Username: author}, Sequence: sequence, Kind: &pb.Activity_Message{Message: &pb.Message{Content: text}}}
}
func incoming() *pb.Activity {
	return activity(1571485045016997890, 8, "Yes, ship it this afternoon.", "reviewer")
}
func state(count uint32) []*pb.ThreadCount {
	return []*pb.ThreadCount{{ThreadId: threadID, State: pb.ThreadState_THREAD_STATE_TOUCHED, Count: count}}
}
func (f *fixture) count(cursors []*pb.Cursor) uint32 {
	if _, err := os.Stat(f.marker); err != nil {
		return 0
	}
	for _, c := range cursors {
		if c.ThreadId == threadID && (c.After == nil || c.After.Epoch != 100 || c.After.Sequence < 8) {
			return 1
		}
	}
	return 0
}
func (f *fixture) Post(_ context.Context, r *pb.PostRequest, _ ...grpc.CallOption) (*pb.PostResponse, error) {
	message := activity(threadID, 7, r.Content.Text, "helper")
	message.SenderKey, message.Nonce = fingerprint, r.Nonce
	return &pb.PostResponse{Thread: thread, Message: message, Position: position(6)}, nil
}
func (f *fixture) Reply(_ context.Context, r *pb.ReplyRequest, _ ...grpc.CallOption) (*pb.ReplyResponse, error) {
	message := activity(1571485045016997892, 9, r.Content.Text, "helper")
	message.SenderKey, message.Nonce = fingerprint, r.Nonce
	return &pb.ReplyResponse{Thread: thread, Message: message, Position: position(8)}, nil
}
func (f *fixture) GetThread(context.Context, *pb.GetThreadRequest, ...grpc.CallOption) (*pb.GetThreadResponse, error) {
	return &pb.GetThreadResponse{Thread: thread}, nil
}
func (f *fixture) ListChannels(context.Context, *pb.ListChannelsRequest, ...grpc.CallOption) (*pb.ListChannelsResponse, error) {
	return &pb.ListChannelsResponse{Channels: []*pb.Channel{{Bot: thread.Bot, ServerId: thread.ServerId, ChannelId: thread.ChannelId, ServerName: "Demo server", Name: "questions", Type: 15}}}, nil
}
func (f *fixture) CountUnread(_ context.Context, r *pb.CountUnreadRequest, _ ...grpc.CallOption) (*pb.CountUnreadResponse, error) {
	return &pb.CountUnreadResponse{Threads: state(f.count(r.Cursors))}, nil
}
func (f *fixture) ListUnread(_ context.Context, r *pb.ListUnreadRequest, _ ...grpc.CallOption) (*pb.ListUnreadResponse, error) {
	count := f.count(r.Cursors)
	result := &pb.ListUnreadResponse{Threads: state(count), Through: position(8)}
	if count > 0 {
		result.Activity = []*pb.Activity{incoming()}
	}
	return result, nil
}

type responseStream struct {
	grpc.ClientStream
	response *pb.ListenResponse
}

func (s *responseStream) Recv() (*pb.ListenResponse, error) {
	if s.response == nil {
		return nil, io.EOF
	}
	r := s.response
	s.response = nil
	return r, nil
}
func (f *fixture) Listen(ctx context.Context, _ *pb.ListenRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ListenResponse], error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(500 * time.Millisecond):
	}
	if err := os.WriteFile(f.marker, []byte("synthetic reply received\n"), 0600); err != nil {
		return nil, err
	}
	return &responseStream{response: &pb.ListenResponse{Threads: state(1), Activity: []*pb.Activity{incoming()}, Through: position(8)}}, nil
}

func main() {
	path := os.Getenv("DECK_DEMO_CONFIG")
	if path == "" {
		fmt.Fprintln(os.Stderr, "run task demo or task demo:check")
		os.Exit(1)
	}
	api := &fixture{marker: filepath.Join(filepath.Dir(path), "incoming")}
	app := cli.App{Dial: func(context.Context, *config.Client) (cli.Connection, error) {
		return cli.Connection{API: api, Fingerprint: fingerprint}, nil
	}}
	args := append([]string{"--config", path}, os.Args[1:]...)
	os.Exit(app.Run(context.Background(), args))
}
