package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"google.golang.org/protobuf/proto"
)

func TestUnsendableRequestIsNotRecorded(t *testing.T) {
	dir := t.TempDir()
	latin1, large := filepath.Join(dir, "latin1.txt"), filepath.Join(dir, "large.bin")
	if err := os.WriteFile(latin1, []byte("caf\xe9 notes\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(large, make([]byte, 64<<20), 0600); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"post", "reply"} {
		for _, jsonMode := range []bool{false, true} {
			for _, test := range []struct {
				name, flag, value, field, cause string
			}{
				{"latin1 file", "--text-file", latin1, "content.text", "UTF-8"},
				{"invalid text", "--text", "bad\xffbyte", "content.text", "UTF-8"},
				{"oversize file", "--file", large, "content.files", "64 MiB"},
			} {
				t.Run(fmt.Sprintf("%s/json=%v/%s", kind, jsonMode, test.name), func(t *testing.T) {
					f := setup(t)
					if kind == "reply" {
						f.seed()
					}
					s := f.open()
					calls := 0
					f.fake.post = func(context.Context, *pb.PostRequest) (*pb.PostResponse, error) {
						calls++
						return &pb.PostResponse{}, nil
					}
					f.fake.reply = func(context.Context, *pb.ReplyRequest) (*pb.ReplyResponse, error) {
						calls++
						return &pb.ReplyResponse{}, nil
					}
					args := []string{kind}
					if kind == "post" {
						args = append(args, "team", "--title", "T")
					} else {
						args = append(args, "10")
					}
					args = append(args, test.flag, test.value, "--nonce", "localFailure")
					if jsonMode {
						args = append(args, "--json")
					}
					code, out, stderr := f.run(args...)
					message := out + stderr
					if code != 2 || !strings.Contains(message, test.field) || !strings.Contains(message, test.cause) || calls != 0 {
						t.Fatalf("local send failure not refused: exit=%d calls=%d out=%s err=%s", code, calls, out, stderr)
					}
					if strings.Contains(message, "outcome unknown") || strings.Contains(message, "before repeating") {
						t.Fatal("local failure suggests uncertain remote outcome", message)
					}
					if _, err := s.SendRecord(keyA, "localFailure"); !errors.Is(err, sql.ErrNoRows) {
						t.Fatal("local failure recorded as a send", err)
					}
				})
			}
		}
	}
}

func TestSendRequestWireBoundary(t *testing.T) {
	request := &pb.PostRequest{Content: &pb.Content{Files: []*pb.File{{Data: make([]byte, (64<<20)-32)}}}}
	file := request.Content.Files[0]
	file.Data = append(file.Data, make([]byte, (64<<20)-proto.Size(request))...)
	if proto.Size(request) != 64<<20 {
		t.Fatal("fixture is not exactly the wire limit")
	}
	if err := validateSendRequest(request); err != nil {
		t.Fatal("exactly 64 MiB must be sendable", err)
	}
	file.Data = append(file.Data, 0)
	if err := validateSendRequest(request); ExitCode(err) != 2 || !strings.Contains(err.Error(), "content.files") {
		t.Fatal("envelope size not enforced", err)
	}
}

func TestSendRequestEncodingNamesField(t *testing.T) {
	for _, test := range []struct {
		request proto.Message
		field   string
	}{
		{&pb.PostRequest{Title: "bad\xff"}, "title"},
		{&pb.PostRequest{Tags: []string{"ok", "bad\xff"}}, "tags[1]"},
		{&pb.ReplyRequest{Content: &pb.Content{Files: []*pb.File{{Name: "bad\xff"}}}}, "content.files[0].name"},
	} {
		err := validateSendRequest(test.request)
		if ExitCode(err) != 2 || !strings.Contains(err.Error(), test.field) || !strings.Contains(err.Error(), "UTF-8") {
			t.Fatal("encoding failure lacks field", test.field, err)
		}
	}
}
