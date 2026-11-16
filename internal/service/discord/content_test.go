package discord

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNonceFingerprintScope(t *testing.T) {
	for key, want := range map[string]string{"SHA256:first": "XCPJHYIH6XIMFNQBTFNX7KQRL", "SHA256:second": "YHX4OUNYWB4OTRKUTJVQV2EIT"} {
		if got := Nonce(key, "same"); got != want {
			t.Fatalf("nonce: %q want %q", got, want)
		}
	}
	for _, bad := range []string{"", "has space", "underscore_", strings.Repeat("x", 26)} {
		if ValidNonce(bad) {
			t.Fatal("invalid nonce accepted")
		}
	}
}
func TestV2AndLimits(t *testing.T) {
	display, _ := structpb.NewStruct(map[string]any{"type": 10, "content": "text"})
	c := &pb.Content{Components: []*structpb.Struct{display}, MentionUserIds: []uint64{1}}
	m, err := Build(c)
	if err != nil || m.Flags&d.MessageFlagIsComponentsV2 == 0 || m.Content != "" || len(m.AllowedMentions.Users) != 1 {
		t.Fatal("invalid v2 conversion", err)
	}
	c.Text = "mixed"
	if _, err = Build(c); status.Code(err) != codes.InvalidArgument {
		t.Fatal("mixed v2 accepted")
	}
	c = &pb.Content{Text: strings.Repeat("a", 4096), Persona: &pb.Persona{Name: "p"}}
	if _, err = Build(c); err != nil {
		t.Fatal(err)
	}
	c.Text += "a"
	if _, err = Build(c); err == nil {
		t.Fatal("persona limit ignored")
	}
	for _, c := range []*pb.Content{{Text: "x", Persona: &pb.Persona{IconUrl: "file:///tmp/icon"}}, {Text: "x", Files: make([]*pb.File, 11)}, {Poll: &pb.PollCreate{Question: "?", Answers: []string{strings.Repeat("x", 56)}}}, {Poll: &pb.PollCreate{Question: "?", Answers: []string{"yes"}, DurationHours: 769}}} {
		if _, err := Build(c); status.Code(err) != codes.InvalidArgument {
			t.Fatal("limit accepted", err)
		}
	}
}
func TestSanitizedErrors(t *testing.T) {
	for _, tc := range []struct {
		code int
		http int
		want codes.Code
	}{{50013, 403, codes.FailedPrecondition}, {10008, 404, codes.NotFound}, {40005, 400, codes.InvalidArgument}, {0, 429, codes.ResourceExhausted}, {0, 403, codes.PermissionDenied}, {0, 404, codes.PermissionDenied}, {0, 503, codes.Unavailable}, {98765, 400, codes.Internal}, {50035, 400, codes.InvalidArgument}} {
		err := &rest.Error{Code: rest.JSONErrorCode(tc.code), Message: "TOKEN-DO-NOT-LEAK", Response: &http.Response{StatusCode: tc.http}}
		got := Error(err)
		if status.Code(got) != tc.want || strings.Contains(got.Error(), "TOKEN-DO-NOT-LEAK") {
			t.Fatal(got)
		}
	}
	validation := &rest.Error{Code: 50035, Message: "TOKEN-DO-NOT-LEAK", Errors: json.RawMessage(`{"content":{"_errors":[{"message":"TOKEN-DO-NOT-LEAK"}]},"embeds":{"0":{"title":{"_errors":[{"message":"TOKEN-DO-NOT-LEAK"}]}}}}`)}
	if got := Error(validation); status.Code(got) != codes.InvalidArgument || status.Convert(got).Message() != "Discord validation failed: content, embeds.0.title" {
		t.Fatal(got)
	}
	if got := Error(errors.New("TOKEN-DO-NOT-LEAK")); status.Code(got) != codes.Internal || status.Convert(got).Message() != "Discord operation failed" {
		t.Fatal(got)
	}
	preflight := &limiterError{context.DeadlineExceeded}
	if Unknown(preflight) || status.Code(Error(preflight)) != codes.ResourceExhausted {
		t.Fatal("rate limit treated as an unknown send")
	}
	if !Unknown(errors.New("lost REST response")) {
		t.Fatal("unknown response treated as rejection")
	}
}
