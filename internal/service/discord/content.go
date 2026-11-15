// ---
// relationships:
//
//	implements:
//	  - service
//	  - grpc-api
//
// ---
// Package discord adapts the API's content and messages to disgo.
package discord

import (
	"bytes"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf16"

	d "github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func length(s string) int          { return len(utf16.Encode([]rune(s))) }
func invalid(message string) error { return status.Error(codes.InvalidArgument, message) }
func Nonce(fingerprint, nonce string) string {
	sum := sha256.Sum256([]byte(fingerprint + ":" + nonce))
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])[:25]
}
func ValidNonce(n string) bool {
	if len(n) < 1 || len(n) > 25 {
		return false
	}
	for _, r := range n {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
func Build(c *pb.Content) (d.MessageCreate, error) {
	var out d.MessageCreate
	if c == nil || c.Text == "" && len(c.Embeds) == 0 && len(c.Components) == 0 && c.Poll == nil && len(c.Files) == 0 {
		return out, invalid("content: at least one content field is required")
	}
	maxText, maxEmbeds := 2000, 10
	if c.Persona != nil {
		maxText, maxEmbeds = 4096, 9
		if length(c.Persona.Name) > 256 {
			return out, invalid("persona.name: maximum 256 characters")
		}
		if c.Persona.IconUrl != "" {
			u, err := url.Parse(c.Persona.IconUrl)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
				return out, invalid("persona.icon_url: must be an http or https URL")
			}
		}
	}
	if length(c.Text) > maxText || len(c.Embeds) > maxEmbeds || len(c.Files) > 10 {
		return out, invalid("content: text, embed or file limit exceeded")
	}
	// Decode via MessageCreate so disgo handles component interfaces.
	values := map[string]any{"embeds": []any{}, "components": []any{}}
	v2 := false
	for _, s := range c.Embeds {
		if s == nil {
			return out, invalid("embeds: invalid object")
		}
		values["embeds"] = append(values["embeds"].([]any), s.AsMap())
	}
	for _, s := range c.Components {
		if s == nil {
			return out, invalid("components: invalid object")
		}
		values["components"] = append(values["components"].([]any), s.AsMap())
		if s.GetFields()["type"].GetNumberValue() >= 9 {
			v2 = true
		}
	}
	if v2 && (c.Text != "" || len(c.Embeds) > 0 || c.Poll != nil || c.Persona != nil) {
		return out, invalid("components v2 require empty text, embeds, poll and persona")
	}
	raw, _ := json.Marshal(values)
	if json.Unmarshal(raw, &out) != nil {
		return out, invalid("embeds or components: invalid Discord object")
	}
	if v2 {
		out.Flags |= d.MessageFlagIsComponentsV2
	}
	allowed := &d.AllowedMentions{Parse: []d.AllowedMentionType{}, Users: []snowflake.ID{}, Roles: []snowflake.ID{}}
	var mentions []string
	for _, id := range c.MentionUserIds {
		if id == 0 {
			return out, invalid("mention_user_ids: zero id")
		}
		allowed.Users = append(allowed.Users, snowflake.ID(id))
		mentions = append(mentions, fmt.Sprintf("<@%d>", id))
	}
	for _, id := range c.MentionRoleIds {
		if id == 0 {
			return out, invalid("mention_role_ids: zero id")
		}
		allowed.Roles = append(allowed.Roles, snowflake.ID(id))
		mentions = append(mentions, fmt.Sprintf("<@&%d>", id))
	}
	out.AllowedMentions = allowed
	if !v2 {
		out.Content = strings.Join(mentions, " ")
		if c.Persona != nil {
			out.Embeds = append([]d.Embed{{Description: c.Text, Author: &d.EmbedAuthor{Name: c.Persona.Name, IconURL: c.Persona.IconUrl}}}, out.Embeds...)
		} else {
			if out.Content != "" && c.Text != "" {
				out.Content += "\n"
			}
			out.Content += c.Text
		}
		if length(out.Content) > 2000 {
			return out, invalid("content: mentions and text exceed 2000 characters")
		}
	}
	chars := 0
	for _, e := range out.Embeds {
		chars += length(e.Title) + length(e.Description)
		if e.Author != nil {
			chars += length(e.Author.Name)
		}
		if e.Footer != nil {
			chars += length(e.Footer.Text)
		}
		for _, f := range e.Fields {
			chars += length(f.Name) + length(f.Value)
		}
	}
	if chars > 6000 {
		return out, invalid("embeds: maximum 6000 characters combined")
	}
	if p := c.Poll; p != nil {
		if length(p.Question) < 1 || length(p.Question) > 300 || len(p.Answers) < 1 || len(p.Answers) > 10 || p.DurationHours > 768 {
			return out, invalid("poll: invalid question, answers or duration")
		}
		poll := d.NewPollCreate(p.Question)
		poll.Duration = int(p.DurationHours)
		if poll.Duration == 0 {
			poll.Duration = 24
		}
		poll.AllowMultiselect = p.AllowMultiselect
		for _, answer := range p.Answers {
			if length(answer) < 1 || length(answer) > 55 {
				return out, invalid("poll.answers: 1 to 55 characters required")
			}
			poll = poll.AddAnswer(answer, nil)
		}
		out.Poll = &poll
	}
	for _, f := range c.Files {
		if f == nil || f.Name == "" || length(f.Description) > 1024 {
			return out, invalid("files: name required and description at most 1024 characters")
		}
		out.Files = append(out.Files, &d.File{Name: f.Name, Description: f.Description, Reader: bytes.NewReader(f.Data)})
	}
	return out, nil
}
func structs[T any](items []T) []*structpb.Struct {
	var out []*structpb.Struct
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(raw, &fields) != nil {
			continue
		}
		s, err := structpb.NewStruct(fields)
		if err == nil {
			out = append(out, s)
		}
	}
	return out
}
