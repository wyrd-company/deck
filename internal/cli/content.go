package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"time"

	pb "github.com/wyrd-company/deck/internal/api/deckv1"
	"google.golang.org/protobuf/types/known/structpb"
)

type contentReader struct {
	input io.Reader
	used  bool
}

func (r *contentReader) read(path string) ([]byte, error) {
	if path == "-" {
		if r.used {
			return nil, invalid("only one content flag may read stdin")
		}
		r.used = true
		raw, err := io.ReadAll(r.input)
		if err != nil {
			return nil, invalid("read stdin: %v", err)
		}
		return raw, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, invalid("read content file %q: %v", path, err)
	}
	return raw, nil
}
func (r *contentReader) objects(path string, arrayOnly bool) ([]*structpb.Struct, error) {
	raw, err := r.read(path)
	if err != nil {
		return nil, err
	}
	var value any
	if err = json.Unmarshal(raw, &value); err != nil {
		return nil, invalid("%s: expected Discord JSON objects", path)
	}
	values, ok := value.([]any)
	if !ok {
		if arrayOnly {
			return nil, invalid("%s: expected a JSON array", path)
		}
		values = []any{value}
	}
	result := []*structpb.Struct{}
	for _, value := range values {
		object, ok := value.(map[string]any)
		if !ok {
			return nil, invalid("%s: expected a JSON object", path)
		}
		s, err := structpb.NewStruct(object)
		if err != nil {
			return nil, invalid("%s: invalid Discord object", path)
		}
		result = append(result, s)
	}
	return result, nil
}
func (i *invocation) content() (*pb.Content, error) {
	f := i.flags
	c := &pb.Content{Text: f.one("text")}
	r := contentReader{input: i.app.Stdin}
	if f.has("text") && f.has("text-file") {
		return nil, invalid("use only one of --text and --text-file")
	}
	if f.has("text-file") {
		raw, err := r.read(f.one("text-file"))
		if err != nil {
			return nil, err
		}
		c.Text = string(raw)
	}
	for _, kind := range []string{"mention", "mention-role"} {
		aliases := i.client.Config.Aliases.Users
		if kind == "mention-role" {
			aliases = i.client.Config.Aliases.Roles
		}
		for _, value := range f.values[kind] {
			if a, ok := aliases[value]; ok {
				value = a
			}
			id, err := parseID(value)
			if err != nil {
				return nil, invalid("--%s requires a snowflake or configured alias", kind)
			}
			if kind == "mention" {
				c.MentionUserIds = append(c.MentionUserIds, id)
			} else {
				c.MentionRoleIds = append(c.MentionRoleIds, id)
			}
		}
	}
	for _, path := range f.values["embed"] {
		objects, err := r.objects(path, false)
		if err != nil {
			return nil, err
		}
		c.Embeds = append(c.Embeds, objects...)
	}
	if f.has("components") {
		objects, err := r.objects(f.one("components"), true)
		if err != nil {
			return nil, err
		}
		c.Components = objects
	}
	if f.has("poll") {
		duration, err := f.duration("poll-duration", 24*time.Hour)
		if err != nil {
			return nil, err
		}
		hours := math.Ceil(float64(duration) / float64(time.Hour))
		if hours > 768 {
			return nil, invalid("--poll-duration must be at most 768h")
		}
		answers := f.values["answer"]
		if len(answers) < 1 || len(answers) > 10 {
			return nil, invalid("--poll requires 1 to 10 --answer values")
		}
		c.Poll = &pb.PollCreate{Question: f.one("poll"), Answers: answers, DurationHours: uint32(hours), AllowMultiselect: f.has("poll-multiselect")}
	} else if f.has("answer") || f.has("poll-duration") || f.has("poll-multiselect") {
		return nil, invalid("poll options require --poll QUESTION")
	}
	for _, path := range f.values["file"] {
		raw, err := r.read(path)
		if err != nil {
			return nil, err
		}
		name := filepath.Base(path)
		if path == "-" {
			name = "stdin"
		}
		c.Files = append(c.Files, &pb.File{Name: name, Data: raw})
	}
	if f.has("persona") && f.has("no-persona") {
		return nil, invalid("use only one of --persona and --no-persona")
	}
	name := i.client.Config.DefaultPersona
	if f.has("persona") {
		name = f.one("persona")
	}
	if !f.has("no-persona") && name != "" {
		p, ok := i.client.Config.Personas[name]
		if !ok {
			return nil, invalid("unknown persona %q; configure personas or use --no-persona", name)
		}
		c.Persona = &pb.Persona{Name: p.Name, IconUrl: p.IconURL}
	}
	if c.Text == "" && len(c.Embeds) == 0 && len(c.Components) == 0 && c.Poll == nil && len(c.Files) == 0 {
		return nil, invalid("message needs --text, --embed, --components, --poll or --file")
	}
	return c, nil
}
func parseID(value string) (uint64, error) {
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid snowflake")
		}
	}
	n, err := strconv.ParseUint(value, 10, 63)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("invalid snowflake")
	}
	return n, nil
}
