package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/santhosh-tekuri/jsonschema/v6/kind"
	deck "github.com/wyrd-company/deck"
	"go.yaml.in/yaml/v3"
)

const MaxSize = 1 << 20
const ServicePath = "/etc/deck/config.yml"

type FieldError struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

func (e FieldError) Error() string { return e.Path + ": " + e.Reason }

type Errors []FieldError

func (e Errors) Error() string {
	var lines []string
	for _, item := range e {
		lines = append(lines, item.Error())
	}
	return strings.Join(lines, "\n")
}
func (e *Errors) add(path, reason string) {
	if path == "" {
		path = "$"
	}
	*e = append(*e, FieldError{path, reason})
}

var schemas = sync.OnceValues(func() (map[string]*jsonschema.Schema, error) {
	result := map[string]*jsonschema.Schema{}
	for _, name := range []string{"client", "service"} {
		raw, err := deck.ConfigurationSchemas.ReadFile("docs/specifications/assets/configuration/" + name + ".schema.json")
		if err != nil {
			return nil, err
		}
		var doc any
		if err = json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
		c := jsonschema.NewCompiler()
		if err = c.AddResource(name+".json", doc); err != nil {
			return nil, err
		}
		s, err := c.Compile(name + ".json")
		if err != nil {
			return nil, err
		}
		result[name] = s
	}
	return result, nil
})

func read(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, MaxSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxSize {
		return nil, Errors{{"$", "configuration exceeds 1 MiB"}}
	}
	return data, nil
}
func LoadClient(path string) (*Client, error) {
	data, err := read(path)
	if err != nil {
		return nil, err
	}
	return ParseClient(data, path)
}
func LoadService(path string) (*Service, error) {
	data, err := read(path)
	if err != nil {
		return nil, err
	}
	return ParseService(data)
}

// ParseClient uses filename only to locate the default store beside the config.
func ParseClient(data []byte, filename string) (*Client, error) {
	if err := validate(data, "client"); err != nil {
		return nil, err
	}
	filename, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}
	c := Client{Service: ServiceEndpoint{Timeout: 15 * time.Second}, Vault: vaultDefaults(), Key: TransitKey{Mount: "transit"}, Store: Store{Path: filepath.Join(filepath.Dir(filename), "deck.db")}}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, Errors{{"$", "cannot decode typed configuration"}}
	}
	defaultAuth(&c.Vault)
	return &c, nil
}
func ParseService(data []byte) (*Service, error) {
	if err := validate(data, "service"); err != nil {
		return nil, err
	}
	c := Service{Listen: Listen{SSH: ":2222", Health: ":8080"}, Vault: vaultDefaults(), NATS: NATS{Name: "deck", Timeout: 10 * time.Second, SubjectPrefix: "deck", Stream: Stream{Name: "DECK", MaxAge: 168 * time.Hour, DuplicateWindow: 2 * time.Minute, Replicas: 1, Manage: true}, Threads: Threads{Bucket: "deck-threads", Replicas: 1, Manage: true}}}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, Errors{{"$", "cannot decode typed configuration"}}
	}
	defaultAuth(&c.Vault)
	for _, r := range []*SecretRef{&c.HostKey, &c.NATS.Creds} {
		if r.Mount == "" {
			r.Mount = c.Vault.KVMount
		}
	}
	for i := range c.Bots {
		if c.Bots[i].Token.Mount == "" {
			c.Bots[i].Token.Mount = c.Vault.KVMount
		}
	}
	return &c, nil
}
func vaultDefaults() Vault { return Vault{Timeout: 10 * time.Second, KVMount: "secret"} }
func defaultAuth(v *Vault) {
	if v.Auth.AppRole != nil && v.Auth.AppRole.Mount == "" {
		v.Auth.AppRole.Mount = "approle"
	}
}

func validate(data []byte, name string) error {
	if len(data) > MaxSize {
		return Errors{{"$", "configuration exceeds 1 MiB"}}
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return Errors{{"$", "invalid YAML (check syntax and duplicate keys)"}}
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return Errors{{"$", "expected exactly one YAML document"}}
	}
	compiled, err := schemas()
	if err != nil {
		return fmt.Errorf("compile configuration schema: %w", err)
	}
	var problems Errors
	if err := compiled[name].Validate(raw); err != nil {
		var ve *jsonschema.ValidationError
		if !errors.As(err, &ve) {
			return err
		}
		schemaErrors(&problems, ve, raw)
	}
	semantic(&problems, raw, name)
	if len(problems) == 0 {
		return nil
	}
	sort.Slice(problems, func(i, j int) bool {
		if problems[i].Path == problems[j].Path {
			return problems[i].Reason < problems[j].Reason
		}
		return problems[i].Path < problems[j].Path
	})
	unique := problems[:0]
	for _, p := range problems {
		if len(unique) == 0 || unique[len(unique)-1] != p {
			unique = append(unique, p)
		}
	}
	return unique
}
func fieldPath(tokens []string, raw any) string {
	var out string
	for _, token := range tokens {
		if a, ok := raw.([]any); ok {
			out += "[" + token + "]"
			i, err := strconv.Atoi(token)
			if err == nil && i >= 0 && i < len(a) {
				raw = a[i]
			} else {
				raw = nil
			}
		} else {
			if out != "" {
				out += "."
			}
			out += token
			if m, ok := raw.(map[string]any); ok {
				raw = m[token]
			}
		}
	}
	return out
}
func schemaErrors(problems *Errors, e *jsonschema.ValidationError, raw any) {
	path := fieldPath(e.InstanceLocation, raw)
	switch k := e.ErrorKind.(type) {
	case *kind.Required:
		for _, name := range k.Missing {
			problems.add(join(path, name), "required field is missing")
		}
		return
	case *kind.AdditionalProperties:
		for _, name := range k.Properties {
			problems.add(join(path, name), "unknown field")
		}
		return
	}
	if len(e.Causes) > 0 {
		for _, cause := range e.Causes {
			schemaErrors(problems, cause, raw)
		}
		return
	}
	// Schema diagnostics must never echo a rejected value (it may be a secret).
	keyword := strings.Join(e.ErrorKind.KeywordPath(), ".")
	problems.add(path, "does not satisfy "+keyword+" in the configuration schema")
}
func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}
