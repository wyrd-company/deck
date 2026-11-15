// Package session detects only the explicitly configured ambient identifiers.
package session

import (
	"fmt"
	"os"
	"strings"
)

type Identifier struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
type Session struct{ Identifiers []Identifier }

func (s Session) Key() string {
	if len(s.Identifiers) == 0 {
		return ""
	}
	return s.Identifiers[0].Name + "=" + s.Identifiers[0].Value
}
func Detect(variables []string, override string) (Session, error) {
	s := Session{Identifiers: []Identifier{}}
	if override != "" {
		name, value, ok := strings.Cut(override, "=")
		if !ok || name == "" || value == "" {
			return s, fmt.Errorf("--session requires NAME=VALUE")
		}
		s.Identifiers = append(s.Identifiers, Identifier{name, value})
		return s, nil
	}
	for _, name := range variables {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			s.Identifiers = append(s.Identifiers, Identifier{name, value})
		}
	}
	return s, nil
}
