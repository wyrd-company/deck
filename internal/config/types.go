// Package config loads the client and service config.yml contracts without
// reading environment variables or contacting any server.
package config

import (
	"fmt"
	"time"
)

type Client struct {
	Service        ServiceEndpoint    `yaml:"service"`
	Vault          Vault              `yaml:"vault"`
	Key            TransitKey         `yaml:"key"`
	Session        Session            `yaml:"session"`
	Store          Store              `yaml:"store"`
	Channels       Channels           `yaml:"channels"`
	Aliases        Aliases            `yaml:"aliases"`
	Personas       map[string]Persona `yaml:"personas"`
	DefaultPersona string             `yaml:"default_persona"`
}
type ServiceEndpoint struct {
	Address string        `yaml:"address"`
	HostKey string        `yaml:"host_key"`
	Timeout time.Duration `yaml:"timeout"`
}
type TransitKey struct {
	Mount string `yaml:"mount"`
	Name  string `yaml:"name"`
}
type Session struct {
	Variables []string `yaml:"variables"`
}
type Store struct {
	Path string `yaml:"path"`
}
type Channels struct {
	Default string   `yaml:"default"`
	Allow   []string `yaml:"allow"`
	Deny    []string `yaml:"deny"`
}
type Aliases struct {
	Channels map[string]string `yaml:"channels"`
	Users    map[string]string `yaml:"users"`
	Roles    map[string]string `yaml:"roles"`
}
type Persona struct {
	Name    string `yaml:"name"`
	IconURL string `yaml:"icon_url"`
}

type Service struct {
	Listen  Listen    `yaml:"listen"`
	Vault   Vault     `yaml:"vault"`
	HostKey SecretRef `yaml:"host_key"`
	NATS    NATS      `yaml:"nats"`
	Bots    []Bot     `yaml:"bots"`
	Keys    []Key     `yaml:"keys"`
}
type Listen struct {
	SSH    string `yaml:"ssh"`
	Health string `yaml:"health"`
}
type Vault struct {
	Address   string        `yaml:"address"`
	Namespace string        `yaml:"namespace"`
	CAFile    string        `yaml:"ca_file"`
	Timeout   time.Duration `yaml:"timeout"`
	KVMount   string        `yaml:"kv_mount"`
	Auth      Auth          `yaml:"auth"`
}
type Auth struct {
	TokenFile string   `yaml:"token_file"`
	AppRole   *AppRole `yaml:"approle"`
}
type AppRole struct {
	Mount        string `yaml:"mount"`
	RoleID       string `yaml:"role_id"`
	SecretIDFile string `yaml:"secret_id_file"`
}
type SecretRef struct {
	Mount string `yaml:"mount"`
	Path  string `yaml:"path"`
	Field string `yaml:"field"`
}
type NATS struct {
	Servers       []string      `yaml:"servers"`
	Name          string        `yaml:"name"`
	Creds         SecretRef     `yaml:"creds"`
	CAFile        string        `yaml:"ca_file"`
	Timeout       time.Duration `yaml:"timeout"`
	SubjectPrefix string        `yaml:"subject_prefix"`
	Stream        Stream        `yaml:"stream"`
	Threads       Threads       `yaml:"threads"`
}
type Stream struct {
	Name            string        `yaml:"name"`
	MaxAge          time.Duration `yaml:"max_age"`
	DuplicateWindow time.Duration `yaml:"duplicate_window"`
	Replicas        int           `yaml:"replicas"`
	Manage          bool          `yaml:"manage"`
}
type Threads struct {
	Bucket   string        `yaml:"bucket"`
	MaxAge   time.Duration `yaml:"max_age"`
	Replicas int           `yaml:"replicas"`
	Manage   bool          `yaml:"manage"`
}
type Bot struct {
	Name  string    `yaml:"name"`
	Token SecretRef `yaml:"token"`
}
type Key struct {
	Name      string  `yaml:"name"`
	PublicKey string  `yaml:"public_key"`
	Grants    []Grant `yaml:"grants"`
}
type Grant struct {
	Bot      string   `yaml:"bot"`
	Server   string   `yaml:"server"`
	Channels []string `yaml:"channels"`
}

// SecretRefs lists the service's secret fields by their config paths. Values
// have their default mounts after LoadService/ParseService.
func (s *Service) SecretRefs() map[string]SecretRef {
	refs := map[string]SecretRef{"host_key": s.HostKey, "nats.creds": s.NATS.Creds}
	for i, bot := range s.Bots {
		refs[fmt.Sprintf("bots[%d].token", i)] = bot.Token
	}
	return refs
}
