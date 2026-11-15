package config

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

func object(v any) map[string]any { m, _ := v.(map[string]any); return m }
func array(v any) []any           { a, _ := v.([]any); return a }
func str(v any) string            { s, _ := v.(string); return s }

// ValidateVaultAddress is also used by the shared Vault client, so direct
// callers cannot accidentally enable credentials in URLs, proxies or remote HTTP.
func ValidateVaultAddress(address string) error {
	u, err := url.Parse(address)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return FieldError{"vault.address", "use unix:///path, https://host:port, or loopback http:// without user information, query or fragment"}
	}
	valid := false
	switch u.Scheme {
	case "unix":
		valid = u.Host == "" && strings.HasPrefix(u.Path, "/") && len(u.Path) > 1
	case "https":
		valid = u.Hostname() != "" && (u.Path == "" || u.Path == "/")
	case "http":
		ip := net.ParseIP(u.Hostname())
		valid = (u.Hostname() == "localhost" || (ip != nil && ip.IsLoopback())) && (u.Path == "" || u.Path == "/")
	}
	if !valid {
		return FieldError{"vault.address", "use unix:///path, https://host:port, or loopback http://"}
	}
	return nil
}
func semantic(problems *Errors, raw any, name string) {
	root := object(raw)
	walkValues(problems, raw, "")
	v := object(root["vault"])
	if address := str(v["address"]); address != "" {
		if err := ValidateVaultAddress(address); err != nil {
			problems.add("vault.address", err.(FieldError).Reason)
		}
	}
	if name == "client" {
		clientRules(problems, root)
	} else {
		serviceRules(problems, root)
	}
}
func walkValues(problems *Errors, raw any, path string) {
	switch v := raw.(type) {
	case map[string]any:
		for key, value := range v {
			p := join(path, key)
			s, ok := value.(string)
			if ok {
				switch key {
				case "timeout", "max_age", "duplicate_window":
					if p != "service.timeout" && p != "vault.timeout" && p != "nats.timeout" && p != "nats.stream.max_age" && p != "nats.stream.duplicate_window" && p != "nats.threads.max_age" {
						break
					}
					if _, err := time.ParseDuration(s); err != nil {
						problems.add(p, "must be a Go duration within the supported range")
					}
				case "public_key":
					checkPublicKey(problems, p, s)
				case "host_key":
					if path == "service" {
						checkPublicKey(problems, p, s)
					}
				case "ssh", "health":
					if path == "listen" {
						checkEndpoint(problems, p, s)
					}
				case "address":
					if path == "service" {
						checkEndpoint(problems, p, s)
					}
				}
			}
			walkValues(problems, value, p)
		}
	case []any:
		for i, value := range v {
			walkValues(problems, value, path+"["+strconv.Itoa(i)+"]")
		}
	}
}
func checkPublicKey(problems *Errors, path, value string) {
	key, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(value))
	if err != nil || len(options) > 0 || len(rest) > 0 || key.Type() != ssh.KeyAlgoED25519 {
		problems.add(path, "must be one valid ed25519 authorized-keys public key")
	}
}
func checkEndpoint(problems *Errors, path, value string) {
	_, port, err := net.SplitHostPort(value)
	n, e := strconv.Atoi(port)
	if err != nil || e != nil || n < 1 || n > 65535 {
		problems.add(path, "must be host:port with a port from 1 to 65535")
	}
}
func clientRules(problems *Errors, root map[string]any) {
	if persona := str(root["default_persona"]); persona != "" {
		if _, ok := object(root["personas"])[persona]; !ok {
			problems.add("default_persona", "must name a configured persona")
		}
	}
	aliases := object(object(root["aliases"])["channels"])
	channels := object(root["channels"])
	check := func(path, ref string) {
		if ref == "" || strings.Contains(ref, "/") {
			return
		}
		digits := true
		for _, r := range ref {
			if r < '0' || r > '9' {
				digits = false
				break
			}
		}
		if digits {
			return
		}
		if _, ok := aliases[ref]; !ok {
			problems.add(path, "must name a configured channel alias")
		}
	}
	check("channels.default", str(channels["default"]))
	for _, field := range []string{"allow", "deny"} {
		for i, ref := range array(channels[field]) {
			check("channels."+field+"["+strconv.Itoa(i)+"]", str(ref))
		}
	}
}
func serviceRules(problems *Errors, root map[string]any) {
	bots := map[string]bool{}
	for i, value := range array(root["bots"]) {
		b := object(value)
		name := str(b["name"])
		if name != "" {
			if bots[name] {
				problems.add("bots["+strconv.Itoa(i)+"].name", "must be unique")
			}
			bots[name] = true
		}
	}
	names, publicKeys := map[string]bool{}, map[string]bool{}
	for i, value := range array(root["keys"]) {
		k := object(value)
		p := "keys[" + strconv.Itoa(i) + "]"
		name := str(k["name"])
		if name != "" {
			if names[name] {
				problems.add(p+".name", "must be unique")
			}
			names[name] = true
		}
		key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(str(k["public_key"])))
		if err == nil {
			identity := string(key.Marshal())
			if publicKeys[identity] {
				problems.add(p+".public_key", "must be unique (comments do not distinguish keys)")
			}
			publicKeys[identity] = true
		}
		for j, value := range array(k["grants"]) {
			g := object(value)
			if bot := str(g["bot"]); bot != "" && !bots[bot] {
				problems.add(p+".grants["+strconv.Itoa(j)+"].bot", "must name a configured bot")
			}
		}
	}
}
