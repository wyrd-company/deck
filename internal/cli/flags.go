package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type flags struct {
	command string
	args    []string
	values  map[string][]string
}

var common = "config json help h session no-color"
var options = map[string]string{
	"post":  "title tag text text-file mention mention-role embed components poll answer poll-duration poll-multiselect file persona no-persona nonce",
	"reply": "reply-to text text-file mention mention-role embed components poll answer poll-duration poll-multiselect file persona no-persona nonce",
	"inbox": "all", "read": "keep-unread limit", "history": "after before limit", "listen": "timeout keep-unread", "resolve": "event", "channels": "", "skill": "",
}
var bools = map[string]bool{"json": true, "help": true, "h": true, "no-color": true, "all": true, "keep-unread": true, "no-persona": true, "poll-multiselect": true}

func parse(args []string) (flags, error) {
	f := flags{values: map[string][]string{}}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			f.args = append(f.args, args[i+1:]...)
			break
		}
		if strings.HasPrefix(arg, "-") && arg != "-" {
			name, val, has := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(arg, "-"), "-"), "=")
			if !bools[name] && !has {
				if i+1 >= len(args) || strings.HasPrefix(args[i+1], "--") {
					return f, invalid("--%s requires a value; use --help", name)
				}
				i++
				val = args[i]
			}
			if bools[name] {
				if has {
					return f, invalid("--%s does not take a value", name)
				}
				val = "true"
			}
			f.values[name] = append(f.values[name], val)
		} else if f.command == "" {
			f.command = arg
		} else {
			f.args = append(f.args, arg)
		}
	}
	allowed, exists := options[f.command]
	if !exists && f.command != "" {
		return f, invalid("unknown command %q; use deck --help", f.command)
	}
	for name := range f.values {
		if !containsWord(common+" "+allowed, name) || (f.command == "resolve" && name == "session") {
			return f, invalid("unknown option --%s for %s; use --help", name, f.command)
		}
	}
	return f, nil
}
func containsWord(list, word string) bool {
	for _, w := range strings.Fields(list) {
		if w == word {
			return true
		}
	}
	return false
}
func (f flags) one(name string) string {
	a := f.values[name]
	if len(a) == 0 {
		return ""
	}
	return a[len(a)-1]
}
func (f flags) has(name string) bool { return len(f.values[name]) > 0 }
func (f flags) limit(def, max int) (uint32, error) {
	if !f.has("limit") {
		return uint32(def), nil
	}
	n, err := strconv.Atoi(f.one("limit"))
	if err != nil || n < 1 || n > max {
		return 0, invalid("--limit must be between 1 and %d", max)
	}
	return uint32(n), nil
}
func (f flags) duration(name string, def time.Duration) (time.Duration, error) {
	if !f.has(name) {
		return def, nil
	}
	d, err := time.ParseDuration(f.one(name))
	if err != nil || d <= 0 {
		return 0, invalid("--%s requires a positive Go duration (for example 30s)", name)
	}
	return d, nil
}
func (f flags) arity(min, max int) error {
	if len(f.args) < min || len(f.args) > max {
		return invalid("usage: %s", usage(f.command))
	}
	return nil
}
func usage(cmd string) string {
	switch cmd {
	case "post":
		return "deck post [CHANNEL] --title TITLE [content flags]"
	case "reply":
		return "deck reply THREAD [--reply-to MESSAGE] [content flags]"
	case "inbox":
		return "deck inbox [--all]"
	case "read":
		return "deck read [THREAD...] [--keep-unread] [--limit N]"
	case "history":
		return "deck history THREAD [--after MESSAGE | --before MESSAGE] [--limit N]"
	case "listen":
		return "deck listen [THREAD...] [--timeout DURATION] [--keep-unread]"
	case "resolve":
		return "deck resolve THREAD | deck resolve --event PATH"
	case "channels", "skill":
		return "deck " + cmd
	}
	return "deck COMMAND [options]"
}
func help(f flags) string {
	result := "usage: " + usage(f.command) + "\n\nOptions: --config PATH --json --help (-h) --no-color\n"
	if extras := options[f.command]; extras != "" {
		result += wrapHelp("Command options: --"+strings.ReplaceAll(extras, " ", " --")) + "\n"
	}
	if f.command != "resolve" {
		result += "  --session NAME=VALUE  Override configured session detection.\n"
	}
	if f.command == "post" || f.command == "reply" {
		result += "Content: --text TEXT | --text-file PATH (- for stdin), --mention USER,\n  --mention-role ROLE, --embed PATH, --components PATH, --file PATH,\n  --poll QUESTION --answer TEXT [--poll-duration 24h] [--poll-multiselect],\n  --persona NAME | --no-persona, --nonce NONCE (explicit repeat).\n"
	}
	if f.command == "" {
		result += "Commands: post, reply, inbox, read, history, listen, resolve, channels, skill,\n  config validate/key, service start/validate/health/host-key, --version.\n"
	}
	example := map[string]string{"post": "post --title 'Question' --text 'Can you help?'", "reply": "reply 1571484634081529857 --text 'Thank you'", "inbox": "inbox --all", "read": "read --keep-unread", "history": "history 1571484634081529857 --limit 50", "listen": "listen --timeout 5m", "resolve": "resolve --event -", "channels": "channels --json", "skill": "skill"}[f.command]
	if example == "" {
		example = "post --title 'Question' --text 'Can you help?'"
	}
	return result + fmt.Sprintf("\nExample: deck %s\n\nExit: 0 success; 1 failed; 2 invalid; 3 refused; 4 unavailable or unknown\nsend outcome; 5 listen timeout; 130 interrupted.\n", example)
}

func wrapHelp(text string) string {
	var out strings.Builder
	column := 0
	for _, word := range strings.Fields(text) {
		if column > 0 {
			if column+1+len(word) > 78 {
				out.WriteByte('\n')
				column = 0
			} else {
				out.WriteByte(' ')
				column++
			}
		}
		out.WriteString(word)
		column += len(word)
	}
	return out.String()
}
