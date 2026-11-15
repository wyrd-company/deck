package client

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// Reference parses an optional bot and nonzero snowflake, or a Discord thread
// URL. Aliases are resolved by the caller before this function.
func Reference(value string) (string, uint64, error) {
	original := value
	if strings.HasPrefix(value, "https://") {
		u, err := url.Parse(value)
		if err != nil || u.Host != "discord.com" || u.RawQuery != "" || u.Fragment != "" {
			return "", 0, fmt.Errorf("invalid Discord thread URL; use https://discord.com/channels/SERVER/THREAD")
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if (len(parts) != 3 && len(parts) != 4) || parts[0] != "channels" {
			return "", 0, fmt.Errorf("invalid Discord thread URL")
		}
		if _, err := snowflake(parts[1]); err != nil {
			return "", 0, err
		}
		if len(parts) == 4 {
			if _, err := snowflake(parts[3]); err != nil {
				return "", 0, err
			}
		}
		value = parts[2]
	}
	bot := ""
	if before, after, ok := strings.Cut(value, "/"); ok {
		bot, value = before, after
		if bot == "" {
			return "", 0, fmt.Errorf("invalid reference %q", original)
		}
	}
	id, err := snowflake(value)
	if err != nil {
		return "", 0, fmt.Errorf("invalid reference %q: use [BOT/]SNOWFLAKE or a configured alias", original)
	}
	return bot, id, nil
}
func snowflake(value string) (uint64, error) {
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid snowflake")
		}
	}
	id, err := strconv.ParseUint(value, 10, 63)
	if err != nil || id == 0 {
		return 0, fmt.Errorf("invalid snowflake")
	}
	return id, nil
}
