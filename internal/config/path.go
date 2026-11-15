package config

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

// ClientPath returns an explicit path or the default under the user's database
// home. It never reads HOME, USER or XDG_CONFIG_HOME.
func ClientPath(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	home, err := userHome()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "deck", "config.yml"), nil
}
func homeFromPasswd(r io.Reader, uid string) (string, error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), ":")
		if len(fields) >= 7 && fields[2] == uid && filepath.IsAbs(fields[5]) {
			return fields[5], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read user database for UID %s: %w", uid, err)
	}
	return "", fmt.Errorf("no home in user database for UID %s; pass --config PATH", uid)
}
