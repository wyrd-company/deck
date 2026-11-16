package config

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
)

func userHome() (string, error) {
	uid := strconv.Itoa(os.Getuid())
	u, err := user.LookupId(uid)
	if err != nil || !filepath.IsAbs(u.HomeDir) {
		return "", fmt.Errorf("no home in user database for UID %s; pass --config PATH", uid)
	}
	return u.HomeDir, nil
}
