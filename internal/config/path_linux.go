package config

import (
	"fmt"
	"os"
	"strconv"
)

func userHome() (string, error) {
	uid := strconv.Itoa(os.Getuid())
	f, err := os.Open("/etc/passwd")
	if err != nil {
		return "", fmt.Errorf("read user database for UID %s; pass --config PATH: %w", uid, err)
	}
	defer f.Close()
	return homeFromPasswd(f, uid)
}
