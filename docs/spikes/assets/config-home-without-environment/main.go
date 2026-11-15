// Command config-home-without-environment is the throwaway probe behind the
// config-home-without-environment spike. It finds the current user's home
// directory by reading the passwd file for the process's user ID, the way
// deck's client locates its default configuration on Linux, and prints it
// beside what Go's os/user returns, so a run with HOME and USER set to other
// values shows which of the two follows the environment.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"strings"
)

// homeFromPasswd returns the home directory field of the passwd entry whose
// user ID is uid. It reads only the file at path and no environment variable.
func homeFromPasswd(path string, uid int) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	want := strconv.Itoa(uid)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), ":")
		if len(fields) >= 7 && fields[2] == want && fields[5] != "" {
			return fields[5], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", errors.New("no passwd entry for user ID " + want)
}

func main() {
	uid := os.Getuid()
	result := map[string]any{"uid": uid}
	if home, err := homeFromPasswd("/etc/passwd", uid); err != nil {
		result["passwd_home"] = nil
		result["passwd_error"] = err.Error()
	} else {
		result["passwd_home"] = home
	}
	if u, err := user.LookupId(strconv.Itoa(uid)); err != nil {
		result["lookupid_error"] = err.Error()
	} else {
		result["lookupid_home"] = u.HomeDir
	}
	line, _ := json.Marshal(result)
	fmt.Println(string(line))
}
