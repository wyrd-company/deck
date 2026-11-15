//go:build !linux && !darwin

package config

import "fmt"

func userHome() (string, error) {
	return "", fmt.Errorf("user database lookup is unsupported on this platform; pass --config PATH")
}
