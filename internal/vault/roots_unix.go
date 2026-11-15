//go:build !darwin && !windows

package vault

import "crypto/x509"

func systemRoots() (*x509.CertPool, error) { return unixRoots() }
