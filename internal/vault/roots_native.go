//go:build darwin || windows

package vault

import "crypto/x509"

// These platforms obtain roots from the native certificate store.
func systemRoots() (*x509.CertPool, error) { return x509.SystemCertPool() }
