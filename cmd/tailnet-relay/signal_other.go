//go:build !linux

package main

import "os"

// Non-Linux builds retain the platform's default signal behavior.
func relayShutdownSignal() (<-chan os.Signal, func()) {
	return nil, func() {}
}
