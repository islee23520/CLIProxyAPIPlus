//go:build linux

package api

import "testing"

func TestServerListenSharesPortOnlyWithReusePort(t *testing.T) {
	t.Setenv(listenReusePortEnv, "1")
	first, errFirst := serverListen("tcp", "127.0.0.1:0")
	if errFirst != nil {
		t.Fatalf("first listen: %v", errFirst)
	}
	defer func() { _ = first.Close() }()
	addr := first.Addr().String()

	second, errSecond := serverListen("tcp", addr)
	if errSecond != nil {
		t.Fatalf("second listen on %s with reuseport: %v", addr, errSecond)
	}
	_ = second.Close()

	t.Setenv(listenReusePortEnv, "")
	if third, errThird := serverListen("tcp", addr); errThird == nil {
		_ = third.Close()
		t.Fatalf("listen on %s without reuseport succeeded, want address in use", addr)
	}
}
