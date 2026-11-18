//go:build linux

package api

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// listenReusePortEnv enables SO_REUSEPORT on the main API listener so that a
// replacement process can bind the same address before the old process stops
// listening, which allows zero-downtime rolling restarts.
const listenReusePortEnv = "CLIPROXY_LISTEN_REUSEPORT"

func serverListen(network, address string) (net.Listener, error) {
	enabled, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv(listenReusePortEnv)))
	if !enabled {
		return net.Listen(network, address)
	}
	lc := net.ListenConfig{Control: func(_, _ string, conn syscall.RawConn) error {
		var errSet error
		if errControl := conn.Control(func(fd uintptr) {
			errSet = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); errControl != nil {
			return errControl
		}
		return errSet
	}}
	return lc.Listen(context.Background(), network, address)
}
