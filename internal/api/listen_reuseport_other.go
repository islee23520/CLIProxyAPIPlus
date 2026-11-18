//go:build !linux

package api

import "net"

func serverListen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}
