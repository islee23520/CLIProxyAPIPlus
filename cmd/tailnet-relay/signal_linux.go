package main

import (
	"os"
	ossignal "os/signal"
	"syscall"
)

func relayShutdownSignal() (<-chan os.Signal, func()) {
	shutdown := make(chan os.Signal, 1)
	ossignal.Notify(shutdown, syscall.SIGTERM)
	return shutdown, func() { ossignal.Stop(shutdown) }
}
