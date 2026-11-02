package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Two real connections establish SSE and WebSocket before the next Accept is
// held at a channel barrier. No socket deadline is needed to inject the error.
type acceptErrorListener struct {
	net.Listener
	accepts  atomic.Int32
	closes   atomic.Int32
	blocked  chan struct{}
	inject   chan error
	closed   chan struct{}
	draining chan struct{}
}

func (l *acceptErrorListener) Accept() (net.Conn, error) {
	if l.accepts.Add(1) <= 2 {
		return l.Listener.Accept()
	}
	close(l.blocked)
	select {
	case err := <-l.inject:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *acceptErrorListener) Close() error {
	switch l.closes.Add(1) {
	case 1:
		close(l.closed)
		return l.Listener.Close()
	case 2:
		// Serve closes its listener before returning the Accept error. The
		// next Close belongs to serveRelay, after controller.Close retires it.
		close(l.draining)
	}
	return net.ErrClosed
}

func TestServeRelayAcceptErrorPreservesStreams(t *testing.T) {
	const secret = "synthetic-accept-secret-8ab6"
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"ordinary", errors.New(secret)},
		{"closed", net.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			next := make(chan string)
			sseDone, wsDone := make(chan struct{}), make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					defer close(wsDone)
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
					defer stop()
					for {
						kind, payload, err := conn.ReadMessage()
						if err != nil {
							return
						}
						if err := conn.WriteMessage(kind, payload); err != nil {
							return
						}
					}
				}
				defer close(sseDone)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: established\n\n")
				w.(http.Flusher).Flush()
				for {
					select {
					case nonce := <-next:
						if _, err := io.WriteString(w, "data: "+nonce+"\n\n"); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					case <-r.Context().Done():
						return
					case <-ctx.Done():
						return
					}
				}
			}))
			defer upstream.Close()
			// Cancellation precedes httptest.Close even on an early assertion.
			defer cancel()
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			controlled := &acceptErrorListener{Listener: listener, blocked: make(chan struct{}), inject: make(chan error), closed: make(chan struct{}), draining: make(chan struct{})}
			defer controlled.Close()
			controller, err := newGenerationController(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			returned := make(chan struct{})
			var serveErr error
			go func() {
				serveErr = serveRelay(controlled, controller, make(chan os.Signal))
				close(returned)
			}()
			defer func() {
				cancel()
				_ = controlled.Close()
				signal(t, returned)
			}()
			transport := &http.Transport{DisableKeepAlives: true}
			defer transport.CloseIdleConnections()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String()+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := (&http.Client{Transport: transport}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			reader := bufio.NewReader(response.Body)
			readSSE := func(nonce string) {
				t.Helper()
				for _, want := range []string{"data: " + nonce + "\n", "\n"} {
					line, err := reader.ReadString('\n')
					if err != nil || line != want {
						t.Fatalf("SSE event: %q, %v; want %q", line, err, want)
					}
				}
			}
			readSSE("established")
			dialer := websocket.Dialer{}
			conn, _, err := dialer.DialContext(ctx, "ws://"+listener.Addr().String()+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			defer stop()
			echo := func(nonce string) {
				t.Helper()
				if err := conn.WriteMessage(websocket.BinaryMessage, []byte(nonce)); err != nil {
					t.Fatal(err)
				}
				kind, payload, err := conn.ReadMessage()
				if err != nil || kind != websocket.BinaryMessage || string(payload) != nonce {
					t.Fatalf("WebSocket echo: %d %q, %v", kind, payload, err)
				}
			}
			echo("established")
			signal(t, controlled.blocked)
			select {
			case controlled.inject <- tc.err:
			case <-ctx.Done():
				t.Fatal("Accept error injection deadline exceeded")
			}
			signal(t, controlled.draining)
			select {
			case next <- "post-error-nonce":
			case <-ctx.Done():
				t.Fatal("SSE producer deadline exceeded")
			}
			readSSE("post-error-nonce")
			echo("post-error-nonce")
			assertNotDrained(t, returned)
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			signal(t, sseDone)
			// A hijacked WebSocket alone must still keep serveRelay draining.
			echo("after-sse-close")
			assertNotDrained(t, returned)
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			signal(t, wsDone)
			signal(t, returned)
			if serveErr == nil || strings.Contains(serveErr.Error(), secret) {
				t.Fatalf("expected a nonnil sanitized serve error, got %v", serveErr)
			}
		})
	}
}

type preShutdownListener struct {
	accepts atomic.Int32
	closes  atomic.Int32
}

func (l *preShutdownListener) Accept() (net.Conn, error) {
	l.accepts.Add(1)
	return nil, net.ErrClosed
}

func (l *preShutdownListener) Close() error   { l.closes.Add(1); return nil }
func (l *preShutdownListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestServeRelayPreSignaledShutdown(t *testing.T) {
	controller, err := newGenerationController("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	listener := &preShutdownListener{}
	shutdown := make(chan os.Signal, 1)
	shutdown <- os.Interrupt
	if err := serveRelay(listener, controller, shutdown); err != nil {
		t.Fatal(err)
	}
	if listener.accepts.Load() != 0 || listener.closes.Load() != 1 {
		t.Fatalf("listener calls: Accept=%d Close=%d", listener.accepts.Load(), listener.closes.Load())
	}
	signal(t, controller.done)
}
