package main

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func assertGenerationCloseRejectsRequest(t *testing.T, ctx context.Context, relay *httptest.Server) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, relay.URL+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := relay.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, errRead := io.Copy(io.Discard, res.Body)
	errClose := res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable || errRead != nil || errClose != nil {
		t.Fatalf("request after Close: %d, %v, %v", res.StatusCode, errRead, errClose)
	}
}

func TestGenerationClosePreservesSSE(t *testing.T) {
	next, finish, upstreamClosed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	old := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range []string{"first", "after-close"} {
			if _, err := io.WriteString(w, "data: "+event+"\n\n"); err != nil {
				t.Error(err)
				return
			}
			w.(http.Flusher).Flush()
			if event == "first" {
				select {
				case <-next:
				case <-r.Context().Done():
					return
				}
			}
		}
		select {
		case <-finish:
		case <-r.Context().Done():
		}
	}))
	// Register before accepting the stream, not after Close can release it.
	old.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			close(upstreamClosed)
		}
	}
	old.Start()
	defer old.Close()
	defer finishOnce.Do(func() { close(finish) })
	c, relay := testGenerationController(t, old.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, relay.URL+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := relay.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			t.Error(err)
		}
	}()
	reader := bufio.NewReader(res.Body)
	readEvent := func(event string) {
		t.Helper()
		for _, expected := range []string{"data: " + event + "\n", "\n"} {
			line, err := reader.ReadString('\n')
			if err != nil || line != expected {
				t.Fatalf("event: %q, %v; want %q", line, err, expected)
			}
		}
	}
	readEvent("first")
	done := c.Close()
	assertNotDrained(t, done)
	assertGenerationCloseRejectsRequest(t, ctx, relay)
	// This event cannot have been buffered before Close.
	close(next)
	readEvent("after-close")
	assertNotDrained(t, done)
	assertNotDrained(t, upstreamClosed)
	finishOnce.Do(func() { close(finish) })
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	signal(t, done)
	signal(t, upstreamClosed)
}

func TestGenerationClosePreservesWebSocket(t *testing.T) {
	upstreamEOF, handlerDone := make(chan struct{}), make(chan struct{})
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := conn.WriteMessage(websocket.TextMessage, []byte("first")); err != nil {
			t.Error(err)
			return
		}
		for {
			kind, payload, err := conn.ReadMessage()
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				// The default close handler sent its reply, but leaves TCP open.
				// Observe the relay releasing the upstream socket, not our Close.
				var b [1]byte
				if n, err := conn.UnderlyingConn().Read(b[:]); n != 0 || !errors.Is(err, io.EOF) {
					t.Errorf("upstream TCP termination: %d, %v", n, err)
				} else {
					close(upstreamEOF)
				}
				return
			}
			if err != nil {
				return
			}
			if err := conn.WriteMessage(kind, payload); err != nil {
				t.Error(err)
				return
			}
		}
	}))
	defer old.Close()
	c, relay := testGenerationController(t, old.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(relay.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	var closeOnce sync.Once
	closeClient := func() {
		closeOnce.Do(func() {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	defer closeClient()
	stop := context.AfterFunc(ctx, closeClient)
	defer stop()
	_, first, err := conn.ReadMessage()
	if err != nil || string(first) != "first" {
		t.Fatalf("initial frame: %q, %v", first, err)
	}
	done := c.Close()
	assertNotDrained(t, done)
	assertGenerationCloseRejectsRequest(t, ctx, relay)
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("after-close")); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := conn.ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || string(payload) != "after-close" {
		t.Fatalf("echo after Close: %d %q, %v", kind, payload, err)
	}
	assertNotDrained(t, done)
	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("close handshake: %v", err)
	}
	// Neither TCP direction has terminated merely by exchanging close frames.
	assertNotDrained(t, done)
	assertNotDrained(t, upstreamEOF)
	closeClient()
	signal(t, done)
	signal(t, upstreamEOF)
	signal(t, handlerDone)
}
