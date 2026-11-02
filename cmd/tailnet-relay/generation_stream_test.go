package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testGenerationController(t *testing.T, origin string) (*generationController, *httptest.Server) {
	t.Helper()
	c, err := newGenerationController(origin)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { signal(t, c.Close()) })
	r := httptest.NewServer(c)
	t.Cleanup(r.Close)
	return c, r
}

func assertNotDrained(t *testing.T, drained <-chan struct{}) {
	t.Helper()
	select {
	case <-drained:
		t.Fatal("generation drained with an active handler")
	default:
	}
}

func TestGenerationSwitchPreservesSSE(t *testing.T) {
	// Given an established stream whose next event and completion are gated.
	next, finish := make(chan struct{}), make(chan struct{})
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if _, err := io.WriteString(w, "data: first\n\n"); err != nil {
			t.Error(err)
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-next:
		case <-r.Context().Done():
			return
		}
		if _, err := io.WriteString(w, "data: second\n\n"); err != nil {
			t.Error(err)
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-finish:
		case <-r.Context().Done():
		}
	}))
	defer old.Close()
	newOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.WriteString(w, "new-origin"); err != nil {
			t.Error(err)
		}
	}))
	defer newOrigin.Close()
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
	for _, expected := range []string{"data: first\n", "\n"} {
		line, err := reader.ReadString('\n')
		if err != nil || line != expected {
			t.Fatalf("first event: %q, %v", line, err)
		}
	}
	// When the fixed backend is switched, only new acquisitions move.
	drained, err := c.Switch(newOrigin.URL)
	if err != nil {
		t.Fatal(err)
	}
	assertNotDrained(t, drained)
	newReq, err := http.NewRequestWithContext(ctx, http.MethodGet, relay.URL+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	newRes, err := relay.Client().Do(newReq)
	if err != nil {
		t.Fatal(err)
	}
	body, errRead := io.ReadAll(newRes.Body)
	errClose := newRes.Body.Close()
	if errRead != nil || errClose != nil || string(body) != "new-origin" {
		t.Fatalf("new request: %q, %v, %v", body, errRead, errClose)
	}
	// Then the original stream still delivers, and drains only at completion.
	close(next)
	for _, expected := range []string{"data: second\n", "\n"} {
		line, err := reader.ReadString('\n')
		if err != nil || line != expected {
			t.Fatalf("second event: %q, %v", line, err)
		}
	}
	assertNotDrained(t, drained)
	close(finish)
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	signal(t, drained)
}

func TestGenerationSwitchPreservesWebSocket(t *testing.T) {
	// Given a bidirectional WebSocket on the old fixed backend.
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if err := conn.WriteMessage(websocket.TextMessage, []byte("old-origin")); err != nil {
			t.Error(err)
			return
		}
		for {
			kind, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if err := conn.WriteMessage(kind, payload); err != nil {
				return
			}
		}
	}))
	defer old.Close()
	newOrigin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.WriteString(w, "new-origin"); err != nil {
			t.Error(err)
		}
	}))
	defer newOrigin.Close()
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
	// A bounded test watchdog closes the socket on failure; no network deadlines.
	stop := context.AfterFunc(ctx, closeClient)
	defer stop()
	_, first, err := conn.ReadMessage()
	if err != nil || string(first) != "old-origin" {
		t.Fatalf("initial frame: %q, %v", first, err)
	}
	// When switching, the old upgraded handler remains counted.
	drained, err := c.Switch(newOrigin.URL)
	if err != nil {
		t.Fatal(err)
	}
	assertNotDrained(t, drained)
	// Then both directions still work across the switch.
	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("after-switch")); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := conn.ReadMessage()
	if err != nil || kind != websocket.BinaryMessage || string(payload) != "after-switch" {
		t.Fatalf("echo: %d %q, %v", kind, payload, err)
	}
	assertNotDrained(t, drained)
	if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("close handshake: %v", err)
	}
	// A close frame does not close the client TCP connection. ReverseProxy can
	// keep copying that direction until the client closes the socket as well.
	closeClient()
	signal(t, drained)
}
