package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type relayTestServer struct {
	URL    string
	client *http.Client
}

func (s *relayTestServer) Client() *http.Client { return s.client }

func testRelay(t *testing.T, upstream http.Handler) *relayTestServer {
	t.Helper()
	u := httptest.NewServer(upstream)
	t.Cleanup(u.Close)
	h, transport, err := newRelay(u.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	r := httptest.NewServer(h)
	t.Cleanup(r.Close)
	return &relayTestServer{URL: r.URL, client: r.Client()}
}

func signal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("expected event did not arrive")
	}
}

func TestConfiguration(t *testing.T) {
	for _, address := range []string{"", ":8080", "0.0.0.0:8080", "localhost:8080", "127.0.0.1:0", "127.0.0.1:80", "[::]:8080", "127.0.0.1:08080"} {
		if validateListen(address) == nil {
			t.Errorf("accepted listen %q", address)
		}
	}
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080"} {
		if err := validateListen(address); err != nil {
			t.Error(err)
		}
	}
	for _, origin := range []string{"", "ftp://localhost", "http://user:pass@localhost", "http://localhost/api", "http://localhost?key=x", "http://localhost#fragment", "http://localhost:0"} {
		if _, _, err := newRelay(origin); err == nil {
			t.Errorf("accepted origin %q", origin)
		}
	}
}

func TestPassThrough(t *testing.T) {
	r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if r.Host != "original.example" || r.URL.RequestURI() != "/v1/messages?key=synthetic%2Bkey&x=1&x=2" || string(body) != "payload" {
			t.Errorf("request semantics changed: host=%q target=%q body=%q", r.Host, r.URL.RequestURI(), body)
		}
		for _, key := range []string{"Authorization", "X-Api-Key", "X-Goog-Api-Key", "Anthropic-Version", "Openai-Beta"} {
			if r.Header.Get(key) != "synthetic" {
				t.Errorf("missing header %s", key)
			}
		}
		for _, key := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
			if r.Header.Get(key) != "" {
				t.Errorf("forwarded spoofed %s", key)
			}
		}
		w.Header().Add("X-Test", "one")
		w.Header().Add("X-Test", "two")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.WriteString(w, "response")
	}))
	req, _ := http.NewRequest(http.MethodPost, r.URL+"/v1/messages?key=synthetic%2Bkey&x=1&x=2", strings.NewReader("payload"))
	req.Host = "original.example"
	for _, key := range []string{"Authorization", "X-Api-Key", "X-Goog-Api-Key", "Anthropic-Version", "Openai-Beta", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
		req.Header.Set(key, "synthetic")
	}
	res, err := r.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != 422 || string(body) != "response" || len(res.Header.Values("X-Test")) != 2 {
		t.Fatalf("response semantics changed: status=%d body=%q err=%v", res.StatusCode, body, err)
	}
}

func TestRouteCoverage(t *testing.T) {
	posts := []string{"/v1/chat/completions", "/v1/completions", "/v1/images/generations", "/v1/images/edits", "/v1/videos", "/v1/videos/generations", "/v1/videos/edits", "/v1/videos/extensions", "/v1/messages", "/v1/messages/count_tokens", "/v1/responses", "/v1/responses/compact", "/openai/v1/videos", "/backend-api/codex/responses", "/backend-api/codex/responses/compact", "/v1beta/interactions", "/v1beta/models/gemini:generateContent", "/v1beta/models/org/model:streamGenerateContent"}
	gets := []string{"/v1/models", "/v1/responses", "/backend-api/codex/responses", "/v1beta/models", "/v1beta/models/gemini", "/v1/videos/id", "/openai/v1/videos/id", "/openai/v1/videos/id/content"}
	for method, paths := range map[string][]string{http.MethodPost: posts, http.MethodGet: gets} {
		for _, p := range paths {
			if !allowedRoute(method, p) {
				t.Errorf("blocked %s %s", method, p)
			}
		}
	}
}

func TestBlockedRequestsNeverReachUpstream(t *testing.T) {
	var calls atomic.Int32
	r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	for _, p := range []string{"/", "/v1/ws", "/healthz", "/management.html", "/v0/management/config", "/v0/oauth/kiro/start", "/codex/callback?code=synthetic", "/api/event_logging/batch", "/v1/unknown", "/v1//models", "/v1/./models", "/v1/a/../models", "/v1/models/", "/v1/%6dodels", "/v1%2fmodels", "/v1%2Fmodels", "/v1%5cmodels", "/v1beta/models/%252e%252e", "/openai/v1/videos/a/unknown"} {
		res, err := r.Client().Get(r.URL + p)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != 404 {
			t.Errorf("%s returned %d", p, res.StatusCode)
		}
	}
	for _, method := range []string{http.MethodDelete, http.MethodPut, http.MethodHead, http.MethodOptions, http.MethodPost} {
		req, _ := http.NewRequest(method, r.URL+"/v1/models", nil)
		res, err := r.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != 404 {
			t.Errorf("accepted method %s", method)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("blocked requests reached upstream %d times", calls.Load())
	}
	h, transport, _ := newRelay("http://127.0.0.1:1")
	defer transport.CloseIdleConnections()
	req := httptest.NewRequest(http.MethodGet, "http://attacker.example/v1/models", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatal("accepted absolute-form request")
	}
}

func TestSSEStreamsAndCancels(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	stop := make(chan struct{})
	r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume the HTTP/1 request body so the server can detect disconnects.
		_, err := io.Copy(io.Discard, r.Body)
		errClose := r.Body.Close()
		if err != nil || errClose != nil {
			t.Errorf("consume upstream request body: read=%v close=%v", err, errClose)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-stop:
		}
	}))
	// Release the handler before testRelay closes its servers on assertion failure.
	t.Cleanup(func() { close(stop) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, r.URL+"/v1/responses", strings.NewReader("{}"))
	type result struct {
		res *http.Response
		err error
	}
	response := make(chan result, 1)
	go func() { res, err := r.Client().Do(req); response <- result{res, err} }()
	signal(t, started)
	var res *http.Response
	select {
	case got := <-response:
		if got.err != nil {
			t.Fatal(got.err)
		}
		res = got.res
	case <-time.After(5 * time.Second):
		t.Fatal("response headers buffered until completion")
	}
	defer res.Body.Close()
	chunk := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(res.Body).ReadString('\n'); chunk <- line }()
	select {
	case line := <-chunk:
		if line != "data: first\n" {
			t.Fatalf("unexpected first chunk %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first chunk buffered until completion")
	}
	cancel()
	signal(t, cancelled)
}

func TestWebSocketBidirectional(t *testing.T) {
	for _, p := range []string{"/v1/responses", "/backend-api/codex/responses"} {
		t.Run(p, func(t *testing.T) {
			done := make(chan struct{})
			r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				if r.Header.Get("Authorization") != "Bearer synthetic" {
					t.Error("lost websocket auth")
				}
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				if err := conn.WriteMessage(websocket.TextMessage, []byte("server-first")); err != nil {
					t.Error(err)
					return
				}
				kind, data, err := conn.ReadMessage()
				if err != nil {
					t.Error(err)
					return
				}
				if err := conn.WriteMessage(kind, data); err != nil {
					t.Error(err)
				}
			}))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(r.URL, "http")+p, http.Header{"Authorization": {"Bearer synthetic"}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			errs := make(chan error, 1)
			go func() {
				_, first, err := conn.ReadMessage()
				if err == nil && string(first) != "server-first" {
					err = io.ErrUnexpectedEOF
				}
				if err == nil {
					err = conn.WriteMessage(websocket.BinaryMessage, []byte("client-payload"))
				}
				if err == nil {
					kind, payload, errRead := conn.ReadMessage()
					err = errRead
					if err == nil && (kind != websocket.BinaryMessage || string(payload) != "client-payload") {
						err = io.ErrUnexpectedEOF
					}
				}
				errs <- err
			}()
			select {
			case err := <-errs:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("websocket exchange did not finish")
			}
			signal(t, done)
		})
	}
}
