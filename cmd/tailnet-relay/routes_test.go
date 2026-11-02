package main

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestGETAllowlistThroughRelay(t *testing.T) {
	// Given an upstream that identifies forwarded requests.
	r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusAccepted)
	}))
	for _, p := range []string{"/v1/models", "/v1/responses", "/backend-api/codex/responses", "/v1beta/models", "/v1beta/models/org/model:action", "/v1/videos/id", "/openai/v1/videos/id", "/openai/v1/videos/id/content"} {
		t.Run(p, func(t *testing.T) {
			// When the client requests an allowed GET path.
			res, err := r.Client().Get(r.URL + p)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			// Then it reaches the upstream without an artificial upgrade requirement.
			if res.StatusCode != 202 || res.Header.Get("X-Upstream") != "yes" {
				t.Fatalf("GET %s was not forwarded", p)
			}
		})
	}
}

func TestNoRetryWhenUpstreamClosesConnection(t *testing.T) {
	// Given an upstream that closes before sending headers.
	var calls atomic.Int32
	r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}))
	// When an idempotent GET fails on the wire.
	res, err := r.Client().Get(r.URL + "/v1/models?key=synthetic")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	// Then the relay returns one failure rather than replaying the request.
	if res.StatusCode != 502 || calls.Load() != 1 {
		t.Fatalf("status=%d attempts=%d", res.StatusCode, calls.Load())
	}
}

func TestNoRetryWhenUpstreamReturnsFailure(t *testing.T) {
	// Given an upstream returning a complete error response.
	var calls atomic.Int32
	r := testRelay(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		if _, err := io.WriteString(w, "unavailable"); err != nil {
			t.Error(err)
		}
	}))
	// When a request with a replay hint fails.
	req, err := http.NewRequest(http.MethodPost, r.URL+"/v1/responses", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", "synthetic")
	res, err := r.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	// Then the original status is returned after exactly one attempt.
	if res.StatusCode != 503 || calls.Load() != 1 {
		t.Fatalf("status=%d attempts=%d", res.StatusCode, calls.Load())
	}
}
