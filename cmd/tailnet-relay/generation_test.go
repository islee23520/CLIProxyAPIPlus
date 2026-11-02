package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestGenerationConcurrentSwitchAndClose(t *testing.T) {
	// Given an acquired old request and competing new requests and switches.
	const requests = 32
	entered := make(chan struct{}, requests+1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	defer releaseOnce.Do(func() { close(release) })
	c, relay := testGenerationController(t, upstream.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results := make(chan error, requests+1)
	request := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, relay.URL+"/v1/models", nil)
		if err != nil {
			results <- err
			return
		}
		res, err := relay.Client().Do(req)
		if err != nil {
			results <- err
			return
		}
		_, errRead := io.Copy(io.Discard, res.Body)
		errClose := res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			results <- errors.New("request reached a closed controller")
			return
		}
		results <- errors.Join(errRead, errClose)
	}
	go request()
	signal(t, entered)
	firstDrain, err := c.Switch(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	drains := make(chan (<-chan struct{}), requests)
	switchErrors := make(chan error, requests)
	for range requests {
		go func() { <-start; request() }()
		go func() {
			<-start
			drained, err := c.Switch(upstream.URL)
			drains <- drained
			switchErrors <- err
		}()
	}
	// When acquisition and publication contend, none can retire a held request.
	close(start)
	for range requests {
		signal(t, entered)
	}
	retired := []<-chan struct{}{firstDrain}
	for range requests {
		select {
		case err := <-switchErrors:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("switch failed to finish")
		}
		retired = append(retired, <-drains)
	}
	assertNotDrained(t, firstDrain)
	done := c.Close()
	if c.Close() != done {
		t.Fatal("Close returned a different completion signal")
	}
	assertNotDrained(t, done)
	// Then closure includes previously retired generations, without cancellation.
	releaseOnce.Do(func() { close(release) })
	for range requests + 1 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("request failed to complete")
		}
	}
	for _, drained := range retired {
		signal(t, drained)
	}
	signal(t, done)
	if c.Close() != done {
		t.Fatal("completed Close is not idempotent")
	}
}

func TestGenerationSwitchRacesConcurrentClose(t *testing.T) {
	// Given an acquired request in an already retired generation.
	c, err := newGenerationController("http://127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}
	entered, release, requestDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	old := c.current
	old.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	w := httptest.NewRecorder()
	go func() {
		defer close(requestDone)
		c.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	}()
	signal(t, entered)
	firstDrain, err := c.Switch("http://127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}
	assertNotDrained(t, firstDrain)

	// When switches and many Close callers leave the same channel barrier.
	const callers = 32
	start, ready := make(chan struct{}), make(chan struct{}, 2*callers)
	type switchResult struct {
		drained <-chan struct{}
		err     error
	}
	switches := make(chan switchResult, callers)
	closes := make(chan (<-chan struct{}), callers)
	for range callers {
		go func() {
			ready <- struct{}{}
			<-start
			drained, err := c.Switch("http://127.0.0.1:12345")
			switches <- switchResult{drained, err}
		}()
		go func() {
			ready <- struct{}{}
			<-start
			closes <- c.Close()
		}()
	}
	for range 2 * callers {
		signal(t, ready)
	}
	close(start)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for range callers {
		select {
		case done := <-closes:
			if done != c.done {
				t.Fatal("concurrent Close returned a different completion signal")
			}
		case <-ctx.Done():
			t.Fatal("concurrent Close failed to return")
		}
		select {
		case result := <-switches:
			if result.err == nil {
				// Every replacement after the held generation has no requests.
				select {
				case <-result.drained:
				default:
					t.Fatal("successfully replaced idle generation did not drain")
				}
			} else if !errors.Is(result.err, errGenerationControllerClosed) || result.drained != nil {
				t.Fatalf("unexpected concurrent switch result: %v", result.err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent Switch failed to return")
		}
	}

	// Then closure cannot revive the controller or finish the held request.
	assertNotDrained(t, firstDrain)
	assertNotDrained(t, c.done)
	c.mu.Lock()
	closedWithHeldRequest := c.current == nil && c.live == 1 && old.retired && old.inFlight == 1
	c.mu.Unlock()
	if !closedWithHeldRequest {
		t.Fatal("concurrent operations revived the controller or lost the held generation")
	}
	if drained, err := c.Switch("http://127.0.0.1:12345"); !errors.Is(err, errGenerationControllerClosed) || drained != nil {
		t.Fatalf("switch after concurrent close: %v", err)
	}
	rejected := httptest.NewRecorder()
	c.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("request after concurrent close: %d", rejected.Code)
	}
	releaseOnce.Do(func() { close(release) })
	signal(t, requestDone)
	if w.Code != http.StatusNoContent {
		t.Fatalf("held request status: %d", w.Code)
	}
	signal(t, firstDrain)
	signal(t, c.done)
	if c.Close() != c.done {
		t.Fatal("completed Close returned a different completion signal")
	}
}

func TestGenerationInvalidSwitchAndClosedController(t *testing.T) {
	// Given a valid idle controller, malformed replacements cannot publish.
	if _, err := newGenerationController("not-an-origin"); err == nil {
		t.Fatal("accepted invalid initial backend")
	}
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer origin.Close()
	c, relay := testGenerationController(t, origin.URL)
	for _, invalid := range []string{"", "http://user:secret@example.test", origin.URL + "/path", origin.URL + "?query=x"} {
		if drained, err := c.Switch(invalid); err == nil || drained != nil {
			t.Fatal("accepted invalid replacement")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, relay.URL+"/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := relay.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("invalid switch altered current backend: %d", res.StatusCode)
	}
	// Wait for the response handler, not merely client-side body completion.
	drained, err := c.Switch(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	signal(t, drained)
	// When an unused generation is replaced, its drain completes synchronously.
	drained, err = c.Switch(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-drained:
	default:
		t.Fatal("idle generation did not drain")
	}
	done := c.Close()
	select {
	case <-done:
	default:
		t.Fatal("idle controller did not drain")
	}
	// Then new acquisitions and further switches are rejected.
	if drained, err := c.Switch(origin.URL); !errors.Is(err, errGenerationControllerClosed) || drained != nil {
		t.Fatalf("switch after close: %v", err)
	}
	w := httptest.NewRecorder()
	c.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("request after close: %d", w.Code)
	}
	if c.Close() != done {
		t.Fatal("Close is not idempotent")
	}
}
