package main

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
)

var errGenerationControllerClosed = errors.New("relay generation controller is closed")

// Construct generationController with newGenerationController; it owns every
// handler and transport it creates.
// A request belongs to the generation current at acquisition, not at accept.
type generationController struct {
	mu      sync.Mutex
	current *relayGeneration
	live    int
	done    chan struct{}
}

type relayGeneration struct {
	handler   http.Handler
	transport *http.Transport
	inFlight  int
	retired   bool
	drained   chan struct{}
}

func newRelayGeneration(origin string) (*relayGeneration, error) {
	handler, transport, err := newRelay(origin)
	if err != nil {
		return nil, fmt.Errorf("create relay generation: %w", err)
	}
	return &relayGeneration{handler: handler, transport: transport, drained: make(chan struct{})}, nil
}

func newGenerationController(origin string) (*generationController, error) {
	generation, err := newRelayGeneration(origin)
	if err != nil {
		return nil, err
	}
	return &generationController{current: generation, live: 1, done: make(chan struct{})}, nil
}

func (c *generationController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	generation := c.current
	if generation == nil {
		c.mu.Unlock()
		http.Error(w, "relay unavailable", http.StatusServiceUnavailable)
		return
	}
	generation.inFlight++
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		generation.inFlight--
		if generation.retired && generation.inFlight == 0 {
			c.finishLocked(generation)
		}
	}()
	// ReverseProxy keeps ServeHTTP active for SSE and upgraded WebSockets.
	// Do not wrap ResponseWriter or release on headers, hijack, or cancellation.
	generation.handler.ServeHTTP(w, r)
}

// Switch validates a fresh fixed backend before atomically publishing it.
// Failure leaves the current generation untouched. The returned signal means
// the replaced handler has no requests left and its idle transport is closed.
func (c *generationController) Switch(origin string) (<-chan struct{}, error) {
	next, err := newRelayGeneration(origin)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil {
		next.transport.CloseIdleConnections()
		return nil, errGenerationControllerClosed
	}
	previous := c.current
	c.current = next
	c.live++
	previous.retired = true
	if previous.inFlight == 0 {
		c.finishLocked(previous)
	}
	return previous.drained, nil
}

// Close rejects new requests without cancelling existing streams. Its stable
// signal closes after ALL generations, including earlier switches, drain.
// It does not wait, start workers, or impose a stream deadline.
func (c *generationController) Close() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil {
		previous := c.current
		c.current = nil
		previous.retired = true
		if previous.inFlight == 0 {
			c.finishLocked(previous)
		}
	}
	return c.done
}

// Called exactly once per retired generation, under mu, at zero in-flight.
func (c *generationController) finishLocked(generation *relayGeneration) {
	generation.transport.CloseIdleConnections()
	close(generation.drained)
	c.live--
	if c.current == nil && c.live == 0 {
		close(c.done)
	}
}
