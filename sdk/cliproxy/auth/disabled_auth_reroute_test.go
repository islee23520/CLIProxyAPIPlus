package auth

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func newDisabledRerouteManager(t *testing.T, enabled bool, executor *claudeCancellationTestExecutor) (*Manager, string, *Auth, *Auth) {
	t.Helper()
	model := "claude-reroute-" + uuid.NewString()
	manager := NewManager(nil, nil, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{DisabledAuthReroute: enabled}})
	manager.RegisterExecutor(executor)
	newAuth := func(name string) *Auth {
		a := &Auth{
			ID:         name + "-" + uuid.NewString(),
			Provider:   "claude",
			Attributes: map[string]string{"auth_kind": "oauth"},
			Metadata:   map[string]any{"access_token": "token-" + name, "request_retry": float64(0)},
		}
		registry.GetGlobalRegistry().RegisterClient(a.ID, a.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
		if _, err := manager.Register(context.Background(), a); err != nil {
			t.Fatalf("Register(%s): %v", name, err)
		}
		return a
	}
	return manager, model, newAuth("a"), newAuth("b")
}

func disableAuth(t *testing.T, manager *Manager, authID string) {
	t.Helper()
	current, ok := manager.GetByID(authID)
	if !ok {
		t.Fatalf("auth %s missing", authID)
	}
	current.Disabled = true
	current.Status = StatusDisabled
	if _, err := manager.Update(context.Background(), current); err != nil {
		t.Fatalf("Update(disable): %v", err)
	}
}

func bindSession(t *testing.T, manager *Manager, model, sessionID string, auths ...*Auth) (*SessionAffinitySelector, string) {
	t.Helper()
	selector := NewSessionAffinitySelector(nil)
	t.Cleanup(selector.Stop)
	manager.SetSelector(selector)
	opts := cliproxyexecutor.Options{Headers: map[string][]string{"X-Claude-Code-Session-Id": {sessionID}}, Metadata: map[string]any{}}
	picked, err := selector.Pick(context.Background(), "claude", model, opts, auths)
	if err != nil || picked == nil {
		t.Fatalf("Pick: %v", err)
	}
	return selector, picked.ID
}

func TestDisabledAuthRerouteDropsSessionBinding(t *testing.T) {
	for _, tc := range []struct {
		name       string
		enabled    bool
		wantStatus string
	}{
		{name: "enabled", enabled: true, wantStatus: "unbound"},
		{name: "disabled option keeps legacy binding", enabled: false, wantStatus: "bound"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, model, a, b := newDisabledRerouteManager(t, tc.enabled, &claudeCancellationTestExecutor{})
			_, boundID := bindSession(t, manager, model, "sess-disable", a, b)
			disableAuth(t, manager, boundID)
			_, status := manager.LookupSessionAffinity("claude", model, "sess-disable")
			if status != tc.wantStatus {
				t.Fatalf("status after disable = %q, want %q", status, tc.wantStatus)
			}
		})
	}
}

func TestDisabledAuthRerouteCancelsInflightExecuteAndRetriesOtherAuth(t *testing.T) {
	started := make(chan string, 4)
	var calls atomic.Int32
	executor := &claudeCancellationTestExecutor{}
	executor.executeFn = func(ctx context.Context, auth *Auth) (cliproxyexecutor.Response, error) {
		if calls.Add(1) == 1 {
			started <- auth.ID
			<-ctx.Done()
			return cliproxyexecutor.Response{}, ctx.Err()
		}
		return cliproxyexecutor.Response{Payload: []byte("served-by:" + auth.ID)}, nil
	}
	manager, model, _, _ := newDisabledRerouteManager(t, true, executor)

	type outcome struct {
		resp cliproxyexecutor.Response
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		resp, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		done <- outcome{resp, err}
	}()
	firstID := <-started
	disableAuth(t, manager, firstID)

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Execute() error = %v, want reroute success", got.err)
		}
		if string(got.resp.Payload) == "served-by:"+firstID || len(got.resp.Payload) == 0 {
			t.Fatalf("response %q was not served by the other auth", got.resp.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request was not cancelled after disable")
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("execute calls = %d, want 2", n)
	}
	if current, _ := manager.GetByID(firstID); current == nil || !current.Disabled {
		t.Fatalf("disabled auth state changed unexpectedly: %+v", current)
	}
}

func TestDisabledAuthRerouteEndsRunningStreamWithRetryableError(t *testing.T) {
	executor := &claudeCancellationTestExecutor{}
	started := make(chan string, 1)
	executor.streamFn = func(ctx context.Context, auth *Auth) (*cliproxyexecutor.StreamResult, error) {
		chunks := make(chan cliproxyexecutor.StreamChunk)
		go func() {
			defer close(chunks)
			chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("first")}
			started <- auth.ID
			<-ctx.Done()
		}()
		return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
	}
	manager, model, _, _ := newDisabledRerouteManager(t, true, executor)

	result, err := manager.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	first := <-result.Chunks
	if string(first.Payload) != "first" {
		t.Fatalf("first chunk = %q", first.Payload)
	}
	disableAuth(t, manager, <-started)

	var lastErr error
	deadline := time.After(5 * time.Second)
	for open := true; open; {
		select {
		case chunk, ok := <-result.Chunks:
			if !ok {
				open = false
				break
			}
			if chunk.Err != nil {
				lastErr = chunk.Err
			}
		case <-deadline:
			t.Fatal("stream kept running after its auth was disabled")
		}
	}
	var authErr *Error
	if !errors.As(lastErr, &authErr) || authErr.Code != "auth_disabled" || !authErr.Retryable {
		t.Fatalf("stream terminal error = %v, want retryable auth_disabled", lastErr)
	}
}

func TestDisabledAuthRerouteOffLeavesInflightRunning(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	executor := &claudeCancellationTestExecutor{}
	executor.executeFn = func(ctx context.Context, auth *Auth) (cliproxyexecutor.Response, error) {
		started <- struct{}{}
		select {
		case <-release:
			return cliproxyexecutor.Response{Payload: []byte("served-by:" + auth.ID)}, nil
		case <-ctx.Done():
			return cliproxyexecutor.Response{}, ctx.Err()
		}
	}
	manager, model, a, b := newDisabledRerouteManager(t, false, executor)
	done := make(chan error, 1)
	go func() {
		_, err := manager.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		done <- err
	}()
	<-started
	disableAuth(t, manager, a.ID)
	disableAuth(t, manager, b.ID)
	select {
	case err := <-done:
		t.Fatalf("request ended early with option off: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}
