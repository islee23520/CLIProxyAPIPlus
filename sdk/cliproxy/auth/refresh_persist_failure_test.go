package auth

import (
	"context"
	"errors"
	"testing"
)

type refreshPersistFailureStore struct {
	persisted *Auth
	attempts  []*Auth
	saveErr   error
}

func (s *refreshPersistFailureStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.attempts = append(s.attempts, auth.Clone())
	return "", s.saveErr
}

func (s *refreshPersistFailureStore) List(context.Context) ([]*Auth, error) {
	return []*Auth{s.persisted.Clone()}, nil
}

func (s *refreshPersistFailureStore) Delete(context.Context, string) error { return nil }

type refreshPersistFailureExecutor struct {
	schedulerTestExecutor
	refreshCalls int
}

func (e *refreshPersistFailureExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	e.refreshCalls++
	updated := auth.Clone()
	updated.Metadata["access_token"] = "new-access"
	updated.Metadata["refresh_token"] = "new-refresh"
	return updated, nil
}

// TestRefreshPersistFailureReturnsNewTokensButReloadsOldTokens characterizes an
// unsafe current behavior: a failed durable save is silently reported as success.
// The nil-error assertion records this defect; it neither fixes nor endorses it.
func TestRefreshPersistFailureReturnsNewTokensButReloadsOldTokens(t *testing.T) {
	// Given an isolated persisted snapshot and a store that rejects every save.
	ctx := context.Background()
	initial := &Auth{
		ID:       "refresh-persist-failure",
		Provider: "test",
		Metadata: map[string]any{
			"access_token":  "old-access",
			"refresh_token": "old-refresh",
		},
	}
	store := &refreshPersistFailureStore{
		persisted: initial.Clone(),
		saveErr:   errors.New("synthetic refresh persistence failure"),
	}
	executor := &refreshPersistFailureExecutor{}
	manager := NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	if _, err := manager.Register(WithSkipPersist(ctx), initial); err != nil {
		t.Fatalf("Register error = %v", err)
	}
	if got := len(store.attempts); got != 0 {
		t.Fatalf("Save calls after skipped registration = %d, want 0", got)
	}

	// When one request refresh rotates both tokens but persistence fails.
	refreshed, err := manager.refreshAuthForRequest(ctx, initial.ID, "old-access")

	// Then the caller receives new tokens and nil error despite the rejected save.
	if err != nil {
		t.Fatalf("refreshAuthForRequest error = %v, want current silent nil error", err)
	}
	if refreshed == nil {
		t.Fatal("refreshAuthForRequest returned nil auth")
	}
	if executor.refreshCalls != 1 {
		t.Fatalf("Refresh calls = %d, want 1", executor.refreshCalls)
	}
	if got := len(store.attempts); got != 1 {
		t.Fatalf("Save calls = %d, want 1", got)
	}
	for _, auth := range []*Auth{refreshed, store.attempts[0]} {
		if got := auth.Metadata["access_token"]; got != "new-access" {
			t.Fatalf("returned or attempted access_token = %v, want new-access", got)
		}
		if got := auth.Metadata["refresh_token"]; got != "new-refresh" {
			t.Fatalf("returned or attempted refresh_token = %v, want new-refresh", got)
		}
	}
	runtimeAuth, ok := manager.GetByID(initial.ID)
	if !ok || runtimeAuth == nil {
		t.Fatal("runtime auth missing after refresh")
	}
	if got := runtimeAuth.Metadata["access_token"]; got != "new-access" {
		t.Fatalf("runtime access_token = %v, want new-access", got)
	}
	if got := runtimeAuth.Metadata["refresh_token"]; got != "new-refresh" {
		t.Fatalf("runtime refresh_token = %v, want new-refresh", got)
	}

	// Then a fresh manager reloads only old tokens: rotation was not durable.
	reloaded := NewManager(store, nil, nil)
	if errLoad := reloaded.Load(ctx); errLoad != nil {
		t.Fatalf("Load error = %v", errLoad)
	}
	persistedAuth, ok := reloaded.GetByID(initial.ID)
	if !ok || persistedAuth == nil {
		t.Fatal("persisted auth missing after reload")
	}
	if got := persistedAuth.Metadata["access_token"]; got != "old-access" {
		t.Fatalf("reloaded access_token = %v, want old-access", got)
	}
	if got := persistedAuth.Metadata["refresh_token"]; got != "old-refresh" {
		t.Fatalf("reloaded refresh_token = %v, want old-refresh", got)
	}
}
