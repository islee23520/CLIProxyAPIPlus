package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"

	log "github.com/sirupsen/logrus"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// errAuthDisabledInFlight is the cancellation cause attached to upstream attempts
// whose credential was disabled while the attempt was still running.
var errAuthDisabledInFlight = &Error{
	Code:       "auth_disabled",
	Message:    "credential was disabled while the request was in flight",
	Retryable:  true,
	HTTPStatus: http.StatusServiceUnavailable,
}

// inflightAuthAttempts tracks cancel functions of running upstream attempts per auth ID.
type inflightAuthAttempts struct {
	mu     sync.Mutex
	nextID uint64
	byAuth map[string]map[uint64]context.CancelCauseFunc
}

func (m *Manager) disabledAuthRerouteEnabled() bool {
	cfg := m.runtimeConfigSnapshot()
	return cfg != nil && cfg.Routing.DisabledAuthReroute
}

// bindAttemptToAuth derives an attempt context that is cancelled with
// errAuthDisabledInFlight when authID gets disabled. The returned release
// function unregisters the attempt; it is also unregistered when ctx ends.
func (m *Manager) bindAttemptToAuth(ctx context.Context, authID string) (context.Context, func()) {
	if m == nil || ctx == nil || authID == "" || !m.disabledAuthRerouteEnabled() {
		return ctx, func() {}
	}
	attemptCtx, cancel := context.WithCancelCause(ctx)
	reg := &m.inflightAttempts
	reg.mu.Lock()
	if reg.byAuth == nil {
		reg.byAuth = make(map[string]map[uint64]context.CancelCauseFunc)
	}
	reg.nextID++
	id := reg.nextID
	if reg.byAuth[authID] == nil {
		reg.byAuth[authID] = make(map[uint64]context.CancelCauseFunc)
	}
	reg.byAuth[authID][id] = cancel
	reg.mu.Unlock()

	// The credential may have been disabled between selection and registration.
	m.mu.RLock()
	current := m.auths[authID]
	m.mu.RUnlock()
	if authIsDisabled(current) {
		cancel(errAuthDisabledInFlight)
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			reg.mu.Lock()
			if attempts := reg.byAuth[authID]; attempts != nil {
				delete(attempts, id)
				if len(attempts) == 0 {
					delete(reg.byAuth, authID)
				}
			}
			reg.mu.Unlock()
		})
	}
	context.AfterFunc(attemptCtx, release)
	return attemptCtx, release
}

// cancelInflightForAuth aborts every running attempt bound to authID.
func (m *Manager) cancelInflightForAuth(authID string) int {
	reg := &m.inflightAttempts
	reg.mu.Lock()
	attempts := reg.byAuth[authID]
	delete(reg.byAuth, authID)
	reg.mu.Unlock()
	for _, cancel := range attempts {
		cancel(errAuthDisabledInFlight)
	}
	return len(attempts)
}

// onAuthDisabledTransition drops session bindings and in-flight attempts of a
// credential that just moved from enabled to disabled, so sessions round-robin
// onto the remaining credentials.
func (m *Manager) onAuthDisabledTransition(authID string) {
	if m == nil || authID == "" || !m.disabledAuthRerouteEnabled() {
		return
	}
	m.invalidateSessionAffinity(authID)
	cancelled := m.cancelInflightForAuth(authID)
	log.Infof("disabled-auth-reroute: auth %s disabled; session bindings dropped, %d in-flight attempt(s) cancelled", authID, cancelled)
}

// attemptCancelledByDisable reports whether attemptCtx was cancelled because its
// credential was disabled while the client request itself is still alive.
func attemptCancelledByDisable(parent, attemptCtx context.Context) bool {
	if attemptCtx == nil || (parent != nil && parent.Err() != nil) {
		return false
	}
	return errors.Is(context.Cause(attemptCtx), errAuthDisabledInFlight)
}

func authIsDisabled(a *Auth) bool {
	return a != nil && (a.Disabled || a.Status == StatusDisabled)
}

// guardStreamForDisable forwards a started stream and, if the credential is
// disabled mid-stream, terminates it with a retryable auth_disabled error chunk
// instead of a silently truncated stream. It releases the attempt registration
// when the stream ends.
func guardStreamForDisable(parent, attemptCtx context.Context, result *cliproxyexecutor.StreamResult, release func()) *cliproxyexecutor.StreamResult {
	if result == nil || result.Chunks == nil || attemptCtx == parent {
		release()
		return result
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	in := result.Chunks
	go func() {
		defer close(out)
		defer release()
		for chunk := range in {
			if attemptCancelledByDisable(parent, attemptCtx) {
				break
			}
			select {
			case out <- chunk:
			case <-parent.Done():
				discardStreamChunks(in)
				return
			}
		}
		if attemptCancelledByDisable(parent, attemptCtx) {
			discardStreamChunks(in)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: errAuthDisabledInFlight}:
			case <-parent.Done():
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}
}
