package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

type persistenceLogStore struct {
	saveCalls int
	saveErr   error
}

func (s *persistenceLogStore) List(context.Context) ([]*Auth, error) { return nil, nil }
func (s *persistenceLogStore) Delete(context.Context, string) error  { return nil }
func (s *persistenceLogStore) Save(context.Context, *Auth) (string, error) {
	s.saveCalls++
	return "", s.saveErr
}

type persistenceLogLifecycleHook struct {
	NoopHook
	registered func(*Auth)
	updated    func(*Auth)
}

func (h persistenceLogLifecycleHook) OnAuthRegistered(_ context.Context, auth *Auth) {
	h.registered(auth)
}

func (h persistenceLogLifecycleHook) OnAuthUpdated(_ context.Context, auth *Auth) {
	h.updated(auth)
}

func TestManagerPersistenceFailureLog(t *testing.T) {
	// Isolate the standard logger from parallel package tests and their workers.
	const childEnv = "CLIPROXY_PERSISTENCE_LOG_TEST_CHILD"
	if os.Getenv(childEnv) != "1" {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagerPersistenceFailureLog$", "-test.count=1")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated persistence log test: %v\n%s", err, output)
		}
		return
	}

	logger := log.StandardLogger()
	originalHooks := logger.ReplaceHooks(make(log.LevelHooks))
	originalOutput, originalLevel := logger.Out, logger.GetLevel()
	logger.SetOutput(io.Discard)
	logger.SetLevel(log.DebugLevel)
	t.Cleanup(func() {
		logger.ReplaceHooks(originalHooks)
		logger.SetOutput(originalOutput)
		logger.SetLevel(originalLevel)
	})
	logs := logtest.NewGlobal()

	const (
		errorSentinel    = "sentinel-store-error-private"
		authSentinel     = "sentinel-auth-private"
		providerSentinel = "sentinel-provider-private"
		tokenSentinel    = "sentinel-token-private"
		metadataSentinel = "sentinel-metadata-private"
		filenameSentinel = "sentinel-filename-private"
	)
	for _, operation := range []string{"Register", "Update"} {
		for _, scenario := range []string{"failure", "success", "no_store", "skip_context", "config_api_key", "runtime_only", "plugin_virtual", "no_metadata"} {
			t.Run(operation+"/"+scenario, func(t *testing.T) {
				logs.Reset()
				auth := &Auth{
					ID:       authSentinel,
					Provider: providerSentinel,
					FileName: filenameSentinel,
					Label:    metadataSentinel,
					Status:   StatusActive,
					Metadata: map[string]any{
						"access_token":  tokenSentinel,
						"refresh_token": tokenSentinel,
						"private":       metadataSentinel,
					},
				}
				ctx := context.Background()
				store := &persistenceLogStore{saveErr: errors.New(errorSentinel)}
				var backend Store = store
				wantSaves, wantLogs := 0, 0
				switch scenario {
				case "failure":
					wantSaves, wantLogs = 1, 1
				case "success":
					store.saveErr = nil
					wantSaves = 1
				case "no_store":
					backend = nil
				case "skip_context":
					ctx = WithSkipPersist(ctx)
				case "config_api_key":
					auth.Attributes = map[string]string{"api_key": tokenSentinel, "source": "config:codex[synthetic]"}
				case "runtime_only":
					auth.Attributes = map[string]string{"runtime_only": "true"}
				case "plugin_virtual":
					MarkPluginVirtualAuth(auth, filenameSentinel, 0)
				case "no_metadata":
					auth.Metadata = nil
				}
				manager := NewManager(backend, nil, nil)
				if operation == "Update" {
					initial := auth.Clone()
					initial.Label = "initial-label"
					if _, err := manager.Register(WithSkipPersist(ctx), initial); err != nil {
						t.Fatalf("setup Register error = %v", err)
					}
				}
				registered, updated := 0, 0
				checkHook := func(snapshot *Auth) {
					if store.saveCalls != wantSaves || len(logs.AllEntries()) != wantLogs {
						t.Error("persistence and logging must precede the lifecycle hook")
					}
					runtimeAuth, ok := manager.GetByID(auth.ID)
					if !ok || !reflect.DeepEqual(runtimeAuth, snapshot) {
						t.Error("hook snapshot does not match runtime auth")
					}
				}
				manager.hook = persistenceLogLifecycleHook{
					registered: func(snapshot *Auth) { registered++; checkHook(snapshot) },
					updated:    func(snapshot *Auth) { updated++; checkHook(snapshot) },
				}
				var result *Auth
				var err error
				if operation == "Register" {
					result, err = manager.Register(ctx, auth)
				} else {
					result, err = manager.Update(ctx, auth)
				}
				if err != nil || result == nil || !reflect.DeepEqual(result, auth) {
					t.Fatalf("%s must retain the input auth and nil error: result=%v, error=%v", operation, result, err)
				}
				runtimeAuth, ok := manager.GetByID(auth.ID)
				if !ok || !reflect.DeepEqual(runtimeAuth, result) {
					t.Fatal("runtime auth must retain the returned state")
				}
				if operation == "Register" && (registered != 1 || updated != 0) || operation == "Update" && (registered != 0 || updated != 1) {
					t.Fatalf("lifecycle hook calls: registered=%d, updated=%d", registered, updated)
				}
				if store.saveCalls != wantSaves {
					t.Fatalf("Save calls = %d, want %d", store.saveCalls, wantSaves)
				}
				entries := logs.AllEntries()
				if len(entries) != wantLogs {
					t.Fatalf("log events = %d, want %d", len(entries), wantLogs)
				}
				for _, entry := range entries {
					// Upstream v8 (02548d67) logs persistence failures as warnings that
					// name the auth ID, provider and store error; credential material
					// and metadata must still never appear.
					if entry.Level != log.WarnLevel {
						t.Errorf("log level = %v, want Warn", entry.Level)
					}
					logged := entry.Message + fmt.Sprint(entry.Data)
					for _, secret := range []string{tokenSentinel, metadataSentinel} {
						if strings.Contains(logged, secret) {
							t.Error("persistence failure log contains synthetic private data")
						}
					}
				}
			})
		}
	}
}
