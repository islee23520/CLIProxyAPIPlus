package cliproxy

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type readinessTokenProvider struct{}

func (readinessTokenProvider) Load(context.Context, *config.Config) (*TokenClientResult, error) {
	return &TokenClientResult{}, nil
}

type readinessAPIKeyProvider struct{}

func (readinessAPIKeyProvider) Load(context.Context, *config.Config) (*APIKeyClientResult, error) {
	return &APIKeyClientResult{}, nil
}

func runReadinessSubprocess(t *testing.T) bool {
	t.Helper()
	const marker = "CLIPROXY_READINESS_TEST"
	if os.Getenv(marker) == t.Name() {
		return false
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.count=1")
	command.Dir = t.TempDir()
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if strings.EqualFold(key, marker) || strings.EqualFold(key, "MANAGEMENT_PASSWORD") {
			continue
		}
		command.Env = append(command.Env, value)
	}
	command.Env = append(command.Env, marker+"="+t.Name(), "MANAGEMENT_PASSWORD=")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("readiness subprocess failed: %v (context: %v)\n%s", err, ctx.Err(), output)
	}
	return true
}

func newReadinessService(t *testing.T) *Service {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("MANAGEMENT_PASSWORD", "")
	return &Service{
		cfg: &config.Config{
			Host:           "127.0.0.1",
			Port:           0,
			AuthDir:        filepath.Join(dir, "auths"),
			CommercialMode: true,
			RemoteManagement: config.RemoteManagement{
				DisableControlPanel:    true,
				DisableAutoUpdatePanel: true,
			},
		},
		configPath:     filepath.Join(dir, "config.yaml"),
		tokenProvider:  readinessTokenProvider{},
		apiKeyProvider: readinessAPIKeyProvider{},
		authManager:    &sdkAuth.Manager{},
	}
}

func awaitReadinessRun(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not finish")
		return nil
	}
}

func TestServiceRunStartupFailureSkipsHookAndWatcher(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	for _, name := range []string{"occupied port", "invalid TLS"} {
		t.Run(name, func(t *testing.T) {
			s := newReadinessService(t)
			if name == "occupied port" {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if errClose := listener.Close(); errClose != nil {
						t.Error(errClose)
					}
				})
				s.cfg.Port = listener.Addr().(*net.TCPAddr).Port
			} else {
				s.cfg.TLS.Enable = true
				s.cfg.TLS.Cert = filepath.Join(t.TempDir(), "missing.pem")
				s.cfg.TLS.Key = filepath.Join(t.TempDir(), "missing.key")
			}
			hooks, watchers := 0, 0
			s.hooks.OnAfterStart = func(*Service) { hooks++ }
			s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
				watchers++
				return &WatcherWrapper{}, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- s.Run(ctx) }()
			err := awaitReadinessRun(t, result)
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run error = %v, want startup failure", err)
			}
			if hooks != 0 || watchers != 0 {
				t.Fatalf("hook calls = %d, watcher creations = %d", hooks, watchers)
			}
		})
	}
}

func TestServiceRunReadyThenCancelCleansUp(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	s := newReadinessService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hooks := 0
	started := make(chan struct{})
	stopped := make(chan struct{})
	s.hooks.OnAfterStart = func(*Service) { hooks++ }
	s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
		if hooks != 1 {
			t.Errorf("hook calls before watcher = %d, want 1", hooks)
		}
		return &WatcherWrapper{
			start: func(context.Context) error { close(started); return nil },
			stop:  func() error { close(stopped); return nil },
		}, nil
	}
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("Run returned before watcher startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not start")
	}
	cancel()
	if err := awaitReadinessRun(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
	if hooks != 1 {
		t.Fatalf("hook calls = %d, want 1", hooks)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("watcher was not stopped before Run returned")
	}
	if err := awaitReadinessRun(t, s.serverErr); err != nil {
		t.Fatalf("server shutdown error = %v", err)
	}
}

func TestServiceRunHookCancellationSkipsWatcher(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			s := newReadinessService(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks, watchers := 0, 0
			s.hooks.OnBeforeStart = func(*config.Config) {
				if stage == "before" {
					cancel()
				}
			}
			s.hooks.OnAfterStart = func(*Service) { hooks++; cancel() }
			s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
				watchers++
				return &WatcherWrapper{}, nil
			}
			result := make(chan error, 1)
			go func() { result <- s.Run(ctx) }()
			if err := awaitReadinessRun(t, result); !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error = %v, want context.Canceled", err)
			}
			wantHooks := 0
			if stage == "after" {
				wantHooks = 1
			}
			if hooks != wantHooks || watchers != 0 {
				t.Fatalf("hook calls = %d, watcher creations = %d", hooks, watchers)
			}
		})
	}
}

func TestServiceRunSynchronousHookShutdownSkipsWatcher(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	s := newReadinessService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hooks, watchers := 0, 0
	s.hooks.OnAfterStart = func(service *Service) {
		hooks++
		if err := service.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}
	s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
		watchers++
		return &WatcherWrapper{}, nil
	}
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	if err := awaitReadinessRun(t, result); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if hooks != 1 || watchers != 0 {
		t.Fatalf("hook calls = %d, watcher creations = %d", hooks, watchers)
	}
}

func TestServiceRunShutdownDuringWatcherFactorySkipsStart(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	s := newReadinessService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	starts := 0
	s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &WatcherWrapper{start: func(context.Context) error { starts++; return nil }}, nil
	}
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("watcher factory was not entered")
	}
	errShutdown := s.Shutdown(ctx)
	close(release)
	if err := awaitReadinessRun(t, result); err != nil {
		t.Fatalf("Run error = %v", err)
	}
	if errShutdown != nil {
		t.Fatalf("Shutdown error = %v", errShutdown)
	}
	if starts != 0 || s.watcher != nil || s.authQueueStop != nil {
		t.Fatalf("resources started after Shutdown: watcher starts = %d, watcher = %v, auth queue active = %t", starts, s.watcher, s.authQueueStop != nil)
	}
}

func TestServiceShutdownPreservesFailure(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	s := newReadinessService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	want := errors.New("watcher shutdown failed")
	stops := 0
	s.hooks.OnAfterStart = func(service *Service) {
		service.watcher = &WatcherWrapper{stop: func() error { stops++; return want }}
		for range 2 {
			if err := service.Shutdown(ctx); !errors.Is(err, want) {
				t.Errorf("Shutdown error = %v, want %v", err, want)
			}
		}
	}
	watchers := 0
	s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
		watchers++
		return &WatcherWrapper{}, nil
	}
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	awaitReadinessRun(t, result)
	if stops != 1 || watchers != 0 {
		t.Fatalf("stop calls = %d, watcher creations = %d", stops, watchers)
	}
}

func TestServiceRunShutdownAfterWatcherPublicationSkipsStart(t *testing.T) {
	if runReadinessSubprocess(t) {
		return
	}
	s := newReadinessService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	configured := make(chan struct{})
	release := make(chan struct{})
	starts := 0
	s.watcherFactory = func(string, string, func(*config.Config)) (*WatcherWrapper, error) {
		return &WatcherWrapper{
			setConfig: func(*config.Config) {
				close(configured)
				select {
				case <-release:
				case <-ctx.Done():
				}
			},
			start: func(context.Context) error { starts++; return nil },
		}, nil
	}
	result := make(chan error, 1)
	go func() { result <- s.Run(ctx) }()
	select {
	case <-configured:
	case <-ctx.Done():
		t.Fatal("watcher configuration was not entered")
	}
	errShutdown := s.Shutdown(ctx)
	close(release)
	awaitReadinessRun(t, result)
	if errShutdown != nil {
		t.Fatalf("Shutdown error = %v", errShutdown)
	}
	if starts != 0 {
		t.Fatalf("watcher starts after Shutdown = %d, want 0", starts)
	}
}
