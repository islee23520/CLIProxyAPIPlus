package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

// TestFileTokenStoreAntigravityKnownUnsafeStaleWriteCharacterization records a
// known unsafe behavior, not a normative regression expectation. A production
// fix should invalidate this characterization rather than preserve token loss.
func TestFileTokenStoreAntigravityKnownUnsafeStaleWriteCharacterization(t *testing.T) {
	const childTag = "CLIPROXY_TEST_ANTIGRAVITY_STALE_WRITE_CHARACTERIZATION_CHILD"
	if os.Getenv(childTag) == "synthetic-only" {
		characterizeAntigravityStaleWrite(t)
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	executable, errExecutable := os.Executable()
	if errExecutable != nil {
		t.Fatalf("locate test binary: %v", errExecutable)
	}
	cmd := exec.CommandContext(ctx, executable,
		"-test.run=^TestFileTokenStoreAntigravityKnownUnsafeStaleWriteCharacterization$",
		"-test.timeout=20s")
	cmd.Env = append(os.Environ(), childTag+"=synthetic-only")
	// Only the tagged child changes http.DefaultClient; CombinedOutput also waits
	// for process exit, including on cancellation, before temporary files expire.
	output, errRun := cmd.CombinedOutput()
	if errRun != nil {
		t.Fatalf("synthetic characterization subprocess: %v (context: %v)\n%s", errRun, ctx.Err(), output)
	}
}

func characterizeAntigravityStaleWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "synthetic-antigravity.json")
	seed := []byte(`{"type":"antigravity","access_token":"synthetic-old-access","refresh_token":"synthetic-old-refresh"}`)
	if errWrite := os.WriteFile(path, seed, 0o600); errWrite != nil {
		t.Fatalf("seed synthetic auth file: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	result := make(chan error, 1)
	previousClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: antigravityCharacterizationTransport(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1internal:loadCodeAssist" || req.Header.Get("Authorization") != "Bearer synthetic-old-access" {
			return nil, fmt.Errorf("unexpected synthetic discovery request")
		}
		close(entered)
		select {
		case <-release:
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"cloudaicompanionProject":"synthetic-project"}`)),
				Request:    req,
			}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})}
	go func() {
		defer close(done)
		auths, errList := store.List(ctx)
		if errList == nil && len(auths) != 1 {
			errList = fmt.Errorf("List returned %d synthetic auth records, want 1", len(auths))
		}
		result <- errList
	}()
	defer func() {
		// Cancellation unblocks the fake even when an assertion aborts before
		// release. Join before restoring the global client or removing the file.
		cancel()
		<-done
		http.DefaultClient = previousClient
	}()

	select {
	case <-entered:
	case <-done:
		t.Fatalf("List finished before synthetic discovery barrier: %v", <-result)
	case <-ctx.Done():
		t.Fatalf("synthetic discovery entry: %v", ctx.Err())
	}

	savedPath, errSave := store.Save(ctx, &cliproxyauth.Auth{
		ID:       "synthetic-antigravity.json",
		Provider: "antigravity",
		FileName: "synthetic-antigravity.json",
		Metadata: map[string]any{
			"type":          "antigravity",
			"access_token":  "synthetic-new-access",
			"refresh_token": "synthetic-new-refresh",
		},
	})
	if errSave != nil {
		t.Fatalf("Save synthetic new tokens: %v", errSave)
	}
	if savedPath != path {
		t.Fatal("Save did not target the discovery file")
	}
	baseline, errBaseline := os.ReadFile(path)
	if errBaseline != nil {
		t.Fatalf("read Save baseline: %v", errBaseline)
	}
	var saved map[string]any
	if errDecode := json.Unmarshal(baseline, &saved); errDecode != nil {
		t.Fatalf("decode Save baseline: %v", errDecode)
	}
	if saved["access_token"] != "synthetic-new-access" || saved["refresh_token"] != "synthetic-new-refresh" {
		t.Fatal("Save baseline does not contain both synthetic new tokens")
	}
	if _, present := saved["project_id"]; present {
		t.Fatal("project discovery completed before barrier release")
	}

	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("synthetic discovery completion: %v", ctx.Err())
	}
	if errList := <-result; errList != nil {
		t.Fatalf("List synthetic auth: %v", errList)
	}
	final, errFinal := os.ReadFile(path)
	if errFinal != nil {
		t.Fatalf("read discovery result: %v", errFinal)
	}
	if bytes.Equal(final, baseline) {
		t.Fatal("known unsafe behavior not observed: Save baseline survived discovery")
	}
	var overwritten map[string]any
	if errDecode := json.Unmarshal(final, &overwritten); errDecode != nil {
		t.Fatalf("decode discovery result: %v", errDecode)
	}
	if overwritten["access_token"] != "synthetic-old-access" || overwritten["refresh_token"] != "synthetic-old-refresh" {
		t.Fatal("known unsafe behavior not observed: discovery did not restore both synthetic old tokens")
	}
	if overwritten["project_id"] != "synthetic-project" {
		t.Fatal("discovery result is missing the synthetic project_id")
	}
}

type antigravityCharacterizationTransport func(*http.Request) (*http.Response, error)

func (f antigravityCharacterizationTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
