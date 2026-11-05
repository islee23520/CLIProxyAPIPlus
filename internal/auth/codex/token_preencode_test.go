package codex

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/misc"
)

type preencodeTokenMarshaler struct {
	err   error
	calls *int
}

func (m preencodeTokenMarshaler) MarshalJSON() ([]byte, error) {
	(*m.calls)++
	if m.err != nil {
		return nil, m.err
	}
	return []byte(`"synthetic"`), nil
}

func TestSaveTokenToFile_PreencodeUnsupportedMetadataPreservesDestination(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan int)},
		{name: "function", value: func() {}},
	} {
		for _, existing := range []bool{false, true} {
			name := tc.name + "/absent"
			if existing {
				name = tc.name + "/existing"
			}
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "nested", "token.json")
				sentinel := []byte(`{"sentinel":"existing synthetic token"}`)
				if existing {
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, sentinel, 0600); err != nil {
						t.Fatal(err)
					}
				}
				ts := &CodexTokenStorage{Metadata: map[string]any{"unsupported": tc.value}}
				err := ts.SaveTokenToFile(path)
				var unsupported *json.UnsupportedTypeError
				if !errors.As(err, &unsupported) {
					t.Fatalf("expected UnsupportedTypeError, got %T", err)
				}
				if ts.Type != "codex" {
					t.Fatal("failed save did not set Codex type")
				}
				got, errRead := os.ReadFile(path)
				if !existing {
					if !errors.Is(errRead, os.ErrNotExist) {
						t.Fatalf("expected absent destination, got %v", errRead)
					}
					return
				}
				if errRead != nil {
					t.Fatal(errRead)
				}
				if !bytes.Equal(got, sentinel) {
					t.Fatal("serialization failure changed existing file")
				}
			})
		}
	}
}

func TestSaveTokenToFile_PreencodeMarshalerErrorPreservesDestination(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token.json")
	sentinel := []byte(`{"sentinel":"existing synthetic token"}`)
	if err := os.WriteFile(path, sentinel, 0600); err != nil {
		t.Fatal(err)
	}
	errSentinel := errors.New("synthetic marshaler failure")
	calls := 0
	ts := &CodexTokenStorage{Metadata: map[string]any{"custom": preencodeTokenMarshaler{err: errSentinel, calls: &calls}}}
	if err := ts.SaveTokenToFile(path); !errors.Is(err, errSentinel) {
		t.Fatalf("expected wrapped marshaler sentinel, got %T", err)
	}
	if calls != 1 {
		t.Fatalf("MarshalJSON called %d times, want 1", calls)
	}
	if ts.Type != "codex" {
		t.Fatal("failed save did not set Codex type")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Fatal("marshaler failure changed existing file")
	}
}

func TestSaveTokenToFile_WritesMarshalOutputAndPreservesInvalidDestination(t *testing.T) {
	t.Run("new file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token.json")
		calls := 0
		ts := &CodexTokenStorage{
			AccessToken: "synthetic-access",
			Type:        "codex",
			Metadata: map[string]any{
				"custom": preencodeTokenMarshaler{calls: &calls},
				"html":   "<tag>&value</tag>",
				"nested": map[string]any{"count": 7, "enabled": true},
			},
		}
		data, err := misc.MergeMetadata(ts, ts.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		calls = 0
		ts.Type = "stale"
		if err := ts.SaveTokenToFile(path); err != nil {
			t.Fatal(err)
		}
		if calls != 1 {
			t.Fatalf("MarshalJSON called %d times, want 1", calls)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("saved bytes differ from json.Marshal output")
		}
	})
	t.Run("invalid existing file is preserved", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "token.json")
		original := bytes.Repeat([]byte("x"), 4096)
		if err := os.WriteFile(path, original, 0600); err != nil {
			t.Fatal(err)
		}
		ts := &CodexTokenStorage{AccessToken: "synthetic-access"}
		if err := ts.SaveTokenToFile(path); err == nil {
			t.Fatal("expected an error for an invalid existing credential file")
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, original) {
			t.Fatal("failed save changed the existing file")
		}
	})
}
