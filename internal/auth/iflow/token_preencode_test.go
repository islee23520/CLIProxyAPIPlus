package iflow

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
				sentinel := []byte("existing synthetic token sentinel\n")
				if existing {
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, sentinel, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				ts := &IFlowTokenStorage{Type: "stale", Metadata: map[string]any{"unsupported": tc.value}}
				err := ts.SaveTokenToFile(path)
				var unsupported *json.UnsupportedTypeError
				if !errors.As(err, &unsupported) {
					t.Fatalf("expected UnsupportedTypeError, got %T", err)
				}
				if ts.Type != "iflow" {
					t.Fatal("failed save did not set iFlow type")
				}
				if info, err := os.Stat(filepath.Dir(path)); err != nil || !info.IsDir() {
					t.Fatalf("expected destination directory after failed save, got %v", err)
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
	for _, existing := range []bool{false, true} {
		name := "absent"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token.json")
			sentinel := []byte("existing synthetic token sentinel\n")
			if existing {
				if err := os.WriteFile(path, sentinel, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			errSentinel := errors.New("synthetic marshaler failure")
			calls := 0
			ts := &IFlowTokenStorage{Type: "stale", Metadata: map[string]any{"custom": preencodeTokenMarshaler{err: errSentinel, calls: &calls}}}
			if err := ts.SaveTokenToFile(path); !errors.Is(err, errSentinel) {
				t.Fatalf("expected wrapped marshaler sentinel, got %T", err)
			}
			if calls != 1 {
				t.Fatalf("MarshalJSON called %d times, want 1", calls)
			}
			if ts.Type != "iflow" {
				t.Fatal("failed save did not set iFlow type")
			}
			got, err := os.ReadFile(path)
			if !existing {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("expected absent destination, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, sentinel) {
				t.Fatal("marshaler failure changed existing file")
			}
		})
	}
}

func TestSaveTokenToFile_PreencodePreservesEncoderOutputAndFileIdentity(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new file"
		if existing {
			name = "shorter overwrite"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token.json")
			var before os.FileInfo
			if existing {
				if err := os.WriteFile(path, bytes.Repeat([]byte("x"), 4096), 0o600); err != nil {
					t.Fatal(err)
				}
				var err error
				before, err = os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			ts := &IFlowTokenStorage{
				AccessToken: "synthetic-access",
				Type:        "iflow",
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
			var want bytes.Buffer
			if err := json.NewEncoder(&want).Encode(data); err != nil {
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
			if !bytes.Equal(got, want.Bytes()) {
				t.Fatal("saved bytes differ from json.Encoder output")
			}
			if !bytes.HasSuffix(got, []byte("\n")) || !bytes.Contains(got, []byte(`\u003ctag\u003e\u0026`)) {
				t.Fatal("expected Encoder newline and HTML escaping")
			}
			if existing {
				after, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if !os.SameFile(before, after) {
					t.Fatal("save replaced existing file identity")
				}
				if after.Size() >= before.Size() {
					t.Fatal("fixture did not exercise shorter overwrite")
				}
			}
		})
	}
}
