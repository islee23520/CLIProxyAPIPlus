package vertex

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/misc"
)

type failingCredentialMarshaler struct {
	calls int
	err   error
}

func (m *failingCredentialMarshaler) MarshalJSON() ([]byte, error) {
	m.calls++
	return nil, m.err
}

func TestVertexSaveTokenToFilePreservesDestinationOnUnsupportedValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan int)},
		{name: "function", value: func() {}},
	} {
		for _, existing := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/missing", true: "/existing"}[existing], func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "nested", "credential.json")
				original := []byte("existing synthetic credential bytes\n")
				if existing {
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, original, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				storage := &VertexCredentialStorage{
					ServiceAccount: map[string]any{"unsupported": tc.value},
					Type:           "previous",
				}
				err := storage.SaveTokenToFile(path)
				var unsupported *json.UnsupportedTypeError
				if !errors.As(err, &unsupported) {
					t.Fatalf("expected UnsupportedTypeError, got %v", err)
				}
				if unsupported.Type != reflect.TypeOf(tc.value) {
					t.Fatalf("unsupported type = %v, want %v", unsupported.Type, reflect.TypeOf(tc.value))
				}
				if storage.Type != "vertex" {
					t.Fatalf("provider type = %q, want vertex", storage.Type)
				}
				if info, errStat := os.Stat(filepath.Dir(path)); errStat != nil || !info.IsDir() {
					t.Fatalf("parent directory not created: %v", errStat)
				}
				got, errRead := os.ReadFile(path)
				if existing {
					if errRead != nil {
						t.Fatal(errRead)
					}
					if !bytes.Equal(got, original) {
						t.Fatalf("existing bytes changed: %q", got)
					}
				} else if !errors.Is(errRead, os.ErrNotExist) {
					t.Fatalf("destination should remain absent, got %v", errRead)
				}
			})
		}
	}
}

func TestVertexSaveTokenToFilePreservesDestinationOnMarshalerFailure(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "existing"}[existing], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credential.json")
			original := []byte("synthetic credential retained\n")
			if existing {
				if err := os.WriteFile(path, original, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			sentinel := errors.New("synthetic marshaler failure")
			marshaler := &failingCredentialMarshaler{err: sentinel}
			storage := &VertexCredentialStorage{ServiceAccount: map[string]any{"custom": marshaler}, Type: "previous"}
			if err := storage.SaveTokenToFile(path); !errors.Is(err, sentinel) {
				t.Fatalf("expected wrapped marshaler failure, got %v", err)
			}
			if marshaler.calls != 1 {
				t.Fatalf("MarshalJSON calls = %d, want 1", marshaler.calls)
			}
			if storage.Type != "vertex" {
				t.Fatalf("provider type = %q, want vertex", storage.Type)
			}
			got, errRead := os.ReadFile(path)
			if existing {
				if errRead != nil {
					t.Fatal(errRead)
				}
				if !bytes.Equal(got, original) {
					t.Fatalf("existing bytes changed: %q", got)
				}
			} else if !errors.Is(errRead, os.ErrNotExist) {
				t.Fatalf("destination should remain absent, got %v", errRead)
			}
		})
	}
}

func TestVertexSaveTokenToFileKeepsEncoderFormatAndTruncatesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credential.json")
	storage := &VertexCredentialStorage{
		ServiceAccount: map[string]any{
			"nested": map[string]any{"html": "<synthetic>&value", "items": []any{true, float64(3), nil}},
		},
		ProjectID: "synthetic-project",
		Email:     "synthetic@example.invalid",
		Location:  "synthetic-location",
		Prefix:    "synthetic-prefix",
		Type:      "previous",
	}
	expected := *storage
	expected.Type = "vertex"
	data, errMerge := misc.MergeMetadata(&expected, expected.Metadata)
	if errMerge != nil {
		t.Fatal(errMerge)
	}
	var want bytes.Buffer
	enc := json.NewEncoder(&want)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), want.Len()+100), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = storage.SaveTokenToFile(path); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("existing file was replaced")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("encoded bytes = %q, want %q", got, want.Bytes())
	}
}

func TestVertexSaveTokenToFileRejectsNilCredentials(t *testing.T) {
	for _, tc := range []struct {
		name    string
		storage *VertexCredentialStorage
	}{
		{name: "nil storage"},
		{name: "nil service account", storage: &VertexCredentialStorage{Type: "previous"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "credential.json")
			if err := tc.storage.SaveTokenToFile(path); err == nil {
				t.Fatal("expected nil credential rejection")
			}
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("parent should remain absent, got %v", err)
			}
			if tc.storage != nil && tc.storage.Type != "previous" {
				t.Fatalf("provider type changed before nil guard: %q", tc.storage.Type)
			}
		})
	}
}
