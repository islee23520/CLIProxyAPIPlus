package kimi

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
)

type kimiFailingJSONMarshaler struct {
	calls int
	err   error
}

func (m *kimiFailingJSONMarshaler) MarshalJSON() ([]byte, error) {
	m.calls++
	return nil, m.err
}

func TestSaveTokenToFile_UnsupportedMetadataPreservesFile(t *testing.T) {
	for _, value := range []struct {
		name string
		data any
	}{
		{name: "channel", data: make(chan int)},
		{name: "function", data: func() {}},
	} {
		for _, existing := range []bool{false, true} {
			name := "missing"
			if existing {
				name = "existing"
			}
			t.Run(value.name+"/"+name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "nested", "token.json")
				original := []byte("synthetic existing token contents\n")
				if existing {
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, original, 0600); err != nil {
						t.Fatal(err)
					}
				}
				storage := &KimiTokenStorage{Metadata: map[string]any{"unsupported": value.data}}
				err := storage.SaveTokenToFile(path)
				var unsupported *json.UnsupportedTypeError
				if !errors.As(err, &unsupported) || unsupported.Type != reflect.TypeOf(value.data) {
					t.Fatalf("expected unsupported metadata type error, got %v", err)
				}
				if storage.Type != "kimi" {
					t.Fatalf("type after failure = %q, want kimi", storage.Type)
				}
				if info, errStat := os.Stat(filepath.Dir(path)); errStat != nil || !info.IsDir() {
					t.Fatalf("parent directory not created: %v", errStat)
				}
				got, errRead := os.ReadFile(path)
				if existing {
					if errRead != nil || !bytes.Equal(got, original) {
						t.Fatalf("existing file changed: %q, error %v", got, errRead)
					}
				} else if !errors.Is(errRead, os.ErrNotExist) {
					t.Fatalf("missing file was created: %v", errRead)
				}
			})
		}
	}
}

func TestSaveTokenToFile_MarshalerFailurePreservesFile(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "missing"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token.json")
			original := []byte("synthetic previous contents\n")
			if existing {
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			sentinel := errors.New("synthetic marshal failure")
			marshaler := &kimiFailingJSONMarshaler{err: sentinel}
			storage := &KimiTokenStorage{Metadata: map[string]any{"custom": marshaler}}
			err := storage.SaveTokenToFile(path)
			if !errors.Is(err, sentinel) {
				t.Fatalf("expected wrapped marshal failure, got %v", err)
			}
			if marshaler.calls != 1 {
				t.Fatalf("MarshalJSON calls = %d, want 1", marshaler.calls)
			}
			if storage.Type != "kimi" {
				t.Fatalf("type after failure = %q, want kimi", storage.Type)
			}
			got, errRead := os.ReadFile(path)
			if existing {
				if errRead != nil || !bytes.Equal(got, original) {
					t.Fatalf("existing file changed: %q, error %v", got, errRead)
				}
			} else if !errors.Is(errRead, os.ErrNotExist) {
				t.Fatalf("missing file was created: %v", errRead)
			}
		})
	}
}

func TestSaveTokenToFile_PreservesEncodingAndFileIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token.json")
	storage := &KimiTokenStorage{
		AccessToken: "synthetic <access>&",
		TokenType:   "Bearer",
		Type:        "kimi",
		Domain:      KimiDefaultDomain,
		BaseURL:     ResolveKimiAPIBaseURL(KimiDefaultDomain),
		Metadata: map[string]any{
			"nested": map[string]any{"html": "<div>&</div>", "values": []any{"first", true, 42}},
		},
	}
	data, err := misc.MergeMetadata(storage, storage.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	var reference bytes.Buffer
	encoder := json.NewEncoder(&reference)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(data); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, bytes.Repeat([]byte("x"), reference.Len()+100), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = storage.SaveTokenToFile(path); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, reference.Bytes()) {
		t.Fatalf("saved bytes differ from original indented encoder:\ngot %q\nwant %q", got, reference.Bytes())
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("save replaced the existing file")
	}
}
