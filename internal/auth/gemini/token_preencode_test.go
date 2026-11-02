package gemini

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
)

type preencodeMarshaler struct {
	err   error
	calls *int
}

func (m preencodeMarshaler) MarshalJSON() ([]byte, error) {
	*m.calls++
	return []byte(`"<synthetic>&"`), m.err
}

func TestGeminiTokenStorage_PreencodePreservesFileOnSerializationError(t *testing.T) {
	marshalErr := errors.New("synthetic marshal failure")
	marshalCalls := 0
	for _, tt := range []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan int)},
		{name: "function", value: func() {}},
		{name: "marshaler", value: preencodeMarshaler{err: marshalErr, calls: &marshalCalls}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, existing := range []bool{true, false} {
				name := "absent"
				if existing {
					name = "existing"
				}
				t.Run(name, func(t *testing.T) {
					marshalCalls = 0
					path := filepath.Join(t.TempDir(), "nested", "token.json")
					sentinel := []byte("synthetic existing token file\n")
					if existing {
						if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(path, sentinel, 0600); err != nil {
							t.Fatal(err)
						}
					}
					storage := &GeminiTokenStorage{Type: "other", Metadata: map[string]any{"synthetic": tt.value}}
					err := storage.SaveTokenToFile(path)
					if err == nil {
						t.Fatal("expected serialization failure")
					}
					if tt.name == "marshaler" {
						if marshalCalls != 1 {
							t.Fatalf("expected one marshal call, got %d", marshalCalls)
						}
						if !errors.Is(err, marshalErr) {
							t.Fatalf("expected wrapped marshaler error, got %v", err)
						}
					} else {
						var unsupported *json.UnsupportedTypeError
						if !errors.As(err, &unsupported) {
							t.Fatalf("expected wrapped UnsupportedTypeError, got %v", err)
						}
					}
					if storage.Type != "gemini" {
						t.Fatal("provider type was not assigned")
					}
					if existing {
						got, errRead := os.ReadFile(path)
						if errRead != nil {
							t.Fatal(errRead)
						}
						if !bytes.Equal(got, sentinel) {
							t.Fatal("serialization failure changed existing file")
						}
					} else if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
						t.Fatalf("expected absent token file, got %v", errStat)
					}
					if _, errStat := os.Stat(filepath.Dir(path)); errStat != nil {
						t.Fatalf("expected directory creation before final encoding, got %v", errStat)
					}
				})
			}
		})
	}
}

func TestGeminiTokenStorage_PreencodeMatchesEncoderAndRetainsFileIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token.json")
	if err := os.WriteFile(path, []byte(strings.Repeat("synthetic old token data\n", 100)), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	marshalCalls := 0
	storage := &GeminiTokenStorage{
		Token: map[string]any{"access_token": "synthetic <token>&"},
		Metadata: map[string]any{
			"nested": map[string]any{"html": "<b>&synthetic</b>", "values": []any{true, 3.0, nil}},
			"custom": preencodeMarshaler{calls: &marshalCalls},
		},
	}
	reference := *storage
	reference.Type = "gemini"
	data, err := misc.MergeMetadata(&reference, reference.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	enc := json.NewEncoder(&want)
	enc.SetIndent("", "  ")
	if err = enc.Encode(data); err != nil {
		t.Fatal(err)
	}
	if int64(want.Len()) >= before.Size() {
		t.Fatal("fixture must exercise a shorter overwrite")
	}
	marshalCalls = 0
	if err = storage.SaveTokenToFile(path); err != nil {
		t.Fatal(err)
	}
	if storage.Type != "gemini" {
		t.Fatal("provider type was not assigned")
	}
	if marshalCalls != 1 {
		t.Fatalf("expected one marshal call, got %d", marshalCalls)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatal("saved bytes differ from reference JSON encoder")
	}
	if !bytes.HasSuffix(got, []byte("\n")) || !bytes.Contains(got, []byte(`\u003c`)) {
		t.Fatal("expected encoder newline and HTML escaping")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("overwrite replaced the existing file")
	}
}

func TestGeminiTokenStorage_MergeFailureDoesNotCreateDirectory(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
	}{
		{name: "channel", value: make(chan int)},
		{name: "function", value: func() {}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "token.json")
			storage := &GeminiTokenStorage{Token: map[string]any{"synthetic": tt.value}}
			err := storage.SaveTokenToFile(path)
			var unsupported *json.UnsupportedTypeError
			if !errors.As(err, &unsupported) {
				t.Fatalf("expected wrapped UnsupportedTypeError, got %v", err)
			}
			if storage.Type != "gemini" {
				t.Fatal("provider type was not assigned")
			}
			if _, errStat := os.Stat(filepath.Dir(path)); !errors.Is(errStat, os.ErrNotExist) {
				t.Fatalf("expected absent directory after merge failure, got %v", errStat)
			}
		})
	}
}
