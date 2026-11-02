package xai

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

type failingTokenMetadata struct {
	calls int
	err   error
}

func (m *failingTokenMetadata) MarshalJSON() ([]byte, error) {
	m.calls++
	return nil, m.err
}

func TestSaveTokenToFilePreservesTargetOnEncodingFailure(t *testing.T) {
	for _, kind := range []string{"channel", "function", "marshaler"} {
		for _, exists := range []bool{false, true} {
			name := kind + "/missing"
			if exists {
				name = kind + "/existing"
			}
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "nested", "token.json")
				original := []byte("synthetic existing token bytes\n")
				if exists {
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, original, 0o600); err != nil {
						t.Fatal(err)
					}
				}

				sentinel := errors.New("synthetic metadata failure")
				marshaler := &failingTokenMetadata{err: sentinel}
				var value any
				switch kind {
				case "channel":
					value = make(chan int)
				case "function":
					value = func() {}
				case "marshaler":
					value = marshaler
				}
				storage := &TokenStorage{
					Type: "before", AuthKind: "before", AccessToken: "synthetic",
					Metadata: map[string]any{"invalid": value},
				}
				err := storage.SaveTokenToFile(path)
				if err == nil {
					t.Fatal("SaveTokenToFile() succeeded, want encoding error")
				}
				if kind == "marshaler" {
					if !errors.Is(err, sentinel) {
						t.Fatalf("error = %v, want sentinel in chain", err)
					}
					if marshaler.calls != 1 {
						t.Fatalf("MarshalJSON calls = %d, want 1", marshaler.calls)
					}
				} else {
					var unsupported *json.UnsupportedTypeError
					if !errors.As(err, &unsupported) || unsupported.Type != reflect.TypeOf(value) {
						t.Fatalf("error = %v, want UnsupportedTypeError for %T", err, value)
					}
				}
				if storage.Type != "xai" || storage.AuthKind != "oauth" {
					t.Fatalf("Type/AuthKind = %q/%q, want xai/oauth", storage.Type, storage.AuthKind)
				}
				if info, errStat := os.Stat(filepath.Dir(path)); errStat != nil || !info.IsDir() {
					t.Fatalf("parent directory missing after failure: %v", errStat)
				}
				got, errRead := os.ReadFile(path)
				if exists {
					if errRead != nil {
						t.Fatal(errRead)
					}
					if !bytes.Equal(got, original) {
						t.Fatalf("existing bytes changed: got %q, want %q", got, original)
					}
				} else if !errors.Is(errRead, os.ErrNotExist) {
					t.Fatalf("missing target read error = %v, want ErrNotExist", errRead)
				}
			})
		}
	}
}

func TestSaveTokenToFileMatchesIndentedEncoderAndOverwritesInPlace(t *testing.T) {
	storage := &TokenStorage{
		Type: "xai", AuthKind: "oauth", AccessToken: "synthetic <access>&",
		Metadata: map[string]any{
			"nested": map[string]any{"html": "<b>&\u2028\u2029", "values": []any{true, "<nested>", float64(3)}},
		},
	}
	data, errMerge := misc.MergeMetadata(storage, storage.Metadata)
	if errMerge != nil {
		t.Fatal(errMerge)
	}
	var reference bytes.Buffer
	encoder := json.NewEncoder(&reference)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		t.Fatal(err)
	}
	for _, exists := range []bool{false, true} {
		name := "new"
		if exists {
			name = "shorter overwrite"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "token.json")
			var before os.FileInfo
			if exists {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, bytes.Repeat([]byte("x"), reference.Len()+100), 0o600); err != nil {
					t.Fatal(err)
				}
				var errStat error
				before, errStat = os.Stat(path)
				if errStat != nil {
					t.Fatal(errStat)
				}
			}
			if err := storage.SaveTokenToFile(path); err != nil {
				t.Fatal(err)
			}
			got, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			if !bytes.Equal(got, reference.Bytes()) {
				t.Fatalf("output = %q, want original indented encoder output %q", got, reference.Bytes())
			}
			if exists {
				after, errStat := os.Stat(path)
				if errStat != nil {
					t.Fatal(errStat)
				}
				if !os.SameFile(before, after) {
					t.Fatal("overwrite replaced the file instead of truncating in place")
				}
			}
		})
	}
}
