package executor

import (
	"strings"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestEnsureXAINativeXSearchToolInjectsWhenMissing(t *testing.T) {
	body := []byte(`{"model":"grok-4.6","tools":[{"type":"web_search"}]}`)
	got := ensureXAINativeXSearchTool(body)
	types := []string{}
	for _, tool := range ojson.GetBytes(got, "tools").Array() {
		types = append(types, tool.Get("type").String())
	}
	if strings.Join(types, ",") != "web_search,x_search" {
		t.Fatalf("tools = %v, want web_search,x_search", types)
	}
}

func TestEnsureXAINativeXSearchToolIsIdempotent(t *testing.T) {
	body := []byte(`{"tools":[{"type":"x_search"}]}`)
	got := ensureXAINativeXSearchTool(body)
	if n := len(ojson.GetBytes(got, "tools").Array()); n != 1 {
		t.Fatalf("tool count = %d, want 1", n)
	}
}

func TestXAIExecutorHonorsInjectXSearchConfig(t *testing.T) {
	if (&config.Config{}).XAI.InjectXSearch {
		t.Fatal("inject-x-search defaulted to true, want false")
	}
	enabled := &config.Config{XAI: config.XAIConfig{InjectXSearch: true}}
	if !enabled.XAI.InjectXSearch {
		t.Fatal("inject-x-search stayed false after enable")
	}
}
