package executor

import (
	"net/http"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestApplyCodexRoutingHintServiceTiers(t *testing.T) {
	for _, tier := range []string{"priority", "ultrafast"} {
		t.Run(tier, func(t *testing.T) {
			headers := http.Header{}
			applyCodexRoutingHint(headers, nil, "gpt-6.1-sol", []byte(`{"model":"gpt-6.1-sol","service_tier":"`+tier+`"}`))
			if got, want := headers.Get(codexRoutingHintHeader), "model=gpt-6.1-sol;tier="+tier; got != want {
				t.Fatalf("routing hint = %q, want %q", got, want)
			}
		})
	}
}

func TestApplyCodexRoutingHintPreservesOperatorAndSkipsAPIKey(t *testing.T) {
	headers := http.Header{codexRoutingHintHeader: []string{"model=operator;tier=priority"}}
	applyCodexRoutingHint(headers, nil, "gpt-6.1-sol", []byte(`{"service_tier":"ultrafast"}`))
	if got := headers.Get(codexRoutingHintHeader); got != "model=operator;tier=priority" {
		t.Fatalf("operator routing hint = %q", got)
	}

	headers = http.Header{}
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "key"}}
	applyCodexRoutingHint(headers, auth, "gpt-6.1-sol", []byte(`{"service_tier":"ultrafast"}`))
	if got := headers.Get(codexRoutingHintHeader); got != "" {
		t.Fatalf("API-key routing hint = %q, want empty", got)
	}
}
