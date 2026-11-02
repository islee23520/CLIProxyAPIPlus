package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	xxHash64 "github.com/pierrec/xxHash/xxHash64"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestClaudeCloakProfilePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, attribute, metadata, configured string
		want                                  bool
	}{
		{name: "default"},
		{name: "metadata", metadata: " agent-sdk ", want: true},
		{name: "attribute overrides metadata", attribute: "claude-code", metadata: "agent-sdk"},
		{name: "empty attribute falls back", attribute: " ", metadata: "agent-sdk", want: true},
		{name: "config overrides attribute", configured: "claude-code", attribute: "agent-sdk"},
		{name: "config opts in", configured: "agent-sdk", attribute: "claude-code", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"cloak_profile": tc.attribute}, Metadata: map[string]any{"cloak_profile": tc.metadata}}
			if got := helps.ClaudeAgentSDKProfile(&config.CloakConfig{Profile: tc.configured}, auth); got != tc.want {
				t.Fatalf("Agent SDK profile = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClaudeCloakProfileHeaders(t *testing.T) {
	for _, tc := range []struct {
		name, profile, want string
	}{
		{name: "default", want: "claude-cli/2.1.70 (external, cli)"},
		{name: "SDK", profile: "agent-sdk", want: "claude-cli/2.1.70 (external, sdk-ts)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{ClaudeHeaderDefaults: config.ClaudeHeaderDefaults{UserAgent: "claude-cli/2.1.70 (external, cli)"}}
			auth := &cliproxyauth.Auth{Metadata: map[string]any{"cloak_profile": tc.profile}}
			req := newClaudeHeaderTestRequest(t, http.Header{})
			if err := applyClaudeHeaders(req, auth, "sk-ant-oat-test", false, nil, cfg); err != nil {
				t.Fatal(err)
			}
			if got := req.Header.Get("User-Agent"); got != tc.want {
				t.Fatalf("User-Agent = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClaudeCloakProfileSystem(t *testing.T) {
	for _, tc := range []struct {
		name, profile, system string
		strict                bool
	}{
		{name: "default", system: `"caller rules"`},
		{name: "SDK string", profile: "agent-sdk", system: `"caller rules"`},
		{name: "SDK blocks", profile: "agent-sdk", system: `[{"type":"text","text":"caller rules","cache_control":{"type":"ephemeral"}}]`},
		{name: "SDK strict preserves instructions", profile: "agent-sdk", system: `"caller rules"`, strict: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{ClaudeKey: []config.ClaudeKey{{APIKey: "sk-ant-oat-test", Cloak: &config.CloakConfig{Profile: tc.profile, StrictMode: tc.strict}}}}
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-ant-oat-test"}}
			payload := []byte(`{"system":` + tc.system + `,"messages":[{"role":"user","content":"hello"}]}`)
			out, err := applyCloaking(context.Background(), cfg, auth, payload, "claude-sonnet-4-5", "sk-ant-oat-test")
			if err != nil {
				t.Fatal(err)
			}
			identity := gjson.GetBytes(out, "system.1.text").String()
			if tc.profile == "" {
				if identity != "You are Claude Code, Anthropic's official CLI for Claude." || gjson.GetBytes(out, "system.#").Int() != 3 {
					t.Fatalf("default system changed: %s", out)
				}
				return
			}
			if !strings.Contains(identity, "Claude Agent SDK") || bytes.Contains(out, []byte("You are Claude Code,")) {
				t.Fatalf("incorrect SDK identity: %s", out)
			}
			if got := gjson.GetBytes(out, "system.2.text").String(); got != "caller rules" {
				t.Fatalf("caller instructions = %q", got)
			}
			if got := gjson.GetBytes(out, "messages.0.content").String(); got != "hello" {
				t.Fatalf("user content changed: %q", got)
			}
			again, err := applyCloaking(context.Background(), cfg, auth, out, "claude-sonnet-4-5", "sk-ant-oat-test")
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(again, "system.#").Int() != 3 || strings.Count(gjson.GetBytes(again, "system").Raw, "caller rules") != 1 {
				t.Fatalf("duplicated SDK prefix: %s", again)
			}
			if strings.Contains(tc.system, "cache_control") && !gjson.GetBytes(again, "system.2.cache_control").Exists() {
				t.Fatal("caller cache control lost")
			}
		})
	}
}

func TestClaudeAgentSDKUpstreamIdentityAndSigning(t *testing.T) {
	for _, mode := range []string{"execute", "stream", "count"} {
		t.Run(mode, func(t *testing.T) {
			type captured struct {
				body   []byte
				header http.Header
			}
			requests := make(chan captured, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				requests <- captured{body, r.Header.Clone()}
				if mode == "stream" {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
				} else {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","model":"claude-sonnet-4-5","role":"assistant","content":[{"type":"text","text":"ok"}],"input_tokens":1,"usage":{"input_tokens":1,"output_tokens":1}}`)
				}
			}))
			defer server.Close()
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-ant-oat-test", "base_url": server.URL}, Metadata: map[string]any{"cloak_profile": "agent-sdk"}}
			executor := NewClaudeExecutor(&config.Config{})
			req := cliproxyexecutor.Request{Model: "claude-sonnet-4-5", Payload: []byte(`{"system":"caller rules","messages":[{"role":"user","content":"hello"}]}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude")}
			var err error
			switch mode {
			case "execute":
				_, err = executor.Execute(context.Background(), auth, req, opts)
			case "count":
				_, err = executor.CountTokens(context.Background(), auth, req, opts)
			case "stream":
				var result *cliproxyexecutor.StreamResult
				result, err = executor.ExecuteStream(context.Background(), auth, req, opts)
				if err == nil {
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			seen := <-requests
			if got := parseEntrypointFromUA(seen.header.Get("User-Agent")); got != "sdk-ts" {
				t.Fatalf("UA entrypoint = %q", got)
			}
			billing := gjson.GetBytes(seen.body, "system.0.text").String()
			if !strings.Contains(billing, "cc_entrypoint=sdk-ts;") {
				t.Fatalf("billing = %q", billing)
			}
			if gjson.GetBytes(seen.body, "system.2.text").String() != "caller rules" {
				t.Fatalf("caller system lost: %s", seen.body)
			}
			unsignedBilling := claudeBillingHeaderCCHPattern.ReplaceAllString(billing, "cch=00000;")
			unsigned, _ := sjson.SetBytes(seen.body, "system.0.text", unsignedBilling)
			want := fmt.Sprintf("%05x", xxHash64.Checksum(unsigned, 0x6E52736AC806831E)&0xFFFFF)
			match := claudeBillingHeaderCCHPattern.FindStringSubmatch(billing)
			if len(match) != 2 || match[1] != want {
				t.Fatalf("invalid final CCH: %q, want %s", billing, want)
			}
		})
	}
}
