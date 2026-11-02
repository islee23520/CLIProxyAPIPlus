package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ClaudeAgentSDKProfile resolves the profile with the same precedence as cloak settings.
func ClaudeAgentSDKProfile(cloak *config.CloakConfig, auth *cliproxyauth.Auth) bool {
	profile := ""
	if auth != nil {
		profile = strings.TrimSpace(auth.Attributes["cloak_profile"])
		if profile == "" {
			profile, _ = auth.Metadata["cloak_profile"].(string)
		}
	}
	if cloak != nil && strings.TrimSpace(cloak.Profile) != "" {
		profile = cloak.Profile
	}
	return strings.EqualFold(strings.TrimSpace(profile), "agent-sdk")
}

// ClaudeAgentSDKUserAgent changes only the entrypoint, retaining the device version.
func ClaudeAgentSDKUserAgent(userAgent string) string {
	start := strings.Index(userAgent, " (external,")
	if start < 0 || !strings.HasPrefix(userAgent, "claude-cli/") {
		userAgent = defaultClaudeFingerprintUserAgent
		start = strings.Index(userAgent, " (external,")
	}
	return userAgent[:start] + " (external, sdk-ts)"
}

// ApplyClaudeAgentSDKSystem keeps caller instructions in the system role without a CLI preset.
func ApplyClaudeAgentSDKSystem(payload []byte, billing string) []byte {
	const identity = "You are a Claude agent, built on Anthropic's Claude Agent SDK."
	system := gjson.GetBytes(payload, "system")
	blocks := []map[string]any{{"type": "text", "text": billing}, {"type": "text", "text": identity}}
	if system.Type == gjson.String {
		if system.String() != "" && system.String() != identity {
			blocks = append(blocks, map[string]any{"type": "text", "text": system.String()})
		}
	} else if system.IsArray() {
		for _, block := range system.Array() {
			text := block.Get("text").String()
			if strings.HasPrefix(text, "x-anthropic-billing-header:") || text == identity {
				continue
			}
			if value, ok := block.Value().(map[string]any); ok {
				blocks = append(blocks, value)
			}
		}
	}
	out, _ := sjson.SetBytes(payload, "system", blocks)
	return out
}
