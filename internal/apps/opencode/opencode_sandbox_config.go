package opencode

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// DefaultOpenCodeSandboxPort is the default container TCP port served by
// `kodema sandbox serve` in Option 2 sandbox mode.
const DefaultOpenCodeSandboxPort = 8090

// DefaultSandboxDisabledLocalTools lists the built-in local shell and filesystem
// tools that are disabled in Option 2 sandbox mode so the OpenCode service pod never runs
// untrusted local shell commands or mutates local files.
var DefaultSandboxDisabledLocalTools = []string{
	"bash",
	"write",
	"edit",
	"read",
	"glob",
	"grep",
}

// NormalizeSandboxConfig validates and applies safe defaults to OpenCodeSandboxConfig.
func NormalizeSandboxConfig(cfg OpenCodeSandboxConfig) (OpenCodeSandboxConfig, error) {
	out := cfg
	if len(out.DisabledLocalTools) == 0 {
		out.DisabledLocalTools = append([]string(nil), DefaultSandboxDisabledLocalTools...)
	} else {
		// Always ensure all required local shell/fs tools are included so a
		// misconfiguration can never re-enable local shell execution in sandbox mode.
		seen := make(map[string]bool, len(out.DisabledLocalTools)+len(DefaultSandboxDisabledLocalTools))
		cleaned := make([]string, 0, len(out.DisabledLocalTools)+len(DefaultSandboxDisabledLocalTools))
		for _, tool := range out.DisabledLocalTools {
			t := strings.TrimSpace(tool)
			if t == "" || seen[t] {
				continue
			}
			seen[t] = true
			cleaned = append(cleaned, t)
		}
		for _, required := range DefaultSandboxDisabledLocalTools {
			if !seen[required] {
				seen[required] = true
				cleaned = append(cleaned, required)
			}
		}
		out.DisabledLocalTools = cleaned
	}
	if out.SandboxPort == 0 {
		out.SandboxPort = DefaultOpenCodeSandboxPort
	}
	if out.SandboxPort < 1024 || out.SandboxPort > 65535 {
		return OpenCodeSandboxConfig{}, fmt.Errorf("opencode sandbox port %d must be between 1024 and 65535", out.SandboxPort)
	}
	if strings.TrimSpace(out.DefaultSandboxImage) != "" {
		out.DefaultSandboxImage = strings.TrimSpace(out.DefaultSandboxImage)
		if !validSandboxImage(out.DefaultSandboxImage) {
			return OpenCodeSandboxConfig{}, fmt.Errorf("opencode default sandbox image %q is invalid", out.DefaultSandboxImage)
		}
	}
	if len(out.DefaultSandboxCaches) > 0 {
		caches, err := normalizeSandboxCaches(out.DefaultSandboxCaches)
		if err != nil {
			return OpenCodeSandboxConfig{}, fmt.Errorf("opencode default sandbox caches: %w", err)
		}
		out.DefaultSandboxCaches = caches
	}
	if strings.TrimSpace(out.DefaultSandboxMemory) != "" {
		out.DefaultSandboxMemory = strings.TrimSpace(out.DefaultSandboxMemory)
		if !validSandboxResourceQty(out.DefaultSandboxMemory) {
			return OpenCodeSandboxConfig{}, fmt.Errorf("opencode default sandbox memory %q is invalid", out.DefaultSandboxMemory)
		}
	}
	if strings.TrimSpace(out.DefaultSandboxCPU) != "" {
		out.DefaultSandboxCPU = strings.TrimSpace(out.DefaultSandboxCPU)
		if !validSandboxResourceQty(out.DefaultSandboxCPU) {
			return OpenCodeSandboxConfig{}, fmt.Errorf("opencode default sandbox cpu %q is invalid", out.DefaultSandboxCPU)
		}
	}
	return out, nil
}

type openCodeV2SandboxConfigDoc struct {
	Schema      string                            `json:"$schema"`
	Permissions []openCodePermissionRule          `json:"permissions"`
	Providers   map[string]openCodeV2ProviderConf `json:"providers,omitempty"`
}

type openCodeV2ProviderConf struct {
	Settings *openCodeV2ProviderSettings `json:"settings,omitempty"`
	Headers  map[string]string           `json:"headers,omitempty"`
}

type openCodeV2ProviderSettings struct {
	BaseURL string `json:"baseURL,omitempty"`
	APIKey  string `json:"apiKey,omitempty"`
}

// GenerateSandboxOpenCodeConfig builds an OpenCode v2 sandbox-mode `opencode.json`
// policy document that denies every local shell/filesystem tool permission
// (`shell`, `edit`, `read`, `glob`, `grep`) and optionally points the `kodema`
// provider at the local loopback attribution proxy (never embedding raw
// upstream LLM provider API keys).
func GenerateSandboxOpenCodeConfig(cfg OpenCodeSandboxConfig, proxyBaseURL, localAPIKey string) ([]byte, error) {
	norm, err := NormalizeSandboxConfig(cfg)
	if err != nil {
		return nil, err
	}
	doc := openCodeV2SandboxConfigDoc{
		Schema:      "https://opencode.ai/config.json",
		Permissions: mapOpenCodeDisabledToolsToPermissions(norm.DisabledLocalTools),
	}
	if baseURL := strings.TrimRight(strings.TrimSpace(proxyBaseURL), "/"); baseURL != "" {
		provider := openCodeV2ProviderConf{
			Settings: &openCodeV2ProviderSettings{
				BaseURL: baseURL,
			},
		}
		if key := strings.TrimSpace(localAPIKey); key != "" {
			provider.Settings.APIKey = key
			provider.Headers = map[string]string{
				"Authorization": "Bearer " + key,
			}
		}
		doc.Providers = map[string]openCodeV2ProviderConf{
			"kodema": provider,
		}
	}
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

// ValidateSandboxOpenCodeConfig verifies that an OpenCode v2 `opencode.json`
// policy document uses the v2 schema (`permissions` array and optional v2
// `agents` and `providers` with `settings`/`headers`, rejecting legacy v1
// `tools`, `permission`, `provider`, `agent`, or `options` keys) and denies
// all required local shell and filesystem permissions (`shell`, `edit`,
// `read`, `glob`, `grep`) for resource `"*"` with no subsequent overriding
// allow/ask rule (including within any `agents.<name>.permissions` list).
func ValidateSandboxOpenCodeConfig(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("decode opencode sandbox config: %w", err)
	}
	if raw == nil {
		return fmt.Errorf("opencode sandbox config must be a JSON object")
	}
	for _, legacyKey := range []string{"tools", "permission", "provider", "agent"} {
		if _, present := raw[legacyKey]; present {
			return fmt.Errorf("opencode sandbox config uses legacy v1 key %q; use OpenCode v2 permissions/providers/agents schema", legacyKey)
		}
	}

	rawPerms, ok := raw["permissions"]
	if !ok {
		return fmt.Errorf("opencode sandbox config must declare OpenCode v2 permissions array")
	}
	var perms []openCodePermissionRule
	if err := json.Unmarshal(rawPerms, &perms); err != nil {
		return fmt.Errorf("decode opencode sandbox config permissions: %w", err)
	}
	if len(perms) == 0 {
		return fmt.Errorf("opencode sandbox config must explicitly deny local tool permissions")
	}
	if err := validateOpenCodePermissionRules("permissions", perms); err != nil {
		return err
	}

	requiredRules := mapOpenCodeDisabledToolsToPermissions(DefaultSandboxDisabledLocalTools)
	if err := validateRequiredSandboxDenies("permissions", requiredRules, perms); err != nil {
		return err
	}

	if rawAgents, hasAgents := raw["agents"]; hasAgents {
		var agents map[string]map[string]json.RawMessage
		if err := json.Unmarshal(rawAgents, &agents); err != nil || agents == nil {
			return fmt.Errorf("opencode sandbox config agents must be a JSON object")
		}
		for agentName, entry := range agents {
			if entry == nil {
				return fmt.Errorf("opencode sandbox config agent %q must be a JSON object", agentName)
			}
			for _, legacyKey := range []string{"tools", "permission"} {
				if _, present := entry[legacyKey]; present {
					return fmt.Errorf("opencode sandbox config agent %q uses legacy v1 key %q; use OpenCode v2 \"permissions\"", agentName, legacyKey)
				}
			}
			if rawAgentPerms, hasPerms := entry["permissions"]; hasPerms {
				var agentPerms []openCodePermissionRule
				if err := json.Unmarshal(rawAgentPerms, &agentPerms); err != nil || agentPerms == nil {
					return fmt.Errorf("opencode sandbox config agent %q permissions must be a JSON array", agentName)
				}
				scope := fmt.Sprintf("agents.%s.permissions", agentName)
				if err := validateOpenCodePermissionRules(scope, agentPerms); err != nil {
					return err
				}
				// OpenCode v2 appends agent permissions after top-level
				// permissions (last match wins), so evaluate the effective
				// combined list to prevent agent-level local tool bypasses.
				combined := make([]openCodePermissionRule, 0, len(perms)+len(agentPerms))
				combined = append(combined, perms...)
				combined = append(combined, agentPerms...)
				if err := validateRequiredSandboxDenies(scope, requiredRules, combined); err != nil {
					return err
				}
			}
		}
	}

	if rawProviders, hasProviders := raw["providers"]; hasProviders {
		var providers map[string]map[string]json.RawMessage
		if err := json.Unmarshal(rawProviders, &providers); err != nil || providers == nil {
			return fmt.Errorf("opencode sandbox config providers must be a JSON object")
		}
		for name, entry := range providers {
			if entry == nil {
				return fmt.Errorf("opencode sandbox config provider %q must be a JSON object", name)
			}
			if _, hasLegacyOpts := entry["options"]; hasLegacyOpts {
				return fmt.Errorf("opencode sandbox config provider %q uses legacy v1 key \"options\"; use OpenCode v2 \"settings\" and \"headers\"", name)
			}
			if rawSettings, hasSettings := entry["settings"]; hasSettings {
				var settings map[string]any
				if err := json.Unmarshal(rawSettings, &settings); err != nil || settings == nil {
					return fmt.Errorf("opencode sandbox config provider %q has invalid settings object", name)
				}
			}
			if rawHeaders, hasHeaders := entry["headers"]; hasHeaders {
				var headers map[string]string
				if err := json.Unmarshal(rawHeaders, &headers); err != nil || headers == nil {
					return fmt.Errorf("opencode sandbox config provider %q has invalid headers map", name)
				}
			}
		}
	}
	return nil
}

func validateOpenCodePermissionRules(scope string, perms []openCodePermissionRule) error {
	for i, rule := range perms {
		if strings.TrimSpace(rule.Action) == "" {
			return fmt.Errorf("opencode sandbox config %s[%d] has empty action", scope, i)
		}
		if strings.TrimSpace(rule.Resource) == "" {
			return fmt.Errorf("opencode sandbox config %s[%d] has empty resource", scope, i)
		}
		switch rule.Effect {
		case "allow", "deny", "ask":
		default:
			return fmt.Errorf("opencode sandbox config %s[%d] has invalid effect %q", scope, i, rule.Effect)
		}
	}
	return nil
}

func validateRequiredSandboxDenies(scope string, requiredRules, perms []openCodePermissionRule) error {
	for _, reqRule := range requiredRules {
		deniedAll := false
		for _, rule := range perms {
			if !matchOpenCodePattern(rule.Action, reqRule.Action) {
				continue
			}
			if rule.Effect == "deny" && rule.Resource == "*" {
				deniedAll = true
			} else if rule.Effect != "deny" {
				// OpenCode v2 evaluates rules in order (last match wins), so
				// any later allow/ask rule matching this action weakens the
				// full-wildcard deny.
				deniedAll = false
			}
		}
		if !deniedAll {
			return fmt.Errorf("opencode sandbox config (%s) must deny local tool permission %q for resource \"*\" with no overriding allow/ask rule", scope, reqRule.Action)
		}
	}
	return nil
}

// matchOpenCodePattern matches OpenCode v2 whole-value wildcard patterns where
// '*' matches zero or more characters and '?' matches any single character.
func matchOpenCodePattern(pattern, value string) bool {
	p, v := 0, 0
	starIdx, matchIdx := -1, 0
	for v < len(value) {
		if p < len(pattern) && (pattern[p] == '?' || pattern[p] == value[v]) {
			p++
			v++
		} else if p < len(pattern) && pattern[p] == '*' {
			starIdx = p
			matchIdx = v
			p++
		} else if starIdx != -1 {
			p = starIdx + 1
			matchIdx++
			v = matchIdx
		} else {
			return false
		}
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

func validSandboxImage(image string) bool {
	if image == "" || len(image) > 256 {
		return false
	}
	if strings.HasPrefix(image, "-") {
		return false
	}
	for i := 0; i < len(image); i++ {
		ch := image[i]
		if ch <= 0x20 || ch == 0x7f || ch == '\'' || ch == '"' || ch == ';' || ch == '&' || ch == '|' || ch == '$' || ch == '`' || ch == '\\' {
			return false
		}
	}
	return true
}

func validSandboxCacheName(name string) bool {
	if name == "" || len(name) > 64 {
		return false
	}
	if name == "." || name == ".." || strings.Contains(name, "..") || strings.HasPrefix(name, "-") {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		isValid := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_'
		if !isValid {
			return false
		}
	}
	return true
}

func parseSandboxCachesCSV(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	return normalizeSandboxCaches(parts)
}

func normalizeSandboxCaches(raw []string) ([]string, error) {
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if !validSandboxCacheName(trimmed) {
			return nil, fmt.Errorf("cache name %q is invalid", trimmed)
		}
		if slices.Contains(out, trimmed) {
			return nil, fmt.Errorf("cache name %q is repeated", trimmed)
		}
		out = append(out, trimmed)
	}
	return out, nil
}

func validSandboxResourceQty(raw string) bool {
	if raw == "" || len(raw) > 32 {
		return false
	}
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		isValid := (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '.'
		if !isValid {
			return false
		}
	}
	return true
}
