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

// GenerateSandboxOpenCodeConfig builds a sandbox-mode `opencode.json` policy
// document that disables every local shell/filesystem tool and optionally
// points the LLM provider at the local loopback attribution proxy (never
// embedding raw LLM API keys).
func GenerateSandboxOpenCodeConfig(cfg OpenCodeSandboxConfig, proxyBaseURL, localAPIKey string) ([]byte, error) {
	norm, err := NormalizeSandboxConfig(cfg)
	if err != nil {
		return nil, err
	}
	tools := make(map[string]bool, len(norm.DisabledLocalTools))
	permission := make(map[string]string, len(norm.DisabledLocalTools))
	for _, tool := range norm.DisabledLocalTools {
		tools[tool] = false
		permission[tool] = "deny"
	}
	doc := map[string]any{
		"$schema":    "https://opencode.ai/config.json",
		"tools":      tools,
		"permission": permission,
	}
	if strings.TrimSpace(proxyBaseURL) != "" {
		providerOpts := map[string]any{
			"baseURL": strings.TrimRight(strings.TrimSpace(proxyBaseURL), "/"),
		}
		if strings.TrimSpace(localAPIKey) != "" {
			providerOpts["apiKey"] = strings.TrimSpace(localAPIKey)
		}
		doc["provider"] = map[string]any{
			"kodema": map[string]any{
				"options": providerOpts,
			},
		}
	}
	encoded, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

// ValidateSandboxOpenCodeConfig verifies that an `opencode.json` policy document
// explicitly disables all local shell and filesystem tools (in both `tools` and
// `permission`) required for Option 2 sandbox mode.
func ValidateSandboxOpenCodeConfig(data []byte) error {
	var parsed struct {
		Tools      map[string]bool   `json:"tools"`
		Permission map[string]string `json:"permission"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return fmt.Errorf("decode opencode sandbox config: %w", err)
	}
	if len(parsed.Tools) == 0 {
		return fmt.Errorf("opencode sandbox config must explicitly disable local tools")
	}
	if len(parsed.Permission) == 0 {
		return fmt.Errorf("opencode sandbox config must explicitly deny local tool permissions")
	}
	for _, required := range DefaultSandboxDisabledLocalTools {
		enabled, present := parsed.Tools[required]
		if !present || enabled {
			return fmt.Errorf("opencode sandbox config must disable local tool %q (got present=%v enabled=%v)", required, present, enabled)
		}
		perm, permPresent := parsed.Permission[required]
		if !permPresent || perm != "deny" {
			return fmt.Errorf("opencode sandbox config must deny permission for local tool %q (got present=%v value=%q)", required, permPresent, perm)
		}
	}
	return nil
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
