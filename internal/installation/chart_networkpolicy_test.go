package installation

// Network-policy and upgrade-safety chart checks. The chart is inspected as
// text: no helm binary, template engine, YAML decoder, cluster or network
// access is used. The check functions are exercised against deliberately
// mutated templates so a regression cannot silently pass.

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// checkTenantNetworkPolicyTemplate verifies that the NetworkPolicy template
// renders one default-deny policy per tenant namespace, never into the release
// namespace, and that every permitted destination is namespace-scoped.
func checkTenantNetworkPolicyTemplate(body string) error {
	if !regexp.MustCompile(`range\s+\$tenant\s*:=\s*\.Values\.tenants`).MatchString(body) {
		return fmt.Errorf("network policy template does not range over .Values.tenants")
	}
	if !regexp.MustCompile(`if\s+\.Values\.networkPolicy\.defaultDeny`).MatchString(body) {
		return fmt.Errorf("network policy template is not gated by networkPolicy.defaultDeny")
	}
	if got := strings.Count(body, "kind: NetworkPolicy"); got != 1 {
		return fmt.Errorf("network policy template renders %d policies per range iteration, want 1", got)
	}

	namespaceField := regexp.MustCompile(`(?m)^[ \t]*namespace:[ \t]*(\S.*)$`)
	matches := namespaceField.FindAllStringSubmatch(body, -1)
	if len(matches) != 1 {
		return fmt.Errorf("network policy template has %d namespace fields, want exactly 1 per tenant policy", len(matches))
	}
	namespace := strings.TrimSpace(matches[0][1])
	if namespace != "kodema-tenant-{{ $tenant }}" {
		return fmt.Errorf("tenant policy namespace = %q, want kodema-tenant-{{ $tenant }}", namespace)
	}
	for line := range strings.SplitSeq(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "namespace:") && strings.Contains(line, ".Release.Namespace") {
			return fmt.Errorf("network policy is placed in the release namespace: %s", strings.TrimSpace(line))
		}
	}

	if !regexp.MustCompile(`(?m)^[ \t]*podSelector:[ \t]*\{[ \t]*\}[ \t]*$`).MatchString(body) {
		return fmt.Errorf("tenant policy has no empty default-deny podSelector")
	}
	for _, policyType := range []string{"Ingress", "Egress"} {
		if !regexp.MustCompile(`(?m)^[ \t]*-[ \t]*` + policyType + `[ \t]*$`).MatchString(body) {
			return fmt.Errorf("tenant policy omits the %s policy type", policyType)
		}
	}

	if got := strings.Count(body, "namespaceSelector:"); got < 3 {
		return fmt.Errorf("tenant policy has %d namespaceSelector peers, want the Kodema, Git and DNS peers", got)
	}
	kodemaPeer := regexp.MustCompile(`namespaceSelector:[ \t]*\r?\n[ \t]*matchLabels:[ \t]*\r?\n[ \t]*kubernetes\.io/metadata\.name:[ \t]*\{\{[ \t]*\$?\.Release\.Namespace[ \t]*\}\}`)
	if matches := kodemaPeer.FindAllString(body, -1); len(matches) != 2 {
		return fmt.Errorf("tenant policy has %d namespace-scoped Kodema allows, want ingress and egress", len(matches))
	}
	if !regexp.MustCompile(`(?m)^[ \t]*kubernetes\.io/metadata\.name:[ \t]*git[ \t]*$`).MatchString(body) {
		return fmt.Errorf("tenant policy does not scope the Git allow to a namespace")
	}
	if !regexp.MustCompile(`(?m)^[ \t]*k8s-app:[ \t]*kube-dns[ \t]*$`).MatchString(body) {
		return fmt.Errorf("tenant policy does not permit cluster DNS egress")
	}
	if !regexp.MustCompile(`(?m)^[ \t]*port:[ \t]*53[ \t]*$`).MatchString(body) {
		return fmt.Errorf("tenant policy does not permit DNS port 53")
	}
	for _, gate := range []string{"permitKodema", "permitGit"} {
		if !regexp.MustCompile(`if\s+\$?\.Values\.networkPolicy\.` + gate).MatchString(body) {
			return fmt.Errorf("tenant policy is not gated by networkPolicy.%s", gate)
		}
	}
	if strings.Contains(body, "8080") {
		return fmt.Errorf("tenant policy permits unserved port 8080")
	}
	if regexp.MustCompile(`(?m)^[ \t]*port:[ \t]*8443[ \t]*$`).MatchString(body) {
		return fmt.Errorf("tenant policy hardcodes port 8443 instead of reading service.targetPort")
	}
	if !strings.Contains(body, ".Values.service.targetPort | default 8443") {
		return fmt.Errorf("tenant policy does not derive $apiPort from .Values.service.targetPort | default 8443")
	}
	apiPortUses := regexp.MustCompile(`(?m)^[ \t]*port:[ \t]*\{\{[ \t]*\$apiPort[ \t]*\}\}[ \t]*$`).FindAllString(body, -1)
	if len(apiPortUses) != 2 {
		return fmt.Errorf("tenant policy uses $apiPort in %d port entries, want ingress and egress (2)", len(apiPortUses))
	}
	if !strings.Contains(body, "(ne (int ($.Values.gitService.port | default 8443)) $apiPort)") {
		return fmt.Errorf("tenant policy does not deduplicate gitService.port against $apiPort")
	}
	return nil
}

// checkRecreateStrategy verifies that the single controller never overlaps an
// old pod during an upgrade.
func checkRecreateStrategy(body string) error {
	if !regexp.MustCompile(`(?m)^[ \t]*strategy:[ \t]*\r?\n[ \t]+type:[ \t]*Recreate[ \t]*$`).MatchString(body) {
		return fmt.Errorf("Deployment does not use strategy.type: Recreate")
	}
	return nil
}

func TestTenantNetworkPoliciesAreNamespacedAndDefaultDeny(t *testing.T) {
	body := readText(t, chartPath(t, "templates", "networkpolicy.yaml"))
	if err := checkTenantNetworkPolicyTemplate(body); err != nil {
		t.Fatalf("networkpolicy.yaml: %v", err)
	}

	// The tenant namespace naming must stay identical to namespaces.yaml.
	namespaces := readText(t, chartPath(t, "templates", "namespaces.yaml"))
	if !strings.Contains(namespaces, "kodema-tenant-{{ $tenant }}") {
		t.Fatalf("namespaces.yaml no longer names tenants kodema-tenant-<tenant>")
	}
}

func TestTenantNetworkPolicyTemplateRejectsReleaseNamespace(t *testing.T) {
	body := readText(t, chartPath(t, "templates", "networkpolicy.yaml"))
	mutated := strings.Replace(body, "namespace: kodema-tenant-{{ $tenant }}", "namespace: {{ .Release.Namespace }}", 1)
	if mutated == body {
		t.Fatal("test mutation did not change the tenant namespace")
	}
	if err := checkTenantNetworkPolicyTemplate(mutated); err == nil {
		t.Fatal("checker accepted a tenant policy in the release namespace")
	}
}

func TestTenantNetworkPolicyTemplateRejectsUnscopedPeers(t *testing.T) {
	body := readText(t, chartPath(t, "templates", "networkpolicy.yaml"))
	mutated := strings.ReplaceAll(body, "namespaceSelector:", "podSelector:")
	if mutated == body {
		t.Fatal("test mutation did not remove the namespace selectors")
	}
	if err := checkTenantNetworkPolicyTemplate(mutated); err == nil {
		t.Fatal("checker accepted same-namespace-only pod selectors")
	}
}

func TestTenantNetworkPolicyTemplateRejectsMissingGitGate(t *testing.T) {
	body := readText(t, chartPath(t, "templates", "networkpolicy.yaml"))
	mutated := strings.Replace(body, "if $.Values.networkPolicy.permitGit", "if true", 1)
	if mutated == body {
		t.Fatal("test mutation did not remove the Git gate")
	}
	if err := checkTenantNetworkPolicyTemplate(mutated); err == nil {
		t.Fatal("checker accepted an ungated Git allow")
	}
}

func TestTenantNetworkPolicyUsesTargetPortAndDropsUnservedPort8080(t *testing.T) {
	deployment := readText(t, chartPath(t, "templates", "deployment.yaml"))
	if strings.Contains(deployment, "8080") {
		t.Fatalf("deployment.yaml unexpectedly references port 8080")
	}
	if !strings.Contains(deployment, ".Values.service.targetPort | default 8443") {
		t.Fatalf("deployment.yaml does not bind to .Values.service.targetPort | default 8443")
	}

	body := readText(t, chartPath(t, "templates", "networkpolicy.yaml"))
	if err := checkTenantNetworkPolicyTemplate(body); err != nil {
		t.Fatalf("networkpolicy.yaml: %v", err)
	}

	for _, mutation := range []struct {
		name   string
		mutate func(string) string
	}{
		{
			name: "re-add unserved port 8080",
			mutate: func(src string) string {
				return strings.Replace(src, "port: {{ $apiPort }}", "port: {{ $apiPort }}\n        - protocol: TCP\n          port: 8080", 1)
			},
		},
		{
			name: "hardcode port 8443",
			mutate: func(src string) string {
				return strings.Replace(src, "port: {{ $apiPort }}", "port: 8443", 1)
			},
		},
		{
			name: "deduplicate gitService.port against 8443 instead of $apiPort",
			mutate: func(src string) string {
				return strings.Replace(src, "(ne (int ($.Values.gitService.port | default 8443)) $apiPort)", "(ne (int ($.Values.gitService.port | default 8443)) 8443)", 1)
			},
		},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			mutated := mutation.mutate(body)
			if mutated == body {
				t.Fatalf("mutation %q did not change networkpolicy.yaml", mutation.name)
			}
			if err := checkTenantNetworkPolicyTemplate(mutated); err == nil {
				t.Fatalf("checker accepted %s", mutation.name)
			}
		})
	}
}

func TestControllerUsesRecreateStrategy(t *testing.T) {
	name, body := templateContaining(t, templateBodies(t), "kind: Deployment")
	if err := checkRecreateStrategy(body); err != nil {
		t.Fatalf("%s: %v", name, err)
	}

	mutated := strings.Replace(body, "strategy:\n    type: Recreate\n", "", 1)
	if mutated == body {
		t.Fatal("test mutation did not remove the strategy")
	}
	if err := checkRecreateStrategy(mutated); err == nil {
		t.Fatal("checker accepted a Deployment without the Recreate strategy")
	}
}

func TestNetworkPolicySchemaRequiresDefaultDeny(t *testing.T) {
	schema := objectAt(t, loadJSON(t, chartPath(t, "values.schema.json")), "schema")
	properties := objectAt(t, schema["properties"], "schema.properties")
	policy := objectAt(t, properties["networkPolicy"], "schema.properties.networkPolicy")
	if allowed, ok := policy["additionalProperties"].(bool); !ok || allowed {
		t.Errorf("networkPolicy additionalProperties = %v, want false", policy["additionalProperties"])
	}
	policyProperties := objectAt(t, policy["properties"], "networkPolicy.properties")
	defaultDeny := objectAt(t, policyProperties["defaultDeny"], "networkPolicy.properties.defaultDeny")
	if defaultDeny["type"] != "boolean" {
		t.Errorf("networkPolicy.defaultDeny type = %v, want boolean", defaultDeny["type"])
	}
	if defaultDeny["const"] != true {
		t.Errorf("networkPolicy.defaultDeny const = %v, want true", defaultDeny["const"])
	}
	for _, name := range []string{"permitKodema", "permitGit"} {
		property := objectAt(t, policyProperties[name], "networkPolicy.properties."+name)
		if property["type"] != "boolean" {
			t.Errorf("networkPolicy.%s type = %v, want boolean", name, property["type"])
		}
	}

	fixture := loadJSON(t, filepath.Join("testdata", "chart", "values.invalid-networkpolicy.json"))
	err := validateAgainstSchema(schema, fixture, "values")
	if err == nil {
		t.Fatal("schema accepted networkPolicy.defaultDeny: false")
	}
	if !strings.Contains(err.Error(), "networkPolicy") {
		t.Fatalf("schema refused defaultDeny:false for the wrong reason: %v", err)
	}
}

// checkOpenCodeBrainNetworkPolicyTemplate verifies the opt-in Option 2
// Brain <-> Sandbox MCP NetworkPolicy template:
//   - gated on .Values.opencodeBrain.enabled and checks tenant membership,
//   - Brain pod policy denies all ingress (ingress: []) and allows egress only
//     to kube-dns, control-plane API ($apiPort), and matching tenant/project
//     sandbox pods (kodema.dev/role: sandbox) on $sandboxPort,
//   - Sandbox pod policy lives in kodema-tenant-{{ $tenant }}, declares only
//     Ingress policyType (never widening sandbox egress or sandbox-to-sandbox
//     traffic), and admits only the matching tenant/project Brain pod on $sandboxPort.
func checkOpenCodeBrainNetworkPolicyTemplate(body string) error {
	if !regexp.MustCompile(`if\s+\.Values\.opencodeBrain\.enabled`).MatchString(body) {
		return fmt.Errorf("opencode brain network policy template is not gated by opencodeBrain.enabled")
	}
	if !regexp.MustCompile(`range\s+\$entry\s*:=\s*\$brain\.projects`).MatchString(body) {
		return fmt.Errorf("opencode brain network policy template does not range over $brain.projects")
	}
	if !strings.Contains(body, `has $tenant $.Values.tenants`) {
		return fmt.Errorf("opencode brain network policy template does not verify tenant membership in values.tenants")
	}
	if got := strings.Count(body, "kind: NetworkPolicy"); got != 2 {
		return fmt.Errorf("opencode brain network policy template renders %d NetworkPolicy objects per entry, want 2 (brain + sandbox)", got)
	}

	// Split the two policy documents cleanly at the second manifest separator.
	parts := strings.Split(body, "\n---\n")
	if len(parts) != 3 {
		return fmt.Errorf("opencode brain network policy template has %d YAML sections, want header + 2 NetworkPolicy manifests", len(parts))
	}
	brainDoc, sandboxDoc := parts[1], parts[2]

	// Brain policy checks.
	if !strings.Contains(brainDoc, "name: kodema-opencode-brain-{{ $name }}") {
		return fmt.Errorf("brain policy missing expected name")
	}
	if !regexp.MustCompile(`(?m)^[ \t]*ingress:[ \t]*\[\][ \t]*$`).MatchString(brainDoc) {
		return fmt.Errorf("brain policy must deny all ingress (ingress: []) so sandboxes cannot reach Brain session storage")
	}
	for _, want := range []string{
		"app.kubernetes.io/component: opencode-brain",
		"kodema.dev/tenant: {{ $tenant | quote }}",
		"kodema.dev/project: {{ $project | quote }}",
		"kubernetes.io/metadata.name: kodema-tenant-{{ $tenant }}",
		"kodema.dev/role: sandbox",
		"port: {{ $sandboxPort }}",
		"port: {{ $apiPort }}",
		"k8s-app: kube-dns",
	} {
		if !strings.Contains(brainDoc, want) {
			return fmt.Errorf("brain policy missing required selector/rule %q", want)
		}
	}

	// Sandbox policy checks.
	if !strings.Contains(sandboxDoc, "name: kodema-opencode-sandbox-{{ $name }}") {
		return fmt.Errorf("sandbox policy missing expected name")
	}
	if !regexp.MustCompile(`(?m)^[ \t]*namespace:[ \t]*kodema-tenant-\{\{[ \t]*\$tenant[ \t]*\}\}[ \t]*$`).MatchString(sandboxDoc) {
		return fmt.Errorf("sandbox policy must live in kodema-tenant-{{ $tenant }}")
	}
	if regexp.MustCompile(`(?m)^[ \t]*-[ \t]*Egress[ \t]*$`).MatchString(sandboxDoc) || regexp.MustCompile(`(?m)^[ \t]*egress:`).MatchString(sandboxDoc) {
		return fmt.Errorf("sandbox policy must not declare Egress rules (tenant default-deny owns sandbox egress)")
	}
	for _, want := range []string{
		"kodema.dev/tenant: {{ $tenant | quote }}",
		"kodema.dev/project: {{ $project | quote }}",
		"kodema.dev/role: sandbox",
		"kubernetes.io/metadata.name: {{ $brainNS | quote }}",
		"app.kubernetes.io/component: opencode-brain",
		"port: {{ $sandboxPort }}",
	} {
		if !strings.Contains(sandboxDoc, want) {
			return fmt.Errorf("sandbox policy missing required isolation selector %q", want)
		}
	}
	// Ensure sandbox policy ingress peer is NOT matching other sandbox pods!
	if strings.Contains(sandboxDoc, "from:") {
		_, fromBlock, _ := strings.Cut(sandboxDoc, "from:")
		if strings.Contains(fromBlock, "kodema.dev/role: sandbox") {
			return fmt.Errorf("sandbox policy ingress peer allows sandbox-to-sandbox traffic")
		}
	}
	return nil
}

func TestOpenCodeBrainNetworkPolicyTemplateAndMutations(t *testing.T) {
	body := readText(t, chartPath(t, "templates", "workloads-opencode-brain.yaml"))
	if err := checkOpenCodeBrainNetworkPolicyTemplate(body); err != nil {
		t.Fatalf("workloads-opencode-brain.yaml: %v", err)
	}

	// Verify tenant label matches runtime adapter's LabelTenant.
	wantLabel := adapterTenantLabel(t)
	requireOnlyTenantLabel(t, "templates/workloads-opencode-brain.yaml", body, wantLabel)

	mutations := []struct {
		name   string
		mutate func(string) string
	}{
		{
			name: "allow ingress to Brain pod",
			mutate: func(s string) string {
				return strings.Replace(s, "ingress: []", "ingress:\n    - {}", 1)
			},
		},
		{
			name: "drop project scoping from sandbox selector",
			mutate: func(s string) string {
				return strings.ReplaceAll(s, "kodema.dev/project: {{ $project | quote }}", "")
			},
		},
		{
			name: "allow sandbox-to-sandbox ingress",
			mutate: func(s string) string {
				return strings.Replace(s, "app.kubernetes.io/component: opencode-brain\n              kodema.dev/tenant: {{ $tenant | quote }}\n              kodema.dev/project: {{ $project | quote }}", "kodema.dev/role: sandbox", 1)
			},
		},
		{
			name: "place sandbox policy in release namespace",
			mutate: func(s string) string {
				return strings.Replace(s, "namespace: kodema-tenant-{{ $tenant }}", "namespace: {{ $.Release.Namespace }}", 1)
			},
		},
		{
			name: "add egress rule to sandbox policy",
			mutate: func(s string) string {
				return s + "\n  egress:\n    - {}\n"
			},
		},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			mutated := m.mutate(body)
			if mutated == body {
				t.Fatalf("mutation %q did not change template", m.name)
			}
			if err := checkOpenCodeBrainNetworkPolicyTemplate(mutated); err == nil {
				t.Fatalf("checker accepted mutation %q", m.name)
			}
		})
	}
}

func TestOpenCodeBrainSchemaValidation(t *testing.T) {
	schema := chartSchema(t)
	properties := objectAt(t, schema["properties"], "schema.properties")
	brain := objectAt(t, properties["opencodeBrain"], "schema.properties.opencodeBrain")
	if allowed, ok := brain["additionalProperties"].(bool); !ok || allowed {
		t.Errorf("opencodeBrain additionalProperties = %v, want false", brain["additionalProperties"])
	}

	// Enabled with a valid project entry must pass schema validation.
	valid := fqdnValues([]any{})
	valid["opencodeBrain"] = map[string]any{
		"enabled":     true,
		"sandboxPort": float64(8090),
		"projects": []any{
			map[string]any{
				"name":    "app-brain",
				"tenant":  "tenant-a",
				"project": "project-a",
			},
		},
	}
	if err := validateAgainstSchema(schema, valid, "values"); err != nil {
		t.Fatalf("schema refused valid opencodeBrain values: %v", err)
	}

	// Enabled with empty projects list must be refused.
	emptyProjects := fqdnValues([]any{})
	emptyProjects["opencodeBrain"] = map[string]any{
		"enabled":  true,
		"projects": []any{},
	}
	if err := validateAgainstSchema(schema, emptyProjects, "values"); err == nil {
		t.Fatal("schema accepted opencodeBrain.enabled: true with empty projects")
	}

	// Privileged port (<1024) must be refused.
	badPort := fqdnValues([]any{})
	badPort["opencodeBrain"] = map[string]any{
		"enabled":     true,
		"sandboxPort": float64(80),
		"projects": []any{
			map[string]any{
				"name":    "app-brain",
				"tenant":  "tenant-a",
				"project": "project-a",
			},
		},
	}
	if err := validateAgainstSchema(schema, badPort, "values"); err == nil {
		t.Fatal("schema accepted opencodeBrain privileged sandboxPort 80")
	}
}

// TestOpenCodeBrainSandboxSelectorsMatchRuntimeKubeLabels cross-checks every
// sandbox podSelector label key/value in workloads-opencode-brain.yaml against
// the exact exported constants and manifest emitters in
// internal/shell/runtime/kube/{api,manifest}.go so NetworkPolicy selectors and
// real Kubernetes workload pod labels can never drift out of sync.
func TestOpenCodeBrainSandboxSelectorsMatchRuntimeKubeLabels(t *testing.T) {
	kubeManifest := readText(t, filepath.Join(repoRoot(t), "internal", "adapter", "runtime", "kube", "manifest.go"))
	template := readText(t, chartPath(t, "templates", "workloads-opencode-brain.yaml"))

	tenantKey := readKubeRuntimeConst(t, "LabelTenant")
	projectKey := readKubeRuntimeConst(t, "LabelProject")
	roleKey := readKubeRuntimeConst(t, "LabelRole")
	roleSandbox := readKubeRuntimeConst(t, "RoleSandbox")

	// Verify manifest.go actually emits every one of those labels onto Job/Pod labels.
	for _, wantRef := range []string{
		"LabelTenant:",
		"labels[LabelProject]",
		"labels[LabelRole]",
		"maps.Copy(podLabels, labels)",
		"PodLabels:",
	} {
		if !strings.Contains(kubeManifest, wantRef) {
			t.Fatalf("internal/adapter/runtime/kube/manifest.go missing expected pod label emission %q", wantRef)
		}
	}

	// Verify both the Brain egress sandbox podSelector AND the Sandbox ingress
	// top-level podSelector select strictly using the exact runtime labels.
	parts := strings.Split(template, "\n---\n")
	if len(parts) != 3 {
		t.Fatalf("workloads-opencode-brain.yaml sections = %d; want 3", len(parts))
	}
	brainDoc, sandboxDoc := parts[1], parts[2]

	// Extract every podSelector -> matchLabels block strictly by indentation.
	lines := strings.Split(brainDoc+"\n"+sandboxDoc, "\n")
	matchedSandboxBlocks := 0
	kvRe := regexp.MustCompile(`^([ \t]*)([^:\s]+):[ \t]*(.+?)[ \t]*$`)
	allowedSandboxKeys := map[string]bool{
		tenantKey:  true,
		projectKey: true,
		roleKey:    true,
	}
	indentOf := func(s string) int {
		return len(s) - len(strings.TrimLeft(s, " \t"))
	}
	for i := 0; i+1 < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != "podSelector:" {
			continue
		}
		if strings.TrimSpace(lines[i+1]) != "matchLabels:" {
			continue
		}
		matchIndent := indentOf(lines[i+1])
		seenKeys := map[string]string{}
		for j := i + 2; j < len(lines); j++ {
			trimmed := strings.TrimSpace(lines[j])
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			if indentOf(lines[j]) <= matchIndent {
				break
			}
			kv := kvRe.FindStringSubmatch(lines[j])
			if kv == nil {
				continue
			}
			seenKeys[kv[2]] = kv[3]
		}
		if _, isSandbox := seenKeys[roleKey]; !isSandbox {
			continue
		}
		matchedSandboxBlocks++
		for key := range seenKeys {
			if !allowedSandboxKeys[key] {
				t.Errorf("sandbox podSelector references label key %q which is not emitted by internal/shell/runtime/kube", key)
			}
		}
		for reqKey := range allowedSandboxKeys {
			if _, ok := seenKeys[reqKey]; !ok {
				t.Errorf("sandbox podSelector missing required runtime label key %q (got keys: %v)", reqKey, seenKeys)
			}
		}
		if gotRole := strings.Trim(seenKeys[roleKey], `"'`); gotRole != roleSandbox {
			t.Errorf("sandbox podSelector %s = %q; want kube.RoleSandbox (%q)", roleKey, gotRole, roleSandbox)
		}
	}
	if matchedSandboxBlocks != 2 {
		t.Fatalf("matched %d sandbox podSelector blocks in workloads-opencode-brain.yaml; want 2 (Brain egress + Sandbox ingress)", matchedSandboxBlocks)
	}
}
