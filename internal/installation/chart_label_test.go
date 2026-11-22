package installation

// Tenant-label contract tests.
//
// The Kubernetes runtime adapter is the reference for the tenant label key:
// internal/shell/runtime/kube/api.go declares
//
//	LabelTenant = "kodema.dev/tenant"
//
// and the adapter refuses a tenant namespace whose labels do not carry that key
// with the tenant id (internal/shell/runtime/kube/adapter.go checkTenantNamespace
// and Prepare; internal/shell/runtime/kube/workspace.go checkTenantNamespace).
// A namespace created by this chart must therefore use exactly that key, or a
// real cluster deployment fails the adapter's ownership check.
//
// internal/installation may not import internal/shell/runtime/kube (the
// installation layer guard only permits the standard library, api/**,
// internal/id and internal/fault), so the expected key is pinned here and also
// read back from the adapter source text. A drift in either direction fails the
// test instead of silently testing a stale literal.

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// pinnedTenantLabel mirrors kube.LabelTenant at
// internal/shell/runtime/kube/api.go:30.
const pinnedTenantLabel = "kodema.dev/tenant"

// tenantLabelTokenPattern matches any label-like token whose final segment
// contains "tenant" (for example kodema.io/tenant or kodema.dev/tenant).
var tenantLabelTokenPattern = regexp.MustCompile(`[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9._-]*[Tt]enant[A-Za-z0-9._-]*`)

// readKubeRuntimeConst resolves a constant declared in
// internal/adapter/runtime/kube/api.go. It accepts either a direct string
// literal declaration or a direct alias to a constant in
// internal/adapter/runtime/container/constants.go, verifying both the alias
// and the underlying literal string so neither file can drift silently.
func readKubeRuntimeConst(t *testing.T, name string) string {
	t.Helper()
	apiPath := filepath.Join(repoRoot(t), "internal", "adapter", "runtime", "kube", "api.go")
	apiBody, err := os.ReadFile(apiPath)
	if err != nil {
		t.Fatalf("read adapter constant source %s: %v", apiPath, err)
	}
	literalDecl := regexp.MustCompile(`(?m)^[ \t]*` + regexp.QuoteMeta(name) + `[ \t]*=[ \t]*"([^"]+)"[ \t]*$`)
	if match := literalDecl.FindStringSubmatch(string(apiBody)); match != nil {
		return match[1]
	}
	aliasDecl := regexp.MustCompile(`(?m)^[ \t]*` + regexp.QuoteMeta(name) + `[ \t]*=[ \t]*container\.([A-Za-z0-9_]+)[ \t]*$`)
	aliasMatch := aliasDecl.FindStringSubmatch(string(apiBody))
	if aliasMatch == nil {
		t.Fatalf("%s: no %s constant declaration or container alias found", apiPath, name)
	}
	target := aliasMatch[1]
	constPath := filepath.Join(repoRoot(t), "internal", "adapter", "runtime", "container", "constants.go")
	constBody, err := os.ReadFile(constPath)
	if err != nil {
		t.Fatalf("read shared container constant source %s: %v", constPath, err)
	}
	targetDecl := regexp.MustCompile(`(?m)^[ \t]*` + regexp.QuoteMeta(target) + `[ \t]*=[ \t]*"([^"]+)"[ \t]*$`)
	targetMatch := targetDecl.FindStringSubmatch(string(constBody))
	if targetMatch == nil {
		t.Fatalf("%s: missing constant %s referenced by %s (%s)", constPath, target, apiPath, name)
	}
	return targetMatch[1]
}

// adapterTenantLabel reads the runtime adapter's LabelTenant declaration from
// its source text. It fails when the declaration disappears or no longer
// matches the pinned literal, so the chart test cannot drift away from the
// adapter it is supposed to satisfy.
func adapterTenantLabel(t *testing.T) string {
	t.Helper()
	got := readKubeRuntimeConst(t, "LabelTenant")
	if got != pinnedTenantLabel {
		t.Fatalf("adapter tenant label drift: kube.LabelTenant resolves to %q, test pins %q; update the chart and this test together",
			got, pinnedTenantLabel)
	}
	return got
}

// requireOnlyTenantLabel fails when body mentions any tenant label token other
// than want, or when it mentions none. It covers metadata labels and any
// podSelector/namespaceSelector matchLabels that reference a tenant key.
func requireOnlyTenantLabel(t *testing.T, name, body, want string) {
	t.Helper()
	tokens := tenantLabelTokenPattern.FindAllString(body, -1)
	if len(tokens) == 0 {
		t.Fatalf("%s: no tenant label key found; want %q", name, want)
	}
	for _, token := range tokens {
		if token != want {
			t.Errorf("%s: tenant label key %q does not match the Kubernetes runtime adapter label %q", name, token, want)
		}
	}
}

// requireTenantLabelValue fails unless body labels want with the rendered
// tenant value.
func requireTenantLabelValue(t *testing.T, name, body, want string) {
	t.Helper()
	expression := `(?m)^[ \t]*` + regexp.QuoteMeta(want) + `[ \t]*:[ \t]*\{\{[ \t]*\$tenant[ \t]*\|[ \t]*quote[ \t]*\}\}[ \t]*$`
	if !regexp.MustCompile(expression).MatchString(body) {
		t.Errorf("%s: does not set %q to the quoted tenant value", name, want)
	}
}

// TestTenantNamespaceUsesRuntimeAdapterLabel proves the rendered tenant
// namespace carries the adapter's ownership label, so Prepare and every
// workspace operation pass checkTenantNamespace on a real cluster.
func TestTenantNamespaceUsesRuntimeAdapterLabel(t *testing.T) {
	want := adapterTenantLabel(t)
	name := "templates/namespaces.yaml"
	body := readText(t, chartPath(t, "templates", "namespaces.yaml"))

	requireOnlyTenantLabel(t, name, body, want)
	requireTenantLabelValue(t, name, body, want)

	// The namespace name is the value the operator binds to kube.Config.Namespace;
	// the chart and the policy template must keep agreeing on it.
	if !regexp.MustCompile(`(?m)^[ \t]*name:[ \t]*kodema-tenant-\{\{[ \t]*\$tenant[ \t]*\}\}[ \t]*$`).MatchString(body) {
		t.Errorf("%s: tenant namespace name is not kodema-tenant-{{ $tenant }}", name)
	}
}

// TestTenantNetworkPolicyUsesRuntimeAdapterLabel proves the per-tenant
// NetworkPolicy objects carry the adapter's ownership label. Any tenant
// podSelector or namespaceSelector added later must use the same key; the
// whole-body token check above makes a second, foreign key fail here.
func TestTenantNetworkPolicyUsesRuntimeAdapterLabel(t *testing.T) {
	want := adapterTenantLabel(t)
	name := "templates/networkpolicy.yaml"
	body := readText(t, chartPath(t, "templates", "networkpolicy.yaml"))

	requireOnlyTenantLabel(t, name, body, want)
	requireTenantLabelValue(t, name, body, want)
}

// TestDeployDocumentationUsesRuntimeAdapterLabel keeps the deployment
// documentation from advertising a label key the adapter does not read.
func TestDeployDocumentationUsesRuntimeAdapterLabel(t *testing.T) {
	want := adapterTenantLabel(t)
	path := filepath.Join(repoRoot(t), "deploy", "README.md")
	body := readText(t, path)

	requireOnlyTenantLabel(t, "deploy/README.md", body, want)
	if !regexp.MustCompile("`" + regexp.QuoteMeta(want) + "`").MatchString(body) {
		t.Errorf("deploy/README.md does not document the runtime adapter tenant label %q", want)
	}
}
