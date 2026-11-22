// Kubernetes runtime adapter API surface, version/capabilities declarations,
// configuration validation, stable claim naming, and pinned label vocabulary.
package kube

import (
	"context"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// TestAdapterSatisfiesRuntimeClient is a compile-time proof that the adapter
// exposes exactly the declared runtime seam and no parallel vocabulary.
var _ runtime.Client = (*Adapter)(nil)

// TestAdapterSatisfiesWorkspace is a compile-time proof that the adapter also
// implements the declared Kernel retained-workspace port.
var _ reconcile.Workspace = (*Adapter)(nil)

// TestAdapterPinsKubernetesAPIVersion pins batch/v1 and the Kubernetes family.
func TestAdapterPinsKubernetesAPIVersion(t *testing.T) {
	adapter := newTestAdapter(t, newFakeCluster(), newFakeClock())

	version, err := adapter.Version(context.Background())
	if err != nil {
		t.Fatalf("Version returned error: %v", err)
	}
	want := runtime.APIVersion{Adapter: runtime.AdapterKubernetes, Version: "batch/v1"}
	if version != want {
		t.Fatalf("Version = %+v; want %+v", version, want)
	}

	var pinned bool
	for _, candidate := range runtime.PinnedAPIVersions() {
		if candidate.Adapter == runtime.AdapterKubernetes && candidate.Version == version.Version {
			pinned = true
		}
	}
	if !pinned {
		t.Fatalf("adapter version %q is not the declared Kubernetes API version", version.Version)
	}
}

// TestAdapterCapabilitiesAreExplicit pins the Kubernetes capability set and
// proves it makes no process or process-group claim.
func TestAdapterCapabilitiesAreExplicit(t *testing.T) {
	fake := newFakeCluster()
	adapter := newTestAdapter(t, fake, newFakeClock())

	capabilities, err := adapter.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities returned error: %v", err)
	}
	want := map[string]bool{}
	for _, capability := range runtime.CapabilitiesFor(runtime.AdapterKubernetes) {
		want[string(capability)] = true
	}
	if len(capabilities.Names) != len(want) {
		t.Fatalf("capabilities = %v; want the %d declared Kubernetes capabilities", capabilities.Names, len(want))
	}
	for _, name := range capabilities.Names {
		if !want[name] {
			t.Errorf("capabilities declare %q, which the declared Kubernetes family does not have", name)
		}
	}
	for _, forbidden := range []string{
		string(runtime.CapabilityProcess),
		string(runtime.CapabilityProcessGroupStop),
	} {
		if capabilities.Supports(forbidden) {
			t.Errorf("kubernetes adapter claims %q", forbidden)
		}
	}
	if !capabilities.Supports(string(runtime.CapabilityJobDeletionNotStopProof)) {
		t.Error("kubernetes adapter does not mark Job deletion as non-proof of stop")
	}
	if calls := fake.callTotal(); calls != 0 {
		t.Fatalf("Capabilities made %d external calls; it must be declaration-only", calls)
	}
}

// TestCheckRequirementsRefusesUnsupportedWork pins pre-dispatch capability
// refusal. A selected adapter that cannot satisfy an admitted requirement must
// refuse the work instead of degrading silently.
func TestCheckRequirementsRefusesUnsupportedWork(t *testing.T) {
	if err := CheckRequirements(nil); err != nil {
		t.Fatalf("CheckRequirements(nil) = %v; want nil", err)
	}

	supported := make([]string, 0, 5)
	for _, capability := range runtime.CapabilitiesFor(runtime.AdapterKubernetes) {
		supported = append(supported, string(capability))
	}
	if err := CheckRequirements(supported); err != nil {
		t.Fatalf("CheckRequirements(%v) = %v; want nil", supported, err)
	}

	for _, required := range []string{
		string(runtime.CapabilityProcess),
		string(runtime.CapabilityProcessGroupStop),
		"gpu",
	} {
		err := CheckRequirements([]string{string(runtime.CapabilityContainer), required})
		if err == nil {
			t.Fatalf("CheckRequirements accepted unsupported capability %q", required)
		}
		class, ok := fault.ClassOf(err)
		if !ok || class != fault.UnsupportedCapability {
			t.Fatalf("CheckRequirements(%q) error class = %v (present=%v); want %q", required, class, ok, fault.UnsupportedCapability)
		}
	}
}

// TestNewRefusesIncompleteConfiguration proves the adapter cannot be composed
// with an unknown namespace, tenant, limits or cleanup bound.
func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	fake := newFakeCluster()
	clock := newFakeClock()

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "nil cluster", mutate: func(config *Config) { config.Cluster = nil }},
		{name: "empty namespace", mutate: func(config *Config) { config.Namespace = "" }},
		{name: "empty tenant", mutate: func(config *Config) { config.TenantID = "" }},
		{name: "empty url", mutate: func(config *Config) { config.KodemaURL = "" }},
		{name: "empty cpu limit", mutate: func(config *Config) { config.Policy.CPULimit = "" }},
		{name: "zero ttl", mutate: func(config *Config) { config.Policy.JobTTLSeconds = 0 }},
		{name: "zero cleanup attempts", mutate: func(config *Config) { config.Cleanup.Attempts = 0 }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			config := testConfig(fake, clock)
			testCase.mutate(&config)
			if _, err := New(config); err == nil {
				t.Fatalf("New accepted configuration with %s", testCase.name)
			}
		})
	}

	if _, err := New(testConfig(fake, clock)); err != nil {
		t.Fatalf("New refused a complete configuration: %v", err)
	}
}

// TestPinnedLabelVocabulary pins the exact ownership/selector keys the manifests
// and pod selectors share, so a deployment network policy and the adapter agree.
func TestPinnedLabelVocabulary(t *testing.T) {
	pairs := map[string]string{
		LabelTenant:              "kodema.dev/tenant",
		LabelExecution:           "kodema.dev/execution",
		LabelAction:              "kodema.dev/action",
		LabelWorkload:            "kodema.dev/workload",
		LabelWorkspace:           "kodema.dev/workspace",
		AnnotationIdempotencyKey: "kodema.dev/idempotency-key",
	}
	for got, want := range pairs {
		if got != want {
			t.Errorf("pinned key = %q; want %q", got, want)
		}
	}
	if Redacted != "[REDACTED]" {
		t.Errorf("Redacted = %q; want %q", Redacted, "[REDACTED]")
	}
	if MaxLogBytes <= 0 {
		t.Errorf("MaxLogBytes = %d; want a positive bound", MaxLogBytes)
	}
}

// TestWorkspaceClaimNameIsStable pins deterministic, workspace-only naming so
// the Shell can provision the retained claim before Prepare and the adapter can
// attach exactly that claim after a restart.
func TestWorkspaceClaimNameIsStable(t *testing.T) {
	first, err := WorkspaceClaimName(testWorkspaceID())
	if err != nil {
		t.Fatalf("WorkspaceClaimName returned error: %v", err)
	}
	second, err := WorkspaceClaimName(testWorkspaceID())
	if err != nil {
		t.Fatalf("WorkspaceClaimName returned error: %v", err)
	}
	if first != second {
		t.Fatalf("WorkspaceClaimName is not stable: %q then %q", first, second)
	}
	if first == "" {
		t.Fatal("WorkspaceClaimName returned an empty name")
	}

	other, err := WorkspaceClaimName("workspace-2")
	if err != nil {
		t.Fatalf("WorkspaceClaimName returned error: %v", err)
	}
	if other == first {
		t.Fatalf("distinct workspaces share claim name %q", first)
	}

	if _, err := WorkspaceClaimName("bad workspace"); err == nil {
		t.Fatal("WorkspaceClaimName accepted an invalid workspace id")
	}
}

// testIdentity is the persisted effect identity every operation must reuse.
func testIdentity() reconcile.EffectIdentity {
	return reconcile.EffectIdentity{
		ExecutionID:    "exec-1",
		ActionID:       "action-1",
		WorkloadID:     "workload-1",
		IdempotencyKey: "idem-1",
	}
}

func testWorkspaceID() id.WorkspaceID {
	return "workspace-1"
}

// testDeadline is a fixed authoritative time; tests never read the wall clock.
var testDeadline = time.Date(2026, time.November, 3, 12, 0, 0, 0, time.UTC)
