// Podman runtime adapter API surface, version/capabilities declarations,
// configuration validation, stable naming, and pinned label vocabulary.
package podman

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

func TestAdapterPinsLibpodAPIVersion(t *testing.T) {
	libpod := newFakeLibpod()
	adapter, err := New(Config{
		Libpod:    libpod,
		Files:     newFakeFiles(),
		Root:      t.TempDir(),
		KodemaURL: testKodemaURL,
		Cleanup:   CleanupPolicy{Attempts: 1},
		Clock:     newFakeClock(),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	version, err := adapter.Version(context.Background())
	if err != nil {
		t.Fatalf("Version returned error: %v", err)
	}
	if version.Adapter != runtime.AdapterPodman {
		t.Fatalf("version adapter = %q, want %q", version.Adapter, runtime.AdapterPodman)
	}
	if version.Version != "libpod/v4" {
		t.Fatalf("version = %q, want libpod/v4", version.Version)
	}
	if calls := libpod.callTotal(); calls != 0 {
		t.Fatalf("Version made %d libpod calls, want 0", calls)
	}

	// The runtime contract pins the same version for the Podman family, so the
	// adapter and the contract cannot drift apart.
	var pinned bool
	for _, candidate := range runtime.PinnedAPIVersions() {
		if candidate.Adapter != runtime.AdapterPodman {
			continue
		}
		pinned = true
		if candidate.Version != version.Version {
			t.Fatalf("pinned podman version = %q, want %q", candidate.Version, version.Version)
		}
	}
	if !pinned {
		t.Fatal("runtime contract has no pinned podman API version")
	}
}

func TestAdapterCapabilitiesAreExplicit(t *testing.T) {
	libpod := newFakeLibpod()
	adapter, err := New(Config{
		Libpod:    libpod,
		Files:     newFakeFiles(),
		Root:      t.TempDir(),
		KodemaURL: testKodemaURL,
		Cleanup:   CleanupPolicy{Attempts: 1},
		Clock:     newFakeClock(),
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	capabilities, err := adapter.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities returned error: %v", err)
	}
	want := []string{
		string(runtime.CapabilityContainer),
		string(runtime.CapabilityRetainedWorkspace),
		string(runtime.CapabilityCredentialRefresh),
		string(runtime.CapabilityProcessGroupStop),
	}
	if len(capabilities.Names) != len(want) {
		t.Fatalf("capabilities = %v, want exactly %v", capabilities.Names, want)
	}
	for _, name := range want {
		if !capabilities.Supports(name) {
			t.Fatalf("capabilities %v missing %q", capabilities.Names, name)
		}
	}
	for _, forbidden := range []string{
		string(runtime.CapabilityProcess),
		string(runtime.CapabilityNetworkIsolation),
		string(runtime.CapabilityJobDeletionNotStopProof),
	} {
		if capabilities.Supports(forbidden) {
			t.Fatalf("podman adapter claims %q", forbidden)
		}
	}
	if calls := libpod.callTotal(); calls != 0 {
		t.Fatalf("Capabilities made %d libpod calls, want 0", calls)
	}
}

func TestCheckRequirementsRefusesUnsupportedWork(t *testing.T) {
	libpod := newFakeLibpod()
	// CheckRequirements is a pure declaration check; no adapter or libpod call
	// is needed to exercise it.
	for _, name := range []string{
		string(runtime.CapabilityContainer),
		string(runtime.CapabilityRetainedWorkspace),
		string(runtime.CapabilityCredentialRefresh),
		string(runtime.CapabilityProcessGroupStop),
	} {
		if err := CheckRequirements([]string{name}); err != nil {
			t.Fatalf("CheckRequirements(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range []string{
		string(runtime.CapabilityProcess),
		string(runtime.CapabilityNetworkIsolation),
		string(runtime.CapabilityJobDeletionNotStopProof),
		"not-a-capability",
	} {
		err := CheckRequirements([]string{name})
		assertFaultClass(t, err, fault.UnsupportedCapability)
	}
	if calls := libpod.callTotal(); calls != 0 {
		t.Fatalf("CheckRequirements made %d libpod calls, want 0", calls)
	}
}

func TestNewRefusesIncompleteConfiguration(t *testing.T) {
	valid := func() Config {
		return Config{
			Libpod:    newFakeLibpod(),
			Files:     newFakeFiles(),
			Root:      t.TempDir(),
			KodemaURL: testKodemaURL,
			Cleanup:   CleanupPolicy{Attempts: 2, Backoff: time.Second},
			Clock:     newFakeClock(),
		}
	}
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing libpod", func(config *Config) { config.Libpod = nil }},
		{"missing files", func(config *Config) { config.Files = nil }},
		{"relative root", func(config *Config) { config.Root = "relative/path" }},
		{"empty root", func(config *Config) { config.Root = "" }},
		{"empty kodema url", func(config *Config) { config.KodemaURL = "  " }},
		{"zero attempts", func(config *Config) { config.Cleanup.Attempts = 0 }},
		{"negative backoff", func(config *Config) { config.Cleanup.Backoff = -time.Second }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			config := valid()
			testCase.mutate(&config)
			if adapter, err := New(config); err == nil || adapter != nil {
				t.Fatalf("New(%s) = (%v, %v), want refusal", testCase.name, adapter, err)
			} else {
				assertFaultClass(t, err, fault.InvalidInput)
			}
		})
	}
	config := valid()
	if adapter, err := New(config); err != nil || adapter == nil {
		t.Fatalf("New(valid) = (%v, %v), want an adapter", adapter, err)
	}
}

func TestStableIdentityNames(t *testing.T) {
	first, err := ContainerName("workload-1")
	if err != nil {
		t.Fatalf("ContainerName returned error: %v", err)
	}
	again, err := ContainerName("workload-1")
	if err != nil {
		t.Fatalf("ContainerName returned error: %v", err)
	}
	other, err := ContainerName("workload-2")
	if err != nil {
		t.Fatalf("ContainerName returned error: %v", err)
	}
	if first != again {
		t.Fatalf("ContainerName is not stable: %q != %q", first, again)
	}
	if first == other {
		t.Fatalf("distinct workloads share container name %q", first)
	}
	if first == "" || strings.ToLower(first) != first || strings.ContainsAny(first, " /:@") {
		t.Fatalf("container name %q is not a safe stable name", first)
	}
	if _, err := ContainerName(""); err == nil {
		t.Fatal("ContainerName accepted an empty workload id")
	}

	volume, err := WorkspaceVolumeName("workspace-1")
	if err != nil {
		t.Fatalf("WorkspaceVolumeName returned error: %v", err)
	}
	volumeAgain, err := WorkspaceVolumeName("workspace-1")
	if err != nil {
		t.Fatalf("WorkspaceVolumeName returned error: %v", err)
	}
	otherVolume, err := WorkspaceVolumeName("workspace-2")
	if err != nil {
		t.Fatalf("WorkspaceVolumeName returned error: %v", err)
	}
	if volume != volumeAgain {
		t.Fatalf("WorkspaceVolumeName is not stable: %q != %q", volume, volumeAgain)
	}
	if volume == otherVolume {
		t.Fatalf("distinct workspaces share volume name %q", volume)
	}
	if volume == "" || strings.ToLower(volume) != volume || strings.ContainsAny(volume, " /:@") {
		t.Fatalf("volume name %q is not a safe stable name", volume)
	}
	if _, err := WorkspaceVolumeName(""); err == nil {
		t.Fatal("WorkspaceVolumeName accepted an empty workspace id")
	}
}

func TestPinnedLabelVocabulary(t *testing.T) {
	want := map[string]string{
		"LabelExecution":      "kodema.dev/execution",
		"LabelAction":         "kodema.dev/action",
		"LabelWorkload":       "kodema.dev/workload",
		"LabelWorkspace":      "kodema.dev/workspace",
		"LabelProject":        "kodema.dev/project",
		"LabelRole":           "kodema.dev/role",
		"RoleSandbox":         "sandbox",
		"LabelIdempotencyKey": "kodema.dev/idempotency-key",
		"LabelPreparation":    "kodema.dev/preparation",
		"WorkloadCAPath":      "/etc/kodema/ca/ca.crt",
		"WorkloadCAMountPath": "/etc/kodema/ca",
		"WorkloadCAKey":       "ca.crt",
	}
	got := map[string]string{
		"LabelExecution":      LabelExecution,
		"LabelAction":         LabelAction,
		"LabelWorkload":       LabelWorkload,
		"LabelWorkspace":      LabelWorkspace,
		"LabelProject":        LabelProject,
		"LabelRole":           LabelRole,
		"RoleSandbox":         RoleSandbox,
		"LabelIdempotencyKey": LabelIdempotencyKey,
		"LabelPreparation":    LabelPreparation,
		"WorkloadCAPath":      WorkloadCAPath,
		"WorkloadCAMountPath": WorkloadCAMountPath,
		"WorkloadCAKey":       WorkloadCAKey,
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("%s = %q, want %q", name, got[name], value)
		}
	}
	if ErrRedactionSourceUnavailable == nil {
		t.Fatal("ErrRedactionSourceUnavailable is nil")
	}
}

var _ runtime.Client = (*Adapter)(nil)
var _ reconcile.Workspace = (*Adapter)(nil)
