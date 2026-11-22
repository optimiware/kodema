package container

import (
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func validIdentity() reconcile.EffectIdentity {
	return reconcile.EffectIdentity{
		ExecutionID:    "exec-1",
		ActionID:       "act-1",
		WorkloadID:     "workload-1",
		IdempotencyKey: "idem-1",
	}
}

func assertFaultClass(t *testing.T, err error, want fault.Class) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error of class %q, got nil", want)
	}
	got, ok := fault.ClassOf(err)
	if !ok || got != want {
		t.Fatalf("error = %v (class %q, ok=%v); want class %q", err, got, ok, want)
	}
}

func TestPinnedConstants(t *testing.T) {
	if WorkloadCAPath != "/etc/kodema/ca/ca.crt" {
		t.Errorf("WorkloadCAPath = %q; want /etc/kodema/ca/ca.crt", WorkloadCAPath)
	}
	if WorkloadCAMountPath != "/etc/kodema/ca" {
		t.Errorf("WorkloadCAMountPath = %q; want /etc/kodema/ca", WorkloadCAMountPath)
	}
	if WorkloadCAKey != "ca.crt" {
		t.Errorf("WorkloadCAKey = %q; want ca.crt", WorkloadCAKey)
	}
	if ActivationRoot != "/kodema" {
		t.Errorf("ActivationRoot = %q; want /kodema", ActivationRoot)
	}
	if WorkloadScriptPath != "/kodema/script" {
		t.Errorf("WorkloadScriptPath = %q; want /kodema/script", WorkloadScriptPath)
	}
	if ReadOnlyFileMode != 0o444 || DirectoryMode != 0o755 || SecretDirectoryMode != 0o700 {
		t.Errorf("modes = (%#o, %#o, %#o); want (0444, 0755, 0700)", ReadOnlyFileMode, DirectoryMode, SecretDirectoryMode)
	}
	if MaxLogBytes != 1<<20 || Redacted != "[REDACTED]" {
		t.Errorf("log constants = (%d, %q)", MaxLogBytes, Redacted)
	}
	if DefaultMemoryBytes != 2<<30 || DefaultMaxMemoryBytes != 4<<30 {
		t.Errorf("memory defaults = (%d, %d)", DefaultMemoryBytes, DefaultMaxMemoryBytes)
	}
	if DefaultCPUMillicores != 2000 || DefaultMaxCPUMillicores != 4000 {
		t.Errorf("cpu defaults = (%d, %d)", DefaultCPUMillicores, DefaultMaxCPUMillicores)
	}
	labels := map[string]string{
		"tenant":      LabelTenant,
		"project":     LabelProject,
		"role":        LabelRole,
		"execution":   LabelExecution,
		"action":      LabelAction,
		"workload":    LabelWorkload,
		"workspace":   LabelWorkspace,
		"idempotency": LabelIdempotencyKey,
		"preparation": LabelPreparation,
		"retained":    LabelRetained,
		"cache-key":   LabelCacheKey,
		"cache":       LabelCache,
	}
	for name, key := range labels {
		if !strings.HasPrefix(key, "kodema.dev/") {
			t.Errorf("label %s = %q; want kodema.dev/ prefix", name, key)
		}
	}
	if LabelCacheKey != "kodema.dev/cache-key" || LabelCache != "kodema.dev/cache" {
		t.Errorf("cache labels = (%q, %q); want (kodema.dev/cache-key, kodema.dev/cache)", LabelCacheKey, LabelCache)
	}
	if RoleSandbox != "sandbox" {
		t.Errorf("RoleSandbox = %q; want sandbox", RoleSandbox)
	}
}

func TestValidateIdentity(t *testing.T) {
	if err := ValidateIdentity(validIdentity()); err != nil {
		t.Fatalf("ValidateIdentity(valid) = %v; want nil", err)
	}

	cases := []struct {
		name   string
		mutate func(*reconcile.EffectIdentity)
	}{
		{"invalid workload", func(i *reconcile.EffectIdentity) { i.WorkloadID = "bad workload" }},
		{"empty workload", func(i *reconcile.EffectIdentity) { i.WorkloadID = "" }},
		{"invalid execution", func(i *reconcile.EffectIdentity) { i.ExecutionID = "bad exec" }},
		{"invalid action", func(i *reconcile.EffectIdentity) { i.ActionID = "" }},
		{"empty idempotency key", func(i *reconcile.EffectIdentity) { i.IdempotencyKey = "   " }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ident := validIdentity()
			tc.mutate(&ident)
			assertFaultClass(t, ValidateIdentity(ident), fault.InvalidInput)
		})
	}
}

func TestValidateWorkspaceIDAndResourceName(t *testing.T) {
	ws := id.WorkspaceID("ws-123")
	if err := ValidateWorkspaceID(ws); err != nil {
		t.Fatalf("ValidateWorkspaceID(valid) = %v; want nil", err)
	}
	name, err := WorkspaceResourceName(ws)
	if err != nil {
		t.Fatalf("WorkspaceResourceName(valid) = %v; want nil", err)
	}
	if !strings.HasPrefix(name, "kodema-workspace-") {
		t.Fatalf("WorkspaceResourceName = %q; want kodema-workspace- prefix", name)
	}
	if want := "kodema-workspace-" + StableDigest(string(ws)); name != want {
		t.Fatalf("WorkspaceResourceName = %q; want %q", name, want)
	}

	name2, err := WorkspaceResourceName(ws)
	if err != nil || name2 != name {
		t.Fatalf("WorkspaceResourceName is not deterministic: %q vs %q (%v)", name, name2, err)
	}
	other, err := WorkspaceResourceName("ws-456")
	if err != nil || other == name {
		t.Fatalf("distinct workspaces collided: %q vs %q (%v)", name, other, err)
	}

	assertFaultClass(t, ValidateWorkspaceID(""), fault.InvalidInput)
	assertFaultClass(t, ValidateWorkspaceID("bad workspace"), fault.InvalidInput)
	if _, err := WorkspaceResourceName("bad workspace"); err == nil {
		t.Fatal("WorkspaceResourceName accepted invalid workspace id")
	} else {
		assertFaultClass(t, err, fault.InvalidInput)
	}
}

func TestValidatePrepareContext(t *testing.T) {
	if err := ValidatePrepareContext("build-step", ""); err != nil {
		t.Fatalf("task-free context refused: %v", err)
	}
	if err := ValidatePrepareContext("build-step", "task-1"); err != nil {
		t.Fatalf("tasked context refused: %v", err)
	}
	assertFaultClass(t, ValidatePrepareContext("BAD_ACTION", ""), fault.InvalidInput)
	assertFaultClass(t, ValidatePrepareContext("", ""), fault.InvalidInput)
	assertFaultClass(t, ValidatePrepareContext("build-step", "bad task"), fault.InvalidInput)
}

func TestStableDigestAndDescribeIdentity(t *testing.T) {
	d1 := StableDigest("workload-1")
	d2 := StableDigest("workload-1")
	d3 := StableDigest("workload-2")
	if len(d1) != 24 {
		t.Fatalf("StableDigest length = %d; want 24", len(d1))
	}
	if d1 != d2 {
		t.Fatalf("StableDigest not deterministic: %q != %q", d1, d2)
	}
	if d1 == d3 {
		t.Fatalf("StableDigest collision for distinct inputs: %q", d1)
	}
	for _, r := range d1 {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			t.Fatalf("StableDigest %q contains non-lowercase-hex char %q", d1, r)
		}
	}

	desc := DescribeIdentity(validIdentity())
	want := `workload "workload-1" (execution "exec-1" action "act-1")`
	if desc != want {
		t.Fatalf("DescribeIdentity = %q; want %q", desc, want)
	}
}

func TestCacheResourceName(t *testing.T) {
	validKey := "0123456789abcdef0123456789abcdef"
	name, err := CacheResourceName(validKey)
	if err != nil {
		t.Fatalf("CacheResourceName(32-hex) = %v; want nil", err)
	}
	want := "kodema-cache-" + validKey
	if name != want {
		t.Fatalf("CacheResourceName = %q; want %q", name, want)
	}
	name2, err := CacheResourceName(validKey)
	if err != nil || name2 != name {
		t.Fatalf("CacheResourceName is not deterministic: %q vs %q (%v)", name, name2, err)
	}

	// Boundary valid lengths: 16 hex chars (minimum) and 50 hex chars (63-char total name).
	if got, err := CacheResourceName(strings.Repeat("a", 16)); err != nil || got != "kodema-cache-"+strings.Repeat("a", 16) {
		t.Fatalf("CacheResourceName(16-hex) = (%q, %v)", got, err)
	}
	if got, err := CacheResourceName(strings.Repeat("b", 50)); err != nil || len(got) != 63 {
		t.Fatalf("CacheResourceName(50-hex) = (%q len=%d, %v); want len 63", got, len(got), err)
	}

	invalid := []struct {
		name string
		key  string
	}{
		{name: "empty", key: ""},
		{name: "too short (15 hex)", key: strings.Repeat("a", 15)},
		{name: "too long for 63-char name (51 hex)", key: strings.Repeat("a", 51)},
		{name: "too long (65 hex)", key: strings.Repeat("a", 65)},
		{name: "uppercase hex", key: strings.Repeat("A", 32)},
		{name: "mixed-case hex", key: "0123456789ABCDEF0123456789abcdef"},
		{name: "non-hex letter", key: strings.Repeat("g", 32)},
		{name: "hyphen", key: "0123456789abcdef-0123456789abcde"},
		{name: "whitespace", key: "0123456789abcdef 0123456789abcde"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CacheResourceName(tc.key)
			if err == nil {
				t.Fatalf("CacheResourceName(%q) = %q; want InvalidInput", tc.key, got)
			}
			assertFaultClass(t, err, fault.InvalidInput)
		})
	}
}
