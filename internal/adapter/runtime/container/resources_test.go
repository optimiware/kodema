package container

import (
	"errors"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/fault"
)

func TestNormalizeResourcePolicy(t *testing.T) {
	// Zero policy selects canonical defaults.
	got, err := NormalizeResourcePolicy(ResourcePolicy{})
	if err != nil {
		t.Fatalf("NormalizeResourcePolicy(zero) = %v; want nil", err)
	}
	if got.DefaultMemoryBytes != DefaultMemoryBytes ||
		got.MaxMemoryBytes != DefaultMaxMemoryBytes ||
		got.DefaultCPUMillicores != DefaultCPUMillicores ||
		got.MaxCPUMillicores != DefaultMaxCPUMillicores {
		t.Fatalf("zero policy normalized = %+v", got)
	}

	// Lower max than canonical default clamps default down to max when default is zero.
	got, err = NormalizeResourcePolicy(ResourcePolicy{
		MaxMemoryBytes:   512 << 20,
		MaxCPUMillicores: 500,
	})
	if err != nil {
		t.Fatalf("NormalizeResourcePolicy(small max) = %v; want nil", err)
	}
	if got.DefaultMemoryBytes != 512<<20 || got.DefaultCPUMillicores != 500 {
		t.Fatalf("small max policy = %+v", got)
	}

	// Larger default than canonical default max raises max when max is zero.
	got, err = NormalizeResourcePolicy(ResourcePolicy{
		DefaultMemoryBytes:   8 << 30,
		DefaultCPUMillicores: 6000,
	})
	if err != nil {
		t.Fatalf("NormalizeResourcePolicy(large default) = %v; want nil", err)
	}
	if got.MaxMemoryBytes != 8<<30 || got.MaxCPUMillicores != 6000 {
		t.Fatalf("large default policy = %+v", got)
	}

	// Negative or default > max refused.
	for _, tc := range []struct {
		name   string
		policy ResourcePolicy
	}{
		{"negative default mem", ResourcePolicy{DefaultMemoryBytes: -1}},
		{"negative max mem", ResourcePolicy{MaxMemoryBytes: -1}},
		{"negative default cpu", ResourcePolicy{DefaultCPUMillicores: -1}},
		{"negative max cpu", ResourcePolicy{MaxCPUMillicores: -1}},
		{"default mem > max mem", ResourcePolicy{DefaultMemoryBytes: 4 << 30, MaxMemoryBytes: 2 << 30}},
		{"default cpu > max cpu", ResourcePolicy{DefaultCPUMillicores: 4000, MaxCPUMillicores: 2000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeResourcePolicy(tc.policy)
			assertFaultClass(t, err, fault.InvalidInput)
		})
	}
}

func TestParseResourcePolicy(t *testing.T) {
	got, err := ParseResourcePolicy("", "", "", "")
	if err != nil {
		t.Fatalf("ParseResourcePolicy(empty) = %v", err)
	}
	if got.DefaultMemoryBytes != DefaultMemoryBytes || got.MaxCPUMillicores != DefaultMaxCPUMillicores {
		t.Fatalf("empty ParseResourcePolicy = %+v", got)
	}

	got, err = ParseResourcePolicy("512Mi", "2Gi", "500m", "2")
	if err != nil {
		t.Fatalf("ParseResourcePolicy(valid) = %v", err)
	}
	if got.DefaultMemoryBytes != 512<<20 || got.MaxMemoryBytes != 2<<30 ||
		got.DefaultCPUMillicores != 500 || got.MaxCPUMillicores != 2000 {
		t.Fatalf("parsed policy = %+v", got)
	}

	for _, tc := range []struct {
		name                           string
		defMem, maxMem, defCPU, maxCPU string
		wantField                      ResourcePolicyField
	}{
		{"bad default mem", "not-mem", "", "", "", ResourceFieldDefaultMemory},
		{"bad max mem", "", "not-mem", "", "", ResourceFieldMaxMemory},
		{"bad default cpu", "", "", "not-cpu", "", ResourceFieldDefaultCPU},
		{"bad max cpu", "", "", "", "not-cpu", ResourceFieldMaxCPU},
		{"default mem > max mem", "4Gi", "2Gi", "", "", ""},
		{"default cpu > max cpu", "", "", "4", "2", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseResourcePolicy(tc.defMem, tc.maxMem, tc.defCPU, tc.maxCPU)
			assertFaultClass(t, err, fault.InvalidInput)
			if tc.wantField != "" {
				var parseErr *ResourcePolicyParseError
				if !errors.As(err, &parseErr) {
					t.Fatalf("error %v does not wrap *ResourcePolicyParseError", err)
				}
				if parseErr.Field != tc.wantField {
					t.Fatalf("parse error field = %q; want %q", parseErr.Field, tc.wantField)
				}
			}
		})
	}
}

func TestResourcePolicyResolve(t *testing.T) {
	policy, err := ParseResourcePolicy("1Gi", "4Gi", "1", "4")
	if err != nil {
		t.Fatalf("ParseResourcePolicy = %v", err)
	}

	// Nil requested selects defaults.
	resolved, err := policy.Resolve(nil)
	if err != nil {
		t.Fatalf("Resolve(nil) = %v", err)
	}
	if resolved.MemoryBytes != 1<<30 || resolved.CPUMillicores != 1000 ||
		resolved.MemoryQuantity != "1Gi" || resolved.CPUQuantity != "1" {
		t.Fatalf("Resolve(nil) = %+v", resolved)
	}

	// Partial override preserves other default.
	resolved, err = policy.Resolve(&definition.Resources{Memory: "512Mi"})
	if err != nil {
		t.Fatalf("Resolve(512Mi) = %v", err)
	}
	if resolved.MemoryBytes != 512<<20 || resolved.MemoryQuantity != "512Mi" ||
		resolved.CPUMillicores != 1000 || resolved.CPUQuantity != "1" {
		t.Fatalf("Resolve(512Mi) = %+v", resolved)
	}

	// Full override within ceiling.
	resolved, err = policy.Resolve(&definition.Resources{Memory: "2Gi", CPU: "2500m"})
	if err != nil {
		t.Fatalf("Resolve(2Gi, 2500m) = %v", err)
	}
	if resolved.MemoryBytes != 2<<30 || resolved.MemoryQuantity != "2Gi" ||
		resolved.CPUMillicores != 2500 || resolved.CPUQuantity != "2500m" {
		t.Fatalf("Resolve(2Gi, 2500m) = %+v", resolved)
	}

	// Exceeding memory ceiling fails with clear diagnostic.
	_, err = policy.Resolve(&definition.Resources{Memory: "8Gi"})
	assertFaultClass(t, err, fault.InvalidInput)
	if !strings.Contains(err.Error(), "4 GiB") {
		t.Fatalf("memory ceiling error %q should name max", err.Error())
	}

	// Exceeding CPU ceiling fails with clear diagnostic.
	_, err = policy.Resolve(&definition.Resources{CPU: "8"})
	assertFaultClass(t, err, fault.InvalidInput)
	if !strings.Contains(err.Error(), "4") {
		t.Fatalf("CPU ceiling error %q should name max", err.Error())
	}

	// Empty non-nil resources struct is rejected by definition.ValidateResources.
	_, err = policy.Resolve(&definition.Resources{})
	assertFaultClass(t, err, fault.InvalidInput)
}

func TestFormatMemoryQuantityAndClampKubeRequest(t *testing.T) {
	cases := []struct {
		bytes int64
		want  string
	}{
		{0, ""},
		{-1, ""},
		{1 << 40, "1Ti"},
		{2 << 30, "2Gi"},
		{512 << 20, "512Mi"},
		{64 << 10, "64Ki"},
		{12345, "12345"},
	}
	for _, tc := range cases {
		if got := FormatMemoryQuantity(tc.bytes); got != tc.want {
			t.Errorf("FormatMemoryQuantity(%d) = %q; want %q", tc.bytes, got, tc.want)
		}
	}

	if got := ClampKubeRequest("512Mi", 256<<20, true); got != "256Mi" {
		t.Errorf("ClampKubeRequest(512Mi, 256Mi) = %q; want 256Mi", got)
	}
	if got := ClampKubeRequest("128Mi", 256<<20, true); got != "128Mi" {
		t.Errorf("ClampKubeRequest(128Mi, 256Mi) = %q; want 128Mi", got)
	}
	if got := ClampKubeRequest("1", 500, false); got != "500m" {
		t.Errorf("ClampKubeRequest(1, 500m) = %q; want 500m", got)
	}
	if got := ClampKubeRequest("250m", 500, false); got != "250m" {
		t.Errorf("ClampKubeRequest(250m, 500m) = %q; want 250m", got)
	}
	if got := ClampKubeRequest("512Mi", 0, true); got != "512Mi" {
		t.Errorf("ClampKubeRequest(512Mi, 0) = %q; want 512Mi", got)
	}
	if got := ClampKubeRequest("250m", -10, false); got != "250m" {
		t.Errorf("ClampKubeRequest(250m, -10) = %q; want 250m", got)
	}
}

func TestNormalizeCeilingPolicy(t *testing.T) {
	raw, err := ParseRawResourcePolicy("256Mi", "", "500m", "")
	if err != nil {
		t.Fatalf("ParseRawResourcePolicy = %v", err)
	}
	got, err := NormalizeCeilingPolicy(raw)
	if err != nil {
		t.Fatalf("NormalizeCeilingPolicy = %v", err)
	}
	if got.MaxMemoryBytes != 256<<20 || got.MaxCPUMillicores != 500 {
		t.Fatalf("NormalizeCeilingPolicy = %+v; want ceiling equal to defaults (256Mi / 500m)", got)
	}
	// Exceeding the default-pinned ceiling must be refused.
	if _, err := got.Resolve(&definition.Resources{Memory: "512Mi"}); err == nil {
		t.Fatal("expected 512Mi to exceed default-pinned 256Mi ceiling")
	}
}
