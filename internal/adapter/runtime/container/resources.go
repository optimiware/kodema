package container

import (
	"fmt"
	"strings"

	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/fault"
)

// ResourcePolicy bounds container memory and CPU limits for one installation.
// Zero fields select the canonical defaults; installation maximums are always
// enforced so a pipeline can never request unbounded host resources.
type ResourcePolicy struct {
	DefaultMemoryBytes   int64
	DefaultCPUMillicores int64
	MaxMemoryBytes       int64
	MaxCPUMillicores     int64
}

// ResolvedResources are the bounded container memory and CPU limits resolved
// for one workload, rendered both numerically and as canonical Kubernetes
// quantity strings.
type ResolvedResources struct {
	MemoryBytes    int64
	CPUMillicores  int64
	MemoryQuantity string
	CPUQuantity    string
}

// NormalizeResourcePolicy fills zero fields with canonical Podman-style bounded
// defaults (2 GiB / 2 CPU default, 4 GiB / 4 CPU ceiling) and checks that all
// limits are positive and defaults do not exceed maximums.
//
// For installations where an omitted maximum means "the default limit IS the
// ceiling" (such as Kubernetes), use NormalizeCeilingPolicy instead so an empty
// maximum never silently widens the tenant ceiling to 4 GiB / 4 CPUs.
func NormalizeResourcePolicy(policy ResourcePolicy) (ResourcePolicy, error) {
	if policy.DefaultMemoryBytes < 0 || policy.MaxMemoryBytes < 0 {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy memory limit must not be negative")
	}
	if policy.DefaultCPUMillicores < 0 || policy.MaxCPUMillicores < 0 {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy CPU limit must not be negative")
	}
	if policy.DefaultMemoryBytes == 0 {
		if policy.MaxMemoryBytes > 0 && policy.MaxMemoryBytes < DefaultMemoryBytes {
			policy.DefaultMemoryBytes = policy.MaxMemoryBytes
		} else {
			policy.DefaultMemoryBytes = DefaultMemoryBytes
		}
	}
	if policy.MaxMemoryBytes == 0 {
		policy.MaxMemoryBytes = max(DefaultMaxMemoryBytes, policy.DefaultMemoryBytes)
	}
	if policy.DefaultMemoryBytes > policy.MaxMemoryBytes {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy default memory limit exceeds maximum memory limit")
	}
	if policy.DefaultCPUMillicores == 0 {
		if policy.MaxCPUMillicores > 0 && policy.MaxCPUMillicores < DefaultCPUMillicores {
			policy.DefaultCPUMillicores = policy.MaxCPUMillicores
		} else {
			policy.DefaultCPUMillicores = DefaultCPUMillicores
		}
	}
	if policy.MaxCPUMillicores == 0 {
		policy.MaxCPUMillicores = max(DefaultMaxCPUMillicores, policy.DefaultCPUMillicores)
	}
	if policy.DefaultCPUMillicores > policy.MaxCPUMillicores {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy default CPU limit exceeds maximum CPU limit")
	}
	return policy, nil
}

// NormalizeCeilingPolicy validates a ResourcePolicy where both defaults must be
// positive and an omitted (zero) maximum pins the ceiling to the default limit
// rather than widening it to DefaultMaxMemoryBytes / DefaultMaxCPUMillicores.
func NormalizeCeilingPolicy(policy ResourcePolicy) (ResourcePolicy, error) {
	if policy.DefaultMemoryBytes <= 0 {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy default memory limit must be positive")
	}
	if policy.DefaultCPUMillicores <= 0 {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy default CPU limit must be positive")
	}
	if policy.MaxMemoryBytes < 0 {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy maximum memory limit must not be negative")
	}
	if policy.MaxCPUMillicores < 0 {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy maximum CPU limit must not be negative")
	}
	if policy.MaxMemoryBytes == 0 {
		policy.MaxMemoryBytes = policy.DefaultMemoryBytes
	}
	if policy.MaxCPUMillicores == 0 {
		policy.MaxCPUMillicores = policy.DefaultCPUMillicores
	}
	if policy.DefaultMemoryBytes > policy.MaxMemoryBytes {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy default memory limit exceeds maximum memory limit")
	}
	if policy.DefaultCPUMillicores > policy.MaxCPUMillicores {
		return ResourcePolicy{}, fault.New(fault.InvalidInput, "container resource policy default CPU limit exceeds maximum CPU limit")
	}
	return policy, nil
}

// ParseRawResourcePolicy parses optional Kubernetes-compatible memory and CPU
// quantity strings without applying default ceilings so callers can choose
// NormalizeResourcePolicy (Podman bounded defaults) or NormalizeCeilingPolicy
// (Kubernetes default-equals-max ceiling).
func ParseRawResourcePolicy(defaultMem, maxMem, defaultCPU, maxCPU string) (ResourcePolicy, error) {
	var policy ResourcePolicy
	if raw := strings.TrimSpace(defaultMem); raw != "" {
		parsed, err := definition.ParseMemoryQuantity(raw)
		if err != nil {
			return ResourcePolicy{}, fault.Wrap(fault.InvalidInput, "default memory limit is invalid", err)
		}
		policy.DefaultMemoryBytes = parsed
	}
	if raw := strings.TrimSpace(maxMem); raw != "" {
		parsed, err := definition.ParseMemoryQuantity(raw)
		if err != nil {
			return ResourcePolicy{}, fault.Wrap(fault.InvalidInput, "maximum memory limit is invalid", err)
		}
		policy.MaxMemoryBytes = parsed
	}
	if raw := strings.TrimSpace(defaultCPU); raw != "" {
		parsed, err := definition.ParseCPUQuantity(raw)
		if err != nil {
			return ResourcePolicy{}, fault.Wrap(fault.InvalidInput, "default CPU limit is invalid", err)
		}
		policy.DefaultCPUMillicores = parsed
	}
	if raw := strings.TrimSpace(maxCPU); raw != "" {
		parsed, err := definition.ParseCPUQuantity(raw)
		if err != nil {
			return ResourcePolicy{}, fault.Wrap(fault.InvalidInput, "maximum CPU limit is invalid", err)
		}
		policy.MaxCPUMillicores = parsed
	}
	return policy, nil
}

// ParseResourcePolicy parses optional Kubernetes-compatible memory and CPU
// quantity strings and returns a normalized ResourcePolicy using canonical
// Podman bounded defaults (NormalizeResourcePolicy).
func ParseResourcePolicy(defaultMem, maxMem, defaultCPU, maxCPU string) (ResourcePolicy, error) {
	policy, err := ParseRawResourcePolicy(defaultMem, maxMem, defaultCPU, maxCPU)
	if err != nil {
		return ResourcePolicy{}, err
	}
	return NormalizeResourcePolicy(policy)
}

// Resolve validates optional per-action resource hints against the policy's
// installation maximums and returns the concrete resolved memory and CPU limits.
func (p ResourcePolicy) Resolve(requested *definition.Resources) (ResolvedResources, error) {
	policy, err := NormalizeResourcePolicy(p)
	if err != nil {
		return ResolvedResources{}, err
	}
	if err := definition.ValidateResources(requested); err != nil {
		return ResolvedResources{}, err
	}
	memBytes := policy.DefaultMemoryBytes
	cpuMillicores := policy.DefaultCPUMillicores
	memQty := FormatMemoryQuantity(memBytes)
	cpuQty := definition.FormatCPUMillicores(cpuMillicores)
	if requested != nil {
		if requested.Memory != "" {
			parsed, err := definition.ParseMemoryQuantity(requested.Memory)
			if err != nil {
				return ResolvedResources{}, err
			}
			if parsed > policy.MaxMemoryBytes {
				return ResolvedResources{}, fault.New(fault.InvalidInput, fmt.Sprintf(
					"container memory limit %q (%s) exceeds installation maximum %s",
					requested.Memory,
					definition.FormatMemoryBytes(parsed),
					definition.FormatMemoryBytes(policy.MaxMemoryBytes),
				))
			}
			memBytes = parsed
			memQty = requested.Memory
		}
		if requested.CPU != "" {
			parsed, err := definition.ParseCPUQuantity(requested.CPU)
			if err != nil {
				return ResolvedResources{}, err
			}
			if parsed > policy.MaxCPUMillicores {
				return ResolvedResources{}, fault.New(fault.InvalidInput, fmt.Sprintf(
					"container CPU limit %q (%s) exceeds installation maximum %s",
					requested.CPU,
					definition.FormatCPUMillicores(parsed),
					definition.FormatCPUMillicores(policy.MaxCPUMillicores),
				))
			}
			cpuMillicores = parsed
			cpuQty = requested.CPU
		}
	}
	return ResolvedResources{
		MemoryBytes:    memBytes,
		CPUMillicores:  cpuMillicores,
		MemoryQuantity: memQty,
		CPUQuantity:    cpuQty,
	}, nil
}

// FormatMemoryQuantity renders a positive byte count in canonical Kubernetes
// quantity syntax (e.g. "2Gi", "512Mi", "64Ki", or bare bytes).
func FormatMemoryQuantity(bytes int64) string {
	if bytes <= 0 {
		return ""
	}
	const (
		kib = int64(1 << 10)
		mib = int64(1 << 20)
		gib = int64(1 << 30)
		tib = int64(1 << 40)
	)
	switch {
	case bytes%tib == 0:
		return fmt.Sprintf("%dTi", bytes/tib)
	case bytes%gib == 0:
		return fmt.Sprintf("%dGi", bytes/gib)
	case bytes%mib == 0:
		return fmt.Sprintf("%dMi", bytes/mib)
	case bytes%kib == 0:
		return fmt.Sprintf("%dKi", bytes/kib)
	default:
		return fmt.Sprintf("%d", bytes)
	}
}

// ClampKubeRequest returns requestQty unchanged unless it parses to a value
// exceeding limitValue (bytes when isMemory is true, millicores otherwise), in
// which case it returns the canonical Kubernetes quantity for limitValue so
// Kubernetes pod requests never exceed their resolved limits.
func ClampKubeRequest(requestQty string, limitValue int64, isMemory bool) string {
	if limitValue <= 0 {
		return requestQty
	}
	trimmed := strings.TrimSpace(requestQty)
	if isMemory {
		if reqBytes, err := definition.ParseMemoryQuantity(trimmed); err == nil && reqBytes > limitValue {
			return FormatMemoryQuantity(limitValue)
		}
		return requestQty
	}
	if reqMillicores, err := definition.ParseCPUQuantity(trimmed); err == nil && reqMillicores > limitValue {
		return definition.FormatCPUMillicores(limitValue)
	}
	return requestQty
}
