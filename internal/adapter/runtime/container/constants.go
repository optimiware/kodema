package container

import "os"

// Canonical in-container paths, file modes, log bounds, and default resource
// limits shared across container runtime adapters.
const (
	// WorkloadCAKey is the literal key/filename carrying the installation CA
	// bundle.
	WorkloadCAKey = "ca.crt"
	// WorkloadCAMountPath is the fixed read-only mount root of the workload CA.
	WorkloadCAMountPath = "/etc/kodema/ca"
	// WorkloadCAPath is the canonical absolute path of the mounted CA bundle
	// inside container workloads.
	WorkloadCAPath = WorkloadCAMountPath + "/" + WorkloadCAKey

	// ActivationRoot is the read-only mount root of the immutable activation
	// snapshot.
	ActivationRoot = "/kodema"
	// WorkloadScriptPath is the rendered fail-fast entrypoint script path.
	WorkloadScriptPath = "/kodema/script"

	// ReadOnlyFileMode is the explicit read-only mode for projected files.
	ReadOnlyFileMode os.FileMode = 0o444
	// DirectoryMode is the explicit mode for non-secret adapter directories.
	DirectoryMode os.FileMode = 0o755
	// SecretDirectoryMode is the confined owner-only mode for host-side secret
	// and credential directories.
	SecretDirectoryMode os.FileMode = 0o700

	// MaxLogBytes bounds one log read per stream. Requests above this bound
	// are refused before any external call.
	MaxLogBytes = 1 << 20
	// Redacted replaces configured credential and secret values in log output.
	Redacted = "[REDACTED]"

	// DefaultMemoryBytes is the default container memory limit (2 GiB).
	DefaultMemoryBytes int64 = 2 << 30
	// DefaultCPUMillicores is the default container CPU limit (2 CPUs).
	DefaultCPUMillicores int64 = 2000
	// DefaultMaxMemoryBytes is the default installation ceiling for per-action
	// memory requests (4 GiB).
	DefaultMaxMemoryBytes int64 = 4 << 30
	// DefaultMaxCPUMillicores is the default installation ceiling for
	// per-action CPU requests (4 CPUs).
	DefaultMaxCPUMillicores int64 = 4000
)

// Pinned runtime label keys and role values shared by generated manifests,
// containers, volumes, and selectors.
const (
	// LabelTenant names the owning tenant.
	LabelTenant = "kodema.dev/tenant"
	// LabelProject names the owning project when supplied by the Shell.
	LabelProject = "kodema.dev/project"
	// LabelRole names the optional workload role (for example RoleSandbox).
	LabelRole = "kodema.dev/role"
	// RoleSandbox is the pinned LabelRole value for ephemeral OpenCode sandbox
	// workloads (`kodema sandbox serve`).
	RoleSandbox = "sandbox"
	// LabelExecution names the execution.
	LabelExecution = "kodema.dev/execution"
	// LabelAction names the action.
	LabelAction = "kodema.dev/action"
	// LabelWorkload names the stable runtime workload identity.
	LabelWorkload = "kodema.dev/workload"
	// LabelWorkspace names the retained execution workspace.
	LabelWorkspace = "kodema.dev/workspace"
	// LabelIdempotencyKey carries the persisted idempotency key so a retry can
	// be attributed to the original effect.
	LabelIdempotencyKey = "kodema.dev/idempotency-key"
	// LabelPreparation carries the deterministic preparation declaration digest.
	LabelPreparation = "kodema.dev/preparation"
	// LabelRetained marks a retained workspace resource.
	LabelRetained = "kodema.dev/retained"
)
