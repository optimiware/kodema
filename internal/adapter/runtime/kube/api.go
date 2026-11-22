// Pinned Kubernetes runtime adapter API.
//
// This file declares the minimal Kubernetes API seam the adapter consumes and
// the adapter configuration surface. It is intentionally much smaller than
// client-go: the seam is an interface implemented by the production client and
// by the deterministic in-memory fake in tests. Nothing here opens a network
// connection or starts a process.
package kube

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// Pinned manifest vocabulary shared by the generated manifests and the pod
// selectors, so a tenant network policy and the adapter agree.
const (
	// LabelTenant names the owning tenant.
	LabelTenant = container.LabelTenant
	// LabelProject names the owning project when supplied by the Shell.
	LabelProject = container.LabelProject
	// LabelRole names the optional workload role (for example "sandbox").
	LabelRole = container.LabelRole
	// RoleSandbox is the pinned LabelRole value for ephemeral OpenCode sandbox
	// workloads (`kodema sandbox serve`).
	RoleSandbox = container.RoleSandbox
	// LabelExecution names the execution.
	LabelExecution = container.LabelExecution
	// LabelAction names the action.
	LabelAction = container.LabelAction
	// LabelWorkload names the stable runtime workload identity.
	LabelWorkload = container.LabelWorkload
	// LabelWorkspace names the retained execution workspace.
	LabelWorkspace = container.LabelWorkspace
	// LabelRetained marks a workspace claim whose retention must survive a
	// stale release.
	LabelRetained = container.LabelRetained
	// LabelCacheKey carries the scoped cache key on a persistent named cache
	// PersistentVolumeClaim.
	LabelCacheKey = container.LabelCacheKey
	// LabelCache carries the informational cache slug on a persistent named
	// cache PersistentVolumeClaim when the slug is a valid Kubernetes label.
	LabelCache = container.LabelCache
	// LabelPreparation carries the deterministic preparation declaration
	// digest on the immutable activation ConfigMap.
	LabelPreparation = container.LabelPreparation
	// AnnotationIdempotencyKey carries the persisted idempotency key so a
	// retry can be attributed to the original effect.
	AnnotationIdempotencyKey = container.LabelIdempotencyKey
	// AnnotationRetained carries the durable workspace-retention marker.
	AnnotationRetained = container.LabelRetained
)

const (
	// MaxLogBytes bounds one log read. A request above this bound is refused
	// before any external call.
	MaxLogBytes = container.MaxLogBytes
	// Redacted replaces configured credential and secret values in log output.
	Redacted = container.Redacted
)

const (
	// WorkloadCAKey is the literal Secret key carrying the installation CA
	// bundle. Secret keys are not path-encoded.
	WorkloadCAKey = container.WorkloadCAKey
	// WorkloadCAMountPath is the fixed read-only mount root of the workload CA.
	WorkloadCAMountPath = container.WorkloadCAMountPath
	// WorkloadCAPath is the fixed absolute path of the mounted CA bundle.
	WorkloadCAPath = container.WorkloadCAPath
)

// WorkloadCAConfig configures the installation-provided CA bundle projected
// read-only into every workload. An empty SecretName disables the mount, and the
// adapter never reads or re-materializes the Secret. KODEMA_CA_FILE names
// the mount without changing the image's system trust.
type WorkloadCAConfig struct {
	// SecretName is the tenant-local, CA-only Secret in the adapter namespace.
	SecretName string
}

// Sentinel errors the Cluster seam wraps. The adapter interprets them; an
// absent object is never converted into proof that a command never ran or that
// a process stopped.
var (
	// ErrNotFound reports that an object does not exist.
	ErrNotFound = errors.New("kubernetes: object not found")
	// ErrAlreadyExists reports that an object with the same name exists.
	ErrAlreadyExists = errors.New("kubernetes: object already exists")
	// ErrConflict reports a stale resource version or an immutable change.
	ErrConflict = errors.New("kubernetes: object conflict")
)

// Clock is the injected time source. Production uses the real clock; tests use
// a deterministic clock that never sleeps in wall-clock time.
type Clock = container.Clock

// Config binds one adapter instance to one tenant namespace.
type Config struct {
	// Cluster is the only external effect surface.
	Cluster Cluster
	// Namespace is the tenant namespace this adapter is bound to.
	Namespace string
	// TenantID is the tenant ownership label expected on that namespace.
	TenantID id.TenantID
	// KodemaURL is the reserved KODEMA_URL value injected into workloads.
	KodemaURL string
	// Policy carries the tenant resource limits and finished-Job TTL.
	Policy Policy
	// Cleanup bounds cleanup attempts per Remove call.
	Cleanup CleanupPolicy
	// WorkloadCA configures the installation CA bundle mounted into workloads.
	WorkloadCA WorkloadCAConfig
	// ToolboxImage is the installation's digest-pinned Kodema image. Empty
	// retains the plain shell launch for adapters that do not supply tools.
	ToolboxImage string
	// AllowUnpinnedToolbox permits a repository:tag toolbox image reference
	// (without a sha256 digest) in non-production profiles such as the durable
	// test profile. Production leaves this false and strictly enforces a
	// digest-pinned reference.
	AllowUnpinnedToolbox bool
	// GitURL is the optional HTTPS Git endpoint for this adapter's tenant.
	GitURL string
	// ImagePullSecrets names the tenant-local pull Secrets the Job references.
	// An empty list renders no imagePullSecrets field.
	ImagePullSecrets []string
	// CacheClaimName optionally names a tenant-local PersistentVolumeClaim used
	// to back declared non-authoritative container caches via subPath isolation
	// keyed by (tenant_id, project_id, cache_name). When empty or absent at
	// Prepare time, caches cleanly degrade to emptyDir volumes.
	CacheClaimName string
	// Clock is optional; nil means a real clock.
	Clock Clock
}

// Policy is the admitted workload resource policy. MaxCPULimit and
// MaxMemoryLimit optionally raise the per-action ceiling above CPULimit and
// MemoryLimit; when empty, the default limits are also the installation
// maximums so workloads cannot exceed tenant policy.
type Policy struct {
	CPURequest     string
	CPULimit       string
	MaxCPULimit    string
	MemoryRequest  string
	MemoryLimit    string
	MaxMemoryLimit string
	JobTTLSeconds  int
}

// CleanupPolicy bounds one Remove call.
type CleanupPolicy = container.CleanupPolicy

// Namespace is the tenant namespace and its ownership labels.
type Namespace struct {
	Name   string
	Labels map[string]string
}

// Resources are the container resource requests and limits.
type Resources struct {
	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string
}

// EnvVar is one literal environment variable.
type EnvVar struct {
	Name  string
	Value string
}

// VolumeItem is one projected file in a volume.
type VolumeItem struct {
	Key  string
	Path string
	Mode os.FileMode
}

// Volume is one pod volume and its mount.
type Volume struct {
	Name          string
	MountPath     string
	ClaimName     string
	ConfigMapName string
	SecretName    string
	ReadOnly      bool
	Items         []VolumeItem
	// EmptyDir requests pod-local storage. MemoryBytes > 0 selects a bounded tmpfs.
	EmptyDir    bool
	MemoryBytes int64
	// SubPath optionally selects a scoped subdirectory within ClaimName (used
	// for tenant/project-isolated non-authoritative dependency caches).
	SubPath string
}

// JobSpec is the generated Job pod template.
type JobSpec struct {
	ToolboxImage  string
	WorkingDir    string
	Image         string
	Command       []string
	Env           []EnvVar
	Resources     Resources
	Volumes       []Volume
	RestartPolicy string
	BackoffLimit  int
	// PodReplacementPolicy maps to the batch/v1 Job spec.podReplacementPolicy.
	// Pinning Failed means a running pod is never replaced while it may still
	// write to the retained workspace.
	PodReplacementPolicy string
	// TTLSecondsAfterFinished is the finished-Job TTL sent on create. A
	// successful suspension clears it, so nil means no TTL remains.
	TTLSecondsAfterFinished      *int
	AutomountServiceAccountToken bool
	HostNetwork                  bool
	PodLabels                    map[string]string
	// ImagePullSecrets names the configured private-registry pull Secrets. An
	// empty list renders no imagePullSecrets field.
	ImagePullSecrets []string
	// Suspend maps to the batch/v1 Job spec.suspend intent. Nil means the
	// server did not report suspension state. The manifest builder never sends
	// it on create; only SuspendJob changes it.
	Suspend *bool
}

// JobCondition is one observed Job condition. Status keeps the exact server
// value ("True", "False" or "Unknown") instead of collapsing it to a bool, so
// an unacknowledged or unknown condition can never look affirmative.
type JobCondition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// JobStatus is the observed Job status. It is informational only: it never
// proves a command stopped.
type JobStatus struct {
	Active         int
	Succeeded      int
	Failed         int
	StartTime      *time.Time
	CompletionTime *time.Time
	// Terminating is the number of Pending or Running pods that carry a
	// deletion timestamp. It is presence-preserving: nil means the field was
	// absent or null (unknown), never an affirmative zero.
	Terminating *int
	// Conditions are the Job conditions exactly as the server reported them.
	Conditions []JobCondition
}

// Job is one generated Kubernetes Job.
type Job struct {
	Name            string
	Namespace       string
	UID             string
	Labels          map[string]string
	Annotations     map[string]string
	Spec            JobSpec
	Status          JobStatus
	ResourceVersion string
	// DeletionTimestamp is the observed metadata.deletionTimestamp. A Job with
	// a deletion timestamp is being removed and can never confirm a suspension.
	DeletionTimestamp *time.Time
}

// ContainerStatus is one pod container's observed status.
type ContainerStatus struct {
	Name        string
	Terminated  bool
	ExitCode    int
	Reason      string
	MemoryLimit string
}

// Pod is one observed workload pod.
type Pod struct {
	Name              string
	Namespace         string
	Labels            map[string]string
	Phase             string
	NodeName          string
	Containers        []ContainerStatus
	DeletionTimestamp *time.Time
}

// ConfigMap is an immutable activation snapshot.
type ConfigMap struct {
	Name            string
	Namespace       string
	Labels          map[string]string
	Data            map[string][]byte
	Immutable       bool
	ResourceVersion string
}

// Secret is the mutable credential and declared-secret mount.
type Secret struct {
	Name            string
	Namespace       string
	Labels          map[string]string
	Data            map[string][]byte
	Immutable       bool
	ResourceVersion string
}

// Claim is the pre-existing retained workspace volume claim.
type Claim struct {
	Name              string
	Namespace         string
	Labels            map[string]string
	Annotations       map[string]string
	Finalizers        []string
	DeletionTimestamp *time.Time
	ResourceVersion   string
}

const (
	// LogStreamCombined names the one raw interleaved pod log (stdout and
	// stderr together).
	LogStreamCombined = "combined"
	// LogStreamSplit reads the same portable pod log endpoint and
	// demultiplexes framed stdout and stderr records in a single pass, falling
	// back to stdout for unframed lines.
	LogStreamSplit = "split"
)

// PodLogRequest selects one bounded read of the pod log.
type PodLogRequest struct {
	Stream       string
	Offset       int64
	StderrOffset int64
	Limit        int
}

// PodLogPage is one bounded pod-log read result.
type PodLogPage struct {
	Data             []byte
	NextOffset       int64
	Truncated        bool
	StderrData       []byte
	StderrNextOffset int64
	StderrTruncated  bool
}

// Cluster is the only external effect surface of the adapter.
type Cluster interface {
	GetNamespace(context.Context, string) (Namespace, error)
	CreateJob(context.Context, Job) (Job, error)
	GetJob(context.Context, string, string) (Job, error)
	DeleteJob(context.Context, string, string) error
	// SuspendJob asks the controller to suspend exactly the Job with the
	// expected UID and clears its finished-Job TTL in the same request, so a Job
	// that finishes while suspended survives until Remove deletes it instead of
	// being removed (with foreground propagation, taking its pods) by the TTL
	// controller. The UID guard makes a reused Job name a conflict instead of
	// suspending an unrelated Job. The answer carries the observed state; a
	// successful request is intent, not a confirmation.
	SuspendJob(context.Context, string, string, string) (Job, error)
	ListPods(context.Context, string, map[string]string) ([]Pod, error)
	ReadPodLogs(context.Context, string, string, PodLogRequest) (PodLogPage, error)
	PutConfigMap(context.Context, ConfigMap) (ConfigMap, error)
	GetConfigMap(context.Context, string, string) (ConfigMap, error)
	DeleteConfigMap(context.Context, string, string) error
	PutSecret(context.Context, Secret) (Secret, error)
	GetSecret(context.Context, string, string) (Secret, error)
	DeleteSecret(context.Context, string, string) error
	GetClaim(context.Context, string, string) (Claim, error)
	UpdateClaim(context.Context, Claim) (Claim, error)
	DeleteClaim(context.Context, string, string) error
	// DeleteClaimVersioned deletes only when the claim still carries the
	// expected resource version, so a stale Release never drops a retained
	// workspace.
	DeleteClaimVersioned(context.Context, string, string, string) error
	ListJobs(context.Context, string, map[string]string) ([]Job, error)
}

// preparedWorkload is the in-process binding between one persisted workload
// identity and the activation declaration. It is keyed by WorkloadID so a retry
// inside the same process cannot rebind the workload to a changed identity or
// declaration.
type preparedCache struct {
	name      string
	key       string
	claimName string
	subPath   string
}

type preparedWorkload struct {
	identity        reconcile.EffectIdentity
	digest          string
	workspace       id.WorkspaceID
	project         id.ProjectID
	role            string
	actionName      id.ActionName
	taskID          id.TaskID
	container       containerDeclaration
	claimName       string
	configMap       string
	secret          string
	job             string
	activationPaths []string
	secretNames     []id.SecretName
	toolboxImage    string
	caSecret        string
	gitURL          string
	outputDir       string
	caches          []preparedCache
	resources       Resources
}

// containerDeclaration is the subset of the admitted container declaration the
// adapter needs after Prepare. Values are copied so later mutation by a caller
// cannot change an already-prepared workload.
type containerDeclaration struct {
	image string
	env   []definition.EnvVar
}

// Adapter implements runtime.Client and engine.Workspace for the Kubernetes Job
// family.
type Adapter struct {
	cluster              Cluster
	namespace            string
	tenant               id.TenantID
	kodemaURL            string
	toolboxImage         string
	allowUnpinnedToolbox bool
	gitURL               string
	policy               Policy
	resourcePolicy       container.ResourcePolicy
	cleanup              CleanupPolicy
	workloadCA           WorkloadCAConfig
	imagePullSecrets     []string
	cacheClaimName       string
	clock                Clock

	mu       sync.Mutex
	prepared map[id.WorkloadID]preparedWorkload

	locks container.KeyedLock[id.WorkspaceID]
}

// New validates one adapter configuration. An incomplete configuration is
// refused: the adapter never guesses a namespace, tenant, limit or cleanup
// bound.
func New(config Config) (*Adapter, error) {
	if config.Cluster == nil {
		return nil, fault.New(fault.InvalidInput, "kubernetes adapter requires a cluster client")
	}
	if strings.TrimSpace(config.Namespace) == "" {
		return nil, fault.New(fault.InvalidInput, "kubernetes adapter requires a tenant namespace")
	}
	if _, err := id.ParseTenantID(string(config.TenantID)); err != nil {
		return nil, fault.Wrap(fault.InvalidInput, "kubernetes adapter tenant id is invalid", err)
	}
	if strings.TrimSpace(config.KodemaURL) == "" {
		return nil, fault.New(fault.InvalidInput, "kubernetes adapter requires the Kodema URL")
	}
	for name, value := range map[string]string{
		"cpu request":    config.Policy.CPURequest,
		"cpu limit":      config.Policy.CPULimit,
		"memory request": config.Policy.MemoryRequest,
		"memory limit":   config.Policy.MemoryLimit,
	} {
		if strings.TrimSpace(value) == "" {
			return nil, fault.New(fault.InvalidInput, "kubernetes adapter requires a "+name)
		}
	}
	if config.Policy.JobTTLSeconds <= 0 {
		return nil, fault.New(fault.InvalidInput, "kubernetes adapter requires a positive Job TTL")
	}
	rawPolicy, err := container.ParseRawResourcePolicy(
		config.Policy.MemoryLimit,
		config.Policy.MaxMemoryLimit,
		config.Policy.CPULimit,
		config.Policy.MaxCPULimit,
	)
	if err != nil {
		return nil, err
	}
	resourcePolicy, err := container.NormalizeCeilingPolicy(rawPolicy)
	if err != nil {
		return nil, err
	}
	if err := config.Cleanup.Validate(); err != nil {
		return nil, err
	}
	if config.WorkloadCA.SecretName != "" {
		if err := validateWorkloadCASecretName(config.WorkloadCA.SecretName); err != nil {
			return nil, err
		}
	}
	if err := validateImagePullSecretNames(config.ImagePullSecrets); err != nil {
		return nil, err
	}
	if config.CacheClaimName != "" && !validDNS1123Subdomain(config.CacheClaimName) {
		return nil, fault.New(fault.InvalidInput, "kubernetes adapter cache claim name is not a DNS-1123 subdomain")
	}
	if err := validateToolbox(config.ToolboxImage, config.GitURL, config.AllowUnpinnedToolbox); err != nil {
		return nil, err
	}
	clock := config.Clock
	if clock == nil {
		clock = container.RealClock{}
	}
	return &Adapter{
		cluster:              config.Cluster,
		namespace:            config.Namespace,
		tenant:               config.TenantID,
		kodemaURL:            config.KodemaURL,
		toolboxImage:         config.ToolboxImage,
		allowUnpinnedToolbox: config.AllowUnpinnedToolbox,
		gitURL:               config.GitURL,
		policy:               config.Policy,
		resourcePolicy:       resourcePolicy,
		cleanup:              config.Cleanup,
		workloadCA:           config.WorkloadCA,
		imagePullSecrets:     append([]string(nil), config.ImagePullSecrets...),
		cacheClaimName:       config.CacheClaimName,
		clock:                clock,
		prepared:             map[id.WorkloadID]preparedWorkload{},
	}, nil
}

// validateWorkloadCASecretName refuses a reference that is not a DNS-1123
// subdomain before any cluster call, so an unusable CA Secret name never reaches
// the API server. The name is a configuration reference, not secret material.
func validateWorkloadCASecretName(name string) error {
	if !validDNS1123Subdomain(name) {
		return fault.New(fault.InvalidInput, "kubernetes adapter workload CA Secret name is not a DNS-1123 subdomain")
	}
	return nil
}

// validateImagePullSecretNames refuses an unusable or duplicated pull Secret
// reference before any cluster call. The names are configuration references,
// never credential material.
func validateImagePullSecretNames(names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if !validDNS1123Subdomain(name) {
			return fault.New(fault.InvalidInput, "kubernetes adapter image pull secret name is not a DNS-1123 subdomain")
		}
		if _, duplicate := seen[name]; duplicate {
			return fault.New(fault.InvalidInput, "kubernetes adapter image pull secret name is duplicated")
		}
		seen[name] = struct{}{}
	}
	return nil
}

// validDNS1123Subdomain reports whether name is a Kubernetes DNS-1123
// subdomain (lowercase alphanumerics, '-' and '.', at most 253 characters).
func validDNS1123Subdomain(name string) bool {
	const maxDNSSubdomainLength = 253
	const maxDNSLabelLength = 63
	if len(name) == 0 || len(name) > maxDNSSubdomainLength {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if len(label) == 0 || len(label) > maxDNSLabelLength {
			return false
		}
		for index := 0; index < len(label); index++ {
			character := label[index]
			alphanumeric := character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
			if !alphanumeric && character != '-' {
				return false
			}
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

// Version reports the pinned Kubernetes API surface without contacting the
// cluster.
func (a *Adapter) Version(context.Context) (runtime.APIVersion, error) {
	return runtime.APIVersion{Adapter: runtime.AdapterKubernetes, Version: "batch/v1"}, nil
}

// Capabilities reports the explicit Kubernetes capability set. It is
// declaration-only and makes no cluster call.
func (a *Adapter) Capabilities(context.Context) (reconcile.Capabilities, error) {
	family := runtime.CapabilitiesFor(runtime.AdapterKubernetes)
	names := make([]string, len(family))
	for index, capability := range family {
		names[index] = string(capability)
	}
	return reconcile.Capabilities{Names: names}, nil
}

// CheckRequirements refuses admitted work the Kubernetes family cannot run.
func CheckRequirements(required []string) error {
	caps := make([]runtime.Capability, len(required))
	for index, name := range required {
		caps[index] = runtime.Capability(name)
	}
	return container.CheckRequirements(runtime.AdapterKubernetes, caps)
}

// WorkspaceClaimName returns the deterministic claim name for one retained
// workspace. The Shell provisions this claim before Prepare and the adapter
// attaches exactly this claim after a restart.
func WorkspaceClaimName(workspace id.WorkspaceID) (string, error) {
	return container.WorkspaceResourceName(workspace)
}

// CacheClaimName returns the deterministic PersistentVolumeClaim name for one
// scoped named cache key.
func CacheClaimName(key string) (string, error) {
	return container.CacheResourceName(key)
}
