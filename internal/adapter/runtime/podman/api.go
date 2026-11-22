// Pinned Podman (libpod) runtime adapter API.
//
// This file declares the minimal libpod seam the adapter consumes and the
// adapter configuration surface. It is deliberately much smaller than the
// containers/podman Go client: the seam is an interface implemented by the
// production client and by the deterministic in-memory fake in tests. Nothing
// here opens a socket, starts a container or contacts a daemon.
package podman

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

const (
	// MaxLogBytes bounds one log read per stream. A request above this bound is
	// refused before any libpod call.
	MaxLogBytes = container.MaxLogBytes
	// Redacted replaces configured credential and secret values in log output.
	Redacted = container.Redacted
	// WorkloadCAKey is the filename carrying the installation CA bundle.
	WorkloadCAKey = container.WorkloadCAKey
	// WorkloadCAMountPath is the fixed read-only mount root of the workload CA.
	WorkloadCAMountPath = container.WorkloadCAMountPath
	// WorkloadCAPath is the read-only mounted CA bundle path inside the
	// container when Config.WorkloadCAFile is configured.
	WorkloadCAPath = container.WorkloadCAPath
	// EnvWorkloadCAFile exposes WorkloadCAPath in the container environment
	// when Config.WorkloadCAFile is configured.
	EnvWorkloadCAFile = workspace.EnvCAFile
	// DefaultMemoryBytes is the default container memory limit for local Podman
	// workloads (2 GiB), sufficient for standard Node/Next.js/Go builds.
	DefaultMemoryBytes = container.DefaultMemoryBytes
	// DefaultMaxMemoryBytes is the default installation ceiling for per-action
	// memory requests (4 GiB) when no explicit maximum is configured.
	DefaultMaxMemoryBytes = container.DefaultMaxMemoryBytes
	// DefaultCPUMillicores is the default container CPU limit (2 CPUs).
	DefaultCPUMillicores = container.DefaultCPUMillicores
	// DefaultMaxCPUMillicores is the default installation ceiling for
	// per-action CPU requests (4 CPUs) when no explicit maximum is configured.
	DefaultMaxCPUMillicores = container.DefaultMaxCPUMillicores
	// DefaultCPUPeriod is the CFS cgroup period in microseconds (100ms).
	DefaultCPUPeriod int64 = 100000
)

// Container statuses the adapter interprets. Any other status stays unknown.
const (
	// ContainerStatusCreated is a container that exists in created state.
	ContainerStatusCreated = "created"
	// ContainerStatusRunning is a container whose process may still be alive.
	ContainerStatusRunning = "running"
	// ContainerStatusExited is a container whose main process exited.
	ContainerStatusExited = "exited"
)

// Pinned identity labels shared by the generated container and the stable
// names the adapter derives, so a retry finds exactly the same object.
const (
	// LabelExecution names the execution.
	LabelExecution = container.LabelExecution
	// LabelAction names the action.
	LabelAction = container.LabelAction
	// LabelWorkload names the stable runtime workload identity.
	LabelWorkload = container.LabelWorkload
	// LabelWorkspace names the retained execution workspace.
	LabelWorkspace = container.LabelWorkspace
	// LabelProject names the owning project when supplied by the Shell.
	LabelProject = container.LabelProject
	// LabelRole names the optional workload role (for example RoleSandbox).
	LabelRole = container.LabelRole
	// RoleSandbox is the pinned LabelRole value for ephemeral sandbox workloads.
	RoleSandbox = container.RoleSandbox
	// LabelIdempotencyKey carries the persisted idempotency key so a retry can
	// be attributed to the original effect.
	LabelIdempotencyKey = container.LabelIdempotencyKey
	// LabelPreparation carries the deterministic preparation declaration digest.
	LabelPreparation = container.LabelPreparation
)

// Sentinel errors the Libpod seam wraps. The adapter interprets them; an absent
// object is never converted into proof that a command never ran or that a
// process stopped.
var (
	// ErrNotFound reports that an object does not exist.
	ErrNotFound = errors.New("podman: object not found")
	// ErrAlreadyExists reports that an object with the same name exists.
	ErrAlreadyExists = errors.New("podman: object already exists")
	// ErrRedactionSourceUnavailable reports that the configured credential or
	// secret values needed to redact log output could not be loaded.
	ErrRedactionSourceUnavailable = container.ErrRedactionSourceUnavailable
)

// Clock is the injected time source. Production uses the real clock; tests use
// a deterministic clock that never sleeps in wall-clock time.
type Clock = container.Clock

// CleanupPolicy bounds one Remove call.
type CleanupPolicy = container.CleanupPolicy

// ResourcePolicy bounds container memory and CPU limits for one Podman
// installation. Zero fields select the adapter defaults; installation maximums
// are always enforced so a pipeline can never request unbounded host memory or
// CPU.
type ResourcePolicy = container.ResourcePolicy

// Config binds one adapter instance to one adapter root.
type Config struct {
	// Libpod is the only container/volume effect surface.
	Libpod Libpod
	// Files is the host file/directory boundary.
	Files Files
	// Root is the daemon-visible adapter root; it must be absolute.
	Root string
	// KodemaURL is the reserved KODEMA_URL value injected into workloads.
	KodemaURL string
	// WorkloadCAFile optionally names an absolute host PEM CA bundle mounted
	// read-only at /etc/kodema/ca/ca.crt with
	// KODEMA_CA_FILE=/etc/kodema/ca/ca.crt. Empty mounts no CA file and sets no
	// KODEMA_CA_FILE variable.
	WorkloadCAFile string
	// ToolboxFile optionally names the absolute host path of the kodema
	// binary, built for the containers' platform, that every workload mounts
	// read-only at /kodema/bin. Empty mounts no toolbox: the entrypoint is the
	// plain script and an action gets no output directory.
	ToolboxFile string
	// GitURL optionally names the base URL of the installation's Git service
	// as workloads reach it. It is exposed as KODEMA_GIT_URL and, with a
	// toolbox, configures Git for that origin alone.
	GitURL string
	// Resources bounds default and maximum workload memory/CPU limits. Zero
	// values select the practical defaults (2 GiB / 2 CPUs, max 4 GiB / 4 CPUs).
	Resources ResourcePolicy
	// Cleanup bounds cleanup attempts per Remove call.
	Cleanup CleanupPolicy
	// Clock is optional; nil means a real clock.
	Clock Clock
}

// Mount is one container mount.
type Mount struct {
	// Type is "bind", "volume" or "tmpfs".
	Type string
	// Source is a host path or a named volume. A tmpfs has none.
	Source string
	// Destination is the in-container mount path.
	Destination string
	// ReadOnly reports whether the mount is read-only.
	ReadOnly bool
	// SizeBytes bounds a tmpfs. Other mount types ignore it.
	SizeBytes int64
}

// ContainerResources are the resolved cgroup memory and CPU bounds for one
// container. Zero fields fall back to the libpod driver's defaults.
type ContainerResources struct {
	MemoryBytes int64
	CPUQuota    int64
	CPUPeriod   int64
}

// ContainerSpec is the complete requested container.
type ContainerSpec struct {
	Name  string
	Image string
	// Entrypoint replaces the image entrypoint with the fail-fast script.
	Entrypoint []string
	// Env is "NAME=value" in declaration order, reserved context last.
	Env []string
	// Labels is the stable identity label set.
	Labels map[string]string
	// WorkingDir is the directory the commands start in.
	WorkingDir string
	// Mounts is the activation bind, isolated secrets bind, retained workspace
	// volume, and any optional CA, toolbox, output tmpfs, or cache mounts.
	Mounts []Mount
	// RestartPolicy is "no": the command is never replayed automatically.
	RestartPolicy string
	// Resources carries the bounded memory and CPU limits for the container.
	Resources ContainerResources
}

// ContainerState is the observed container. It separates "a start was accepted"
// from "a delayed start may still run" from affirmative whole-container stop
// evidence so uncertainty never collapses into success.
type ContainerState struct {
	ID               string
	Name             string
	Image            string
	Status           string
	Started          bool
	ExitCode         int
	OOMKilled        bool
	MemoryLimitBytes int64
	StartPending     bool
	WholeStopped     bool
	Labels           map[string]string
}

// Volume is the retained named workspace volume.
type Volume struct {
	Name   string
	Labels map[string]string
}

// LogRequest selects one bounded stream read.
type LogRequest struct {
	Name   string
	Stream reconcile.LogStream
	Offset int64
	Limit  int
}

// LogPage is one bounded stream read result.
type LogPage struct {
	Data       []byte
	NextOffset int64
	Truncated  bool
}

// Files is the host file/directory boundary. Lstat never follows a symlink, so
// a symlink escape is observable instead of silently accepted. ReadDir reports
// entry type bits without following a symlinked entry.
type Files interface {
	MkdirAll(context.Context, string, os.FileMode) error
	WriteFile(context.Context, string, []byte, os.FileMode) error
	ReadFile(context.Context, string) ([]byte, error)
	Lstat(context.Context, string) (os.FileInfo, error)
	ReadDir(context.Context, string) ([]os.DirEntry, error)
	Rename(context.Context, string, string) error
	RemoveAll(context.Context, string) error
}

// Libpod is the only external effect surface of the adapter.
type Libpod interface {
	CreateContainer(context.Context, ContainerSpec) (ContainerState, error)
	// InspectImage checks that a digest-pinned image reference is present in
	// the local container store. An absent image is ErrNotFound.
	InspectImage(context.Context, string) error
	InspectContainer(context.Context, string) (ContainerState, error)
	// ListContainers returns every container matching all supplied labels. It
	// is how workspace ownership is checked before a volume is released.
	ListContainers(context.Context, map[string]string) ([]ContainerState, error)
	StartContainer(context.Context, string) error
	StopContainer(context.Context, string) error
	RemoveContainer(context.Context, string) error
	ReadLogs(context.Context, LogRequest) (LogPage, error)
	InspectVolume(context.Context, string) (Volume, error)
	// RemoveVolume removes one named volume. An absent volume is ErrNotFound,
	// never a fabricated success.
	RemoveVolume(context.Context, string) error
}

// Adapter implements runtime.Client for the Podman container family.
type Adapter struct {
	libpod        Libpod
	files         Files
	root          string
	kodemaURL     string
	workloadCAPEM []byte
	caDir         string
	caMu          sync.Mutex
	gitURL        string
	resources     ResourcePolicy
	cleanup       CleanupPolicy
	clock         Clock

	// toolboxFile is the configured toolbox source and toolboxDir the
	// installation copy every workload binds; both are empty without a
	// toolbox. toolboxMu serializes the one installation of that copy.
	toolboxFile string
	toolboxDir  string
	toolboxMu   sync.Mutex

	mu       sync.Mutex
	prepared map[id.WorkloadID]preparedWorkload

	locks          container.KeyedLock[id.WorkloadID]
	workspaceLocks container.KeyedLock[id.WorkspaceID]
}

// New validates one adapter configuration. An incomplete configuration is
// refused: the adapter never guesses a root, URL or cleanup bound.
func New(config Config) (*Adapter, error) {
	if config.Libpod == nil {
		return nil, fault.New(fault.InvalidInput, "podman adapter requires a libpod client")
	}
	if config.Files == nil {
		return nil, fault.New(fault.InvalidInput, "podman adapter requires a host file boundary")
	}
	if strings.TrimSpace(config.Root) == "" || !filepath.IsAbs(config.Root) {
		return nil, fault.New(fault.InvalidInput, "podman adapter requires an absolute adapter root")
	}
	if strings.TrimSpace(config.KodemaURL) == "" {
		return nil, fault.New(fault.InvalidInput, "podman adapter requires the Kodema URL")
	}
	if err := config.Cleanup.Validate(); err != nil {
		return nil, err
	}
	root := filepath.Clean(config.Root)
	var workloadCAPEM []byte
	caDir := ""
	if caFile := strings.TrimSpace(config.WorkloadCAFile); caFile != "" {
		loaded, err := loadWorkloadCAPEM(caFile)
		if err != nil {
			return nil, err
		}
		workloadCAPEM = loaded
		caDir = filepath.Join(root, caDirectory, container.StableDigest(string(loaded)))
	}
	toolboxFile, toolboxDir := "", ""
	if file := strings.TrimSpace(config.ToolboxFile); file != "" {
		digest, err := toolboxDigest(file)
		if err != nil {
			return nil, err
		}
		toolboxFile, toolboxDir = file, filepath.Join(root, toolboxDirectory, digest)
	}
	gitURL := strings.TrimRight(strings.TrimSpace(config.GitURL), "/")
	if gitURL != "" {
		if _, err := workspace.GitOrigin(gitURL); err != nil {
			return nil, err
		}
	}
	resources, err := container.NormalizeResourcePolicy(config.Resources)
	if err != nil {
		return nil, err
	}
	clock := config.Clock
	if clock == nil {
		clock = container.RealClock{}
	}
	return &Adapter{
		libpod:        config.Libpod,
		files:         config.Files,
		root:          root,
		kodemaURL:     config.KodemaURL,
		workloadCAPEM: workloadCAPEM,
		caDir:         caDir,
		gitURL:        gitURL,
		resources:     resources,
		toolboxFile:   toolboxFile,
		toolboxDir:    toolboxDir,
		cleanup:       config.Cleanup,
		clock:         clock,
		prepared:      map[id.WorkloadID]preparedWorkload{},
	}, nil
}

func loadWorkloadCAPEM(path string) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, fault.New(fault.InvalidInput, "podman adapter workload CA file must be an absolute path")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, fault.New(fault.InvalidInput, "podman adapter workload CA file is unreadable")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fault.New(fault.InvalidInput, "podman adapter workload CA file is unreadable")
	}
	// Workloads receive only the certificates: any other block in the file,
	// such as a private key, is dropped.
	var certs []byte
	for block, rest := pem.Decode(data); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			continue
		}
		certs = append(certs, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes})...)
	}
	if len(certs) == 0 {
		return nil, fault.New(fault.InvalidInput, "podman adapter workload CA file is not a usable PEM certificate")
	}
	return certs, nil
}

// toolboxDigest names one toolbox binary by its content, so two versions of
// the control plane never share an installation copy and a workload that is
// still running keeps the binary it started with.
func toolboxDigest(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", fault.New(fault.InvalidInput, "podman adapter toolbox file must be an absolute path")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", fault.New(fault.InvalidInput, "podman adapter toolbox file is unreadable")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fault.New(fault.InvalidInput, "podman adapter toolbox file is unreadable")
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fault.New(fault.InvalidInput, "podman adapter toolbox file is unreadable")
	}
	return toolboxDigestName(digest.Sum(nil)), nil
}

func toolboxDigestName(sum []byte) string { return hex.EncodeToString(sum)[:24] }

// lockFor acquires the per-workload mutex that serializes lifecycle effects on
// one stable identity inside this process and returns its unlock function.
func (a *Adapter) lockFor(workload id.WorkloadID) func() {
	return a.locks.Lock(workload)
}

// lockForWorkspace acquires the per-workspace mutex shared by every operation
// that attaches, detaches or observes one retained workspace volume, returning
// its unlock function. Lifecycle operations acquire the workload lock first and
// this lock second, so the ordering is consistent and a release can never
// interleave with a start.
func (a *Adapter) lockForWorkspace(workspace id.WorkspaceID) func() {
	return a.workspaceLocks.Lock(workspace)
}

// Version reports the pinned libpod API surface without contacting the daemon.
func (a *Adapter) Version(context.Context) (runtime.APIVersion, error) {
	return runtime.APIVersion{Adapter: runtime.AdapterPodman, Version: "libpod/v4"}, nil
}

// Capabilities reports the explicit Podman capability set. It is
// declaration-only and makes no libpod call.
func (a *Adapter) Capabilities(context.Context) (reconcile.Capabilities, error) {
	family := runtime.CapabilitiesFor(runtime.AdapterPodman)
	names := make([]string, len(family))
	for index, capability := range family {
		names[index] = string(capability)
	}
	return reconcile.Capabilities{Names: names}, nil
}

// CheckRequirements refuses admitted work the Podman family cannot run.
func CheckRequirements(required []string) error {
	capabilities := make([]runtime.Capability, len(required))
	for index, name := range required {
		capabilities[index] = runtime.Capability(name)
	}
	return container.CheckRequirements(runtime.AdapterPodman, capabilities)
}

// ContainerName returns the deterministic container name for one stable
// workload. A retry must find exactly this container, never a generated one.
func ContainerName(workload id.WorkloadID) (string, error) {
	if _, err := id.ParseWorkloadID(string(workload)); err != nil {
		return "", fault.Wrap(fault.InvalidInput, "workload id is invalid", err)
	}
	return "kodema-workload-" + container.StableDigest(string(workload)), nil
}

// WorkspaceVolumeName returns the deterministic name of one retained workspace
// volume. The Shell provisions the volume before Prepare; the adapter attaches
// exactly this volume and never creates or removes it.
func WorkspaceVolumeName(workspace id.WorkspaceID) (string, error) {
	return container.WorkspaceResourceName(workspace)
}

// CacheVolumeName returns the deterministic Podman named volume for one
// tenant/project-scoped non-authoritative cache key. Podman creates it empty on
// first mount if missing or evicted, and Remove/Release never delete it.
func CacheVolumeName(key string) (string, error) {
	if len(key) < 16 || len(key) > 64 {
		return "", fault.New(fault.InvalidInput, "cache key is invalid")
	}
	for index := 0; index < len(key); index++ {
		ch := key[index]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return "", fault.New(fault.InvalidInput, "cache key is invalid")
		}
	}
	return "kodema-cache-" + key, nil
}
