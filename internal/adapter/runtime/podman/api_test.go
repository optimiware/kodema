// Podman runtime adapter API pinned by these tests.
//
// This file is the test-side declaration of the minimal Podman (libpod) API
// seam that the production adapter under internal/shell/runtime/podman
// implements. It imports no libpod/containers module, opens no socket and
// starts no container: every test drives the deterministic in-memory libpod,
// file and clock fakes in fake_test.go.
//
// The adapter implements the declared runtime.Client seam exactly:
//
//	Capabilities(context.Context) (engine.Capabilities, error)
//	Prepare(context.Context, engine.PrepareRequest) (engine.Preparation, error)
//	Start(context.Context, engine.StartRequest) (engine.StartResult, error)
//	Observe(context.Context, engine.ObserveRequest) (kernel.Observation, error)
//	Logs(context.Context, engine.LogsRequest) (engine.LogBatch, error)
//	Stop(context.Context, engine.StopRequest) (kernel.StopEvidence, error)
//	Remove(context.Context, engine.RemoveRequest) (engine.Removal, error)
//	RefreshCredential(context.Context, engine.CredentialRequest) (engine.CredentialRefresh, error)
//	Version(context.Context) (runtime.APIVersion, error)
//
// It also implements the declared Kernel engine.Workspace port:
//
//	Status(context.Context, id.WorkspaceID) (engine.WorkspaceObservation, error)
//	Retain(context.Context, id.WorkspaceID) error
//	Release(context.Context, id.WorkspaceID) error
//
// Pinned package declarations (in api.go, adapter.go, evidence.go, manifest.go
// and doc.go):
//
//	func New(Config) (*Adapter, error)
//	type Adapter struct{ ... }                      // implements runtime.Client
//	func CheckRequirements([]string) error          // fault.UnsupportedCapability
//	func ContainerName(id.WorkloadID) (string, error)
//	func WorkspaceVolumeName(id.WorkspaceID) (string, error)
//
//	const MaxLogBytes = 1 << 20                     // bounded log read per stream
//	const Redacted = "[REDACTED]"
//	const (
//	    ContainerStatusCreated = "created"          // created, never started
//	    ContainerStatusRunning = "running"
//	    ContainerStatusExited  = "exited"
//	)
//	const (
//	    LabelExecution      = "kodema.dev/execution"
//	    LabelAction         = "kodema.dev/action"
//	    LabelWorkload       = "kodema.dev/workload"
//	    LabelWorkspace      = "kodema.dev/workspace"
//	    LabelIdempotencyKey = "kodema.dev/idempotency-key"
//	)
//
//	var (
//	    ErrNotFound      = errors.New("podman: object not found")
//	    ErrAlreadyExists = errors.New("podman: object already exists")
//	)
//
//	type Config struct {
//	    Libpod    Libpod       // the only container/volume effect surface
//	    Files     Files        // host file/directory boundary
//	    Root      string       // daemon-visible adapter root; absolute
//	    KodemaURL string       // reserved KODEMA_URL value
//	    Cleanup   CleanupPolicy
//	    Clock     Clock        // optional; nil means a real clock
//	}
//	type CleanupPolicy struct {
//	    Attempts int           // total attempts per resource per Remove call, >= 1
//	    Backoff  time.Duration // delay between attempts via Clock.Sleep
//	}
//	type Clock interface {
//	    Now() time.Time
//	    Sleep(context.Context, time.Duration) error
//	}
//
//	type Libpod interface {
//	    CreateContainer(context.Context, ContainerSpec) (ContainerState, error)
//	    InspectImage(context.Context, string) error
//	    InspectContainer(context.Context, string) (ContainerState, error)
//	    ListContainers(context.Context, map[string]string) ([]ContainerState, error)
//	    StartContainer(context.Context, string) error
//	    StopContainer(context.Context, string) error
//	    RemoveContainer(context.Context, string) error
//	    ReadLogs(context.Context, LogRequest) (LogPage, error)
//	    InspectVolume(context.Context, string) (Volume, error)
//	    RemoveVolume(context.Context, string) error // absent is ErrNotFound
//	}
//	type ContainerSpec struct {
//	    Name          string
//	    Image         string
//	    Entrypoint    []string          // exactly {script.ShellPath, "/kodema/script"}
//	    Env           []string          // "NAME=value" in declaration order, reserved last
//	    Labels        map[string]string // stable identity labels
//	    Mounts        []Mount
//	    RestartPolicy string            // "no": never replay automatically
//	}
//	type Mount struct {
//	    Type        string // "bind" or "volume"
//	    Source      string
//	    Destination string
//	    ReadOnly    bool
//	}
//	type ContainerState struct {
//	    ID           string
//	    Name         string
//	    Image        string
//	    Status       string // ContainerStatusCreated/Running/Exited
//	    Started      bool   // a start was accepted at least once
//	    ExitCode     int
//	    StartPending bool   // accepted start not settled: a delayed start may still run
//	    WholeStopped bool   // affirmative evidence every container process exited
//	    Labels       map[string]string
//	}
//	type Volume struct {
//	    Name   string
//	    Labels map[string]string
//	}
//	type LogRequest struct {
//	    Name   string
//	    Stream engine.LogStream
//	    Offset int64
//	    Limit  int
//	}
//	type LogPage struct {
//	    Data       []byte
//	    NextOffset int64
//	    Truncated  bool
//	}
//	type Files interface {
//	    MkdirAll(context.Context, string, os.FileMode) error
//	    WriteFile(context.Context, string, []byte, os.FileMode) error
//	    ReadFile(context.Context, string) ([]byte, error)
//	    Lstat(context.Context, string) (os.FileInfo, error) // never follows a symlink
//	    ReadDir(context.Context, string) ([]os.DirEntry, error) // never follows a symlink
//	    Rename(context.Context, string, string) error
//	    RemoveAll(context.Context, string) error
//	}
//
// Host layout for one stable identity, rooted at
// <Config.Root>/<Identity.WorkloadID>:
//
//	<root>/<workload>/kodema/inputs/<key>              exact admitted input bytes
//	<root>/<workload>/kodema/outputs/<action>/<key>    earlier succeeded outputs only
//	<root>/<workload>/kodema/task.json                 only when the request has a task
//	<root>/<workload>/kodema/script                    script.Render output
//	<root>/<workload>/kodema/secrets/                  empty mount-point directory
//	<root>/<workload>/secrets/<NAME>                   declared activation-time values (0700 dir)
//	<root>/<workload>/secrets/KODEMA_TOKEN             the sole mutable mounted file (0700 dir)
//	<root>/.ca/<digest>/ca.crt                         shared CA bundle (mounted at /etc/kodema/ca)
//
// Non-secret logical paths P under /kodema are written at
// filepath.Join(<root>/<workload>, P) and bind-mounted read-only at /kodema.
// Declared secrets and KODEMA_TOKEN live in the isolated 0700 directory
// <root>/<workload>/secrets and are bind-mounted read-only at /kodema/secrets
// (a directory bind so atomic token replacement is visible without a restart).
// The execution workspace is the pre-existing named Podman volume returned by
// WorkspaceVolumeName(WorkspaceID); Prepare and Start attach it read-write at
// /workspace and never create it. Only the separate reconcile.Workspace Release
// operation may remove it, and only when no container references it.
//
// Behavior rules these tests pin (S02 §5–6 and S07 §1–3):
//   - Version/Capabilities/CheckRequirements are declaration-only and make no
//     libpod call.
//   - Prepare reuses container.BuildMaterialization and script.Render. It
//     requires a digest-pinned image present in the local store, valid commands,
//     action slug/task id, non-empty credential, existing owned workspace volume
//     and confined, non-symlinked files. It is idempotent for unchanged content,
//     refuses changed content with changed_idempotency_content, refuses
//     workspace conflicts with forbidden, and never starts a container.
//   - Start consumes the persisted reconcile.EffectIdentity and the completed
//     in-process preparation (or reuses an existing running/exited container
//     after controller restart). A lost create or start response stays
//     StartUnknown, and an existing container in "created" status stays
//     StartUnknown conservatively.
//   - Observe reports AbsentObservation for any absent container,
//     UnknownObservation for "created" status or daemon errors, Running, and
//     Exited with confirmed stop only on whole-container termination.
//   - Remove is idempotent, identity-scoped and bounded by Cleanup.Attempts
//     with Clock.Sleep(Backoff) between attempts. It removes only that
//     identity's container and materialization tree and NEVER removes or writes
//     the retained workspace volume.
//   - Workspace is separate from Runtime. Status reports availability only for
//     an existing owned volume; Retain verifies the owned volume exists;
//     Release removes the volume only when owned and unreferenced by any
//     container.
//   - RefreshCredential atomically replaces only
//     <root>/<workload>/secrets/KODEMA_TOKEN (write-temp-then-rename) without
//     creating, starting, stopping or removing the container.
//   - Logs are ordered stdout/stderr chunks with a packed continuation token,
//     bounded by Limit, redacted from <root>/<workload>/secrets, and fail
//     closed with ErrRedactionSourceUnavailable when the redaction source is
//     unavailable.
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
