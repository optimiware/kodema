//go:build component

package runtimeconformance

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
	podmandriver "github.com/optimiware/kodema/internal/server/drivers/podman"
	runtimesupport "github.com/optimiware/kodema/test/support/infrastructure/runtime"
)

// TestPodmanKillHelperProcess is the child controller for
// TestPodmanRuntimeKilledControllerBetweenLaunchAndObservation. It prepares and
// creates the container, then blocks inside the start call after publishing a
// marker. The parent SIGKILLs it between launch intent and observation.
func TestPodmanKillHelperProcess(t *testing.T) {
	if os.Getenv("KODEMA_RUNTIME_HELPER") != "1" {
		t.Skip("helper process only")
	}
	root := os.Getenv("KODEMA_HELPER_ROOT")
	socket := os.Getenv("KODEMA_HELPER_SOCKET")
	marker := os.Getenv("KODEMA_HELPER_MARKER")
	image := os.Getenv("KODEMA_HELPER_IMAGE")
	// The parent chooses the complete run-scoped identity and workspace; the
	// helper must never mint its own, or the killed controller's durable state
	// and container would belong to a different run namespace.
	identity := reconcile.EffectIdentity{
		ExecutionID:    id.ExecutionID(os.Getenv("KODEMA_HELPER_EXECUTION")),
		ActionID:       id.ActionID(os.Getenv("KODEMA_HELPER_ACTION")),
		WorkloadID:     id.WorkloadID(os.Getenv("KODEMA_HELPER_WORKLOAD")),
		IdempotencyKey: os.Getenv("KODEMA_HELPER_IDEMPOTENCY"),
	}
	workspaceID := id.WorkspaceID(os.Getenv("KODEMA_HELPER_WORKSPACE"))
	if identity.ExecutionID == "" || identity.WorkloadID == "" || workspaceID == "" {
		t.Fatal("helper environment is missing the parent-chosen identity/workspace")
	}

	libpod, err := podmandriver.NewLibpod(podmandriver.LibpodConfig{SocketPath: socket, Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("helper NewLibpod: %v", err)
	}
	files, err := podmandriver.NewFiles(root)
	if err != nil {
		t.Fatalf("helper NewFiles: %v", err)
	}
	adapter, err := podman.New(podman.Config{
		Libpod:    runtimesupport.ScopedLibpod(&blockingStartLibpod{Libpod: libpod, marker: marker}),
		Files:     files,
		Root:      root,
		KodemaURL: testKodemaURL,
		Cleanup:   podman.CleanupPolicy{Attempts: 1},
	})
	if err != nil {
		t.Fatalf("helper adapter: %v", err)
	}
	request := testPrepareRequest(identity, workspaceID, "build", "task-1",
		testContainer(image, []string{`echo launched >> /workspace/launch-count`}, nil, nil), nil, "tok-abc")
	if _, err := adapter.Prepare(context.Background(), request); err != nil {
		t.Fatalf("helper Prepare: %v", err)
	}
	_, _ = adapter.Start(context.Background(), reconcile.StartRequest{Identity: identity, WorkspaceID: workspaceID})
	t.Fatal("helper returned from a blocked start")
}

// blockingStartLibpod publishes a marker and blocks inside the start call.
type blockingStartLibpod struct {
	podman.Libpod
	marker string
}

func (b *blockingStartLibpod) StartContainer(ctx context.Context, name string) error {
	if err := os.WriteFile(b.marker, []byte("start-issued"), 0o644); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

// TestPodmanRuntimeKilledControllerBetweenLaunchAndObservation SIGKILLs a real
// controller after it persisted the launch intent and created the container but
// before it observed the start. A reconstructed controller must report unknown,
// never never-started, and must not replay the command.
func TestPodmanRuntimeKilledControllerBetweenLaunchAndObservation(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-kill")
	identity := testIdentity("exec-kill", "action-kill", "workload-kill", "idem-kill")
	marker := filepath.Join(t.TempDir(), "start-issued")

	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	helper := exec.Command(executable, "-test.run=^TestPodmanKillHelperProcess$", "-test.v")
	helper.Env = append(os.Environ(),
		"KODEMA_RUNTIME_HELPER=1",
		"KODEMA_HELPER_ROOT="+fixture.root,
		"KODEMA_HELPER_SOCKET="+fixture.service.Socket(),
		"KODEMA_HELPER_MARKER="+marker,
		"KODEMA_HELPER_IMAGE="+fixture.image.Reference,
		"KODEMA_HELPER_EXECUTION="+string(identity.ExecutionID),
		"KODEMA_HELPER_ACTION="+string(identity.ActionID),
		"KODEMA_HELPER_WORKLOAD="+string(identity.WorkloadID),
		"KODEMA_HELPER_IDEMPOTENCY="+identity.IdempotencyKey,
		"KODEMA_HELPER_WORKSPACE="+string(fixture.workspaceID),
		// The helper's container belongs to the parent suite's run scope, so a
		// sweep by the parent still removes it and dead-owner cleanup sees the
		// parent's pid, not the killed helper's.
		runtimesupport.RunOwnerEnv+"="+runtimesupport.RunOwnerPID(),
	)
	var output bytes.Buffer
	helper.Stdout = &output
	helper.Stderr = &output
	if err := helper.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer func() {
		_ = helper.Process.Kill()
		_, _ = helper.Process.Wait()
	}()
	waitForFile(t, ctx, marker)
	if err := helper.Process.Kill(); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	_, _ = helper.Process.Wait()

	recovered := newPodmanAdapterOnly(t, fixture)
	observation, err := recovered.Observe(ctx, reconcile.ObserveRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err != nil {
		t.Fatalf("Observe after kill: %v", err)
	}
	if observation.Kind == engine.ObservationNeverStarted || observation.ProofOfNonexecution() {
		t.Fatalf("killed controller collapsed into never-started: %+v", observation)
	}
	start, err := recovered.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err == nil || start.Outcome != reconcile.StartUnknown {
		t.Fatalf("reconstructed start after kill = %q/%v; want unknown", start.Outcome, err)
	}
	if _, found := runtimesupport.ReadVolumeFile(ctx, t, fixture.volumeName, "launch-count"); found {
		t.Fatal("the killed controller's command ran before observation")
	}
	if names := podmanContainerNames(t, ctx, fixture.workspaceID); len(names) != 1 {
		t.Fatalf("workspace containers after kill = %v; want exactly one created container", names)
	}
	if err := runtimesupport.RemoveRunContainersInWorkspace(ctx, string(fixture.workspaceID)); err != nil {
		t.Fatalf("remove created container: %v", err)
	}
	if removal, err := recovered.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}); err != nil || !removal.Removed {
		t.Fatalf("Remove = %+v, %v; want removed", removal, err)
	}
	if err := recovered.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "killed-controller-between-launch-and-observation",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}
