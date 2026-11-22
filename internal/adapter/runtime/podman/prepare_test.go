// Podman-specific preparation and host-layout behavior: 0444/0700 host modes,
// permissive secrets-directory refusal, symlink confinement, local image store
// checks, partial-write recovery, libpod mount wiring, and post-restart
// container digest conflict detection.
//
// Shared container-runtime preparation invariants (activation materialization,
// task files, idempotency, ChangedIdempotencyContent, invalid input refusal,
// missing workspace refusal, labels) are verified in TestPodmanConformance.
package podman

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func TestPrepareHostLayoutModesAndConfinement(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)

	for _, path := range []string{
		fx.hostPath("/kodema/inputs/input-a"),
		fx.hostPath("/kodema/outputs/build/out-a"),
		fx.hostPath(workspace.TaskPath),
		fx.scriptPath(),
		fx.secretPath("API_KEY"),
		fx.credentialPath(),
	} {
		if mode := hostFileMode(t, path); mode != 0o444 {
			t.Fatalf("%s mode = %o, want 0444", path, mode)
		}
	}
	if mode := hostFileMode(t, fx.secretsRoot()); mode != 0o700 {
		t.Fatalf("secrets directory mode = %o, want 0700", mode)
	}

	// /kodema/secrets inside the activation bind must be an empty mount point;
	// secrets and KODEMA_TOKEN must never be written into <root>/<workload>/kodema.
	if entries, err := os.ReadDir(fx.hostPath(workspace.SecretRoot)); err != nil {
		t.Fatalf("reading /kodema/secrets mount point: %v", err)
	} else if len(entries) != 0 {
		t.Fatalf("/kodema/secrets mount point holds %d entries; want empty", len(entries))
	}

	if calls := fx.libpod.inspectedVolumes(); len(calls) != 1 || calls[0] != fx.volumeName {
		t.Fatalf("Inspected volumes = %v, want %q", calls, fx.volumeName)
	}
	if calls := fx.libpod.inspectedImages(); len(calls) != 1 || calls[0] != testImage {
		t.Fatalf("Inspected images = %v, want %q", calls, testImage)
	}
}

func TestPrepareRefusesPermissiveSecretsDirectory(t *testing.T) {
	fx := newFixture(t)
	if err := os.MkdirAll(fx.secretsRoot(), 0o755); err != nil {
		t.Fatalf("creating permissive secrets dir: %v", err)
	}
	if err := os.Chmod(fx.secretsRoot(), 0o755); err != nil {
		t.Fatalf("chmod permissive secrets dir: %v", err)
	}

	_, err := fx.adapter.Prepare(context.Background(), fx.request)
	assertFaultClass(t, err, fault.Forbidden)
	if calls := fx.files.callCount("WriteFile"); calls != 0 {
		t.Fatalf("permissive secrets dir wrote %d files", calls)
	}
}

func TestPrepareRefusesForeignWorkspaceVolume(t *testing.T) {
	fx := newFixture(t)
	fx.libpod.addVolume(fx.volumeName, map[string]string{LabelWorkspace: "workspace-2"})
	_, err := fx.adapter.Prepare(context.Background(), fx.request)
	assertFaultClass(t, err, fault.Forbidden)
	if calls := fx.files.callCount("WriteFile"); calls != 0 {
		t.Fatalf("foreign workspace volume wrote %d files", calls)
	}
}

func TestPrepareFailsFastWhenWorkloadImageIsAbsentLocally(t *testing.T) {
	t.Run("missing image refuses before writing preparation", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.deleteImage(testImage)

		_, err := fx.adapter.Prepare(context.Background(), fx.request)
		assertFaultClass(t, err, fault.DependencyUnavailable)
		wantDetail := `workload image "` + testImage + `" is not present in the local container store; pull it before starting the execution`
		if message, ok := fault.MessageOf(err); !ok || message != wantDetail {
			t.Fatalf("Prepare error message = %q (ok=%v); want %q", message, ok, wantDetail)
		}
		if calls := fx.files.callCount("WriteFile"); calls != 0 {
			t.Fatalf("missing workload image wrote %d files", calls)
		}
		if hostFileExists(t, fx.workloadRoot()) {
			t.Fatal("missing workload image left a materialization root behind")
		}

		// Once the image is present locally, preparation and start succeed.
		fx.libpod.addImage(testImage)
		fx.prepare(t)
		if accepted := fx.start(t); accepted.Outcome != reconcile.StartAccepted {
			t.Fatalf("start outcome after pulling image = %q, want accepted", accepted.Outcome)
		}
	})

	t.Run("unreachable image store refuses as dependency unavailable", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.setImageError(errFakeUnavailable)

		_, err := fx.adapter.Prepare(context.Background(), fx.request)
		assertFaultClass(t, err, fault.DependencyUnavailable)
		if strings.Contains(err.Error(), errFakeUnavailable.Error()) {
			t.Fatalf("Prepare leaked private image store error: %v", err)
		}
		if calls := fx.files.callCount("WriteFile"); calls != 0 {
			t.Fatalf("unreachable image store wrote %d files", calls)
		}
	})
}

func TestPrepareRefusesSymlinkEscape(t *testing.T) {
	fx := newFixture(t)
	outside := t.TempDir()
	if err := os.MkdirAll(fx.workloadRoot(), 0o755); err != nil {
		t.Fatalf("creating workload root: %v", err)
	}
	if err := os.Symlink(outside, fx.kodemaRoot()); err != nil {
		t.Fatalf("creating escape symlink: %v", err)
	}

	_, err := fx.adapter.Prepare(context.Background(), fx.request)
	assertFaultClass(t, err, fault.Forbidden)

	if entries, readErr := os.ReadDir(outside); readErr != nil {
		t.Fatalf("reading outside directory: %v", readErr)
	} else if len(entries) != 0 {
		t.Fatalf("Prepare wrote %d entries through a symlink outside the root", len(entries))
	}

	result, startErr := fx.startResult(t)
	if startErr == nil || result.Outcome != reconcile.StartUnknown {
		t.Fatalf("Start after symlink escape = (%+v, %v); want unknown error", result, startErr)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 0 {
		t.Fatal("Start created a container from an unsafe preparation")
	}
}

func TestPreparePartialFailureNeverAuthorizesStart(t *testing.T) {
	fx := newFixture(t)
	fx.files.failOnCall("WriteFile", 2, errFakeUnavailable)

	_, err := fx.adapter.Prepare(context.Background(), fx.request)
	assertFaultClass(t, err, fault.DependencyUnavailable)

	result, startErr := fx.startResult(t)
	if startErr == nil || result.Outcome != reconcile.StartUnknown {
		t.Fatalf("Start after partial prepare = (%+v, %v); want unknown error", result, startErr)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 0 {
		t.Fatal("Start created a container from a partial preparation")
	}

	fx.files.clearFailOn("WriteFile")
	fx.prepare(t)
	if accepted := fx.start(t); accepted.Outcome != reconcile.StartAccepted {
		t.Fatalf("start outcome after recovery = %q, want accepted", accepted.Outcome)
	}
}

func TestStartBuildsLibpodContainerMountsAndSpec(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.start(t)

	spec := fx.createdSpec(t)
	if spec.Name != fx.containerName {
		t.Fatalf("container name = %q, want %q", spec.Name, fx.containerName)
	}
	if spec.Image != testImage {
		t.Fatalf("container image = %q, want %q", spec.Image, testImage)
	}
	if spec.RestartPolicy != "no" {
		t.Fatalf("restart policy = %q, want no", spec.RestartPolicy)
	}
	if len(spec.Labels[LabelPreparation]) != 48 {
		t.Fatalf("label %q = %q, want 48-char preparation digest", LabelPreparation, spec.Labels[LabelPreparation])
	}
	if len(spec.Mounts) != 3 {
		t.Fatalf("mounts = %+v, want activation bind, isolated secrets bind, and workspace volume", spec.Mounts)
	}

	bind, ok := mountAt(spec, "/kodema")
	if !ok || bind.Type != "bind" || bind.Source != fx.kodemaRoot() || !bind.ReadOnly {
		t.Fatalf("activation bind = %+v (ok=%v), want read-only bind from %q", bind, ok, fx.kodemaRoot())
	}
	secBind, ok := mountAt(spec, workspace.SecretRoot)
	if !ok || secBind.Type != "bind" || secBind.Source != fx.secretsRoot() || !secBind.ReadOnly {
		t.Fatalf("secrets bind = %+v (ok=%v), want read-only bind from %q", secBind, ok, fx.secretsRoot())
	}
	if !hostFileExists(t, filepath.Join(secBind.Source, "KODEMA_TOKEN")) {
		t.Fatal("credential is not inside the isolated secrets bind directory")
	}
	vol, ok := mountAt(spec, "/workspace")
	if !ok || vol.Type != "volume" || vol.Source != fx.volumeName || vol.ReadOnly {
		t.Fatalf("workspace volume mount = %+v (ok=%v), want rw volume %q", vol, ok, fx.volumeName)
	}
}

func TestPrepareRefusesPostRestartContainerDigestConflict(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.start(t)

	reconstructed := newAdapterWithRoot(t, fx)
	changed := fx.request
	changed.Inputs = map[id.InputKey]string{"input-a": "mutated\n"}
	_, err := reconstructed.Prepare(context.Background(), changed)
	assertFaultClass(t, err, fault.ChangedIdempotencyContent)
}

func TestPrepareRefusesPostRestartDroppedActivationFile(t *testing.T) {
	fx := newFixture(t)
	fx.request.Inputs = map[id.InputKey]string{
		"input-a": "first\n",
		"input-b": "second\n",
	}
	fx.prepare(t)

	// Simulate an orphaned staging temp file from a crashed write alongside a
	// fresh adapter with an empty in-memory prepared map (before Start).
	staging := filepath.Join(fx.kodemaRoot(), "inputs", ".kodema-write-orphan")
	if err := fx.files.WriteFile(context.Background(), staging, []byte("partial"), readOnlyMode); err != nil {
		t.Fatalf("seed staging file: %v", err)
	}

	reconstructed := newAdapterWithRoot(t, fx)
	// Identical retry ignores the orphaned staging file and succeeds.
	if _, err := reconstructed.Prepare(context.Background(), fx.request); err != nil {
		t.Fatalf("identical post-restart Prepare with staging file = %v; want nil", err)
	}

	// Dropping a previously written input file after restart must conflict.
	reconstructed2 := newAdapterWithRoot(t, fx)
	dropped := fx.request
	dropped.Inputs = map[id.InputKey]string{"input-a": "first\n"}
	_, err := reconstructed2.Prepare(context.Background(), dropped)
	assertFaultClass(t, err, fault.ChangedIdempotencyContent)
}
