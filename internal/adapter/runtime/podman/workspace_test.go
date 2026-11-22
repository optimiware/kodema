// Podman-specific retained workspace behavior: volume label ownership,
// disk-marker-free retention, container-reference-safe release, partial
// RemoveVolume visibility, and per-workspace lock serialization with Start and
// Retain.
//
// Shared reconcile.Workspace invariants (Status/Retain/Release basic lifecycle,
// missing/invalid/cancelled handling, secret-free details) are verified in
// TestPodmanConformance.
package podman

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func testWorkspacePort() id.WorkspaceID {
	return testWorkspaceID()
}

func TestWorkspaceStatusVolumeOwnershipAndErrors(t *testing.T) {
	t.Run("foreign", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.addVolume(fx.volumeName, map[string]string{LabelWorkspace: "workspace-2"})
		observation, err := fx.adapter.Status(context.Background(), testWorkspacePort())
		if err != nil || observation.Available || !strings.Contains(observation.Detail, "not owned") {
			t.Fatalf("Status(foreign) = (%+v, %v); want unavailable foreign detail", observation, err)
		}
	})

	t.Run("unlabelled", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.addVolume(fx.volumeName, nil)
		observation, err := fx.adapter.Status(context.Background(), testWorkspacePort())
		if err != nil || observation.Available {
			t.Fatalf("Status(unlabelled) = (%+v, %v); want unavailable", observation, err)
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.setVolumeError(errFakeUnavailable)
		observation, err := fx.adapter.Status(context.Background(), testWorkspacePort())
		assertFaultClass(t, err, fault.DependencyUnavailable)
		if observation.Available {
			t.Fatalf("observation = %+v, want unreachable volume unavailable", observation)
		}
	})
}

func TestWorkspaceRetainVerifiesOwnershipWithoutDiskMarkers(t *testing.T) {
	t.Run("no host files written", func(t *testing.T) {
		fx := newFixture(t)
		if err := fx.adapter.Retain(context.Background(), testWorkspacePort()); err != nil {
			t.Fatalf("Retain returned error: %v", err)
		}
		if calls := fx.files.callCount("WriteFile"); calls != 0 {
			t.Fatalf("Retain wrote %d host files, want 0 (PostgreSQL owns retention)", calls)
		}
	})

	t.Run("foreign volume", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.addVolume(fx.volumeName, map[string]string{LabelWorkspace: "workspace-2"})
		assertFaultClass(t, fx.adapter.Retain(context.Background(), testWorkspacePort()), fault.Forbidden)
	})
}

func TestWorkspaceReleaseVolumeOwnershipIsolation(t *testing.T) {
	t.Run("foreign workspace is refused", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.addVolume(fx.volumeName, map[string]string{LabelWorkspace: "workspace-2"})
		assertFaultClass(t, fx.adapter.Release(context.Background(), testWorkspacePort()), fault.Forbidden)
		if _, ok := fx.libpod.volume(fx.volumeName); !ok || len(fx.libpod.removedVolumes()) != 0 {
			t.Fatal("Release removed a foreign volume")
		}
	})

	t.Run("unlabelled workspace is refused", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.addVolume(fx.volumeName, nil)
		assertFaultClass(t, fx.adapter.Release(context.Background(), testWorkspacePort()), fault.Forbidden)
		if _, ok := fx.libpod.volume(fx.volumeName); !ok {
			t.Fatal("Release removed an unlabelled volume")
		}
	})

	t.Run("other workspace volume is untouched", func(t *testing.T) {
		fx := newFixture(t)
		otherName, err := WorkspaceVolumeName("workspace-2")
		if err != nil {
			t.Fatalf("WorkspaceVolumeName: %v", err)
		}
		fx.libpod.addVolume(otherName, map[string]string{LabelWorkspace: "workspace-2"})
		if err := fx.adapter.Release(context.Background(), testWorkspacePort()); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if _, ok := fx.libpod.volume(otherName); !ok {
			t.Fatal("Release removed another workspace's volume")
		}
	})
}

func TestWorkspaceReleaseFollowsKernelCleanupOrder(t *testing.T) {
	fx := newFixture(t)
	if err := fx.adapter.Retain(context.Background(), testWorkspacePort()); err != nil {
		t.Fatalf("Retain returned error: %v", err)
	}
	fx.prepare(t)
	fx.start(t)

	evidence := fx.stop(t)
	if !evidence.IsConfirmed() {
		t.Fatalf("Stop evidence = %+v, want a confirmed whole-container stop", evidence)
	}
	removal := fx.remove(t)
	if !removal.Removed {
		t.Fatalf("Remove = %+v, want a confirmed removal", removal)
	}
	if _, ok := fx.libpod.container(fx.containerName); ok {
		t.Fatal("Remove left the container behind")
	}

	if err := fx.adapter.Release(context.Background(), testWorkspacePort()); err != nil {
		t.Fatalf("Release after confirmed cleanup returned error: %v", err)
	}
	if _, ok := fx.libpod.volume(fx.volumeName); ok {
		t.Fatal("Release left the workspace volume behind after confirmed cleanup")
	}
	if removed := fx.libpod.removedVolumes(); len(removed) != 1 || removed[0] != fx.volumeName {
		t.Fatalf("removed volumes = %v, want exactly %q", removed, fx.volumeName)
	}
	if err := fx.adapter.Release(context.Background(), testWorkspacePort()); err != nil {
		t.Fatalf("repeated Release after cleanup returned error: %v", err)
	}
	if removed := fx.libpod.removedVolumes(); len(removed) != 1 {
		t.Fatalf("repeated Release issued %d volume removals, want idempotent 1", len(removed))
	}
}

func TestWorkspaceReleaseRefusesReferencedWorkspace(t *testing.T) {
	t.Run("running container", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.start(t)
		err := fx.adapter.Release(context.Background(), testWorkspacePort())
		assertFaultClass(t, err, fault.Forbidden)
		if !strings.Contains(err.Error(), fx.containerName) {
			t.Fatalf("release error %q does not name the referencing container %q", err, fx.containerName)
		}
		if _, ok := fx.libpod.volume(fx.volumeName); !ok {
			t.Fatal("Release removed a volume referenced by a running container")
		}
		if removed := fx.libpod.removedVolumes(); len(removed) != 0 {
			t.Fatalf("Release issued removals %v, want none", removed)
		}
	})

	t.Run("created container", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedCreated(t, true)
		err := fx.adapter.Release(context.Background(), testWorkspacePort())
		assertFaultClass(t, err, fault.Forbidden)
		if _, ok := fx.libpod.volume(fx.volumeName); !ok {
			t.Fatal("Release removed a volume referenced by an uncertain container")
		}
	})

	t.Run("container reference listing unavailable", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.failNext("ListContainers", errFakeUnavailable, false)
		err := fx.adapter.Release(context.Background(), testWorkspacePort())
		assertFaultClass(t, err, fault.DependencyUnavailable)
		if _, ok := fx.libpod.volume(fx.volumeName); !ok {
			t.Fatal("Release removed a volume whose references were unreachable")
		}
	})
}

func TestWorkspaceReleaseSurfacesPartialRemoval(t *testing.T) {
	t.Run("removal refused before effect", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.failNext("RemoveVolume", errFakeUnavailable, false)
		err := fx.adapter.Release(context.Background(), testWorkspacePort())
		assertFaultClass(t, err, fault.UncertainExternalEffect)
		if _, ok := fx.libpod.volume(fx.volumeName); !ok {
			t.Fatal("a failed removal deleted the volume")
		}
		observation, statusErr := fx.adapter.Status(context.Background(), testWorkspacePort())
		if statusErr != nil || !observation.Available {
			t.Fatalf("post-failure status = %+v, %v; want the volume still observable", observation, statusErr)
		}
		if err := fx.adapter.Release(context.Background(), testWorkspacePort()); err != nil {
			t.Fatalf("retry Release returned error: %v", err)
		}
		if _, ok := fx.libpod.volume(fx.volumeName); ok {
			t.Fatal("retry Release left the volume behind")
		}
	})

	t.Run("lost removal response", func(t *testing.T) {
		fx := newFixture(t)
		fx.libpod.failNext("RemoveVolume", errFakeLost, true)
		err := fx.adapter.Release(context.Background(), testWorkspacePort())
		assertFaultClass(t, err, fault.UncertainExternalEffect)
		if _, ok := fx.libpod.volume(fx.volumeName); ok {
			t.Fatal("the lost-response fake did not apply the removal effect")
		}
		observation, statusErr := fx.adapter.Status(context.Background(), testWorkspacePort())
		if statusErr != nil || observation.Available {
			t.Fatalf("post-lost status = %+v, %v; want the removed volume unavailable", observation, statusErr)
		}
		if err := fx.adapter.Release(context.Background(), testWorkspacePort()); err != nil {
			t.Fatalf("retry Release returned error: %v", err)
		}
	})
}

func TestWorkspaceReleaseWaitsForInFlightStart(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	gate := fx.libpod.newGate("StartContainer")

	startDone := make(chan error, 1)
	go func() {
		_, err := fx.adapter.Start(context.Background(), startRequest(fx))
		startDone <- err
	}()
	waitFor(t, gate.entered, "StartContainer at the barrier")

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- fx.adapter.Release(context.Background(), testWorkspacePort())
	}()
	select {
	case err := <-releaseDone:
		t.Fatalf("Release returned %v while Start held the workspace lock", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(gate.release)
	fx.libpod.clearGate("StartContainer")
	if err := <-startDone; err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if err := <-releaseDone; err == nil {
		t.Fatal("Release succeeded while the started container still referenced the workspace")
	}
	if _, ok := fx.libpod.volume(fx.volumeName); !ok {
		t.Fatal("Release removed the workspace volume after a concurrent start")
	}
}

func TestWorkspaceRetainSerializesWithRelease(t *testing.T) {
	fx := newFixture(t)
	gate := fx.libpod.newGate("InspectVolume")

	retainDone := make(chan error, 1)
	go func() {
		retainDone <- fx.adapter.Retain(context.Background(), testWorkspacePort())
	}()
	waitFor(t, gate.entered, "InspectVolume at the retain barrier")

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- fx.adapter.Release(context.Background(), testWorkspacePort())
	}()
	select {
	case err := <-releaseDone:
		t.Fatalf("Release returned %v while Retain held the workspace lock", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(gate.release)
	fx.libpod.clearGate("InspectVolume")
	if err := <-retainDone; err != nil {
		t.Fatalf("Retain returned error: %v", err)
	}
	if err := <-releaseDone; err != nil {
		t.Fatalf("Release after a completed Retain returned error: %v", err)
	}
	if _, ok := fx.libpod.volume(fx.volumeName); ok {
		t.Fatal("the serialized Retain/Release pair left the volume behind")
	}
}

func TestWorkspaceConcurrentOperationsAreIdempotent(t *testing.T) {
	t.Run("concurrent release", func(t *testing.T) {
		fx := newFixture(t)
		var wait sync.WaitGroup
		errs := make([]error, 2)
		for index := range errs {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				errs[index] = fx.adapter.Release(context.Background(), testWorkspacePort())
			}(index)
		}
		wait.Wait()
		for index, err := range errs {
			if err != nil {
				t.Fatalf("concurrent Release %d returned error: %v", index, err)
			}
		}
		if _, ok := fx.libpod.volume(fx.volumeName); ok {
			t.Fatal("concurrent Release left the workspace volume behind")
		}
		if removed := fx.libpod.removedVolumes(); len(removed) != 1 {
			t.Fatalf("concurrent Release issued %d removals, want exactly 1", len(removed))
		}
	})

	t.Run("concurrent retain", func(t *testing.T) {
		fx := newFixture(t)
		var wait sync.WaitGroup
		errs := make([]error, 2)
		for index := range errs {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				errs[index] = fx.adapter.Retain(context.Background(), testWorkspacePort())
			}(index)
		}
		wait.Wait()
		for index, err := range errs {
			if err != nil {
				t.Fatalf("concurrent Retain %d returned error: %v", index, err)
			}
		}
		if writes := fx.files.callCount("WriteFile"); writes != 0 {
			t.Fatalf("concurrent Retain wrote %d files, want 0", writes)
		}
	})
}
