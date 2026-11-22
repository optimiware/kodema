// Lifecycle behavior: stable identities, start outcomes, restart recovery, the
// declared observation vocabulary, the whole-container stop barrier, idempotent
// identity-scoped cleanup, 5-op workspace ownership enforcement, and credential
// refresh without a restart.
package podman

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func startRequest(fx *fixture) reconcile.StartRequest {
	return reconcile.StartRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	}
}

func observeRequest(fx *fixture) reconcile.ObserveRequest {
	return reconcile.ObserveRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	}
}

func stopRequest(fx *fixture) reconcile.StopRequest {
	return reconcile.StopRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	}
}

func removeRequest(fx *fixture) reconcile.RemoveRequest {
	return reconcile.RemoveRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	}
}

func TestStartCreatesOneStableContainerAndReusesOnRetry(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)

	result := fx.start(t)
	if result.Outcome != reconcile.StartAccepted {
		t.Fatalf("start outcome = %q, want %q", result.Outcome, reconcile.StartAccepted)
	}
	if result.WorkloadID != fx.request.Identity.WorkloadID {
		t.Fatalf("start workload = %q, want the persisted identity", result.WorkloadID)
	}
	if result.Detail == "" {
		t.Fatal("accepted start has no detail")
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
		t.Fatalf("CreateContainer calls = %d, want exactly 1", calls)
	}
	if calls := fx.libpod.callsFor("StartContainer"); calls != 1 {
		t.Fatalf("StartContainer calls = %d, want exactly 1", calls)
	}
	state, ok := fx.libpod.container(fx.containerName)
	if !ok {
		t.Fatal("Start did not create the stable container")
	}
	if state.Status != ContainerStatusRunning {
		t.Fatalf("container status = %q, want running", state.Status)
	}

	again := fx.start(t)
	if again.Outcome != reconcile.StartAccepted {
		t.Fatalf("retry outcome = %q, want accepted reuse", again.Outcome)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
		t.Fatalf("retry created a second container: %d creates", calls)
	}
	if calls := fx.libpod.callsFor("StartContainer"); calls != 1 {
		t.Fatalf("retry restarted the running container: %d starts", calls)
	}

	fx.seedExited(t, 0, true)
	afterExit := fx.start(t)
	if afterExit.Outcome != reconcile.StartAccepted {
		t.Fatalf("start after exit outcome = %q, want accepted reuse", afterExit.Outcome)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
		t.Fatalf("start after exit replayed the command: %d creates", calls)
	}
}

func TestStartLostCreateReplyStaysUnknownAndCreatedContainerStaysUncertain(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.libpod.failNext("CreateContainer", errFakeLost, true)

	result, err := fx.startResult(t)
	if err == nil {
		t.Fatal("Start hid a lost create response")
	}
	if result.Outcome != reconcile.StartUnknown {
		t.Fatalf("lost-create outcome = %q, want %q", result.Outcome, reconcile.StartUnknown)
	}
	if count := fx.libpod.containerCount(); count != 1 {
		t.Fatalf("lost create response left %d containers, want 1", count)
	}

	// Without local disk state machines, a container left in "created" status
	// stays conservative: Start reports StartUnknown (UncertainExternalEffect)
	// and never creates a second container or replays StartContainer.
	retry, retryErr := fx.startResult(t)
	assertFaultClass(t, retryErr, fault.UncertainExternalEffect)
	if retry.Outcome != reconcile.StartUnknown {
		t.Fatalf("retry outcome = %q, want unknown", retry.Outcome)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
		t.Fatalf("retry created a second container: %d creates", calls)
	}
	if calls := fx.libpod.callsFor("StartContainer"); calls != 0 {
		t.Fatalf("retry issued StartContainer %d times for a created container", calls)
	}

	// Stop confirms whole-container termination and allows Remove to clean up.
	if evidence := fx.stop(t); !evidence.IsConfirmed() {
		t.Fatal("Stop on a created container did not confirm termination")
	}
	if removal := fx.remove(t); !removal.Removed {
		t.Fatalf("Remove after confirmed stop = %+v, want success", removal)
	}
}

func TestStartCreateFailureBeforeEffectStaysUnknownThenRetryCreates(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.libpod.failNext("CreateContainer", errFakeUnavailable, false)

	result, err := fx.startResult(t)
	if err == nil {
		t.Fatal("Start hid an unavailable daemon")
	}
	if result.Outcome != reconcile.StartUnknown {
		t.Fatalf("outcome = %q, want unknown", result.Outcome)
	}
	if count := fx.libpod.containerCount(); count != 0 {
		t.Fatalf("failed create left %d containers", count)
	}

	retry := fx.start(t)
	if retry.Outcome != reconcile.StartAccepted {
		t.Fatalf("retry outcome = %q, want accepted", retry.Outcome)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 2 {
		t.Fatalf("CreateContainer calls = %d, want one failed and one successful", calls)
	}
	if count := fx.libpod.containerCount(); count != 1 {
		t.Fatalf("containers after retry = %d, want 1", count)
	}
}

func TestStartLostStartReplyStaysUnknownAndNeverReplays(t *testing.T) {
	t.Run("lost response after the effect", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.libpod.failNext("StartContainer", errFakeLost, true)

		result, err := fx.startResult(t)
		if err == nil {
			t.Fatal("Start hid a lost start response")
		}
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("lost-start outcome = %q, want unknown", result.Outcome)
		}
		state, ok := fx.libpod.container(fx.containerName)
		if !ok || state.Status != ContainerStatusRunning {
			t.Fatalf("container after lost start = %+v, want running", state)
		}

		retry := fx.start(t)
		if retry.Outcome != reconcile.StartAccepted {
			t.Fatalf("retry outcome = %q, want accepted reuse", retry.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
			t.Fatalf("retry created a second container: %d", calls)
		}
		if calls := fx.libpod.callsFor("StartContainer"); calls != 1 {
			t.Fatalf("retry re-issued start after the effect: %d starts", calls)
		}
	})

	t.Run("failure before the effect", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.libpod.failNext("StartContainer", errFakeUnavailable, false)

		result, err := fx.startResult(t)
		if err == nil {
			t.Fatal("Start hid an unavailable daemon")
		}
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", result.Outcome)
		}
		state, ok := fx.libpod.container(fx.containerName)
		if !ok || state.Status != ContainerStatusCreated || state.Started {
			t.Fatalf("container after failed start = %+v, want created", state)
		}
		if observation := fx.observe(t); observation.Kind != engine.ObservationUnknown {
			t.Fatalf("observation after an ambiguous start = %q, want unknown", observation.Kind)
		}
		if removal := fx.remove(t); removal.Removed {
			t.Fatal("Remove succeeded while a created container may still start")
		}

		retry, err := fx.startResult(t)
		assertFaultClass(t, err, fault.UncertainExternalEffect)
		if retry.Outcome != reconcile.StartUnknown {
			t.Fatalf("retry outcome = %q, want unknown", retry.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
			t.Fatalf("retry created a second container: %d", calls)
		}
		if calls := fx.libpod.callsFor("StartContainer"); calls != 1 {
			t.Fatalf("retry starts = %d, want the ambiguous start never replayed", calls)
		}
		if count := fx.libpod.containerCount(); count != 1 {
			t.Fatalf("containers after retry = %d, want 1", count)
		}

		if evidence := fx.stop(t); !evidence.IsConfirmed() {
			t.Fatal("Stop on a created container did not confirm whole-container termination")
		}
		if removal := fx.remove(t); !removal.Removed {
			t.Fatalf("removal after confirmed stop = %+v, want success", removal)
		}
	})
}

func TestStartPendingDelayedStartIsUnknownAndNotRestarted(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedCreated(t, true)

	result, err := fx.startResult(t)
	assertFaultClass(t, err, fault.UncertainExternalEffect)
	if result.Outcome != reconcile.StartUnknown {
		t.Fatalf("outcome = %q, want unknown", result.Outcome)
	}
	if calls := fx.libpod.callsFor("StartContainer"); calls != 0 {
		t.Fatalf("Start re-issued a delayed start %d times", calls)
	}

	observation := fx.observe(t)
	if observation.Kind != engine.ObservationUnknown {
		t.Fatalf("observation = %q, want unknown while a delayed start is possible", observation.Kind)
	}

	fx.seedRunning(t)
	observation = fx.observe(t)
	if observation.Kind != engine.ObservationRunning {
		t.Fatalf("observation after delayed start = %q, want running", observation.Kind)
	}
}

func TestStartRefusesWithoutPreparationOrWorkspace(t *testing.T) {
	t.Run("no preparation", func(t *testing.T) {
		fx := newFixture(t)
		result, err := fx.startResult(t)
		assertFaultClass(t, err, fault.InvalidInput)
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", result.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 0 {
			t.Fatal("Start created a container without preparation")
		}
	})

	t.Run("missing workspace volume after preparation", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.libpod.deleteVolume(fx.volumeName)
		result, err := fx.startResult(t)
		assertFaultClass(t, err, fault.NotFound)
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", result.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 0 {
			t.Fatal("Start created a container without the retained workspace volume")
		}
	})

	t.Run("workspace mismatch", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		result, err := fx.adapter.Start(context.Background(), reconcile.StartRequest{
			Identity:    fx.request.Identity,
			WorkspaceID: "workspace-2",
		})
		assertFaultClass(t, err, fault.Forbidden)
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", result.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 0 {
			t.Fatal("Start created a container for a mismatched workspace")
		}
	})
}

func TestStartAfterReconstructionReusesExistingContainerOrRequiresPrepare(t *testing.T) {
	t.Run("without container requires completed preparation", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)

		reconstructed := newAdapterWithRoot(t, fx)
		result, err := reconstructed.Start(context.Background(), startRequest(fx))
		assertFaultClass(t, err, fault.InvalidInput)
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", result.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 0 {
			t.Fatalf("CreateContainer calls = %d, want 0 without preparation", calls)
		}

		// Re-preparing idempotently repopulates in-process state and Start succeeds.
		if _, err := reconstructed.Prepare(context.Background(), fx.request); err != nil {
			t.Fatalf("reconstructed Prepare: %v", err)
		}
		result, err = reconstructed.Start(context.Background(), startRequest(fx))
		if err != nil {
			t.Fatalf("Start after re-Prepare: %v", err)
		}
		if result.Outcome != reconcile.StartAccepted {
			t.Fatalf("outcome = %q, want accepted", result.Outcome)
		}
	})

	t.Run("reuses existing running or exited container without re-Prepare", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.start(t)

		reconstructed := newAdapterWithRoot(t, fx)
		result, err := reconstructed.Start(context.Background(), startRequest(fx))
		if err != nil {
			t.Fatalf("reconstructed Start on running container: %v", err)
		}
		if result.Outcome != reconcile.StartAccepted {
			t.Fatalf("outcome = %q, want accepted reuse", result.Outcome)
		}
		if calls := fx.libpod.callsFor("CreateContainer"); calls != 1 {
			t.Fatalf("reconstruction replayed the container: %d creates", calls)
		}
	})

	t.Run("created container stays uncertain after reconstruction", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedCreated(t, false)

		reconstructed := newAdapterWithRoot(t, fx)
		result, err := reconstructed.Start(context.Background(), startRequest(fx))
		assertFaultClass(t, err, fault.UncertainExternalEffect)
		if result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", result.Outcome)
		}
	})
}

func TestObserveUsesKernelVocabulary(t *testing.T) {
	t.Run("absent container always reports absent without proof", func(t *testing.T) {
		fx := newFixture(t)
		// Even before Prepare or after Prepare without Start, absence is never
		// proof of nonexecution (S07 §3).
		for _, stage := range []string{"unprepared", "prepared"} {
			if stage == "prepared" {
				fx.prepare(t)
			}
			observation := fx.observe(t)
			if observation.Kind != engine.ObservationAbsent {
				t.Fatalf("%s: observation = %q, want absent", stage, observation.Kind)
			}
			if observation.ProofOfNonexecution() {
				t.Fatalf("%s: absence was treated as proof of nonexecution", stage)
			}
			if !observation.Valid() {
				t.Fatalf("%s: observation is not internally consistent: %+v", stage, observation)
			}
		}
	})

	t.Run("created container stays unknown conservatively", func(t *testing.T) {
		for _, startPending := range []bool{false, true} {
			fx := newFixture(t)
			fx.prepare(t)
			fx.seedCreated(t, startPending)
			observation := fx.observe(t)
			if observation.Kind != engine.ObservationUnknown {
				t.Fatalf("startPending=%t: observation = %q, want unknown", startPending, observation.Kind)
			}
			if observation.ProofOfNonexecution() {
				t.Fatalf("startPending=%t: created container was treated as never started", startPending)
			}
		}
	})

	t.Run("running", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedRunning(t)
		observation := fx.observe(t)
		if observation.Kind != engine.ObservationRunning {
			t.Fatalf("observation = %q, want running", observation.Kind)
		}
		if observation.StopConfirmed() {
			t.Fatal("a running container reported a confirmed stop")
		}
	})

	t.Run("exited with whole-container stop", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedExited(t, 7, true)
		observation := fx.observe(t)
		if observation.Kind != engine.ObservationExited {
			t.Fatalf("observation = %q, want exited", observation.Kind)
		}
		code, ok := observation.ExitStatus()
		if !ok || code != 7 {
			t.Fatalf("exit status = %d (present=%v), want 7", code, ok)
		}
		if !observation.StopConfirmed() {
			t.Fatal("affirmative whole-container termination did not confirm the stop")
		}
		if !observation.Valid() {
			t.Fatalf("observation is not internally consistent: %+v", observation)
		}
	})

	t.Run("exited without whole-container evidence", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedExited(t, 7, false)
		observation := fx.observe(t)
		if observation.Kind != engine.ObservationExited {
			t.Fatalf("observation = %q, want exited", observation.Kind)
		}
		if observation.StopConfirmed() {
			t.Fatal("an init exit without whole-container evidence confirmed the stop")
		}
		if !observation.Valid() {
			t.Fatalf("observation is not internally consistent: %+v", observation)
		}
	})

	t.Run("inspect error", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedRunning(t)
		fx.libpod.setInspectError(errFakeUnavailable)
		observation, err := fx.adapter.Observe(context.Background(), observeRequest(fx))
		if err == nil {
			t.Fatal("Observe hid an unreachable daemon")
		}
		if observation.Kind != engine.ObservationUnknown {
			t.Fatalf("observation = %q, want unknown", observation.Kind)
		}
		if observation.ProofOfNonexecution() {
			t.Fatal("an unreachable daemon was treated as never started")
		}
	})

	t.Run("foreign container labels", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		state := fx.seedRunning(t)
		state.Labels[LabelWorkload] = "other-workload"
		fx.libpod.setContainer(state)
		observation, err := fx.adapter.Observe(context.Background(), observeRequest(fx))
		if err == nil {
			t.Fatal("Observe accepted a foreign container")
		}
		if observation.Kind != engine.ObservationUnknown {
			t.Fatalf("observation = %q, want unknown for a foreign container", observation.Kind)
		}
	})
}

func TestWorkspaceOwnershipEnforcedAcrossAllFiveOperations(t *testing.T) {
	wrongWorkspace := id.WorkspaceID("workspace-2")

	t.Run("in-process prepared workspace mismatch", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedExited(t, 0, true)

		prepReq := fx.request
		prepReq.WorkspaceID = wrongWorkspace
		_, err := fx.adapter.Prepare(context.Background(), prepReq)
		assertFaultClass(t, err, fault.Forbidden)

		_, err = fx.adapter.Start(context.Background(), reconcile.StartRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)

		_, err = fx.adapter.Observe(context.Background(), reconcile.ObserveRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)

		evidence, err := fx.adapter.Stop(context.Background(), reconcile.StopRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)
		if evidence.IsConfirmed() {
			t.Fatal("Stop confirmed on mismatched workspace")
		}

		removal, err := fx.adapter.Remove(context.Background(), reconcile.RemoveRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)
		if removal.Removed {
			t.Fatal("Remove succeeded on mismatched workspace")
		}
	})

	t.Run("post-restart container workspace label mismatch", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedExited(t, 0, true)

		otherVol, err := WorkspaceVolumeName(wrongWorkspace)
		if err != nil {
			t.Fatal(err)
		}
		fx.libpod.addVolume(otherVol, map[string]string{LabelWorkspace: string(wrongWorkspace)})

		reconstructed := newAdapterWithRoot(t, fx)

		prepReq := fx.request
		prepReq.WorkspaceID = wrongWorkspace
		_, err = reconstructed.Prepare(context.Background(), prepReq)
		assertFaultClass(t, err, fault.Forbidden)

		_, err = reconstructed.Start(context.Background(), reconcile.StartRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)

		_, err = reconstructed.Observe(context.Background(), reconcile.ObserveRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)

		evidence, err := reconstructed.Stop(context.Background(), reconcile.StopRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)
		if evidence.IsConfirmed() {
			t.Fatal("post-restart Stop confirmed on mismatched workspace")
		}

		removal, err := reconstructed.Remove(context.Background(), reconcile.RemoveRequest{
			Identity: fx.request.Identity, WorkspaceID: wrongWorkspace,
		})
		assertFaultClass(t, err, fault.Forbidden)
		if removal.Removed {
			t.Fatal("post-restart Remove succeeded on mismatched workspace")
		}
		if _, ok := fx.libpod.container(fx.containerName); !ok {
			t.Fatal("post-restart Remove deleted container bound to another workspace")
		}
	})
}

func TestStopAcknowledgmentIsNotConfirmation(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedRunning(t)
	fx.libpod.setStopEffect(false)

	evidence := fx.stop(t)
	if evidence.IsConfirmed() {
		t.Fatal("a stop acknowledgment confirmed the stop")
	}
	if evidence.Detail == "" {
		t.Fatal("pending stop has no detail")
	}
	if calls := fx.libpod.stoppedNames(); len(calls) != 1 || calls[0] != fx.containerName {
		t.Fatalf("stopped names = %v, want the stable container %q", calls, fx.containerName)
	}

	fx.seedExited(t, 0, false)
	evidence = fx.stop(t)
	if evidence.IsConfirmed() {
		t.Fatal("an init exit without whole-container evidence confirmed the stop")
	}

	fx.seedExited(t, 0, true)
	evidence = fx.stop(t)
	if !evidence.IsConfirmed() {
		t.Fatal("affirmative whole-container termination did not confirm the stop")
	}
	if evidence.Detail == "" {
		t.Fatal("confirmed stop has no detail")
	}
}

func TestStopOnAbsentContainerStaysPending(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	evidence := fx.stop(t)
	if evidence.IsConfirmed() {
		t.Fatal("stopping an absent container confirmed the stop")
	}
	if evidence.Detail == "" {
		t.Fatal("pending stop has no detail")
	}
}

func TestStopLostResponseStaysPending(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedRunning(t)
	fx.libpod.failNext("StopContainer", errFakeLost, false)

	evidence, err := fx.adapter.Stop(context.Background(), stopRequest(fx))
	if err == nil {
		t.Fatal("Stop hid a lost stop response")
	}
	if evidence.IsConfirmed() {
		t.Fatal("a lost stop response was treated as confirmed")
	}

	fx.seedExited(t, 0, true)
	evidence = fx.stop(t)
	if !evidence.IsConfirmed() {
		t.Fatal("observed whole-container termination after a lost response did not confirm the stop")
	}
}

func TestRemoveIsIdempotentAndIdentityScoped(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedExited(t, 0, true)

	otherWorkload := id.WorkloadID("workload-2")
	otherName, err := ContainerName(otherWorkload)
	if err != nil {
		t.Fatalf("ContainerName returned error: %v", err)
	}
	otherRoot := filepath.Join(fx.root, string(otherWorkload))
	if err := os.MkdirAll(filepath.Join(otherRoot, "kodema"), 0o755); err != nil {
		t.Fatalf("creating other workload root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(otherRoot, "kodema", "keep"), []byte("keep"), 0o444); err != nil {
		t.Fatalf("seeding other workload: %v", err)
	}
	fx.libpod.setContainer(ContainerState{
		ID:     "container-" + otherName,
		Name:   otherName,
		Status: ContainerStatusRunning,
		Labels: map[string]string{LabelWorkload: string(otherWorkload)},
	})

	removal := fx.remove(t)
	if !removal.Removed {
		t.Fatalf("removal = %+v, want the owned resources removed", removal)
	}
	if removal.Detail == "" {
		t.Fatal("removal has no detail")
	}
	if _, ok := fx.libpod.container(fx.containerName); ok {
		t.Fatal("Remove left the identity's container behind")
	}
	if _, ok := fx.libpod.container(otherName); !ok {
		t.Fatal("Remove deleted another identity's container")
	}
	if hostFileExists(t, fx.workloadRoot()) {
		t.Fatal("Remove left the identity's materialization behind")
	}
	if !hostFileExists(t, filepath.Join(otherRoot, "kodema", "keep")) {
		t.Fatal("Remove deleted another identity's materialization")
	}
	if volume, ok := fx.libpod.volume(fx.volumeName); !ok {
		t.Fatal("Remove deleted the retained workspace volume")
	} else if volume.Labels[LabelWorkspace] != string(fx.request.WorkspaceID) {
		t.Fatalf("workspace volume changed: %+v", volume)
	}
	for _, name := range fx.libpod.removedNames() {
		if name != fx.containerName {
			t.Fatalf("Remove removed a container it does not own: %q", name)
		}
	}

	second := fx.remove(t)
	if !second.Removed {
		t.Fatalf("repeated removal = %+v, want idempotent success", second)
	}
	if _, ok := fx.libpod.container(otherName); !ok {
		t.Fatal("repeated Remove deleted another identity's container")
	}
}

func TestRemoveRetainsWhileContainerMayRun(t *testing.T) {
	t.Run("running", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedRunning(t)
		removal := fx.remove(t)
		if removal.Removed {
			t.Fatal("Remove reported success while the container may still run")
		}
		if !strings.Contains(removal.Detail, fx.containerName) {
			t.Fatalf("removal detail %q does not name the retained container %q", removal.Detail, fx.containerName)
		}
		if !hostFileExists(t, fx.workloadRoot()) {
			t.Fatal("Remove deleted the materialization while the container may run")
		}
		if calls := fx.libpod.removedNames(); len(calls) != 0 {
			t.Fatalf("Remove removed a live container: %v", calls)
		}
	})

	t.Run("exited without whole-container evidence", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		fx.seedExited(t, 0, false)
		removal := fx.remove(t)
		if removal.Removed {
			t.Fatal("Remove reported success without whole-container stop evidence")
		}
		if !hostFileExists(t, fx.workloadRoot()) {
			t.Fatal("Remove deleted the materialization without stop evidence")
		}
	})

	t.Run("created container is conservatively non-removable", func(t *testing.T) {
		for _, startPending := range []bool{false, true} {
			fx := newFixture(t)
			fx.prepare(t)
			fx.seedCreated(t, startPending)
			removal := fx.remove(t)
			if removal.Removed {
				t.Fatalf("startPending=%t: Remove reported success for created container", startPending)
			}
			if _, ok := fx.libpod.container(fx.containerName); !ok {
				t.Fatalf("startPending=%t: Remove deleted created container", startPending)
			}
			if !hostFileExists(t, fx.workloadRoot()) {
				t.Fatalf("startPending=%t: Remove deleted materialization of created container", startPending)
			}
		}
	})
}

func TestRemoveBoundsRetriesWithInjectedBackoff(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedExited(t, 0, true)
	fx.libpod.failRemoveTimes(2, errFakeUnavailable)

	removal := fx.remove(t)
	if !removal.Removed {
		t.Fatalf("removal after transient failures = %+v, want success", removal)
	}
	sleeps := fx.clock.sleepDurations()
	if len(sleeps) != 2 {
		t.Fatalf("backoff sleeps = %v, want exactly 2", sleeps)
	}
	for _, duration := range sleeps {
		if duration != time.Second {
			t.Fatalf("backoff = %v, want 1s", duration)
		}
	}
	if calls := fx.libpod.callsFor("RemoveContainer"); calls != 3 {
		t.Fatalf("RemoveContainer calls = %d, want 3 bounded attempts", calls)
	}
}

func TestRemoveExhaustionLeavesVisiblePendingWork(t *testing.T) {
	fx := newFixture(t)
	adapter, err := New(Config{
		Libpod:    fx.libpod,
		Files:     fx.files,
		Root:      fx.root,
		KodemaURL: testKodemaURL,
		Cleanup:   CleanupPolicy{Attempts: 2, Backoff: time.Second},
		Clock:     fx.clock,
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	fx.adapter = adapter
	fx.prepare(t)
	fx.seedExited(t, 0, true)
	fx.libpod.failRemoveTimes(2, errFakeUnavailable)

	removal, err := fx.adapter.Remove(context.Background(), removeRequest(fx))
	if err != nil {
		t.Fatalf("Remove returned error on exhaustion: %v", err)
	}
	if removal.Removed {
		t.Fatal("Remove reported success after exhausting cleanup attempts")
	}
	if !strings.Contains(removal.Detail, fx.containerName) {
		t.Fatalf("exhaustion detail %q does not name the retained container %q", removal.Detail, fx.containerName)
	}
	if !hostFileExists(t, fx.workloadRoot()) {
		t.Fatal("exhausted cleanup deleted the materialization it must retain")
	}
	if sleeps := fx.clock.sleepDurations(); len(sleeps) != 1 {
		t.Fatalf("backoff sleeps = %v, want 1", sleeps)
	}
	if calls := fx.libpod.callsFor("RemoveContainer"); calls != 2 {
		t.Fatalf("RemoveContainer calls = %d, want the bounded 2", calls)
	}

	fx.libpod.failRemoveTimes(0, nil)
	if retry := fx.remove(t); !retry.Removed {
		t.Fatalf("retry after exhaustion = %+v, want success", retry)
	}
}

func TestRemoveCancellationDuringBackoff(t *testing.T) {
	fx := newFixture(t)
	adapter, err := New(Config{
		Libpod:    fx.libpod,
		Files:     fx.files,
		Root:      fx.root,
		KodemaURL: testKodemaURL,
		Cleanup:   CleanupPolicy{Attempts: 3, Backoff: time.Second},
		Clock:     fx.clock,
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	fx.adapter = adapter
	fx.prepare(t)
	fx.seedExited(t, 0, true)
	fx.libpod.failRemoveTimes(3, errFakeUnavailable)

	gate := fx.clock.newGate()
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		removal reconcile.Removal
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		removal, err := fx.adapter.Remove(ctx, removeRequest(fx))
		done <- outcome{removal: removal, err: err}
	}()
	waitFor(t, gate.entered, "the injected backoff")
	cancel()

	select {
	case got := <-done:
		if got.err == nil {
			t.Fatal("cancelled Remove returned no error")
		}
		if got.removal.Removed {
			t.Fatal("cancelled Remove reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Remove did not return after cancellation")
	}
	if sleeps := fx.clock.sleepDurations(); len(sleeps) != 1 {
		t.Fatalf("backoff sleeps = %v, want cancellation during the first backoff", sleeps)
	}
	if calls := fx.libpod.callsFor("RemoveContainer"); calls != 1 {
		t.Fatalf("RemoveContainer calls = %d, want no attempt after cancellation", calls)
	}
	if !hostFileExists(t, fx.workloadRoot()) {
		t.Fatal("cancelled Remove deleted the materialization")
	}
}

func TestRemoveRefusesForeignContainerWithSameName(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	state := fx.seedExited(t, 0, true)
	state.Labels[LabelExecution] = "exec-other"
	fx.libpod.setContainer(state)

	removal := fx.remove(t)
	if removal.Removed {
		t.Fatalf("Remove deleted a container owned by another identity: %+v", removal)
	}
	if _, ok := fx.libpod.container(fx.containerName); !ok {
		t.Fatal("Remove deleted a container owned by another identity")
	}
	if !hostFileExists(t, fx.workloadRoot()) {
		t.Fatal("Remove deleted the materialization of a foreign container identity")
	}
}

func TestRemoveIssuesNoContainerRemovalForAnAbsentContainer(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)

	removal := fx.remove(t)
	if !removal.Removed {
		t.Fatalf("removal of an absent container = %+v, want success", removal)
	}
	if calls := fx.libpod.callsFor("RemoveContainer"); calls != 0 {
		t.Fatalf("Remove issued %d RemoveContainer call(s) for an absent container", calls)
	}
}

func TestRefreshCredentialReplacesTokenWithoutRestart(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedRunning(t)

	createCalls := fx.libpod.callsFor("CreateContainer")
	startCalls := fx.libpod.callsFor("StartContainer")
	stopCalls := fx.libpod.callsFor("StopContainer")
	removeCalls := fx.libpod.callsFor("RemoveContainer")
	directWrites := fx.files.countPath("WriteFile", fx.credentialPath())
	renames := fx.files.countPath("Rename", fx.credentialPath())

	refresh := fx.refresh(t, "tok-new")
	if !refresh.Delivered {
		t.Fatalf("refresh = %+v, want delivered", refresh)
	}
	if refresh.Detail == "" {
		t.Fatal("refresh has no detail")
	}
	if got := readHostFile(t, fx.credentialPath()); string(got) != "tok-new" {
		t.Fatalf("credential bytes = %q, want tok-new", got)
	}
	if mode := hostFileMode(t, fx.credentialPath()); mode != 0o444 {
		t.Fatalf("credential mode = %o, want 0444", mode)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != createCalls {
		t.Fatalf("refresh created a container: %d -> %d", createCalls, calls)
	}
	if calls := fx.libpod.callsFor("StartContainer"); calls != startCalls {
		t.Fatalf("refresh restarted the container: %d -> %d", startCalls, calls)
	}
	if calls := fx.libpod.callsFor("StopContainer"); calls != stopCalls {
		t.Fatalf("refresh stopped the container: %d -> %d", stopCalls, calls)
	}
	if calls := fx.libpod.callsFor("RemoveContainer"); calls != removeCalls {
		t.Fatalf("refresh removed the container: %d -> %d", removeCalls, calls)
	}
	if got := fx.files.countPath("WriteFile", fx.credentialPath()); got != directWrites {
		t.Fatalf("refresh wrote the token in place (%d direct writes), want an atomic rename", got)
	}
	if got := fx.files.countPath("Rename", fx.credentialPath()); got != renames+1 {
		t.Fatalf("refresh renames = %d, want one atomic replacement", got-renames)
	}
	if !hostFileExists(t, fx.secretPath("API_KEY")) {
		t.Fatal("refresh disturbed the declared secret mounts")
	}

	// RefreshCredential also works after controller restart without prepared.json.
	reconstructed := newAdapterWithRoot(t, fx)
	afterRestart, err := reconstructed.RefreshCredential(context.Background(), reconcile.CredentialRequest{
		Identity:   fx.request.Identity,
		Credential: "tok-after-restart",
	})
	if err != nil || !afterRestart.Delivered {
		t.Fatalf("reconstructed RefreshCredential = (%+v, %v); want delivered", afterRestart, err)
	}
	if got := readHostFile(t, fx.credentialPath()); string(got) != "tok-after-restart" {
		t.Fatalf("credential after restart = %q, want tok-after-restart", got)
	}
}

func TestRefreshCredentialRecoversLostResponseAndAtomicity(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedRunning(t)

	fx.files.injectBefore("Rename", "KODEMA_TOKEN", errFakeLost)
	refresh, err := fx.adapter.RefreshCredential(context.Background(), reconcile.CredentialRequest{
		Identity:   fx.request.Identity,
		Credential: "tok-failed",
	})
	if err == nil {
		t.Fatal("refresh hid a failed atomic replacement")
	}
	if refresh.Delivered {
		t.Fatal("a failed replacement reported delivery")
	}
	if got := readHostFile(t, fx.credentialPath()); string(got) != "tok-abc" {
		t.Fatalf("failed replacement corrupted the previous token: %q", got)
	}

	fx.files.clear("Rename", "KODEMA_TOKEN")
	refresh = fx.refresh(t, "tok-b")
	if !refresh.Delivered {
		t.Fatalf("retry refresh = %+v, want delivered", refresh)
	}
	if got := readHostFile(t, fx.credentialPath()); string(got) != "tok-b" {
		t.Fatalf("credential bytes = %q, want tok-b", got)
	}

	fx.files.injectAfter("Rename", "KODEMA_TOKEN", errFakeLost)
	refresh, err = fx.adapter.RefreshCredential(context.Background(), reconcile.CredentialRequest{
		Identity:   fx.request.Identity,
		Credential: "tok-c",
	})
	if err == nil {
		t.Fatal("refresh hid a lost replacement response")
	}
	if refresh.Delivered {
		t.Fatal("a lost replacement response reported delivery")
	}
	if got := readHostFile(t, fx.credentialPath()); string(got) != "tok-c" {
		t.Fatalf("lost-response token = %q, want tok-c", got)
	}
	fx.files.clear("Rename", "KODEMA_TOKEN")
	refresh = fx.refresh(t, "tok-c")
	if !refresh.Delivered {
		t.Fatalf("recovered refresh = %+v, want delivered", refresh)
	}
	if calls := fx.libpod.callsFor("StartContainer"); calls != 0 {
		t.Fatalf("credential recovery restarted the container %d times", calls)
	}

	if _, err := fx.adapter.RefreshCredential(context.Background(), reconcile.CredentialRequest{
		Identity:   fx.request.Identity,
		Credential: "  ",
	}); err == nil {
		t.Fatal("refresh accepted an empty credential")
	} else {
		assertFaultClass(t, err, fault.InvalidInput)
	}
}

func TestRefreshRefusedAfterRemoval(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedExited(t, 0, true)
	if removal := fx.remove(t); !removal.Removed {
		t.Fatalf("removal = %+v, want success", removal)
	}
	writesBefore := fx.files.callCount("WriteFile")
	renamesBefore := fx.files.callCount("Rename")

	refresh, err := fx.adapter.RefreshCredential(context.Background(), reconcile.CredentialRequest{
		Identity:   fx.request.Identity,
		Credential: "tok-after-removal",
	})
	assertFaultClass(t, err, fault.NotFound)
	if refresh.Delivered {
		t.Fatal("refresh delivered a credential for a removed identity")
	}
	if writes := fx.files.callCount("WriteFile"); writes != writesBefore {
		t.Fatalf("refresh wrote %d files for a removed identity", writes-writesBefore)
	}
	if renames := fx.files.callCount("Rename"); renames != renamesBefore {
		t.Fatalf("refresh renamed %d files for a removed identity", renames-renamesBefore)
	}
}

func TestSecretValuesNeverAppearInDiagnostics(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)

	forbidden := []string{"s3cr3t", "tok-abc"}
	assertClean := func(where string, values ...string) {
		t.Helper()
		for _, value := range values {
			for _, secret := range forbidden {
				if strings.Contains(value, secret) {
					t.Fatalf("%s leaked %q: %q", where, secret, value)
				}
			}
		}
	}

	fx.libpod.failNext("CreateContainer", errFakeUnavailable, false)
	result, err := fx.startResult(t)
	if err == nil {
		t.Fatal("Start hid an unavailable daemon")
	}
	assertClean("start error", err.Error(), result.Detail, fmt.Sprintf("%+v", result))

	fx.files.injectBefore("Rename", "KODEMA_TOKEN", errFakeUnavailable)
	refresh, err := fx.adapter.RefreshCredential(context.Background(), reconcile.CredentialRequest{
		Identity:   fx.request.Identity,
		Credential: "tok-abc",
	})
	if err == nil {
		t.Fatal("refresh hid a failed replacement")
	}
	assertClean("refresh error", err.Error(), refresh.Detail)
}

func TestRemoveNeverCallsAnExternalVolumeRemoval(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.seedExited(t, 0, true)
	if removal := fx.remove(t); !removal.Removed {
		t.Fatalf("removal = %+v, want success", removal)
	}
	if removed := fx.libpod.removedVolumes(); len(removed) != 0 {
		t.Fatalf("Remove issued volume removals %v, want none", removed)
	}
	volume, ok := fx.libpod.volume(fx.volumeName)
	if !ok {
		t.Fatal("the retained workspace volume disappeared")
	}
	if volume.Name != fx.volumeName {
		t.Fatalf("workspace volume name = %q, want %q", volume.Name, fx.volumeName)
	}
}

func TestObserveSurfacesOOMKilledAndMemoryLimit(t *testing.T) {
	t.Run("uses inspected HostConfig memory limit", func(t *testing.T) {
		fx := newFixture(t)
		fx.prepare(t)
		state := fx.seedExited(t, 137, true)
		state.OOMKilled = true
		state.MemoryLimitBytes = 512 << 20
		fx.libpod.setContainer(state)

		obs := fx.observe(t)
		if obs.Kind != engine.ObservationExited || !obs.OOMKilled || !obs.StopConfirmed() {
			t.Fatalf("observation = %+v; want exited OOMKilled with confirmed stop", obs)
		}
		if code, ok := obs.ExitStatus(); !ok || code != 137 {
			t.Fatalf("exit status = %d (%v); want 137", code, ok)
		}
		if obs.MemoryLimit != "512 MiB" {
			t.Fatalf("memory limit = %q; want 512 MiB", obs.MemoryLimit)
		}
		if !strings.Contains(obs.Stop.Detail, "OOMKilled") {
			t.Fatalf("stop detail = %q; want OOMKilled", obs.Stop.Detail)
		}
	})

	t.Run("falls back to prepared memory limit when daemon omits HostConfig.Memory", func(t *testing.T) {
		fx := newFixture(t)
		fx.request.Container.Resources = &definition.Resources{Memory: "3Gi"}
		fx.prepare(t)
		state := fx.seedExited(t, 137, true)
		state.OOMKilled = true
		state.MemoryLimitBytes = 0
		fx.libpod.setContainer(state)

		obs := fx.observe(t)
		if !obs.OOMKilled || obs.MemoryLimit != "3 GiB" {
			t.Fatalf("observation = %+v; want OOMKilled with 3 GiB fallback", obs)
		}
	})
}

func TestPrepareAppliesBoundedResourcesAndEnforcesInstallationMax(t *testing.T) {
	t.Run("applies per-action resources within installation bounds", func(t *testing.T) {
		fx := newFixture(t)
		fx.request.Container.Resources = &definition.Resources{Memory: "3Gi", CPU: "1500m"}
		fx.prepare(t)
		fx.start(t)

		specs := fx.libpod.createSpecs()
		if len(specs) != 1 {
			t.Fatalf("createSpecs = %d; want 1", len(specs))
		}
		got := specs[0].Resources
		if got.MemoryBytes != 3<<30 {
			t.Fatalf("MemoryBytes = %d; want %d", got.MemoryBytes, int64(3<<30))
		}
		if got.CPUQuota != 150000 || got.CPUPeriod != DefaultCPUPeriod {
			t.Fatalf("CPU = quota %d period %d; want 150000/%d", got.CPUQuota, got.CPUPeriod, DefaultCPUPeriod)
		}
	})

	t.Run("refuses memory and cpu above installation maximum before any effect", func(t *testing.T) {
		fx := newFixture(t)
		fx.request.Container.Resources = &definition.Resources{Memory: "8Gi"}
		_, err := fx.adapter.Prepare(context.Background(), fx.request)
		assertFaultClass(t, err, fault.InvalidInput)
		if !strings.Contains(err.Error(), "exceeds installation maximum") {
			t.Fatalf("error = %v; want installation maximum diagnostic", err)
		}
		if hostFileExists(t, fx.workloadRoot()) {
			t.Fatal("refused memory request created workload root")
		}

		fx2 := newFixture(t)
		fx2.request.Container.Resources = &definition.Resources{CPU: "8"}
		_, err = fx2.adapter.Prepare(context.Background(), fx2.request)
		assertFaultClass(t, err, fault.InvalidInput)
		if !strings.Contains(err.Error(), "exceeds installation maximum") {
			t.Fatalf("error = %v; want installation maximum diagnostic", err)
		}
	})

	t.Run("rejects invalid resource policy at adapter construction", func(t *testing.T) {
		fx := newFixture(t)
		_, err := New(Config{
			Libpod:    fx.libpod,
			Files:     fx.files,
			Root:      fx.root,
			KodemaURL: testKodemaURL,
			Cleanup:   CleanupPolicy{Attempts: 1},
			Resources: ResourcePolicy{DefaultMemoryBytes: 4 << 30, MaxMemoryBytes: 2 << 30},
		})
		assertFaultClass(t, err, fault.InvalidInput)
	})
}
