// Podman-specific race and barrier schedules: cancellation at the libpod
// CreateContainer barrier, start-vs-remove fence serialization, and stop during
// an in-flight start.
//
// Shared cancellation and concurrency invariants (pre-effect cancelled Start,
// concurrent duplicate Starts, concurrent RefreshCredential) are verified in
// TestPodmanConformance and internal/adapter/runtime/container/lifecycle_test.go.
package podman

import (
	"context"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
)

func TestStartCancelledAtCreateBarrierStaysUnknown(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	gate := fx.libpod.newGate("CreateContainer")

	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		result reconcile.StartResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := fx.adapter.Start(ctx, startRequest(fx))
		done <- outcome{result: result, err: err}
	}()
	waitFor(t, gate.entered, "CreateContainer at the barrier")
	cancel()

	select {
	case got := <-done:
		if got.err == nil {
			t.Fatal("cancelled Start returned no error")
		}
		if got.result.Outcome != reconcile.StartUnknown {
			t.Fatalf("outcome = %q, want unknown", got.result.Outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
	if count := fx.libpod.containerCount(); count != 0 {
		t.Fatalf("cancelled create left %d containers", count)
	}

	fx.libpod.clearGate("CreateContainer")
	retry := fx.start(t)
	if retry.Outcome != reconcile.StartAccepted {
		t.Fatalf("retry outcome = %q, want accepted", retry.Outcome)
	}
	if count := fx.libpod.containerCount(); count != 1 {
		t.Fatalf("containers after retry = %d, want 1", count)
	}
	if calls := fx.libpod.callsFor("CreateContainer"); calls != 2 {
		t.Fatalf("CreateContainer calls = %d, want one cancelled and one successful", calls)
	}
}

func TestStartAndRemoveDoNotCrossTheFence(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	gate := fx.libpod.newGate("CreateContainer")

	type startOutcome struct {
		result reconcile.StartResult
		err    error
	}
	started := make(chan startOutcome, 1)
	go func() {
		result, err := fx.adapter.Start(context.Background(), startRequest(fx))
		started <- startOutcome{result: result, err: err}
	}()
	waitFor(t, gate.entered, "CreateContainer at the barrier")

	type removeOutcome struct {
		removal reconcile.Removal
		err     error
	}
	removed := make(chan removeOutcome, 1)
	go func() {
		removal, err := fx.adapter.Remove(context.Background(), removeRequest(fx))
		removed <- removeOutcome{removal: removal, err: err}
	}()

	close(gate.release)

	var startResult startOutcome
	select {
	case startResult = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
	var removeResult removeOutcome
	select {
	case removeResult = <-removed:
	case <-time.After(5 * time.Second):
		t.Fatal("Remove did not return")
	}
	if startResult.err != nil {
		t.Fatalf("Start returned error: %v", startResult.err)
	}
	if startResult.result.Outcome != reconcile.StartAccepted {
		t.Fatalf("start outcome = %q, want accepted", startResult.result.Outcome)
	}

	if removeResult.err != nil {
		t.Fatalf("Remove returned error: %v", removeResult.err)
	}
	if removeResult.removal.Removed {
		if count := fx.libpod.containerCount(); count != 0 {
			t.Fatalf("Remove reported success but left %d orphan containers", count)
		}
		if hostFileExists(t, fx.workloadRoot()) {
			t.Fatal("Remove reported success but left the materialization behind")
		}
		if _, err := fx.adapter.Start(context.Background(), startRequest(fx)); err == nil {
			t.Fatal("Start crossed the durable removal fence")
		}
		return
	}

	if _, ok := fx.libpod.container(fx.containerName); !ok {
		t.Fatal("Remove reported a retained removal but the container disappeared")
	}
	if !hostFileExists(t, fx.workloadRoot()) {
		t.Fatal("Remove retained the container but deleted its materialization")
	}
}

func TestStopDuringStartNeverConfirms(t *testing.T) {
	fx := newFixture(t)
	fx.prepare(t)
	fx.libpod.setStopEffect(false)
	gate := fx.libpod.newGate("CreateContainer")

	type startOutcome struct {
		result reconcile.StartResult
		err    error
	}
	started := make(chan startOutcome, 1)
	go func() {
		result, err := fx.adapter.Start(context.Background(), startRequest(fx))
		started <- startOutcome{result: result, err: err}
	}()
	waitFor(t, gate.entered, "CreateContainer at the barrier")

	type stopOutcome struct {
		evidence engine.StopEvidence
		err      error
	}
	stopped := make(chan stopOutcome, 1)
	go func() {
		evidence, err := fx.adapter.Stop(context.Background(), stopRequest(fx))
		stopped <- stopOutcome{evidence: evidence, err: err}
	}()

	close(gate.release)

	select {
	case got := <-started:
		if got.err != nil || got.result.Outcome != reconcile.StartAccepted {
			t.Fatalf("Start = (%+v, %v), want accepted", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return")
	}
	select {
	case got := <-stopped:
		if got.err != nil {
			t.Fatalf("Stop returned error: %v", got.err)
		}
		if got.evidence.IsConfirmed() {
			t.Fatal("a stop racing with a start confirmed the stop from an acknowledgment")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
}
