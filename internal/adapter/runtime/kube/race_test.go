// Kubernetes-specific barrier cancellation schedules: CreateJob and SuspendJob
// gates.
//
// Shared cancellation and concurrency invariants (pre-effect cancelled Start,
// concurrent duplicate Starts, concurrent RefreshCredential + Observe) are
// verified in TestKubeConformance.
package kube

import (
	"context"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
)

func TestStartCancellationAtBarrierStaysUnknown(t *testing.T) {
	fake, _, adapter := testSetup(t)
	request := testPrepareRequest()
	prepareTestWorkload(t, adapter, request)

	gate := fake.newGate("CreateJob")
	ctx, cancel := context.WithCancel(context.Background())

	type outcome struct {
		result reconcile.StartResult
		err    error
	}
	results := make(chan outcome, 1)
	go func() {
		result, err := adapter.Start(ctx, reconcile.StartRequest{
			Identity:    request.Identity,
			WorkspaceID: request.WorkspaceID,
		})
		results <- outcome{result: result, err: err}
	}()

	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Start never reached the CreateJob barrier")
	}
	cancel()
	close(gate.release)

	got := <-results
	if got.err == nil {
		t.Fatal("cancelled Start hid the cancellation")
	}
	if got.result.Outcome != reconcile.StartUnknown {
		t.Fatalf("cancelled-at-barrier outcome = %q; want %q", got.result.Outcome, reconcile.StartUnknown)
	}
}

func TestStopCancellationStaysUnconfirmed(t *testing.T) {
	fake, _, adapter, request := setupStarted(t)
	gate := fake.newGate("SuspendJob")
	ctx, cancel := context.WithCancel(context.Background())

	type outcome struct {
		evidence engine.StopEvidence
		err      error
	}
	results := make(chan outcome, 1)
	go func() {
		evidence, err := adapter.Stop(ctx, reconcile.StopRequest{
			Identity:    request.Identity,
			WorkspaceID: request.WorkspaceID,
		})
		results <- outcome{evidence: evidence, err: err}
	}()

	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop never reached the SuspendJob barrier")
	}
	cancel()
	close(gate.release)

	got := <-results
	if got.err == nil {
		t.Fatal("cancelled Stop hid the cancellation")
	}
	if got.evidence.IsConfirmed() {
		t.Fatal("cancelled Stop reported a confirmed stop")
	}
}
