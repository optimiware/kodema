// Cancellation and concurrency schedules: explicit barriers, no wall-clock
// sleeps and exactly one stable process group under concurrent retries.
package process

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
)

// TestStartCancelledBeforeEffectIsUnknownWithoutSpawn proves a cancelled
// context never reaches the executor and never becomes an accepted start.
func TestStartCancelledBeforeEffectIsUnknownWithoutSpawn(t *testing.T) {
	fx := newFixture(t)
	prepareTestWorkload(t, fx.adapter, fx.request)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := fx.adapter.Start(ctx, reconcile.StartRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	})
	if err == nil {
		t.Fatal("Start succeeded with a cancelled context")
	}
	if result.Outcome != reconcile.StartUnknown {
		t.Fatalf("cancelled start outcome = %q; want %q", result.Outcome, reconcile.StartUnknown)
	}
	if result.Outcome == reconcile.StartProvenNotStarted {
		t.Fatal("a cancelled start collapsed into proven-not-started")
	}
	if fx.executor.spawnRequestCount() != 0 {
		t.Fatal("cancelled Start reached the executor")
	}
	if fx.executor.spawnCount() != 0 {
		t.Fatal("cancelled Start created a process")
	}
}

// TestStartCancellationAtBarrierStaysUnknown proves cancelling a request that
// may already have reached the executor stays unknown.
func TestStartCancellationAtBarrierStaysUnknown(t *testing.T) {
	fx := newFixture(t)
	prepareTestWorkload(t, fx.adapter, fx.request)
	gate := fx.executor.newGate("Spawn")

	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		result reconcile.StartResult
		err    error
	}
	results := make(chan outcome, 1)
	go func() {
		result, err := fx.adapter.Start(ctx, reconcile.StartRequest{
			Identity:    fx.request.Identity,
			WorkspaceID: fx.request.WorkspaceID,
		})
		results <- outcome{result: result, err: err}
	}()

	waitFor(t, gate.entered, "the spawn barrier")
	cancel()
	close(gate.release)

	got := <-results
	if got.err == nil {
		t.Fatal("cancelled Start hid the cancellation")
	}
	if got.result.Outcome != reconcile.StartUnknown {
		t.Fatalf("cancelled-at-barrier outcome = %q; want %q", got.result.Outcome, reconcile.StartUnknown)
	}
	if got.result.Outcome == reconcile.StartProvenNotStarted {
		t.Fatal("cancelled-at-barrier outcome collapsed into proven-not-started")
	}
	if fx.executor.spawnCount() != 0 {
		t.Fatal("cancelled-at-barrier start created a process")
	}
}

// TestStartConcurrentSameIdentitySpawnsOnce proves concurrent retries of the
// same persisted identity converge on the single stable process group.
func TestStartConcurrentSameIdentitySpawnsOnce(t *testing.T) {
	fx := newFixture(t)
	prepareTestWorkload(t, fx.adapter, fx.request)
	gate := fx.executor.newGate("Spawn")

	const workers = 4
	start := make(chan struct{})
	results := make([]reconcile.StartResult, workers)
	errs := make([]error, workers)
	var wait sync.WaitGroup
	for index := range workers {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			results[index], errs[index] = fx.adapter.Start(context.Background(), reconcile.StartRequest{
				Identity:    fx.request.Identity,
				WorkspaceID: fx.request.WorkspaceID,
			})
		}(index)
	}
	close(start)
	waitFor(t, gate.entered, "the first spawn barrier")
	close(gate.release)
	wait.Wait()

	for index := range workers {
		if errs[index] != nil {
			t.Fatalf("worker %d Start returned error: %v", index, errs[index])
		}
		if results[index].Outcome != reconcile.StartAccepted {
			t.Fatalf("worker %d outcome = %q; want accepted", index, results[index].Outcome)
		}
	}
	if fx.executor.spawnCount() != 1 {
		t.Fatalf("concurrent starts created %d processes; want exactly 1", fx.executor.spawnCount())
	}
	if fx.executor.spawnRequestCount() != 1 {
		t.Fatalf("concurrent starts made %d spawn requests; want exactly 1", fx.executor.spawnRequestCount())
	}
}

// TestStopCancellationStaysUnconfirmed proves a cancelled stop request never
// fabricates a stop barrier.
func TestStopCancellationStaysUnconfirmed(t *testing.T) {
	fx := newFixture(t)
	prepareTestWorkload(t, fx.adapter, fx.request)
	startTestWorkload(t, fx.adapter, fx.request.Identity, fx.request.WorkspaceID)
	fx.observer.markRunning(fx.groupID(t))
	gate := fx.executor.newGate("SignalGroup")

	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		evidence engine.StopEvidence
		err      error
	}
	results := make(chan outcome, 1)
	go func() {
		evidence, err := fx.adapter.Stop(ctx, reconcile.StopRequest{
			Identity:    fx.request.Identity,
			WorkspaceID: fx.request.WorkspaceID,
		})
		results <- outcome{evidence: evidence, err: err}
	}()

	waitFor(t, gate.entered, "the signal barrier")
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

// TestConcurrentRefreshAndObserve proves credential refresh and observation are
// safe to interleave and never restart the command.
func TestConcurrentRefreshAndObserve(t *testing.T) {
	fx := newFixture(t)
	prepareTestWorkload(t, fx.adapter, fx.request)
	startTestWorkload(t, fx.adapter, fx.request.Identity, fx.request.WorkspaceID)
	fx.observer.markRunning(fx.groupID(t))

	const workers = 4
	start := make(chan struct{})
	errs := make(chan error, workers*2)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			refresh, err := fx.adapter.RefreshCredential(context.Background(), reconcile.CredentialRequest{
				Identity:   fx.request.Identity,
				Credential: "tok-rotated",
			})
			if err == nil && !refresh.Delivered {
				err = errNotDelivered
			}
			errs <- err
		}()
		go func() {
			defer wait.Done()
			<-start
			observation, err := fx.adapter.Observe(context.Background(), reconcile.ObserveRequest{
				Identity:    fx.request.Identity,
				WorkspaceID: fx.request.WorkspaceID,
			})
			if err == nil && observation.Kind != engine.ObservationRunning {
				err = errNotRunning
			}
			errs <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent operation failed: %v", err)
		}
	}

	if fx.executor.spawnCount() != 1 || fx.executor.spawnRequestCount() != 1 {
		t.Fatalf("concurrent operations replayed the start: %d spawns/%d requests",
			fx.executor.spawnCount(), fx.executor.spawnRequestCount())
	}
}

var (
	errNotDelivered = errors.New("refresh was not delivered")
	errNotRunning   = errors.New("observation was not running")
)
