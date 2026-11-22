//go:build component

package runtimeconformance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	runtimesupport "github.com/optimiware/kodema/test/support/infrastructure/runtime"
)

// TestPodmanRuntimeWorkspacePersistenceAndCredentialRefresh proves a retained
// workspace volume survives container recreation and that a credential refresh
// atomically updates <root>/<workload>/secrets/KODEMA_TOKEN and is visible to
// the running workload without a restart.
func TestPodmanRuntimeWorkspacePersistenceAndCredentialRefresh(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-2")

	h := fixture.harness(t)
	h.BeforeRefresh = func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity) func(testing.TB, context.Context) {
		t.Helper()
		name, err := podman.ContainerName(identity.WorkloadID)
		if err != nil {
			t.Fatalf("ContainerName: %v", err)
		}
		before, err := fixture.libpod.InspectContainer(ctx, name)
		if err != nil {
			t.Fatalf("inspect before refresh: %v", err)
		}
		return func(t testing.TB, ctx context.Context) {
			t.Helper()
			assertPodmanSecretConfinement(t, fixture.root, identity.WorkloadID, runtimesupport.RefreshedCredential, nil)
			after, err := fixture.libpod.InspectContainer(ctx, name)
			if err != nil {
				t.Fatalf("inspect after refresh: %v", err)
			}
			if before.ID != after.ID || after.Status != podman.ContainerStatusRunning {
				t.Fatalf("credential refresh restarted the container: before %+v after %+v", before, after)
			}
		}
	}

	runtimesupport.AssertLiveCredentialRefresh(t, ctx, h)
	if !runtimesupport.VolumeExists(ctx, fixture.volumeName) {
		t.Fatal("workspace volume disappeared after live credential refresh")
	}

	runtimesupport.AssertWorkspacePersistence(t, ctx, h)
	if runtimesupport.VolumeExists(ctx, fixture.volumeName) {
		t.Fatal("workspace volume still exists after Release")
	}

	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "workspace-persistence-and-credential-refresh",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}

// TestPodmanRuntimeNoReplayAcrossControllerRestart proves a lost start response
// is StartUnknown, a reconstructed controller adopts the existing running or
// exited container via libpod without replaying, and an absent container
// reports ObservationAbsent and refuses Start without preparation.
func TestPodmanRuntimeNoReplayAcrossControllerRestart(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-3")
	identity := testIdentity("exec-c3", "action-c3", "workload-c3", "idem-c3")
	wrapper := &lostStartLibpod{Libpod: fixture.libpod, lose: true}
	_, _, adapter := newPodmanAdapter(t, fixture, wrapper)
	request := fixture.request(identity, []string{
		`echo launched >> /workspace/launch-count`,
	}, nil, nil, "tok-abc")
	if _, err := adapter.Prepare(ctx, request); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	start, err := adapter.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err == nil || start.Outcome != reconcile.StartUnknown {
		t.Fatalf("lost start = %q/%v; want unknown", start.Outcome, err)
	}
	if wrapper.startCalls() != 1 {
		t.Fatalf("start calls = %d; want 1", wrapper.startCalls())
	}

	recovered := newPodmanAdapterOnly(t, fixture)
	recoveredStart, err := recovered.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err != nil {
		t.Fatalf("recovered Start: %v", err)
	}
	if recoveredStart.Outcome != reconcile.StartAccepted {
		t.Fatalf("recovered start outcome = %q; want accepted", recoveredStart.Outcome)
	}
	if wrapper.startCalls() != 1 {
		t.Fatalf("recovered start issued another start: %d calls", wrapper.startCalls())
	}
	if names := podmanContainerNames(t, ctx, fixture.workspaceID); len(names) != 1 {
		t.Fatalf("workspace containers = %v; want exactly one", names)
	}
	if got := strings.Count(string(readVolume(t, ctx, fixture, "launch-count")), "launched"); got != 1 {
		t.Fatalf("workload launched %d times; want 1", got)
	}
	waitObservation(t, ctx, recovered,
		reconcile.ObserveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}, engine.ObservationExited)
	recoveredStart, err = recovered.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err != nil || recoveredStart.Outcome != reconcile.StartAccepted {
		t.Fatalf("start after exit = %q/%v; want accepted", recoveredStart.Outcome, err)
	}
	if got := strings.Count(string(readVolume(t, ctx, fixture, "launch-count")), "launched"); got != 1 {
		t.Fatalf("workload relaunched after exit: %d", got)
	}

	// Under S07 §3, an externally removed container has no host disk state
	// machine: Observe reports ObservationAbsent (never NeverStarted), and a
	// reconstructed controller refuses Start without preparation rather than
	// recreating the container implicitly.
	goneIdentity := testIdentity("exec-c3b", "action-c3b", "workload-c3b", "idem-c3b")
	goneWrapper := &lostStartLibpod{Libpod: fixture.libpod, lose: true}
	_, _, goneAdapter := newPodmanAdapter(t, fixture, goneWrapper)
	goneRequest := fixture.request(goneIdentity, []string{`echo gone >> /workspace/gone`}, nil, nil, "tok-abc")
	if _, err := goneAdapter.Prepare(ctx, goneRequest); err != nil {
		t.Fatalf("gone Prepare: %v", err)
	}
	if start, err := goneAdapter.Start(ctx, reconcile.StartRequest{Identity: goneIdentity, WorkspaceID: fixture.workspaceID}); err == nil || start.Outcome != reconcile.StartUnknown {
		t.Fatalf("gone lost start = %q/%v; want unknown", start.Outcome, err)
	}
	goneName, err := podman.ContainerName(goneIdentity.WorkloadID)
	if err != nil {
		t.Fatalf("gone ContainerName: %v", err)
	}
	if err := fixture.libpod.RemoveContainer(ctx, goneName); err != nil && !errors.Is(err, podman.ErrNotFound) {
		t.Fatalf("remove container: %v", err)
	}

	recoveredGone := newPodmanAdapterOnly(t, fixture)
	absentObs, err := recoveredGone.Observe(ctx, reconcile.ObserveRequest{Identity: goneIdentity, WorkspaceID: fixture.workspaceID})
	if err != nil {
		t.Fatalf("Observe absent container: %v", err)
	}
	if absentObs.Kind != engine.ObservationAbsent || absentObs.ProofOfNonexecution() {
		t.Fatalf("absent container observation = %+v; want ObservationAbsent", absentObs)
	}
	retry, err := recoveredGone.Start(ctx, reconcile.StartRequest{Identity: goneIdentity, WorkspaceID: fixture.workspaceID})
	if retry.Outcome != reconcile.StartUnknown {
		t.Fatalf("reconstructed start of absent container = %q/%v; want unknown", retry.Outcome, err)
	}
	runtimesupport.RequireFaultClass(t, err, fault.InvalidInput)
	if _, err := recoveredGone.Remove(ctx, reconcile.RemoveRequest{Identity: goneIdentity, WorkspaceID: fixture.workspaceID}); err != nil {
		t.Fatalf("gone Remove: %v", err)
	}
	observation, err := recoveredGone.Observe(ctx, reconcile.ObserveRequest{Identity: goneIdentity, WorkspaceID: fixture.workspaceID})
	if err != nil {
		t.Fatalf("gone Observe after Remove: %v", err)
	}
	if observation.Kind != engine.ObservationAbsent {
		t.Fatalf("removed observation = %+v; want absent", observation)
	}

	if evidence := runtimesupport.WaitForStopConfirmed(t, ctx, recovered, reconcile.StopRequest{Identity: identity, WorkspaceID: fixture.workspaceID}, 0, 0); !evidence.IsConfirmed() {
		t.Fatalf("stop evidence = %+v; want confirmed", evidence)
	}
	if removal, err := recovered.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}); err != nil || !removal.Removed {
		t.Fatalf("Remove = %+v, %v; want removed", removal, err)
	}
	if err := recovered.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "no-replay-across-controller-restart",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}

// TestPodmanRuntimeUncertainStartAndStop interrupts the real Podman service
// mid-call and asserts the uncertain classification; after recovery a created
// container stays ObservationUnknown and the stop barrier confirms only real
// whole-container termination.
func TestPodmanRuntimeUncertainStartAndStop(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-4")

	startIdentity := testIdentity("exec-c4a", "action-c4a", "workload-c4a", "idem-c4a")
	gated := newGatedLibpod(fixture.libpod)
	_, _, startAdapter := newPodmanAdapter(t, fixture, gated)
	startRequest := fixture.request(startIdentity, []string{`sleep 30`}, nil, nil, "tok-abc")
	if _, err := startAdapter.Prepare(ctx, startRequest); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	type startOutcome struct {
		result reconcile.StartResult
		err    error
	}
	started := make(chan startOutcome, 1)
	go func() {
		result, err := startAdapter.Start(ctx, reconcile.StartRequest{Identity: startIdentity, WorkspaceID: fixture.workspaceID})
		started <- startOutcome{result: result, err: err}
	}()
	<-gated.startEntered
	fixture.service.Kill()
	close(gated.startRelease)
	outcome := <-started
	if outcome.err == nil || outcome.result.Outcome != reconcile.StartUnknown {
		t.Fatalf("interrupted start = %q/%v; want unknown", outcome.result.Outcome, outcome.err)
	}
	fixture.service.Restart(t)
	observation, err := startAdapter.Observe(ctx, reconcile.ObserveRequest{Identity: startIdentity, WorkspaceID: fixture.workspaceID})
	if err != nil {
		t.Fatalf("Observe after restart: %v", err)
	}
	if observation.Kind != engine.ObservationUnknown || observation.ProofOfNonexecution() {
		t.Fatalf("created container after interrupted start = %+v; want ObservationUnknown", observation)
	}
	// A created container has no confirmed stop, so Remove retains it; Stop
	// recovers via non-forced removal on real Podman and confirms termination,
	// after which Remove cleans up host files and leaves zero containers so the
	// workspace can be released cleanly.
	if retry, retryErr := startAdapter.Start(ctx, reconcile.StartRequest{Identity: startIdentity, WorkspaceID: fixture.workspaceID}); retry.Outcome != reconcile.StartUnknown {
		t.Fatalf("retry Start on created container = %+v, %v; want StartUnknown", retry, retryErr)
	} else {
		runtimesupport.RequireFaultClass(t, retryErr, fault.UncertainExternalEffect)
	}
	if retained, err := startAdapter.Remove(ctx, reconcile.RemoveRequest{Identity: startIdentity, WorkspaceID: fixture.workspaceID}); err != nil || retained.Removed {
		t.Fatalf("Remove on created container = %+v, %v; want retained", retained, err)
	}
	if evidence, err := startAdapter.Stop(ctx, reconcile.StopRequest{Identity: startIdentity, WorkspaceID: fixture.workspaceID}); err != nil || !evidence.IsConfirmed() {
		t.Fatalf("Stop on created container = %+v, %v; want confirmed termination", evidence, err)
	}
	if names := podmanContainerNames(t, ctx, fixture.workspaceID); len(names) != 0 {
		t.Fatalf("containers remaining after created-container Stop = %v; want none", names)
	}
	if removal, err := startAdapter.Remove(ctx, reconcile.RemoveRequest{Identity: startIdentity, WorkspaceID: fixture.workspaceID}); err != nil || !removal.Removed {
		t.Fatalf("clean ambiguous workload = %+v, %v; want removed", removal, err)
	}

	stopIdentity := testIdentity("exec-c4b", "action-c4b", "workload-c4b", "idem-c4b")
	stopRequest := fixture.request(stopIdentity, []string{`sleep 30`}, nil, nil, "tok-abc")
	if _, err := fixture.adapter.Prepare(ctx, stopRequest); err != nil {
		t.Fatalf("stop Prepare: %v", err)
	}
	if _, err := fixture.adapter.Start(ctx, reconcile.StartRequest{Identity: stopIdentity, WorkspaceID: fixture.workspaceID}); err != nil {
		t.Fatalf("stop Start: %v", err)
	}
	waitObservation(t, ctx, fixture.adapter,
		reconcile.ObserveRequest{Identity: stopIdentity, WorkspaceID: fixture.workspaceID}, engine.ObservationRunning)

	stopGate := newGatedLibpod(fixture.libpod)
	_, _, stopAdapter := newPodmanAdapter(t, fixture, stopGate)
	type stopOutcome struct {
		evidence engine.StopEvidence
		err      error
	}
	stopped := make(chan stopOutcome, 1)
	go func() {
		evidence, err := stopAdapter.Stop(ctx, reconcile.StopRequest{Identity: stopIdentity, WorkspaceID: fixture.workspaceID})
		stopped <- stopOutcome{evidence: evidence, err: err}
	}()
	<-stopGate.stopEntered
	fixture.service.Kill()
	close(stopGate.stopRelease)
	pending := <-stopped
	runtimesupport.RequireFaultClass(t, pending.err, fault.UncertainExternalEffect)
	if pending.evidence.IsConfirmed() {
		t.Fatalf("interrupted stop confirmed termination: %+v", pending.evidence)
	}
	fixture.service.Restart(t)
	if evidence := runtimesupport.WaitForStopConfirmed(t, ctx, fixture.adapter, reconcile.StopRequest{Identity: stopIdentity, WorkspaceID: fixture.workspaceID}, 0, 0); !evidence.IsConfirmed() {
		t.Fatalf("recovered stop evidence = %+v; want confirmed", evidence)
	}
	stopName, err := podman.ContainerName(stopIdentity.WorkloadID)
	if err != nil {
		t.Fatalf("ContainerName: %v", err)
	}
	state, err := fixture.libpod.InspectContainer(ctx, stopName)
	if err != nil {
		t.Fatalf("inspect after confirmed stop: %v", err)
	}
	if state.Status != podman.ContainerStatusExited || !state.WholeStopped {
		t.Fatalf("confirmed stop state = %+v; want exited with whole-container evidence", state)
	}
	if removal, err := fixture.adapter.Remove(ctx, reconcile.RemoveRequest{Identity: stopIdentity, WorkspaceID: fixture.workspaceID}); err != nil || !removal.Removed {
		t.Fatalf("Remove = %+v, %v; want removed", removal, err)
	}
	if err := fixture.adapter.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "uncertain-start-and-stop",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}

// TestPodmanRuntimeCleanupAndWorkspaceExclusivity proves a workspace is
// released only when no labelled container owns it, removal failures stay
// pending, and two controllers contend for exactly one owner.
func TestPodmanRuntimeCleanupAndWorkspaceExclusivity(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-5")
	identity := testIdentity("exec-c5", "action-c5", "workload-c5", "idem-c5")
	request := fixture.request(identity, []string{`echo done > /workspace/done`}, nil, nil, "tok-abc")
	if _, err := fixture.adapter.Prepare(ctx, request); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := fixture.adapter.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitObservation(t, ctx, fixture.adapter,
		reconcile.ObserveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}, engine.ObservationExited)
	if evidence := runtimesupport.WaitForStopConfirmed(t, ctx, fixture.adapter, reconcile.StopRequest{Identity: identity, WorkspaceID: fixture.workspaceID}, 0, 0); !evidence.IsConfirmed() {
		t.Fatalf("stop evidence = %+v; want confirmed", evidence)
	}
	// A second controller sees the labelled container as the owner and cannot
	// release the workspace while it exists.
	second := newPodmanAdapterOnly(t, fixture)
	if err := second.Release(ctx, fixture.workspaceID); err == nil {
		t.Fatal("released a workspace still owned by a labelled container")
	} else {
		runtimesupport.RequireFaultClass(t, err, fault.Forbidden)
	}
	if removal, err := fixture.adapter.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}); err != nil || !removal.Removed {
		t.Fatalf("Remove = %+v, %v; want removed", removal, err)
	}
	if names := podmanContainerNames(t, ctx, fixture.workspaceID); len(names) != 0 {
		t.Fatalf("containers remain after Remove: %v", names)
	}
	if err := second.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release after Remove: %v", err)
	}
	if runtimesupport.VolumeExists(ctx, fixture.volumeName) {
		t.Fatal("workspace volume still exists after release")
	}
	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "cleanup-and-workspace-exclusivity",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}

// TestPodmanRuntimeRemovalFailureStaysPending proves a bounded removal failure
// keeps the owned container and host materialization visible (blocking
// Release), and a retry completes cleanup without touching the retained
// workspace.
func TestPodmanRuntimeRemovalFailureStaysPending(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-6")
	identity := testIdentity("exec-c6", "action-c6", "workload-c6", "idem-c6")
	failing := &failingRemoveLibpod{Libpod: fixture.libpod, failures: 3}
	_, _, adapter := newPodmanAdapter(t, fixture, failing)
	request := fixture.request(identity, []string{`echo done > /workspace/done`}, nil, nil, "tok-abc")
	if _, err := adapter.Prepare(ctx, request); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitObservation(t, ctx, adapter,
		reconcile.ObserveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}, engine.ObservationExited)
	if evidence := runtimesupport.WaitForStopConfirmed(t, ctx, adapter, reconcile.StopRequest{Identity: identity, WorkspaceID: fixture.workspaceID}, 0, 0); !evidence.IsConfirmed() {
		t.Fatalf("stop evidence = %+v; want confirmed", evidence)
	}
	removal, err := adapter.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if removal.Removed {
		t.Fatalf("removal with exhausted attempts = %+v; want retained", removal)
	}
	name, _ := podman.ContainerName(identity.WorkloadID)
	if !strings.Contains(removal.Detail, name) {
		t.Fatalf("removal detail does not name the retained container: %q", removal.Detail)
	}
	workloadDir := filepath.Join(fixture.root, string(identity.WorkloadID))
	if _, err := os.Stat(workloadDir); err != nil {
		t.Fatalf("exhausted removal deleted host materialization %s: %v", workloadDir, err)
	}
	runtimesupport.RequireFaultClass(t, adapter.Release(ctx, fixture.workspaceID), fault.Forbidden)

	failing.clearFailures()
	removal, err = adapter.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err != nil || !removal.Removed {
		t.Fatalf("retried Remove = %+v, %v; want removed", removal, err)
	}
	if _, err := os.Stat(workloadDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("host materialization %s survived completed Remove: %v", workloadDir, err)
	}
	if !runtimesupport.VolumeExists(ctx, fixture.volumeName) {
		t.Fatal("Remove touched the retained workspace volume")
	}
	if err := adapter.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "removal-failure-stays-pending",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// lostStartLibpod loses exactly one start response after the real effect.
type lostStartLibpod struct {
	podman.Libpod
	mu     sync.Mutex
	lose   bool
	starts int
}

func (l *lostStartLibpod) StartContainer(ctx context.Context, name string) error {
	l.mu.Lock()
	l.starts++
	lose := l.lose
	l.lose = false
	l.mu.Unlock()
	if lose {
		if err := l.Libpod.StartContainer(ctx, name); err != nil {
			return err
		}
		return errors.New("injected lost start response")
	}
	return l.Libpod.StartContainer(ctx, name)
}

func (l *lostStartLibpod) startCalls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.starts
}

// gatedLibpod blocks the first start and stop call until the test releases it.
type gatedLibpod struct {
	podman.Libpod
	startEntered chan struct{}
	startRelease chan struct{}
	stopEntered  chan struct{}
	stopRelease  chan struct{}
	startOnce    sync.Once
	stopOnce     sync.Once
}

func newGatedLibpod(inner podman.Libpod) *gatedLibpod {
	return &gatedLibpod{
		Libpod:       inner,
		startEntered: make(chan struct{}),
		startRelease: make(chan struct{}),
		stopEntered:  make(chan struct{}),
		stopRelease:  make(chan struct{}),
	}
}

func (g *gatedLibpod) StartContainer(ctx context.Context, name string) error {
	block := false
	g.startOnce.Do(func() {
		block = true
		close(g.startEntered)
	})
	if block {
		select {
		case <-g.startRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.Libpod.StartContainer(ctx, name)
}

func (g *gatedLibpod) StopContainer(ctx context.Context, name string) error {
	block := false
	g.stopOnce.Do(func() {
		block = true
		close(g.stopEntered)
	})
	if block {
		select {
		case <-g.stopRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.Libpod.StopContainer(ctx, name)
}

// failingRemoveLibpod fails the first N container removals before the effect.
type failingRemoveLibpod struct {
	podman.Libpod
	mu       sync.Mutex
	failures int
}

func (f *failingRemoveLibpod) RemoveContainer(ctx context.Context, name string) error {
	f.mu.Lock()
	fail := f.failures > 0
	if fail {
		f.failures--
	}
	f.mu.Unlock()
	if fail {
		return errors.New("injected removal failure")
	}
	return f.Libpod.RemoveContainer(ctx, name)
}

func (f *failingRemoveLibpod) clearFailures() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = 0
}

// newPodmanAdapterOnly builds a fresh adapter over the fixture's real client
// and root.
func newPodmanAdapterOnly(t *testing.T, fixture *podmanFixture) *podman.Adapter {
	t.Helper()
	_, _, adapter := newPodmanAdapter(t, fixture, nil)
	return adapter
}
