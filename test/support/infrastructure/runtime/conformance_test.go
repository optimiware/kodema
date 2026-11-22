package runtimesupport

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

type fakeWorkload struct {
	req       reconcile.PrepareRequest
	obs       engine.Observation
	stdout    string
	stderr    string
	stopped   bool
	removed   bool
	refreshed string
}

type fakeRuntime struct {
	mu           sync.Mutex
	workloads    map[id.WorkloadID]*fakeWorkload
	workspaceOK  bool
	files        map[string]string
	observePolls int
	stopPolls    int
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		workloads:   map[id.WorkloadID]*fakeWorkload{},
		workspaceOK: true,
		files:       map[string]string{},
	}
}

func (f *fakeRuntime) Capabilities(context.Context) (reconcile.Capabilities, error) {
	return reconcile.Capabilities{}, nil
}

func (f *fakeRuntime) Prepare(_ context.Context, req reconcile.PrepareRequest) (reconcile.Preparation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workloads[req.Identity.WorkloadID] = &fakeWorkload{
		req: req,
		obs: engine.AbsentObservation(),
	}
	return reconcile.Preparation{WorkspaceID: req.WorkspaceID}, nil
}

func (f *fakeRuntime) Start(_ context.Context, req reconcile.StartRequest) (reconcile.StartResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.workloads[req.Identity.WorkloadID]
	joined := strings.Join(w.req.Container.Commands, "\n")
	switch {
	case strings.Contains(joined, "cred-ready-before"):
		w.obs = engine.RunningObservation()
		w.stdout = "cred-ready-before\n"
	case strings.Contains(joined, "contract-ok"):
		w.obs = engine.ExitedObservation(0, engine.ConfirmedStop("exited 0"))
		w.stdout = container.Redacted + container.Redacted + "\ncontract-ok\n"
		f.files["contract.txt"] = strings.Join([]string{
			"PWD=/workspace",
			"INPUT=line1|line2|",
			"OUTPUT=earlier-output",
			`TASK={"id":"task-1"}`,
			"ENV:BUILD_MODE=release",
			"ENV:KODEMA_URL=" + StandardKodemaURL,
			"ENV:KODEMA_EXECUTION=" + string(req.Identity.ExecutionID),
			"ENV:KODEMA_ACTION=" + string(StandardActionName),
			"ENV:KODEMA_TASK=" + string(StandardTaskID),
			"ENV:" + workspace.EnvCAFile + "=" + container.WorkloadCAPath,
		}, "\n") + "\n"
	case strings.Contains(joined, "first"):
		w.obs = engine.ExitedObservation(0, engine.ConfirmedStop("exited 0"))
		f.files["persist.txt"] = "persisted\n"
		f.files["launch-count"] = "first\n"
	case strings.Contains(joined, "second"):
		w.obs = engine.ExitedObservation(0, engine.ConfirmedStop("exited 0"))
		f.files["launch-count"] += "second\n"
		w.stdout = f.files["persist.txt"] + f.files["launch-count"]
	default:
		w.obs = engine.ExitedObservation(0, engine.ConfirmedStop("exited 0"))
	}
	return reconcile.StartResult{Outcome: reconcile.StartAccepted, WorkloadID: req.Identity.WorkloadID}, nil
}

func (f *fakeRuntime) Observe(_ context.Context, req reconcile.ObserveRequest) (engine.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observePolls++
	w, ok := f.workloads[req.Identity.WorkloadID]
	if !ok || w.removed {
		return engine.AbsentObservation(), nil
	}
	return w.obs, nil
}

func (f *fakeRuntime) Logs(_ context.Context, req reconcile.LogsRequest) (reconcile.LogBatch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w, ok := f.workloads[req.Identity.WorkloadID]
	if !ok {
		return reconcile.LogBatch{}, fault.New(fault.NotFound, "missing")
	}
	combined := w.stdout
	if int(req.AfterOffset) >= len(combined) {
		return reconcile.LogBatch{NextOffset: req.AfterOffset}, nil
	}
	end := min(int(req.AfterOffset)+req.Limit, len(combined))
	chunk := reconcile.LogChunk{
		Stream: reconcile.LogStdout,
		Offset: req.AfterOffset,
		Data:   []byte(combined[req.AfterOffset:end]),
	}
	return reconcile.LogBatch{
		Chunks:     []reconcile.LogChunk{chunk},
		NextOffset: int64(end),
	}, nil
}

func (f *fakeRuntime) Stop(_ context.Context, req reconcile.StopRequest) (engine.StopEvidence, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopPolls++
	if f.stopPolls == 1 {
		return engine.PendingStop("stopping"), nil
	}
	if w, ok := f.workloads[req.Identity.WorkloadID]; ok {
		w.stopped = true
		w.obs = engine.ExitedObservation(0, engine.ConfirmedStop("stopped"))
	}
	return engine.ConfirmedStop("stopped"), nil
}

func (f *fakeRuntime) Remove(_ context.Context, req reconcile.RemoveRequest) (reconcile.Removal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if w, ok := f.workloads[req.Identity.WorkloadID]; ok {
		w.removed = true
	}
	return reconcile.Removal{Removed: true}, nil
}

func (f *fakeRuntime) RefreshCredential(_ context.Context, req reconcile.CredentialRequest) (reconcile.CredentialRefresh, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.workloads[req.Identity.WorkloadID]
	w.refreshed = req.Credential
	w.stdout += "cred-ready-after\n"
	return reconcile.CredentialRefresh{Delivered: true}, nil
}

func (f *fakeRuntime) Status(_ context.Context, ws id.WorkspaceID) (reconcile.WorkspaceObservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return reconcile.WorkspaceObservation{WorkspaceID: ws, Available: f.workspaceOK}, nil
}

func (f *fakeRuntime) Retain(context.Context, id.WorkspaceID) error { return nil }

func (f *fakeRuntime) Release(context.Context, id.WorkspaceID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workspaceOK = false
	return nil
}

func TestConformanceHelpersAndContractSuites(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fake := newFakeRuntime()
	ws := id.WorkspaceID("ws-fake-1")

	h := ContainerHarness{
		Runtime:          fake,
		Workspace:        fake,
		WorkspaceID:      ws,
		Image:            "docker.io/library/alpine@sha256:" + strings.Repeat("a", 64),
		ActionTimeout:    5 * time.Second,
		PollInterval:     10 * time.Millisecond,
		ExpectWorkloadCA: true,
		NewIdentity: func(_ testing.TB, label string) reconcile.EffectIdentity {
			return reconcile.EffectIdentity{
				ExecutionID:    id.ExecutionID("exec-" + label),
				ActionID:       id.ActionID("act-" + label),
				WorkloadID:     id.WorkloadID("wl-" + label),
				IdempotencyKey: "idem-" + label,
			}
		},
		ReadWorkspaceFile: func(_ testing.TB, _ context.Context, path string) string {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			return fake.files[path]
		},
	}

	var noRestartChecked bool
	h.AssertNoRestart = func(_ testing.TB, _ reconcile.EffectIdentity) {
		noRestartChecked = true
	}

	AssertAbsentObservation(t, ctx, h)
	AssertMaterializationContract(t, ctx, h)
	AssertLiveCredentialRefresh(t, ctx, h)
	if !noRestartChecked {
		t.Fatal("AssertNoRestart hook was not invoked")
	}
	AssertWorkspacePersistence(t, ctx, h)

	RequireFaultClass(t, fault.New(fault.Forbidden, "denied"), fault.Forbidden)
	if pemBytes := GenerateTestCAPEM(t, "test-ca"); !strings.Contains(string(pemBytes), "BEGIN CERTIFICATE") {
		t.Fatalf("GenerateTestCAPEM returned invalid PEM: %q", pemBytes)
	}
}
