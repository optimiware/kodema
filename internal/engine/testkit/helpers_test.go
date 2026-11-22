package testkit

import (
	"context"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/flow"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// This file contains test-only constructors. The production testkit API is
// deliberately exercised from the same package so the red state is a precise
// missing symbol rather than a missing import path.

var testNow = time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)

func testExecution(executionID, workspaceID string, actions ...engine.Action) engine.Execution {
	return engine.Execution{
		ID:          id.ExecutionID(executionID),
		Version:     0,
		State:       engine.ExecutionQueued,
		WorkspaceID: id.WorkspaceID(workspaceID),
		Snapshot: definition.NewSnapshot(definition.SnapshotInput{
			Pipeline: definition.Pipeline{},
			YAML:     []byte("name: test\n"),
			Inputs:   map[string]string{"input": "value"},
		}),
		Actions:       append([]engine.Action(nil), actions...),
		CurrentAction: -1,
	}
}

func testContainerAction(actionID, name string, index int, state engine.ActionState, version uint64) engine.Action {
	workloadID := id.WorkloadID("wl-" + actionID)
	return engine.Action{
		ID:      id.ActionID(actionID),
		Index:   index,
		Name:    id.ActionName(name),
		Kind:    definition.ActionKindContainer,
		State:   state,
		Version: version,
		Timeout: time.Hour,
		Outputs: engine.Outputs{},
		Container: &engine.ContainerAction{
			Definition: definition.Container{
				Image:    "registry.example/app@sha256:abc",
				Commands: []string{"run"},
				Outputs:  []id.OutputKey{"candidate"},
			},
			WorkloadID:  workloadID,
			WorkspaceID: id.WorkspaceID("ws-1"),
			Launch: engine.LaunchIntent{
				WorkloadID:     workloadID,
				State:          engine.LaunchNotIssued,
				IdempotencyKey: "idem-" + actionID,
			},
		},
	}
}

func testRequestAction(actionID, name string, index int, state engine.ActionState, version uint64) engine.Action {
	return engine.Action{
		ID:      id.ActionID(actionID),
		Index:   index,
		Name:    id.ActionName(name),
		Kind:    definition.ActionKindRequest,
		State:   state,
		Version: version,
		Timeout: time.Hour,
		Request: &engine.RequestAction{
			Definition: definition.Request{
				Assignment: "assignee",
				Outputs:    []id.OutputKey{"decision"},
			},
		},
	}
}

func testCommit(executionID, actionID string, executionVersion, actionVersion uint64) flow.Commit {
	return flow.Commit{
		ExecutionID:              id.ExecutionID(executionID),
		ExpectedExecutionVersion: executionVersion,
		NextExecutionState:       engine.ExecutionRunning,
		ActionID:                 id.ActionID(actionID),
		ExpectedActionVersion:    actionVersion,
		NextActionState:          engine.ActionRunning,
		Output:                   flow.OutputChange{Action: flow.OutputNone},
		At:                       testNow,
	}
}

func testIdentity(executionID, actionID, workloadID string) reconcile.EffectIdentity {
	return reconcile.EffectIdentity{
		ExecutionID:    id.ExecutionID(executionID),
		ActionID:       id.ActionID(actionID),
		WorkloadID:     id.WorkloadID(workloadID),
		IdempotencyKey: "idem-" + workloadID,
	}
}

func testPrepare(t *testing.T, runtime *Runtime, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID) reconcile.Preparation {
	t.Helper()
	preparation, err := runtime.Prepare(context.Background(), reconcile.PrepareRequest{
		Identity:    identity,
		WorkspaceID: workspaceID,
		Container: definition.Container{
			Image:    "registry.example/app@sha256:abc",
			Commands: []string{"run"},
		},
	})
	if err != nil {
		t.Fatalf("Prepare(%s) error = %v", identity.WorkloadID, err)
	}
	return preparation
}

func testStart(t *testing.T, runtime *Runtime, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID) reconcile.StartResult {
	t.Helper()
	result, err := runtime.Start(context.Background(), reconcile.StartRequest{
		Identity:    identity,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		t.Fatalf("Start(%s) error = %v", identity.WorkloadID, err)
	}
	return result
}

func testRuntimeWithPreparedWorkload(t *testing.T) (*Runtime, reconcile.EffectIdentity, id.WorkspaceID) {
	t.Helper()
	runtime := NewRuntime()
	identity := testIdentity("exec-1", "act-1", "wl-1")
	workspaceID := id.WorkspaceID("ws-1")
	testPrepare(t, runtime, identity, workspaceID)
	return runtime, identity, workspaceID
}

func testRuntimeWithStartedWorkload(t *testing.T) (*Runtime, reconcile.EffectIdentity, id.WorkspaceID) {
	t.Helper()
	runtime, identity, workspaceID := testRuntimeWithPreparedWorkload(t)
	result := testStart(t, runtime, identity, workspaceID)
	if result.Outcome != reconcile.StartAccepted {
		t.Fatalf("Start outcome = %q, want %q", result.Outcome, reconcile.StartAccepted)
	}
	return runtime, identity, workspaceID
}

func testClass(t *testing.T, err error, want fault.Class) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want fault class %q", want)
	}
	class, ok := fault.ClassOf(err)
	if !ok || class != want {
		t.Fatalf("error class = (%q, %v) for %v, want %q", class, ok, err, want)
	}
}
