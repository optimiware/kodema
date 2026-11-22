package testkit

import (
	"testing"

	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/kernel"
	"github.com/optimiware/kodema/internal/kernel/definition"
)

// TestSnapshotClonePreservesTaskID pins that the testkit's defensive snapshot
// copy keeps the task identity, so seeded and loaded executions do
// not silently lose the task context an engine Prepare would carry.
func TestSnapshotClonePreservesTaskID(t *testing.T) {
	snapshot := definition.NewSnapshot(definition.SnapshotInput{
		YAML:   []byte("name: clone\n"),
		TaskID: "task-1",
		Task:   []byte(`{"id":"task-1"}`),
		TaskFiles: map[string][]byte{
			"title": []byte("Clone title"),
		},
	})

	cloned := cloneSnapshot(snapshot)
	if got := cloned.TaskID(); got != id.TaskID("task-1") {
		t.Fatalf("cloned snapshot task id = %q; want task-1", got)
	}
	if got := cloned.TaskFiles(); string(got["title"]) != "Clone title" {
		t.Fatalf("cloned snapshot task files = %v; want title=Clone title", got)
	}

	execution := testExecution("exec-clone-task", "ws-clone-task", testContainerAction("act-build", "build", 0, kernel.ActionPending, 0))
	execution.Snapshot = snapshot
	store := NewStore()
	store.Seed(execution)

	loaded, ok := store.Execution(execution.ID)
	if !ok {
		t.Fatal("seeded execution is missing from the store")
	}
	if got := loaded.Snapshot.TaskID(); got != id.TaskID("task-1") {
		t.Fatalf("stored snapshot task id = %q; want task-1", got)
	}

	// The store's defensive copy must not alias the caller's snapshot.
	execution.Snapshot = definition.NewSnapshot(definition.SnapshotInput{TaskID: "mutated"})
	loaded, _ = store.Execution(execution.ID)
	if got := loaded.Snapshot.TaskID(); got != id.TaskID("task-1") {
		t.Fatalf("stored snapshot task id mutated through caller: %q", got)
	}
}

// TestCloneDefinitionContainerDeepCopiesCachesAndBudget pins that testkit's
// defensive container clone isolates both Caches slices and Budget pointers.
func TestCloneDefinitionContainerDeepCopiesCachesAndBudget(t *testing.T) {
	action := testContainerAction("act-build", "build", 0, kernel.ActionPending, 0)
	action.Container.Definition.Caches = []string{"go-mod", "npm"}
	action.Container.Definition.Budget = &definition.Budget{MaxTokens: 4000, MaxCostUSD: 1.25}
	execution := testExecution("exec-clone-container", "ws-clone-container", action)

	store := NewStore()
	store.Seed(execution)

	// Mutate caller's slice and budget pointer after Seed.
	execution.Actions[0].Container.Definition.Caches[0] = "caller-mutated"
	execution.Actions[0].Container.Definition.Budget.MaxTokens = 1
	execution.Actions[0].Container.Definition.Budget.MaxCostUSD = 99.0

	loaded, ok := store.Execution(execution.ID)
	if !ok {
		t.Fatal("seeded execution is missing from the store")
	}
	gotDef := loaded.Actions[0].Container.Definition
	if len(gotDef.Caches) != 2 || gotDef.Caches[0] != "go-mod" || gotDef.Caches[1] != "npm" {
		t.Fatalf("stored container caches mutated through caller: %v", gotDef.Caches)
	}
	if gotDef.Budget == nil || gotDef.Budget.MaxTokens != 4000 || gotDef.Budget.MaxCostUSD != 1.25 {
		t.Fatalf("stored container budget mutated through caller: %+v", gotDef.Budget)
	}

	// Mutate loaded copy and verify stored state remains untouched.
	gotDef.Caches[1] = "loaded-mutated"
	gotDef.Budget.MaxTokens = 2

	reloaded, _ := store.Execution(execution.ID)
	reloadedDef := reloaded.Actions[0].Container.Definition
	if reloadedDef.Caches[1] != "npm" {
		t.Fatalf("stored container caches mutated through loaded copy: %v", reloadedDef.Caches)
	}
	if reloadedDef.Budget.MaxTokens != 4000 {
		t.Fatalf("stored container budget mutated through loaded copy: %+v", reloadedDef.Budget)
	}
}
