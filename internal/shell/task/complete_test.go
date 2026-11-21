package task_test

import (
	"testing"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/kernel"
	"github.com/optimiware/kodema/internal/shell/authorize"
	"github.com/optimiware/kodema/internal/shell/task"
	"github.com/optimiware/kodema/internal/shell/tenancy"
)

func completeCommand(expectedLifecycle uint64) task.CompleteCommand {
	return task.CompleteCommand{
		Scope:                    projectScope(),
		Actor:                    humanActor(engineerPrincipal, tenancy.RoleEngineer),
		TaskID:                   taskID,
		ExpectedLifecycleVersion: expectedLifecycle,
		Reason:                   "merged and verified",
		CallerKey:                "complete-1",
		Content:                  receiptContent(`{"reason":"merged and verified"}`),
	}
}

// TestCompleteRetriesWhenTerminalPredecessorAdvancesVersion proves that a
// background version bump on an already-terminal current execution (such as
// post-terminal workspace cleanup) does not fail task.complete with a spurious
// version_conflict when the task itself stays open at the caller's version.
func TestCompleteRetriesWhenTerminalPredecessorAdvancesVersion(t *testing.T) {
	env := newTestEnv(t)
	env.reader.put(baseTask())
	env.executions.put(task.ExecutionView{
		ID:      executionOne,
		Version: 3,
		State:   kernel.ExecutionSucceeded,
	})

	attempts := 0
	env.ports.setComplete(func(unit task.CompleteUnit) (task.CompleteResult, error) {
		attempts++
		if attempts == 1 {
			if unit.ExpectedExecutionVersion != 3 {
				t.Fatalf("attempt 1 ExpectedExecutionVersion = %d; want 3", unit.ExpectedExecutionVersion)
			}
			// Simulate post-terminal background cleanup advancing the execution
			// version while the execution stays succeeded.
			env.executions.put(task.ExecutionView{
				ID:      executionOne,
				Version: 4,
				State:   kernel.ExecutionSucceeded,
			})
			return task.CompleteResult{}, fault.New(fault.VersionConflict, "a guarded row was not at the expected version")
		}
		if unit.ExpectedExecutionVersion != 4 {
			t.Fatalf("attempt 2 ExpectedExecutionVersion = %d; want 4", unit.ExpectedExecutionVersion)
		}
		var sawExecCheck bool
		for _, check := range unit.Authority {
			if check.Check == authorize.CheckExecutionVersion {
				sawExecCheck = true
				if check.ExpectedVersion != 4 {
					t.Fatalf("attempt 2 CheckExecutionVersion = %d; want 4", check.ExpectedVersion)
				}
			}
		}
		if !sawExecCheck {
			t.Fatal("attempt 2 missing CheckExecutionVersion authority read")
		}
		return task.CompleteResult{LifecycleVersion: unit.ExpectedLifecycleVersion + 1}, nil
	})

	result, err := env.service.Complete(t.Context(), completeCommand(4))
	requireNoError(t, err, "Complete after terminal predecessor version bump")
	if result.LifecycleVersion != 5 {
		t.Fatalf("LifecycleVersion = %d; want 5", result.LifecycleVersion)
	}
	if attempts != 2 {
		t.Fatalf("complete attempts = %d; want 2", attempts)
	}
}

// TestCompleteDoesNotRetryWhenTaskLifecycleMovedOrPredecessorUnchanged proves
// genuine version conflicts (the task moved, or the execution version did not
// advance) are returned immediately without spinning.
func TestCompleteDoesNotRetryWhenTaskLifecycleMovedOrPredecessorUnchanged(t *testing.T) {
	t.Run("task lifecycle moved", func(t *testing.T) {
		env := newTestEnv(t)
		env.reader.put(baseTask())
		env.executions.put(task.ExecutionView{
			ID:      executionOne,
			Version: 3,
			State:   kernel.ExecutionSucceeded,
		})

		attempts := 0
		env.ports.setComplete(func(task.CompleteUnit) (task.CompleteResult, error) {
			attempts++
			moved := baseTask()
			moved.LifecycleVersion = 5
			env.reader.put(moved)
			env.executions.put(task.ExecutionView{
				ID:      executionOne,
				Version: 4,
				State:   kernel.ExecutionSucceeded,
			})
			return task.CompleteResult{}, fault.New(fault.VersionConflict, "task moved")
		})

		_, err := env.service.Complete(t.Context(), completeCommand(4))
		requireFaultClass(t, err, fault.VersionConflict)
		if attempts != 1 {
			t.Fatalf("attempts = %d; want 1 when task lifecycle moved", attempts)
		}
	})

	t.Run("predecessor version unchanged", func(t *testing.T) {
		env := newTestEnv(t)
		env.reader.put(baseTask())
		env.executions.put(task.ExecutionView{
			ID:      executionOne,
			Version: 3,
			State:   kernel.ExecutionSucceeded,
		})

		attempts := 0
		env.ports.setComplete(func(task.CompleteUnit) (task.CompleteResult, error) {
			attempts++
			return task.CompleteResult{}, fault.New(fault.VersionConflict, "conflict without predecessor move")
		})

		_, err := env.service.Complete(t.Context(), completeCommand(4))
		requireFaultClass(t, err, fault.VersionConflict)
		if attempts != 1 {
			t.Fatalf("attempts = %d; want 1 when predecessor version did not advance", attempts)
		}
	})
}
