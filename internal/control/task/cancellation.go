package task

// Both task cancel paths build the Kernel cancel reduction.
//
// `Ask` and `Cancel` never write `executions.state = cancelled` directly. When
// the task has a known current execution the service loads its
// version-guarded aggregate through the consumer-owned
// `ExecutionAggregateReader`, reduces `flow.CancelFact` exactly as the
// execution service does, carries the resulting `flow.Commit` and aggregate
// version, and validates the unit with `ValidateCancellationCommit` before the
// port. A terminal predecessor is preserved with no commit.
//
// The unit's own task event stays first; the appended projection is
// `event.ExcludeOwned(event.ProjectLifecycle(pre, commit))`. Neither
// `task.question` nor `task.cancelled` is a projected lifecycle class, so
// nothing is excluded and every transition is published exactly once.

import (
	"context"
	"fmt"
	"time"

	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/flow"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// cancelReduction loads the task's current execution aggregate and returns the
// Kernel cancel commit for a non-terminal predecessor. A terminal predecessor
// returns the aggregate with a nil commit so its row is preserved.
//
// The aggregate read is scoped and never retried in another scope. A missing
// aggregate for a known task pointer is an inconsistency and is reported as
// the reader's uniform not_found; a dependency failure keeps its class. Either
// way the operation fails closed before the port and no unit is built.
func (service *Service) cancelReduction(
	ctx context.Context,
	scope tenancy.Scope,
	pointer id.ExecutionID,
	at time.Time,
) (engine.Execution, *flow.Commit, error) {
	return service.cancelReductionWithReason(ctx, scope, pointer, flow.CancelRequested, "", at)
}

func (service *Service) cancelReductionWithReason(
	ctx context.Context,
	scope tenancy.Scope,
	pointer id.ExecutionID,
	reason flow.CancelReason,
	detail string,
	at time.Time,
) (engine.Execution, *flow.Commit, error) {
	aggregate, err := service.aggregates.Load(ctx, scope, pointer)
	if err != nil {
		return engine.Execution{}, nil, err
	}
	if aggregate.ID != pointer {
		return engine.Execution{}, nil, fault.New(fault.InvalidInput,
			fmt.Sprintf("execution aggregate %q does not match the task's current execution %q",
				aggregate.ID, pointer))
	}
	if aggregate.State.Terminal() {
		return aggregate, nil, nil
	}
	_, commit, err := flow.Reduce(aggregate, flow.CancelFact{
		ExecutionID: aggregate.ID,
		Reason:      reason,
		Detail:      detail,
		At:          at,
	})
	if err != nil {
		return engine.Execution{}, nil, err
	}
	if commit == nil {
		return engine.Execution{}, nil, fault.New(fault.InternalFailure,
			fmt.Sprintf("cancel reduction for execution %q returned no commit", aggregate.ID))
	}
	return aggregate, commit, nil
}
