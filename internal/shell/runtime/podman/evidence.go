// Observation, stop, ownership and redaction evidence for the Podman adapter.
//
// The rules here keep uncertainty visible: an absent container, an unreachable
// daemon, a created container whose start may still be delayed and an exited
// main process without whole-container evidence never collapse into
// nonexecution or stop confirmation.
package podman

import (
	"context"
	"fmt"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/kernel"
	"github.com/optimiware/kodema/internal/kernel/definition"
	"github.com/optimiware/kodema/internal/kernel/engine"
	"github.com/optimiware/kodema/internal/shell/runtime/internal/adapterutil"
	"github.com/optimiware/kodema/internal/shell/workspace"
)

// checkContainerIdentity reports whether the observed container belongs to the
// persisted effect identity. A container created for another identity is never
// silently reused or removed.
func checkContainerIdentity(state ContainerState, identity engine.EffectIdentity) error {
	switch {
	case state.Labels[LabelWorkload] != string(identity.WorkloadID):
		return fault.New(fault.Forbidden, "container is not owned by this workload identity")
	case state.Labels[LabelExecution] != string(identity.ExecutionID):
		return fault.New(fault.Forbidden, "container is not owned by this execution identity")
	case state.Labels[LabelAction] != string(identity.ActionID):
		return fault.New(fault.Forbidden, "container is not owned by this action identity")
	case state.Labels[LabelIdempotencyKey] != identity.IdempotencyKey:
		return fault.New(fault.ChangedIdempotencyContent, "container carries a different idempotency key")
	}
	return nil
}

// checkContainerWorkspace reports whether the observed container is bound to the
// requested retained workspace.
func checkContainerWorkspace(state ContainerState, workspaceID id.WorkspaceID) error {
	if state.Labels[LabelWorkspace] != string(workspaceID) {
		return fault.New(fault.Forbidden, "container is bound to a different workspace")
	}
	return nil
}

// checkExistingContainer reports whether an already existing container belongs
// to this exact effect identity: same labels, same retained workspace and the
// image pinned by the completed preparation.
func checkExistingContainer(state ContainerState, identity engine.EffectIdentity, workspaceID id.WorkspaceID, image string) error {
	if err := checkContainerIdentity(state, identity); err != nil {
		return err
	}
	if err := checkContainerWorkspace(state, workspaceID); err != nil {
		return err
	}
	if state.Image != "" && state.Image != image {
		return fault.New(fault.ChangedIdempotencyContent, "existing container uses a different image")
	}
	return nil
}

// observationFor maps libpod container evidence to the declared Kernel
// vocabulary. A created container is affirmative never-started proof only when
// no start attempt was durably recorded and none is pending; an attempted start
// whose outcome was lost stays uncertain.
func observationFor(state ContainerState, attempted bool, memoryLimitBytes int64) kernel.Observation {
	switch state.Status {
	case ContainerStatusCreated:
		if attempted || state.Started || state.StartPending {
			// A start was attempted, accepted or may still be delivered: the
			// command may run, so nonexecution is not proven.
			return kernel.UnknownObservation()
		}
		return kernel.NeverStarted(fmt.Sprintf(
			"container %q exists in status %q with no start attempt, no accepted start and no pending start",
			state.Name, state.Status))
	case ContainerStatusRunning:
		return kernel.RunningObservation()
	case ContainerStatusExited:
		var stop kernel.StopEvidence
		if state.WholeStopped {
			if state.OOMKilled {
				stop = kernel.ConfirmedStop(fmt.Sprintf(
					"container %q was OOMKilled (exit code %d) and every container process stopped",
					state.Name, state.ExitCode))
			} else {
				stop = kernel.ConfirmedStop(fmt.Sprintf(
					"container %q exited with code %d and every container process stopped",
					state.Name, state.ExitCode))
			}
		} else {
			stop = kernel.PendingStop(fmt.Sprintf(
				"container %q exited with code %d without whole-container termination evidence",
				state.Name, state.ExitCode))
		}
		if state.OOMKilled {
			return kernel.OOMKilledObservation(state.ExitCode, definition.FormatMemoryBytes(memoryLimitBytes), stop)
		}
		return kernel.ExitedObservation(state.ExitCode, stop)
	default:
		return kernel.UnknownObservation()
	}
}

// removable reports whether cleanup may proceed for one observed container.
// Only affirmative never-started evidence (no recorded start attempt) or
// affirmative whole-container termination qualifies; a running, attempted-start,
// delayed-start, ambiguous or partially stopped container retains every owned
// resource.
func removable(state ContainerState, attempted bool) bool {
	switch state.Status {
	case ContainerStatusCreated:
		return !attempted && !state.Started && !state.StartPending
	case ContainerStatusExited:
		return state.WholeStopped
	default:
		return false
	}
}

// describeContainerState renders an operator-facing, non-secret state summary.
func describeContainerState(state ContainerState) string {
	return fmt.Sprintf("status %q started=%t start_pending=%t whole_stopped=%t",
		state.Status, state.Started, state.StartPending, state.WholeStopped)
}

// redactionValues returns every configured value that must be removed from log
// output, longest first so a shorter value cannot partially obscure a longer
// one. A missing or unreadable source is an error: log output is never emitted
// without redaction.
func (a *Adapter) redactionValues(ctx context.Context, workload id.WorkloadID, record preparationRecord) ([]string, error) {
	logicalPaths := make([]string, 0, len(record.Secrets)+1)
	for _, name := range record.Secrets {
		path, err := workspace.SecretPath(name)
		if err != nil {
			return nil, err
		}
		logicalPaths = append(logicalPaths, path)
	}
	logicalPaths = append(logicalPaths, workspace.ReservedCredentialPath)

	values := make([]string, 0, len(logicalPaths))
	for _, logical := range logicalPaths {
		data, err := a.files.ReadFile(ctx, hostPath(a.root, workload, logical))
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			continue
		}
		values = append(values, string(data))
	}
	return adapterutil.SortRedactionValues(values), nil
}
