// Observation, stop, ownership and redaction evidence for the Podman adapter.
//
// The rules here keep uncertainty visible: an absent container, an unreachable
// daemon, a container in "created" status, and an exited main process without
// whole-container evidence never collapse into nonexecution or stop
// confirmation.
package podman

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// checkContainerIdentity reports whether the observed container belongs to the
// persisted effect identity. A container created for another identity is never
// silently reused or removed.
func checkContainerIdentity(state ContainerState, identity reconcile.EffectIdentity) error {
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
func checkExistingContainer(state ContainerState, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, image string) error {
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
// vocabulary. A container in "created" status stays unknown conservatively:
// without external side-band state, a libpod client cannot prove whether a
// start request is still in flight.
func observationFor(state ContainerState, memoryLimitBytes int64) engine.Observation {
	switch state.Status {
	case ContainerStatusCreated:
		return engine.UnknownObservation()
	case ContainerStatusRunning:
		return engine.RunningObservation()
	case ContainerStatusExited:
		var stop engine.StopEvidence
		if state.WholeStopped {
			if state.OOMKilled {
				stop = engine.ConfirmedStop(fmt.Sprintf(
					"container %q was OOMKilled (exit code %d) and every container process stopped",
					state.Name, state.ExitCode))
			} else {
				stop = engine.ConfirmedStop(fmt.Sprintf(
					"container %q exited with code %d and every container process stopped",
					state.Name, state.ExitCode))
			}
		} else {
			stop = engine.PendingStop(fmt.Sprintf(
				"container %q exited with code %d without whole-container termination evidence",
				state.Name, state.ExitCode))
		}
		if state.OOMKilled {
			return engine.OOMKilledObservation(state.ExitCode, definition.FormatMemoryBytes(memoryLimitBytes), stop)
		}
		return engine.ExitedObservation(state.ExitCode, stop)
	default:
		return engine.UnknownObservation()
	}
}

// removable reports whether cleanup may proceed for one observed container.
// Only affirmative whole-container termination qualifies; a running, created,
// ambiguous, or partially stopped container retains every owned resource.
func removable(state ContainerState) bool {
	return state.Status == ContainerStatusExited && state.WholeStopped
}

// describeContainerState renders an operator-facing, non-secret state summary.
func describeContainerState(state ContainerState) string {
	return fmt.Sprintf("status %q started=%t start_pending=%t whole_stopped=%t",
		state.Status, state.Started, state.StartPending, state.WholeStopped)
}

// isTransientTempFile reports whether name is an atomic-write staging file
// created either by replaceFile (KODEMA_TOKEN.tmp-*) or by the production host
// file boundary (.kodema-write-*).
func isTransientTempFile(name string) bool {
	return strings.HasPrefix(name, string(id.SecretTokenName)+".tmp-") ||
		strings.HasPrefix(name, ".kodema-write-")
}

// redactionValues reads every configured secret and credential file directly
// from the isolated <root>/<workload>/secrets/data directory, returning
// non-empty values longest first. A missing/unreadable/non-directory secrets
// root, a symlink or non-regular entry, a missing KODEMA_TOKEN, or any missing
// declared secret fails closed so log output is never emitted without redaction.
func (a *Adapter) redactionValues(ctx context.Context, workload id.WorkloadID) ([]string, error) {
	parentInfo, err := a.files.Lstat(ctx, secretsParentDir(a.root, workload))
	if err != nil {
		return nil, err
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return nil, fault.New(fault.Forbidden, "redaction source directory is not a directory")
	}
	dir := secretsDir(a.root, workload)
	dirInfo, err := a.files.Lstat(ctx, dir)
	if err != nil {
		return nil, err
	}
	if dirInfo.Mode()&os.ModeSymlink != 0 || !dirInfo.IsDir() {
		return nil, fault.New(fault.Forbidden, "redaction source directory is not a directory")
	}
	entries, err := a.files.ReadDir(ctx, dir)
	if err != nil {
		return nil, err
	}

	present := make(map[string]bool, len(entries))
	values := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type()&os.ModeSymlink != 0 {
			return nil, fault.New(fault.Forbidden, "redaction source entry is a symlink")
		}
		path := filepath.Join(dir, name)
		info, err := a.files.Lstat(ctx, path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && isTransientTempFile(name) {
				continue
			}
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fault.New(fault.Forbidden, "redaction source entry is not a regular file")
		}
		data, err := a.files.ReadFile(ctx, path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && isTransientTempFile(name) {
				continue
			}
			return nil, err
		}
		present[name] = true
		if len(data) > 0 {
			values = append(values, string(data))
		}
	}
	if !present[string(id.SecretTokenName)] {
		return nil, os.ErrNotExist
	}
	a.mu.Lock()
	prepared, ok := a.prepared[workload]
	a.mu.Unlock()
	if ok {
		for _, secretName := range prepared.secretNames {
			if !present[string(secretName)] {
				return nil, os.ErrNotExist
			}
		}
	}
	return container.SortRedactionValues(values), nil
}
