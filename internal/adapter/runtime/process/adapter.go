// Process runtime adapter operations.
//
// Every operation consumes the persisted engine.EffectIdentity. Prepare
// materializes one confined all-or-nothing activation snapshot outside the
// execution workspace. Start may only spawn the one stable process group; a
// lost response stays unknown and a retry never replays the command. Stop
// acknowledges a signal but confirms only affirmative whole-group termination.
// Remove is idempotent, attributed and bounded.
package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/engine/script"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// Prepare materializes the activation snapshot once and attaches the existing
// execution workspace. It never starts a process and never recreates a missing
// workspace.
func (a *Adapter) Prepare(ctx context.Context, request reconcile.PrepareRequest) (reconcile.Preparation, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidatePrepareContext(request.ActionName, request.TaskID); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := workspace.ValidateCacheMounts(request.Container.Caches, request.Caches); err != nil {
		return reconcile.Preparation{}, err
	}
	var caches []CacheMount
	if len(request.Caches) > 0 {
		caches = make([]CacheMount, len(request.Caches))
		for index, cache := range request.Caches {
			mountPath, err := workspace.CachePath(cache.Name)
			if err != nil {
				return reconcile.Preparation{}, err
			}
			hostDir, err := CacheDir(a.root, cache.Key)
			if err != nil {
				return reconcile.Preparation{}, err
			}
			caches[index] = CacheMount{
				Name:      cache.Name,
				Key:       cache.Key,
				MountPath: mountPath,
				HostPath:  hostDir,
			}
		}
	}
	renderedScript, err := script.Render(request.Container.Commands)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	env, err := finalEnvironment(request, a.kodemaURL)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	materialization, err := container.BuildMaterialization(request, readOnlyMode)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if err := ctx.Err(); err != nil {
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "preparation cancelled before any effect", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	tombstoned, err := a.tombstoned(ctx, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if tombstoned {
		return reconcile.Preparation{}, fault.New(fault.Forbidden, "workload identity was removed and cannot be prepared again")
	}
	workspaceDir, err := WorkspaceDir(a.workspaceRoot, request.WorkspaceID)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if err := a.requireWorkspace(ctx, workspaceDir); err != nil {
		return reconcile.Preparation{}, err
	}

	digest := snapshotDigest(request, env, materialization)
	existing, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if found {
		if existing.WorkspaceID != request.WorkspaceID {
			return reconcile.Preparation{}, fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
		}
		if existing.Identity != request.Identity || existing.Digest != digest {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"activation snapshot content changed for the same workload identity")
		}
		return reconcile.Preparation{
			WorkspaceID: request.WorkspaceID,
			Detail:      fmt.Sprintf("reused activation snapshot for %s", container.DescribeIdentity(request.Identity)),
		}, nil
	}

	if err := a.materialize(ctx, resourceRoot, materialization, renderedScript); err != nil {
		return reconcile.Preparation{}, err
	}
	for _, cache := range caches {
		// Non-authoritative: create empty cache dir if absent/evicted.
		if err := a.ensureConfinedDir(ctx, cache.HostPath); err != nil {
			return reconcile.Preparation{}, err
		}
	}
	secretNames := make([]id.SecretName, 0, len(materialization.Secrets))
	for _, secret := range materialization.Secrets {
		secretNames = append(secretNames, secret.Name)
	}
	loaded := manifest{
		Version:     1,
		Identity:    request.Identity,
		WorkspaceID: request.WorkspaceID,
		ActionName:  request.ActionName,
		TaskID:      request.TaskID,
		Image:       request.Container.Image,
		Commands:    append([]string(nil), request.Container.Commands...),
		Env:         env,
		SecretNames: secretNames,
		Caches:      caches,
		Digest:      digest,
	}
	if err := a.writeManifest(ctx, resourceRoot, loaded); err != nil {
		return reconcile.Preparation{}, err
	}
	return reconcile.Preparation{
		WorkspaceID: request.WorkspaceID,
		Detail:      fmt.Sprintf("materialized activation snapshot for %s", container.DescribeIdentity(request.Identity)),
	}, nil
}

// materialize writes every prepared file, the generated script and the mutable
// credential. The caller writes the completion manifest last.
func (a *Adapter) materialize(ctx context.Context, resourceRoot string, materialization workspace.Materialization, renderedScript string) error {
	for _, file := range materialization.Files {
		path := hostPath(resourceRoot, file.Path)
		if err := a.ensureConfinedDir(ctx, filepath.Dir(path)); err != nil {
			return err
		}
		if err := a.files.WriteFile(ctx, path, file.Data, readOnlyMode); err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "activation file write failed", err)
		}
	}
	for _, secret := range materialization.Secrets {
		path := hostPath(resourceRoot, workspace.SecretRoot+"/"+string(secret.Name))
		if err := a.ensureConfinedDir(ctx, filepath.Dir(path)); err != nil {
			return err
		}
		if err := a.files.WriteFile(ctx, path, []byte(secret.Value), readOnlyMode); err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "declared secret write failed", err)
		}
	}
	if err := a.ensureConfinedDir(ctx, resourceRoot); err != nil {
		return err
	}
	if err := a.files.WriteFile(ctx, scriptPath(resourceRoot), []byte(renderedScript), readOnlyMode); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "fail-fast script write failed", err)
	}
	credentialPath := hostPath(resourceRoot, workspace.ReservedCredentialPath)
	if err := a.ensureConfinedDir(ctx, filepath.Dir(credentialPath)); err != nil {
		return err
	}
	if err := a.files.WriteFile(ctx, credentialPath, []byte(materialization.Credential), readOnlyMode); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "execution credential write failed", err)
	}
	if err := a.ensureConfinedDir(ctx, filepath.Join(resourceRoot, logsDirectoryName)); err != nil {
		return err
	}
	return nil
}

// Start spawns the one stable process group for a prepared identity. A retry,
// lost response or concurrent duplicate never spawns a second process.
func (a *Adapter) Start(ctx context.Context, request reconcile.StartRequest) (reconcile.StartResult, error) {
	unknown := func(detail string, err error) (reconcile.StartResult, error) {
		return reconcile.StartResult{
			Outcome:    reconcile.StartUnknown,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     detail,
		}, err
	}
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return unknown("start request identity is invalid", err)
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return unknown("start request workspace is invalid", err)
	}
	if err := ctx.Err(); err != nil {
		return unknown("start cancelled before any effect",
			fault.Wrap(fault.DependencyUnavailable, "start cancelled before any effect", err))
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return unknown("start request identity is invalid", err)
	}
	tombstoned, err := a.tombstoned(ctx, request.Identity.WorkloadID)
	if err != nil {
		return unknown("removal tombstone is unreachable", err)
	}
	if tombstoned {
		return unknown("workload identity was removed and cannot be started again",
			fault.New(fault.Forbidden, "workload identity was removed and cannot be started again"))
	}
	loaded, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return unknown("activation snapshot is unreadable", err)
	}
	if !found {
		return unknown("start requires a completed preparation",
			fault.New(fault.InvalidInput, "start requires a completed preparation"))
	}
	if loaded.WorkspaceID != request.WorkspaceID {
		return unknown("start workspace does not match the prepared workspace",
			fault.New(fault.Forbidden, "start workspace does not match the prepared workspace"))
	}
	if loaded.Identity != request.Identity {
		return unknown("activation snapshot belongs to a different identity",
			fault.New(fault.ChangedIdempotencyContent, "activation snapshot belongs to a different identity"))
	}
	workspaceDir, err := WorkspaceDir(a.workspaceRoot, request.WorkspaceID)
	if err != nil {
		return unknown("start request workspace is invalid", err)
	}
	if err := a.requireWorkspace(ctx, workspaceDir); err != nil {
		return unknown("execution workspace is unavailable", err)
	}
	group, err := GroupName(request.Identity.WorkloadID)
	if err != nil {
		return unknown("process group name is invalid", err)
	}

	state, stateFound, err := a.readGroupState(ctx, resourceRoot)
	if err != nil {
		return unknown("process-group bookkeeping is unreadable", err)
	}
	if stateFound {
		if state.Identity != request.Identity {
			return unknown("process-group bookkeeping belongs to a different identity",
				fault.New(fault.ChangedIdempotencyContent, "process-group bookkeeping belongs to a different identity"))
		}
		switch state.State {
		case groupStarted:
			if _, err := a.observer.Status(ctx, group); err != nil {
				return unknown("process-group state is unreachable",
					fault.Wrap(fault.DependencyUnavailable, "process-group state is unreachable", err))
			}
			return reconcile.StartResult{
				Outcome:    reconcile.StartAccepted,
				WorkloadID: request.Identity.WorkloadID,
				Detail:     fmt.Sprintf("reused existing process group %q", group),
			}, nil
		case groupNeverStarted:
			return reconcile.StartResult{
				Outcome:    reconcile.StartProvenNotStarted,
				WorkloadID: request.Identity.WorkloadID,
				Detail:     nonEmptyProof(state.Detail),
			}, nil
		case groupSpawning, groupUnknown:
			status, statusErr := a.observer.Status(ctx, group)
			if statusErr != nil {
				return unknown("process-group state is unreachable",
					fault.Wrap(fault.DependencyUnavailable, "process-group state is unreachable", statusErr))
			}
			if status.NeverCreated {
				proof := nonEmptyProof(state.Detail)
				neverStarted := groupState{
					Group:    group,
					Identity: request.Identity,
					State:    groupNeverStarted,
					Detail:   proof,
				}
				if err := a.writeGroupState(ctx, resourceRoot, neverStarted); err != nil {
					return unknown("affirmative never-created evidence could not be persisted", err)
				}
				return reconcile.StartResult{
					Outcome:    reconcile.StartProvenNotStarted,
					WorkloadID: request.Identity.WorkloadID,
					Detail:     proof,
				}, nil
			}
			if status.Present || status.MembersAlive || status.LeaderExited || status.GroupGone {
				return reconcile.StartResult{
					Outcome:    reconcile.StartAccepted,
					WorkloadID: request.Identity.WorkloadID,
					Detail:     fmt.Sprintf("reused existing process group %q", group),
				}, nil
			}
			return unknown("spawn intent exists but the outcome is unknown",
				fault.New(fault.UncertainExternalEffect, "spawn intent exists but the outcome is unknown"))
		}
	}

	// Persist the spawn intent before the effect so a crash cannot produce a
	// second process group.
	intent := groupState{
		Group:    group,
		Identity: request.Identity,
		State:    groupSpawning,
		Detail:   "spawn intent persisted",
	}
	if err := a.writeGroupState(ctx, resourceRoot, intent); err != nil {
		return unknown("spawn intent could not be persisted", err)
	}

	for _, cache := range loaded.Caches {
		// Non-authoritative: if evicted between Prepare and Start, recreate empty.
		if err := a.ensureConfinedDir(ctx, cache.HostPath); err != nil {
			return unknown("cache directory could not be initialized", err)
		}
	}

	result, spawnErr := a.executor.Spawn(ctx, SpawnRequest{
		Identity:   request.Identity,
		Group:      group,
		Command:    []string{script.ShellPath, scriptPath(resourceRoot)},
		Env:        append([]string(nil), loaded.Env...),
		Dir:        workspaceDir,
		Root:       resourceRoot,
		StdoutPath: stdoutPath(resourceRoot),
		StderrPath: stderrPath(resourceRoot),
		Caches:     append([]CacheMount(nil), loaded.Caches...),
	})

	switch {
	case spawnErr == nil && result.Outcome == SpawnStarted:
		started := groupState{
			Group:    group,
			Identity: request.Identity,
			State:    groupStarted,
			Detail:   "process group started",
		}
		if err := a.writeGroupState(ctx, resourceRoot, started); err != nil {
			return unknown("spawn outcome could not be persisted", err)
		}
		return reconcile.StartResult{
			Outcome:    reconcile.StartAccepted,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     fmt.Sprintf("started process group %q", group),
		}, nil
	case spawnErr == nil && result.Outcome == SpawnRefused:
		proof := nonEmptyProof(result.Detail)
		neverStarted := groupState{
			Group:    group,
			Identity: request.Identity,
			State:    groupNeverStarted,
			Detail:   proof,
		}
		if err := a.writeGroupState(ctx, resourceRoot, neverStarted); err != nil {
			return unknown("affirmative spawn refusal could not be persisted", err)
		}
		return reconcile.StartResult{
			Outcome:    reconcile.StartProvenNotStarted,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     proof,
		}, nil
	default:
		detail := strings.TrimSpace(result.Detail)
		if detail == "" {
			detail = "process spawn outcome is unknown"
		}
		uncertain := groupState{
			Group:    group,
			Identity: request.Identity,
			State:    groupUnknown,
			Detail:   detail,
		}
		_ = a.writeGroupState(ctx, resourceRoot, uncertain)
		if spawnErr != nil {
			return unknown(detail, fault.Wrap(fault.UncertainExternalEffect, "process spawn outcome is unknown", spawnErr))
		}
		return unknown(detail, fault.New(fault.UncertainExternalEffect, "process spawn outcome is unknown"))
	}
}

// Observe reports the declared Kernel vocabulary for the stable identity.
func (a *Adapter) Observe(ctx context.Context, request reconcile.ObserveRequest) (engine.Observation, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return engine.UnknownObservation(), err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return engine.UnknownObservation(), err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return engine.UnknownObservation(), err
	}
	loaded, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return engine.UnknownObservation(), err
	}
	if !found {
		// An unprepared or already removed identity is absent. Absence is never
		// proof of nonexecution.
		return engine.AbsentObservation(), nil
	}
	if loaded.WorkspaceID != request.WorkspaceID {
		return engine.UnknownObservation(), fault.New(fault.Forbidden, "observation workspace does not match the prepared workspace")
	}
	if loaded.Identity != request.Identity {
		return engine.UnknownObservation(), fault.New(fault.ChangedIdempotencyContent, "observation identity does not match the prepared identity")
	}
	group, err := GroupName(request.Identity.WorkloadID)
	if err != nil {
		return engine.UnknownObservation(), err
	}
	state, _, err := a.readGroupState(ctx, resourceRoot)
	if err != nil {
		return engine.UnknownObservation(), err
	}
	status, err := a.observer.Status(ctx, group)
	if err != nil {
		return engine.UnknownObservation(), fault.Wrap(fault.DependencyUnavailable, "process-group state is unreachable", err)
	}
	return observationFor(state, status), nil
}

// Logs returns ordered, bounded and redacted chunks. It carries no lifecycle
// authority and fails closed when the redaction source is unavailable.
func (a *Adapter) Logs(ctx context.Context, request reconcile.LogsRequest) (reconcile.LogBatch, error) {
	if err := container.ValidateLogsRequest(request); err != nil {
		return reconcile.LogBatch{}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.LogBatch{}, err
	}
	loaded, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return reconcile.LogBatch{}, err
	}
	if !found {
		return reconcile.LogBatch{}, fault.New(fault.NotFound, "log read requires a prepared activation snapshot")
	}
	if loaded.Identity != request.Identity {
		return reconcile.LogBatch{}, fault.New(fault.ChangedIdempotencyContent, "log request identity does not match the prepared identity")
	}
	values, err := a.redactionSource(ctx, resourceRoot, loaded)
	if err != nil {
		return reconcile.LogBatch{}, err
	}
	group, err := GroupName(request.Identity.WorkloadID)
	if err != nil {
		return reconcile.LogBatch{}, err
	}

	stdoutOffset, stderrOffset := container.DecodeLogCursor(request.AfterOffset)
	positions := map[reconcile.LogStream]int64{
		reconcile.LogStdout: stdoutOffset,
		reconcile.LogStderr: stderrOffset,
	}
	batch := reconcile.LogBatch{}
	sequence := int64(0)
	for _, stream := range []reconcile.LogStream{reconcile.LogStdout, reconcile.LogStderr} {
		offset := positions[stream]
		page, err := a.observer.Logs(ctx, GroupLogsRequest{
			Group:  group,
			Stream: stream,
			Offset: offset,
			Limit:  request.Limit,
		})
		if err != nil {
			return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "process log read is unavailable", err)
		}
		chunk, hasChunk, nextPosition, err := container.BuildLogChunk(container.LogPageInput{
			Stream:        stream,
			Offset:        offset,
			Sequence:      sequence + 1,
			Raw:           page.Data,
			PageTruncated: page.Truncated,
			Values:        values,
			Limit:         request.Limit,
			MayGrow:       page.Truncated && len(page.Data) == request.Limit,
		})
		if err != nil {
			return reconcile.LogBatch{}, err
		}
		positions[stream] = nextPosition
		if !hasChunk {
			continue
		}
		sequence++
		batch.Chunks = append(batch.Chunks, chunk)
	}
	batch.NextOffset = container.EncodeLogCursor(positions[reconcile.LogStdout], positions[reconcile.LogStderr])
	return batch, nil
}

// redactionSource loads the current credential and every declared secret value.
// A missing source fails the whole read closed.
func (a *Adapter) redactionSource(ctx context.Context, resourceRoot string, loaded manifest) ([]string, error) {
	values := make([]string, 0, len(loaded.SecretNames)+1)
	for _, name := range loaded.SecretNames {
		data, err := a.files.ReadFile(ctx, hostPath(resourceRoot, workspace.SecretRoot+"/"+string(name)))
		if err != nil {
			return nil, fault.Wrap(fault.DependencyUnavailable, "redaction source is unavailable; log read refused", err)
		}
		values = append(values, string(data))
	}
	credential, err := a.files.ReadFile(ctx, hostPath(resourceRoot, workspace.ReservedCredentialPath))
	if err != nil {
		return nil, fault.Wrap(fault.DependencyUnavailable, "redaction source is unavailable; log read refused", err)
	}
	values = append(values, string(credential))
	return container.SortRedactionValues(values), nil
}

// Stop requests whole-process-group termination. A signal acknowledgment is
// not confirmation; only affirmative whole-group termination confirms.
func (a *Adapter) Stop(ctx context.Context, request reconcile.StopRequest) (engine.StopEvidence, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return engine.PendingStop("stop request identity is invalid"), err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return engine.PendingStop("stop request workspace is invalid"), err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return engine.PendingStop("stop request identity is invalid"), err
	}
	tombstoned, err := a.tombstoned(ctx, request.Identity.WorkloadID)
	if err != nil {
		return engine.PendingStop("stop unconfirmed: removal tombstone is unreachable"), err
	}
	if tombstoned {
		return engine.PendingStop("stop unconfirmed: workload identity was already removed"),
			fault.New(fault.NotFound, "workload identity was already removed")
	}
	loaded, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return engine.PendingStop("stop unconfirmed: activation snapshot is unreadable"), err
	}
	if !found {
		return engine.PendingStop("stop unconfirmed: workload identity is not prepared"),
			fault.New(fault.NotFound, "workload identity is not prepared")
	}
	if loaded.WorkspaceID != request.WorkspaceID {
		return engine.PendingStop("stop unconfirmed: workspace does not match the prepared workspace"),
			fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
	}
	group, err := GroupName(request.Identity.WorkloadID)
	if err != nil {
		return engine.PendingStop("stop request identity is invalid"), err
	}
	if err := a.executor.SignalGroup(ctx, group); err != nil {
		return engine.PendingStop("stop unconfirmed: process-group signal outcome is unknown"),
			fault.Wrap(fault.UncertainExternalEffect, "process-group signal outcome is unknown", err)
	}
	status, err := a.observer.Status(ctx, group)
	if err != nil {
		return engine.PendingStop("stop unconfirmed: process-group state is unreachable"),
			fault.Wrap(fault.DependencyUnavailable, "process-group state is unreachable", err)
	}
	if status.GroupGone {
		return engine.ConfirmedStop(fmt.Sprintf("process group %q affirmatively terminated", group)), nil
	}
	return engine.PendingStop(fmt.Sprintf("stop pending: process group %q has not affirmatively terminated", group)), nil
}

// Remove is idempotent, identity-scoped and bounded cleanup. It never touches
// the execution workspace and refuses to remove resources while the process
// group may still run.
func (a *Adapter) Remove(ctx context.Context, request reconcile.RemoveRequest) (reconcile.Removal, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.Removal{Detail: "removal request identity is invalid"}, err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return reconcile.Removal{Detail: "removal request workspace is invalid"}, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Removal{Detail: "removal request identity is invalid"}, err
	}
	group, err := GroupName(request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Removal{Detail: "removal request identity is invalid"}, err
	}
	rootExists, err := a.pathExists(ctx, resourceRoot)
	if err != nil {
		return reconcile.Removal{Detail: retainedDetail(group, "resource root is unreachable")}, nil
	}
	tombstoned, err := a.tombstoned(ctx, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Removal{Detail: retainedDetail(group, "removal tombstone is unreachable")}, nil
	}
	if tombstoned {
		if !rootExists {
			return reconcile.Removal{
				Removed: true,
				Detail:  fmt.Sprintf("owned resources for %s were already removed", container.DescribeIdentity(request.Identity)),
			}, nil
		}
		return a.cleanupRoot(ctx, resourceRoot, group)
	}

	loaded, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return reconcile.Removal{Detail: retainedDetail(group, "activation snapshot is unreadable")}, nil
	}
	if !found {
		if !rootExists {
			return reconcile.Removal{
				Removed: true,
				Detail:  fmt.Sprintf("no owned resources exist for %s", container.DescribeIdentity(request.Identity)),
			}, nil
		}
		if err := a.writeTombstone(ctx, request.Identity.WorkloadID); err != nil {
			return reconcile.Removal{Detail: retainedDetail(group, "removal tombstone write failed")}, nil
		}
		return a.cleanupRoot(ctx, resourceRoot, group)
	}
	if loaded.WorkspaceID != request.WorkspaceID {
		return reconcile.Removal{Detail: retainedDetail(group, "workspace does not match the prepared workspace")},
			fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
	}
	if loaded.Identity != request.Identity {
		return reconcile.Removal{Detail: retainedDetail(group, "identity does not match the prepared identity")},
			fault.New(fault.ChangedIdempotencyContent, "identity does not match the prepared identity")
	}

	state, _, err := a.readGroupState(ctx, resourceRoot)
	if err != nil {
		return reconcile.Removal{Detail: retainedDetail(group, "process-group bookkeeping is unreadable")}, nil
	}
	switch state.State {
	case groupStarted:
		status, statusErr := a.observer.Status(ctx, group)
		if statusErr != nil {
			return reconcile.Removal{Detail: retainedDetail(group, "process-group state is unreachable")}, nil
		}
		if !status.GroupGone {
			return reconcile.Removal{Detail: retainedDetail(group, "the process group has not affirmatively terminated")}, nil
		}
		if err := a.executor.RemoveGroup(ctx, group); err != nil {
			return reconcile.Removal{Detail: retainedDetail(group, "process-group bookkeeping removal failed")}, nil
		}
	case groupSpawning, groupUnknown:
		status, statusErr := a.observer.Status(ctx, group)
		if statusErr != nil {
			return reconcile.Removal{Detail: retainedDetail(group, "process-group state is unreachable")}, nil
		}
		switch {
		case status.NeverCreated:
			// Affirmative no-effect evidence: there is no group to signal or
			// remove, so the tombstone and cleanup may proceed.
		case status.GroupGone:
			if err := a.executor.RemoveGroup(ctx, group); err != nil {
				return reconcile.Removal{Detail: retainedDetail(group, "process-group bookkeeping removal failed")}, nil
			}
		default:
			// Present, alive or zero evidence: the group may still run.
			return reconcile.Removal{Detail: retainedDetail(group, "the process-group spawn outcome is unknown")}, nil
		}
	}

	if err := a.writeTombstone(ctx, request.Identity.WorkloadID); err != nil {
		return reconcile.Removal{Detail: retainedDetail(group, "removal tombstone write failed")}, nil
	}
	return a.cleanupRoot(ctx, resourceRoot, group)
}

// cleanupRoot removes only the identity's resource root with bounded retries.
func (a *Adapter) cleanupRoot(ctx context.Context, resourceRoot string, group GroupID) (reconcile.Removal, error) {
	maxAttempts := max(a.cleanup.Attempts, 1)
	attemptsMade := 0
	err := container.RetryWithBackoff(ctx, a.clock, a.cleanup, func(ctx context.Context) error {
		attemptsMade++
		return a.files.RemoveAll(ctx, resourceRoot)
	})
	if err == nil {
		return reconcile.Removal{
			Removed: true,
			Detail:  fmt.Sprintf("removed owned resources and process group %q", group),
		}, nil
	}
	if attemptsMade < maxAttempts {
		return reconcile.Removal{Detail: retainedDetail(group, "cleanup backoff interrupted")}, err
	}
	return reconcile.Removal{Detail: retainedDetail(group, "resource removal failed: "+err.Error())}, nil
}

// pathExists reports whether one host path exists.
func (a *Adapter) pathExists(ctx context.Context, path string) (bool, error) {
	_, err := a.files.Stat(ctx, path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// retainedDetail names the retained process group so pending cleanup is
// attributable.
func retainedDetail(group GroupID, reason string) string {
	return fmt.Sprintf("cleanup pending: retained process group %q: %s", group, reason)
}

// RefreshCredential replaces only the mutable mounted credential. It never
// restarts or re-signals the process group and never revives a removed
// identity.
func (a *Adapter) RefreshCredential(ctx context.Context, request reconcile.CredentialRequest) (reconcile.CredentialRefresh, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.CredentialRefresh{}, err
	}
	if strings.TrimSpace(request.Credential) == "" {
		return reconcile.CredentialRefresh{}, fault.New(fault.InvalidInput, "credential refresh requires a non-empty credential")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	resourceRoot, err := ResourceRoot(a.root, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.CredentialRefresh{}, err
	}
	tombstoned, err := a.tombstoned(ctx, request.Identity.WorkloadID)
	if err != nil {
		return reconcile.CredentialRefresh{}, err
	}
	if tombstoned {
		return reconcile.CredentialRefresh{Detail: "credential refresh refused for a removed identity"},
			fault.New(fault.NotFound, "credential refresh refused for a removed identity")
	}
	loaded, found, err := a.readManifest(ctx, resourceRoot)
	if err != nil {
		return reconcile.CredentialRefresh{}, err
	}
	if !found {
		return reconcile.CredentialRefresh{Detail: "credential refresh requires a prepared identity"},
			fault.New(fault.NotFound, "credential refresh requires a prepared identity")
	}
	if loaded.Identity != request.Identity {
		return reconcile.CredentialRefresh{Detail: "credential refresh identity does not match the prepared identity"},
			fault.New(fault.ChangedIdempotencyContent, "credential refresh identity does not match the prepared identity")
	}
	credentialPath := hostPath(resourceRoot, workspace.ReservedCredentialPath)
	if err := a.files.WriteFile(ctx, credentialPath, []byte(request.Credential), readOnlyMode); err != nil {
		return reconcile.CredentialRefresh{Detail: "credential update outcome is unknown"},
			fault.Wrap(fault.UncertainExternalEffect, "credential update outcome is unknown", err)
	}
	return reconcile.CredentialRefresh{
		Delivered: true,
		Detail:    fmt.Sprintf("replaced credential for %s without restarting the process group", container.DescribeIdentity(request.Identity)),
	}, nil
}
