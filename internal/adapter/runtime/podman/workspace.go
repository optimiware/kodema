// Retained workspace behavior for the Podman runtime adapter.
//
// The workspace port is separate from the workload port because workspace
// ownership differs from workload ownership. The Shell provisions the named
// volume before admission; this adapter never creates it. Availability is only
// ever reported for the deterministic volume that carries this workspace's
// ownership label, and a missing or foreign volume is evidence, never
// permission to reconstruct scratch state.
//
// Durable workspace retention lives in PostgreSQL rather than in host marker
// files: Retain verifies that the owned volume exists, and Release removes the
// volume only when it is owned and unreferenced by any container in libpod. An
// absent volume is idempotent success, while an uncertain removal returns an
// error instead of fabricating success.
package podman

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// ownsWorkspaceVolume reports whether the inspected volume is the one this
// adapter/installation owns for the workspace. A deterministic name alone is
// not enough: the workspace ownership label must name exactly this workspace.
func ownsWorkspaceVolume(volume Volume, volumeName string, workspace id.WorkspaceID) bool {
	if volume.Name != "" && volume.Name != volumeName {
		return false
	}
	return volume.Labels[LabelWorkspace] == string(workspace)
}

// Status reports availability without changing it. Only an existing volume
// that carries this workspace's ownership label is available; a missing or
// foreign volume is unavailable evidence and is never recreated.
func (a *Adapter) Status(ctx context.Context, workspace id.WorkspaceID) (reconcile.WorkspaceObservation, error) {
	observation := reconcile.WorkspaceObservation{WorkspaceID: workspace}
	if err := container.ValidateWorkspaceID(workspace); err != nil {
		observation.Detail = "workspace status request workspace is invalid"
		return observation, err
	}
	if err := ctx.Err(); err != nil {
		observation.Detail = "workspace status cancelled before any effect"
		return observation, fault.Wrap(fault.DependencyUnavailable, "workspace status cancelled before any effect", err)
	}

	unlock := a.lockForWorkspace(workspace)
	defer unlock()

	volumeName, err := WorkspaceVolumeName(workspace)
	if err != nil {
		observation.Detail = "workspace volume name is invalid"
		return observation, err
	}
	volume, err := a.libpod.InspectVolume(ctx, volumeName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			observation.Detail = fmt.Sprintf(
				"retained workspace volume %q is missing; a missing workspace is never recreated", volumeName)
			return observation, nil
		}
		observation.Detail = fmt.Sprintf("workspace volume %q state is unreachable", volumeName)
		return observation, fault.Wrap(fault.DependencyUnavailable, "workspace volume state is unreachable", err)
	}
	if !ownsWorkspaceVolume(volume, volumeName, workspace) {
		observation.Detail = fmt.Sprintf(
			"volume %q is not owned by workspace %q; a foreign or unlabelled volume is never treated as the workspace",
			volumeName, workspace)
		return observation, nil
	}
	observation.Available = true
	observation.Detail = fmt.Sprintf(
		"retained workspace volume %q is present and owned by workspace %q", volumeName, workspace)
	return observation, nil
}

// Retain verifies that the owned workspace volume exists. It is idempotent and
// never creates a missing volume: a missing or foreign volume is a failure.
func (a *Adapter) Retain(ctx context.Context, workspace id.WorkspaceID) error {
	if err := container.ValidateWorkspaceID(workspace); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "workspace retention cancelled before any effect", err)
	}

	unlock := a.lockForWorkspace(workspace)
	defer unlock()

	volumeName, err := WorkspaceVolumeName(workspace)
	if err != nil {
		return err
	}
	volume, err := a.libpod.InspectVolume(ctx, volumeName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return fault.New(fault.NotFound,
				"retained workspace volume is missing; retention never recreates it")
		}
		return fault.Wrap(fault.DependencyUnavailable, "workspace volume state is unreachable", err)
	}
	if !ownsWorkspaceVolume(volume, volumeName, workspace) {
		return fault.New(fault.Forbidden, "workspace volume is not owned by this workspace")
	}
	return nil
}

// Release removes the workspace volume only when it is owned and unreferenced
// by any container. An absent volume is idempotent success, while an uncertain
// removal returns an error instead of fabricating success.
func (a *Adapter) Release(ctx context.Context, workspace id.WorkspaceID) error {
	if err := container.ValidateWorkspaceID(workspace); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "workspace release cancelled before any effect", err)
	}

	unlock := a.lockForWorkspace(workspace)
	defer unlock()

	volumeName, err := WorkspaceVolumeName(workspace)
	if err != nil {
		return err
	}
	volume, inspectErr := a.libpod.InspectVolume(ctx, volumeName)
	switch {
	case inspectErr == nil:
		if !ownsWorkspaceVolume(volume, volumeName, workspace) {
			return fault.New(fault.Forbidden,
				"workspace volume is not owned by this workspace; a foreign volume is never released")
		}
		if err := a.refuseReleaseWhileReferenced(ctx, workspace); err != nil {
			return err
		}
		if err := a.libpod.RemoveVolume(ctx, volumeName); err != nil {
			if !errors.Is(err, ErrNotFound) {
				return fault.Wrap(fault.UncertainExternalEffect,
					"workspace volume removal outcome is unknown", err)
			}
		}
	case errors.Is(inspectErr, ErrNotFound):
		// Already absent: idempotent release succeeds.
	default:
		return fault.Wrap(fault.DependencyUnavailable, "workspace volume state is unreachable", inspectErr)
	}
	return nil
}

// refuseReleaseWhileReferenced refuses release while any container in libpod
// still carries this workspace's ownership label.
func (a *Adapter) refuseReleaseWhileReferenced(ctx context.Context, workspace id.WorkspaceID) error {
	containers, err := a.libpod.ListContainers(ctx, map[string]string{LabelWorkspace: string(workspace)})
	if err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "workspace container references are unreachable", err)
	}
	if len(containers) > 0 {
		names := make([]string, 0, len(containers))
		for _, c := range containers {
			names = append(names, c.Name)
		}
		sort.Strings(names)
		return fault.New(fault.Forbidden, fmt.Sprintf(
			"workspace is still referenced by container(s) %s", strings.Join(names, ", ")))
	}
	return nil
}

// Adapter implements the declared reconcile.Workspace port in addition to the
// runtime workload port.
var _ reconcile.Workspace = (*Adapter)(nil)
