// Fresh-only workspace provisioning for the Kernel workspace port.
//
// The Kernel calls engine.Workspace.Retain before it commits an activation
// (internal/kernel/engine/reconcile.go activate), so the activating action is
// still pending when Retain runs. Both the Podman and Kubernetes runtime
// adapters deliberately refuse an absent volume or PersistentVolumeClaim: a
// missing workspace of an execution that already ran is evidence of loss,
// never permission to recreate empty scratch state. The composition therefore
// provisions the deterministic owned resource before Retain, but only for a
// never-activated workspace.
//
// The engine.Workspace.Retain request carries only the workspace id, so
// the decorator takes one app-owned fact source, WorkspaceActivation. Its real
// implementation reads durable persisted execution/action facts, so it survives
// a controller restart and never infers activation from resource presence or
// process memory.
package server

import (
	"context"
	"errors"
	"strings"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	runtimepodman "github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/engine"
	kernelengine "github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	driverkube "github.com/optimiware/kodema/internal/server/drivers/kube"
	podmandriver "github.com/optimiware/kodema/internal/server/drivers/podman"
)

// VolumeProvisioner is the one external effect the fresh-only Podman decorator
// adds: the idempotent creation of the deterministic owned workspace volume. It
// is satisfied by the real *drivers/podman.Libpod client.
type VolumeProvisioner interface {
	CreateVolume(ctx context.Context, volume runtimepodman.Volume) (runtimepodman.Volume, error)
}

var _ VolumeProvisioner = (*podmandriver.Libpod)(nil)

// ClaimProvisioner is the one external effect the fresh-only Kubernetes
// decorator adds: the idempotent creation of the deterministic owned workspace
// claim. It is satisfied by the real *drivers/kube.Cluster.
type ClaimProvisioner interface {
	CreateClaim(ctx context.Context, spec driverkube.ClaimSpec) (runtimekube.Claim, error)
}

var _ ClaimProvisioner = (*driverkube.Cluster)(nil)

// WorkspacePVC is the trusted workspace claim template.
//
// Every value comes from composition configuration and is never derived from a
// request, a principal or a rendered workload. The template namespace must
// equal the runtime adapter's configured tenant namespace; the
// engine.Workspace seam exposes no namespace, so that pairing is an operator
// configuration invariant, while the durable owning tenant is checked by the
// decorator for every fresh workspace.
type WorkspacePVC struct {
	// Namespace is the tenant namespace that owns the claim. Required.
	Namespace string
	// TenantID is the tenant ownership label value. Required and validated.
	TenantID id.TenantID
	// StorageClass is the requested StorageClass. Empty selects the cluster's
	// default StorageClass.
	StorageClass string
	// Size is the requested storage quantity, for example "64Mi". Required.
	Size string
	// AccessMode is one of the four Kubernetes PVC access modes. Required.
	AccessMode string
}

// workspaceClaimAccessModes is the closed set of PVC access modes accepted by
// the driver's CreateClaim. It mirrors the driver-owned set; a template value
// outside it would be refused by the driver after the tenant fact was read, so
// it is refused at construction and validation instead.
var workspaceClaimAccessModes = map[string]bool{
	"ReadWriteOnce":    true,
	"ReadOnlyMany":     true,
	"ReadWriteMany":    true,
	"ReadWriteOncePod": true,
}

// validateWorkspacePVC refuses an incomplete or unusable trusted template
// before any adapter or external effect exists. It never renders a configured
// value.
func validateWorkspacePVC(template WorkspacePVC) error {
	if strings.TrimSpace(template.Namespace) == "" {
		return fault.New(fault.InvalidInput, "workspace claim template requires a namespace")
	}
	if _, err := id.ParseTenantID(string(template.TenantID)); err != nil {
		return fault.New(fault.InvalidInput, "workspace claim template tenant id is invalid")
	}
	if template.StorageClass != "" && strings.TrimSpace(template.StorageClass) == "" {
		return fault.New(fault.InvalidInput, "workspace claim template storage class must not be blank")
	}
	if strings.TrimSpace(template.Size) == "" {
		return fault.New(fault.InvalidInput, "workspace claim template requires a storage size")
	}
	if strings.TrimSpace(template.AccessMode) == "" {
		return fault.New(fault.InvalidInput, "workspace claim template requires an access mode")
	}
	if !workspaceClaimAccessModes[template.AccessMode] {
		return fault.New(fault.InvalidInput, "workspace claim template access mode is not supported")
	}
	return nil
}

// WorkspaceActivation reports whether the workspace's execution had already
// activated an action before the activation that triggered this Retain.
//
// false means the Kernel is about to commit the first activation of a fresh
// queued workspace: no action of the execution has left pending yet. The
// implementation must read durable persisted state: process memory loses the
// fact on restart and volume/claim presence cannot distinguish a fresh
// workspace from a lost one.
type WorkspaceActivation interface {
	PreviouslyActivated(ctx context.Context, workspaceID id.WorkspaceID) (bool, error)
	// OwningTenant reports the durable tenant that owns the workspace's
	// execution. It is the persisted executions.tenant_id, never the request,
	// the configured template or process memory. Any missing or unreadable
	// fact is an error, never an empty tenant.
	OwningTenant(ctx context.Context, workspaceID id.WorkspaceID) (id.TenantID, error)
}

// provisioningWorkspace decorates the Kernel workspace port with fresh-only
// workspace provisioning (Podman volumes or Kubernetes claims). Status and
// Release delegate unchanged.
type provisioningWorkspace struct {
	inner          kernelengine.Workspace
	activation     WorkspaceActivation
	expectedTenant id.TenantID
	provision      func(ctx context.Context, workspaceID id.WorkspaceID) error

	// provisioner and claim retain the configured backend dependency and
	// optional PVC template for composition inspection.
	provisioner any
	claim       WorkspacePVC
}

var _ kernelengine.Workspace = (*provisioningWorkspace)(nil)

// newProvisioningWorkspace validates every dependency and returns a fresh-only
// Podman volume provisioning decorator. An incomplete decorator is refused at
// construction rather than failing open later.
func newProvisioningWorkspace(inner kernelengine.Workspace, provisioner VolumeProvisioner, activation WorkspaceActivation) (*provisioningWorkspace, error) {
	if inner == nil {
		return nil, fault.New(fault.InvalidInput, "provisioning workspace requires a workspace adapter")
	}
	if provisioner == nil {
		return nil, fault.New(fault.InvalidInput, "provisioning workspace requires a volume provisioner")
	}
	if activation == nil {
		return nil, fault.New(fault.InvalidInput, "provisioning workspace requires an activation fact source")
	}
	return &provisioningWorkspace{
		inner:       inner,
		activation:  activation,
		provisioner: provisioner,
		provision: func(ctx context.Context, workspaceID id.WorkspaceID) error {
			volumeName, err := runtimepodman.WorkspaceVolumeName(workspaceID)
			if err != nil {
				return err
			}
			volume := runtimepodman.Volume{
				Name:   volumeName,
				Labels: map[string]string{runtimepodman.LabelWorkspace: string(workspaceID)},
			}
			if _, err := provisioner.CreateVolume(ctx, volume); err != nil {
				if errors.Is(err, podmandriver.ErrAmbiguous) {
					return fault.Wrap(fault.UncertainExternalEffect,
						"workspace volume provisioning outcome is unknown", err)
				}
				return err
			}
			return nil
		},
	}, nil
}

// newKubeProvisioningWorkspace validates every dependency and the trusted
// template and returns a fresh-only Kubernetes claim provisioning decorator.
// An incomplete decorator is refused at construction rather than failing open
// later.
//
// The decorator never waits for the claim to become Bound. A
// WaitForFirstConsumer StorageClass only binds the claim when the workload Job
// runs, so waiting during Retain would deadlock the activation that triggered
// Retain.
func newKubeProvisioningWorkspace(inner kernelengine.Workspace, provisioner ClaimProvisioner, activation WorkspaceActivation, claim WorkspacePVC) (*provisioningWorkspace, error) {
	if inner == nil {
		return nil, fault.New(fault.InvalidInput, "claim provisioning workspace requires a workspace adapter")
	}
	if provisioner == nil {
		return nil, fault.New(fault.InvalidInput, "claim provisioning workspace requires a claim provisioner")
	}
	if activation == nil {
		return nil, fault.New(fault.InvalidInput, "claim provisioning workspace requires an activation fact source")
	}
	if err := validateWorkspacePVC(claim); err != nil {
		return nil, err
	}
	return &provisioningWorkspace{
		inner:          inner,
		activation:     activation,
		expectedTenant: claim.TenantID,
		provisioner:    provisioner,
		claim:          claim,
		provision: func(ctx context.Context, workspaceID id.WorkspaceID) error {
			spec := driverkube.ClaimSpec{
				Namespace:    claim.Namespace,
				TenantID:     claim.TenantID,
				WorkspaceID:  workspaceID,
				StorageClass: claim.StorageClass,
				Size:         claim.Size,
				AccessMode:   claim.AccessMode,
			}
			if _, err := provisioner.CreateClaim(ctx, spec); err != nil {
				if errors.Is(err, driverkube.ErrAmbiguous) {
					return fault.Wrap(fault.UncertainExternalEffect,
						"workspace claim provisioning outcome is unknown", err)
				}
				return err
			}
			return nil
		},
	}, nil
}

// Status delegates unchanged and never provisions.
func (workspace *provisioningWorkspace) Status(ctx context.Context, workspaceID id.WorkspaceID) (kernelengine.WorkspaceObservation, error) {
	return workspace.inner.Status(ctx, workspaceID)
}

// Release delegates unchanged and never provisions.
func (workspace *provisioningWorkspace) Release(ctx context.Context, workspaceID id.WorkspaceID) error {
	return workspace.inner.Release(ctx, workspaceID)
}

// Retain provisions the deterministic owned workspace resource exactly once
// for a fresh workspace, then delegates retention. An already-activated
// workspace is never provisioned: a missing resource surfaces from the
// adapter's Retain as not_found, which is the intended workspace-loss
// evidence.
//
// When an expected tenant is configured (Kubernetes), the durable owning
// tenant of the workspace's execution must match it; any mismatch is forbidden
// before any external effect. A cancelled context performs no external effect
// and reads no fact. A failed or uncertain provisioning is returned without
// delegating Retain, and no resource, label, tenant, or template detail is
// rendered into an error.
func (workspace *provisioningWorkspace) Retain(ctx context.Context, workspaceID id.WorkspaceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	previouslyActivated, err := workspace.activation.PreviouslyActivated(ctx, workspaceID)
	if err != nil {
		// An unknown activation fact fails closed: never provision, never
		// retain, and never guess that the workspace is fresh.
		return err
	}
	if previouslyActivated {
		return workspace.inner.Retain(ctx, workspaceID)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if workspace.expectedTenant != "" {
		tenant, err := workspace.activation.OwningTenant(ctx, workspaceID)
		if err != nil {
			// An unknown tenant fact fails closed exactly like an unknown
			// activation fact.
			return err
		}
		if tenant != workspace.expectedTenant {
			// Fixed text: neither tenant value is rendered.
			return fault.New(fault.Forbidden,
				"workspace claim template tenant does not own the workspace execution")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := workspace.provision(ctx, workspaceID); err != nil {
		return err
	}
	return workspace.inner.Retain(ctx, workspaceID)
}

// workspaceActivation is the real WorkspaceActivation over the durable Kernel
// state seam. The executions row is the durable workspace attribution
// (executions.workspace_id is derived from the execution identity and written
// by Start), and the ordered action rows carry the durable activation facts.
//
// The Kernel calls Retain before it commits the triggering activation, so the
// triggering action is still pending. Any action that has left pending
// therefore means the workspace was activated before; an execution whose
// actions are all pending is about to activate for the first time. Any missing,
// ambiguous or unreadable fact is an error, never a fresh-workspace inference.
type workspaceActivation struct {
	controller store.Controller
}

var _ WorkspaceActivation = (*workspaceActivation)(nil)

// newWorkspaceActivation builds the durable activation fact source.
func newWorkspaceActivation(controller store.Controller) (*workspaceActivation, error) {
	if controller == nil {
		return nil, fault.New(fault.InvalidInput, "workspace activation fact requires a store controller")
	}
	return &workspaceActivation{controller: controller}, nil
}

// PreviouslyActivated reports whether any action of the workspace's execution
// left the pending state before the current activation. The activation that
// triggered this Retain is not committed yet, so one non-pending action is
// enough to prove an earlier activation.
func (activation *workspaceActivation) PreviouslyActivated(ctx context.Context, workspaceID id.WorkspaceID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := id.ParseWorkspaceID(string(workspaceID)); err != nil {
		return false, err
	}
	executionID, err := activation.executionForWorkspace(ctx, workspaceID)
	if err != nil {
		return false, err
	}
	return activation.hasEarlierActivation(ctx, executionID)
}

// OwningTenant reports the durable owning tenant of the workspace's execution.
// The answer is scoped to the workspace attribution, so one workspace's tenant
// never leaks into another's. A missing execution, an ambiguous attribution or
// an unreadable/invalid tenant column is an error, never an empty tenant.
func (activation *workspaceActivation) OwningTenant(ctx context.Context, workspaceID id.WorkspaceID) (id.TenantID, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, err := id.ParseWorkspaceID(string(workspaceID)); err != nil {
		return "", err
	}
	row, err := activation.executionRowForWorkspace(ctx, workspaceID)
	if err != nil {
		return "", err
	}
	tenantText, err := rowText(row, "tenant_id")
	if err != nil {
		return "", err
	}
	tenant, err := id.ParseTenantID(tenantText)
	if err != nil {
		return "", fault.Wrap(fault.DependencyUnavailable, "workspace execution tenant is invalid", err)
	}
	return tenant, nil
}

// executionForWorkspace resolves the single durable execution that owns the
// workspace. The derivation makes the attribution unique; a missing or
// duplicated attribution is unknown state, never a fresh workspace.
func (activation *workspaceActivation) executionForWorkspace(ctx context.Context, workspaceID id.WorkspaceID) (id.ExecutionID, error) {
	row, err := activation.executionRowForWorkspace(ctx, workspaceID)
	if err != nil {
		return "", err
	}
	executionID, err := rowText(row, "id")
	if err != nil {
		return "", err
	}
	if _, err := id.ParseExecutionID(executionID); err != nil {
		return "", fault.Wrap(fault.DependencyUnavailable, "workspace execution identity is invalid", err)
	}
	return id.ExecutionID(executionID), nil
}

// executionRowForWorkspace reads the single durable executions row that owns
// the workspace. A missing row is not_found; more than one row, or a scanner
// that reports a further page, is unknown state.
func (activation *workspaceActivation) executionRowForWorkspace(ctx context.Context, workspaceID id.WorkspaceID) (store.Row, error) {
	page, err := activation.controller.Scan(ctx, store.ControllerScan{
		Table:   "executions",
		Equals:  map[string]any{"workspace_id": string(workspaceID)},
		OrderBy: []store.Order{{Column: "id"}},
		Limit:   2})
	if err != nil {
		return store.Row{}, fault.Wrap(fault.DependencyUnavailable, "workspace activation state is unreachable", err)
	}
	if len(page.Rows) == 0 {
		return store.Row{}, fault.New(fault.NotFound, "workspace has no persisted execution")
	}
	if len(page.Rows) > 1 || page.HasMore {
		return store.Row{}, fault.New(fault.DependencyUnavailable, "workspace activation state is ambiguous")
	}
	return page.Rows[0], nil
}

// hasEarlierActivation reads the execution's actions in durable index order. It
// stops as soon as a non-pending action proves an earlier activation. An
// execution with no action rows is unknown state, never a fresh workspace.
func (activation *workspaceActivation) hasEarlierActivation(ctx context.Context, executionID id.ExecutionID) (bool, error) {
	query := store.ScanQuery{
		Table:   "actions",
		Equals:  map[string]any{"execution_id": string(executionID)},
		OrderBy: []store.Order{{Column: "index"}, {Column: "id"}},
		Limit:   store.MaxQueryLimit,
	}
	actions := 0
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		page, err := activation.controller.Scan(ctx, store.ControllerScan{ScanQuery: query})
		if err != nil {
			return false, fault.Wrap(fault.DependencyUnavailable, "workspace activation state is unreachable", err)
		}
		for _, row := range page.Rows {
			stateText, err := rowText(row, "state")
			if err != nil {
				return false, err
			}
			state := engine.ActionState(stateText)
			if !state.Valid() {
				return false, fault.New(fault.DependencyUnavailable, "workspace activation state has an unknown action state")
			}
			if state != engine.ActionPending {
				return true, nil
			}
			actions++
		}
		if !page.HasMore {
			break
		}
		query.After = page.Next
	}
	if actions == 0 {
		return false, fault.New(fault.DependencyUnavailable, "workspace activation state has no action")
	}
	return false, nil
}
