package identity

import (
	"context"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	shellidentity "github.com/optimiware/kodema/internal/shell/identity"
	"github.com/optimiware/kodema/internal/shell/store"
	"github.com/optimiware/kodema/internal/shell/tenancy"
)

// Compile-time pin: the current-principal read seam is implemented by one
// adapter.
var _ shellidentity.PrincipalReader = (*Adapter)(nil)

// ReadPrincipal resolves one principal's current committed kind, roles,
// enablement, scope and version.
//
// Resolution is fail-closed and deterministic: an execution row whose
// principal_id is the requested principal wins, then a service account with
// that identity, then a scoped membership. A principal that matches more than
// one committed kind is refused rather than resolved by precedence, because a
// service identity and a human identity that collide must never grant the
// other kind's authority.
func (a *Adapter) ReadPrincipal(ctx context.Context, request shellidentity.PrincipalRequest) (shellidentity.Principal, error) {
	if err := request.Scope.Validate(); err != nil {
		return shellidentity.Principal{}, err
	}
	if _, err := id.ParsePrincipalID(string(request.PrincipalID)); err != nil {
		return shellidentity.Principal{}, err
	}
	matches := make([]shellidentity.Principal, 0, 3)
	if request.Scope.ProjectScoped() {
		execution, found, err := a.findExecutionByPrincipal(ctx, request.Scope, request.PrincipalID)
		if err != nil {
			return shellidentity.Principal{}, err
		}
		if found {
			principal, err := decodeExecutionPrincipal(request, execution)
			if err != nil {
				return shellidentity.Principal{}, err
			}
			matches = append(matches, principal)
		}
	}
	service, found, _, err := a.readNullableScope(ctx, request.Scope, "services", string(request.PrincipalID))
	if err != nil {
		return shellidentity.Principal{}, err
	}
	if found {
		principal, err := decodeServicePrincipal(request, service)
		if err != nil {
			return shellidentity.Principal{}, err
		}
		matches = append(matches, principal)
	}
	rows, err := a.readMembershipRows(ctx, request.Scope, request.PrincipalID)
	if err != nil {
		return shellidentity.Principal{}, err
	}
	var tenantWide, project *tenancy.Membership
	if rows.TenantWide.Membership != nil {
		tenantWide = rows.TenantWide.Membership
	}
	if rows.Project.Membership != nil {
		project = rows.Project.Membership
	}
	roles, applicable := tenancy.EffectiveRoles(request.Scope, tenantWide, project)
	if applicable {
		matches = append(matches, shellidentity.Principal{
			ID:      request.PrincipalID,
			Kind:    shellidentity.KindHuman,
			Scope:   request.Scope,
			Roles:   roles,
			Enabled: true,
			Version: membershipVersion(rows, request.Scope),
		})
	}
	switch len(matches) {
	case 0:
		return shellidentity.Principal{}, fault.New(fault.NotFound, "no committed principal exists in the requested scope")
	case 1:
		return matches[0], nil
	default:
		return shellidentity.Principal{}, fault.New(fault.Forbidden, "the principal identity matches more than one committed principal kind")
	}
}

// HasProjectMembership reports whether principalID currently holds at least one
// active project-scoped human membership (with at least one role) inside
// tenantID. It scans only projects owned by tenantID and confirms any matching
// project through ReadPrincipal so an identity colliding with a service or
// execution row in that project is never accepted as a human membership.
func (a *Adapter) HasProjectMembership(ctx context.Context, tenantID id.TenantID, principalID id.PrincipalID) (bool, error) {
	tenantScope := tenancy.Scope{TenantID: tenantID}
	if err := tenantScope.Validate(); err != nil {
		return false, err
	}
	if _, err := id.ParsePrincipalID(string(principalID)); err != nil {
		return false, fault.Wrap(fault.InvalidInput, "principal id is invalid", err)
	}
	var after string
	for {
		page, err := a.scanner.Query(ctx, tenantScope, store.ScanQuery{
			Table:   "projects",
			OrderBy: []store.Order{{Column: "id"}},
			After:   after,
			Limit:   store.MaxQueryLimit,
		})
		if err != nil {
			return false, err
		}
		for _, row := range page.Rows {
			projectID, err := rowString(row, "id")
			if err != nil {
				return false, err
			}
			projectScope := tenancy.Scope{
				TenantID:  tenantID,
				ProjectID: id.ProjectID(projectID),
			}
			_, membership, found, err := a.membershipRowInScope(ctx, projectScope, principalID)
			if err != nil {
				return false, err
			}
			if !found || len(membership.Roles) == 0 {
				continue
			}
			current, err := a.ReadPrincipal(ctx, shellidentity.PrincipalRequest{
				Scope:       projectScope,
				PrincipalID: principalID,
			})
			if err != nil {
				if class, ok := fault.ClassOf(err); ok && class == fault.NotFound {
					continue
				}
				return false, err
			}
			if current.Kind == shellidentity.KindHuman && current.Enabled && len(current.Roles) > 0 {
				return true, nil
			}
		}
		if !page.HasMore {
			return false, nil
		}
		after = page.Next
	}
}

// decodeExecutionPrincipal builds an execution principal from its execution
// row. An execution is enabled only while its container is running.
func decodeExecutionPrincipal(request shellidentity.PrincipalRequest, row store.Row) (shellidentity.Principal, error) {
	state, err := rowString(row, "state")
	if err != nil {
		return shellidentity.Principal{}, err
	}
	version, err := rowUint(row, "version")
	if err != nil {
		return shellidentity.Principal{}, err
	}
	execution, err := rowString(row, "id")
	if err != nil {
		return shellidentity.Principal{}, err
	}
	return shellidentity.Principal{
		ID:          request.PrincipalID,
		Kind:        shellidentity.KindExecution,
		Scope:       request.Scope,
		Enabled:     state == executionStateRunning,
		Version:     version,
		ExecutionID: id.ExecutionID(execution),
	}, nil
}

// decodeServicePrincipal builds a service principal from its service row. Admin
// is never service-assignable, so it is filtered even from a corrupt row and the
// resulting principal stays structurally valid.
func decodeServicePrincipal(request shellidentity.PrincipalRequest, row store.Row) (shellidentity.Principal, error) {
	state, err := rowString(row, "state")
	if err != nil {
		return shellidentity.Principal{}, err
	}
	roles, err := decodeRolesFromRow(row)
	if err != nil {
		return shellidentity.Principal{}, err
	}
	version, err := rowUint(row, "version")
	if err != nil {
		return shellidentity.Principal{}, err
	}
	return shellidentity.Principal{
		ID:      request.PrincipalID,
		Kind:    shellidentity.KindService,
		Scope:   request.Scope,
		Roles:   tenancy.AssignableServiceRoles(roles),
		Enabled: state == serviceStateEnabled,
		Version: version,
	}, nil
}
