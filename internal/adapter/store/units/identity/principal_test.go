package identity_test

import (
	"context"
	"reflect"
	"testing"

	shellidentity "github.com/optimiware/kodema/internal/control/identity"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func TestReadPrincipalHumanFromMembership(t *testing.T) {
	h := newHarness(t)
	scope := projectScope("tenant-a", "project-a")
	h.seedMembership(scope, "membership-admin", "principal-human", 3, tenancy.RoleAdmin, tenancy.RoleReviewer)
	principal, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: scope, PrincipalID: "principal-human"})
	if err != nil {
		t.Fatalf("ReadPrincipal: %v", err)
	}
	if principal.Kind != shellidentity.KindHuman || !principal.Enabled || principal.Version != 3 {
		t.Fatalf("principal = %+v; want enabled human version 3", principal)
	}
	if !reflect.DeepEqual(principal.Roles, []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleReviewer}) {
		t.Fatalf("roles = %v; want admin and reviewer", principal.Roles)
	}
}

// TestReadPrincipalTenantWideMembershipCoversProjectScope states the union
// truth: a tenant-wide row is a UNION contribution to a project scope, not a
// fallback that applies only when no project row exists. A human with no
// project row is still a project-scope human carrying the tenant-wide roles.
func TestReadPrincipalTenantWideMembershipCoversProjectScope(t *testing.T) {
	h := newHarness(t)
	h.seedMembership(tenantScope("tenant-a"), "membership-admin", "principal-human", 1, tenancy.RoleAdmin)
	scope := projectScope("tenant-a", "project-a")
	principal, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: scope, PrincipalID: "principal-human"})
	if err != nil {
		t.Fatalf("ReadPrincipal: %v", err)
	}
	if principal.Kind != shellidentity.KindHuman || principal.Scope != scope {
		t.Fatalf("principal = %+v; want a human in the requested project scope", principal)
	}
	if !reflect.DeepEqual(principal.Roles, []tenancy.Role{tenancy.RoleAdmin}) {
		t.Fatalf("roles = %v; want the tenant-wide admin contribution", principal.Roles)
	}
	if principal.Version != 1 {
		t.Fatalf("principal version = %d; want the observed tenant-wide version 1", principal.Version)
	}
}

func TestReadPrincipalService(t *testing.T) {
	h := newHarness(t)
	scope := projectScope("tenant-a", "project-a")
	h.seedService(scope, "service-a", 4, string(serviceStateEnabledForTest), nil, nil)
	principal, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: scope, PrincipalID: "service-a"})
	if err != nil {
		t.Fatalf("ReadPrincipal: %v", err)
	}
	if principal.Kind != shellidentity.KindService || !principal.Enabled || principal.Version != 4 {
		t.Fatalf("principal = %+v; want enabled service version 4", principal)
	}
	if len(principal.Roles) != 1 || principal.Roles[0] != tenancy.RoleEngineer {
		t.Fatalf("roles = %v; want engineer", principal.Roles)
	}
}

func TestReadPrincipalDisabledServiceIsNotEnabled(t *testing.T) {
	h := newHarness(t)
	scope := projectScope("tenant-a", "project-a")
	h.seedService(scope, "service-a", 2, "disabled", nil, nil)
	principal, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: scope, PrincipalID: "service-a"})
	if err != nil {
		t.Fatalf("ReadPrincipal: %v", err)
	}
	if principal.Enabled {
		t.Fatalf("principal = %+v; want a disabled service fact", principal)
	}
}

func TestReadPrincipalExecution(t *testing.T) {
	h := newHarness(t)
	scope := projectScope("tenant-a", "project-a")
	h.seedExecution(scope, "execution-a", "principal-exec", 5, "running", nil, nil)
	principal, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: scope, PrincipalID: "principal-exec"})
	if err != nil {
		t.Fatalf("ReadPrincipal: %v", err)
	}
	if principal.Kind != shellidentity.KindExecution || !principal.Enabled || principal.Version != 5 {
		t.Fatalf("principal = %+v; want a running execution principal version 5", principal)
	}
	if principal.ExecutionID != "execution-a" {
		t.Fatalf("execution id = %q; want execution-a", principal.ExecutionID)
	}
	if len(principal.Roles) != 0 {
		t.Fatalf("execution roles = %v; want none", principal.Roles)
	}
}

func TestReadPrincipalMissingIsNotFound(t *testing.T) {
	h := newHarness(t)
	_, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: projectScope("tenant-a", "project-a"), PrincipalID: "principal-missing"})
	requireFaultClass(t, err, fault.NotFound)
}

func TestReadPrincipalCrossTenantInvisible(t *testing.T) {
	h := newHarness(t)
	h.seedMembership(tenantScope("tenant-b"), "membership-b", "principal-b", 1, tenancy.RoleViewer)
	_, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: tenantScope("tenant-a"), PrincipalID: "principal-b"})
	requireFaultClass(t, err, fault.NotFound)
}

func TestReadPrincipalAmbiguousIdentityRefused(t *testing.T) {
	h := newHarness(t)
	scope := projectScope("tenant-a", "project-a")
	h.seedService(scope, "principal-shared", 1, string(serviceStateEnabledForTest), nil, nil)
	h.seedMembership(scope, "membership-shared", "principal-shared", 1, tenancy.RoleViewer)
	_, err := h.adapter.ReadPrincipal(context.Background(), shellidentity.PrincipalRequest{Scope: scope, PrincipalID: "principal-shared"})
	requireFaultClass(t, err, fault.Forbidden)
}

func TestHasProjectMembership(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Seed two tenants with projects.
	h.seedTenantRow("tenant-a")
	h.seedTenantRow("tenant-b")
	h.seed(tenantScope("tenant-a"), "projects", "project-1", map[string]any{"name": "Project 1", "version": int64(1), "created_at": testNow})
	h.seed(tenantScope("tenant-a"), "projects", "project-2", map[string]any{"name": "Project 2", "version": int64(1), "created_at": testNow})
	h.seed(tenantScope("tenant-b"), "projects", "project-b", map[string]any{"name": "Project B", "version": int64(1), "created_at": testNow})

	// No memberships yet -> false.
	has, err := h.adapter.HasProjectMembership(ctx, id.TenantID("tenant-a"), id.PrincipalID("principal-eng"))
	if err != nil {
		t.Fatalf("HasProjectMembership empty: %v", err)
	}
	if has {
		t.Fatal("HasProjectMembership = true for zero-membership user; want false")
	}

	// Membership in another tenant is invisible in tenant-a.
	h.seedMembership(projectScope("tenant-b", "project-b"), "membership-b", "principal-other", 1, tenancy.RoleEngineer)
	has, err = h.adapter.HasProjectMembership(ctx, id.TenantID("tenant-a"), id.PrincipalID("principal-other"))
	if err != nil {
		t.Fatalf("HasProjectMembership cross-tenant: %v", err)
	}
	if has {
		t.Fatal("HasProjectMembership = true for foreign-tenant member; want false")
	}

	// Active project membership in tenant-a/project-2 -> true.
	h.seedMembership(projectScope("tenant-a", "project-2"), "membership-eng-2", "principal-eng", 1, tenancy.RoleEngineer)
	has, err = h.adapter.HasProjectMembership(ctx, id.TenantID("tenant-a"), id.PrincipalID("principal-eng"))
	if err != nil {
		t.Fatalf("HasProjectMembership member: %v", err)
	}
	if !has {
		t.Fatal("HasProjectMembership = false for active project member; want true")
	}

	// Revoking the last project membership immediately flips HasProjectMembership back to false.
	if _, err := h.store.Execute(ctx, projectScope("tenant-a", "project-2"), store.Unit{
		Writes: []store.Write{{Table: "memberships", ID: "membership-eng-2", Delete: true, ExpectVersion: 1}},
	}); err != nil {
		t.Fatalf("delete membership: %v", err)
	}
	has, err = h.adapter.HasProjectMembership(ctx, id.TenantID("tenant-a"), id.PrincipalID("principal-eng"))
	if err != nil {
		t.Fatalf("HasProjectMembership after revoke: %v", err)
	}
	if has {
		t.Fatal("HasProjectMembership = true after revocation; want false")
	}

	// An ambiguous identity colliding with a service row in the project fails closed.
	h.seedService(projectScope("tenant-a", "project-1"), "principal-shared", 1, string(serviceStateEnabledForTest), nil, nil)
	h.seedMembership(projectScope("tenant-a", "project-1"), "membership-shared", "principal-shared", 1, tenancy.RoleEngineer)
	_, err = h.adapter.HasProjectMembership(ctx, id.TenantID("tenant-a"), id.PrincipalID("principal-shared"))
	requireFaultClass(t, err, fault.Forbidden)

	// Invalid tenant or principal ID is refused with invalid_input.
	_, err = h.adapter.HasProjectMembership(ctx, "", id.PrincipalID("principal-eng"))
	requireFaultClass(t, err, fault.InvalidInput)
	_, err = h.adapter.HasProjectMembership(ctx, id.TenantID("tenant-a"), "")
	requireFaultClass(t, err, fault.InvalidInput)
}

// serviceStateEnabledForTest mirrors the declared enabled state without
// importing the service package into every fixture.
const serviceStateEnabledForTest = "enabled"
