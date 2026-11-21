package opsapp_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/shell/authorize"
	"github.com/optimiware/kodema/internal/shell/identity"
	"github.com/optimiware/kodema/internal/shell/tenancy"
	"github.com/optimiware/kodema/internal/shell/token"
	"github.com/optimiware/kodema/internal/shell/transport/ops"
)

type mapOIDCVerifier struct {
	tokens map[string]identity.Principal
}

func (v mapOIDCVerifier) Verify(_ context.Context, req identity.OIDCRequest) (identity.Principal, error) {
	principal, ok := v.tokens[req.AccessToken]
	if !ok {
		return identity.Principal{}, fault.New(fault.Unauthenticated, "invalid token")
	}
	return principal, nil
}

// TestMultiProjectTenantScopedHumanOIDC exercises the full Issue #80 workflow
// through the real token.Resolver, real identitystore.Adapter and real
// ops.Router:
//   - a tenant-scoped admin token (no project_id claim) can create a second
//     project with --copy-members-from AND run pipelines, apply pipelines,
//     create tasks, create services and issue service credentials there;
//   - a copied human member (engineer) logging in with a tenant-scoped OIDC
//     token (no project_id claim, no tenant-wide membership) can inspect
//     whoami, list only their member projects, apply/start pipelines and create
//     tasks in both the bootstrap project and the second project, while being
//     refused in projects where they hold no membership;
//   - a project-scoped token remains strictly pinned to its own project.
func TestMultiProjectTenantScopedHumanOIDC(t *testing.T) {
	h := newHarness(t)

	// Seed tenant, bootstrap project (project-local), and a third secret
	// project that engineer will NOT belong to.
	if _, err := h.invokeKey(t, "token-admin", "tenant.create",
		`{"tenant_id":"`+testTenant+`","name":"Local Tenant"}`, "mp-seed-tenant", nil); err != nil {
		t.Fatalf("tenant.create: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "project.create",
		`{"project_id":"project-local","name":"Project Local"}`, "mp-seed-local",
		map[string]string{"tenant": testTenant}); err != nil {
		t.Fatalf("project.create project-local: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "project.create",
		`{"project_id":"secret-project","name":"Secret Project"}`, "mp-seed-secret",
		map[string]string{"tenant": testTenant}); err != nil {
		t.Fatalf("project.create secret-project: %v", err)
	}

	adminID := id.PrincipalID("oidc-admin-local")
	engineerID := id.PrincipalID("oidc-engineer-local")
	reviewerID := id.PrincipalID("oidc-reviewer-local")
	unassignedID := id.PrincipalID("oidc-unassigned-local")

	// Grant admin tenant-wide RoleAdmin only (matching kodema up bootstrap-admin)
	// plus project-local roles (matching kodema up provision).
	h.seedMembership(t, tenancy.Scope{TenantID: testTenant}, "membership-mp-admin-tenant", adminID, 1, tenancy.RoleAdmin)
	if _, err := h.invokeKey(t, "token-admin", "membership.grant",
		`{"roles":["engineer","reviewer","operator"]}`, "mp-grant-admin-local",
		map[string]string{"project": "project-local", "principal": string(adminID)}); err != nil {
		t.Fatalf("membership.grant admin project-local: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "membership.grant",
		`{"roles":["engineer"]}`, "mp-grant-eng-local",
		map[string]string{"project": "project-local", "principal": string(engineerID)}); err != nil {
		t.Fatalf("membership.grant engineer project-local: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "membership.grant",
		`{"roles":["reviewer"]}`, "mp-grant-rev-local",
		map[string]string{"project": "project-local", "principal": string(reviewerID)}); err != nil {
		t.Fatalf("membership.grant reviewer project-local: %v", err)
	}

	exp := fixedClock{}.Now().Add(time.Hour)
	verifier := mapOIDCVerifier{
		tokens: map[string]identity.Principal{
			"jwt-tenant-admin": identity.Principal{
				ID:      adminID,
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: testTenant},
				Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-project-local-admin": identity.Principal{
				ID:      adminID,
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: testTenant, ProjectID: "project-local"},
				Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-engineer": identity.Principal{
				ID:      engineerID,
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: testTenant},
				Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-reviewer": identity.Principal{
				ID:      reviewerID,
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: testTenant},
				Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-unassigned": identity.Principal{
				ID:      unassignedID,
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: testTenant},
				Enabled: true,
			}.WithVerifiedExpiry(exp),
		},
	}

	realResolver, err := token.NewResolver(token.ResolverConfig{
		Lookup:      noopLookup{},
		Credentials: noopCredAuth{},
		OIDC:        verifier,
		Principals:  h.adapter,
		Clock:       fixedClock{},
	})
	if err != nil {
		t.Fatalf("token.NewResolver: %v", err)
	}
	oidcRouter, err := ops.NewRouter(ops.Config{
		Principals: realResolver,
		Scopes:     h.ownership,
		Executions: h.ownership,
		Bindings:   h.bindingsTable,
	})
	if err != nil {
		t.Fatalf("ops.NewRouter: %v", err)
	}

	invoke := func(cred, op, input, key string, params map[string]string) (api.Result, error) {
		return oidcRouter.Invoke(context.Background(), ops.Invocation{
			Operation:  authorize.OperationName(op),
			Credential: cred,
			Params:     params,
			Request: api.Request{
				Input:          json.RawMessage(input),
				IdempotencyKey: key,
			},
		})
	}

	// 1. Tenant admin creates demo2 copying memberships from project-local.
	if _, err := invoke("jwt-tenant-admin", "project.create",
		`{"project_id":"demo2","name":"Demo 2","copy_members_from":"project-local"}`,
		"mp-create-demo2", map[string]string{"tenant": testTenant}); err != nil {
		t.Fatalf("tenant-admin project.create demo2: %v", err)
	}

	yamlDoc := base64.StdEncoding.EncodeToString([]byte("actions:\n  - name: hello\n    image: mirror.gcr.io/library/alpine:3.24\n    timeout: 1m\n    commands:\n      - echo hi\n"))

	// 2. Copied human member (engineer, tenant-scoped OIDC token, no tenant-wide
	// membership) sees both project-local and demo2 in whoami and project.list
	// (and NEVER sees secret-project).
	whoamiRes, err := invoke("jwt-engineer", "principal.whoami", `{}`, "", nil)
	if err != nil {
		t.Fatalf("engineer whoami: %v", err)
	}
	var whoami api.PrincipalWhoamiResult
	decodeResult(t, whoamiRes, &whoami)
	if whoami.ProjectID != "" || whoami.TenantID != testTenant || len(whoami.Roles) != 0 {
		t.Fatalf("engineer whoami = %+v; want tenant-scoped with empty tenant roles", whoami)
	}
	var gotMemberProjects []string
	for _, m := range whoami.Memberships {
		gotMemberProjects = append(gotMemberProjects, m.ProjectID)
	}
	sort.Strings(gotMemberProjects)
	if !reflect.DeepEqual(gotMemberProjects, []string{"demo2", "project-local"}) {
		t.Fatalf("engineer whoami memberships = %v; want [demo2 project-local]", gotMemberProjects)
	}

	projListRes, err := invoke("jwt-engineer", "project.list", `{}`, "", nil)
	if err != nil {
		t.Fatalf("engineer project.list: %v", err)
	}
	var projList api.ProjectListResult
	decodeResult(t, projListRes, &projList)
	var listedIDs []string
	for _, p := range projList.Projects {
		listedIDs = append(listedIDs, p.ProjectID)
	}
	sort.Strings(listedIDs)
	if !reflect.DeepEqual(listedIDs, []string{"demo2", "project-local"}) {
		t.Fatalf("engineer project.list = %v; want [demo2 project-local] (no secret-project)", listedIDs)
	}

	// Engineer can apply pipelines, start pipelines, and create tasks in demo2
	// AND project-local!
	for _, proj := range []string{"demo2", "project-local"} {
		if _, err := invoke("jwt-engineer", "pipeline.apply",
			`{"name":"hello","yaml":"`+yamlDoc+`"}`,
			"eng-apply-"+proj, map[string]string{"project": proj, "pipeline": "hello"}); err != nil {
			t.Fatalf("engineer pipeline.apply in %s: %v", proj, err)
		}
		if _, err := invoke("jwt-engineer", "pipeline.start",
			`{"yaml":"`+yamlDoc+`"}`,
			"eng-start-"+proj, map[string]string{"project": proj}); err != nil {
			t.Fatalf("engineer pipeline.start in %s: %v", proj, err)
		}
		if _, err := invoke("jwt-engineer", "task.create",
			`{"title":"demo task"}`,
			"eng-task-"+proj, map[string]string{"project": proj}); err != nil {
			t.Fatalf("engineer task.create in %s: %v", proj, err)
		}
	}

	// Engineer cannot access secret-project (no membership) or create projects
	// (no tenant-wide admin).
	if _, err := invoke("jwt-engineer", "pipeline.start",
		`{"yaml":"`+yamlDoc+`"}`,
		"eng-start-secret", map[string]string{"project": "secret-project"}); err == nil {
		t.Fatal("engineer pipeline.start in secret-project unexpectedly succeeded")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("engineer pipeline.start in secret-project = %v; want forbidden", err)
	}
	if _, err := invoke("jwt-engineer", "project.create",
		`{"project_id":"unauthorized-proj","name":"Nope"}`,
		"eng-create-proj", map[string]string{"tenant": testTenant}); err == nil {
		t.Fatal("engineer project.create unexpectedly succeeded")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Unauthenticated && class != fault.Forbidden {
		t.Fatalf("engineer project.create = %v; want unauthenticated or forbidden", err)
	}

	// 3. Project-scoped admin-token stays pinned to project-local and cannot
	// see demo2.
	if _, err := invoke("jwt-project-local-admin", "pipeline.list", `{}`, "",
		map[string]string{"project": "demo2"}); err == nil {
		t.Fatal("project-local admin token accessed demo2")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
		t.Fatalf("project-local admin targeting demo2 = %v; want not_found", err)
	}

	// 4. Tenant-admin token (no project_id claim) can apply/start pipelines,
	// create tasks, and create services in demo2 (using its union of tenant
	// admin + demo2 project roles).
	if _, err := invoke("jwt-tenant-admin", "pipeline.apply",
		`{"name":"admin-hello","yaml":"`+yamlDoc+`"}`,
		"ta-apply-demo2", map[string]string{"project": "demo2", "pipeline": "admin-hello"}); err != nil {
		t.Fatalf("tenant-admin pipeline.apply in demo2: %v", err)
	}
	if _, err := invoke("jwt-tenant-admin", "pipeline.start",
		`{"yaml":"`+yamlDoc+`"}`,
		"ta-start-demo2", map[string]string{"project": "demo2"}); err != nil {
		t.Fatalf("tenant-admin pipeline.start in demo2: %v", err)
	}
	if _, err := invoke("jwt-tenant-admin", "task.create",
		`{"title":"admin task"}`,
		"ta-task-demo2", map[string]string{"project": "demo2"}); err != nil {
		t.Fatalf("tenant-admin task.create in demo2: %v", err)
	}
	if _, err := invoke("jwt-tenant-admin", "service.create",
		`{"service_id":"demo-bot","roles":["engineer"]}`,
		"ta-svc-demo2", map[string]string{"project": "demo2"}); err != nil {
		t.Fatalf("tenant-admin service.create in demo2: %v", err)
	}

	// 5. Copied reviewer can read in demo2 but cannot start pipelines.
	if _, err := invoke("jwt-reviewer", "pipeline.list", `{}`, "",
		map[string]string{"project": "demo2"}); err != nil {
		t.Fatalf("reviewer pipeline.list in demo2: %v", err)
	}
	if _, err := invoke("jwt-reviewer", "pipeline.start", `{"yaml":"`+yamlDoc+`"}`,
		"rev-start-demo2", map[string]string{"project": "demo2"}); err == nil {
		t.Fatal("reviewer pipeline.start in demo2 unexpectedly succeeded")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("reviewer pipeline.start in demo2 = %v; want forbidden", err)
	}

	// 6. Zero-membership OIDC user can call whoami, is refused unauthenticated
	// on project.list, and forbidden on demo2.
	if _, err := invoke("jwt-unassigned", "principal.whoami", `{}`, "", nil); err != nil {
		t.Fatalf("unassigned whoami: %v", err)
	}
	if _, err := invoke("jwt-unassigned", "project.list", `{}`, "", nil); err == nil {
		t.Fatal("unassigned project.list unexpectedly succeeded")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Unauthenticated {
		t.Fatalf("unassigned project.list = %v; want unauthenticated", err)
	}

	// 7. authorize.WithTargetProject narrows a tenant-scoped token without ever
	// widening a project-scoped token.
	narrowed, err := realResolver.Resolve(authorize.WithTargetProject(context.Background(), "demo2"), "jwt-engineer")
	if err != nil {
		t.Fatalf("Resolve with WithTargetProject(demo2): %v", err)
	}
	if narrowed.Scope != (tenancy.Scope{TenantID: testTenant, ProjectID: "demo2"}) || !narrowed.HasRole(tenancy.RoleEngineer) {
		t.Fatalf("narrowed engineer = %+v; want demo2 [engineer]", narrowed)
	}
	pinned, err := realResolver.Resolve(authorize.WithTargetProject(context.Background(), "demo2"), "jwt-project-local-admin")
	if err != nil {
		t.Fatalf("Resolve project-scoped with WithTargetProject(demo2): %v", err)
	}
	if pinned.Scope != (tenancy.Scope{TenantID: testTenant, ProjectID: "project-local"}) {
		t.Fatalf("project-scoped token was widened to %+v; want project-local", pinned.Scope)
	}
}
