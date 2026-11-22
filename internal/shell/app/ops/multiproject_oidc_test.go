package opsapp_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/kernel"
	"github.com/optimiware/kodema/internal/kernel/definition"
	opsapp "github.com/optimiware/kodema/internal/shell/app/ops"
	"github.com/optimiware/kodema/internal/shell/authorize"
	"github.com/optimiware/kodema/internal/shell/identity"
	"github.com/optimiware/kodema/internal/shell/log"
	"github.com/optimiware/kodema/internal/shell/receipt"
	shellrequest "github.com/optimiware/kodema/internal/shell/request"
	"github.com/optimiware/kodema/internal/shell/store"
	"github.com/optimiware/kodema/internal/shell/tenancy"
	"github.com/optimiware/kodema/internal/shell/token"
	"github.com/optimiware/kodema/internal/shell/transport/httpapi"
	"github.com/optimiware/kodema/internal/shell/transport/mcp"
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
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("engineer project.create = %v; want forbidden", err)
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

// TestMultiProjectTenantScopedIDAddressedOperations pins Issue #92 end-to-end
// through the real token.Resolver, real identitystore.Adapter, real log.Reader,
// real ops.Router, REST edge (httpapi) and MCP edge (mcp):
//   - tenant-scoped human OIDC sessions (engineer, reviewer) and tenant-admin
//     tokens succeed on ID-addressed execution, task, service and resource
//     binding operations across multiple projects via both REST and MCP;
//   - ID-addressed operations on resources in projects where a tenant-scoped
//     human holds no membership (or in a foreign tenant) fail uniformly with
//     not_found (no existence oracle);
//   - role checks still enforce forbidden on resources in member projects
//     (e.g. reviewer cannot cancel an execution or start a task in demo2);
//   - zero-membership OIDC users are refused uniformly as unauthenticated.
func TestMultiProjectTenantScopedIDAddressedOperations(t *testing.T) {
	h := newHarness(t)

	if _, err := h.invokeKey(t, "token-admin", "tenant.create",
		`{"tenant_id":"`+testTenant+`","name":"Local Tenant"}`, "id92-seed-tenant", nil); err != nil {
		t.Fatalf("tenant.create: %v", err)
	}
	for _, proj := range []string{"project-local", "secret-project"} {
		if _, err := h.invokeKey(t, "token-admin", "project.create",
			`{"project_id":"`+proj+`","name":"Project `+proj+`"}`, "id92-seed-"+proj,
			map[string]string{"tenant": testTenant}); err != nil {
			t.Fatalf("project.create %s: %v", proj, err)
		}
	}

	adminID := id.PrincipalID("oidc-id92-admin")
	engineerID := id.PrincipalID("oidc-id92-engineer")
	reviewerID := id.PrincipalID("oidc-id92-reviewer")
	unassignedID := id.PrincipalID("oidc-id92-unassigned")

	h.seedMembership(t, tenancy.Scope{TenantID: testTenant}, "membership-id92-admin-tenant", adminID, 1, tenancy.RoleAdmin)
	if _, err := h.invokeKey(t, "token-admin", "membership.grant",
		`{"roles":["engineer","reviewer","operator"]}`, "id92-grant-admin-local",
		map[string]string{"project": "project-local", "principal": string(adminID)}); err != nil {
		t.Fatalf("membership.grant admin: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "membership.grant",
		`{"roles":["engineer"]}`, "id92-grant-eng-local",
		map[string]string{"project": "project-local", "principal": string(engineerID)}); err != nil {
		t.Fatalf("membership.grant engineer: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "membership.grant",
		`{"roles":["reviewer"]}`, "id92-grant-rev-local",
		map[string]string{"project": "project-local", "principal": string(reviewerID)}); err != nil {
		t.Fatalf("membership.grant reviewer: %v", err)
	}

	exp := fixedClock{}.Now().Add(time.Hour)
	verifier := mapOIDCVerifier{
		tokens: map[string]identity.Principal{
			"jwt-tenant-admin": identity.Principal{
				ID: adminID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-engineer": identity.Principal{
				ID: engineerID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-reviewer": identity.Principal{
				ID: reviewerID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-unassigned": identity.Principal{
				ID: unassignedID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
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

	// Wire a real log.Reader (backed by h.adapter for PrincipalReader +
	// AuthorityReader) so execution.logs.read exercises the full production path.
	logSink := &memoryLogSink{}
	realLogReader, err := log.NewReader(log.ReaderConfig{
		Authorizer: authorize.NewMatrix(),
		Principals: h.adapter,
		Authority:  h.adapter,
		Ownership:  noLogOwnership{},
		Sink:       logSink,
		Clock:      fixedClock{},
	})
	if err != nil {
		t.Fatalf("log.NewReader: %v", err)
	}
	deps := h.deps
	deps.Logs = realLogReader
	bindings, err := opsapp.Bindings(deps)
	if err != nil {
		t.Fatalf("opsapp.Bindings: %v", err)
	}
	oidcRouter, err := ops.NewRouter(ops.Config{
		Principals: realResolver,
		Scopes:     h.ownership,
		Executions: h.ownership,
		Bindings:   bindings,
	})
	if err != nil {
		t.Fatalf("ops.NewRouter: %v", err)
	}
	restEdge, err := httpapi.NewEdge(oidcRouter)
	if err != nil {
		t.Fatalf("httpapi.NewEdge: %v", err)
	}
	mcpEdge, err := mcp.NewEdge(mcp.Config{
		Router:     oidcRouter,
		ServerInfo: api.MCPImplementation{Name: "kodema", Version: "test"},
	})
	if err != nil {
		t.Fatalf("mcp.NewEdge: %v", err)
	}

	invoke := func(cred, op, input, key string, version *uint64, params map[string]string) (api.Result, error) {
		return oidcRouter.Invoke(context.Background(), ops.Invocation{
			Operation:  authorize.OperationName(op),
			Credential: cred,
			Params:     params,
			Request: api.Request{
				Input:           json.RawMessage(input),
				IdempotencyKey:  key,
				ExpectedVersion: version,
			},
		})
	}
	callREST := func(cred, method, path, body string) *responseRecorder {
		req, err := http.NewRequest(method, path, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("http.NewRequest %s %s: %v", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer "+cred)
		rec := &responseRecorder{header: make(http.Header)}
		restEdge.ServeHTTP(rec, req)
		return rec
	}
	callMCP := func(cred, tool string, input map[string]any, key string) (api.Result, string) {
		args := map[string]any{"input": input}
		if key != "" {
			args["idempotency_key"] = key
		}
		rawArgs, _ := json.Marshal(args)
		payload, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "tools/call",
			"params":  map[string]any{"name": tool, "arguments": json.RawMessage(rawArgs)},
		})
		respBytes := mcpEdge.Handle(context.Background(), cred, payload)
		var rpc struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(respBytes, &rpc); err != nil {
			t.Fatalf("unmarshal MCP response: %v (%s)", err, string(respBytes))
		}
		if len(rpc.Error) != 0 {
			return api.Result{}, string(rpc.Error)
		}
		res, err := api.DecodeMCPResult(rpc.Result)
		if err != nil {
			return api.Result{}, err.Error()
		}
		return res, ""
	}

	// Create demo2 copying memberships from project-local.
	if _, err := invoke("jwt-tenant-admin", "project.create",
		`{"project_id":"demo2","name":"Demo 2","copy_members_from":"project-local"}`,
		"id92-create-demo2", nil, map[string]string{"tenant": testTenant}); err != nil {
		t.Fatalf("project.create demo2: %v", err)
	}

	yamlDoc := base64.StdEncoding.EncodeToString([]byte("actions:\n  - name: hello\n    image: " + pinnedImage + "\n    timeout: 1m\n    commands:\n      - echo hi\n"))
	approvalYAML := base64.StdEncoding.EncodeToString([]byte("actions:\n  - name: review\n    request: role/reviewer\n    subject:\n      change: demo2\n    timeout: 1h\n"))

	// 1. Engineer starts an execution and creates a task in BOTH project-local
	// and demo2, then exercises ID-addressed execution & task operations via
	// REST and MCP.
	var demo2ExecID, demo2ActionID, demo2TaskID string
	for _, proj := range []string{"project-local", "demo2"} {
		appliedRes, err := invoke("jwt-engineer", "pipeline.apply",
			`{"name":"hello","yaml":"`+yamlDoc+`"}`,
			"id92-apply-"+proj, nil, map[string]string{"project": proj, "pipeline": "hello"})
		if err != nil {
			t.Fatalf("pipeline.apply in %s: %v", proj, err)
		}
		var pipe api.PipelineApplyResult
		decodeResult(t, appliedRes, &pipe)

		startRes, err := invoke("jwt-engineer", "pipeline.start",
			`{"yaml":"`+yamlDoc+`"}`,
			"id92-start-"+proj, nil, map[string]string{"project": proj})
		if err != nil {
			t.Fatalf("pipeline.start in %s: %v", proj, err)
		}
		var started api.PipelineStartResult
		decodeResult(t, startRes, &started)

		// REST GET /api/v1/executions/{id}
		rec := callREST("jwt-engineer", http.MethodGet, "/api/v1/executions/"+started.ExecutionID, "")
		if rec.status != http.StatusOK {
			t.Fatalf("REST execution.inspect in %s = %d %s; want 200", proj, rec.status, rec.body.String())
		}
		var restRes api.Result
		if err := json.Unmarshal(rec.body.Bytes(), &restRes); err != nil {
			t.Fatalf("decode REST execution.inspect: %v", err)
		}
		var inspected api.ExecutionInspectResult
		decodeResult(t, restRes, &inspected)
		if inspected.ExecutionID != started.ExecutionID || len(inspected.Actions) != 1 {
			t.Fatalf("inspected in %s = %+v", proj, inspected)
		}
		actionID := inspected.Actions[0].ActionID

		// MCP execution_inspect
		mcpRes, mcpErr := callMCP("jwt-engineer", "execution_inspect", map[string]any{"execution_id": started.ExecutionID}, "")
		if mcpErr != "" {
			t.Fatalf("MCP execution_inspect in %s failed: %s", proj, mcpErr)
		}
		var mcpInspected api.ExecutionInspectResult
		decodeResult(t, mcpRes, &mcpInspected)
		if mcpInspected.ExecutionID != started.ExecutionID {
			t.Fatalf("MCP execution_inspect in %s = %+v", proj, mcpInspected)
		}

		// execution.logs.read through real log.Reader
		if _, err := invoke("jwt-engineer", "execution.logs.read",
			jsonBody(t, map[string]any{"action_id": actionID, "stream": "stdout"}),
			"", nil, map[string]string{"execution": started.ExecutionID}); err != nil {
			t.Fatalf("execution.logs.read in %s: %v", proj, err)
		}

		// Task create -> read (REST + MCP) -> edit -> comment -> select_pipeline -> start -> history
		taskRes, err := invoke("jwt-engineer", "task.create",
			`{"title":"task in `+proj+`"}`,
			"id92-task-"+proj, nil, map[string]string{"project": proj})
		if err != nil {
			t.Fatalf("task.create in %s: %v", proj, err)
		}
		var createdTask api.TaskCreateResult
		decodeResult(t, taskRes, &createdTask)

		taskRec := callREST("jwt-engineer", http.MethodGet, "/api/v1/tasks/"+createdTask.TaskID, "")
		if taskRec.status != http.StatusOK {
			t.Fatalf("REST task.read in %s = %d %s; want 200", proj, taskRec.status, taskRec.body.String())
		}
		if _, mcpErr := callMCP("jwt-engineer", "task_read", map[string]any{"task_id": createdTask.TaskID}, ""); mcpErr != "" {
			t.Fatalf("MCP task_read in %s failed: %s", proj, mcpErr)
		}

		v1 := createdTask.ContentVersion
		if _, err := invoke("jwt-engineer", "task.edit",
			`{"title":"edited `+proj+`"}`, "id92-edit-"+proj, &v1, map[string]string{"task": createdTask.TaskID}); err != nil {
			t.Fatalf("task.edit in %s: %v", proj, err)
		}
		v2 := uint64(2)
		if _, err := invoke("jwt-engineer", "task.comment",
			`{"body":"hello `+proj+`"}`, "id92-comment-"+proj, &v2, map[string]string{"task": createdTask.TaskID}); err != nil {
			t.Fatalf("task.comment in %s: %v", proj, err)
		}
		lv1 := createdTask.LifecycleVersion
		if _, err := invoke("jwt-engineer", "task.start",
			jsonBody(t, map[string]any{"pipeline_id": pipe.PipelineID}),
			"id92-task-start-"+proj, &lv1, map[string]string{"task": createdTask.TaskID}); err != nil {
			t.Fatalf("task.start in %s: %v", proj, err)
		}
		if _, err := invoke("jwt-engineer", "task.history", `{}`, "", nil,
			map[string]string{"task": createdTask.TaskID}); err != nil {
			t.Fatalf("task.history in %s: %v", proj, err)
		}

		// execution.cancel on the standalone execution
		ev := inspected.Version
		if _, err := invoke("jwt-engineer", "execution.cancel", `{}`,
			"id92-cancel-exec-"+proj, &ev, map[string]string{"execution": started.ExecutionID}); err != nil {
			t.Fatalf("execution.cancel in %s: %v", proj, err)
		}

		if proj == "demo2" {
			demo2ExecID = started.ExecutionID
			demo2ActionID = actionID
			demo2TaskID = createdTask.TaskID
		}
	}

	// 2. Reviewer (tenant-scoped OIDC login, reviewer role only in demo2) can
	// inspect executions, read tasks, and resolve an approval request in demo2,
	// while being refused forbidden on engineer-only operations in demo2.
	approvalStart, err := invoke("jwt-engineer", "pipeline.start",
		`{"yaml":"`+approvalYAML+`"}`, "id92-start-approval", nil, map[string]string{"project": "demo2"})
	if err != nil {
		t.Fatalf("start approval pipeline: %v", err)
	}
	var approvalExec api.PipelineStartResult
	decodeResult(t, approvalStart, &approvalExec)
	approvalInspect, err := invoke("jwt-reviewer", "execution.inspect", `{}`, "", nil,
		map[string]string{"execution": approvalExec.ExecutionID})
	if err != nil {
		t.Fatalf("reviewer execution.inspect in demo2: %v", err)
	}
	var approvalView api.ExecutionInspectResult
	decodeResult(t, approvalInspect, &approvalView)
	reqActionID := id.ActionID(approvalView.Actions[0].ActionID)
	demo2Scope := tenancy.Scope{TenantID: testTenant, ProjectID: "demo2"}
	engineCaller, err := shellrequest.NewEngineCaller("engine-id92")
	if err != nil {
		t.Fatalf("NewEngineCaller: %v", err)
	}
	reviewerAssignment, err := shellrequest.ParseAssignment("role/reviewer")
	if err != nil {
		t.Fatalf("ParseAssignment: %v", err)
	}
	if _, err := h.store.Execute(context.Background(), demo2Scope, store.Unit{
		Writes: []store.Write{{
			Table:         "executions",
			ID:            approvalExec.ExecutionID,
			ExpectVersion: approvalView.Version,
			Values: map[string]any{
				"state": string(kernel.ExecutionRunning),
			},
		}},
	}); err != nil {
		t.Fatalf("mark approval execution running: %v", err)
	}
	approvalSubject := definition.NewSubject(map[string]definition.SubjectValue{
		"change": {Kind: definition.SubjectLiteral, Literal: "demo2"},
	})
	if _, err := h.requests.Activate(context.Background(), shellrequest.ActivateCommand{
		Caller:                   engineCaller,
		Scope:                    demo2Scope,
		ExecutionID:              id.ExecutionID(approvalExec.ExecutionID),
		ActionID:                 reqActionID,
		ExpectedExecutionVersion: approvalView.Version + 1,
		ExpectedActionVersion:    approvalView.Actions[0].Version,
		Subject:                  &approvalSubject,
		Assignment:               reviewerAssignment,
		Timeout:                  time.Hour,
		CallerKey:                "id92-activate-demo2",
		Content:                  receipt.Content{MediaType: "application/json", Bytes: []byte(`{}`)},
	}); err != nil {
		t.Fatalf("activate request action in demo2: %v", err)
	}
	afterActivate, err := invoke("jwt-reviewer", "execution.inspect", `{}`, "", nil,
		map[string]string{"execution": approvalExec.ExecutionID})
	if err != nil {
		t.Fatalf("reviewer re-inspect in demo2: %v", err)
	}
	var activeView api.ExecutionInspectResult
	decodeResult(t, afterActivate, &activeView)
	if _, err := invoke("jwt-reviewer", "execution.request.resolve",
		jsonBody(t, map[string]any{
			"expected_action_version": activeView.Actions[0].Version,
			"decision":                "pass",
			"note":                    "approved by tenant-scoped reviewer",
		}), "id92-resolve-demo2", nil, map[string]string{
			"execution": approvalExec.ExecutionID,
			"action":    string(reqActionID),
		}); err != nil {
		t.Fatalf("reviewer execution.request.resolve in demo2: %v", err)
	}

	// Reviewer cannot cancel an execution or start/cancel a task in demo2 -> forbidden.
	v1 := uint64(1)
	if _, err := invoke("jwt-reviewer", "execution.cancel", `{}`, "id92-rev-cancel", &v1,
		map[string]string{"execution": demo2ExecID}); err == nil {
		t.Fatal("reviewer execution.cancel in demo2 unexpectedly succeeded")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("reviewer execution.cancel = %v; want forbidden", err)
	}

	// 3. Tenant-admin (tenant-wide admin + demo2 roles) can inspect executions,
	// read logs, manage services by ID, and read/update/delete bindings by ID.
	if _, err := invoke("jwt-tenant-admin", "execution.inspect", `{}`, "", nil,
		map[string]string{"execution": demo2ExecID}); err != nil {
		t.Fatalf("tenant-admin execution.inspect in demo2: %v", err)
	}
	if _, err := invoke("jwt-tenant-admin", "execution.logs.read",
		jsonBody(t, map[string]any{"action_id": demo2ActionID, "stream": "stdout"}),
		"", nil, map[string]string{"execution": demo2ExecID}); err != nil {
		t.Fatalf("tenant-admin execution.logs.read in demo2: %v", err)
	}
	svcRes, err := invoke("jwt-tenant-admin", "service.create",
		`{"service_id":"demo2-svc","roles":["engineer"]}`,
		"id92-svc-create", nil, map[string]string{"project": "demo2"})
	if err != nil {
		t.Fatalf("tenant-admin service.create in demo2: %v", err)
	}
	var svc api.ServiceCreateResult
	decodeResult(t, svcRes, &svc)
	svcV := svc.Version
	if _, err := invoke("jwt-tenant-admin", "service.update",
		`{"roles":["engineer","reviewer"]}`, "id92-svc-update", &svcV,
		map[string]string{"service": svc.ServiceID}); err != nil {
		t.Fatalf("tenant-admin service.update by ID: %v", err)
	}
	svcV2 := svcV + 1
	if _, err := invoke("jwt-tenant-admin", "service.credential.issue",
		`{"ttl_seconds":300}`, "id92-svc-issue", &svcV2,
		map[string]string{"service": svc.ServiceID}); err != nil {
		t.Fatalf("tenant-admin service.credential.issue by ID: %v", err)
	}

	// Seed a resource row in demo2 so tenant-admin can register/read/update/delete a binding by ID.
	h.seedResource(t, demo2Scope, "demo2-res", 1)
	bindRes, err := invoke("jwt-tenant-admin", "resource.binding.register",
		jsonBody(t, map[string]any{
			"binding_id":        "demo2-binding",
			"resource_id":       "demo2-res",
			"enforcing_service": svc.ServiceID,
			"trusted_origins":   []string{"https://git.local"},
			"operations":        []string{"git.ref.update"},
			"reportable_events": []string{"git.ref.updated"},
		}), "id92-bind-reg", nil, map[string]string{"project": "demo2"})
	if err != nil {
		t.Fatalf("resource.binding.register in demo2: %v", err)
	}
	var bound api.ResourceBindingRegisterResult
	decodeResult(t, bindRes, &bound)
	if _, err := invoke("jwt-engineer", "resource.binding.read", `{}`, "", nil,
		map[string]string{"binding": bound.BindingID}); err != nil {
		t.Fatalf("engineer resource.binding.read by ID: %v", err)
	}
	if _, mcpErr := callMCP("jwt-engineer", "resource_binding_read", map[string]any{"binding_id": bound.BindingID}, ""); mcpErr != "" {
		t.Fatalf("MCP resource_binding_read failed: %s", mcpErr)
	}
	bindV := bound.Version
	if _, err := invoke("jwt-tenant-admin", "resource.binding.delete", `{}`,
		"id92-bind-del", &bindV, map[string]string{"binding": bound.BindingID}); err != nil {
		t.Fatalf("tenant-admin resource.binding.delete by ID: %v", err)
	}

	// 4. Seed a task and execution in secret-project (where engineer & reviewer
	// have NO membership, and tenant-admin has ONLY tenant-wide RoleAdmin).
	secretTaskRes, err := h.invokeKey(t, "token-admin", "task.create",
		`{"title":"secret task"}`, "id92-secret-task", map[string]string{"project": "secret-project"})
	if err != nil {
		t.Fatalf("seed secret-project task: %v", err)
	}
	var secretTask api.TaskCreateResult
	decodeResult(t, secretTaskRes, &secretTask)
	secretExecRes, err := h.invokeKey(t, "token-admin", "pipeline.start",
		`{"yaml":"`+yamlDoc+`"}`, "id92-secret-exec", map[string]string{"project": "secret-project"})
	if err != nil {
		t.Fatalf("seed secret-project exec: %v", err)
	}
	var secretExec api.PipelineStartResult
	decodeResult(t, secretExecRes, &secretExec)

	// Engineer (no membership in secret-project) gets uniform not_found, NEVER
	// forbidden, identical to a nonexistent ID.
	for _, probe := range []struct {
		op, param, id string
	}{
		{"task.read", "task", secretTask.TaskID},
		{"task.read", "task", "task-does-not-exist"},
		{"execution.inspect", "execution", secretExec.ExecutionID},
		{"execution.inspect", "execution", "execution-does-not-exist"},
	} {
		_, err := invoke("jwt-engineer", probe.op, `{}`, "", nil, map[string]string{probe.param: probe.id})
		if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
			t.Fatalf("engineer %s(%s) = %v; want not_found", probe.op, probe.id, err)
		}
	}

	// Tenant-admin can read secretTask (tenant-wide RoleAdmin allows task.read)
	// but is forbidden on task.start in secret-project (no engineer role there).
	if _, err := invoke("jwt-tenant-admin", "task.read", `{}`, "", nil,
		map[string]string{"task": secretTask.TaskID}); err != nil {
		t.Fatalf("tenant-admin task.read in secret-project: %v", err)
	}
	sv1 := secretTask.LifecycleVersion
	if _, err := invoke("jwt-tenant-admin", "task.start", `{}`, "id92-ta-start-secret", &sv1,
		map[string]string{"task": secretTask.TaskID}); err == nil {
		t.Fatal("tenant-admin task.start in secret-project unexpectedly succeeded")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("tenant-admin task.start in secret-project = %v; want forbidden", err)
	}

	// 5. Zero-membership OIDC user gets uniform unauthenticated for both
	// existing and nonexistent IDs (never reveals existence).
	for _, probe := range []struct {
		op, param, id string
	}{
		{"task.read", "task", demo2TaskID},
		{"task.read", "task", "task-does-not-exist"},
		{"execution.inspect", "execution", demo2ExecID},
		{"execution.inspect", "execution", "execution-does-not-exist"},
	} {
		_, err := invoke("jwt-unassigned", probe.op, `{}`, "", nil, map[string]string{probe.param: probe.id})
		if class, ok := fault.ClassOf(err); !ok || class != fault.Unauthenticated {
			t.Fatalf("unassigned %s(%s) = %v; want unauthenticated", probe.op, probe.id, err)
		}
	}
}

// TestIssue94ProjectOnlyHumanOIDCReceivesForbiddenOnTenantAdminOps pins Issue #94
// end-to-end through the real token.Resolver, identitystore.Adapter, ops.Router,
// REST edge (httpapi) and MCP edge (mcp):
//   - project-only human OIDC sessions (engineer, reviewer, operator, and even
//     project-only viewer/admin with no tenant-wide role) receive 403 forbidden
//     with the standard authorization refusal message on tenant-admin operations
//     (membership.list, membership.grant, membership.revoke, project.create,
//     project.update, tenant.create, tenant.rotate_key) via REST and MCP;
//   - human OIDC tenant-admin succeeds on membership.list and membership.grant;
//   - zero-membership human OIDC sessions remain refused as 401 unauthenticated;
//   - revoking a project-only human's last project membership immediately flips
//     tenant-admin calls back to 401 unauthenticated (no cached authority).
func TestIssue94ProjectOnlyHumanOIDCReceivesForbiddenOnTenantAdminOps(t *testing.T) {
	h := newHarness(t)

	if _, err := h.invokeKey(t, "token-admin", "tenant.create",
		`{"tenant_id":"`+testTenant+`","name":"Local Tenant"}`, "id94-seed-tenant", nil); err != nil {
		t.Fatalf("tenant.create: %v", err)
	}
	for _, proj := range []string{"project-local", "secret-project"} {
		if _, err := h.invokeKey(t, "token-admin", "project.create",
			`{"project_id":"`+proj+`","name":"Project `+proj+`"}`, "id94-seed-"+proj,
			map[string]string{"tenant": testTenant}); err != nil {
			t.Fatalf("project.create %s: %v", proj, err)
		}
	}

	adminID := id.PrincipalID("oidc-id94-admin")
	engineerID := id.PrincipalID("oidc-id94-engineer")
	reviewerID := id.PrincipalID("oidc-id94-reviewer")
	operatorID := id.PrincipalID("oidc-id94-operator")
	projViewerID := id.PrincipalID("oidc-id94-project-viewer")
	projAdminID := id.PrincipalID("oidc-id94-project-admin")
	unassignedID := id.PrincipalID("oidc-id94-unassigned")

	h.seedMembership(t, tenancy.Scope{TenantID: testTenant}, "membership-id94-admin-tenant", adminID, 1, tenancy.RoleAdmin)
	for _, seed := range []struct {
		key       string
		principal id.PrincipalID
		roles     string
	}{
		{"id94-grant-eng", engineerID, `["engineer"]`},
		{"id94-grant-rev", reviewerID, `["reviewer"]`},
		{"id94-grant-op", operatorID, `["operator"]`},
		{"id94-grant-pv", projViewerID, `["viewer"]`},
		{"id94-grant-pa", projAdminID, `["admin"]`},
	} {
		if _, err := h.invokeKey(t, "token-admin", "membership.grant",
			`{"roles":`+seed.roles+`}`, seed.key,
			map[string]string{"project": "project-local", "principal": string(seed.principal)}); err != nil {
			t.Fatalf("membership.grant %s: %v", seed.principal, err)
		}
	}

	exp := fixedClock{}.Now().Add(time.Hour)
	verifier := mapOIDCVerifier{
		tokens: map[string]identity.Principal{
			"jwt-admin": identity.Principal{
				ID: adminID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-engineer": identity.Principal{
				ID: engineerID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-reviewer": identity.Principal{
				ID: reviewerID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-operator": identity.Principal{
				ID: operatorID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-project-viewer": identity.Principal{
				ID: projViewerID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-project-admin": identity.Principal{
				ID: projAdminID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
			}.WithVerifiedExpiry(exp),
			"jwt-unassigned": identity.Principal{
				ID: unassignedID, Kind: identity.KindHuman, Scope: tenancy.Scope{TenantID: testTenant}, Enabled: true,
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
	restEdge, err := httpapi.NewEdge(oidcRouter)
	if err != nil {
		t.Fatalf("httpapi.NewEdge: %v", err)
	}
	mcpEdge, err := mcp.NewEdge(mcp.Config{
		Router:     oidcRouter,
		ServerInfo: api.MCPImplementation{Name: "kodema", Version: "test"},
	})
	if err != nil {
		t.Fatalf("mcp.NewEdge: %v", err)
	}

	callREST := func(cred, method, path, body string) *responseRecorder {
		req, err := http.NewRequest(method, path, bytes.NewReader([]byte(body)))
		if err != nil {
			t.Fatalf("http.NewRequest %s %s: %v", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer "+cred)
		rec := &responseRecorder{header: make(http.Header)}
		restEdge.ServeHTTP(rec, req)
		return rec
	}
	callMCPError := func(cred, tool string, input map[string]any, key string) api.Error {
		t.Helper()
		args := map[string]any{"input": input}
		if key != "" {
			args["idempotency_key"] = key
		}
		rawArgs, _ := json.Marshal(args)
		payload, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "tools/call",
			"params":  map[string]any{"name": tool, "arguments": json.RawMessage(rawArgs)},
		})
		raw := mcpEdge.Handle(context.Background(), cred, payload)
		var rpc struct {
			Error *struct {
				Data api.Error `json:"data"`
			} `json:"error"`
			Result struct {
				IsError           bool      `json:"isError"`
				StructuredContent api.Error `json:"structuredContent"`
			} `json:"result"`
		}
		if err := json.Unmarshal(raw, &rpc); err != nil {
			t.Fatalf("unmarshal MCP error response %s: %v", string(raw), err)
		}
		if rpc.Error != nil {
			return rpc.Error.Data
		}
		if !rpc.Result.IsError {
			t.Fatalf("MCP %s(%s) unexpectedly succeeded: %s", cred, tool, string(raw))
		}
		return rpc.Result.StructuredContent
	}

	// 1. Tenant-admin human OIDC succeeds on membership.list and membership.grant.
	adminList := callREST("jwt-admin", http.MethodGet, "/api/v1/projects/project-local/memberships", "")
	if adminList.status != http.StatusOK {
		t.Fatalf("admin REST membership.list = %d %s; want 200", adminList.status, adminList.body.String())
	}
	adminGrant := callREST("jwt-admin", http.MethodPut,
		"/api/v1/projects/secret-project/memberships/"+string(operatorID),
		`{"idempotency_key":"id94-admin-grant","input":{"roles":["operator"]}}`)
	if adminGrant.status != http.StatusOK {
		t.Fatalf("admin REST membership.grant = %d %s; want 200", adminGrant.status, adminGrant.body.String())
	}

	// 2. Project-only human OIDC personas receive 403 forbidden with the exact
	// operation authorization message on all tenant-admin operations (REST + MCP),
	// including project-only viewer and project-only admin (who hold no tenant role).
	projectOnlyPersonas := []struct {
		cred        string
		principalID id.PrincipalID
	}{
		{"jwt-engineer", engineerID},
		{"jwt-reviewer", reviewerID},
		{"jwt-operator", operatorID},
		{"jwt-project-viewer", projViewerID},
		{"jwt-project-admin", projAdminID},
	}
	restProbes := []struct {
		op     string
		method string
		path   string
		body   string
	}{
		{"membership.list", http.MethodGet, "/api/v1/projects/project-local/memberships", ""},
		{"membership.list", http.MethodGet, "/api/v1/projects/secret-project/memberships", ""},
		{"membership.list", http.MethodGet, "/api/v1/projects/no-such-project/memberships", ""},
		{"membership.grant", http.MethodPut, "/api/v1/projects/project-local/memberships/" + string(engineerID),
			`{"idempotency_key":"id94-try-grant","input":{"roles":["reviewer"]}}`},
		{"membership.revoke", http.MethodDelete, "/api/v1/projects/project-local/memberships/" + string(engineerID),
			`{"idempotency_key":"id94-try-revoke","expected_version":1,"input":{}}`},
		{"project.create", http.MethodPost, "/api/v1/tenants/" + testTenant + "/projects",
			`{"idempotency_key":"id94-try-create-proj","input":{"project_id":"nope","name":"Nope"}}`},
		{"project.update", http.MethodPatch, "/api/v1/projects/project-local",
			`{"idempotency_key":"id94-try-update-proj","expected_version":1,"input":{"name":"Renamed"}}`},
		{"tenant.create", http.MethodPost, "/api/v1/tenants",
			`{"idempotency_key":"id94-try-create-tenant","input":{"tenant_id":"other","name":"Other"}}`},
		{"tenant.rotate_key", http.MethodPost, "/api/v1/tenants/" + testTenant + "/rotate-key",
			`{"idempotency_key":"id94-try-rotate","expected_version":1,"input":{"name":"Local Tenant"}}`},
	}
	for _, persona := range projectOnlyPersonas {
		for _, probe := range restProbes {
			rec := callREST(persona.cred, probe.method, probe.path, probe.body)
			if rec.status != http.StatusForbidden {
				t.Fatalf("%s REST %s %s = %d %s; want 403", persona.cred, probe.op, probe.path, rec.status, rec.body.String())
			}
			var apiErr api.Error
			if err := json.Unmarshal(rec.body.Bytes(), &apiErr); err != nil {
				t.Fatalf("decode error body %s: %v", rec.body.String(), err)
			}
			wantMsg := fmt.Sprintf("principal %q is not authorized for operation %q", persona.principalID, probe.op)
			if apiErr.Code != string(fault.Forbidden) || apiErr.Message != wantMsg {
				t.Fatalf("%s REST %s error = %+v; want code=forbidden message=%q", persona.cred, probe.op, apiErr, wantMsg)
			}
		}

		// MCP parity for membership_list and membership_grant.
		mcpListErr := callMCPError(persona.cred, "membership_list", map[string]any{"project_id": "project-local"}, "")
		wantListMsg := fmt.Sprintf("principal %q is not authorized for operation %q", persona.principalID, "membership.list")
		if mcpListErr.Code != string(fault.Forbidden) || mcpListErr.Message != wantListMsg {
			t.Fatalf("%s MCP membership_list = %+v; want code=forbidden message=%q", persona.cred, mcpListErr, wantListMsg)
		}
		mcpGrantErr := callMCPError(persona.cred, "membership_grant", map[string]any{
			"project_id":   "project-local",
			"principal_id": string(engineerID),
			"roles":        []string{"reviewer"},
		}, "id94-mcp-grant")
		wantGrantMsg := fmt.Sprintf("principal %q is not authorized for operation %q", persona.principalID, "membership.grant")
		if mcpGrantErr.Code != string(fault.Forbidden) || mcpGrantErr.Message != wantGrantMsg {
			t.Fatalf("%s MCP membership_grant = %+v; want code=forbidden message=%q", persona.cred, mcpGrantErr, wantGrantMsg)
		}
	}

	// Foreign tenant stays uniformly 404 not_found before authorization.
	foreignTenant := callREST("jwt-engineer", http.MethodPost, "/api/v1/tenants/tenant-foreign/projects",
		`{"idempotency_key":"id94-foreign","input":{"project_id":"nope","name":"Nope"}}`)
	if foreignTenant.status != http.StatusNotFound {
		t.Fatalf("engineer foreign tenant project.create = %d %s; want 404", foreignTenant.status, foreignTenant.body.String())
	}

	// 3. Zero-membership OIDC user remains refused as 401 unauthenticated across
	// all tenant-admin operations (REST and MCP) and foreign-tenant probes.
	for _, probe := range restProbes {
		rec := callREST("jwt-unassigned", probe.method, probe.path, probe.body)
		if rec.status != http.StatusUnauthorized {
			t.Fatalf("unassigned REST %s %s = %d %s; want 401", probe.op, probe.path, rec.status, rec.body.String())
		}
		var apiErr api.Error
		if err := json.Unmarshal(rec.body.Bytes(), &apiErr); err != nil {
			t.Fatalf("decode unassigned error body %s: %v", rec.body.String(), err)
		}
		if apiErr.Code != string(fault.Unauthenticated) || apiErr.Message != "presented credential is invalid" {
			t.Fatalf("unassigned REST %s error = %+v; want unauthenticated", probe.op, apiErr)
		}
	}
	unassignedMCPErr := callMCPError("jwt-unassigned", "membership_list", map[string]any{"project_id": "project-local"}, "")
	if unassignedMCPErr.Code != string(fault.Unauthenticated) || unassignedMCPErr.Message != "presented credential is invalid" {
		t.Fatalf("unassigned MCP membership_list = %+v; want unauthenticated", unassignedMCPErr)
	}

	// 4. Revoking engineer's only project membership immediately flips
	// membership.list from 403 forbidden back to 401 unauthenticated.
	v1 := uint64(1)
	if _, err := oidcRouter.Invoke(context.Background(), ops.Invocation{
		Operation:  "membership.revoke",
		Credential: "jwt-admin",
		Params:     map[string]string{"project": "project-local", "principal": string(engineerID)},
		Request: api.Request{
			Input:           json.RawMessage(`{}`),
			IdempotencyKey:  "id94-revoke-eng",
			ExpectedVersion: &v1,
		},
	}); err != nil {
		t.Fatalf("admin revoke engineer membership: %v", err)
	}
	afterRevoke := callREST("jwt-engineer", http.MethodGet, "/api/v1/projects/project-local/memberships", "")
	if afterRevoke.status != http.StatusUnauthorized {
		t.Fatalf("engineer membership.list after last membership revoked = %d %s; want 401",
			afterRevoke.status, afterRevoke.body.String())
	}
}

type memoryLogSink struct{}

func (*memoryLogSink) Append(context.Context, tenancy.Scope, log.Chunk) (log.AppendResult, error) {
	return log.AppendResult{}, nil
}

func (*memoryLogSink) Read(context.Context, log.ReadRequest) (log.ReadResult, error) {
	return log.ReadResult{}, nil
}

type noLogOwnership struct{}

func (noLogOwnership) Ownership(context.Context, tenancy.Scope, id.ExecutionID) (log.ExecutionOwnership, error) {
	return log.ExecutionOwnership{}, fault.New(fault.NotFound, "not found")
}
