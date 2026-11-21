package opsapp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	opsapp "github.com/optimiware/kodema/internal/shell/app/ops"
	"github.com/optimiware/kodema/internal/shell/authorize"
	"github.com/optimiware/kodema/internal/shell/identity"
	"github.com/optimiware/kodema/internal/shell/receipt"
	"github.com/optimiware/kodema/internal/shell/store"
	"github.com/optimiware/kodema/internal/shell/tenancy"
	"github.com/optimiware/kodema/internal/shell/transport/ops"
)

// spyHandler counts handler invocations so a scope refusal can be proven to
// happen before any capability effect.
type spyHandler struct {
	inner ops.Handler
	calls *int32
}

func (spy spyHandler) Handle(ctx context.Context, call ops.AuthorizedCall) (api.Result, error) {
	atomic.AddInt32(spy.calls, 1)
	return spy.inner.Handle(ctx, call)
}

// newSpyRouter rebuilds the complete binding table over the harness services
// with counting handlers.
func newSpyRouter(t *testing.T, h *harness) (*ops.Router, *int32) {
	t.Helper()
	bindings, err := opsapp.Bindings(h.deps)
	if err != nil {
		t.Fatalf("Bindings: %v", err)
	}
	calls := new(int32)
	wrapped := make([]ops.Binding, len(bindings))
	for index, binding := range bindings {
		wrapped[index] = ops.Binding{
			Operation: binding.Operation,
			Input:     binding.Input,
			Result:    binding.Result,
			Handler:   spyHandler{inner: binding.Handler, calls: calls},
		}
	}
	router, err := ops.NewRouter(ops.Config{
		Principals: h.principals,
		Scopes:     h.ownership,
		Executions: h.ownership,
		Bindings:   wrapped,
	})
	if err != nil {
		t.Fatalf("ops.NewRouter: %v", err)
	}
	return router, calls
}

// scopeCarriage renders the identity-scope route fields of one operation for
// both edges: REST params and the equivalent MCP input object.
func scopeCarriage(codec api.OperationCodec, values map[string]string) (map[string]string, json.RawMessage, bool) {
	params := make(map[string]string)
	fields := make(map[string]string)
	has := false
	for _, param := range codec.RouteParams {
		if param.Name != "tenant" && param.Name != "project" {
			continue
		}
		value, ok := values[param.Name]
		if !ok {
			continue
		}
		has = true
		params[param.Name] = value
		fields[param.Field] = value
	}
	if !has {
		return nil, json.RawMessage(`{}`), false
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		panic(err)
	}
	return params, encoded, true
}

// scopeEffectState is the observable committed state a scope refusal must leave
// untouched.
type scopeEffectState struct {
	events []int
	audit  []int
	keys   int
}

func snapshotScopeEffects(h *harness, scopes []tenancy.Scope) scopeEffectState {
	state := scopeEffectState{keys: h.keys.callsMade()}
	for _, scope := range scopes {
		state.events = append(state.events, len(h.store.Events(scope)))
		state.audit = append(state.audit, len(h.store.Audit(scope)))
	}
	return state
}

// requireScopeRefusal asserts the uniform not_found of a foreign scope identity.
func requireScopeRefusal(t *testing.T, label string, err error) {
	t.Helper()
	if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
		t.Fatalf("%s = %v; want not_found", label, err)
	}
}

// TestScopeIdentityRegistrySweep walks all 58 registry operations. For every
// operation it pins that the principal's own tenant/project is accepted by the
// scope resolver, that a foreign tenant (REST route or MCP input) and a
// project-scoped principal naming another project are uniformly not_found, and
// that the refusal happens before any handler, row, receipt, key or event.
func TestScopeIdentityRegistrySweep(t *testing.T) {
	h := newHarness(t)
	spy, handlerCalls := newSpyRouter(t, h)
	engineer, err := h.principals.Resolve(context.Background(), testToken)
	if err != nil {
		t.Fatalf("resolve engineer: %v", err)
	}
	admin, err := h.principals.Resolve(context.Background(), "token-admin")
	if err != nil {
		t.Fatalf("resolve admin: %v", err)
	}
	scopes := []tenancy.Scope{
		{TenantID: testTenant},
		{TenantID: testTenant, ProjectID: testProject},
		{TenantID: "tenant-b"},
		{TenantID: "tenant-b", ProjectID: "project-b"},
	}
	baseline := snapshotScopeEffects(h, scopes)

	for _, operation := range authorize.Operations() {
		name := string(operation.Name)
		codec, ok := codecForOperation(name)
		if !ok {
			t.Fatalf("%s has no public codec", name)
		}
		// The principal that would normally hold the operation's grant: a
		// tenant-wide admin for tenant administration, the project engineer for
		// everything else.
		credential, principal := testToken, engineer
		if operation.Scope == authorize.ScopeTenant {
			credential, principal = "token-admin", admin
		}

		// Own tenant/project is never refused by the resolver, on either edge.
		ownParams, ownInput, _ := scopeCarriage(codec, map[string]string{
			"tenant":  testTenant,
			"project": testProject,
		})
		if _, err := h.ownership.Resolve(context.Background(), principal, ops.Call{
			Operation: operation.Name,
			Request:   api.Request{Input: ownInput},
		}); err != nil {
			t.Fatalf("%s MCP own scope refused: %v", name, err)
		}
		if _, err := h.ownership.Resolve(context.Background(), principal, ops.Call{
			Operation: operation.Name,
			Request:   api.Request{Input: json.RawMessage(`{}`)},
			Params:    ownParams,
		}); err != nil {
			t.Fatalf("%s REST own scope refused: %v", name, err)
		}

		// A route-named tenant is read from the REST parameter or the MCP input
		// field the route table names, and a foreign one is not_found.
		tenantParams, tenantInput, hasTenant := scopeCarriage(codec, map[string]string{"tenant": "tenant-b"})
		if hasTenant {
			for _, invocation := range []ops.Invocation{
				{
					Operation:  operation.Name,
					Credential: credential,
					Request:    api.Request{Input: tenantInput, IdempotencyKey: "sweep-mcp-tenant-" + name},
				},
				{
					Operation:  operation.Name,
					Credential: credential,
					Request:    api.Request{Input: json.RawMessage(`{}`), IdempotencyKey: "sweep-rest-tenant-" + name},
					Params:     tenantParams,
				},
			} {
				before := atomic.LoadInt32(handlerCalls)
				_, err := spy.Invoke(executionRequest(), invocation)
				requireScopeRefusal(t, name+" foreign tenant", err)
				if atomic.LoadInt32(handlerCalls) != before {
					t.Fatalf("%s foreign tenant reached the handler", name)
				}
			}
		}

		// A project-scoped principal naming another project is not_found on
		// either edge, and the handler never runs.
		projectParams, projectInput, hasProject := scopeCarriage(codec, map[string]string{"project": "project-b"})
		if hasProject {
			for _, invocation := range []ops.Invocation{
				{
					Operation:  operation.Name,
					Credential: testToken,
					Request:    api.Request{Input: projectInput, IdempotencyKey: "sweep-mcp-project-" + name},
				},
				{
					Operation:  operation.Name,
					Credential: testToken,
					Request:    api.Request{Input: json.RawMessage(`{}`), IdempotencyKey: "sweep-rest-project-" + name},
					Params:     projectParams,
				},
			} {
				before := atomic.LoadInt32(handlerCalls)
				_, err := spy.Invoke(executionRequest(), invocation)
				requireScopeRefusal(t, name+" foreign project", err)
				if atomic.LoadInt32(handlerCalls) != before {
					t.Fatalf("%s foreign project reached the handler", name)
				}
				for _, scope := range []tenancy.Scope{
					{TenantID: testTenant, ProjectID: testProject},
					{TenantID: "tenant-b", ProjectID: "project-b"},
				} {
					if _, found := h.store.Receipt(receipt.Key{
						TenantID:    scope.TenantID,
						ProjectID:   scope.ProjectID,
						PrincipalID: engineer.ID,
						Operation:   name,
						CallerKey:   invocation.Request.IdempotencyKey,
					}); found {
						t.Fatalf("%s foreign project committed a receipt", name)
					}
				}
			}

			// A tenant-wide principal naming a project resolves inside its own
			// tenant, never inside the project's (unprovable) tenant.
			resolution, err := h.ownership.Resolve(context.Background(), admin, ops.Call{
				Operation: operation.Name,
				Request:   api.Request{Input: projectInput},
			})
			if err != nil {
				t.Fatalf("%s tenant-wide project resolution: %v", name, err)
			}
			if resolution.Scope.TenantID != testTenant {
				t.Fatalf("%s resolved tenant = %q; want %q", name, resolution.Scope.TenantID, testTenant)
			}
			if operation.Scope == authorize.ScopeProject && resolution.Scope.ProjectID != "project-b" {
				t.Fatalf("%s resolved project = %q; want project-b", name, resolution.Scope.ProjectID)
			}
		}
	}

	if got := snapshotScopeEffects(h, scopes); !reflect.DeepEqual(got, baseline) {
		t.Fatalf("scope refusals changed committed state: %+v -> %+v", baseline, got)
	}
}

// TestMCPTenantIdentityIsConfined pins the tenant identity carried only in the
// MCP input: a foreign tenant_id is uniformly not_found before the handler,
// while the principal's own tenant still works.
func TestMCPTenantIdentityIsConfined(t *testing.T) {
	h := newHarness(t)
	spy, handlerCalls := newSpyRouter(t, h)
	keysBefore := h.keys.callsMade()

	before := atomic.LoadInt32(handlerCalls)
	_, err := spy.Invoke(executionRequest(), ops.Invocation{
		Operation:  authorize.OperationName("project.create"),
		Credential: "token-admin",
		Request: api.Request{Input: json.RawMessage(`{"tenant_id":"tenant-b","project_id":"project-x","name":"Foreign"}`),
			IdempotencyKey: "mcp-project-create-foreign"},
	})
	requireScopeRefusal(t, "MCP project.create foreign tenant", err)
	if atomic.LoadInt32(handlerCalls) != before {
		t.Fatal("MCP project.create foreign tenant reached the handler")
	}
	if _, found := h.store.Snapshot(tenancy.Scope{TenantID: "tenant-b"}, "projects", "project-x"); found {
		t.Fatal("MCP project.create created a project in the foreign tenant")
	}
	if _, found := h.store.Snapshot(tenancy.Scope{TenantID: testTenant}, "projects", "project-x"); found {
		t.Fatal("MCP project.create created a project in the caller tenant")
	}

	before = atomic.LoadInt32(handlerCalls)
	_, err = spy.Invoke(executionRequest(), ops.Invocation{
		Operation:  authorize.OperationName("tenant.rotate_key"),
		Credential: "token-admin",
		Request: api.Request{Input: json.RawMessage(`{"tenant_id":"tenant-b","name":"Tenant B"}`),
			ExpectedVersion: new(uint64(1)), IdempotencyKey: "mcp-rotate-foreign"},
	})
	requireScopeRefusal(t, "MCP tenant.rotate_key foreign tenant", err)
	if atomic.LoadInt32(handlerCalls) != before {
		t.Fatal("MCP tenant.rotate_key foreign tenant reached the handler")
	}
	if h.keys.callsMade() != keysBefore {
		t.Fatalf("foreign tenant minted %d tenant keys", h.keys.callsMade()-keysBefore)
	}

	// The same MCP carriage naming the principal's own tenant is accepted.
	created, err := h.invoke(t, "token-admin", "project.create", `{"tenant_id":"tenant-a","project_id":"project-own","name":"Own"}`, nil)
	if err != nil {
		t.Fatalf("MCP project.create own tenant: %v", err)
	}
	var project api.ProjectCreateResult
	decodeResult(t, created, &project)
	if project.ProjectID != "project-own" {
		t.Fatalf("MCP project.create own tenant = %+v", project)
	}

	// REST route carriage stays uniformly not_found.
	_, err = h.invoke(t, "token-admin", "project.create", `{"project_id":"project-route","name":"Route"}`, map[string]string{"tenant": "tenant-b"})
	requireScopeRefusal(t, "REST project.create foreign tenant", err)
	_, err = h.invokeVersion(t, "token-admin", "tenant.rotate_key", `{"name":"Tenant B"}`, 1, map[string]string{"tenant": "tenant-b"})
	requireScopeRefusal(t, "REST tenant.rotate_key foreign tenant", err)
	if h.keys.callsMade() != keysBefore {
		t.Fatalf("tenant keys minted = %d; want none", h.keys.callsMade())
	}
}

// TestTenantWideProjectStaysInCallerTenant pins that a tenant-wide principal
// naming a project that exists in another tenant reads inside its own tenant:
// the foreign row is uniformly not_found and is never exposed.
func TestTenantWideProjectStaysInCallerTenant(t *testing.T) {
	h := newHarness(t)
	foreign := tenancy.Scope{TenantID: "tenant-b", ProjectID: "project-b"}
	if _, err := h.store.Execute(context.Background(), foreign, store.Unit{
		Writes: []store.Write{{
			Table:  "secrets",
			ID:     "secret-foreign",
			Insert: true,
			Values: map[string]any{
				"name":        "FOREIGN",
				"ciphertext":  []byte("ciphertext"),
				"wrapped_key": []byte("wrapped"),
				"wrap_nonce":  []byte("wrap-nonce"),
				"key_ref":     "kms/foreign",
				"key_version": int64(1),
				"nonce":       make([]byte, 12),
				"version":     int64(1),
				"created_at":  fixedNow,
				"updated_at":  fixedNow,
			},
		}},
	}); err != nil {
		t.Fatalf("seed foreign secret: %v", err)
	}

	_, err := h.invoke(t, "token-admin", "secret.metadata.read", `{}`, map[string]string{
		"project": "project-b",
		"secret":  "secret-foreign",
	})
	requireScopeRefusal(t, "tenant-wide foreign project read", err)
}

// TestScopeIDAddressedRegistrySweep walks every ScopeProject operation whose
// route has no `{project}` segment and proves that:
//   - every human-callable ID-only project operation resolves its owning
//     project for a tenant-scoped human member and a tenant-wide admin across
//     both REST and MCP;
//   - resources in a project where the tenant-scoped human has no membership,
//     or in a foreign tenant, are uniformly not_found (no existence oracle);
//   - a zero-membership human is uniformly unauthenticated;
//   - omitting the resource ID in MCP returns invalid_input;
//   - service-only operations without a project or resource ID still refuse a
//     tenant-wide principal with invalid_input.
func TestScopeIDAddressedRegistrySweep(t *testing.T) {
	h := newHarness(t)
	if _, err := h.invokeKey(t, "token-admin", "tenant.create",
		`{"tenant_id":"`+testTenant+`","name":"Tenant A"}`, "id-sweep-tenant", nil); err != nil {
		t.Fatalf("tenant.create: %v", err)
	}
	for _, proj := range []string{testProject, "secret-project"} {
		if _, err := h.invokeKey(t, "token-admin", "project.create",
			`{"project_id":"`+proj+`","name":"Project `+proj+`"}`, "id-sweep-proj-"+proj,
			map[string]string{"tenant": testTenant}); err != nil {
			t.Fatalf("project.create %s: %v", proj, err)
		}
	}

	// Seed one resource of each ID-addressed family in testProject and one in
	// secret-project (which memberHuman does NOT belong to).
	seedIDResources := func(projectID, suffix string) map[string]string {
		scope := tenancy.Scope{TenantID: testTenant, ProjectID: id.ProjectID(projectID)}
		taskID := "task-sweep-" + suffix
		execID := "execution-sweep-" + suffix
		svcID := "service-sweep-" + suffix
		resID := "resource-sweep-" + suffix
		bindID := "binding-sweep-" + suffix
		if _, err := h.store.Execute(context.Background(), scope, store.Unit{
			Writes: []store.Write{
				{
					Table:  "tasks",
					ID:     taskID,
					Insert: true,
					Values: map[string]any{
						"content_version":   int64(1),
						"lifecycle_version": int64(1),
						"state":             "open",
						"initiator_id":      "initiator-1",
						"title":             "sweep task",
						"created_at":        fixedNow,
					},
				},
				{
					Table:  "executions",
					ID:     execID,
					Insert: true,
					Values: map[string]any{
						"principal_id": "exec-principal-" + suffix,
						"state":        "queued",
						"version":      int64(1),
						"yaml_sha256":  make([]byte, 32),
						"snapshot":     []byte(`{}`),
						"created_at":   fixedNow,
					},
				},
				{
					Table:  "services",
					ID:     svcID,
					Insert: true,
					Values: map[string]any{
						"name":       svcID,
						"state":      "enabled",
						"roles":      []byte(`["engineer"]`),
						"version":    int64(1),
						"created_at": fixedNow,
					},
				},
				{
					Table:  "resources",
					ID:     resID,
					Insert: true,
					Values: map[string]any{
						"version":    int64(1),
						"created_at": fixedNow,
					},
				},
				{
					Table:  "resource_bindings",
					ID:     bindID,
					Insert: true,
					Values: map[string]any{
						"resource_id":       resID,
						"service_id":        svcID,
						"origins":           []byte(`["https://git.local"]`),
						"operations":        []byte(`["git.ref.update"]`),
						"reportable_events": []byte(`[]`),
						"metadata":          []byte(`{}`),
						"version":           int64(1),
						"created_at":        fixedNow,
					},
				},
			},
		}); err != nil {
			t.Fatalf("seed ID resources in %s: %v", projectID, err)
		}
		return map[string]string{
			"task":      taskID,
			"execution": execID,
			"service":   svcID,
			"binding":   bindID,
		}
	}

	ownIDs := seedIDResources(testProject, "own")
	secretIDs := seedIDResources("secret-project", "secret")

	admin, err := h.principals.Resolve(context.Background(), "token-admin")
	if err != nil {
		t.Fatalf("resolve admin: %v", err)
	}
	memberHuman := authorize.Principal{
		ID:      "oidc-sweep-member",
		Kind:    identity.KindHuman,
		Scope:   tenancy.Scope{TenantID: testTenant},
		Enabled: true,
	}
	h.seedMembership(t, tenancy.Scope{TenantID: testTenant, ProjectID: testProject},
		"membership-sweep-member", memberHuman.ID, 1, tenancy.RoleEngineer)
	unassignedHuman := authorize.Principal{
		ID:      "oidc-sweep-unassigned",
		Kind:    identity.KindHuman,
		Scope:   tenancy.Scope{TenantID: testTenant},
		Enabled: true,
	}

	scopes := []tenancy.Scope{
		{TenantID: testTenant},
		{TenantID: testTenant, ProjectID: testProject},
		{TenantID: testTenant, ProjectID: "secret-project"},
	}
	baseline := snapshotScopeEffects(h, scopes)

	for _, operation := range authorize.Operations() {
		if operation.Scope != authorize.ScopeProject {
			continue
		}
		name := string(operation.Name)
		codec, ok := codecForOperation(name)
		if !ok {
			t.Fatalf("%s has no public codec", name)
		}
		hasProject := false
		var idParam, idField string
		for _, param := range codec.RouteParams {
			if param.Name == "project" {
				hasProject = true
				break
			}
			if _, isID := ownIDs[param.Name]; isID {
				idParam = param.Name
				idField = param.Field
			}
		}
		if hasProject {
			continue
		}
		if idParam == "" {
			if len(operation.Requires.HumanRoles) > 0 {
				t.Fatalf("human-callable ScopeProject operation %s has neither {project} nor an ID-addressed resource param", name)
			}
			// Service-only operations with no project/resource ID still refuse
			// tenant-wide principals with invalid_input.
			if _, err := h.ownership.Resolve(context.Background(), admin, ops.Call{
				Operation: operation.Name,
				Request:   api.Request{Input: json.RawMessage(`{}`)},
			}); err == nil {
				t.Fatalf("%s unexpectedly resolved for tenant-wide admin", name)
			} else if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("%s tenant-wide error = %v; want invalid_input", name, err)
			}
			continue
		}

		ownRESTParams := map[string]string{idParam: ownIDs[idParam]}
		ownMCPInput, _ := json.Marshal(map[string]string{idField: ownIDs[idParam]})
		secretRESTParams := map[string]string{idParam: secretIDs[idParam]}
		secretMCPInput, _ := json.Marshal(map[string]string{idField: secretIDs[idParam]})

		// 1. Both tenant-wide admin and tenant-scoped human member resolve the
		// owning project for REST and MCP.
		for label, principal := range map[string]authorize.Principal{
			"tenant-admin": admin,
			"oidc-member":  memberHuman,
		} {
			resREST, err := h.ownership.Resolve(context.Background(), principal, ops.Call{
				Operation: operation.Name,
				Params:    ownRESTParams,
				Request:   api.Request{Input: json.RawMessage(`{}`)},
			})
			if err != nil {
				t.Fatalf("%s %s REST Resolve: %v", name, label, err)
			}
			if resREST.Scope != (tenancy.Scope{TenantID: testTenant, ProjectID: testProject}) {
				t.Fatalf("%s %s REST scope = %+v; want %s/%s", name, label, resREST.Scope, testTenant, testProject)
			}
			resMCP, err := h.ownership.Resolve(context.Background(), principal, ops.Call{
				Operation: operation.Name,
				Request:   api.Request{Input: ownMCPInput},
			})
			if err != nil {
				t.Fatalf("%s %s MCP Resolve: %v", name, label, err)
			}
			if resMCP.Scope != (tenancy.Scope{TenantID: testTenant, ProjectID: testProject}) {
				t.Fatalf("%s %s MCP scope = %+v; want %s/%s", name, label, resMCP.Scope, testTenant, testProject)
			}
		}

		// 2. Resource in a non-member project is uniformly not_found for the
		// tenant-scoped human member (both REST and MCP).
		for _, call := range []ops.Call{
			{Operation: operation.Name, Params: secretRESTParams, Request: api.Request{Input: json.RawMessage(`{}`)}},
			{Operation: operation.Name, Request: api.Request{Input: secretMCPInput}},
		} {
			if _, err := h.ownership.Resolve(context.Background(), memberHuman, call); err == nil {
				t.Fatalf("%s memberHuman unexpectedly resolved secret-project resource", name)
			} else if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
				t.Fatalf("%s memberHuman secret-project error = %v; want not_found", name, err)
			}
		}

		// 3. Zero-membership human is uniformly unauthenticated (never reveals existence).
		if _, err := h.ownership.Resolve(context.Background(), unassignedHuman, ops.Call{
			Operation: operation.Name,
			Params:    ownRESTParams,
			Request:   api.Request{Input: json.RawMessage(`{}`)},
		}); err == nil {
			t.Fatalf("%s unassignedHuman unexpectedly succeeded", name)
		} else if class, ok := fault.ClassOf(err); !ok || class != fault.Unauthenticated {
			t.Fatalf("%s unassignedHuman error = %v; want unauthenticated", name, err)
		}

		// 4. Omitting the resource ID in MCP input returns invalid_input.
		if _, err := h.ownership.Resolve(context.Background(), memberHuman, ops.Call{
			Operation: operation.Name,
			Request:   api.Request{Input: json.RawMessage(`{}`)},
		}); err == nil {
			t.Fatalf("%s missing MCP id unexpectedly succeeded", name)
		} else if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("%s missing MCP id error = %v; want invalid_input", name, err)
		}
	}

	if got := snapshotScopeEffects(h, scopes); !reflect.DeepEqual(got, baseline) {
		t.Fatalf("ID-addressed scope lookups changed committed state: %+v -> %+v", baseline, got)
	}
}
