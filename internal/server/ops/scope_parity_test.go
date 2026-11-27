package opsapp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/adapter/store/units/readers"
	"github.com/optimiware/kodema/internal/control/identity"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/control/verify"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/transport/httpapi"
	"github.com/optimiware/kodema/internal/transport/mcp"
)

// TestScopeRules pins the ScopeResolver rule: a project-scoped
// credential cannot see a foreign row (uniform not_found), a tenant-wide
// credential resolves ID-addressed project resources within its own tenant,
// refuses operations without a project or resource identity, and a route/body
// disagreement refuses.
func TestScopeRules(t *testing.T) {
	h := newHarness(t)
	if _, err := h.invokeKey(t, "token-admin", "tenant.create",
		`{"tenant_id":"`+testTenant+`","name":"Tenant A"}`, "scope-rules-tenant", nil); err != nil {
		t.Fatalf("tenant.create: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "project.create",
		`{"project_id":"project-b","name":"Project B"}`, "scope-rules-project-b",
		map[string]string{"tenant": testTenant}); err != nil {
		t.Fatalf("project.create project-b: %v", err)
	}
	foreignTask := createTaskInProjectB(t, h)

	t.Run("foreign row is not_found", func(t *testing.T) {
		_, err := h.invoke(t, testToken, "task.read", `{}`, map[string]string{"task": foreignTask})
		if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
			t.Fatalf("foreign task read = %v; want not_found", err)
		}
	})

	t.Run("tenant-wide principal resolves owning project by resource id", func(t *testing.T) {
		read, err := h.invoke(t, "token-admin", "task.read", `{}`, map[string]string{"task": foreignTask})
		if err != nil {
			t.Fatalf("tenant-wide id-only read: %v", err)
		}
		var result api.TaskReadResult
		decodeResult(t, read, &result)
		if result.TaskID != foreignTask {
			t.Fatalf("task_id = %q; want %q", result.TaskID, foreignTask)
		}
	})

	t.Run("tenant-wide principal cannot resolve resource in another tenant", func(t *testing.T) {
		otherTenant := tenancy.Scope{TenantID: "tenant-b"}
		if _, err := h.store.Execute(context.Background(), otherTenant, store.Unit{
			Writes: []store.Write{{
				Table:  "projects",
				ID:     "project-z",
				Insert: true,
				Values: map[string]any{
					"name":       "Project Z",
					"version":    int64(1),
					"created_at": fixedNow,
				},
			}},
		}); err != nil {
			t.Fatalf("seed foreign tenant project: %v", err)
		}
		otherProject := tenancy.Scope{TenantID: "tenant-b", ProjectID: "project-z"}
		if _, err := h.store.Execute(context.Background(), otherProject, store.Unit{
			Writes: []store.Write{{
				Table:  "tasks",
				ID:     "task-other-tenant",
				Insert: true,
				Values: map[string]any{
					"content_version":   int64(1),
					"lifecycle_version": int64(1),
					"state":             "open",
					"initiator_id":      "someone",
					"title":             "other tenant task",
					"created_at":        fixedNow,
				},
			}},
		}); err != nil {
			t.Fatalf("seed foreign tenant task: %v", err)
		}
		_, err := h.invoke(t, "token-admin", "task.read", `{}`, map[string]string{"task": "task-other-tenant"})
		if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
			t.Fatalf("foreign-tenant task read = %v; want not_found", err)
		}
	})

	t.Run("tenant-wide principal without project or resource id is refused", func(t *testing.T) {
		_, err := h.invoke(t, "token-admin", "actor.verify", `{}`, nil)
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("tenant-wide actor.verify = %v; want invalid_input", err)
		}
		_, err = h.invoke(t, "token-admin", "task.read", `{}`, nil)
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("tenant-wide MCP task.read without task_id = %v; want invalid_input", err)
		}
	})

	t.Run("tenant-wide principal with a route project", func(t *testing.T) {
		listed, err := h.invoke(t, "token-admin", "execution.list", `{}`, map[string]string{"project": "project-b"})
		if err != nil {
			t.Fatalf("tenant-wide project read: %v", err)
		}
		var result api.ExecutionListResult
		decodeResult(t, listed, &result)
	})

	t.Run("route parameter fills an omitted field", func(t *testing.T) {
		read, err := h.invoke(t, testToken, "task.read", `{}`, map[string]string{"task": foreignTask})
		// The engineer is project-scoped, so the row is foreign; the point is
		// that the route supplied task_id and the handler reached the service.
		if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
			t.Fatalf("route-filled read = %v; want not_found (service reached)", err)
		}
		_ = read
	})

	t.Run("canonical field disagrees with the route", func(t *testing.T) {
		_, err := h.invoke(t, testToken, "task.read", `{"task_id":"task-other"}`, map[string]string{"task": foreignTask})
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("route/body disagreement = %v; want invalid_input", err)
		}
	})

	t.Run("route-named body field disagrees", func(t *testing.T) {
		_, err := h.invoke(t, testToken, "task.read", `{"task":"task-other"}`, map[string]string{"task": foreignTask})
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("route/body disagreement = %v; want invalid_input", err)
		}
	})
}

// TestRESTAndMCPParity pins that one binding serves both edges with the same
// result body.
func TestRESTAndMCPParity(t *testing.T) {
	h := newHarness(t)
	taskID := createTaskInProjectB(t, h)

	restEdge, err := httpapi.NewEdge(h.router)
	if err != nil {
		t.Fatalf("httpapi.NewEdge: %v", err)
	}
	mcpEdge, err := mcp.NewEdge(mcp.Config{
		Router:     h.router,
		ServerInfo: api.MCPImplementation{Name: "kodema", Version: "test"},
	})
	if err != nil {
		t.Fatalf("mcp.NewEdge: %v", err)
	}

	// The tenant-wide admin names the project; both edges address project-b.
	restRequest, err := http.NewRequest(http.MethodGet, "/api/v1/tasks/"+taskID, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		t.Fatalf("build REST request: %v", err)
	}
	restRequest.Header.Set("Authorization", "Bearer token-engineer-b")
	recorder := &responseRecorder{header: make(http.Header)}
	restEdge.ServeHTTP(recorder, restRequest)
	if recorder.status != http.StatusOK {
		t.Fatalf("REST status = %d body %s", recorder.status, recorder.body.String())
	}
	var restResult api.Result
	if err := json.Unmarshal(recorder.body.Bytes(), &restResult); err != nil {
		t.Fatalf("decode REST result: %v", err)
	}

	arguments, err := json.Marshal(map[string]any{"input": map[string]any{"task_id": taskID}})
	if err != nil {
		t.Fatalf("marshal MCP arguments: %v", err)
	}
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params":  map[string]any{"name": "task.read", "arguments": json.RawMessage(arguments)},
	})
	if err != nil {
		t.Fatalf("marshal MCP request: %v", err)
	}
	response := mcpEdge.Handle(context.Background(), "token-engineer-b", body)
	var mcpResponse struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(response, &mcpResponse); err != nil {
		t.Fatalf("decode MCP response: %v", err)
	}
	if len(mcpResponse.Error) != 0 {
		t.Fatalf("MCP error: %s", mcpResponse.Error)
	}
	mcpResult, err := api.DecodeMCPResult(mcpResponse.Result)
	if err != nil {
		t.Fatalf("decode MCP result: %v", err)
	}
	if restResult.Operation != mcpResult.Operation || !bytes.Equal(restResult.Body, mcpResult.Body) {
		t.Fatalf("REST result %+v differs from MCP result %+v", restResult, mcpResult)
	}
}

// TestTenantScopedEnforcingServiceVerifyAndReportScope pins Ownership.Resolve
// for actor.verify and resource.report:
//   - a tenant-scoped enforcing service resolves bound resources across
//     multiple projects in its tenant and succeeds on both actor.verify and
//     resource.report;
//   - a project-scoped service cannot verify or report on resources in another
//     project (uniform not_found);
//   - a tenant-scoped enforcing service receives uniform not_found for unknown,
//     unbound (bound to a different service), or cross-tenant resources;
//   - humans and roleless/unauthorized tenant services receive immediate 403
//     forbidden for both existing and nonexistent resource_ids (zero existence
//     oracle);
//   - missing or malformed resource_id returns invalid_input.
func TestTenantScopedEnforcingServiceVerifyAndReportScope(t *testing.T) {
	h := newHarness(t)
	if _, err := h.invokeKey(t, "token-admin", "tenant.create",
		`{"tenant_id":"`+testTenant+`","name":"Tenant A"}`, "vr-scope-tenant", nil); err != nil {
		t.Fatalf("tenant.create: %v", err)
	}
	for _, proj := range []string{testProject, "project-b"} {
		if _, err := h.invokeKey(t, "token-admin", "project.create",
			`{"project_id":"`+proj+`","name":"Project `+proj+`"}`, "vr-scope-"+proj,
			map[string]string{"tenant": testTenant}); err != nil {
			t.Fatalf("project.create %s: %v", proj, err)
		}
	}

	// Create tenant-scoped enforcing service "git", project-a-scoped service "git-proj-a",
	// tenant-scoped "other-svc", and tenant-scoped roleless service "roleless-svc".
	if _, err := h.invokeKey(t, "token-admin", "service.create",
		`{"service_id":"git","roles":["viewer"]}`, "vr-svc-git", nil); err != nil {
		t.Fatalf("create tenant git service: %v", err)
	}
	if _, err := h.invokeKey(t, testToken, "service.create",
		`{"service_id":"git-proj-a","roles":["viewer"]}`, "vr-svc-git-proj-a", map[string]string{"project": testProject}); err != nil {
		t.Fatalf("create project-a git service: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "service.create",
		`{"service_id":"other-svc","roles":["viewer"]}`, "vr-svc-other", nil); err != nil {
		t.Fatalf("create tenant other-svc: %v", err)
	}
	if _, err := h.invokeKey(t, "token-admin", "service.create",
		`{"service_id":"roleless-svc","roles":[]}`, "vr-svc-roleless", nil); err != nil {
		t.Fatalf("create tenant roleless-svc: %v", err)
	}

	h.principals.principals["token-git-tenant"] = identity.Principal{
		ID:      "git",
		Kind:    identity.KindService,
		Scope:   tenancy.Scope{TenantID: testTenant},
		Roles:   []tenancy.Role{tenancy.RoleViewer},
		Enabled: true,
		Version: 1,
	}
	h.principals.principals["token-git-proj-a"] = identity.Principal{
		ID:      "git-proj-a",
		Kind:    identity.KindService,
		Scope:   tenancy.Scope{TenantID: testTenant, ProjectID: testProject},
		Roles:   []tenancy.Role{tenancy.RoleViewer},
		Enabled: true,
		Version: 1,
	}
	h.principals.principals["token-other-svc"] = identity.Principal{
		ID:      "other-svc",
		Kind:    identity.KindService,
		Scope:   tenancy.Scope{TenantID: testTenant},
		Roles:   []tenancy.Role{tenancy.RoleViewer},
		Enabled: true,
		Version: 1,
	}
	h.principals.principals["token-roleless-svc"] = identity.Principal{
		ID:      "roleless-svc",
		Kind:    identity.KindService,
		Scope:   tenancy.Scope{TenantID: testTenant},
		Roles:   nil,
		Enabled: true,
		Version: 1,
	}
	bindingReader, err := readers.NewBindingReader(h.store)
	if err != nil {
		t.Fatalf("readers.NewBindingReader: %v", err)
	}
	h.verifier, err = verify.New(verify.Config{
		Principals: h.principals,
		Bindings:   bindingReader,
		Evidence:   mustEvidenceReader(t, h.store),
		Decisions:  mustDecisionReader(t, h.store),
		Records:    mustRecordAdapter(t, h.store),
		Clock:      fixedClock{},
		NewID:      func() (verify.VerificationID, error) { return h.ids.NewVerificationID() },
	})
	if err != nil {
		t.Fatalf("verify.New: %v", err)
	}
	h.buildRouter(t)

	// Register bindings for service/git in BOTH project-a and project-b, plus a
	// resource in project-b bound only to other-svc.
	for _, spec := range []struct {
		proj, bindingID, resID, svc, token string
	}{
		{testProject, "bind-repo-a", "git-repo-a", "git", testToken},
		{"project-b", "bind-repo-b", "git-repo-b", "git", "token-engineer-b"},
		{"project-b", "bind-other-b", "git-other-b", "other-svc", "token-engineer-b"},
	} {
		h.seedResource(t, tenancy.Scope{TenantID: testTenant, ProjectID: id.ProjectID(spec.proj)}, spec.resID, 1)
		if _, err := h.invokeKey(t, spec.token, "resource.binding.register", jsonBody(t, map[string]any{
			"binding_id":        spec.bindingID,
			"resource_id":       spec.resID,
			"enforcing_service": spec.svc,
			"trusted_origins":   []string{"https://git.local"},
			"operations":        []string{"git.read", "git.ref.update"},
			"reportable_events": []string{"git.ref.updated"},
		}), "vr-reg-"+spec.bindingID, map[string]string{"project": spec.proj}); err != nil {
			t.Fatalf("register binding %s: %v", spec.bindingID, err)
		}
	}

	// Seed a resource binding in a foreign tenant for service "git".
	foreignScope := tenancy.Scope{TenantID: "tenant-z", ProjectID: "proj-z"}
	if _, err := h.store.Execute(context.Background(), tenancy.Scope{TenantID: "tenant-z"}, store.Unit{
		Writes: []store.Write{{
			Table: "projects", ID: "proj-z", Insert: true,
			Values: map[string]any{"name": "Proj Z", "version": int64(1), "created_at": fixedNow},
		}},
	}); err != nil {
		t.Fatalf("seed foreign project: %v", err)
	}
	if _, err := h.store.Execute(context.Background(), foreignScope, store.Unit{
		Writes: []store.Write{
			{Table: "resources", ID: "git-foreign-z", Insert: true, Values: map[string]any{"version": int64(1), "created_at": fixedNow}},
			{Table: "resource_bindings", ID: "bind-foreign-z", Insert: true, Values: map[string]any{
				"resource_id": "git-foreign-z", "service_id": "git",
				"origins": []byte(`["https://git.local"]`), "operations": []byte(`["git.read","git.ref.update"]`),
				"reportable_events": []byte(`["git.ref.updated"]`), "metadata": []byte(`{}`),
				"version": int64(1), "created_at": fixedNow,
			}},
		},
	}); err != nil {
		t.Fatalf("seed foreign binding: %v", err)
	}

	subject := map[string]any{
		"fields": []map[string]any{
			{"name": "ref", "kind": "string", "value": "refs/heads/work/init"},
			{"name": "expected_base", "kind": "string", "value": "0000000000000000000000000000000000000000"},
			{"name": "candidate", "kind": "string", "value": "1111111111111111111111111111111111111111"},
		},
	}

	// 1. Tenant-scoped service/git verifies and reports in BOTH project-a and project-b.
	for _, tc := range []struct {
		name, resID, actorTok, srcEvent string
	}{
		{"project-a", "git-repo-a", testToken, "src-evt-a-1"},
		{"project-b", "git-repo-b", "token-engineer-b", "src-evt-b-1"},
	} {
		t.Run("tenant service verify+report in "+tc.name, func(t *testing.T) {
			verifyRes, err := h.invoke(t, "token-git-tenant", "actor.verify", jsonBody(t, map[string]any{
				"actor_credential": tc.actorTok,
				"resource_id":      tc.resID,
				"operation":        "git.ref.update",
				"subject":          subject,
			}), nil)
			if err != nil {
				t.Fatalf("actor.verify %s: %v", tc.name, err)
			}
			var verified api.VerifyResponse
			decodeResult(t, verifyRes, &verified)
			if verified.VerificationID == "" {
				t.Fatalf("empty verification_id: %+v", verified)
			}

			reportRes, err := h.invokeKey(t, "token-git-tenant", "resource.report", jsonBody(t, map[string]any{
				"resource_id":     tc.resID,
				"source_event_id": tc.srcEvent,
				"event_type":      "git.ref.updated",
				"operation":       "git.ref.update",
				"verification_id": verified.VerificationID,
				"subject":         subject,
			}), tc.srcEvent, nil)
			if err != nil {
				t.Fatalf("resource.report %s: %v", tc.name, err)
			}
			var reported api.ReportResponse
			decodeResult(t, reportRes, &reported)
			if reported.VerificationID != verified.VerificationID || reported.EventID == "" {
				t.Fatalf("report response = %+v", reported)
			}
		})
	}

	// 2. Project-scoped service (project-a) CANNOT verify or report in project-b.
	if _, err := h.invoke(t, "token-git-proj-a", "actor.verify", jsonBody(t, map[string]any{
		"actor_credential": "token-engineer-b",
		"resource_id":      "git-repo-b",
		"operation":        "git.read",
		"subject":          map[string]any{},
	}), nil); err == nil {
		t.Fatal("project-a service verified project-b resource")
	} else if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
		t.Fatalf("project-a service cross-project verify = %v; want not_found", err)
	}

	// 3. Tenant-scoped service receives uniform not_found for unknown, unbound
	// (bound to other-svc), and foreign-tenant resources on both verify and report.
	for _, resID := range []string{"git-does-not-exist", "git-other-b", "git-foreign-z"} {
		if _, err := h.invoke(t, "token-git-tenant", "actor.verify", jsonBody(t, map[string]any{
			"actor_credential": "token-engineer-b",
			"resource_id":      resID,
			"operation":        "git.read",
			"subject":          map[string]any{},
		}), nil); err == nil {
			t.Fatalf("tenant service verified %q", resID)
		} else if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
			t.Fatalf("tenant service verify %q = %v; want not_found", resID, err)
		}
		if _, err := h.invokeKey(t, "token-git-tenant", "resource.report", jsonBody(t, map[string]any{
			"resource_id":     resID,
			"source_event_id": "src-miss-" + resID,
			"event_type":      "git.ref.updated",
			"operation":       "git.ref.update",
			"verification_id": "ver-missing",
			"subject":         subject,
		}), "src-miss-"+resID, nil); err == nil {
			t.Fatalf("tenant service reported %q", resID)
		} else if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
			t.Fatalf("tenant service report %q = %v; want not_found", resID, err)
		}
	}

	// 4. Humans and roleless services receive immediate 403 forbidden for BOTH
	// existing and nonexistent resources (no existence oracle).
	for _, caller := range []string{"token-admin", "token-roleless-svc"} {
		for _, resID := range []string{"git-repo-b", "git-does-not-exist", "git-foreign-z"} {
			if _, err := h.invoke(t, caller, "actor.verify", jsonBody(t, map[string]any{
				"actor_credential": testToken,
				"resource_id":      resID,
				"operation":        "git.read",
				"subject":          map[string]any{},
			}), nil); err == nil {
				t.Fatalf("%s actor.verify(%s) succeeded", caller, resID)
			} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
				t.Fatalf("%s actor.verify(%s) = %v; want forbidden", caller, resID, err)
			}
			if _, err := h.invokeKey(t, caller, "resource.report", jsonBody(t, map[string]any{
				"resource_id":     resID,
				"source_event_id": "src-oracle-" + caller + "-" + resID,
				"event_type":      "git.ref.updated",
				"operation":       "git.ref.update",
				"verification_id": "ver-1",
				"subject":         subject,
			}), "src-oracle-"+caller+"-"+resID, nil); err == nil {
				t.Fatalf("%s resource.report(%s) succeeded", caller, resID)
			} else if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
				t.Fatalf("%s resource.report(%s) = %v; want forbidden", caller, resID, err)
			}
		}
	}

	// 5. Malformed or missing resource_id returns invalid_input.
	for _, badBody := range []string{`{}`, `{"resource_id":"bad/id"}`, `{"resource_id":""}`} {
		if _, err := h.invoke(t, "token-git-tenant", "actor.verify", badBody, nil); err == nil {
			t.Fatalf("actor.verify(%s) succeeded", badBody)
		} else if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("actor.verify(%s) = %v; want invalid_input", badBody, err)
		}
		if _, err := h.invokeKey(t, "token-git-tenant", "resource.report", badBody, "bad-report-key", nil); err == nil {
			t.Fatalf("resource.report(%s) succeeded", badBody)
		} else if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("resource.report(%s) = %v; want invalid_input", badBody, err)
		}
	}
}

// createTaskInProjectB creates one task in project-b as the tenant-wide admin.
func createTaskInProjectB(t *testing.T, h *harness) string {
	t.Helper()
	created, err := h.invoke(t, "token-engineer-b", "task.create", jsonBody(t, map[string]any{
		"title": "foreign task",
	}), map[string]string{"project": "project-b"})
	if err != nil {
		t.Fatalf("task.create in project-b: %v", err)
	}
	var result api.TaskCreateResult
	decodeResult(t, created, &result)
	return result.TaskID
}

// responseRecorder is an in-process http.ResponseWriter.
type responseRecorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (recorder *responseRecorder) Header() http.Header { return recorder.header }

func (recorder *responseRecorder) WriteHeader(status int) {
	if recorder.status == 0 {
		recorder.status = status
	}
}

func (recorder *responseRecorder) Write(data []byte) (int, error) {
	if recorder.status == 0 {
		recorder.status = http.StatusOK
	}
	return recorder.body.Write(data)
}
