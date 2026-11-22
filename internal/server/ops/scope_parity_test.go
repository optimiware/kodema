package opsapp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/fault"
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
