package server

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/adapter/store/memory"
	"github.com/optimiware/kodema/internal/adapter/store/units/execution/snapshotdoc"
	"github.com/optimiware/kodema/internal/control/authorize"
	"github.com/optimiware/kodema/internal/control/event"
	"github.com/optimiware/kodema/internal/control/gateway"
	"github.com/optimiware/kodema/internal/control/identity"
	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/control/request"
	"github.com/optimiware/kodema/internal/control/secret"
	"github.com/optimiware/kodema/internal/control/service"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/id"
)

const (
	llmProxyTestServiceID    = id.PrincipalID("opencode-project-1")
	llmProxyTestServiceToken = "kodsvc.opencode-project-1.validmac"
)

type serviceRequestSeed struct {
	Scope          tenancy.Scope
	TaskID         id.TaskID
	ExecutionID    id.ExecutionID
	ActionID       id.ActionID
	ExecState      engine.ExecutionState
	ActionKind     string
	ActionState    engine.ActionState
	Decision       string
	Assignment     request.Assignment
	Deadline       time.Time
	PipelineBudget *definition.Budget
}

func seedServiceRequestExecution(t *testing.T, mem *memory.Store, cfg serviceRequestSeed) {
	t.Helper()
	ctx := context.Background()

	_, _ = mem.Execute(ctx, tenancy.Scope{TenantID: cfg.Scope.TenantID}, store.Unit{
		Writes: []store.Write{{
			Table:  "tenants",
			ID:     string(cfg.Scope.TenantID),
			Insert: true,
			Values: map[string]any{"name": string(cfg.Scope.TenantID), "version": int64(1), "created_at": time.Now().UTC()},
		}},
	})
	_, _ = mem.Execute(ctx, cfg.Scope, store.Unit{
		Writes: []store.Write{{
			Table:  "projects",
			ID:     string(cfg.Scope.ProjectID),
			Insert: true,
			Values: map[string]any{"name": string(cfg.Scope.ProjectID), "version": int64(1), "created_at": time.Now().UTC()},
		}},
	})
	if cfg.TaskID != "" {
		_, _ = mem.Execute(ctx, cfg.Scope, store.Unit{
			Writes: []store.Write{{
				Table:  "tasks",
				ID:     string(cfg.TaskID),
				Insert: true,
				Values: map[string]any{
					"title":             "service llm task",
					"description":       "test task",
					"state":             "open",
					"lifecycle_version": int64(1),
					"content_version":   int64(1),
					"created_at":        time.Now().UTC(),
					"updated_at":        time.Now().UTC(),
				},
			}},
		})
	}

	assignStr := cfg.Assignment.String()
	if assignStr == "" {
		assignStr = "service/" + string(llmProxyTestServiceID)
	}
	snap, err := pipeline.NewSnapshot(pipeline.SnapshotInput{
		YAML:   []byte("actions:\n  - name: implement\n    request: " + assignStr + "\n    subject:\n      mode: implement\n    outputs: [branch, candidate, summary_key]\n"),
		Budget: cfg.PipelineBudget,
		TaskID: cfg.TaskID,
	})
	if err != nil {
		t.Fatalf("NewSnapshot: %v", err)
	}
	admitted := snapshotdoc.Admitted{
		Snapshot: snap,
		Actions: []pipeline.AdmittedAction{{
			Index:   0,
			Name:    "implement",
			Kind:    definition.ActionKindRequest,
			Timeout: 10 * time.Minute,
			Request: &definition.Request{
				Assignment: assignStr,
				Subject: definition.NewSubject(map[string]definition.SubjectValue{
					"mode": {Kind: definition.SubjectLiteral, Literal: "implement"},
				}),
				Outputs: []id.OutputKey{"branch", "candidate", "summary_key"},
			},
		}},
	}
	snapBytes, err := snapshotdoc.Encode(admitted)
	if err != nil {
		t.Fatalf("snapshotdoc.Encode: %v", err)
	}

	var assignmentBytes any
	if cfg.Assignment.Valid() {
		raw, err := json.Marshal(cfg.Assignment)
		if err != nil {
			t.Fatalf("marshal assignment: %v", err)
		}
		assignmentBytes = raw
	}
	var nullableTask any
	if cfg.TaskID != "" {
		nullableTask = string(cfg.TaskID)
	}
	var nullableDecision any
	if cfg.Decision != "" {
		nullableDecision = cfg.Decision
	}
	kind := cfg.ActionKind
	if kind == "" {
		kind = string(definition.ActionKindRequest)
	}
	var nullableDeadline any
	if !cfg.Deadline.IsZero() {
		nullableDeadline = cfg.Deadline
	}

	if _, err := mem.Execute(ctx, cfg.Scope, store.Unit{
		Writes: []store.Write{
			{
				Table:  "executions",
				ID:     string(cfg.ExecutionID),
				Insert: true,
				Values: map[string]any{
					"task_id":      nullableTask,
					"principal_id": "exec-principal-" + string(cfg.ExecutionID),
					"state":        string(cfg.ExecState),
					"version":      int64(1),
					"yaml_sha256":  snap.YAMLSHA256[:],
					"snapshot":     snapBytes,
					"workspace_id": "ws-" + string(cfg.ExecutionID),
					"created_at":   time.Now().UTC(),
				},
			},
			{
				Table:  "actions",
				ID:     string(cfg.ActionID),
				Insert: true,
				Values: map[string]any{
					"execution_id":       string(cfg.ExecutionID),
					"index":              int64(0),
					"name":               "implement",
					"kind":               kind,
					"state":              string(cfg.ActionState),
					"version":            int64(1),
					"timeout_ms":         int64(600000),
					"deadline":           nullableDeadline,
					"request_assignment": assignmentBytes,
					"request_decision":   nullableDecision,
					"staged_outputs_set": false,
					"launch_state":       string(engine.LaunchNotIssued),
				},
			},
		},
	}); err != nil {
		t.Fatalf("seed service execution %s: %v", cfg.ExecutionID, err)
	}
}

func rolelessServicePrincipal(scope tenancy.Scope, serviceID id.PrincipalID, enabled bool) identity.Principal {
	return identity.Principal{
		ID:      serviceID,
		Kind:    identity.KindService,
		Scope:   scope,
		Roles:   nil, // strict roles: []
		Enabled: enabled,
		Version: 1,
	}
}

type customActionBudgetReader struct {
	base   gateway.BudgetReader
	action gateway.BudgetLimits
}

func (r customActionBudgetReader) ReadExecutionBudget(ctx context.Context, scope tenancy.Scope, execID id.ExecutionID, actionID id.ActionID) (gateway.ExecutionBudgetContext, error) {
	res, err := r.base.ReadExecutionBudget(ctx, scope, execID, actionID)
	if err != nil {
		return res, err
	}
	res.Action = r.action
	return res, nil
}

func (r customActionBudgetReader) ReadTaskExecutions(ctx context.Context, scope tenancy.Scope, taskID id.TaskID) ([]id.ExecutionID, error) {
	return r.base.ReadTaskExecutions(ctx, scope, taskID)
}

func (r customActionBudgetReader) ReadUsageTotals(ctx context.Context, scope tenancy.Scope, execIDs []id.ExecutionID) ([]gateway.UsageSummaryRow, error) {
	return r.base.ReadUsageTotals(ctx, scope, execIDs)
}

func TestLLMProxyServiceAttributedAnthropicAndOpenAIUsage(t *testing.T) {
	clock := newFakeClock()
	scope := modelTestScope("project-1")
	mem := memory.NewWithClock(clock.Now)

	execID := id.ExecutionID("exec-svc-1")
	actionID := id.ActionID("action-svc-1")
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		TaskID:      "task-svc-1",
		ExecutionID: execID,
		ActionID:    actionID,
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(30 * time.Minute),
	})

	var mu sync.Mutex
	var capturedHeaders []http.Header

	anthropicUpstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		capturedHeaders = append(capturedHeaders, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, frame := range []string{
			`data: {"type":"message_start","message":{"model":"claude-sonnet-4","usage":{"input_tokens":120}}}`,
			`data: {"type":"message_delta","usage":{"output_tokens":80}}`,
			`data: {"type":"message_stop"}`,
		} {
			_, _ = io.WriteString(w, frame+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer anthropicUpstream.Close()

	openaiUpstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		capturedHeaders = append(capturedHeaders, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"resp-1","model":"gpt-5.4","usage":{"input_tokens":50,"output_tokens":25,"total_tokens":75}}`)
	}))
	defer openaiUpstream.Close()

	// Route both TLS test servers through a custom RoundTripper.
	multiTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.HasPrefix(req.URL.String(), anthropicUpstream.URL) {
			return anthropicUpstream.Client().Transport.RoundTrip(req)
		}
		return openaiUpstream.Client().Transport.RoundTrip(req)
	})

	modelsCfg := &ModelsConfig{
		Providers: []ModelProviderConfig{
			{
				Scope:      scope,
				Name:       "anthropic",
				BaseURL:    anthropicUpstream.URL,
				SecretName: llmProxyTestAnthropicSecretName,
			},
			{
				Scope:      scope,
				Name:       "openai",
				BaseURL:    openaiUpstream.URL,
				SecretName: llmProxyTestOpenAISecretName,
			},
		},
		SecretRules: []secret.NameRule{{
			Scope: scope,
			Names: []id.SecretName{llmProxyTestAnthropicSecretName, llmProxyTestOpenAISecretName},
		}},
		Transport: multiTransport,
	}

	cipher := newTestCipher(t)
	var usageSeq atomic.Int64
	var eventSeq atomic.Int64
	handler, stop, err := newLLMProxyHandler(llmProxyDependencies{
		Config:         modelsCfg,
		Profile:        ProfileDevelopment,
		InstallationID: "installation-1",
		Controller:     mem.Controller(),
		Store:          mem,
		Scanner:        mem,
		Principals: staticPrincipalResolver{byToken: map[string]identity.Principal{
			llmProxyTestServiceToken: rolelessServicePrincipal(scope, llmProxyTestServiceID, true),
		}},
		Authority: allowAllToolAuthority{},
		Lookup: mapSecretLookup{
			cipher: cipher,
			values: map[id.SecretName]string{
				llmProxyTestAnthropicSecretName: llmProxyTestAnthropicSecret,
				llmProxyTestOpenAISecretName:    llmProxyTestOpenAISecret,
			},
		},
		Cipher: cipher,
		NewUsageID: func() (gateway.UsageID, error) {
			return gateway.UsageID("usage-svc-" + string(rune('a'+usageSeq.Add(1)))), nil
		},
		NewEventID: func() (id.EventID, error) {
			return id.EventID("evt-svc-" + string(rune('a'+eventSeq.Add(1)))), nil
		},
		Clock:         clock,
		WatchInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("newLLMProxyHandler: %v", err)
	}
	if stop != nil {
		defer func() { _ = stop(context.Background()) }()
	}

	proxyServer := httptest.NewServer(handler)
	defer proxyServer.Close()

	// 1. Anthropic streaming call with kodsvc. + attribution headers
	reqAnt, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/llm/anthropic/v1/messages",
		strings.NewReader(`{"model":"claude-sonnet-4","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	reqAnt.Header.Set("Content-Type", "application/json")
	reqAnt.Header.Set("Authorization", "Bearer "+llmProxyTestServiceToken)
	reqAnt.Header.Set("X-Kodema-Execution", string(execID))
	reqAnt.Header.Set("X-Kodema-Action", string(actionID))

	respAnt, err := http.DefaultClient.Do(reqAnt)
	if err != nil {
		t.Fatalf("anthropic call: %v", err)
	}
	bodyAnt, _ := io.ReadAll(respAnt.Body)
	_ = respAnt.Body.Close()
	if respAnt.StatusCode != http.StatusOK {
		t.Fatalf("anthropic status = %d; want 200 (body=%s)", respAnt.StatusCode, string(bodyAnt))
	}

	// 2. OpenAI non-streaming call with x-api-key: kodsvc. + attribution headers
	reqOAI, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/llm/openai/v1/responses",
		strings.NewReader(`{"model":"gpt-5.4","input":"hello"}`))
	reqOAI.Header.Set("Content-Type", "application/json")
	reqOAI.Header.Set("x-api-key", llmProxyTestServiceToken)
	reqOAI.Header.Set("X-Kodema-Execution", string(execID))
	reqOAI.Header.Set("X-Kodema-Action", string(actionID))

	respOAI, err := http.DefaultClient.Do(reqOAI)
	if err != nil {
		t.Fatalf("openai call: %v", err)
	}
	bodyOAI, _ := io.ReadAll(respOAI.Body)
	_ = respOAI.Body.Close()
	if respOAI.StatusCode != http.StatusOK {
		t.Fatalf("openai status = %d; want 200 (body=%s)", respOAI.StatusCode, string(bodyOAI))
	}

	// Verify upstream received provider secrets and ZERO kodsvc/attribution headers.
	mu.Lock()
	headersCopy := append([]http.Header(nil), capturedHeaders...)
	mu.Unlock()
	if len(headersCopy) != 2 {
		t.Fatalf("captured %d upstream requests; want 2", len(headersCopy))
	}
	if got := headersCopy[0].Get("x-api-key"); got != llmProxyTestAnthropicSecret {
		t.Fatalf("anthropic upstream x-api-key = %q; want provider secret", got)
	}
	if got := headersCopy[1].Get("Authorization"); got != "Bearer "+llmProxyTestOpenAISecret {
		t.Fatalf("openai upstream Authorization = %q; want Bearer provider secret", got)
	}
	for i, hdrs := range headersCopy {
		if hdrs.Get("X-Kodema-Execution") != "" || hdrs.Get("X-Kodema-Action") != "" {
			t.Fatalf("upstream request %d leaked X-Kodema-* attribution headers: %+v", i, hdrs)
		}
		for name, vals := range hdrs {
			for _, v := range vals {
				if strings.Contains(v, "kodsvc.") || strings.Contains(v, llmProxyTestServiceToken) {
					t.Fatalf("upstream request %d header %s leaked service token", i, name)
				}
			}
		}
	}

	// Verify model_usage rows were recorded with execution_id, action_id, and service principal_id.
	page, err := mem.Query(context.Background(), scope, store.ScanQuery{
		Table:   gateway.UsageTable,
		Equals:  map[string]any{"execution_id": string(execID)},
		OrderBy: []store.Order{{Column: "recorded_at"}, {Column: "id"}},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("query model_usage: %v", err)
	}
	if len(page.Rows) != 2 {
		t.Fatalf("model_usage rows = %d; want 2", len(page.Rows))
	}
	var totalTokens int64
	for _, row := range page.Rows {
		if got := row.Values["principal_id"]; got != string(llmProxyTestServiceID) {
			t.Errorf("usage principal_id = %v; want %q", got, llmProxyTestServiceID)
		}
		if got := row.Values["execution_id"]; got != string(execID) {
			t.Errorf("usage execution_id = %v; want %q", got, execID)
		}
		if got := row.Values["action_id"]; got != string(actionID) {
			t.Errorf("usage action_id = %v; want %q", got, actionID)
		}
		if v, ok := row.Values["total_tokens"].(int64); ok {
			totalTokens += v
		}
	}
	if totalTokens != 275 {
		t.Fatalf("attributed total_tokens = %d; want 275 (200 + 75)", totalTokens)
	}
}

func TestLLMProxyServiceRefusalsFailClosed(t *testing.T) {
	clock := newFakeClock()
	scope := modelTestScope("project-1")
	otherProject := modelTestScope("project-2")
	otherTenant := tenancy.Scope{TenantID: "tenant-2", ProjectID: "project-1"}
	mem := memory.NewWithClock(clock.Now)

	// Seed valid running request action in scope.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		TaskID:      "task-valid",
		ExecutionID: "exec-valid",
		ActionID:    "action-valid",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed wrong service assignment.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		ExecutionID: "exec-other-svc",
		ActionID:    "action-other-svc",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: "opencode-other"},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed human role assignment.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		ExecutionID: "exec-role",
		ActionID:    "action-role",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentRole, Role: tenancy.RoleReviewer},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed container action instead of request action.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		ExecutionID: "exec-container",
		ActionID:    "action-container",
		ExecState:   engine.ExecutionRunning,
		ActionKind:  "container",
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed already resolved request action.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		ExecutionID: "exec-resolved",
		ActionID:    "action-resolved",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionSucceeded,
		Decision:    "pass",
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed cancelled execution.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		ExecutionID: "exec-cancelled",
		ActionID:    "action-cancelled",
		ExecState:   engine.ExecutionCancelled,
		ActionState: engine.ActionCancelled,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed expired deadline action.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       scope,
		ExecutionID: "exec-expired",
		ActionID:    "action-expired",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(-time.Second),
	})
	// Seed cross-project execution.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       otherProject,
		ExecutionID: "exec-project-2",
		ActionID:    "action-project-2",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(time.Hour),
	})
	// Seed cross-tenant execution.
	seedServiceRequestExecution(t, mem, serviceRequestSeed{
		Scope:       otherTenant,
		ExecutionID: "exec-tenant-2",
		ActionID:    "action-tenant-2",
		ExecState:   engine.ExecutionRunning,
		ActionState: engine.ActionRunning,
		Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
		Deadline:    clock.Now().Add(time.Hour),
	})

	var upstreamHits atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	modelsCfg := &ModelsConfig{
		Providers: []ModelProviderConfig{{
			Scope:      scope,
			Name:       "openai",
			BaseURL:    upstream.URL,
			SecretName: llmProxyTestOpenAISecretName,
		}},
		SecretRules: []secret.NameRule{{
			Scope: scope,
			Names: []id.SecretName{llmProxyTestOpenAISecretName},
		}},
		Transport: upstream.Client().Transport,
	}

	tenantWideService := rolelessServicePrincipal(tenancy.Scope{TenantID: scope.TenantID}, "svc-tenant-wide", true)
	principals := map[string]identity.Principal{
		llmProxyTestServiceToken:       rolelessServicePrincipal(scope, llmProxyTestServiceID, true),
		"kodsvc.disabled.token":        rolelessServicePrincipal(scope, llmProxyTestServiceID, false),
		"kodsvc.svc-tenant-wide.token": tenantWideService,
	}

	cipher := newTestCipher(t)

	cases := []struct {
		name       string
		token      string
		execHeader []string
		actHeader  []string
		query      string
		authority  authorize.AuthorityReader
		wantStatus int
	}{
		{
			name:       "expired or revoked token",
			token:      "kodsvc.opencode-project-1.revoked",
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing attribution headers",
			token:      llmProxyTestServiceToken,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "missing action header",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "missing execution header",
			token:      llmProxyTestServiceToken,
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "duplicate execution header",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid", "exec-valid"},
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "malformed execution header",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"../bad exec"},
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "malformed action header",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"../bad action"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "service disabled at principal",
			token:      "kodsvc.disabled.token",
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "service disabled at live authority check",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			authority:  deniedModelAuthority{check: authorize.CheckServiceEnabled},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "tenant-wide service account refused",
			token:      "kodsvc.svc-tenant-wide.token",
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "wrong service ID assigned",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-other-svc"},
			actHeader:  []string{"action-other-svc"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "human role assignment refused",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-role"},
			actHeader:  []string{"action-role"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "container action refused for service",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-container"},
			actHeader:  []string{"action-container"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "action belongs to different execution",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-other-svc"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "action already resolved",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-resolved"},
			actHeader:  []string{"action-resolved"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "execution cancelled",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-cancelled"},
			actHeader:  []string{"action-cancelled"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "action deadline expired",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-expired"},
			actHeader:  []string{"action-expired"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "cross-project execution refused",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-project-2"},
			actHeader:  []string{"action-project-2"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "cross-tenant execution refused",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-tenant-2"},
			actHeader:  []string{"action-tenant-2"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "nonexistent execution refused",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-missing"},
			actHeader:  []string{"action-valid"},
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "service token smuggled in query string",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			query:      "?key=kodsvc.leaked",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "percent-encoded service token prefix smuggled in query value",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			query:      "?key=kodsvc%2eleaked",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "percent-encoded service token prefix smuggled in query param name",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			query:      "?kodsvc%2etoken=1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "percent-encoded execution token prefix smuggled in query value",
			token:      llmProxyTestServiceToken,
			execHeader: []string{"exec-valid"},
			actHeader:  []string{"action-valid"},
			query:      "?key=kodex%2Eleaked",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			auth := tc.authority
			if auth == nil {
				auth = allowAllToolAuthority{}
			}
			handler, stop, err := newLLMProxyHandler(llmProxyDependencies{
				Config:         modelsCfg,
				Profile:        ProfileDevelopment,
				InstallationID: "installation-1",
				Controller:     mem.Controller(),
				Store:          mem,
				Scanner:        mem,
				Principals:     staticPrincipalResolver{byToken: principals},
				Authority:      auth,
				Lookup: mapSecretLookup{
					cipher: cipher,
					values: map[id.SecretName]string{llmProxyTestOpenAISecretName: llmProxyTestOpenAISecret},
				},
				Cipher:        cipher,
				Clock:         clock,
				WatchInterval: 25 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("newLLMProxyHandler: %v", err)
			}
			if stop != nil {
				defer func() { _ = stop(context.Background()) }()
			}

			req := httptest.NewRequest(http.MethodPost, "/llm/openai/v1/responses"+tc.query, strings.NewReader(`{"model":"gpt-5.4"}`))
			req.Header.Set("Authorization", "Bearer "+tc.token)
			for _, v := range tc.execHeader {
				req.Header.Add("X-Kodema-Execution", v)
			}
			for _, v := range tc.actHeader {
				req.Header.Add("X-Kodema-Action", v)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d; want %d (body=%s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if hits := upstreamHits.Load(); hits != 0 {
				t.Fatalf("upstream received %d hits; want 0", hits)
			}
		})
	}
}

func TestLLMProxyServiceEnforcesActionExecutionAndTaskBudgets(t *testing.T) {
	t.Run("action execution and task budgets return 429 and emit model.budget_exceeded", func(t *testing.T) {
		clock := newFakeClock()
		scope := modelTestScope("project-1")
		mem := memory.NewWithClock(clock.Now)

		seedServiceRequestExecution(t, mem, serviceRequestSeed{
			Scope:          scope,
			TaskID:         "task-budget-1",
			ExecutionID:    "exec-budget-1",
			ActionID:       "action-budget-1",
			ExecState:      engine.ExecutionRunning,
			ActionState:    engine.ActionRunning,
			Assignment:     request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
			Deadline:       clock.Now().Add(time.Hour),
			PipelineBudget: &definition.Budget{MaxTokens: 200},
		})
		seedServiceRequestExecution(t, mem, serviceRequestSeed{
			Scope:       scope,
			TaskID:      "task-budget-1",
			ExecutionID: "exec-budget-2",
			ActionID:    "action-budget-2",
			ExecState:   engine.ExecutionRunning,
			ActionState: engine.ActionRunning,
			Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
			Deadline:    clock.Now().Add(time.Hour),
		})

		var upstreamHits atomic.Int32
		upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			upstreamHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"id":"resp-1","model":"gpt-5.4","usage":{"input_tokens":60,"output_tokens":40,"total_tokens":100}}`)
		}))
		defer upstream.Close()

		modelsCfg := &ModelsConfig{
			Providers: []ModelProviderConfig{{
				Scope:      scope,
				Name:       "openai",
				BaseURL:    upstream.URL,
				SecretName: llmProxyTestOpenAISecretName,
			}},
			Budgets: gateway.BudgetConfig{
				DefaultTask: gateway.BudgetLimits{MaxTokens: 200},
			},
			SecretRules: []secret.NameRule{{
				Scope: scope,
				Names: []id.SecretName{llmProxyTestOpenAISecretName},
			}},
			Transport: upstream.Client().Transport,
		}

		var usageSeq, eventSeq atomic.Int64
		budgetReader := customActionBudgetReader{
			base:   newScopedBudgetReader(mem, mem),
			action: gateway.BudgetLimits{MaxTokens: 100},
		}
		budgetRecorder := &gateway.StoreBudgetEventRecorder{
			Executor: mem,
			Clock:    clock,
			NewID: func() (id.EventID, error) {
				return id.EventID("evt-budget-" + string(rune('a'+eventSeq.Add(1)))), nil
			},
		}
		tracker, err := gateway.NewBudgetTracker(modelsCfg.Budgets, nil, budgetReader, budgetRecorder)
		if err != nil {
			t.Fatalf("NewBudgetTracker: %v", err)
		}

		cipher := newTestCipher(t)
		handler, stop, err := newLLMProxyHandler(llmProxyDependencies{
			Config:         modelsCfg,
			Profile:        ProfileDevelopment,
			InstallationID: "installation-1",
			Controller:     mem.Controller(),
			Store:          mem,
			Scanner:        mem,
			BudgetTracker:  tracker,
			Principals: staticPrincipalResolver{byToken: map[string]identity.Principal{
				llmProxyTestServiceToken: rolelessServicePrincipal(scope, llmProxyTestServiceID, true),
			}},
			Authority: allowAllToolAuthority{},
			Lookup: mapSecretLookup{
				cipher: cipher,
				values: map[id.SecretName]string{llmProxyTestOpenAISecretName: llmProxyTestOpenAISecret},
			},
			Cipher: cipher,
			NewUsageID: func() (gateway.UsageID, error) {
				return gateway.UsageID("usage-b-" + string(rune('a'+usageSeq.Add(1)))), nil
			},
			Clock:         clock,
			WatchInterval: 25 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("newLLMProxyHandler: %v", err)
		}
		if stop != nil {
			defer func() { _ = stop(context.Background()) }()
		}

		call := func(execID, actionID string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodPost, "/llm/openai/v1/responses", strings.NewReader(`{"model":"gpt-5.4"}`))
			req.Header.Set("Authorization", "Bearer "+llmProxyTestServiceToken)
			req.Header.Set("X-Kodema-Execution", execID)
			req.Header.Set("X-Kodema-Action", actionID)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			return rec
		}

		// Call 1 on exec-budget-1/action-budget-1 consumes 100 tokens -> succeeds.
		if rec := call("exec-budget-1", "action-budget-1"); rec.Code != http.StatusOK {
			t.Fatalf("call 1 status = %d; want 200 (body=%s)", rec.Code, rec.Body.String())
		}
		// Call 2 on same action hits action budget (100/100) -> 429 budget_exceeded.
		rec2 := call("exec-budget-1", "action-budget-1")
		if rec2.Code != http.StatusTooManyRequests {
			t.Fatalf("call 2 status = %d; want 429 (body=%s)", rec2.Code, rec2.Body.String())
		}
		var err2 api.Error
		if err := json.Unmarshal(rec2.Body.Bytes(), &err2); err != nil || err2.Code != api.CodeBudgetExceeded {
			t.Fatalf("call 2 body = %s; want budget_exceeded", rec2.Body.String())
		}

		// Call 3 on sibling execution exec-budget-2 consumes 100 tokens -> total task tokens = 200.
		if rec := call("exec-budget-2", "action-budget-2"); rec.Code != http.StatusOK {
			t.Fatalf("call 3 status = %d; want 200 (body=%s)", rec.Code, rec.Body.String())
		}
		// Seed a 3rd execution on the same task to prove task budget (200/200) rejects even a fresh action!
		seedServiceRequestExecution(t, mem, serviceRequestSeed{
			Scope:       scope,
			TaskID:      "task-budget-1",
			ExecutionID: "exec-budget-3",
			ActionID:    "action-budget-3",
			ExecState:   engine.ExecutionRunning,
			ActionState: engine.ActionRunning,
			Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
			Deadline:    clock.Now().Add(time.Hour),
		})
		rec4 := call("exec-budget-3", "action-budget-3")
		if rec4.Code != http.StatusTooManyRequests {
			t.Fatalf("call 4 (task budget) status = %d; want 429 (body=%s)", rec4.Code, rec4.Body.String())
		}
		if upstreamHits.Load() != 2 {
			t.Fatalf("upstream hits = %d; want 2", upstreamHits.Load())
		}

		// Verify model.budget_exceeded events were persisted in the scoped store.
		evPage, err := mem.Query(context.Background(), scope, store.ScanQuery{
			Table:   "events",
			Equals:  map[string]any{"type": string(event.TypeModelBudgetExceeded)},
			OrderBy: []store.Order{{Column: "id"}},
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("query events: %v", err)
		}
		if len(evPage.Rows) < 2 {
			t.Fatalf("model.budget_exceeded events = %d; want >= 2", len(evPage.Rows))
		}
	})
}

func TestLLMProxyServiceInstantCutoffOnCancelAndDeadline(t *testing.T) {
	for _, mode := range []string{"cancel_execution", "resolve_action", "expire_deadline", "disable_service"} {
		t.Run(mode, func(t *testing.T) {
			clock := newFakeClock()
			scope := modelTestScope("project-1")
			mem := memory.NewWithClock(clock.Now)

			execID := id.ExecutionID("exec-stream-" + mode)
			actionID := id.ActionID("action-stream-" + mode)
			seedServiceRequestExecution(t, mem, serviceRequestSeed{
				Scope:       scope,
				ExecutionID: execID,
				ActionID:    actionID,
				ExecState:   engine.ExecutionRunning,
				ActionState: engine.ActionRunning,
				Assignment:  request.Assignment{Kind: request.AssignmentService, ServiceID: service.ServiceID(llmProxyTestServiceID)},
				Deadline:    clock.Now().Add(5 * time.Minute),
			})

			firstChunkSent := make(chan struct{})
			upstreamCanceled := make(chan struct{})
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				flusher, _ := w.(http.Flusher)
				_, _ = io.WriteString(w, "data: {\"chunk\":1}\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				close(firstChunkSent)
				<-r.Context().Done()
				close(upstreamCanceled)
			}))
			defer upstream.Close()

			modelsCfg := &ModelsConfig{
				Providers: []ModelProviderConfig{{
					Scope:      scope,
					Name:       "anthropic",
					BaseURL:    upstream.URL,
					SecretName: llmProxyTestAnthropicSecretName,
				}},
				SecretRules: []secret.NameRule{{
					Scope: scope,
					Names: []id.SecretName{llmProxyTestAnthropicSecretName},
				}},
				Transport: upstream.Client().Transport,
			}

			var serviceEnabled atomic.Bool
			serviceEnabled.Store(true)
			resolver := principalResolveFunc(func(_ context.Context, cred string) (identity.Principal, error) {
				if cred != llmProxyTestServiceToken {
					return identity.Principal{}, derivedRefusal()
				}
				return rolelessServicePrincipal(scope, llmProxyTestServiceID, serviceEnabled.Load()), nil
			})

			cipher := newTestCipher(t)
			handler, stop, err := newLLMProxyHandler(llmProxyDependencies{
				Config:         modelsCfg,
				Profile:        ProfileDevelopment,
				InstallationID: "installation-1",
				Controller:     mem.Controller(),
				Store:          mem,
				Scanner:        mem,
				Principals:     resolver,
				Authority:      allowAllToolAuthority{},
				Lookup: mapSecretLookup{
					cipher: cipher,
					values: map[id.SecretName]string{llmProxyTestAnthropicSecretName: llmProxyTestAnthropicSecret},
				},
				Cipher:        cipher,
				Clock:         clock,
				WatchInterval: 15 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("newLLMProxyHandler: %v", err)
			}
			if stop != nil {
				defer func() { _ = stop(context.Background()) }()
			}

			proxyServer := httptest.NewServer(handler)
			defer proxyServer.Close()

			req, _ := http.NewRequest(http.MethodPost, proxyServer.URL+"/llm/anthropic/v1/messages",
				strings.NewReader(`{"model":"claude-sonnet-4","stream":true}`))
			req.Header.Set("Authorization", "Bearer "+llmProxyTestServiceToken)
			req.Header.Set("X-Kodema-Execution", string(execID))
			req.Header.Set("X-Kodema-Action", string(actionID))

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("start stream: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d; want 200", resp.StatusCode)
			}

			select {
			case <-firstChunkSent:
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for first chunk")
			}

			reader := bufio.NewReader(resp.Body)
			line, err := reader.ReadString('\n')
			if err != nil || !strings.Contains(line, `"chunk":1`) {
				t.Fatalf("first chunk read = (%q, %v)", line, err)
			}

			// Trigger live cutoff mid-stream.
			switch mode {
			case "cancel_execution":
				if _, err := mem.Execute(context.Background(), scope, store.Unit{
					Writes: []store.Write{{
						Table:         "executions",
						ID:            string(execID),
						ExpectVersion: 1,
						Values:        map[string]any{"state": string(engine.ExecutionCancelled)},
					}},
				}); err != nil {
					t.Fatalf("cancel execution: %v", err)
				}
			case "resolve_action":
				if _, err := mem.Execute(context.Background(), scope, store.Unit{
					Writes: []store.Write{{
						Table:         "actions",
						ID:            string(actionID),
						ExpectVersion: 1,
						Values: map[string]any{
							"state":            string(engine.ActionSucceeded),
							"request_decision": "pass",
						},
					}},
				}); err != nil {
					t.Fatalf("resolve action: %v", err)
				}
			case "expire_deadline":
				clock.Advance(10 * time.Minute)
			case "disable_service":
				serviceEnabled.Store(false)
			}

			select {
			case <-upstreamCanceled:
			case <-time.After(3 * time.Second):
				t.Fatalf("%s: upstream stream was not canceled promptly", mode)
			}
		})
	}
}

type principalResolveFunc func(context.Context, string) (identity.Principal, error)

func (f principalResolveFunc) Resolve(ctx context.Context, cred string) (identity.Principal, error) {
	return f(ctx, cred)
}
