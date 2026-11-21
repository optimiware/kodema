package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/userland/sandbox"
)

type testAPIHandlerFunc func(context.Context, api.Invocation) (api.Result, error)

func (f testAPIHandlerFunc) Invoke(ctx context.Context, inv api.Invocation) (api.Result, error) {
	return f(ctx, inv)
}

func TestBrainOpenCodeConfigDisablesLocalTools(t *testing.T) {
	raw, err := GenerateBrainOpenCodeConfig(OpenCodeBrainConfig{}, "http://127.0.0.1:4100", "ocproxy.localkey123456")
	if err != nil {
		t.Fatalf("GenerateBrainOpenCodeConfig: %v", err)
	}
	if err := ValidateBrainOpenCodeConfig(raw); err != nil {
		t.Fatalf("ValidateBrainOpenCodeConfig rejected valid generated config: %v", err)
	}
	if !strings.Contains(string(raw), `"bash": false`) || !strings.Contains(string(raw), `"write": false`) {
		t.Fatalf("generated brain config missing disabled local tools:\n%s", string(raw))
	}

	// Enabling bash must be rejected by ValidateBrainOpenCodeConfig.
	bad := strings.Replace(string(raw), `"bash": false`, `"bash": true`, 1)
	if err := ValidateBrainOpenCodeConfig([]byte(bad)); err == nil {
		t.Fatal("ValidateBrainOpenCodeConfig accepted config with bash enabled")
	}
	if err := ValidateBrainOpenCodeConfig([]byte(`{"tools":{}}`)); err == nil {
		t.Fatal("ValidateBrainOpenCodeConfig accepted empty tools map")
	}
}

func TestHTTPOpenCodeClientBrainMCPAttachDetachAndSendMessage(t *testing.T) {
	var mu sync.Mutex
	var attachedPayload map[string]any
	var messagePayload map[string]any
	var proxyTurnHeader string
	var detachedPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/session/sess-1/mcp":
			_ = json.Unmarshal(body, &attachedPayload)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		case r.Method == http.MethodPost && r.URL.Path == "/session/sess-1/message":
			_ = json.Unmarshal(body, &messagePayload)
			proxyTurnHeader = r.Header.Get("X-Kodema-Proxy-Turn-Path")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"info":{"id":"m-1"},"parts":[{"type":"text","text":"done"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/session/sess-1/mcp/sandbox":
			detachedPath = r.URL.Path
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}
	mcpCfg := OpenCodeMCPServerConfig{
		Name:    "sandbox",
		Type:    "remote",
		URL:     "http://10.0.0.5:8090/mcp",
		Headers: map[string]string{"Authorization": "Bearer kodsbox.testtoken123456"},
		Enabled: true,
	}
	if err := cli.AttachMCPServer(context.Background(), "sess-1", mcpCfg); err != nil {
		t.Fatalf("AttachMCPServer: %v", err)
	}
	resp, err := cli.SendMessage(context.Background(), "sess-1", OpenCodeMessageRequest{
		Directory:       "/workspace",
		Prompt:          "implement feature",
		Mode:            OpenCodeModeImplement,
		ExecutionID:     "exec-1",
		ActionID:        "act-1",
		DisabledTools:   DefaultBrainDisabledLocalTools,
		MCPServers:      map[string]OpenCodeMCPServerConfig{"sandbox": mcpCfg},
		ProxyPathPrefix: "/turn/exec-1/act-1",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if resp.Text != "done" {
		t.Fatalf("resp.Text = %q; want done", resp.Text)
	}
	if err := cli.DetachMCPServer(context.Background(), "sess-1", "sandbox"); err != nil {
		t.Fatalf("DetachMCPServer: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if attachedPayload["url"] != "http://10.0.0.5:8090/mcp" {
		t.Fatalf("attached MCP payload = %+v", attachedPayload)
	}
	toolsMap, _ := messagePayload["tools"].(map[string]any)
	for _, tool := range DefaultBrainDisabledLocalTools {
		if v, ok := toolsMap[tool]; !ok || v != false {
			t.Errorf("message tools[%q] = %v (present=%v); want false", tool, v, ok)
		}
	}
	if proxyTurnHeader != "/turn/exec-1/act-1" {
		t.Fatalf("X-Kodema-Proxy-Turn-Path = %q; want /turn/exec-1/act-1", proxyTurnHeader)
	}
	if detachedPath != "/session/sess-1/mcp/sandbox" {
		t.Fatalf("detachedPath = %q; want /session/sess-1/mcp/sandbox", detachedPath)
	}
}

func TestKodemaExecutionSandboxManagerAcquireWatchAndConfirmedStop(t *testing.T) {
	// Start a real sandbox.Server to simulate the container's /healthz + /mcp.
	ws := t.TempDir()
	realSrv, err := sandbox.NewServer(sandbox.Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: ws,
		Token:        "kodsbox.initialtesttoken0123456789abcdef",
	})
	if err != nil {
		t.Fatalf("sandbox.NewServer: %v", err)
	}
	srvCtx, cancelSrv := context.WithCancel(context.Background())
	defer cancelSrv()
	baseURL, err := realSrv.Start(srvCtx)
	if err != nil {
		t.Fatalf("realSrv.Start: %v", err)
	}
	defer func() { _ = realSrv.Shutdown(context.Background()) }()

	// Fake Kodema public API handler for pipeline.start, execution.inspect,
	// execution.logs.read, and execution.cancel.
	var mu sync.Mutex
	var startedYAML string
	execState := "running"
	execVersion := uint64(1)
	var execFailure *api.ExecutionFailure
	cancelCalls := 0
	inspectAfterCancel := 0

	handler := testAPIHandlerFunc(func(_ context.Context, inv api.Invocation) (api.Result, error) {
		codec := publicCodecs[inv.Operation]
		mu.Lock()
		defer mu.Unlock()
		switch inv.Operation {
		case OperationPipelineStart:
			var req api.PipelineStartRequest
			_ = json.Unmarshal(inv.Request.Input, &req)
			startedYAML = string(req.YAML)
			body, _ := codec.EncodeResult(api.PipelineStartResult{ExecutionID: "exec-sbox-1", Version: 1})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationExecutionLogsRead:
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
				Chunks: []api.LogChunk{{
					ActionID: "sandbox",
					Stream:   "stdout",
					Data:     []byte("booting...\n" + sandboxReadyLinePrefix + baseURL + "\n"),
				}},
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationInspect:
			if cancelCalls > 0 {
				inspectAfterCancel++
				// Require two polls before confirming terminal state to prove
				// Release actually waits for confirmed stop.
				if inspectAfterCancel >= 2 {
					execState = "cancelled"
					execVersion = 3
				}
			}
			body, _ := codec.EncodeResult(api.ExecutionInspectResult{
				ExecutionID:   "exec-sbox-1",
				State:         execState,
				Version:       execVersion,
				CurrentAction: 0,
				Failure:       execFailure,
				Cleanup:       api.ExecutionCleanup{State: "none"},
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationExecutionCancel:
			cancelCalls++
			execVersion = 2
			body, _ := codec.EncodeResult(api.ExecutionCancelResult{ExecutionVersion: 2})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		default:
			return api.Result{}, fmt.Errorf("unexpected op %s", inv.Operation)
		}
	})

	mgr, err := NewKodemaExecutionSandboxManager(KodemaExecutionSandboxConfig{
		TenantID:       id.TenantID("tenant-1"),
		ProjectID:      id.ProjectID("project-1"),
		Handler:        handler,
		ReadyTimeout:   5 * time.Second,
		HealthInterval: 30 * time.Millisecond,
		StopTimeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewKodemaExecutionSandboxManager: %v", err)
	}

	lease, err := mgr.Acquire(context.Background(), OpenCodeSandboxRequest{
		TenantID:    "tenant-1",
		ProjectID:   "project-1",
		TaskID:      "task-go",
		ExecutionID: "exec-parent-1",
		ActionID:    "act-1",
		Image:       "golang:1.24",
		Repository:  "app/backend",
		BaseRef:     "refs/heads/main",
		WorkBranch:  "refs/heads/work/task-go",
		Caches:      []string{"go-mod", "go-build"},
		Memory:      "2Gi",
		CPU:         "2",
		Port:        8090,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	mu.Lock()
	yamlCopy := startedYAML
	mu.Unlock()
	for _, want := range []string{
		`image: "golang:1.24"`,
		`- "go-mod"`,
		`- "go-build"`,
		`memory: "2Gi"`,
		`cpu: "2"`,
		`/kodema/bin/kodema sandbox serve`,
		`--token-file /tmp/kodema-sandbox.token`,
	} {
		if !strings.Contains(yamlCopy, want) {
			t.Errorf("generated sandbox pipeline YAML missing %q:\n%s", want, yamlCopy)
		}
	}
	if lease.ExecutionID != "exec-sbox-1" || lease.EndpointURL != baseURL+"/mcp" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	// Verify Release invokes execution.cancel and waits for confirmed stop.
	if err := mgr.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release: %v", err)
	}
	mu.Lock()
	gotCancels := cancelCalls
	gotState := execState
	mu.Unlock()
	if gotCancels != 1 || gotState != "cancelled" {
		t.Fatalf("Release did not confirm stop: cancelCalls=%d state=%q", gotCancels, gotState)
	}

	// Now verify mid-turn OOM termination closes lease.Done with diagnostic error.
	mu.Lock()
	execState = "running"
	execVersion = 1
	cancelCalls = 0
	inspectAfterCancel = 0
	execFailure = nil
	mu.Unlock()

	lease2, err := mgr.Acquire(context.Background(), OpenCodeSandboxRequest{
		TenantID:    "tenant-1",
		ProjectID:   "project-1",
		TaskID:      "task-oom",
		ExecutionID: "exec-parent-2",
		ActionID:    "act-2",
		Image:       "golang:1.24",
	})
	if err != nil {
		t.Fatalf("Acquire 2: %v", err)
	}
	defer func() { _ = mgr.Release(context.Background(), lease2) }()

	// Simulate OOMKilled on the sandbox execution.
	mu.Lock()
	execState = "failed"
	execFailure = &api.ExecutionFailure{Kind: "runtime", Reason: "OOMKilled", Detail: "container exceeded 2Gi memory limit"}
	mu.Unlock()

	select {
	case <-lease2.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("lease2.Done did not close after sandbox OOMKilled")
	}
	if err := lease2.Err(); !errors.Is(err, ErrOpenCodeSandboxTerminated) || !strings.Contains(err.Error(), "OOMKilled") {
		t.Fatalf("lease2.Err() = %v; want ErrOpenCodeSandboxTerminated with OOMKilled", err)
	}
}

// realLocalSandboxManager spawns real in-process `sandbox.Server` instances
// backed by real temporary workspaces (and optional real Git repositories) so
// Brain-mode tests exercise authentic HTTP JSON-RPC `/mcp` with real tokens.
type realLocalSandboxManager struct {
	t            *testing.T
	gitBareRepos map[string]string // repo name -> bare git repo path

	mu           sync.Mutex
	acquireCalls []OpenCodeSandboxRequest
	releaseCalls []string
	servers      map[string]*sandbox.Server
	cancelFuncs  map[string]context.CancelFunc
	doneChans    map[string]chan struct{}
	deathErrs    map[string]error
	workspaces   map[string]string
}

func newRealLocalSandboxManager(t *testing.T, bareRepos map[string]string) *realLocalSandboxManager {
	return &realLocalSandboxManager{
		t:            t,
		gitBareRepos: bareRepos,
		servers:      make(map[string]*sandbox.Server),
		cancelFuncs:  make(map[string]context.CancelFunc),
		doneChans:    make(map[string]chan struct{}),
		deathErrs:    make(map[string]error),
		workspaces:   make(map[string]string),
	}
}

func (m *realLocalSandboxManager) Acquire(_ context.Context, req OpenCodeSandboxRequest) (OpenCodeSandboxLease, error) {
	ws := m.t.TempDir()
	// If a real bare repo is registered, clone it into ws and check out work/<taskID>.
	if bare, ok := m.gitBareRepos[req.Repository]; ok {
		runGitCmd(m.t, "", "clone", bare, ws)
		branchShort := strings.TrimPrefix(req.WorkBranch, "refs/heads/")
		if branchShort == "" {
			branchShort = "work/" + req.TaskID
		}
		runGitCmd(m.t, ws, "checkout", "-B", branchShort)
	}

	tok, err := sandbox.GenerateBearerToken()
	if err != nil {
		return OpenCodeSandboxLease{}, err
	}
	srv, err := sandbox.NewServer(sandbox.Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: ws,
		Token:        tok,
	})
	if err != nil {
		return OpenCodeSandboxLease{}, err
	}
	srvCtx, cancel := context.WithCancel(context.Background())
	baseURL, err := srv.Start(srvCtx)
	if err != nil {
		cancel()
		return OpenCodeSandboxLease{}, err
	}

	sandboxID := fmt.Sprintf("real-sbox-%s-%s", req.TaskID, req.ActionID)
	doneCh := make(chan struct{})

	m.mu.Lock()
	m.acquireCalls = append(m.acquireCalls, req)
	m.servers[sandboxID] = srv
	m.cancelFuncs[sandboxID] = cancel
	m.doneChans[sandboxID] = doneCh
	m.workspaces[sandboxID] = ws
	m.mu.Unlock()

	return OpenCodeSandboxLease{
		SandboxID:    sandboxID,
		ExecutionID:  "exec-" + sandboxID,
		EndpointURL:  baseURL + "/mcp",
		HealthURL:    baseURL + "/healthz",
		BearerToken:  tok,
		WorkspaceDir: srv.WorkspaceRoot(),
		Done:         doneCh,
		Err: func() error {
			m.mu.Lock()
			defer m.mu.Unlock()
			return m.deathErrs[sandboxID]
		},
	}, nil
}

func (m *realLocalSandboxManager) Release(_ context.Context, lease OpenCodeSandboxLease) error {
	m.mu.Lock()
	srv := m.servers[lease.SandboxID]
	cancel := m.cancelFuncs[lease.SandboxID]
	m.releaseCalls = append(m.releaseCalls, lease.SandboxID)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if srv != nil {
		return srv.Shutdown(context.Background())
	}
	return nil
}

// SimulateOOMKill terminates the sandbox server abruptly and signals lease.Done.
func (m *realLocalSandboxManager) SimulateOOMKill(sandboxID string, reason string) {
	m.mu.Lock()
	srv := m.servers[sandboxID]
	cancel := m.cancelFuncs[sandboxID]
	doneCh := m.doneChans[sandboxID]
	alreadyDead := m.deathErrs[sandboxID] != nil
	if !alreadyDead {
		m.deathErrs[sandboxID] = fmt.Errorf("%w: %s", ErrOpenCodeSandboxTerminated, reason)
	}
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if srv != nil {
		_ = srv.Shutdown(context.Background())
	}
	if !alreadyDead && doneCh != nil {
		close(doneCh)
	}
}

func initBareGitRepo(t *testing.T) string {
	t.Helper()
	bare := filepath.Join(t.TempDir(), "remote.git")
	seed := t.TempDir()
	runGitCmd(t, "", "init", "--bare", bare)
	runGitCmd(t, "", "clone", bare, seed)
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("# seed\n"), 0o644); err != nil {
		t.Fatalf("WriteFile README: %v", err)
	}
	runGitCmd(t, seed, "add", "README.md")
	runGitCmd(t, seed, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "initial")
	runGitCmd(t, seed, "push", "origin", "HEAD:refs/heads/main")
	runGitCmd(t, bare, "symbolic-ref", "HEAD", "refs/heads/main")
	return bare
}

func runGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

// fakeBrainOpenCodeClient implements both OpenCodeClient and OpenCodeMCPClient
// so tests can inspect MCP attachment/detachment, disabled local tools, and
// make real MCP calls into the attached `kodema sandbox serve` instance!
type fakeBrainOpenCodeClient struct {
	mu           sync.Mutex
	sessionSeq   int
	attached     map[string]OpenCodeMCPServerConfig // sessionID -> cfg
	detached     []string
	abortCalls   []string
	disabledSeen [][]string
	onSend       func(ctx context.Context, sessionID string, req OpenCodeMessageRequest, mcp OpenCodeMCPServerConfig) (OpenCodeMessageResponse, error)
}

func newFakeBrainOpenCodeClient() *fakeBrainOpenCodeClient {
	return &fakeBrainOpenCodeClient{attached: make(map[string]OpenCodeMCPServerConfig)}
}

func (f *fakeBrainOpenCodeClient) Health(context.Context) error { return nil }

func (f *fakeBrainOpenCodeClient) CreateSession(_ context.Context, _ string, _ string) (OpenCodeSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessionSeq++
	return OpenCodeSession{ID: fmt.Sprintf("brain-sess-%d", f.sessionSeq)}, nil
}

func (f *fakeBrainOpenCodeClient) AttachMCPServer(_ context.Context, sessionID string, cfg OpenCodeMCPServerConfig) error {
	f.mu.Lock()
	f.attached[sessionID] = cfg
	f.mu.Unlock()
	return nil
}

func (f *fakeBrainOpenCodeClient) DetachMCPServer(_ context.Context, sessionID string, serverName string) error {
	f.mu.Lock()
	f.detached = append(f.detached, sessionID+"/"+serverName)
	delete(f.attached, sessionID)
	f.mu.Unlock()
	return nil
}

func (f *fakeBrainOpenCodeClient) SendMessage(ctx context.Context, sessionID string, req OpenCodeMessageRequest) (OpenCodeMessageResponse, error) {
	f.mu.Lock()
	f.disabledSeen = append(f.disabledSeen, append([]string(nil), req.DisabledTools...))
	mcpCfg := f.attached[sessionID]
	fn := f.onSend
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, sessionID, req, mcpCfg)
	}
	return OpenCodeMessageResponse{Text: "ok", MessageID: "m-1"}, nil
}

func (f *fakeBrainOpenCodeClient) AbortSession(_ context.Context, sessionID string) error {
	f.mu.Lock()
	f.abortCalls = append(f.abortCalls, sessionID)
	f.mu.Unlock()
	return nil
}

func makeBrainInboxFixture(execID, actionID, taskID, mode, repo, sandboxImage string, outputs []string, deadline time.Time) (api.InboxItem, api.ExecutionInspectResult) {
	fields := []api.SubjectField{
		{Name: OpenCodeFieldMode, Kind: "string", Value: mode},
	}
	if repo != "" {
		fields = append(fields, api.SubjectField{Name: OpenCodeFieldRepository, Kind: "string", Value: repo})
	}
	if sandboxImage != "" {
		fields = append(fields, api.SubjectField{Name: OpenCodeFieldSandboxImage, Kind: "string", Value: sandboxImage})
	}
	subj := &api.Subject{Fields: fields}
	item := api.InboxItem{
		Kind:        "request",
		TaskID:      taskID,
		ExecutionID: execID,
		ActionID:    actionID,
		Assignment:  "service/opencode-brain",
		Version:     1,
		At:          deadline,
		Subject:     subj,
		Outputs:     outputs,
	}
	exec := api.ExecutionInspectResult{
		ExecutionID:   execID,
		TaskID:        taskID,
		State:         "running",
		Version:       1,
		CurrentAction: 0,
		Actions: []api.ExecutionAction{{
			ActionID:   actionID,
			Index:      0,
			Name:       "code",
			Kind:       "request",
			State:      "running",
			Version:    1,
			Deadline:   &deadline,
			Assignment: "service/opencode-brain",
			Subject:    subj,
		}},
		Cleanup: api.ExecutionCleanup{State: "none"},
	}
	return item, exec
}

type brainTestHandler struct {
	mu           sync.Mutex
	inboxItems   []api.InboxItem
	executions   map[string]api.ExecutionInspectResult
	resolveCalls map[string]api.ExecutionRequestResolveRequest // execID -> resolve
	resolveCh    chan api.ExecutionRequestResolveRequest
}

func newBrainTestHandler() *brainTestHandler {
	return &brainTestHandler{
		executions:   make(map[string]api.ExecutionInspectResult),
		resolveCalls: make(map[string]api.ExecutionRequestResolveRequest),
		resolveCh:    make(chan api.ExecutionRequestResolveRequest, 16),
	}
}

func (h *brainTestHandler) Invoke(_ context.Context, inv api.Invocation) (api.Result, error) {
	codec := publicCodecs[inv.Operation]
	h.mu.Lock()
	defer h.mu.Unlock()
	switch inv.Operation {
	case OperationInbox:
		body, _ := codec.EncodeResult(api.TaskInboxResult{Items: append([]api.InboxItem(nil), h.inboxItems...)})
		return api.Result{Operation: inv.Operation, Body: body}, nil
	case OperationInspect:
		var req api.ExecutionInspectRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		ex, ok := h.executions[req.ExecutionID]
		if !ok {
			return api.Result{}, &api.Error{Code: api.CodeNotFound, Message: "not found"}
		}
		body, _ := codec.EncodeResult(ex)
		return api.Result{Operation: inv.Operation, Body: body}, nil
	case OperationTaskRead:
		var req api.TaskReadRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		body, err := codec.EncodeResult(api.TaskReadResult{
			TaskID: req.TaskID,
			Title:  "Task " + req.TaskID,
			State:  "open",
		})
		if err != nil {
			return api.Result{}, err
		}
		return api.Result{Operation: inv.Operation, Body: body}, nil
	case OperationKVPut:
		body, err := codec.EncodeResult(api.KVPutResult{
			Key:         "kv:sha256:" + strings.Repeat("a", 64),
			ProjectID:   "project-1",
			SizeBytes:   32,
			ContentType: "text/markdown; charset=utf-8",
			CreatedAt:   time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC),
		})
		if err != nil {
			return api.Result{}, err
		}
		return api.Result{Operation: inv.Operation, Body: body}, nil
	case OperationEventPublish:
		body, err := codec.EncodeResult(api.EventPublishResult{EventID: "evt-1"})
		if err != nil {
			return api.Result{}, err
		}
		return api.Result{Operation: inv.Operation, Body: body}, nil
	case OperationResolve:
		var req api.ExecutionRequestResolveRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		h.resolveCalls[req.ExecutionID] = req
		// Mark execution terminal so subsequent rescans skip it.
		if ex, ok := h.executions[req.ExecutionID]; ok {
			ex.State = "succeeded"
			if req.Decision == "fail" {
				ex.State = "failed"
			}
			ex.Version++
			h.executions[req.ExecutionID] = ex
		}
		h.resolveCh <- req
		body, err := codec.EncodeResult(api.ExecutionRequestResolveResult{ExecutionVersion: 2, ActionVersion: 2})
		if err != nil {
			return api.Result{}, err
		}
		return api.Result{Operation: inv.Operation, Body: body}, nil
	default:
		return api.Result{}, fmt.Errorf("unexpected op %s", inv.Operation)
	}
}

func TestOpenCodeBrainModeConcurrentPolyglotRealSandboxes(t *testing.T) {
	// Set up two real bare git repositories for Task A (Go) and Task B (Node).
	goBare := initBareGitRepo(t)
	nodeBare := initBareGitRepo(t)

	// Upstream mock Kodema LLM gateway recording per-turn attribution headers.
	var llmMu sync.Mutex
	llmAttrs := make(map[string]string) // execID -> actionID
	upstreamLLM := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		llmMu.Lock()
		llmAttrs[r.Header.Get("X-Kodema-Execution")] = r.Header.Get("X-Kodema-Action")
		llmMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chat-1","ok":true}`))
	}))
	defer upstreamLLM.Close()

	clock := &fixedClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:   upstreamLLM.URL,
		Credentials: staticTestCred("kodsvc.brain.tok"),
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	proxyURL, err := proxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy.Start: %v", err)
	}
	defer func() { _ = proxy.Close(context.Background()) }()

	deadline := clock.Now().Add(5 * time.Minute)
	itemGo, execGo := makeBrainInboxFixture("exec-go", "act-go", "task-go", "implement", "svc/go-api", "golang:1.24", []string{OpenCodeOutputBranch, OpenCodeOutputCandidate, OpenCodeOutputSummaryKey, OpenCodeOutputSessionID}, deadline)
	itemNode, execNode := makeBrainInboxFixture("exec-node", "act-node", "task-node", "implement", "svc/node-web", "node:22", []string{OpenCodeOutputBranch, OpenCodeOutputCandidate, OpenCodeOutputSummaryKey, OpenCodeOutputSessionID}, deadline)

	handler := newBrainTestHandler()
	handler.inboxItems = []api.InboxItem{itemGo, itemNode}
	handler.executions["exec-go"] = execGo
	handler.executions["exec-node"] = execNode

	sboxMgr := newRealLocalSandboxManager(t, map[string]string{
		"svc/go-api":   goBare,
		"svc/node-web": nodeBare,
	})

	// Barrier guarantees BOTH turns are executing concurrently inside the single Brain!
	var arrived atomic.Int32
	bothArrived := make(chan struct{})
	var peerMu sync.Mutex
	peerEndpoints := make(map[string]string) // taskID -> endpointURL
	peerTokens := make(map[string]string)    // taskID -> token

	brainClient := newFakeBrainOpenCodeClient()
	brainClient.onSend = func(ctx context.Context, _ string, req OpenCodeMessageRequest, mcp OpenCodeMCPServerConfig) (OpenCodeMessageResponse, error) {
		if len(req.DisabledTools) < len(DefaultBrainDisabledLocalTools) {
			return OpenCodeMessageResponse{}, fmt.Errorf("expected disabled local tools, got %v", req.DisabledTools)
		}
		if req.ProxyPathPrefix != fmt.Sprintf("/turn/%s/%s", req.ExecutionID, req.ActionID) {
			return OpenCodeMessageResponse{}, fmt.Errorf("unexpected ProxyPathPrefix %q", req.ProxyPathPrefix)
		}
		tok := strings.TrimPrefix(mcp.Headers["Authorization"], "Bearer ")
		peerMu.Lock()
		peerEndpoints[req.TaskID] = mcp.URL
		peerTokens[req.TaskID] = tok
		peerMu.Unlock()

		// Synchronize so both polyglot turns truly overlap.
		if arrived.Add(1) == 2 {
			close(bothArrived)
		}
		select {
		case <-bothArrived:
		case <-ctx.Done():
			return OpenCodeMessageResponse{}, ctx.Err()
		}

		// Verify cross-sandbox bearer token isolation: Task A's token MUST fail (401) on Task B's sandbox.
		otherTask := "task-node"
		if req.TaskID == "task-node" {
			otherTask = "task-go"
		}
		peerMu.Lock()
		otherURL := peerEndpoints[otherTask]
		peerMu.Unlock()
		wrongCli, err := sandbox.NewClient(otherURL, tok, nil)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if _, err := wrongCli.ListTools(ctx); err == nil {
			return OpenCodeMessageResponse{}, fmt.Errorf("cross-sandbox token unexpectedly authorized on %s", otherTask)
		}

		// Call own real sandbox MCP server to write a language-specific file & verify with bash.
		ownCli, err := sandbox.NewClient(mcp.URL, tok, nil)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		fileName := "main.go"
		fileBody := "package main\nfunc main() {}\n"
		if req.TaskID == "task-node" {
			fileName = "index.js"
			fileBody = "console.log('hello node');\n"
		}
		wRes, err := ownCli.CallTool(ctx, "write_file", map[string]any{"path": fileName, "content": fileBody})
		if err != nil || wRes.IsError {
			return OpenCodeMessageResponse{}, fmt.Errorf("write_file failed: err=%v res=%+v", err, wRes)
		}
		rRes, err := ownCli.CallTool(ctx, "read_file", map[string]any{"path": fileName})
		if err != nil || rRes.IsError || !strings.Contains(rRes.FirstText(), strings.TrimSpace(fileBody)) {
			return OpenCodeMessageResponse{}, fmt.Errorf("read_file mismatch: err=%v res=%+v", err, rRes)
		}

		// Call per-turn attributed loopback LLM proxy.
		llmReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL+req.ProxyPathPrefix+"/llm/openai/v1/chat/completions", strings.NewReader(`{}`))
		llmReq.Header.Set("Authorization", "Bearer "+proxy.LocalAPIKey())
		llmResp, err := http.DefaultClient.Do(llmReq)
		if err != nil {
			return OpenCodeMessageResponse{}, fmt.Errorf("per-turn LLM proxy call: %w", err)
		}
		_ = llmResp.Body.Close()
		if llmResp.StatusCode != http.StatusOK {
			return OpenCodeMessageResponse{}, fmt.Errorf("per-turn LLM proxy status = %d", llmResp.StatusCode)
		}

		return OpenCodeMessageResponse{Text: "implemented " + fileName, MessageID: "msg-" + req.TaskID}, nil
	}

	store, err := NewFileOpenCodeStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileOpenCodeStateStore: %v", err)
	}

	adapter, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID:           "opencode-brain",
		TenantID:            "tenant-1",
		ProjectID:           "project-1",
		ExecutionMode:       OpenCodeExecutionModeBrain,
		Handler:             handler,
		OpenCode:            brainClient,
		Store:               store,
		Sandboxes:           sboxMgr,
		Proxy:               proxy,
		PerTurnProxyRouting: true,
		Clock:               clock,
		RescanInterval:      50 * time.Millisecond,
		MaxConcurrentTurns:  4,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- adapter.Run(runCtx)
	}()

	// Wait for BOTH concurrent polyglot tasks to resolve!
	for i := 0; i < 2; i++ {
		select {
		case <-handler.resolveCh:
		case <-time.After(15 * time.Second):
			cancelRun()
			t.Fatalf("timed out waiting for concurrent polyglot turn %d/2 to resolve", i+1)
		}
	}
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("adapter.Run: %v", err)
	}

	handler.mu.Lock()
	resGo := handler.resolveCalls["exec-go"]
	resNode := handler.resolveCalls["exec-node"]
	handler.mu.Unlock()

	if resGo.Decision != DecisionPass || resGo.Outputs[OpenCodeOutputBranch] != "refs/heads/work/task-go" || len(resGo.Outputs[OpenCodeOutputCandidate]) != 40 {
		t.Fatalf("Go task resolve unexpected: %+v", resGo)
	}
	if resNode.Decision != DecisionPass || resNode.Outputs[OpenCodeOutputBranch] != "refs/heads/work/task-node" || len(resNode.Outputs[OpenCodeOutputCandidate]) != 40 {
		t.Fatalf("Node task resolve unexpected: %+v", resNode)
	}

	// Confirm the pushed commits actually landed in the two separate bare Git repos!
	gotGoSHA := runGitCmd(t, goBare, "rev-parse", "refs/heads/work/task-go")
	if gotGoSHA != resGo.Outputs[OpenCodeOutputCandidate] {
		t.Fatalf("go bare repo ref SHA = %q; want %q", gotGoSHA, resGo.Outputs[OpenCodeOutputCandidate])
	}
	gotNodeSHA := runGitCmd(t, nodeBare, "rev-parse", "refs/heads/work/task-node")
	if gotNodeSHA != resNode.Outputs[OpenCodeOutputCandidate] {
		t.Fatalf("node bare repo ref SHA = %q; want %q", gotNodeSHA, resNode.Outputs[OpenCodeOutputCandidate])
	}

	// Confirm distinct images requested and both sandboxes released!
	sboxMgr.mu.Lock()
	gotImages := map[string]bool{}
	for _, r := range sboxMgr.acquireCalls {
		gotImages[r.Image] = true
	}
	gotReleases := len(sboxMgr.releaseCalls)
	sboxMgr.mu.Unlock()
	if !gotImages["golang:1.24"] || !gotImages["node:22"] {
		t.Fatalf("expected golang:1.24 and node:22 sandboxes, got %+v", gotImages)
	}
	if gotReleases != 2 {
		t.Fatalf("sandbox releases = %d; want 2", gotReleases)
	}

	// Confirm per-turn LLM proxy attribution had zero cross-task bleed.
	llmMu.Lock()
	if llmAttrs["exec-go"] != "act-go" || llmAttrs["exec-node"] != "act-node" {
		t.Fatalf("LLM proxy attribution = %+v; want exec-go->act-go and exec-node->act-node", llmAttrs)
	}
	llmMu.Unlock()
}

func TestOpenCodeBrainModeFaultContainmentSandboxOOMKill(t *testing.T) {
	nodeBare := initBareGitRepo(t)
	goBare := initBareGitRepo(t)

	stopUpstream := make(chan struct{})
	upstreamLLM := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Task A's slow LLM call should be cancelled as soon as Task A's sandbox is OOM-killed.
		if r.Header.Get("X-Kodema-Execution") == "exec-a" {
			select {
			case <-r.Context().Done():
			case <-stopUpstream:
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer func() {
		close(stopUpstream)
		upstreamLLM.CloseClientConnections()
		upstreamLLM.Close()
	}()

	clock := &fixedClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:   upstreamLLM.URL,
		Credentials: staticTestCred("kodsvc.brain.tok"),
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	proxyURL, err := proxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy.Start: %v", err)
	}
	defer func() { _ = proxy.Close(context.Background()) }()

	deadline := clock.Now().Add(5 * time.Minute)
	itemA, execA := makeBrainInboxFixture("exec-a", "act-a", "task-a", "implement", "svc/a", "golang:1.24", []string{OpenCodeOutputBranch, OpenCodeOutputCandidate}, deadline)
	itemB, execB := makeBrainInboxFixture("exec-b", "act-b", "task-b", "implement", "svc/b", "node:22", []string{OpenCodeOutputBranch, OpenCodeOutputCandidate}, deadline)

	handler := newBrainTestHandler()
	handler.inboxItems = []api.InboxItem{itemA, itemB}
	handler.executions["exec-a"] = execA
	handler.executions["exec-b"] = execB

	sboxMgr := newRealLocalSandboxManager(t, map[string]string{
		"svc/a": goBare,
		"svc/b": nodeBare,
	})

	taskAStarted := make(chan struct{})
	taskBStarted := make(chan struct{})
	releaseTaskB := make(chan struct{})
	var taskASessionID, taskBSessionID string
	var taskALLMCutOff atomic.Bool

	brainClient := newFakeBrainOpenCodeClient()
	brainClient.onSend = func(ctx context.Context, sessionID string, req OpenCodeMessageRequest, mcp OpenCodeMCPServerConfig) (OpenCodeMessageResponse, error) {
		if req.TaskID == "task-a" {
			taskASessionID = sessionID
			close(taskAStarted)
			// Launch an LLM proxy call bound to Task A's turn; when sandbox dies, proxy must cut it off!
			llmReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, proxyURL+req.ProxyPathPrefix+"/llm/openai/v1/chat/completions", strings.NewReader(`{}`))
			llmReq.Header.Set("Authorization", "Bearer "+proxy.LocalAPIKey())
			resp, err := http.DefaultClient.Do(llmReq)
			if err != nil || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusForbidden {
				taskALLMCutOff.Store(true)
			}
			if resp != nil {
				_ = resp.Body.Close()
			}
			<-ctx.Done()
			return OpenCodeMessageResponse{}, ctx.Err()
		}
		taskBSessionID = sessionID
		close(taskBStarted)
		<-releaseTaskB
		// Task B writes a file into its healthy sandbox and calls LLM proxy -> must succeed unaffected!
		tok := strings.TrimPrefix(mcp.Headers["Authorization"], "Bearer ")
		ownCli, err := sandbox.NewClient(mcp.URL, tok, nil)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if wRes, err := ownCli.CallTool(ctx, "write_file", map[string]any{"path": "ok.txt", "content": "alive\n"}); err != nil || wRes.IsError {
			return OpenCodeMessageResponse{}, fmt.Errorf("task B write_file: err=%v res=%+v", err, wRes)
		}
		llmReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL+req.ProxyPathPrefix+"/llm/openai/v1/chat/completions", strings.NewReader(`{}`))
		llmReq.Header.Set("Authorization", "Bearer "+proxy.LocalAPIKey())
		llmResp, err := http.DefaultClient.Do(llmReq)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		_ = llmResp.Body.Close()
		if llmResp.StatusCode != http.StatusOK {
			return OpenCodeMessageResponse{}, fmt.Errorf("task B LLM status = %d", llmResp.StatusCode)
		}
		return OpenCodeMessageResponse{Text: "task B completed cleanly"}, nil
	}

	store, _ := NewFileOpenCodeStateStore(t.TempDir())
	adapter, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID:           "opencode-brain",
		TenantID:            "tenant-1",
		ProjectID:           "project-1",
		ExecutionMode:       OpenCodeExecutionModeBrain,
		Handler:             handler,
		OpenCode:            brainClient,
		Store:               store,
		Sandboxes:           sboxMgr,
		Proxy:               proxy,
		PerTurnProxyRouting: true,
		Clock:               clock,
		RescanInterval:      50 * time.Millisecond,
		MaxConcurrentTurns:  4,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()

	// Wait until BOTH Task A and Task B are actively mid-turn.
	<-taskAStarted
	<-taskBStarted

	// OOM-kill Task A's sandbox mid-turn!
	sboxMgr.SimulateOOMKill("real-sbox-task-a-act-a", "OOMKilled: exceeded 1Gi memory limit")

	// Wait for Task A to fail cleanly FIRST while Task B is still alive!
	select {
	case resolvedA := <-handler.resolveCh:
		if resolvedA.ExecutionID != "exec-a" || resolvedA.Decision != "fail" {
			t.Fatalf("expected exec-a fail resolution, got %+v", resolvedA)
		}
		if !strings.Contains(resolvedA.Note, "sandbox_terminated") || !strings.Contains(resolvedA.Note, "OOMKilled") {
			t.Fatalf("expected sandbox OOMKilled diagnostic note, got %q", resolvedA.Note)
		}
	case <-time.After(5 * time.Second):
		cancelRun()
		t.Fatal("timed out waiting for Task A to fail after sandbox OOM kill")
	}

	// Now let Task B finish cleanly.
	close(releaseTaskB)
	select {
	case resolvedB := <-handler.resolveCh:
		if resolvedB.ExecutionID != "exec-b" || resolvedB.Decision != DecisionPass {
			t.Fatalf("expected exec-b pass resolution, got %+v", resolvedB)
		}
	case <-time.After(5 * time.Second):
		cancelRun()
		t.Fatal("timed out waiting for Task B to succeed")
	}

	// Verify ONLY Task A's session was aborted by the sandbox OOM kill (before adapter shutdown).
	brainClient.mu.Lock()
	aborts := append([]string(nil), brainClient.abortCalls...)
	brainClient.mu.Unlock()
	if len(aborts) != 1 || aborts[0] != taskASessionID {
		t.Fatalf("AbortSession calls = %v; want only Task A session %q (not Task B %q)", aborts, taskASessionID, taskBSessionID)
	}

	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("adapter.Run: %v", err)
	}

	if !taskALLMCutOff.Load() {
		t.Error("Task A in-flight LLM proxy call was not cut off when its sandbox died")
	}
}

func TestOpenCodeBrainModeFullCancellationAbortsSessionProxyAndSandbox(t *testing.T) {
	stopUpstream := make(chan struct{})
	upstreamLLM := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-stopUpstream:
		}
	}))
	defer func() {
		close(stopUpstream)
		upstreamLLM.CloseClientConnections()
		upstreamLLM.Close()
	}()

	clock := &fixedClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:   upstreamLLM.URL,
		Credentials: staticTestCred("kodsvc.brain.tok"),
		Clock:       clock,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	proxyURL, err := proxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy.Start: %v", err)
	}
	defer func() { _ = proxy.Close(context.Background()) }()

	deadline := clock.Now().Add(5 * time.Minute)
	item, ex := makeBrainInboxFixture("exec-cancel", "act-cancel", "task-cancel", "implement", "svc/app", "golang:1.24", []string{OpenCodeOutputBranch}, deadline)
	handler := newBrainTestHandler()
	handler.inboxItems = []api.InboxItem{item}
	handler.executions["exec-cancel"] = ex

	sboxMgr := newRealLocalSandboxManager(t, nil)
	streamer := &fakeEventStream{ch: make(chan api.StreamEvent, 4)}

	turnInFlight := make(chan struct{})
	llmCutOff := make(chan struct{})
	brainClient := newFakeBrainOpenCodeClient()
	brainClient.onSend = func(ctx context.Context, _ string, req OpenCodeMessageRequest, _ OpenCodeMCPServerConfig) (OpenCodeMessageResponse, error) {
		close(turnInFlight)
		go func() {
			llmReq, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, proxyURL+req.ProxyPathPrefix+"/llm/openai/v1/chat/completions", strings.NewReader(`{}`))
			llmReq.Header.Set("Authorization", "Bearer "+proxy.LocalAPIKey())
			resp, err := http.DefaultClient.Do(llmReq)
			if resp != nil {
				_ = resp.Body.Close()
			}
			if err != nil || resp.StatusCode != http.StatusOK {
				close(llmCutOff)
			}
		}()
		<-ctx.Done()
		return OpenCodeMessageResponse{}, ctx.Err()
	}

	store, _ := NewFileOpenCodeStateStore(t.TempDir())
	adapter, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID:           "opencode-brain",
		TenantID:            "tenant-1",
		ProjectID:           "project-1",
		ExecutionMode:       OpenCodeExecutionModeBrain,
		Handler:             handler,
		Stream:              streamer,
		OpenCode:            brainClient,
		Store:               store,
		Sandboxes:           sboxMgr,
		Proxy:               proxy,
		PerTurnProxyRouting: true,
		Clock:               clock,
		RescanInterval:      5 * time.Second,
		MaxConcurrentTurns:  2,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()

	<-turnInFlight
	// Give the goroutine a brief moment to enter the proxy request.
	time.Sleep(50 * time.Millisecond)

	// Emit execution.cancelled over SSE.
	streamer.ch <- api.StreamEvent{
		Type:      "execution.cancelled",
		SubjectID: "exec-cancel",
		Data:      json.RawMessage(`{"execution_id":"exec-cancel"}`),
	}

	// 1. In-flight LLM proxy call must be cut off.
	select {
	case <-llmCutOff:
	case <-time.After(3 * time.Second):
		cancelRun()
		t.Fatal("in-flight LLM proxy call was not cut off on execution.cancelled")
	}

	// Stop adapter cleanly so all abort/release goroutines complete.
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("adapter.Run: %v", err)
	}

	// 2. AbortSession must have been called.
	brainClient.mu.Lock()
	aborts := len(brainClient.abortCalls)
	brainClient.mu.Unlock()
	if aborts == 0 {
		t.Fatal("AbortSession was not called on execution.cancelled")
	}

	// 3. Sandbox must have been released (confirmed stop).
	sboxMgr.mu.Lock()
	releases := len(sboxMgr.releaseCalls)
	sboxMgr.mu.Unlock()
	if releases != 1 {
		t.Fatalf("sandbox releases = %d; want 1", releases)
	}
}

func TestOpenCodeBrainModeNonCodingPlanSkipsSandbox(t *testing.T) {
	clock := &fixedClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	deadline := clock.Now().Add(5 * time.Minute)
	item, ex := makeBrainInboxFixture("exec-plan", "act-plan", "task-plan", "plan", "", "", []string{OpenCodeOutputPlanKey, OpenCodeOutputSessionID}, deadline)

	handler := newBrainTestHandler()
	handler.inboxItems = []api.InboxItem{item}
	handler.executions["exec-plan"] = ex

	sboxMgr := newRealLocalSandboxManager(t, nil)
	brainClient := newFakeBrainOpenCodeClient()
	brainClient.onSend = func(_ context.Context, _ string, req OpenCodeMessageRequest, _ OpenCodeMCPServerConfig) (OpenCodeMessageResponse, error) {
		if len(req.MCPServers) != 0 {
			return OpenCodeMessageResponse{}, fmt.Errorf("plan turn without sandbox_image attached unexpected MCP servers: %+v", req.MCPServers)
		}
		if len(req.DisabledTools) < len(DefaultBrainDisabledLocalTools) {
			return OpenCodeMessageResponse{}, fmt.Errorf("plan turn did not disable local tools: %v", req.DisabledTools)
		}
		return OpenCodeMessageResponse{Text: "## Plan\n1. Design\n2. Build"}, nil
	}

	store, _ := NewFileOpenCodeStateStore(t.TempDir())
	adapter, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID:          "opencode-brain",
		TenantID:           "tenant-1",
		ProjectID:          "project-1",
		ExecutionMode:      OpenCodeExecutionModeBrain,
		Handler:            handler,
		OpenCode:           brainClient,
		Store:              store,
		Sandboxes:          sboxMgr,
		Clock:              clock,
		MaxConcurrentTurns: 2,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter: %v", err)
	}
	if err := adapter.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	sboxMgr.mu.Lock()
	acquired := len(sboxMgr.acquireCalls)
	sboxMgr.mu.Unlock()
	if acquired != 0 {
		t.Fatalf("plan mode without sandbox_image acquired %d sandbox(es); want 0", acquired)
	}

	handler.mu.Lock()
	resolved := handler.resolveCalls["exec-plan"]
	handler.mu.Unlock()
	if resolved.Decision != DecisionPass || resolved.Outputs[OpenCodeOutputPlanKey] == "" {
		t.Fatalf("plan resolution unexpected: %+v", resolved)
	}
}
