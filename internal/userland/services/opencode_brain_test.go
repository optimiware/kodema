package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	// Allowing bash in permission map must also be rejected.
	badPerm := strings.Replace(string(raw), `"bash": "deny"`, `"bash": "allow"`, 1)
	if err := ValidateBrainOpenCodeConfig([]byte(badPerm)); err == nil {
		t.Fatal("ValidateBrainOpenCodeConfig accepted config with bash permission=allow")
	}
	// Omitting permission map entirely must be rejected.
	if err := ValidateBrainOpenCodeConfig([]byte(`{"tools":{"bash":false,"write":false,"edit":false,"read":false,"glob":false,"grep":false}}`)); err == nil {
		t.Fatal("ValidateBrainOpenCodeConfig accepted config missing permission map")
	}
	if err := ValidateBrainOpenCodeConfig([]byte(`{"tools":{}}`)); err == nil {
		t.Fatal("ValidateBrainOpenCodeConfig accepted empty tools map")
	}
}

func TestHTTPOpenCodeClientBrainMCPAttachDetachAndSendMessage(t *testing.T) {
	var mu sync.Mutex
	var attachedPayload map[string]any
	var patchPayload map[string]any
	var promptPayload map[string]any
	var proxyTurnHeader string
	var detachedPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/experimental/mcp/sandbox":
			_ = json.Unmarshal(body, &attachedPayload)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPatch && r.URL.Path == "/api/session/sess-1":
			_ = json.Unmarshal(body, &patchPayload)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/sess-1/prompt":
			_ = json.Unmarshal(body, &promptPayload)
			proxyTurnHeader = r.Header.Get("X-Kodema-Proxy-Turn-Path")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"id":"u-1"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/experimental/session/sess-1/wait":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/session/sess-1/message":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":[{"id":"i-1","type":"idle","outcome":"succeeded"},{"id":"m-1","type":"assistant","content":[{"type":"text","text":"done"}]},{"id":"u-1","type":"user"}],"cursor":{}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/api/experimental/mcp/sandbox":
			detachedPath = r.URL.Path
			w.WriteHeader(http.StatusNoContent)
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
	attachedCfg, _ := attachedPayload["config"].(map[string]any)
	if attachedCfg["url"] != "http://10.0.0.5:8090/mcp" || attachedCfg["disabled"] != false || attachedCfg["oauth"] != false {
		t.Fatalf("attached MCP payload = %+v", attachedPayload)
	}
	rawPerms, _ := json.Marshal(patchPayload["permissions"])
	var gotPerms []openCodePermissionRule
	_ = json.Unmarshal(rawPerms, &gotPerms)
	denied := make(map[string]bool, len(gotPerms))
	for _, rule := range gotPerms {
		if rule.Effect == "deny" && rule.Resource == "*" {
			denied[rule.Action] = true
		}
	}
	for _, wantAction := range []string{"shell", "edit", "read", "glob", "grep"} {
		if !denied[wantAction] {
			t.Errorf("PATCH permissions missing deny rule for %q: %+v", wantAction, gotPerms)
		}
	}
	if len(promptPayload) != 1 || promptPayload["text"] != "implement feature" {
		t.Fatalf("promptPayload = %+v; want only {text: implement feature}", promptPayload)
	}
	if proxyTurnHeader != "/turn/exec-1/act-1" {
		t.Fatalf("X-Kodema-Proxy-Turn-Path = %q; want /turn/exec-1/act-1", proxyTurnHeader)
	}
	if detachedPath != "/api/experimental/mcp/sandbox" {
		t.Fatalf("detachedPath = %q; want /api/experimental/mcp/sandbox", detachedPath)
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

	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", baseURL, err)
	}
	boundPort, err := strconv.Atoi(parsedBase.Port())
	if err != nil {
		t.Fatalf("Atoi port %q: %v", parsedBase.Port(), err)
	}

	// Fake Kodema public API handler for secret.write, secret.delete,
	// pipeline.start, execution.inspect, execution.logs.read, and execution.cancel.
	var mu sync.Mutex
	var startedYAML string
	execState := "running"
	execVersion := uint64(1)
	var execFailure *api.ExecutionFailure
	cancelCalls := 0
	inspectAfterCancel := 0
	writtenSecrets := map[string]string{}
	deletedSecrets := map[string]int{}
	logPagesServed := 0

	const realSandboxActionID = "act-sandbox-1"

	handler := testAPIHandlerFunc(func(_ context.Context, inv api.Invocation) (api.Result, error) {
		codec := publicCodecs[inv.Operation]
		mu.Lock()
		defer mu.Unlock()
		switch inv.Operation {
		case OperationSecretWrite:
			var req api.SecretWriteRequest
			if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
				return api.Result{}, err
			}
			writtenSecrets[req.SecretID] = req.Value
			body, _ := codec.EncodeResult(api.SecretWriteResult{
				SecretID: "sec-" + req.SecretID,
				Name:     req.SecretID,
				Version:  1,
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationSecretDelete:
			var req api.SecretDeleteRequest
			if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
				return api.Result{}, err
			}
			deletedSecrets[req.SecretID]++
			body, _ := codec.EncodeResult(api.SecretDeleteResult{Version: 2})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationPipelineStart:
			var req api.PipelineStartRequest
			_ = json.Unmarshal(inv.Request.Input, &req)
			startedYAML = string(req.YAML)
			body, _ := codec.EncodeResult(api.PipelineStartResult{ExecutionID: "exec-sbox-1", Version: 1})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationExecutionLogsRead:
			var req api.ExecutionLogsReadRequest
			if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
				return api.Result{}, err
			}
			// Strictly reject hardcoded action name "sandbox"; require server ActionID!
			if req.ActionID != realSandboxActionID {
				return api.Result{}, &api.Error{Code: api.CodeNotFound, Message: fmt.Sprintf("unknown action_id %q", req.ActionID)}
			}
			logPagesServed++
			if req.Page == nil || req.Page.Cursor == "" {
				// Page 1: verbose git clone output with NextCursor pointing to page 2.
				body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
					Chunks: []api.LogChunk{{
						ActionID: realSandboxActionID,
						Stream:   "stdout",
						Data:     []byte("Cloning into '/workspace'...\nReceiving objects: 100%\n"),
					}},
					NextCursor: "cursor-page-2",
				})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			}
			if req.Page.Cursor == "cursor-page-2" {
				// Page 2: announces KODEMA_SANDBOX_ENDPOINT.
				body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
					Chunks: []api.LogChunk{{
						ActionID: realSandboxActionID,
						Stream:   "stdout",
						Data:     []byte("booting...\n" + sandboxReadyLinePrefix + baseURL + "\n"),
					}},
				})
				return api.Result{Operation: inv.Operation, Body: body}, nil
			}
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{})
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
				Actions: []api.ExecutionAction{{
					ActionID: realSandboxActionID,
					Index:    0,
					Name:     "sandbox",
					Kind:     "container",
					State:    "running",
					Version:  1,
				}},
				Failure: execFailure,
				Cleanup: api.ExecutionCleanup{State: "none"},
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
		TenantID:               id.TenantID("tenant-1"),
		ProjectID:              id.ProjectID("project-1"),
		Handler:                handler,
		AllowLoopbackEndpoints: true,
		ReadyTimeout:           5 * time.Second,
		HealthInterval:         30 * time.Millisecond,
		StopTimeout:            5 * time.Second,
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
		Port:        boundPort,
	})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	mu.Lock()
	yamlCopy := startedYAML
	pages := logPagesServed
	secCount := len(writtenSecrets)
	var firstSecretName string
	for k := range writtenSecrets {
		firstSecretName = k
	}
	mu.Unlock()

	if pages < 2 {
		t.Fatalf("expected multi-page log pagination (>=2 reads), got %d", pages)
	}
	if secCount != 1 {
		t.Fatalf("expected 1 ephemeral project secret written, got %d", secCount)
	}
	if strings.Contains(yamlCopy, lease.BearerToken) {
		t.Fatalf("generated sandbox pipeline YAML leaked raw bearer token!\n%s", yamlCopy)
	}
	for _, want := range []string{
		`image: "golang:1.24"`,
		`secrets:`,
		`- "` + firstSecretName + `"`,
		`- "go-mod"`,
		`- "go-build"`,
		`memory: "2Gi"`,
		`cpu: "2"`,
		`/kodema/bin/kodema sandbox serve`,
		`--token-file /kodema/secrets/` + firstSecretName,
		`--token-env ` + firstSecretName,
	} {
		if !strings.Contains(yamlCopy, want) {
			t.Errorf("generated sandbox pipeline YAML missing %q:\n%s", want, yamlCopy)
		}
	}
	if lease.ExecutionID != "exec-sbox-1" || lease.EndpointURL != baseURL+"/mcp" {
		t.Fatalf("unexpected lease: %+v", lease)
	}

	// Verify Release invokes execution.cancel, waits for confirmed stop, and deletes the ephemeral secret.
	if err := mgr.Release(context.Background(), lease); err != nil {
		t.Fatalf("Release: %v", err)
	}
	mu.Lock()
	gotCancels := cancelCalls
	gotState := execState
	gotSecretDel := deletedSecrets[firstSecretName]
	mu.Unlock()
	if gotCancels != 1 || gotState != "cancelled" {
		t.Fatalf("Release did not confirm stop: cancelCalls=%d state=%q", gotCancels, gotState)
	}
	if gotSecretDel != 1 {
		t.Fatalf("Release did not delete ephemeral secret %q: delete calls=%d", firstSecretName, gotSecretDel)
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
		Port:        boundPort,
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

func TestSandboxEndpointSSRFValidationAndRedirectRejection(t *testing.T) {
	badCases := []struct {
		name          string
		raw           string
		port          int
		allowLoopback bool
	}{
		{"dns hostname", "http://evil.example.com:8090", 8090, false},
		{"localhost hostname", "http://localhost:8090", 8090, true},
		{"loopback v4 denied by default", "http://127.0.0.1:8090", 8090, false},
		{"loopback v6 denied by default", "http://[::1]:8090", 8090, false},
		{"ipv4-mapped loopback denied", "http://[::ffff:127.0.0.1]:8090", 8090, false},
		{"cloud metadata 169.254.169.254", "http://169.254.169.254:8090", 8090, true},
		{"ipv4-mapped cloud metadata", "http://[::ffff:169.254.169.254]:8090", 8090, true},
		{"ipv6 link-local", "http://[fe80::1]:8090", 8090, true},
		{"unspecified v4", "http://0.0.0.0:8090", 8090, true},
		{"unspecified v6", "http://[::]:8090", 8090, true},
		{"multicast", "http://224.0.0.1:8090", 8090, true},
		{"wrong port", "http://10.0.0.5:8080", 8090, false},
		{"missing port", "http://10.0.0.5", 8090, false},
		{"userinfo present", "http://user:pass@10.0.0.5:8090", 8090, false},
		{"non-empty path", "http://10.0.0.5:8090/mcp", 8090, false},
		{"root slash path", "http://10.0.0.5:8090/", 8090, false},
		{"query present", "http://10.0.0.5:8090?foo=bar", 8090, false},
		{"fragment present", "http://10.0.0.5:8090#frag", 8090, false},
		{"https scheme rejected", "https://10.0.0.5:8090", 8090, false},
		{"control char", "http://10.0.0.5:8090\r\n", 8090, false},
	}
	for _, tc := range badCases {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := validateSandboxEndpointURL(tc.raw, tc.port, tc.allowLoopback); err == nil {
				t.Fatalf("validateSandboxEndpointURL(%q) unexpectedly accepted %q", tc.raw, got)
			}
		})
	}

	// Valid pod IPv4, pod IPv6, and explicit allowLoopback must pass.
	if got, err := validateSandboxEndpointURL("http://10.244.1.12:8090", 8090, false); err != nil || got != "http://10.244.1.12:8090" {
		t.Fatalf("valid pod IPv4 = (%q, %v)", got, err)
	}
	if got, err := validateSandboxEndpointURL("http://[fd00::12]:8090", 8090, false); err != nil || got != "http://[fd00::12]:8090" {
		t.Fatalf("valid pod IPv6 = (%q, %v)", got, err)
	}
	if got, err := validateSandboxEndpointURL("http://127.0.0.1:8090", 8090, true); err != nil || got != "http://127.0.0.1:8090" {
		t.Fatalf("explicit loopback = (%q, %v)", got, err)
	}

	// Verify HTTP 302 redirect on /healthz is refused (never followed).
	var targetHits atomic.Int32
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer targetSrv.Close()

	redirectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetSrv.URL+"/healthz", http.StatusFound)
	}))
	defer redirectSrv.Close()

	mgr, err := NewKodemaExecutionSandboxManager(KodemaExecutionSandboxConfig{
		TenantID:               "tenant-1",
		ProjectID:              "project-1",
		Handler:                testAPIHandlerFunc(func(context.Context, api.Invocation) (api.Result, error) { return api.Result{}, nil }),
		AllowLoopbackEndpoints: true,
	})
	if err != nil {
		t.Fatalf("NewKodemaExecutionSandboxManager: %v", err)
	}
	if err := mgr.probeHealthz(context.Background(), redirectSrv.URL+"/healthz"); err == nil {
		t.Fatal("probeHealthz followed HTTP 302 redirect and succeeded")
	}
	if targetHits.Load() != 0 {
		t.Fatalf("redirect target was hit %d times; want 0", targetHits.Load())
	}
}

func TestBuildSandboxPipelineYAMLSecurityAndTimeoutClamping(t *testing.T) {
	baseReq := OpenCodeSandboxRequest{
		TenantID:    "tenant-1",
		ProjectID:   "project-1",
		TaskID:      "task-1",
		ExecutionID: "exec-1",
		ActionID:    "act-1",
		Image:       "golang:1.24",
		Repository:  "app/backend",
		BaseRef:     "refs/heads/main",
		WorkBranch:  "refs/heads/work/task-1",
		Port:        8090,
	}

	// Valid YAML uses Kodema secret mount, never embeds raw token, never falls back to 127.0.0.1,
	// fails fast on missing KODEMA_GIT_URL/git, and passes `--` with full refspecs to git.
	yamlBytes, err := BuildSandboxPipelineYAML(baseReq, "KODEMA_SBOX_TOK_0123456789ABCDEF")
	if err != nil {
		t.Fatalf("BuildSandboxPipelineYAML: %v", err)
	}
	y := string(yamlBytes)
	if strings.Contains(y, "127.0.0.1") {
		t.Fatalf("default BuildSandboxPipelineYAML must not fall back to 127.0.0.1:\n%s", y)
	}
	if strings.Contains(y, "kodsbox.") {
		t.Fatalf("BuildSandboxPipelineYAML must never contain a raw bearer token:\n%s", y)
	}
	for _, want := range []string{
		`secrets:`,
		`- "KODEMA_SBOX_TOK_0123456789ABCDEF"`,
		`--token-file /kodema/secrets/KODEMA_SBOX_TOK_0123456789ABCDEF`,
		`--token-env KODEMA_SBOX_TOK_0123456789ABCDEF`,
		`git ls-remote --exit-code --heads -- origin 'refs/heads/work/task-1'`,
		`git fetch --no-tags -- origin 'refs/heads/main'`,
		`KODEMA_GIT_URL is not set`,
		`git binary is not available`,
		`failed to determine non-loopback pod IP`,
	} {
		if !strings.Contains(y, want) {
			t.Errorf("YAML missing %q:\n%s", want, y)
		}
	}

	// Timeout clamping: < 1m clamps to 1m0s; > 30d clamps to 720h0m0s.
	shortReq := baseReq
	shortReq.Timeout = 15 * time.Second
	shortYAML, err := BuildSandboxPipelineYAML(shortReq, "KODEMA_SBOX_TOK_ABCDEF0123456789")
	if err != nil {
		t.Fatalf("BuildSandboxPipelineYAML short timeout: %v", err)
	}
	if !strings.Contains(string(shortYAML), "timeout: 1m0s") {
		t.Fatalf("short timeout not clamped to 1m0s:\n%s", string(shortYAML))
	}

	longReq := baseReq
	longReq.Timeout = 45 * 24 * time.Hour
	longYAML, err := BuildSandboxPipelineYAML(longReq, "KODEMA_SBOX_TOK_ABCDEF0123456789")
	if err != nil {
		t.Fatalf("BuildSandboxPipelineYAML long timeout: %v", err)
	}
	if !strings.Contains(string(longYAML), "timeout: 720h0m0s") {
		t.Fatalf("long timeout not clamped to 720h0m0s:\n%s", string(longYAML))
	}

	// Git flag injection via refs/heads/--upload-pack=sh and malicious TaskID must be refused.
	badReqs := []struct {
		name   string
		mutate func(*OpenCodeSandboxRequest)
		secret string
	}{
		{"base_ref flag injection", func(r *OpenCodeSandboxRequest) { r.BaseRef = "refs/heads/--upload-pack=sh" }, "KODEMA_SBOX_TOK_1"},
		{"work_branch flag injection", func(r *OpenCodeSandboxRequest) { r.WorkBranch = "refs/heads/--upload-pack=sh" }, "KODEMA_SBOX_TOK_1"},
		{"base_ref equals sign", func(r *OpenCodeSandboxRequest) { r.BaseRef = "refs/heads/a=b" }, "KODEMA_SBOX_TOK_1"},
		{"malicious task_id flag", func(r *OpenCodeSandboxRequest) { r.TaskID = "--upload-pack=sh"; r.WorkBranch = "" }, "KODEMA_SBOX_TOK_1"},
		{"malicious task_id traversal", func(r *OpenCodeSandboxRequest) { r.TaskID = "../escape"; r.WorkBranch = "" }, "KODEMA_SBOX_TOK_1"},
		{"control char in image", func(r *OpenCodeSandboxRequest) { r.Image = "golang:1.24\nevil" }, "KODEMA_SBOX_TOK_1"},
		{"control char in repo", func(r *OpenCodeSandboxRequest) { r.Repository = "app/repo\n" }, "KODEMA_SBOX_TOK_1"},
		{"raw bearer token passed as secret name", func(*OpenCodeSandboxRequest) {}, "kodsbox.0123456789abcdef"},
	}
	for _, tc := range badReqs {
		t.Run(tc.name, func(t *testing.T) {
			req := baseReq
			tc.mutate(&req)
			if _, err := BuildSandboxPipelineYAML(req, tc.secret); err == nil {
				t.Fatalf("BuildSandboxPipelineYAML unexpectedly accepted %s", tc.name)
			}
		})
	}
}

func TestKodemaExecutionSandboxManagerRetryGetsDistinctIdempotencyKey(t *testing.T) {
	// Start a real sandbox server for the second attempt.
	ws := t.TempDir()
	realSrv, err := sandbox.NewServer(sandbox.Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: ws,
		Token:        "kodsbox.retrytesttoken0123456789abcdef01",
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
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(baseURL, "http://"))
	boundPort, _ := strconv.Atoi(portStr)

	var mu sync.Mutex
	seenStartKeys := map[string]bool{}
	startAttempts := 0
	deletedSecrets := map[string]int{}
	writtenSecrets := []string{}

	handler := testAPIHandlerFunc(func(_ context.Context, inv api.Invocation) (api.Result, error) {
		codec := publicCodecs[inv.Operation]
		mu.Lock()
		defer mu.Unlock()
		switch inv.Operation {
		case OperationSecretWrite:
			var req api.SecretWriteRequest
			_ = json.Unmarshal(inv.Request.Input, &req)
			writtenSecrets = append(writtenSecrets, req.SecretID)
			body, _ := codec.EncodeResult(api.SecretWriteResult{SecretID: "sec-" + req.SecretID, Name: req.SecretID, Version: 1})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationSecretDelete:
			var req api.SecretDeleteRequest
			_ = json.Unmarshal(inv.Request.Input, &req)
			deletedSecrets[req.SecretID]++
			body, _ := codec.EncodeResult(api.SecretDeleteResult{Version: 2})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationPipelineStart:
			if seenStartKeys[inv.Request.IdempotencyKey] {
				return api.Result{}, &api.Error{Code: api.CodeChangedIdempotencyContent, Message: "reused idempotency key with changed content"}
			}
			seenStartKeys[inv.Request.IdempotencyKey] = true
			startAttempts++
			if startAttempts == 1 {
				return api.Result{}, errors.New("transient control-plane failure")
			}
			body, _ := codec.EncodeResult(api.PipelineStartResult{ExecutionID: "exec-retry-2", Version: 1})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationInspect:
			body, _ := codec.EncodeResult(api.ExecutionInspectResult{
				ExecutionID:   "exec-retry-2",
				State:         "running",
				Version:       1,
				CurrentAction: 0,
				Actions: []api.ExecutionAction{{
					ActionID: "act-retry-sbox",
					Index:    0,
					Name:     "sandbox",
					Kind:     "container",
					State:    "running",
					Version:  1,
				}},
				Cleanup: api.ExecutionCleanup{State: "none"},
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationExecutionLogsRead:
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
				Chunks: []api.LogChunk{{
					ActionID: "act-retry-sbox",
					Stream:   "stdout",
					Data:     []byte(sandboxReadyLinePrefix + baseURL + "\n"),
				}},
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case OperationExecutionCancel:
			body, _ := codec.EncodeResult(api.ExecutionCancelResult{ExecutionVersion: 2})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		default:
			return api.Result{}, fmt.Errorf("unexpected op %s", inv.Operation)
		}
	})

	mgr, err := NewKodemaExecutionSandboxManager(KodemaExecutionSandboxConfig{
		TenantID:               "tenant-1",
		ProjectID:              "project-1",
		Handler:                handler,
		AllowLoopbackEndpoints: true,
		ReadyTimeout:           5 * time.Second,
		HealthInterval:         30 * time.Millisecond,
		StopTimeout:            2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewKodemaExecutionSandboxManager: %v", err)
	}

	req := OpenCodeSandboxRequest{
		TenantID:    "tenant-1",
		ProjectID:   "project-1",
		TaskID:      "task-retry",
		ExecutionID: "exec-parent-retry",
		ActionID:    "act-parent-retry",
		Image:       "golang:1.24",
		Port:        boundPort,
	}

	// Attempt 1 fails transiently; its ephemeral secret must be cleaned up.
	if _, err := mgr.Acquire(context.Background(), req); err == nil {
		t.Fatal("first Acquire unexpectedly succeeded")
	}
	mu.Lock()
	firstSecret := writtenSecrets[0]
	firstDeleted := deletedSecrets[firstSecret]
	mu.Unlock()
	if firstDeleted != 1 {
		t.Fatalf("failed Acquire did not clean up ephemeral secret %q (deletes=%d)", firstSecret, firstDeleted)
	}

	// Attempt 2 with the exact same (ExecutionID, ActionID, TaskID) gets a fresh idempotency key and succeeds.
	lease, err := mgr.Acquire(context.Background(), req)
	if err != nil {
		t.Fatalf("retry Acquire failed: %v", err)
	}
	if lease.ExecutionID != "exec-retry-2" {
		t.Fatalf("retry lease ExecutionID = %q; want exec-retry-2", lease.ExecutionID)
	}
	if len(seenStartKeys) != 2 {
		t.Fatalf("expected 2 distinct pipeline.start idempotency keys, got %d", len(seenStartKeys))
	}
}

func TestCommitAndPushViaSandboxMCPPreservesPushStderrAndValidatesSHA(t *testing.T) {
	// 1. Failing git push must preserve stderr diagnostics in the returned error.
	ws := t.TempDir()
	runGitCmd(t, ws, "init")
	if err := os.WriteFile(filepath.Join(ws, "a.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	runGitCmd(t, ws, "add", "a.txt")
	runGitCmd(t, ws, "-c", "user.name=T", "-c", "user.email=t@t", "commit", "-m", "init")
	// Point origin at a nonexistent path so `git push` fails with diagnostic stderr.
	runGitCmd(t, ws, "remote", "add", "origin", filepath.Join(t.TempDir(), "does-not-exist.git"))

	tok, err := sandbox.GenerateBearerToken()
	if err != nil {
		t.Fatalf("GenerateBearerToken: %v", err)
	}
	srv, err := sandbox.NewServer(sandbox.Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: ws,
		Token:        tok,
	})
	if err != nil {
		t.Fatalf("sandbox.NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	baseURL, err := srv.Start(ctx)
	if err != nil {
		t.Fatalf("srv.Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	_, err = CommitAndPushViaSandboxMCP(
		context.Background(),
		OpenCodeSandboxLease{EndpointURL: baseURL + "/mcp", BearerToken: tok},
		"app/backend",
		"task-1",
		"test commit",
		nil,
	)
	if err == nil {
		t.Fatal("CommitAndPushViaSandboxMCP unexpectedly succeeded against missing remote")
	}
	if !strings.Contains(err.Error(), "does-not-exist.git") && !strings.Contains(err.Error(), "fatal:") {
		t.Fatalf("expected git push stderr diagnostics in error, got: %v", err)
	}

	// 2. Non-40-char-hex SHA must be rejected by isValidCommitSHA.
	for _, bad := range []string{"", "abc123", strings.Repeat("g", 40), strings.Repeat("A", 40), strings.Repeat("a", 39), strings.Repeat("a", 41)} {
		if isValidCommitSHA(bad) {
			t.Errorf("isValidCommitSHA(%q) = true; want false", bad)
		}
	}
	if !isValidCommitSHA("0123456789abcdef0123456789abcdef01234567") {
		t.Error("isValidCommitSHA rejected valid 40-char lowercase hex SHA")
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

func TestResolveEndpointSplitLineAcrossPollsWithoutCursorAdvance(t *testing.T) {
	// Regression test: when Poll 1 reaches the end of live logs mid-line with
	// NextCursor == "", resolveEndpoint must NOT commit lineCarry so that Poll 2
	// re-reading Page 1 does not duplicate the prefix and corrupt or skip the
	// KODEMA_SANDBOX_ENDPOINT= line when Page 1 later gets a NextCursor.
	var mu sync.Mutex
	poll := 0
	const wantEndpoint = "http://10.42.0.15:8090"

	handler := testAPIHandlerFunc(func(_ context.Context, inv api.Invocation) (api.Result, error) {
		if inv.Operation != OperationExecutionLogsRead {
			return api.Result{}, fmt.Errorf("unexpected op %s", inv.Operation)
		}
		var req api.ExecutionLogsReadRequest
		if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
			return api.Result{}, err
		}
		codec := publicCodecs[inv.Operation]
		mu.Lock()
		poll++
		curPoll := poll
		mu.Unlock()

		switch {
		case curPoll == 1 && (req.Page == nil || req.Page.Cursor == ""):
			// Poll 1: page 1 ends mid-line without newline and NextCursor == "".
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
				Chunks: []api.LogChunk{{
					ActionID: "act-sbox-1",
					Stream:   "stdout",
					Data:     []byte("booting...\nKODEMA_SANDBOX_END"),
				}},
				NextCursor: "",
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case req.Page == nil || req.Page.Cursor == "":
			// Poll 2: page 1 now has full chunk plus NextCursor pointing to page 2
			// where the rest of the split line resides.
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
				Chunks: []api.LogChunk{{
					ActionID: "act-sbox-1",
					Stream:   "stdout",
					Data:     []byte("booting...\nKODEMA_SANDBOX_END"),
				}},
				NextCursor: "cursor-page-2",
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		case req.Page.Cursor == "cursor-page-2":
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{
				Chunks: []api.LogChunk{{
					ActionID: "act-sbox-1",
					Stream:   "stdout",
					Data:     []byte("POINT=" + wantEndpoint + "\n"),
				}},
				NextCursor: "",
			})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		default:
			body, _ := codec.EncodeResult(api.ExecutionLogsReadResult{})
			return api.Result{Operation: inv.Operation, Body: body}, nil
		}
	})

	mgr, err := NewKodemaExecutionSandboxManager(KodemaExecutionSandboxConfig{
		TenantID:  id.TenantID("tenant-1"),
		ProjectID: id.ProjectID("project-1"),
		Handler:   handler,
	})
	if err != nil {
		t.Fatalf("NewKodemaExecutionSandboxManager: %v", err)
	}

	var cursor, lineCarry string
	if _, err := mgr.resolveEndpoint(context.Background(), "exec-sbox-1", "act-sbox-1", 8090, &cursor, &lineCarry); err == nil {
		t.Fatal("expected Poll 1 to report endpoint not announced yet")
	}
	if cursor != "" || lineCarry != "" {
		t.Fatalf("after Poll 1 with empty NextCursor: cursor=%q lineCarry=%q; want both empty", cursor, lineCarry)
	}

	got, err := mgr.resolveEndpoint(context.Background(), "exec-sbox-1", "act-sbox-1", 8090, &cursor, &lineCarry)
	if err != nil {
		t.Fatalf("Poll 2 resolveEndpoint: %v", err)
	}
	if got != wantEndpoint {
		t.Fatalf("resolved endpoint = %q; want %q", got, wantEndpoint)
	}
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
