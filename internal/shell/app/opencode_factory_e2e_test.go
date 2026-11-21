package app_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/installation"
	"github.com/optimiware/kodema/internal/kernel"
	kernelengine "github.com/optimiware/kodema/internal/kernel/engine"
	shellapp "github.com/optimiware/kodema/internal/shell/app"
	"github.com/optimiware/kodema/internal/shell/identity"
	"github.com/optimiware/kodema/internal/shell/secret"
	"github.com/optimiware/kodema/internal/shell/tenancy"
	"github.com/optimiware/kodema/internal/userland/services"
	servicesapp "github.com/optimiware/kodema/internal/userland/services/app"
)

// factoryRecordingRuntime is an in-process Kernel Runtime/Workspace that
// records every prepared container workload (including its resolved secret
// values) and runs an optional hook on Start before reporting clean exit 0.
type factoryRecordingRuntime struct {
	mu          sync.Mutex
	prepared    []kernelengine.PrepareRequest
	started     map[id.WorkloadID]bool
	onStart     func(req kernelengine.StartRequest, prep kernelengine.PrepareRequest) error
	lastHookErr error
	byWorkID    map[id.WorkloadID]kernelengine.PrepareRequest
}

func newFactoryRecordingRuntime() *factoryRecordingRuntime {
	return &factoryRecordingRuntime{
		started:  make(map[id.WorkloadID]bool),
		byWorkID: make(map[id.WorkloadID]kernelengine.PrepareRequest),
	}
}

var (
	_ kernelengine.Runtime   = (*factoryRecordingRuntime)(nil)
	_ kernelengine.Workspace = (*factoryRecordingRuntime)(nil)
)

func (r *factoryRecordingRuntime) Capabilities(context.Context) (kernelengine.Capabilities, error) {
	return kernelengine.Capabilities{Names: []string{"process"}}, nil
}

func (r *factoryRecordingRuntime) Prepare(_ context.Context, req kernelengine.PrepareRequest) (kernelengine.Preparation, error) {
	r.mu.Lock()
	r.prepared = append(r.prepared, req)
	r.byWorkID[req.Identity.WorkloadID] = req
	r.mu.Unlock()
	return kernelengine.Preparation{WorkspaceID: req.WorkspaceID, Detail: "prepared"}, nil
}

func (r *factoryRecordingRuntime) Start(_ context.Context, req kernelengine.StartRequest) (kernelengine.StartResult, error) {
	r.mu.Lock()
	prep := r.byWorkID[req.Identity.WorkloadID]
	hook := r.onStart
	r.mu.Unlock()
	if hook != nil {
		if err := hook(req, prep); err != nil {
			r.mu.Lock()
			r.lastHookErr = err
			r.mu.Unlock()
			return kernelengine.StartResult{}, err
		}
	}
	r.mu.Lock()
	r.started[req.Identity.WorkloadID] = true
	r.mu.Unlock()
	return kernelengine.StartResult{Outcome: kernelengine.StartAccepted, WorkloadID: req.Identity.WorkloadID, Detail: "started"}, nil
}

func (r *factoryRecordingRuntime) Observe(_ context.Context, req kernelengine.ObserveRequest) (kernel.Observation, error) {
	r.mu.Lock()
	wasStarted := r.started[req.Identity.WorkloadID]
	r.mu.Unlock()
	if !wasStarted {
		return kernel.AbsentObservation(), nil
	}
	return kernel.ExitedObservation(0, kernel.ConfirmedStop("exited 0")), nil
}

func (r *factoryRecordingRuntime) Logs(context.Context, kernelengine.LogsRequest) (kernelengine.LogBatch, error) {
	return kernelengine.LogBatch{}, nil
}

func (r *factoryRecordingRuntime) Stop(context.Context, kernelengine.StopRequest) (kernel.StopEvidence, error) {
	return kernel.ConfirmedStop("stopped"), nil
}

func (r *factoryRecordingRuntime) Remove(context.Context, kernelengine.RemoveRequest) (kernelengine.Removal, error) {
	return kernelengine.Removal{Removed: true}, nil
}

func (r *factoryRecordingRuntime) RefreshCredential(context.Context, kernelengine.CredentialRequest) (kernelengine.CredentialRefresh, error) {
	return kernelengine.CredentialRefresh{Delivered: true}, nil
}

func (r *factoryRecordingRuntime) Status(context.Context, id.WorkspaceID) (kernelengine.WorkspaceObservation, error) {
	return kernelengine.WorkspaceObservation{Available: true}, nil
}

func (r *factoryRecordingRuntime) Retain(context.Context, id.WorkspaceID) error  { return nil }
func (r *factoryRecordingRuntime) Release(context.Context, id.WorkspaceID) error { return nil }

func (r *factoryRecordingRuntime) snapshotPrepared() []kernelengine.PrepareRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]kernelengine.PrepareRequest(nil), r.prepared...)
}

type staticBearerCred string

func (s staticBearerCred) Credential(context.Context) (client.Credential, error) {
	return client.Credential{
		Kind:      client.KindSession,
		Token:     string(s),
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

func mustInvokeOp[Req any, Res any](
	t *testing.T,
	ctx context.Context,
	tr client.Transport,
	operation string,
	req Req,
	expectedVersion *uint64,
	idemKey string,
) Res {
	t.Helper()
	input, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal %s: %v", operation, err)
	}
	raw, err := tr.Invoke(ctx, api.Invocation{
		Operation: operation,
		Request: api.Request{
			Input:           input,
			ExpectedVersion: expectedVersion,
			IdempotencyKey:  idemKey,
		},
	})
	if err != nil {
		t.Fatalf("invoke %s: %v", operation, err)
	}
	var out Res
	if err := json.Unmarshal(raw.Body, &out); err != nil {
		t.Fatalf("unmarshal %s result: %v (body=%s)", operation, err, string(raw.Body))
	}
	return out
}

func runFactoryGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = []string{
		"LC_ALL=C",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=Factory Test",
		"GIT_AUTHOR_EMAIL=factory@kodema.invalid",
		"GIT_COMMITTER_NAME=Factory Test",
		"GIT_COMMITTER_EMAIL=factory@kodema.invalid",
		"PATH=" + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

// TestOpenCodeOption1FactoryPipelineEndToEnd proves the complete Issue #65
// factory flow across the real Shell composition, attributed kodsvc. LLM proxy,
// per-project OpenCode service adapter, isolated secret-bearing verify action,
// human review producing a cross-execution Decision ID, and approval-guarded
// integration onto refs/heads/main.
func TestOpenCodeOption1FactoryPipelineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		tenantID           = "tenant-opt1"
		projectID          = "project-opt1"
		tenantAdminToken   = "dev-tenant-admin-opt1"
		adminToken         = "dev-admin-opt1"
		reviewerToken      = "dev-reviewer-opt1"
		opencodeServiceID  = "opencode-project-opt1"
		gitEdgeServiceID   = "git-edge-opt1"
		gitResourceID      = "res-git-app"
		anthropicSecretVal = "sk-ant-real-factory-key-999"
		verifyDBSecretVal  = "pg-prod-verify-secret-777"
		pinnedTestImage    = "alpine:3.20@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)

	tmp := t.TempDir()
	gitRoot := filepath.Join(tmp, "git-origin")
	bareRepo := filepath.Join(gitRoot, "app")
	if err := os.MkdirAll(bareRepo, 0o755); err != nil {
		t.Fatalf("mkdir bare repo: %v", err)
	}
	runFactoryGit(t, bareRepo, "init", "--bare", "--initial-branch=main")

	seedDir := filepath.Join(tmp, "seed")
	if err := os.MkdirAll(seedDir, 0o755); err != nil {
		t.Fatalf("mkdir seed: %v", err)
	}
	runFactoryGit(t, seedDir, "init", "--initial-branch=main")
	if err := os.WriteFile(filepath.Join(seedDir, "README.md"), []byte("# app\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runFactoryGit(t, seedDir, "add", "README.md")
	runFactoryGit(t, seedDir, "commit", "-m", "initial main commit")
	baseSHA := runFactoryGit(t, seedDir, "rev-parse", "HEAD")
	runFactoryGit(t, seedDir, "remote", "add", "origin", bareRepo)
	runFactoryGit(t, seedDir, "push", "origin", "HEAD:refs/heads/main")

	// Mock upstream Anthropic TLS provider verifying provider secret injection
	// and returning SSE token usage for attribution assertions.
	var (
		upstreamMu     sync.Mutex
		upstreamAPIKey string
		upstreamCalls  int
	)
	anthropicUpstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamMu.Lock()
		upstreamCalls++
		upstreamAPIKey = r.Header.Get("x-api-key")
		upstreamMu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, frame := range []string{
			`data: {"type":"message_start","message":{"model":"claude-sonnet-4","usage":{"input_tokens":180}}}`,
			`data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Implemented feature.go"}}`,
			`data: {"type":"message_delta","usage":{"output_tokens":95}}`,
			`data: {"type":"message_stop"}`,
		} {
			_, _ = io.WriteString(w, frame+"\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		}
	}))
	defer anthropicUpstream.Close()

	factoryRT := newFactoryRecordingRuntime()
	scope := tenancy.Scope{TenantID: tenantID, ProjectID: projectID}
	expiry := time.Now().Add(2 * time.Hour)

	app, err := shellapp.New(ctx, shellapp.Config{
		Profile:              shellapp.ProfileDevelopment,
		InstallationID:       "inst-opt1",
		DataDir:              filepath.Join(tmp, "shell-data"),
		DefaultActionTimeout: 2 * time.Minute,
		LoopInterval:         10 * time.Millisecond,
		Installation: installation.Config{
			Environment: installation.EnvironmentDevelopment,
			State:       installation.StateMemory,
			Messaging:   installation.MessagingMemory,
			Runtime:     installation.RuntimeProcess,
			Keys:        installation.KeyEphemeral,
			PublicURL:   "http://127.0.0.1:8080",
		},
		Dev: &shellapp.DevelopmentConfig{
			Bootstrap: &shellapp.Bootstrap{
				TenantID:       tenantID,
				TenantName:     "Tenant Opt1",
				ProjectID:      projectID,
				ProjectName:    "Project Opt1",
				AdminPrincipal: "admin-1",
				AdminRoles:     []string{"admin", "engineer", "reviewer", "operator", "viewer"},
			},
			Tokens: []shellapp.DevToken{
				{
					Credential: tenantAdminToken,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID},
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: adminToken,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   scope,
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: reviewerToken,
					Principal: identity.Principal{
						ID:      "reviewer-1",
						Kind:    identity.KindHuman,
						Scope:   scope,
						Roles:   []tenancy.Role{tenancy.RoleReviewer, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
			},
		},
		SecretRules: []secret.NameRule{{
			Scope: scope,
			Names: []id.SecretName{"TEST_DB_PASSWORD"},
		}},
		Models: &shellapp.ModelsConfig{
			Providers: []shellapp.ModelProviderConfig{{
				Scope:      scope,
				Name:       "anthropic",
				BaseURL:    anthropicUpstream.URL,
				SecretName: id.SecretName("ANTHROPIC_API_KEY"),
			}},
			SecretRules: []secret.NameRule{{
				Scope: scope,
				Names: []id.SecretName{"ANTHROPIC_API_KEY"},
			}},
			Transport: anthropicUpstream.Client().Transport,
		},
		Adapters: &shellapp.Adapters{
			Runtime:   factoryRT,
			Workspace: factoryRT,
		},
	})
	if err != nil {
		t.Fatalf("shellapp.New: %v", err)
	}
	appCtx, cancelApp := context.WithCancel(ctx)
	appRunErr := make(chan error, 1)
	go func() {
		appRunErr <- app.Run(appCtx)
	}()
	defer func() {
		cancelApp()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = app.Close(stopCtx)
		<-appRunErr
	}()

	kodemaHTTP := httptest.NewServer(app.Handler())
	defer kodemaHTTP.Close()

	newHTTPTransport := func(source client.CredentialSource) *client.HTTPTransport {
		return &client.HTTPTransport{
			BaseURL:      kodemaHTTP.URL,
			RoundTripper: kodemaHTTP.Client().Transport,
			Source:       source,
			Retry:        client.RetryPolicy{MaxAttempts: 2, BaseDelay: 5 * time.Millisecond, MaxDelay: 25 * time.Millisecond},
		}
	}
	tenantAdminTr := newHTTPTransport(staticBearerCred(tenantAdminToken))
	adminTr := newHTTPTransport(staticBearerCred(adminToken))
	reviewerTr := newHTTPTransport(staticBearerCred(reviewerToken))

	// Grant reviewer membership in the project so reviewer-1 passes live authority checks.
	mustInvokeOp[api.MembershipGrantRequest, api.MembershipGrantResult](
		t, ctx, tenantAdminTr, "membership.grant",
		api.MembershipGrantRequest{
			ProjectID:   projectID,
			PrincipalID: "reviewer-1",
			Roles:       []string{"reviewer", "viewer"},
		}, nil, "grant-reviewer-1",
	)

	// Store both secrets: LLM provider key (Shell-only) and privileged verify DB secret.
	mustInvokeOp[api.SecretWriteRequest, api.SecretWriteResult](
		t, ctx, adminTr, "secret.write",
		api.SecretWriteRequest{ProjectID: projectID, SecretID: "ANTHROPIC_API_KEY", Value: anthropicSecretVal},
		nil, "write-anthropic-secret",
	)
	mustInvokeOp[api.SecretWriteRequest, api.SecretWriteResult](
		t, ctx, adminTr, "secret.write",
		api.SecretWriteRequest{ProjectID: projectID, SecretID: "TEST_DB_PASSWORD", Value: verifyDBSecretVal},
		nil, "write-verify-db-secret",
	)

	// Create strict zero-role per-project OpenCode service (roles: []) and issue kodsvc. token.
	ocSvc := mustInvokeOp[api.ServiceCreateRequest, api.ServiceCreateResult](
		t, ctx, adminTr, "service.create",
		api.ServiceCreateRequest{ProjectID: projectID, ServiceID: opencodeServiceID, Roles: []string{}},
		nil, "create-opencode-svc",
	)
	ocCred := mustInvokeOp[api.ServiceCredentialIssueRequest, api.ServiceCredentialIssueResult](
		t, ctx, adminTr, "service.credential.issue",
		api.ServiceCredentialIssueRequest{ServiceID: opencodeServiceID, TTLSeconds: 3600},
		&ocSvc.Version, "issue-opencode-cred",
	)
	if ocCred.Token == nil || !strings.HasPrefix(*ocCred.Token, "kodsvc.") {
		t.Fatalf("issued opencode token = %v; want kodsvc.* prefix", ocCred.Token)
	}
	ocToken := *ocCred.Token
	ocCredFile := filepath.Join(tmp, "opencode.token")
	if err := os.WriteFile(ocCredFile, []byte(ocToken+"\n"), 0o600); err != nil {
		t.Fatalf("write opencode token: %v", err)
	}

	// Create git-edge service & resource binding so Stage 4 proves real approval-guarded
	// refs/heads/main verification with cross-execution Decision ID!
	gitSvc := mustInvokeOp[api.ServiceCreateRequest, api.ServiceCreateResult](
		t, ctx, adminTr, "service.create",
		api.ServiceCreateRequest{ProjectID: projectID, ServiceID: gitEdgeServiceID, Roles: []string{"viewer"}},
		nil, "create-git-edge-svc",
	)
	gitCred := mustInvokeOp[api.ServiceCredentialIssueRequest, api.ServiceCredentialIssueResult](
		t, ctx, adminTr, "service.credential.issue",
		api.ServiceCredentialIssueRequest{ServiceID: gitEdgeServiceID, TTLSeconds: 3600},
		&gitSvc.Version, "issue-git-edge-cred",
	)
	if gitCred.Token == nil || *gitCred.Token == "" {
		t.Fatal("git edge service credential returned nil token")
	}
	gitEdgeTr := newHTTPTransport(staticBearerCred(*gitCred.Token))

	mustInvokeOp[api.ResourceBindingRegisterRequest, api.ResourceBindingRegisterResult](
		t, ctx, adminTr, "resource.binding.register",
		api.ResourceBindingRegisterRequest{
			ProjectID:        projectID,
			BindingID:        "bind-git-app",
			ResourceID:       gitResourceID,
			EnforcingService: gitEdgeServiceID,
			TrustedOrigins:   []string{"https://git.local"},
			Operations:       []string{string(api.ResourceOperationGitRefUpdate), string(api.ResourceOperationGitRead)},
			ReportableEvents: []string{"git.ref.updated"},
		}, nil, "register-git-binding",
	)

	// Apply Stage 1-3 pipeline (implement -> verify -> review) and Stage 4 pipeline (integrate).
	deliveryYAML := fmt.Sprintf(`inputs: [repository, base_ref, expected_base]
completes_task: false
actions:
  - name: implement
    request: service/%s
    timeout: 5m
    subject:
      mode: implement
      repository: {from: inputs.repository}
      base_ref: {from: inputs.base_ref}
    outputs: [branch, candidate, summary_key]
  - name: verify
    image: %s
    timeout: 5m
    secrets: [TEST_DB_PASSWORD]
    commands: ["echo verify-ok"]
  - name: review
    request: role/reviewer
    timeout: 5m
    subject:
      operation: git.ref.update
      resource: %s
      ref: refs/heads/main
      expected_base: {from: inputs.expected_base}
      candidate: {from: implement.outputs.candidate}
    outputs: [approved]
`, opencodeServiceID, pinnedTestImage, gitResourceID)

	deliveryPipe := mustInvokeOp[api.PipelineApplyRequest, api.PipelineApplyResult](
		t, ctx, adminTr, "pipeline.apply",
		api.PipelineApplyRequest{ProjectID: projectID, Name: "feature-delivery", YAML: []byte(deliveryYAML)},
		nil, "apply-delivery-pipeline",
	)

	integrateYAML := fmt.Sprintf(`inputs: [repository, expected_base, candidate, decision_id]
actions:
  - name: integrate
    image: %s
    timeout: 5m
    commands: ["echo integrate-ok"]
`, pinnedTestImage)

	integratePipe := mustInvokeOp[api.PipelineApplyRequest, api.PipelineApplyResult](
		t, ctx, adminTr, "pipeline.apply",
		api.PipelineApplyRequest{ProjectID: projectID, Name: "integrate-main", YAML: []byte(integrateYAML)},
		nil, "apply-integrate-pipeline",
	)

	// Start the OpenCode local attribution reverse proxy bridging OpenCode's
	// Anthropic calls to real Kodema /llm/anthropic/* with kodsvc. + attribution headers!
	ocCredSource := client.FileCredentialSource{Path: ocCredFile}
	llmProxy, err := services.NewOpenCodeLLMProxy(services.OpenCodeLLMProxyConfig{
		KodemaURL:    kodemaHTTP.URL,
		Credentials:  ocCredSource,
		RoundTripper: kodemaHTTP.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	proxyURL, err := llmProxy.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("llmProxy.Start: %v", err)
	}
	defer func() { _ = llmProxy.Close(context.Background()) }()

	// Mock OpenCode v2 `opencode serve` requiring Basic Auth (`opencode:<pw>`)
	// that calls Kodema's attributed LLM proxy via OpenCodeLLMProxy and writes
	// feature.go into the task's isolated worktree.
	const opencodeServerPassword = "factory-opencode-v2-pass"
	var (
		opencodeMu             sync.Mutex
		opencodeSessionDir     string
		opencodeSawLLMResponse bool
	)
	opencodeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "opencode" || pass != opencodeServerPassword {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"_tag":"UnauthorizedError","message":"Authentication required"}`)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/session":
			var req struct {
				Location *struct {
					Directory string `json:"directory"`
				} `json:"location"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.Location != nil {
				opencodeMu.Lock()
				opencodeSessionDir = req.Location.Directory
				opencodeMu.Unlock()
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":{"id":"oc-factory-sess-1"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/oc-factory-sess-1/move":
			var req struct {
				Directory string `json:"directory"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			opencodeMu.Lock()
			opencodeSessionDir = req.Directory
			opencodeMu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/oc-factory-sess-1/prompt":
			opencodeMu.Lock()
			dir := opencodeSessionDir
			opencodeMu.Unlock()
			llmReq, _ := http.NewRequestWithContext(r.Context(), http.MethodPost,
				proxyURL+"/llm/anthropic/v1/messages",
				strings.NewReader(`{"model":"claude-sonnet-4","stream":true,"messages":[{"role":"user","content":"implement feature"}]}`))
			llmReq.Header.Set("Content-Type", "application/json")
			llmReq.Header.Set("Authorization", "Bearer "+llmProxy.LocalAPIKey())
			llmResp, err := http.DefaultClient.Do(llmReq)
			if err != nil {
				http.Error(w, "llm proxy error: "+err.Error(), http.StatusBadGateway)
				return
			}
			llmBody, _ := io.ReadAll(llmResp.Body)
			_ = llmResp.Body.Close()
			if llmResp.StatusCode != http.StatusOK || !strings.Contains(string(llmBody), "Implemented feature.go") {
				http.Error(w, fmt.Sprintf("unexpected llm response %d: %s", llmResp.StatusCode, string(llmBody)), http.StatusBadGateway)
				return
			}
			opencodeMu.Lock()
			opencodeSawLLMResponse = true
			opencodeMu.Unlock()

			if err := os.WriteFile(filepath.Join(dir, "feature.go"), []byte("package app\n\nfunc Feature() int { return 42 }\n"), 0o644); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":{"id":"usr-1"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/experimental/session/oc-factory-sess-1/wait":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/session/oc-factory-sess-1/message":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":[{"id":"idle-1","type":"idle","outcome":"succeeded"},{"id":"msg-1","type":"assistant","content":[{"type":"text","text":"Implemented Feature() returning 42 in feature.go."}]},{"id":"usr-1","type":"user"}],"cursor":{}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer opencodeServer.Close()

	// Start the real OpenCode per-project service adapter wired to Kodema via
	// real HTTP transport + live SSE event stream!
	ocTransport := newHTTPTransport(ocCredSource)
	ocStateDir := filepath.Join(tmp, "opencode-state")
	ocStore, err := services.NewFileOpenCodeStateStore(filepath.Join(ocStateDir, "opencode"))
	if err != nil {
		t.Fatalf("NewFileOpenCodeStateStore: %v", err)
	}
	gitExec, err := servicesapp.NewOSGitExecutor(servicesapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	worktrees, err := services.NewGitOpenCodeWorktreeManager(services.GitOpenCodeWorktreeConfig{
		RemoteBaseURL:  "file://" + gitRoot,
		StateDirectory: ocStateDir,
		Executor:       gitExec,
	})
	if err != nil {
		t.Fatalf("NewGitOpenCodeWorktreeManager: %v", err)
	}
	ocClient, err := services.NewHTTPOpenCodeClient(services.HTTPOpenCodeClientConfig{
		BaseURL:  opencodeServer.URL,
		Password: opencodeServerPassword,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}
	ocAdapter, err := services.NewOpenCodeAdapter(services.OpenCodeAdapterConfig{
		ServiceID:      opencodeServiceID,
		TenantID:       tenantID,
		ProjectID:      projectID,
		Handler:        ocTransport,
		Stream:         ocTransport,
		OpenCode:       ocClient,
		Store:          ocStore,
		Worktrees:      worktrees,
		Proxy:          llmProxy,
		Clock:          servicesapp.SystemClock{},
		RescanInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter: %v", err)
	}

	adapterCtx, cancelAdapter := context.WithCancel(ctx)
	adapterDone := make(chan error, 1)
	go func() {
		adapterDone <- ocAdapter.Run(adapterCtx)
	}()
	defer func() {
		cancelAdapter()
		<-adapterDone
	}()

	// Launch Execution 1: implement -> verify -> review
	started1 := mustInvokeOp[api.TaskCreateAndStartRequest, api.TaskCreateAndStartResult](
		t, ctx, adminTr, "task.create_and_start",
		api.TaskCreateAndStartRequest{
			ProjectID:   projectID,
			Title:       "Implement Feature 42",
			Description: "Add Feature() returning 42 to app",
			PipelineID:  deliveryPipe.PipelineID,
			Inputs: map[string]string{
				"repository":    "app",
				"base_ref":      "refs/heads/main",
				"expected_base": baseSHA,
			},
		}, nil, "start-task-opt1",
	)
	taskID := started1.TaskID
	exec1ID := started1.ExecutionID

	// Wait until Execution 1 reaches the `review` request action (meaning BOTH
	// `implement` via OpenCode AND isolated `verify` with TEST_DB_PASSWORD succeeded!).
	var exec1 api.ExecutionInspectResult
	var reviewAction api.ExecutionAction
	waitUntil := time.Now().Add(15 * time.Second)
	for time.Now().Before(waitUntil) {
		exec1 = mustInvokeOp[api.ExecutionInspectRequest, api.ExecutionInspectResult](
			t, ctx, adminTr, "execution.inspect",
			api.ExecutionInspectRequest{ExecutionID: exec1ID}, nil, "",
		)
		if exec1.State == "failed" || exec1.State == "cancelled" {
			t.Fatalf("execution 1 terminated unexpectedly: state=%s failure=%+v actions=%+v", exec1.State, exec1.Failure, exec1.Actions)
		}
		if len(exec1.Actions) == 3 && exec1.Actions[2].State == "running" {
			reviewAction = exec1.Actions[2]
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if reviewAction.ActionID == "" {
		t.Fatalf("timed out waiting for execution 1 to reach review; last state: %+v", exec1)
	}

	// Verify Stage 1 (implement) outputs, KV summary, and attributed LLM usage!
	implAction := exec1.Actions[0]
	if implAction.State != "succeeded" {
		t.Fatalf("implement state = %q; want succeeded", implAction.State)
	}
	wantBranch := "refs/heads/work/" + taskID
	candidateSHA := implAction.Outputs["candidate"]
	summaryKey := implAction.Outputs["summary_key"]
	if implAction.Outputs["branch"] != wantBranch {
		t.Fatalf("implement branch = %q; want %q", implAction.Outputs["branch"], wantBranch)
	}
	if len(candidateSHA) != 40 {
		t.Fatalf("implement candidate = %q; want 40-char SHA", candidateSHA)
	}
	if !api.ValidKVKey(summaryKey) {
		t.Fatalf("implement summary_key = %q; want valid KV key", summaryKey)
	}
	summaryObj, err := ocTransport.KVGet(ctx, projectID, summaryKey)
	if err != nil {
		t.Fatalf("kv.get summary_key: %v", err)
	}
	if !strings.Contains(string(summaryObj.Data), "Implemented Feature() returning 42") {
		t.Fatalf("KV summary content = %q; missing expected text", string(summaryObj.Data))
	}
	opencodeMu.Lock()
	sawLLM := opencodeSawLLMResponse
	opencodeMu.Unlock()
	if !sawLLM {
		t.Fatal("OpenCode server did not receive response from attributed LLM proxy")
	}
	upstreamMu.Lock()
	if upstreamCalls != 1 || upstreamAPIKey != anthropicSecretVal {
		t.Fatalf("upstream calls=%d apiKey=%q; want 1 call with injected provider secret", upstreamCalls, upstreamAPIKey)
	}
	upstreamMu.Unlock()

	if exec1.ModelUsage == nil {
		t.Fatal("execution 1 inspect returned nil ModelUsage; expected attributed kodsvc. usage")
	}
	if exec1.ModelUsage.Requests != 1 || exec1.ModelUsage.InputTokens != 180 ||
		exec1.ModelUsage.OutputTokens != 95 || exec1.ModelUsage.TotalTokens != 275 {
		t.Fatalf("execution 1 ModelUsage = %+v; want 1 req / 180 in / 95 out / 275 total", exec1.ModelUsage)
	}

	// Verify Stage 2 (verify) secret isolation:
	// 1. Isolated verify container action received TEST_DB_PASSWORD and NOT ANTHROPIC_API_KEY.
	prepared := factoryRT.snapshotPrepared()
	if len(prepared) != 1 {
		t.Fatalf("prepared container count = %d; want 1 (verify)", len(prepared))
	}
	if len(prepared[0].Secrets) != 1 || prepared[0].Secrets[0].Name != "TEST_DB_PASSWORD" || prepared[0].Secrets[0].Value != verifyDBSecretVal {
		t.Fatalf("verify prepared secrets = %+v; want only TEST_DB_PASSWORD", prepared[0].Secrets)
	}
	// 2. OpenCode service (roles: []) CANNOT mutate secrets/pipelines or call /llm after implement settled!
	for _, forbiddenOp := range []struct {
		op    string
		input any
	}{
		{"secret.write", api.SecretWriteRequest{ProjectID: projectID, SecretID: "TEST_DB_PASSWORD", Value: "hijack"}},
		{"pipeline.apply", api.PipelineApplyRequest{ProjectID: projectID, Name: "evil", YAML: []byte(integrateYAML)}},
	} {
		rawIn, _ := json.Marshal(forbiddenOp.input)
		_, opErr := ocTransport.Invoke(ctx, api.Invocation{
			Operation: forbiddenOp.op,
			Request:   api.Request{Input: rawIn, IdempotencyKey: "forbid-" + forbiddenOp.op},
		})
		if opErr == nil {
			t.Fatalf("roleless opencode service was allowed to call %s", forbiddenOp.op)
		}
	}
	// Calling /llm/anthropic/* after implement resolved MUST fail with 403!
	postResolveReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, kodemaHTTP.URL+"/llm/anthropic/v1/messages",
		strings.NewReader(`{"model":"claude-sonnet-4"}`))
	postResolveReq.Header.Set("Authorization", "Bearer "+ocToken)
	postResolveReq.Header.Set("X-Kodema-Execution", exec1ID)
	postResolveReq.Header.Set("X-Kodema-Action", implAction.ActionID)
	postResolveResp, err := kodemaHTTP.Client().Do(postResolveReq)
	if err != nil {
		t.Fatalf("post-resolve llm call: %v", err)
	}
	_ = postResolveResp.Body.Close()
	if postResolveResp.StatusCode != http.StatusForbidden {
		t.Fatalf("post-resolve kodsvc. /llm status = %d; want 403", postResolveResp.StatusCode)
	}

	// Stage 3 (review): Human reviewer approves -> produces durable cross-execution Decision ID!
	// Re-inspect right before resolving so ExpectedVersion matches any post-verify cleanup increment.
	exec1 = mustInvokeOp[api.ExecutionInspectRequest, api.ExecutionInspectResult](
		t, ctx, adminTr, "execution.inspect",
		api.ExecutionInspectRequest{ExecutionID: exec1ID}, nil, "",
	)
	reviewAction = exec1.Actions[2]
	resolveReview := mustInvokeOp[api.ExecutionRequestResolveRequest, api.ExecutionRequestResolveResult](
		t, ctx, reviewerTr, "execution.request.resolve",
		api.ExecutionRequestResolveRequest{
			ExecutionID:           exec1ID,
			ActionID:              reviewAction.ActionID,
			ExpectedActionVersion: reviewAction.Version,
			Decision:              "pass",
			Outputs:               map[string]string{"approved": "true"},
			Note:                  "LGTM",
		}, &exec1.Version, "approve-review-opt1",
	)
	decisionID := resolveReview.DecisionID
	if decisionID == "" {
		t.Fatal("human reviewer pass resolution returned empty decision_id")
	}

	// Wait for Execution 1 to settle as succeeded (task stays open because completes_task: false).
	wait1 := time.Now().Add(5 * time.Second)
	for time.Now().Before(wait1) {
		exec1 = mustInvokeOp[api.ExecutionInspectRequest, api.ExecutionInspectResult](
			t, ctx, adminTr, "execution.inspect",
			api.ExecutionInspectRequest{ExecutionID: exec1ID}, nil, "",
		)
		if exec1.State == "succeeded" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if exec1.State != "succeeded" {
		t.Fatalf("execution 1 final state = %q; want succeeded", exec1.State)
	}

	// Stage 4 (integrate): Subsequent execution uses Decision ID to pass approval-guarded
	// refs/heads/main verification and pushes the approved candidate onto refs/heads/main!
	var integratedVerified bool
	var integrateMu sync.Mutex
	factoryRT.mu.Lock()
	factoryRT.onStart = func(_ kernelengine.StartRequest, prep kernelengine.PrepareRequest) error {
		if prep.Credential == "" {
			return fmt.Errorf("integrate workload had empty execution credential")
		}
		verifyOp := func(decision string) (api.VerifyResponse, error) {
			in, err := json.Marshal(api.VerifyRequest{
				ActorCredential: prep.Credential,
				ResourceID:      gitResourceID,
				Operation:       string(api.ResourceOperationGitRefUpdate),
				Approval:        "role/reviewer",
				DecisionID:      decision,
				Subject: api.Subject{Fields: []api.SubjectField{
					{Name: "operation", Kind: "string", Value: "git.ref.update"},
					{Name: "resource", Kind: "string", Value: gitResourceID},
					{Name: "ref", Kind: "string", Value: "refs/heads/main"},
					{Name: "expected_base", Kind: "string", Value: baseSHA},
					{Name: "candidate", Kind: "string", Value: candidateSHA},
				}},
			})
			if err != nil {
				return api.VerifyResponse{}, err
			}
			res, err := gitEdgeTr.Invoke(ctx, api.Invocation{
				Operation: "actor.verify",
				Request:   api.Request{Input: in},
			})
			if err != nil {
				return api.VerifyResponse{}, err
			}
			var out api.VerifyResponse
			if err := json.Unmarshal(res.Body, &out); err != nil {
				return api.VerifyResponse{}, err
			}
			return out, nil
		}

		// 1. Without Decision ID -> approval-guarded refs/heads/main MUST be refused (forbidden)!
		if unapproved, err := verifyOp(""); err == nil && unapproved.Approval {
			return fmt.Errorf("refs/heads/main push approved without decision_id: %+v", unapproved)
		} else if err == nil {
			return fmt.Errorf("expected forbidden refusal without decision_id, got %+v", unapproved)
		}

		// 2. With the cross-execution Decision ID -> approval-guarded refs/heads/main MUST succeed!
		approved, err := verifyOp(decisionID)
		if err != nil {
			return fmt.Errorf("actor.verify with decision_id %q failed: %w", decisionID, err)
		}
		if !approved.Approval || approved.VerificationID == "" {
			return fmt.Errorf("actor.verify with decision_id %q was not approved: %+v", decisionID, approved)
		}

		// Push approved candidate onto refs/heads/main in bareRepo!
		cmd := exec.Command("git", "-C", bareRepo, "update-ref", "refs/heads/main", candidateSHA, baseSHA)
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("git update-ref main failed: %v: %s", err, string(out))
		}
		integrateMu.Lock()
		integratedVerified = true
		integrateMu.Unlock()
		return nil
	}
	factoryRT.mu.Unlock()

	taskAfter1 := mustInvokeOp[api.TaskReadRequest, api.TaskReadResult](
		t, ctx, adminTr, "task.read", api.TaskReadRequest{TaskID: taskID}, nil, "",
	)
	started2 := mustInvokeOp[api.TaskStartRequest, api.TaskStartResult](
		t, ctx, adminTr, "task.start",
		api.TaskStartRequest{
			TaskID:     taskID,
			PipelineID: integratePipe.PipelineID,
			Inputs: map[string]string{
				"repository":    "app",
				"expected_base": baseSHA,
				"candidate":     candidateSHA,
				"decision_id":   decisionID,
			},
		}, &taskAfter1.LifecycleVersion, "start-integrate-opt1",
	)

	var exec2 api.ExecutionInspectResult
	wait2 := time.Now().Add(5 * time.Second)
	for time.Now().Before(wait2) {
		exec2 = mustInvokeOp[api.ExecutionInspectRequest, api.ExecutionInspectResult](
			t, ctx, adminTr, "execution.inspect",
			api.ExecutionInspectRequest{ExecutionID: started2.ExecutionID}, nil, "",
		)
		if exec2.State == "succeeded" || exec2.State == "failed" {
			break
		}
		factoryRT.mu.Lock()
		hookErr := factoryRT.lastHookErr
		factoryRT.mu.Unlock()
		if hookErr != nil {
			t.Fatalf("integrate hook failed: %v", hookErr)
		}
		time.Sleep(15 * time.Millisecond)
	}
	if exec2.State != "succeeded" {
		t.Fatalf("execution 2 (integrate) state = %q failure=%+v; want succeeded", exec2.State, exec2.Failure)
	}
	integrateMu.Lock()
	verified := integratedVerified
	integrateMu.Unlock()
	if !verified {
		t.Fatal("integrate execution did not verify decision_id and advance refs/heads/main")
	}

	// Confirm refs/heads/main on the bare repo now points to candidateSHA and contains feature.go!
	mainTip := runFactoryGit(t, bareRepo, "rev-parse", "refs/heads/main")
	if mainTip != candidateSHA {
		t.Fatalf("bare repo refs/heads/main = %q; want %q", mainTip, candidateSHA)
	}
	mainFiles := runFactoryGit(t, bareRepo, "ls-tree", "--name-only", mainTip)
	if !strings.Contains(mainFiles, "feature.go") {
		t.Fatalf("refs/heads/main missing feature.go: %s", mainFiles)
	}
}
