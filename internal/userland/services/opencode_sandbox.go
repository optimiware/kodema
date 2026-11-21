package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/userland/sandbox"
)

const (
	defaultSandboxReadyTimeout      = 2 * time.Minute
	defaultSandboxHealthInterval    = 2 * time.Second
	defaultSandboxStopTimeout       = 30 * time.Second
	defaultSandboxTTL               = 30 * time.Minute
	sandboxReadyLinePrefix          = "KODEMA_SANDBOX_ENDPOINT="
	maxConsecutiveHealthzFailures   = 2
	defaultSandboxWorkspaceDir      = "/workspace"
	sandboxContainerTokenFilePath   = "/tmp/kodema-sandbox.token"
	sandboxCommitPushCommandTimeout = 120
)

// KodemaExecutionSandboxConfig configures KodemaExecutionSandboxManager
// (Approach 2A: ephemeral per-task sandboxes scheduled as Kodema inline
// container executions through the public API — no second container orchestrator).
type KodemaExecutionSandboxConfig struct {
	TenantID         id.TenantID
	ProjectID        id.ProjectID
	Handler          api.Handler
	Clock            Clock
	RoundTripper     http.RoundTripper
	EndpointResolver func(ctx context.Context, executionID string, port int) (string, error)
	EndpointTemplate string
	ReadyTimeout     time.Duration
	HealthInterval   time.Duration
	StopTimeout      time.Duration
	SandboxTTL       time.Duration
}

type managedSandboxMonitor struct {
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
	mu       sync.Mutex
	err      error
}

func (m *managedSandboxMonitor) fail(err error) {
	m.mu.Lock()
	if m.err == nil {
		m.err = err
	}
	m.mu.Unlock()
}

func (m *managedSandboxMonitor) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.err
}

func (m *managedSandboxMonitor) stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

// KodemaExecutionSandboxManager implements OpenCodeSandboxManager using Kodema's
// native public execution API (`pipeline.start` inline YAML, `execution.inspect`,
// `execution.logs.read`, and `execution.cancel`). Kodema's existing Kernel and
// Podman/Kubernetes runtime own scheduling, cgroups, OOM diagnostics,
// NetworkPolicy, and hard SIGKILL cleanup.
type KodemaExecutionSandboxManager struct {
	tenant           id.TenantID
	project          id.ProjectID
	handler          api.Handler
	clock            Clock
	httpClient       *http.Client
	endpointResolver func(ctx context.Context, executionID string, port int) (string, error)
	endpointTemplate string
	readyTimeout     time.Duration
	healthInterval   time.Duration
	stopTimeout      time.Duration
	sandboxTTL       time.Duration

	mu       sync.Mutex
	monitors map[string]*managedSandboxMonitor // sandboxID -> monitor
}

var _ OpenCodeSandboxManager = (*KodemaExecutionSandboxManager)(nil)

// NewKodemaExecutionSandboxManager validates config and constructs an Approach 2A
// Kodema-native sandbox manager.
func NewKodemaExecutionSandboxManager(cfg KodemaExecutionSandboxConfig) (*KodemaExecutionSandboxManager, error) {
	if _, err := id.ParseTenantID(string(cfg.TenantID)); err != nil {
		return nil, fmt.Errorf("sandbox manager tenant %q is invalid: %w", cfg.TenantID, err)
	}
	if _, err := id.ParseProjectID(string(cfg.ProjectID)); err != nil {
		return nil, fmt.Errorf("sandbox manager project %q is invalid: %w", cfg.ProjectID, err)
	}
	if cfg.Handler == nil {
		return nil, errors.New("sandbox manager requires a public API handler")
	}
	rt := cfg.RoundTripper
	if rt == nil {
		rt = defaultNoProxyTransport()
	}
	readyTimeout := cfg.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = defaultSandboxReadyTimeout
	}
	healthInterval := cfg.HealthInterval
	if healthInterval <= 0 {
		healthInterval = defaultSandboxHealthInterval
	}
	stopTimeout := cfg.StopTimeout
	if stopTimeout <= 0 {
		stopTimeout = defaultSandboxStopTimeout
	}
	sandboxTTL := cfg.SandboxTTL
	if sandboxTTL <= 0 {
		sandboxTTL = defaultSandboxTTL
	}
	return &KodemaExecutionSandboxManager{
		tenant:           cfg.TenantID,
		project:          cfg.ProjectID,
		handler:          cfg.Handler,
		clock:            cfg.Clock,
		httpClient:       &http.Client{Transport: rt, Timeout: 5 * time.Second},
		endpointResolver: cfg.EndpointResolver,
		endpointTemplate: strings.TrimSpace(cfg.EndpointTemplate),
		readyTimeout:     readyTimeout,
		healthInterval:   healthInterval,
		stopTimeout:      stopTimeout,
		sandboxTTL:       sandboxTTL,
		monitors:         make(map[string]*managedSandboxMonitor),
	}, nil
}

// Acquire launches an ephemeral Kodema inline container execution running
// `/kodema/bin/kodema sandbox serve`, waits until `/healthz` reports 200 OK,
// and starts a health/lifecycle watcher so mid-turn crashes or OOM kills are
// detected promptly.
func (m *KodemaExecutionSandboxManager) Acquire(ctx context.Context, req OpenCodeSandboxRequest) (OpenCodeSandboxLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSandboxRequest(req); err != nil {
		return OpenCodeSandboxLease{}, err
	}
	port := req.Port
	if port == 0 {
		port = DefaultOpenCodeSandboxPort
	}
	req.Port = port
	if req.Timeout <= 0 {
		req.Timeout = m.sandboxTTL
	}

	token, err := sandbox.GenerateBearerToken()
	if err != nil {
		return OpenCodeSandboxLease{}, err
	}
	yamlBytes, err := BuildSandboxPipelineYAML(req, token)
	if err != nil {
		return OpenCodeSandboxLease{}, err
	}

	idemScope := fmt.Sprintf("opencode-sandbox:%s:%s:%s", req.ExecutionID, req.ActionID, req.TaskID)
	sum := sha256.Sum256([]byte(idemScope))
	idemKey := "oc-sbox:" + hex.EncodeToString(sum[:])[:32]

	decoded, err := m.invoke(ctx, OperationPipelineStart, api.PipelineStartRequest{
		ProjectID: string(m.project),
		YAML:      yamlBytes,
	}, nil, idemKey)
	if err != nil {
		return OpenCodeSandboxLease{}, fmt.Errorf("start ephemeral sandbox execution: %w", err)
	}
	started, ok := decoded.(api.PipelineStartResult)
	if !ok {
		return OpenCodeSandboxLease{}, fmt.Errorf("pipeline.start returned unexpected %T", decoded)
	}
	execID := started.ExecutionID

	// Wait for endpoint discovery + /healthz readiness while watching execution state.
	readyCtx, cancelReady := context.WithTimeout(ctx, m.readyTimeout)
	defer cancelReady()

	baseURL, err := m.waitForSandboxReady(readyCtx, execID, port)
	if err != nil {
		// Clean up the sandbox execution on startup failure or cancellation.
		_ = m.cancelAndConfirmStop(context.WithoutCancel(ctx), execID)
		return OpenCodeSandboxLease{}, err
	}

	cleanBase := strings.TrimRight(baseURL, "/")
	mon := &managedSandboxMonitor{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	sandboxID := "sbox-" + execID
	m.mu.Lock()
	m.monitors[sandboxID] = mon
	m.mu.Unlock()

	lease := OpenCodeSandboxLease{
		SandboxID:    sandboxID,
		ExecutionID:  execID,
		EndpointURL:  cleanBase + "/mcp",
		HealthURL:    cleanBase + "/healthz",
		BearerToken:  token,
		WorkspaceDir: defaultSandboxWorkspaceDir,
		Done:         mon.doneCh,
		Err:          mon.Err,
	}

	go m.watchSandbox(lease, mon)
	return lease, nil
}

// Release cancels the ephemeral sandbox execution via Kodema's public API and
// waits for terminal confirmation (confirmed stop).
func (m *KodemaExecutionSandboxManager) Release(ctx context.Context, lease OpenCodeSandboxLease) error {
	if lease.SandboxID != "" {
		m.mu.Lock()
		mon := m.monitors[lease.SandboxID]
		delete(m.monitors, lease.SandboxID)
		m.mu.Unlock()
		if mon != nil {
			mon.stop()
		}
	}
	if strings.TrimSpace(lease.ExecutionID) == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return m.cancelAndConfirmStop(context.WithoutCancel(ctx), lease.ExecutionID)
}

func (m *KodemaExecutionSandboxManager) watchSandbox(lease OpenCodeSandboxLease, mon *managedSandboxMonitor) {
	ticker := time.NewTicker(m.healthInterval)
	defer ticker.Stop()
	consecutiveHealthFailures := 0

	for {
		select {
		case <-mon.stopCh:
			return
		case <-ticker.C:
		}

		checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		inspected, inspectErr := m.inspectExecution(checkCtx, lease.ExecutionID)
		if inspectErr == nil {
			if isTerminalExecutionState(inspected.State) {
				cancel()
				mon.fail(fmt.Errorf("%w: sandbox execution %s ended in state %s%s",
					ErrOpenCodeSandboxTerminated, lease.ExecutionID, inspected.State, sandboxFailureDetail(inspected)))
				close(mon.doneCh)
				return
			}
		}

		healthErr := m.probeHealthz(checkCtx, lease.HealthURL)
		cancel()
		if healthErr == nil {
			consecutiveHealthFailures = 0
			continue
		}
		consecutiveHealthFailures++
		if consecutiveHealthFailures >= maxConsecutiveHealthzFailures {
			mon.fail(fmt.Errorf("%w: sandbox %s failed health checks: %v",
				ErrOpenCodeSandboxTerminated, lease.SandboxID, healthErr))
			close(mon.doneCh)
			return
		}
	}
}

func (m *KodemaExecutionSandboxManager) waitForSandboxReady(ctx context.Context, executionID string, port int) (string, error) {
	pollDelay := min(m.healthInterval, 100*time.Millisecond)
	if pollDelay <= 0 {
		pollDelay = 50 * time.Millisecond
	}
	ticker := time.NewTicker(pollDelay)
	defer ticker.Stop()

	var resolvedBase string
	for {
		inspected, err := m.inspectExecution(ctx, executionID)
		if err == nil && isTerminalExecutionState(inspected.State) {
			return "", fmt.Errorf("%w: sandbox execution %s terminated before becoming ready (state=%s%s)",
				ErrOpenCodeSandboxTerminated, executionID, inspected.State, sandboxFailureDetail(inspected))
		}

		if resolvedBase == "" {
			resolvedBase, _ = m.resolveEndpoint(ctx, executionID, port)
		}
		if resolvedBase != "" {
			healthURL := strings.TrimRight(resolvedBase, "/") + "/healthz"
			if err := m.probeHealthz(ctx, healthURL); err == nil {
				return resolvedBase, nil
			}
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("timed out waiting for sandbox execution %s readiness: %w", executionID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (m *KodemaExecutionSandboxManager) resolveEndpoint(ctx context.Context, executionID string, port int) (string, error) {
	if m.endpointResolver != nil {
		return m.endpointResolver(ctx, executionID, port)
	}
	if m.endpointTemplate != "" {
		return fmt.Sprintf(m.endpointTemplate, executionID, port), nil
	}
	// Default: inspect stdout logs of the sandbox action for KODEMA_SANDBOX_ENDPOINT=<url>.
	decoded, err := m.invoke(ctx, OperationExecutionLogsRead, api.ExecutionLogsReadRequest{
		ExecutionID: executionID,
		ActionID:    "sandbox",
		Stream:      "stdout",
	}, nil, "")
	if err != nil {
		return "", err
	}
	logs, ok := decoded.(api.ExecutionLogsReadResult)
	if !ok {
		return "", fmt.Errorf("execution.logs.read returned unexpected %T", decoded)
	}
	for _, chunk := range logs.Chunks {
		for _, line := range strings.Split(string(chunk.Data), "\n") {
			line = strings.TrimSpace(line)
			if after, ok := strings.CutPrefix(line, sandboxReadyLinePrefix); ok {
				candidate := strings.TrimSpace(after)
				if parsed, parseErr := url.Parse(candidate); parseErr == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
					return candidate, nil
				}
			}
		}
	}
	return "", errors.New("sandbox endpoint not announced yet")
}

func (m *KodemaExecutionSandboxManager) probeHealthz(ctx context.Context, healthURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func (m *KodemaExecutionSandboxManager) cancelAndConfirmStop(ctx context.Context, executionID string) error {
	stopCtx, cancel := context.WithTimeout(ctx, m.stopTimeout)
	defer cancel()

	// Step 1: Inspect and request cancellation if non-terminal.
	for attempt := 0; attempt < 2; attempt++ {
		inspected, err := m.inspectExecution(stopCtx, executionID)
		if err != nil {
			var pubErr *api.Error
			if errors.As(err, &pubErr) && pubErr.Code == api.CodeNotFound {
				return nil
			}
			return fmt.Errorf("inspect sandbox execution %s for stop: %w", executionID, err)
		}
		if isTerminalExecutionState(inspected.State) {
			return nil
		}
		expected := inspected.Version
		sum := sha256.Sum256([]byte(fmt.Sprintf("cancel-sandbox:%s:v%d", executionID, expected)))
		idemKey := "oc-sbox-cancel:" + hex.EncodeToString(sum[:])[:28]
		_, cancelErr := m.invoke(stopCtx, OperationExecutionCancel, api.ExecutionCancelRequest{
			ExecutionID: executionID,
		}, &expected, idemKey)
		if cancelErr == nil {
			break
		}
		var pubErr *api.Error
		if errors.As(cancelErr, &pubErr) && (pubErr.Code == api.CodeVersionConflict || pubErr.Code == api.CodeNotFound) {
			continue
		}
		// Keep polling if state may already have transitioned concurrently.
		break
	}

	// Step 2: Poll until terminal state confirmation (confirmed stop).
	pollDelay := min(m.healthInterval, 100*time.Millisecond)
	if pollDelay <= 0 {
		pollDelay = 50 * time.Millisecond
	}
	ticker := time.NewTicker(pollDelay)
	defer ticker.Stop()

	for {
		inspected, err := m.inspectExecution(stopCtx, executionID)
		if err != nil {
			var pubErr *api.Error
			if errors.As(err, &pubErr) && pubErr.Code == api.CodeNotFound {
				return nil
			}
		} else if isTerminalExecutionState(inspected.State) {
			return nil
		}
		select {
		case <-stopCtx.Done():
			return fmt.Errorf("timed out waiting for confirmed stop of sandbox execution %s: %w", executionID, stopCtx.Err())
		case <-ticker.C:
		}
	}
}

func (m *KodemaExecutionSandboxManager) inspectExecution(ctx context.Context, executionID string) (api.ExecutionInspectResult, error) {
	decoded, err := m.invoke(ctx, OperationInspect, api.ExecutionInspectRequest{
		ExecutionID: executionID,
	}, nil, "")
	if err != nil {
		return api.ExecutionInspectResult{}, err
	}
	res, ok := decoded.(api.ExecutionInspectResult)
	if !ok {
		return api.ExecutionInspectResult{}, fmt.Errorf("execution.inspect returned unexpected %T", decoded)
	}
	return res, nil
}

func (m *KodemaExecutionSandboxManager) invoke(ctx context.Context, operation string, request any, expectedVersion *uint64, idempotencyKey string) (any, error) {
	codec, ok := publicCodecs[operation]
	if !ok {
		return nil, fmt.Errorf("operation %q has no public codec", operation)
	}
	input, err := codec.EncodeRequest(request)
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", operation, err)
	}
	result, err := m.handler.Invoke(ctx, api.Invocation{
		Operation: operation,
		Request: api.Request{
			Input:           input,
			ExpectedVersion: expectedVersion,
			IdempotencyKey:  idempotencyKey,
		},
	})
	if err != nil {
		return nil, err
	}
	if result.Operation != operation {
		return nil, fmt.Errorf("operation %q returned result for %q", operation, result.Operation)
	}
	return codec.DecodeResult(result.Body)
}

func isTerminalExecutionState(state string) bool {
	switch state {
	case "succeeded", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func sandboxFailureDetail(inspected api.ExecutionInspectResult) string {
	if inspected.Failure != nil {
		parts := make([]string, 0, 2)
		if strings.TrimSpace(inspected.Failure.Reason) != "" {
			parts = append(parts, strings.TrimSpace(inspected.Failure.Reason))
		}
		if strings.TrimSpace(inspected.Failure.Detail) != "" {
			parts = append(parts, strings.TrimSpace(inspected.Failure.Detail))
		}
		if len(parts) > 0 {
			return " (" + strings.Join(parts, ": ") + ")"
		}
	}
	return ""
}

func validateSandboxRequest(req OpenCodeSandboxRequest) error {
	if !validSandboxImage(req.Image) {
		return fmt.Errorf("sandbox image %q is invalid", req.Image)
	}
	if req.Port != 0 && (req.Port < 1024 || req.Port > 65535) {
		return fmt.Errorf("sandbox port %d must be between 1024 and 65535", req.Port)
	}
	if req.Repository != "" && !validRepository(req.Repository) {
		return fmt.Errorf("sandbox repository %q is invalid", req.Repository)
	}
	if req.BaseRef != "" && !validOpenCodeRef(req.BaseRef) {
		return fmt.Errorf("sandbox base_ref %q is invalid", req.BaseRef)
	}
	if req.WorkBranch != "" && !validOpenCodeRef(req.WorkBranch) {
		return fmt.Errorf("sandbox work_branch %q is invalid", req.WorkBranch)
	}
	if _, err := normalizeSandboxCaches(req.Caches); err != nil {
		return err
	}
	if req.Memory != "" && !validSandboxResourceQty(req.Memory) {
		return fmt.Errorf("sandbox memory %q is invalid", req.Memory)
	}
	if req.CPU != "" && !validSandboxResourceQty(req.CPU) {
		return fmt.Errorf("sandbox cpu %q is invalid", req.CPU)
	}
	return nil
}

// BuildSandboxPipelineYAML constructs an inline Kodema pipeline YAML running
// `/kodema/bin/kodema sandbox serve` inside the requested stock toolchain image.
// The ephemeral bearer token is written with umask 077 to /tmp/kodema-sandbox.token
// (outside /workspace) and passed via --token-file so it never appears in argv,
// process environment, or /workspace.
func BuildSandboxPipelineYAML(req OpenCodeSandboxRequest, token string) ([]byte, error) {
	if err := validateSandboxRequest(req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n'\"\\ \t") {
		return nil, errors.New("sandbox bearer token is invalid")
	}
	port := req.Port
	if port == 0 {
		port = DefaultOpenCodeSandboxPort
	}
	ttl := req.Timeout
	if ttl <= 0 {
		ttl = defaultSandboxTTL
	}

	var script strings.Builder
	script.WriteString("set -eu\n")
	script.WriteString("umask 077\n")
	fmt.Fprintf(&script, "cat <<'KODEMA_SBOX_TOK' > %s\n%s\nKODEMA_SBOX_TOK\n", sandboxContainerTokenFilePath, token)
	fmt.Fprintf(&script, "chmod 0600 %s\n", sandboxContainerTokenFilePath)
	script.WriteString("umask 022\n")

	if req.Repository != "" {
		branchShort := strings.TrimPrefix(req.WorkBranch, "refs/heads/")
		if branchShort == "" {
			branchShort = "work/" + req.TaskID
		}
		baseRef := req.BaseRef
		if baseRef == "" {
			baseRef = DefaultOpenCodeBaseRef
		}
		baseShort := strings.TrimPrefix(baseRef, "refs/heads/")
		script.WriteString("if [ -n \"${KODEMA_GIT_URL:-}\" ] && command -v git >/dev/null 2>&1; then\n")
		fmt.Fprintf(&script, "  REPO_URL=\"${KODEMA_GIT_URL%%/}/%s\"\n", req.Repository)
		script.WriteString("  if [ ! -d /workspace/.git ]; then\n")
		script.WriteString("    git clone --no-tags \"$REPO_URL\" /workspace\n")
		script.WriteString("  fi\n")
		script.WriteString("  cd /workspace\n")
		fmt.Fprintf(&script, "  if git ls-remote --exit-code --heads origin %s >/dev/null 2>&1; then\n", shellQuote(branchShort))
		fmt.Fprintf(&script, "    git fetch --no-tags origin %s && git checkout -B %s FETCH_HEAD\n", shellQuote(branchShort), shellQuote(branchShort))
		script.WriteString("  else\n")
		fmt.Fprintf(&script, "    git fetch --no-tags origin %s && git checkout -B %s FETCH_HEAD\n", shellQuote(baseShort), shellQuote(branchShort))
		script.WriteString("  fi\n")
		script.WriteString("fi\n")
	}

	// Announce the container IP:port so the Brain can connect without a custom image.
	fmt.Fprintf(&script, "POD_IP=$(hostname -i 2>/dev/null | awk '{print $1}' || echo 127.0.0.1)\n")
	fmt.Fprintf(&script, "printf '%shttp://%%s:%d\\n' \"$POD_IP\"\n", sandboxReadyLinePrefix, port)
	fmt.Fprintf(&script, "exec /kodema/bin/kodema sandbox serve --listen 0.0.0.0:%d --workspace %s --token-file %s\n",
		port, defaultSandboxWorkspaceDir, sandboxContainerTokenFilePath)

	var y strings.Builder
	y.WriteString("actions:\n")
	y.WriteString("  - name: sandbox\n")
	fmt.Fprintf(&y, "    image: %q\n", req.Image)
	fmt.Fprintf(&y, "    timeout: %s\n", ttl.Round(time.Second).String())
	if len(req.Caches) > 0 {
		y.WriteString("    caches:\n")
		for _, c := range req.Caches {
			fmt.Fprintf(&y, "      - %q\n", c)
		}
	}
	if req.Memory != "" || req.CPU != "" {
		y.WriteString("    resources:\n")
		if req.Memory != "" {
			fmt.Fprintf(&y, "      memory: %q\n", req.Memory)
		}
		if req.CPU != "" {
			fmt.Fprintf(&y, "      cpu: %q\n", req.CPU)
		}
	}
	y.WriteString("    commands:\n")
	y.WriteString("      - |\n")
	for _, line := range strings.Split(strings.TrimRight(script.String(), "\n"), "\n") {
		y.WriteString("        " + line + "\n")
	}
	return []byte(y.String()), nil
}

// CommitAndPushViaSandboxMCP commits any dirty changes in the sandbox's
// /workspace and pushes HEAD to refs/heads/work/<taskID> over the sandbox's
// authenticated MCP `bash` tool, so the Brain pod never mounts or touches Git.
func CommitAndPushViaSandboxMCP(
	ctx context.Context,
	lease OpenCodeSandboxLease,
	repository string,
	taskID string,
	commitMessage string,
	rt http.RoundTripper,
) (WorktreePushResult, error) {
	if !validRepository(repository) {
		return WorktreePushResult{}, fmt.Errorf("sandbox commit repository %q is invalid", repository)
	}
	if !validSafePathSlug(taskID) {
		return WorktreePushResult{}, fmt.Errorf("sandbox commit task id %q is invalid", taskID)
	}
	cli, err := sandbox.NewClient(lease.EndpointURL, lease.BearerToken, rt)
	if err != nil {
		return WorktreePushResult{}, err
	}
	expectedBranch := "refs/heads/work/" + taskID
	msg := sanitizeCommitMessage(commitMessage, "opencode: apply changes for "+taskID)

	var cmd strings.Builder
	cmd.WriteString("set -eu\n")
	cmd.WriteString("COMMITTED=false\n")
	cmd.WriteString("if [ -n \"$(git status --porcelain)\" ]; then\n")
	cmd.WriteString("  git add -A\n")
	cmd.WriteString("  GIT_AUTHOR_NAME='Kodema OpenCode Service' GIT_AUTHOR_EMAIL='opencode@kodema.invalid' ")
	cmd.WriteString("GIT_COMMITTER_NAME='Kodema OpenCode Service' GIT_COMMITTER_EMAIL='opencode@kodema.invalid' ")
	fmt.Fprintf(&cmd, "git commit --no-verify -m %s >/dev/null\n", shellQuote(msg))
	cmd.WriteString("  COMMITTED=true\n")
	cmd.WriteString("fi\n")
	cmd.WriteString("CANDIDATE=$(git rev-parse HEAD)\n")
	fmt.Fprintf(&cmd, "git push --no-verify origin \"HEAD:%s\" >/dev/null 2>&1\n", expectedBranch)
	cmd.WriteString("printf 'KODEMA_CANDIDATE=%s\\nKODEMA_COMMITTED=%s\\n' \"$CANDIDATE\" \"$COMMITTED\"\n")

	res, err := cli.CallTool(ctx, "bash", map[string]any{
		"command":         cmd.String(),
		"timeout_seconds": sandboxCommitPushCommandTimeout,
	})
	if err != nil {
		return WorktreePushResult{}, fmt.Errorf("sandbox git commit/push RPC: %w", err)
	}
	if res.IsError {
		return WorktreePushResult{}, fmt.Errorf("sandbox git commit/push failed: %s", strings.TrimSpace(res.FirstText()))
	}

	var candidate string
	var committed bool
	for _, line := range strings.Split(res.FirstText(), "\n") {
		line = strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(line, "KODEMA_CANDIDATE="); ok {
			candidate = strings.TrimSpace(after)
		} else if after, ok := strings.CutPrefix(line, "KODEMA_COMMITTED="); ok {
			committed = strings.TrimSpace(after) == "true"
		}
	}
	if candidate == "" {
		return WorktreePushResult{}, fmt.Errorf("sandbox git commit/push did not report candidate SHA: %s", res.FirstText())
	}
	return WorktreePushResult{
		Branch:    expectedBranch,
		Candidate: candidate,
		Committed: committed,
	}, nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
