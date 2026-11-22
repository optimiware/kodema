package opencode

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/kit/sandbox"
	"github.com/optimiware/kodema/internal/id"
)

const (
	defaultSandboxReadyTimeout      = 2 * time.Minute
	defaultSandboxHealthInterval    = 2 * time.Second
	defaultSandboxStopTimeout       = 30 * time.Second
	defaultSandboxTTL               = 30 * time.Minute
	minSandboxActionTimeout         = time.Minute
	maxSandboxActionTimeout         = 30 * 24 * time.Hour
	sandboxReadyLinePrefix          = "KODEMA_SANDBOX_ENDPOINT="
	maxConsecutiveHealthzFailures   = 2
	defaultSandboxWorkspaceDir      = "/workspace"
	sandboxSecretMountRoot          = "/kodema/secrets"
	sandboxSecretNamePrefix         = "KODEMA_SBOX_TOK_"
	sandboxActionName               = "sandbox"
	sandboxCommitPushCommandTimeout = 120
	maxSandboxLogPagesPerPoll       = 64
)

// errInvalidSandboxEndpoint indicates the sandbox announced an endpoint URL
// that fails SSRF/host/port/scheme validation and must not be retried.
var errInvalidSandboxEndpoint = errors.New("sandbox announced invalid endpoint URL")

// KodemaExecutionSandboxConfig configures KodemaExecutionSandboxManager
// (Approach 2A: ephemeral per-task sandboxes scheduled as Kodema inline
// container executions through the public API — no second container orchestrator).
type KodemaExecutionSandboxConfig struct {
	TenantID               id.TenantID
	ProjectID              id.ProjectID
	Handler                api.Handler
	Clock                  servicekit.Clock
	RoundTripper           http.RoundTripper
	EndpointResolver       func(ctx context.Context, executionID string, port int) (string, error)
	EndpointTemplate       string
	AllowLoopbackEndpoints bool
	ReadyTimeout           time.Duration
	HealthInterval         time.Duration
	StopTimeout            time.Duration
	SandboxTTL             time.Duration
}

type managedSandboxMonitor struct {
	stopOnce sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	doneCh   chan struct{}
	exited   chan struct{}
	mu       sync.Mutex
	err      error
}

func newManagedSandboxMonitor() *managedSandboxMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &managedSandboxMonitor{
		ctx:    ctx,
		cancel: cancel,
		doneCh: make(chan struct{}),
		exited: make(chan struct{}),
	}
}

type sandboxSecretRef struct {
	name    string
	version uint64
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
		m.cancel()
	})
}

// KodemaExecutionSandboxManager implements OpenCodeSandboxManager using Kodema's
// native public execution API (`secret.write`, `pipeline.start` inline YAML,
// `execution.inspect`, `execution.logs.read`, `execution.cancel`, and
// `secret.delete`). Kodema's existing Kernel and Podman/Kubernetes runtime own
// scheduling, cgroups, OOM diagnostics, NetworkPolicy, secret mounting/redaction,
// and hard SIGKILL cleanup.
type KodemaExecutionSandboxManager struct {
	project                id.ProjectID
	handler                api.Handler
	httpClient             *http.Client
	endpointResolver       func(ctx context.Context, executionID string, port int) (string, error)
	endpointTemplate       string
	allowLoopbackEndpoints bool
	readyTimeout           time.Duration
	healthInterval         time.Duration
	stopTimeout            time.Duration
	sandboxTTL             time.Duration

	mu       sync.Mutex
	monitors map[string]*managedSandboxMonitor // sandboxID -> monitor
	secrets  map[string]sandboxSecretRef       // sandboxID -> ephemeral project secret
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
		project: cfg.ProjectID,
		handler: cfg.Handler,
		httpClient: &http.Client{
			Transport: rt,
			Timeout:   5 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		endpointResolver:       cfg.EndpointResolver,
		endpointTemplate:       strings.TrimSpace(cfg.EndpointTemplate),
		allowLoopbackEndpoints: cfg.AllowLoopbackEndpoints,
		readyTimeout:           readyTimeout,
		healthInterval:         healthInterval,
		stopTimeout:            stopTimeout,
		sandboxTTL:             sandboxTTL,
		monitors:               make(map[string]*managedSandboxMonitor),
		secrets:                make(map[string]sandboxSecretRef),
	}, nil
}

// Acquire launches an ephemeral Kodema inline container execution running
// `/kodema/bin/kodema sandbox serve`, delivers the per-sandbox bearer token via
// an ephemeral project secret (never persisted in pipeline YAML or /kodema/script),
// waits until `/healthz` reports 200 OK, and starts a health/lifecycle watcher
// so mid-turn crashes or OOM kills are detected promptly.
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
	if m.allowLoopbackEndpoints {
		req.AllowLoopback = true
	}

	token, err := sandbox.GenerateBearerToken()
	if err != nil {
		return OpenCodeSandboxLease{}, err
	}
	secretNonce, err := randomHexBytes(8)
	if err != nil {
		return OpenCodeSandboxLease{}, fmt.Errorf("generate sandbox secret name nonce: %w", err)
	}
	tokenSecretName := sandboxSecretNamePrefix + strings.ToUpper(secretNonce)

	yamlBytes, err := BuildSandboxPipelineYAML(req, tokenSecretName)
	if err != nil {
		return OpenCodeSandboxLease{}, err
	}

	attemptNonce, err := randomHexBytes(16)
	if err != nil {
		return OpenCodeSandboxLease{}, fmt.Errorf("generate sandbox attempt nonce: %w", err)
	}

	// Store the per-sandbox bearer token in an ephemeral Kodema project secret so
	// it is mounted out-of-band at /kodema/secrets/<name>, automatically redacted
	// from logs, and never persisted into pipeline YAML or /kodema/script.
	secretWriteIdem := deriveIdemKey("oc-sbox-secw:", fmt.Sprintf("%s:%s:%s:%s", req.ExecutionID, req.ActionID, tokenSecretName, attemptNonce))
	secDecoded, err := servicekit.InvokePublicOperation(ctx, m.handler, OperationSecretWrite, api.SecretWriteRequest{
		ProjectID: string(m.project),
		SecretID:  tokenSecretName,
		Value:     token,
	}, nil, secretWriteIdem)
	if err != nil {
		return OpenCodeSandboxLease{}, fmt.Errorf("write ephemeral sandbox token secret: %w", err)
	}
	secWritten, ok := secDecoded.(api.SecretWriteResult)
	if !ok {
		return OpenCodeSandboxLease{}, fmt.Errorf("secret.write returned unexpected %T", secDecoded)
	}
	secretRef := sandboxSecretRef{name: tokenSecretName, version: secWritten.Version}
	cleanupSecret := func() {
		_ = m.deleteSandboxSecret(context.WithoutCancel(ctx), secretRef)
	}

	idemScope := fmt.Sprintf("opencode-sandbox:%s:%s:%s:%s", req.ExecutionID, req.ActionID, req.TaskID, attemptNonce)
	idemKey := deriveIdemKey("oc-sbox:", idemScope)

	decoded, err := servicekit.InvokePublicOperation(ctx, m.handler, OperationPipelineStart, api.PipelineStartRequest{
		ProjectID: string(m.project),
		YAML:      yamlBytes,
	}, nil, idemKey)
	if err != nil {
		cleanupSecret()
		return OpenCodeSandboxLease{}, fmt.Errorf("start ephemeral sandbox execution: %w", err)
	}
	started, ok := decoded.(api.PipelineStartResult)
	if !ok {
		cleanupSecret()
		return OpenCodeSandboxLease{}, fmt.Errorf("pipeline.start returned unexpected %T", decoded)
	}
	execID := started.ExecutionID

	// Wait for endpoint discovery + /healthz readiness while watching execution state.
	readyCtx, cancelReady := context.WithTimeout(ctx, m.readyTimeout)
	defer cancelReady()

	baseURL, err := m.waitForSandboxReady(readyCtx, execID, port)
	if err != nil {
		// Clean up the sandbox execution and ephemeral secret on startup failure or cancellation.
		_ = m.cancelAndConfirmStop(context.WithoutCancel(ctx), execID)
		cleanupSecret()
		return OpenCodeSandboxLease{}, err
	}

	cleanBase := strings.TrimRight(baseURL, "/")
	mon := newManagedSandboxMonitor()
	sandboxID := "sbox-" + execID
	m.mu.Lock()
	m.monitors[sandboxID] = mon
	m.secrets[sandboxID] = secretRef
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

// Release cancels the ephemeral sandbox execution via Kodema's public API,
// waits for terminal confirmation (confirmed stop), and deletes the ephemeral
// per-sandbox bearer token secret.
func (m *KodemaExecutionSandboxManager) Release(ctx context.Context, lease OpenCodeSandboxLease) error {
	var secretRef sandboxSecretRef
	if lease.SandboxID != "" {
		m.mu.Lock()
		mon := m.monitors[lease.SandboxID]
		delete(m.monitors, lease.SandboxID)
		secretRef = m.secrets[lease.SandboxID]
		delete(m.secrets, lease.SandboxID)
		m.mu.Unlock()
		if mon != nil {
			mon.stop()
			<-mon.exited
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cleanupCtx := context.WithoutCancel(ctx)
	var stopErr error
	if strings.TrimSpace(lease.ExecutionID) != "" {
		stopErr = m.cancelAndConfirmStop(cleanupCtx, lease.ExecutionID)
	}
	var secretErr error
	if secretRef.name != "" {
		secretErr = m.deleteSandboxSecret(cleanupCtx, secretRef)
	}
	if stopErr != nil {
		return stopErr
	}
	return secretErr
}

func (m *KodemaExecutionSandboxManager) deleteSandboxSecret(ctx context.Context, ref sandboxSecretRef) error {
	if ref.name == "" {
		return nil
	}
	delCtx, cancel := context.WithTimeout(ctx, m.stopTimeout)
	defer cancel()
	nonce, _ := randomHexBytes(8)
	idemKey := deriveIdemKey("oc-sbox-secd:", fmt.Sprintf("%s:v%d:%s", ref.name, ref.version, nonce))
	version := ref.version
	_, err := servicekit.InvokePublicOperation(delCtx, m.handler, OperationSecretDelete, api.SecretDeleteRequest{
		ProjectID: string(m.project),
		SecretID:  ref.name,
	}, &version, idemKey)
	if err != nil {
		var pubErr *api.Error
		if errors.As(err, &pubErr) && pubErr.Code == api.CodeNotFound {
			return nil
		}
		return fmt.Errorf("delete ephemeral sandbox secret %s: %w", ref.name, err)
	}
	return nil
}

func (m *KodemaExecutionSandboxManager) watchSandbox(lease OpenCodeSandboxLease, mon *managedSandboxMonitor) {
	defer close(mon.exited)
	ticker := time.NewTicker(m.healthInterval)
	defer ticker.Stop()
	consecutiveHealthFailures := 0

	for {
		select {
		case <-mon.ctx.Done():
			return
		case <-ticker.C:
		}

		checkCtx, cancel := context.WithTimeout(mon.ctx, 5*time.Second)
		inspected, inspectErr := servicekit.InspectExecution(checkCtx, m.handler, lease.ExecutionID)
		if mon.ctx.Err() != nil {
			cancel()
			return
		}
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
		if mon.ctx.Err() != nil {
			return
		}
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

	var (
		resolvedBase string
		actionID     string
		logCursor    string
		lineCarry    string
	)
	for {
		inspected, err := servicekit.InspectExecution(ctx, m.handler, executionID)
		if err == nil {
			if isTerminalExecutionState(inspected.State) {
				return "", fmt.Errorf("%w: sandbox execution %s terminated before becoming ready (state=%s%s)",
					ErrOpenCodeSandboxTerminated, executionID, inspected.State, sandboxFailureDetail(inspected))
			}
			if actionID == "" {
				actionID = findSandboxActionID(inspected.Actions)
			}
		}

		if resolvedBase == "" {
			base, resolveErr := m.resolveEndpoint(ctx, executionID, actionID, port, &logCursor, &lineCarry)
			if resolveErr != nil {
				if errors.Is(resolveErr, errInvalidSandboxEndpoint) {
					return "", resolveErr
				}
			} else if base != "" {
				resolvedBase = base
			}
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

func findSandboxActionID(actions []api.ExecutionAction) string {
	for _, a := range actions {
		if a.Name == sandboxActionName && strings.TrimSpace(a.ActionID) != "" {
			return strings.TrimSpace(a.ActionID)
		}
	}
	return ""
}

func (m *KodemaExecutionSandboxManager) resolveEndpoint(
	ctx context.Context,
	executionID string,
	actionID string,
	port int,
	cursor *string,
	lineCarry *string,
) (string, error) {
	if m.endpointResolver != nil {
		candidate, err := m.endpointResolver(ctx, executionID, port)
		if err != nil {
			return "", err
		}
		validated, valErr := validateSandboxEndpointURL(candidate, port, m.allowLoopbackEndpoints)
		if valErr != nil {
			return "", fmt.Errorf("%w: %v", errInvalidSandboxEndpoint, valErr)
		}
		return validated, nil
	}
	if m.endpointTemplate != "" {
		candidate := fmt.Sprintf(m.endpointTemplate, executionID, port)
		validated, valErr := validateSandboxEndpointURL(candidate, port, m.allowLoopbackEndpoints)
		if valErr != nil {
			return "", fmt.Errorf("%w: %v", errInvalidSandboxEndpoint, valErr)
		}
		return validated, nil
	}
	if strings.TrimSpace(actionID) == "" {
		return "", errors.New("sandbox action ID not available yet")
	}

	// Paginate through execution.logs.read using NextCursor so verbose git clone
	// output never hides KODEMA_SANDBOX_ENDPOINT= on later pages.
	for page := 0; page < maxSandboxLogPagesPerPoll; page++ {
		logReq := api.ExecutionLogsReadRequest{
			ExecutionID: executionID,
			ActionID:    actionID,
			Stream:      "stdout",
		}
		if cursor != nil && *cursor != "" {
			logReq.Page = &api.Page{Cursor: *cursor}
		}
		decoded, err := servicekit.InvokePublicOperation(ctx, m.handler, OperationExecutionLogsRead, logReq, nil, "")
		if err != nil {
			return "", err
		}
		logs, ok := decoded.(api.ExecutionLogsReadResult)
		if !ok {
			return "", fmt.Errorf("execution.logs.read returned unexpected %T", decoded)
		}
		carry := ""
		if lineCarry != nil {
			carry = *lineCarry
		}
		for _, chunk := range logs.Chunks {
			data := string(chunk.Data)
			if carry != "" {
				data = carry + data
				carry = ""
			}
			lines := strings.Split(data, "\n")
			completeCount := len(lines)
			if !strings.HasSuffix(data, "\n") {
				completeCount = len(lines) - 1
				carry = lines[len(lines)-1]
			}
			for i := 0; i < completeCount; i++ {
				line := strings.TrimSpace(strings.TrimSuffix(lines[i], "\r"))
				if after, ok := strings.CutPrefix(line, sandboxReadyLinePrefix); ok {
					validated, valErr := validateSandboxEndpointURL(strings.TrimSpace(after), port, m.allowLoopbackEndpoints)
					if valErr != nil {
						return "", fmt.Errorf("%w: %v", errInvalidSandboxEndpoint, valErr)
					}
					return validated, nil
				}
			}
		}
		if logs.NextCursor == "" || (cursor != nil && logs.NextCursor == *cursor) {
			break
		}
		if cursor != nil {
			*cursor = logs.NextCursor
		}
		if lineCarry != nil {
			*lineCarry = carry
		}
	}
	return "", errors.New("sandbox endpoint not announced yet")
}

// validateSandboxEndpointURL enforces strict SSRF protection on discovered
// sandbox endpoints: http scheme only, no userinfo/path/query/fragment, exact
// port match, literal IP host only (no DNS hostnames), and rejects unspecified,
// multicast, link-local (including 169.254.169.254), and loopback addresses
// (unless allowLoopback is explicitly enabled for local dev/test).
func validateSandboxEndpointURL(raw string, expectedPort int, allowLoopback bool) (string, error) {
	if raw == "" || containsControlOrSpace(raw) {
		return "", errors.New("endpoint URL is empty or contains whitespace/control characters")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse endpoint URL: %w", err)
	}
	if parsed.Scheme != "http" {
		return "", fmt.Errorf("endpoint scheme %q is disallowed (want http)", parsed.Scheme)
	}
	if parsed.User != nil {
		return "", errors.New("endpoint URL must not contain userinfo")
	}
	if parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", errors.New("endpoint URL must not contain path, query, or fragment")
	}
	portStr := parsed.Port()
	if portStr == "" {
		return "", errors.New("endpoint URL must include an explicit port")
	}
	portNum, err := strconv.Atoi(portStr)
	if err != nil || portNum != expectedPort {
		return "", fmt.Errorf("endpoint port %q does not match expected sandbox port %d", portStr, expectedPort)
	}
	host := parsed.Hostname()
	addr, err := netip.ParseAddr(host)
	if err != nil || addr.Zone() != "" {
		return "", fmt.Errorf("endpoint host %q must be a literal IP address without zone", host)
	}
	addr = addr.Unmap()
	if addr.IsUnspecified() {
		return "", fmt.Errorf("endpoint IP %s is unspecified", addr)
	}
	if addr.IsMulticast() {
		return "", fmt.Errorf("endpoint IP %s is multicast", addr)
	}
	if addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() {
		return "", fmt.Errorf("endpoint IP %s is link-local", addr)
	}
	if addr.IsLoopback() && !allowLoopback {
		return "", fmt.Errorf("endpoint IP %s is loopback (disabled by default)", addr)
	}
	return "http://" + net.JoinHostPort(addr.String(), strconv.Itoa(expectedPort)), nil
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
		inspected, err := servicekit.InspectExecution(stopCtx, m.handler, executionID)
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
		_, cancelErr := servicekit.InvokePublicOperation(stopCtx, m.handler, OperationExecutionCancel, api.ExecutionCancelRequest{
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
		inspected, err := servicekit.InspectExecution(stopCtx, m.handler, executionID)
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
	for _, field := range []struct {
		name  string
		value string
	}{
		{"image", req.Image},
		{"repository", req.Repository},
		{"base_ref", req.BaseRef},
		{"work_branch", req.WorkBranch},
		{"task_id", req.TaskID},
		{"memory", req.Memory},
		{"cpu", req.CPU},
	} {
		if containsControlOrSpace(field.value) {
			return fmt.Errorf("sandbox %s contains disallowed whitespace or control characters", field.name)
		}
	}
	if !validSandboxImage(req.Image) {
		return fmt.Errorf("sandbox image %q is invalid", req.Image)
	}
	if req.Port != 0 && (req.Port < 1024 || req.Port > 65535) {
		return fmt.Errorf("sandbox port %d must be between 1024 and 65535", req.Port)
	}
	if req.TaskID != "" && !validSafePathSlug(req.TaskID) {
		return fmt.Errorf("sandbox task_id %q is invalid", req.TaskID)
	}
	if req.Repository != "" {
		if !servicekit.ValidRepository(req.Repository) {
			return fmt.Errorf("sandbox repository %q is invalid", req.Repository)
		}
		if req.WorkBranch == "" && req.TaskID == "" {
			return errors.New("sandbox repository checkout requires task_id or work_branch")
		}
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
// The ephemeral bearer token is NEVER embedded in YAML or `/kodema/script`;
// instead it is declared via Kodema's native `secrets:` list using
// tokenSecretName so Kodema mounts it out-of-band at
// `/kodema/secrets/<tokenSecretName>` and automatically redacts it from logs.
func BuildSandboxPipelineYAML(req OpenCodeSandboxRequest, tokenSecretName string) ([]byte, error) {
	if err := validateSandboxRequest(req); err != nil {
		return nil, err
	}
	if containsControlOrSpace(tokenSecretName) {
		return nil, errors.New("sandbox token secret name contains disallowed whitespace or control characters")
	}
	parsedSecretName, err := id.ParseSecretName(tokenSecretName)
	if err != nil {
		return nil, fmt.Errorf("sandbox token secret name %q is invalid: %w", tokenSecretName, err)
	}
	port := req.Port
	if port == 0 {
		port = DefaultOpenCodeSandboxPort
	}
	ttl := req.Timeout
	if ttl <= 0 {
		ttl = defaultSandboxTTL
	}
	ttl = ttl.Round(time.Second)
	if ttl < minSandboxActionTimeout {
		ttl = minSandboxActionTimeout
	}
	if ttl > maxSandboxActionTimeout {
		ttl = maxSandboxActionTimeout
	}

	var script strings.Builder
	script.WriteString("set -eu\n")

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
		if strings.HasPrefix(branchShort, "-") || !validOpenCodeRef(branchShort) {
			return nil, fmt.Errorf("sandbox work branch %q is invalid", branchShort)
		}
		if strings.HasPrefix(baseShort, "-") || !validOpenCodeRef(baseShort) {
			return nil, fmt.Errorf("sandbox base ref %q is invalid", baseShort)
		}

		branchFullRef := "refs/heads/" + branchShort
		baseFetchRef := baseRef
		if !strings.HasPrefix(baseFetchRef, "refs/") && !isValidCommitSHA(baseFetchRef) {
			baseFetchRef = "refs/heads/" + baseShort
		}

		script.WriteString("if [ -z \"${KODEMA_GIT_URL:-}\" ]; then\n")
		script.WriteString("  echo 'kodema sandbox: KODEMA_GIT_URL is not set for repository checkout' >&2\n")
		script.WriteString("  exit 1\n")
		script.WriteString("fi\n")
		script.WriteString("if ! command -v git >/dev/null 2>&1; then\n")
		script.WriteString("  echo 'kodema sandbox: git binary is not available in sandbox image' >&2\n")
		script.WriteString("  exit 1\n")
		script.WriteString("fi\n")
		fmt.Fprintf(&script, "REPO_URL=\"${KODEMA_GIT_URL%%/}/%s\"\n", req.Repository)
		script.WriteString("if [ ! -d /workspace/.git ]; then\n")
		script.WriteString("  git clone --no-tags -- \"$REPO_URL\" /workspace\n")
		script.WriteString("fi\n")
		script.WriteString("cd /workspace\n")
		fmt.Fprintf(&script, "if git ls-remote --exit-code --heads -- origin %s >/dev/null 2>&1; then\n", shellQuote(branchFullRef))
		fmt.Fprintf(&script, "  git fetch --no-tags -- origin %s && git checkout -B %s FETCH_HEAD --\n", shellQuote(branchFullRef), shellQuote(branchShort))
		script.WriteString("else\n")
		fmt.Fprintf(&script, "  git fetch --no-tags -- origin %s && git checkout -B %s FETCH_HEAD --\n", shellQuote(baseFetchRef), shellQuote(branchShort))
		script.WriteString("fi\n")
	}

	// Discover a non-loopback pod IP without silently falling back to 127.0.0.1
	// unless loopback endpoints were explicitly enabled for local dev/test.
	script.WriteString("POD_IP=\"\"\n")
	script.WriteString("for ip in $(hostname -i 2>/dev/null || true); do\n")
	script.WriteString("  case \"$ip\" in\n")
	script.WriteString("    127.*|::1|0.0.0.0|::|169.254.*|fe80:*|FE80:*) continue ;;\n")
	script.WriteString("    *[0-9a-fA-F]*) POD_IP=\"$ip\"; break ;;\n")
	script.WriteString("  esac\n")
	script.WriteString("done\n")
	if req.AllowLoopback {
		script.WriteString("if [ -z \"$POD_IP\" ]; then\n")
		script.WriteString("  POD_IP=127.0.0.1\n")
		script.WriteString("fi\n")
	} else {
		script.WriteString("if [ -z \"$POD_IP\" ]; then\n")
		script.WriteString("  echo 'kodema sandbox: failed to determine non-loopback pod IP' >&2\n")
		script.WriteString("  exit 1\n")
		script.WriteString("fi\n")
	}
	script.WriteString("case \"$POD_IP\" in\n")
	fmt.Fprintf(&script, "  *:*) printf '%shttp://[%%s]:%d\\n' \"$POD_IP\" ;;\n", sandboxReadyLinePrefix, port)
	fmt.Fprintf(&script, "  *) printf '%shttp://%%s:%d\\n' \"$POD_IP\" ;;\n", sandboxReadyLinePrefix, port)
	script.WriteString("esac\n")
	tokenMountPath := sandboxSecretMountRoot + "/" + string(parsedSecretName)
	fmt.Fprintf(&script, "exec /kodema/bin/kodema sandbox serve --listen 0.0.0.0:%d --workspace %s --token-file %s --token-env %s\n",
		port, defaultSandboxWorkspaceDir, tokenMountPath, string(parsedSecretName))

	var y strings.Builder
	y.WriteString("actions:\n")
	fmt.Fprintf(&y, "  - name: %s\n", sandboxActionName)
	fmt.Fprintf(&y, "    image: %q\n", req.Image)
	fmt.Fprintf(&y, "    timeout: %s\n", ttl.String())
	y.WriteString("    secrets:\n")
	fmt.Fprintf(&y, "      - %q\n", string(parsedSecretName))
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
	if !servicekit.ValidRepository(repository) {
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
	fmt.Fprintf(&cmd, "git push --no-verify -- origin %s >&2\n", shellQuote("HEAD:"+expectedBranch))
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
	if !isValidCommitSHA(candidate) {
		return WorktreePushResult{}, fmt.Errorf("sandbox git commit/push reported invalid candidate SHA %q: %s", candidate, res.FirstText())
	}
	return WorktreePushResult{
		Branch:    expectedBranch,
		Candidate: candidate,
		Committed: committed,
	}, nil
}

func isValidCommitSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func containsControlOrSpace(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] <= 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

func randomHexBytes(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func deriveIdemKey(prefix, scope string) string {
	sum := sha256.Sum256([]byte(scope))
	return prefix + hex.EncodeToString(sum[:])[:32]
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
