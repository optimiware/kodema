package opencode

import (
	"context"
	"errors"
	"fmt"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"net/http"
	"strings"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/id"
)

// OpenCodeMode is one declared turn mode carried in an OpenCode service
// request subject.
type OpenCodeMode string

const (
	OpenCodeModePlan      OpenCodeMode = "plan"
	OpenCodeModeImplement OpenCodeMode = "implement"
	OpenCodeModeFix       OpenCodeMode = "fix"
	OpenCodeModeRevise    OpenCodeMode = "revise"
	OpenCodeModeAIReview  OpenCodeMode = "ai_review"
)

var openCodeModes = []OpenCodeMode{
	OpenCodeModePlan,
	OpenCodeModeImplement,
	OpenCodeModeFix,
	OpenCodeModeRevise,
	OpenCodeModeAIReview,
}

// Valid reports whether mode is one of the declared OpenCode modes.
func (mode OpenCodeMode) Valid() bool {
	for _, candidate := range openCodeModes {
		if candidate == mode {
			return true
		}
	}
	return false
}

// MutatesCode reports whether this mode is expected to commit and push code
// changes to the task's work branch.
func (mode OpenCodeMode) MutatesCode() bool {
	switch mode {
	case OpenCodeModeImplement, OpenCodeModeFix, OpenCodeModeRevise:
		return true
	default:
		return false
	}
}

// OpenCodeExecutionMode selects whether the OpenCode service edits code in
// local per-task Git worktrees (Option 1, default) or delegates all shell and
// filesystem tools to ephemeral per-task toolchain sandboxes over MCP (Option 2).
type OpenCodeExecutionMode string

const (
	// OpenCodeExecutionModeWorktree is Option 1: local per-task Git worktrees.
	OpenCodeExecutionModeWorktree OpenCodeExecutionMode = "worktree"
	// OpenCodeExecutionModeSandbox is Option 2: OpenCode service with
	// local shell/fs tools disabled and ephemeral per-task MCP sandboxes.
	OpenCodeExecutionModeSandbox OpenCodeExecutionMode = "sandbox"
)

// Valid reports whether mode is a declared OpenCode execution mode.
func (mode OpenCodeExecutionMode) Valid() bool {
	return mode == OpenCodeExecutionModeWorktree || mode == OpenCodeExecutionModeSandbox
}

// OpenCode subject field names.
const (
	OpenCodeFieldMode          = "mode"
	OpenCodeFieldRepository    = "repository"
	OpenCodeFieldBaseRef       = "base_ref"
	OpenCodeFieldPlanKey       = "plan_key"
	OpenCodeFieldFeedbackKey   = "feedback_key"
	OpenCodeFieldSandboxImage  = "sandbox_image"
	OpenCodeFieldSandboxCaches = "sandbox_caches"
	OpenCodeFieldSandboxMemory = "sandbox_memory"
	OpenCodeFieldSandboxCPU    = "sandbox_cpu"
)

// OpenCode declared response output names.
const (
	OpenCodeOutputBranch     = "branch"
	OpenCodeOutputCandidate  = "candidate"
	OpenCodeOutputSummaryKey = "summary_key"
	OpenCodeOutputPlanKey    = "plan_key"
	OpenCodeOutputReviewKey  = "review_key"
	OpenCodeOutputSessionID  = "session_id"
)

// DefaultOpenCodeBaseRef is used when a coding turn omits base_ref.
const DefaultOpenCodeBaseRef = "refs/heads/main"

// Public operation names used by the OpenCode service adapter in addition to
// servicekit.OperationInbox, servicekit.OperationInspect and servicekit.OperationResolve.
const (
	OperationTaskRead          = "task.read"
	OperationKVGet             = "kv.get"
	OperationKVPut             = "kv.put"
	OperationEventPublish      = "event.publish"
	OperationPipelineStart     = "pipeline.start"
	OperationExecutionCancel   = "execution.cancel"
	OperationExecutionLogsRead = "execution.logs.read"
	OperationSecretWrite       = "secret.write"
	OperationSecretDelete      = "secret.delete"
)

// ErrOpenCodeBudgetExceeded signals that the LLM proxy rejected a turn call
// with HTTP 429 budget_exceeded.
var ErrOpenCodeBudgetExceeded = errors.New("opencode turn exceeded LLM budget")

// ErrOpenCodeSandboxTerminated signals that an ephemeral task sandbox crashed,
// was killed, or OOM-terminated mid-turn while the OpenCode service stayed healthy.
var ErrOpenCodeSandboxTerminated = errors.New("opencode ephemeral sandbox terminated")

// OpenCodeSubject is the validated subject of an OpenCode service request.
type OpenCodeSubject struct {
	Mode          OpenCodeMode
	Repository    string
	BaseRef       string
	PlanKey       string
	FeedbackKey   string
	SandboxImage  string
	SandboxCaches []string
	SandboxMemory string
	SandboxCPU    string
	Extra         map[string]string
}

// ParseOpenCodeSubject validates and decodes the typed request subject for an
// OpenCode service turn.
func ParseOpenCodeSubject(subject *api.Subject) (OpenCodeSubject, error) {
	if subject == nil {
		return OpenCodeSubject{}, fmt.Errorf("opencode request subject is missing")
	}
	if len(subject.Fields) == 0 {
		return OpenCodeSubject{}, fmt.Errorf("opencode request subject is empty")
	}
	seen := make(map[string]bool, len(subject.Fields))
	var parsed OpenCodeSubject
	extra := make(map[string]string)

	for _, field := range subject.Fields {
		if seen[field.Name] {
			return OpenCodeSubject{}, fmt.Errorf("opencode request subject field %q is repeated", field.Name)
		}
		seen[field.Name] = true
		switch field.Name {
		case OpenCodeFieldMode:
			if field.Kind != "string" || field.Value == "" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject mode must be a non-empty string")
			}
			mode := OpenCodeMode(field.Value)
			if !mode.Valid() {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject mode %q is unsupported", field.Value)
			}
			parsed.Mode = mode
		case OpenCodeFieldRepository:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject repository must be a string")
			}
			if field.Value != "" {
				if !servicekit.ValidRepository(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject repository %q is invalid", field.Value)
				}
				parsed.Repository = field.Value
			}
		case OpenCodeFieldBaseRef:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject base_ref must be a string")
			}
			if field.Value != "" {
				if !validOpenCodeRef(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject base_ref %q is invalid", field.Value)
				}
				parsed.BaseRef = field.Value
			}
		case OpenCodeFieldPlanKey:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject plan_key must be a string")
			}
			if field.Value != "" {
				if !api.ValidKVKey(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject plan_key %q is not a valid KV key", field.Value)
				}
				parsed.PlanKey = field.Value
			}
		case OpenCodeFieldFeedbackKey:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject feedback_key must be a string")
			}
			if field.Value != "" {
				if !api.ValidKVKey(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject feedback_key %q is not a valid KV key", field.Value)
				}
				parsed.FeedbackKey = field.Value
			}
		case OpenCodeFieldSandboxImage:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_image must be a string")
			}
			if field.Value != "" {
				if !validSandboxImage(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_image %q is invalid", field.Value)
				}
				parsed.SandboxImage = field.Value
			}
		case OpenCodeFieldSandboxCaches:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_caches must be a string")
			}
			if field.Value != "" {
				caches, err := parseSandboxCachesCSV(field.Value)
				if err != nil {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_caches: %w", err)
				}
				parsed.SandboxCaches = caches
			}
		case OpenCodeFieldSandboxMemory:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_memory must be a string")
			}
			if field.Value != "" {
				if !validSandboxResourceQty(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_memory %q is invalid", field.Value)
				}
				parsed.SandboxMemory = field.Value
			}
		case OpenCodeFieldSandboxCPU:
			if field.Kind != "string" {
				return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_cpu must be a string")
			}
			if field.Value != "" {
				if !validSandboxResourceQty(field.Value) {
					return OpenCodeSubject{}, fmt.Errorf("opencode request subject sandbox_cpu %q is invalid", field.Value)
				}
				parsed.SandboxCPU = field.Value
			}
		default:
			if field.Kind != "null" && field.Value != "" {
				extra[field.Name] = field.Value
			}
		}
	}

	if parsed.Mode == "" {
		return OpenCodeSubject{}, fmt.Errorf("opencode request subject is missing required field %q", OpenCodeFieldMode)
	}
	if parsed.Mode.MutatesCode() && parsed.Repository == "" {
		return OpenCodeSubject{}, fmt.Errorf("opencode mode %q requires a repository", parsed.Mode)
	}
	if parsed.BaseRef == "" && parsed.Repository != "" {
		parsed.BaseRef = DefaultOpenCodeBaseRef
	}
	if len(extra) > 0 {
		parsed.Extra = extra
	}
	return parsed, nil
}

// validOpenCodeRef accepts concrete Git refs, branch names, or commit SHAs
// while refusing flag injection, path traversal, globbing, and control chars.
func validOpenCodeRef(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	if strings.HasPrefix(value, "-") || strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.HasSuffix(value, ".") {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] <= 0x20 || value[i] == 0x7f {
			return false
		}
	}
	if strings.Contains(value, "..") || strings.Contains(value, "@{") || strings.ContainsAny(value, " \t\r\n\x00*?[]\\~^:=") {
		return false
	}
	for _, seg := range strings.Split(value, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.HasPrefix(seg, ".") || strings.HasPrefix(seg, "-") || strings.HasSuffix(seg, ".lock") {
			return false
		}
	}
	return true
}

// validSafePathSlug enforces a traversal-free, flag-safe single path segment
// used for worktree directories and branch suffixes.
func validSafePathSlug(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	if strings.HasPrefix(value, "-") || strings.HasPrefix(value, ".") {
		return false
	}
	for i := 0; i < len(value); i++ {
		ch := value[i]
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		case ch == '-', ch == '_', ch == '.':
		default:
			return false
		}
	}
	return !strings.Contains(value, "..")
}

// OpenCodeSession represents one durable OpenCode server session.
type OpenCodeSession struct {
	ID string `json:"id"`
}

// OpenCodeMCPServerConfig describes one remote MCP server attached dynamically
// to an OpenCode session in sandbox mode (for example an ephemeral `kodema sandbox serve`
// container endpoint with its per-sandbox bearer token).
type OpenCodeMCPServerConfig struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	URL       string            `json:"url"`
	Headers   map[string]string `json:"headers,omitempty"`
	Enabled   bool              `json:"enabled"`
	Directory string            `json:"directory,omitempty"`
}

// OpenCodeMessageRequest is one turn sent to an existing OpenCode session.
type OpenCodeMessageRequest struct {
	Directory       string
	Prompt          string
	Mode            OpenCodeMode
	Model           string
	Agent           string
	ExecutionID     string
	ActionID        string
	TaskID          string
	DisabledTools   []string
	MCPServers      map[string]OpenCodeMCPServerConfig
	ProxyPathPrefix string
}

// OpenCodeMessageResponse is the assistant output returned by OpenCode for one turn.
type OpenCodeMessageResponse struct {
	Text      string
	MessageID string
}

// OpenCodeClient is the consumer-owned HTTP boundary to `opencode serve`.
type OpenCodeClient interface {
	Health(ctx context.Context) error
	CreateSession(ctx context.Context, directory string, title string) (OpenCodeSession, error)
	SendMessage(ctx context.Context, sessionID string, req OpenCodeMessageRequest) (OpenCodeMessageResponse, error)
	AbortSession(ctx context.Context, sessionID string) error
}

// OpenCodeMCPClient is the optional sandbox-mode extension of OpenCodeClient for
// dynamically attaching and detaching remote sandbox MCP servers per session.
type OpenCodeMCPClient interface {
	AttachMCPServer(ctx context.Context, sessionID string, cfg OpenCodeMCPServerConfig) error
	DetachMCPServer(ctx context.Context, sessionID string, serverName string) error
}

// OpenCodeTaskState persists the durable 1:1 mapping from a Kodema task ID to
// its long-lived OpenCode session and local worktree metadata.
type OpenCodeTaskState struct {
	TenantID          id.TenantID  `json:"tenant_id"`
	ProjectID         id.ProjectID `json:"project_id"`
	TaskID            string       `json:"task_id"`
	OpenCodeSessionID string       `json:"opencode_session_id"`
	Repository        string       `json:"repository,omitempty"`
	WorktreePath      string       `json:"worktree_path,omitempty"`
	Branch            string       `json:"branch,omitempty"`
	UpdatedAt         time.Time    `json:"updated_at"`
}

// Validate checks scope, task identity, and session identity.
func (state OpenCodeTaskState) Validate() error {
	if _, err := id.ParseTenantID(string(state.TenantID)); err != nil {
		return fmt.Errorf("opencode task state tenant %q is invalid: %w", state.TenantID, err)
	}
	if _, err := id.ParseProjectID(string(state.ProjectID)); err != nil {
		return fmt.Errorf("opencode task state project %q is invalid: %w", state.ProjectID, err)
	}
	if !validSafePathSlug(state.TaskID) {
		return fmt.Errorf("opencode task state task_id %q is invalid", state.TaskID)
	}
	if strings.TrimSpace(state.OpenCodeSessionID) == "" {
		return fmt.Errorf("opencode task state opencode_session_id is empty")
	}
	if state.UpdatedAt.IsZero() {
		return fmt.Errorf("opencode task state updated_at is zero")
	}
	return nil
}

// OpenCodeActionRecord checkpoint stages persisted atomically on disk so
// restarts resume cleanly without double-pushing or re-running the LLM.
const (
	OpenCodeActionStagePushed       = "pushed"
	OpenCodeActionStageOutputsReady = "outputs_ready"
	OpenCodeActionStageResolved     = "resolved"
)

// OpenCodeActionRecord persists per-action completion and idempotency facts so
// restarts never double-push or re-run already-settled requests.
type OpenCodeActionRecord struct {
	TenantID     id.TenantID       `json:"tenant_id"`
	ProjectID    id.ProjectID      `json:"project_id"`
	ExecutionID  string            `json:"execution_id"`
	ActionID     string            `json:"action_id"`
	TaskID       string            `json:"task_id,omitempty"`
	SessionID    string            `json:"session_id,omitempty"`
	Stage        string            `json:"stage,omitempty"`
	ResponseText string            `json:"response_text,omitempty"`
	Branch       string            `json:"branch,omitempty"`
	Candidate    string            `json:"candidate,omitempty"`
	SummaryKey   string            `json:"summary_key,omitempty"`
	PlanKey      string            `json:"plan_key,omitempty"`
	Outputs      map[string]string `json:"outputs,omitempty"`
	Pushed       bool              `json:"pushed,omitempty"`
	Resolved     bool              `json:"resolved,omitempty"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// Validate checks scope, execution/action identities, and checkpoint stage.
func (record OpenCodeActionRecord) Validate() error {
	if _, err := id.ParseTenantID(string(record.TenantID)); err != nil {
		return fmt.Errorf("opencode action record tenant %q is invalid: %w", record.TenantID, err)
	}
	if _, err := id.ParseProjectID(string(record.ProjectID)); err != nil {
		return fmt.Errorf("opencode action record project %q is invalid: %w", record.ProjectID, err)
	}
	if _, err := id.ParseExecutionID(record.ExecutionID); err != nil {
		return fmt.Errorf("opencode action record execution %q is invalid: %w", record.ExecutionID, err)
	}
	if _, err := id.ParseActionID(record.ActionID); err != nil {
		return fmt.Errorf("opencode action record action %q is invalid: %w", record.ActionID, err)
	}
	switch record.Stage {
	case "", OpenCodeActionStagePushed, OpenCodeActionStageOutputsReady, OpenCodeActionStageResolved:
	default:
		return fmt.Errorf("opencode action record stage %q is unknown", record.Stage)
	}
	if record.UpdatedAt.IsZero() {
		return fmt.Errorf("opencode action record updated_at is zero")
	}
	return nil
}

// OpenCodeStateStore is the durable atomic state boundary for task->session
// mappings and per-action idempotency records.
type OpenCodeStateStore interface {
	LoadTask(ctx context.Context, tenant id.TenantID, project id.ProjectID, taskID string) (OpenCodeTaskState, bool, error)
	SaveTask(ctx context.Context, state OpenCodeTaskState) error
	LoadAction(ctx context.Context, tenant id.TenantID, project id.ProjectID, executionID, actionID string) (OpenCodeActionRecord, bool, error)
	SaveAction(ctx context.Context, record OpenCodeActionRecord) error
}

// WorktreeCheckout describes one prepared isolated per-task Git worktree.
type WorktreeCheckout struct {
	Repository   string
	TaskID       string
	Branch       string // refs/heads/work/<task-id>
	WorktreePath string
	BaseCommit   string
}

// WorktreePushResult reports the committed & pushed candidate revision.
type WorktreePushResult struct {
	Branch    string // refs/heads/work/<task-id>
	Candidate string // 40-char commit SHA
	Committed bool
}

// OpenCodeWorktreeManager manages project-scoped local Git mirrors and isolated
// per-task worktrees checked out on refs/heads/work/<task-id>.
type OpenCodeWorktreeManager interface {
	Prepare(ctx context.Context, repository, taskID, baseRef string) (WorktreeCheckout, error)
	CommitAndPush(ctx context.Context, checkout WorktreeCheckout, commitMessage string) (WorktreePushResult, error)
	Remove(ctx context.Context, repository, taskID string) error
}

// OpenCodeSandboxConfig configures Option 2 sandbox-mode tool policies and default
// ephemeral toolchain sandbox parameters.
type OpenCodeSandboxConfig struct {
	// DisabledLocalTools lists OpenCode built-in local tools disabled in sandbox
	// mode so the OpenCode service pod never executes local shell commands or file edits.
	// Defaults to DefaultSandboxDisabledLocalTools.
	DisabledLocalTools []string
	// DefaultSandboxImage is used when a coding turn omits subject.sandbox_image.
	DefaultSandboxImage string
	// DefaultSandboxCaches lists optional named Kodema caches mounted into
	// ephemeral sandboxes when a turn omits subject.sandbox_caches.
	DefaultSandboxCaches []string
	// SandboxPort is the container TCP port served by `kodema sandbox serve`
	// (default 8090).
	SandboxPort int
	// DefaultSandboxMemory is the optional default container memory limit.
	DefaultSandboxMemory string
	// DefaultSandboxCPU is the optional default container CPU limit.
	DefaultSandboxCPU string
}

// OpenCodeSandboxRequest describes one ephemeral per-task toolchain sandbox
// requested by the OpenCode service.
type OpenCodeSandboxRequest struct {
	TenantID      id.TenantID
	ProjectID     id.ProjectID
	TaskID        string
	ExecutionID   string
	ActionID      string
	Image         string
	Repository    string
	BaseRef       string
	WorkBranch    string // refs/heads/work/<task-id>
	Caches        []string
	Memory        string
	CPU           string
	Port          int
	Timeout       time.Duration
	AllowLoopback bool
}

// OpenCodeSandboxLease represents one live ephemeral sandbox attached to a turn.
type OpenCodeSandboxLease struct {
	SandboxID    string
	ExecutionID  string
	EndpointURL  string // e.g. http://10.0.0.12:8090/mcp
	HealthURL    string // e.g. http://10.0.0.12:8090/healthz
	BearerToken  string
	WorkspaceDir string // e.g. /workspace
	// Done, when non-nil, is closed if the sandbox dies unexpectedly mid-turn
	// (e.g. OOMKilled or container crash). Err reports the failure reason.
	Done <-chan struct{}
	Err  func() error
}

// OpenCodeSandboxManager is the consumer-owned lifecycle seam for ephemeral
// per-task toolchain sandboxes in sandbox mode (Option 2).
type OpenCodeSandboxManager interface {
	Acquire(ctx context.Context, req OpenCodeSandboxRequest) (OpenCodeSandboxLease, error)
	Release(ctx context.Context, lease OpenCodeSandboxLease) error
}

// OpenCodeSandboxCommitter is an optional extension of OpenCodeSandboxManager
// that commits and pushes work branch changes from inside the sandbox. When a
// manager does not implement it, the OpenCode adapter commits and pushes over the
// sandbox's authenticated MCP `bash` tool.
type OpenCodeSandboxCommitter interface {
	CommitAndPush(ctx context.Context, lease OpenCodeSandboxLease, repository, taskID, commitMessage string) (WorktreePushResult, error)
}

// OpenCodeAdapterConfig is the complete configuration for one project-scoped
// OpenCode service adapter.
type OpenCodeAdapterConfig struct {
	ServiceID           id.PrincipalID
	TenantID            id.TenantID
	ProjectID           id.ProjectID
	ExecutionMode       OpenCodeExecutionMode
	Sandbox             OpenCodeSandboxConfig
	Handler             api.Handler
	Stream              client.EventStreamer
	Events              servicekit.EventSource
	OpenCode            OpenCodeClient
	Store               OpenCodeStateStore
	Worktrees           OpenCodeWorktreeManager
	Sandboxes           OpenCodeSandboxManager
	Proxy               *OpenCodeLLMProxy
	PerTurnProxyRouting bool
	SandboxRoundTripper http.RoundTripper
	Clock               servicekit.Clock
	RescanInterval      time.Duration
	MaxConcurrentTurns  int
	Model               string
	Agent               string
}
