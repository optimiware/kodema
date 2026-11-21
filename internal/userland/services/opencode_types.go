package services

import (
	"context"
	"errors"
	"fmt"
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

// OpenCode subject field names.
const (
	OpenCodeFieldMode        = "mode"
	OpenCodeFieldRepository  = "repository"
	OpenCodeFieldBaseRef     = "base_ref"
	OpenCodeFieldPlanKey     = "plan_key"
	OpenCodeFieldFeedbackKey = "feedback_key"
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
// OperationInbox, OperationInspect and OperationResolve.
const (
	OperationTaskRead     = "task.read"
	OperationKVGet        = "kv.get"
	OperationKVPut        = "kv.put"
	OperationEventPublish = "event.publish"
)

// ErrOpenCodeBudgetExceeded signals that the LLM proxy rejected a turn call
// with HTTP 429 budget_exceeded.
var ErrOpenCodeBudgetExceeded = errors.New("opencode turn exceeded LLM budget")

// OpenCodeSubject is the validated subject of an OpenCode service request.
type OpenCodeSubject struct {
	Mode        OpenCodeMode
	Repository  string
	BaseRef     string
	PlanKey     string
	FeedbackKey string
	Extra       map[string]string
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
				if !validRepository(field.Value) {
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
	if strings.Contains(value, "..") || strings.Contains(value, "@{") || strings.ContainsAny(value, " \t\r\n\x00*?[]\\~^:") {
		return false
	}
	for _, seg := range strings.Split(value, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.HasPrefix(seg, ".") || strings.HasSuffix(seg, ".lock") {
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

// OpenCodeMessageRequest is one turn sent to an existing OpenCode session.
type OpenCodeMessageRequest struct {
	Directory   string
	Prompt      string
	Mode        OpenCodeMode
	Model       string
	Agent       string
	ExecutionID string
	ActionID    string
	TaskID      string
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

// OpenCodeActionRecord persists per-action completion and idempotency facts so
// restarts never double-push or re-run already-settled requests.
type OpenCodeActionRecord struct {
	TenantID    id.TenantID       `json:"tenant_id"`
	ProjectID   id.ProjectID      `json:"project_id"`
	ExecutionID string            `json:"execution_id"`
	ActionID    string            `json:"action_id"`
	TaskID      string            `json:"task_id,omitempty"`
	SessionID   string            `json:"session_id,omitempty"`
	Branch      string            `json:"branch,omitempty"`
	Candidate   string            `json:"candidate,omitempty"`
	SummaryKey  string            `json:"summary_key,omitempty"`
	PlanKey     string            `json:"plan_key,omitempty"`
	Outputs     map[string]string `json:"outputs,omitempty"`
	Pushed      bool              `json:"pushed,omitempty"`
	Resolved    bool              `json:"resolved,omitempty"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// Validate checks scope and execution/action identities.
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

// OpenCodeAdapterConfig is the complete configuration for one project-scoped
// OpenCode service adapter.
type OpenCodeAdapterConfig struct {
	ServiceID          id.PrincipalID
	TenantID           id.TenantID
	ProjectID          id.ProjectID
	Handler            api.Handler
	Stream             client.EventStreamer
	Events             EventSource
	OpenCode           OpenCodeClient
	Store              OpenCodeStateStore
	Worktrees          OpenCodeWorktreeManager
	Proxy              *OpenCodeLLMProxy
	Clock              Clock
	RescanInterval     time.Duration
	MaxConcurrentTurns int
	Model              string
	Agent              string
}
