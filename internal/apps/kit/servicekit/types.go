// Package servicekit provides shared service adapter primitives, public
// operation codecs, retained-notification polling, atomic JSON file storage,
// advisory file locking, and native Git command execution helpers used by the
// independent first-party daemon apps (opencode, merge, email, responder, timer,
// webhook).
//
// All adapters call public APIs with stable keys. They never access the
// factory database and never submit commands over NATS.
package servicekit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/id"
)

// ServiceKind is one independent service adapter kind.
type ServiceKind string

const (
	// KindWebhook answers its explicit request assignments. It delivers no
	// outbound webhook and opens no listener.
	KindWebhook ServiceKind = "webhook"
	// KindEmail answers its explicit request assignments and carries the
	// email-to-task intake. It sends no email.
	KindEmail ServiceKind = "email"
	// KindTimer schedules durable timers.
	KindTimer ServiceKind = "timer"
	// KindResponder answers explicit service-ID assignments.
	KindResponder ServiceKind = "responder"
	// KindMergeCoordinator serializes merges per repository/ref.
	KindMergeCoordinator ServiceKind = "merge_coordinator"
	// KindOpenCode runs a per-project OpenCode service with persistent
	// task sessions, isolated local Git worktrees, and attributed LLM calls.
	KindOpenCode ServiceKind = "opencode"
)

// serviceKinds is the declaration order.
var serviceKinds = []ServiceKind{
	KindWebhook,
	KindEmail,
	KindTimer,
	KindResponder,
	KindMergeCoordinator,
	KindOpenCode,
}

// Valid reports whether the kind is one of the declared kinds.
func (kind ServiceKind) Valid() bool {
	for _, candidate := range serviceKinds {
		if candidate == kind {
			return true
		}
	}
	return false
}

// ServiceAdapter is one independent service adapter.
type ServiceAdapter interface {
	Kind() ServiceKind
	Run(ctx context.Context) error
}

// ValidRepository reports whether value is a tenant-relative repository name.
func ValidRepository(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	if strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") {
		return false
	}
	if strings.ContainsAny(value, "*?[]\\ ") || strings.Contains(value, "..") {
		return false
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." || strings.HasPrefix(segment, "-") {
			return false
		}
		for index := 0; index < len(segment); index++ {
			character := segment[index]
			switch {
			case character >= 'a' && character <= 'z':
			case character >= '0' && character <= '9':
			case character == '-', character == '_', character == '.':
			default:
				return false
			}
		}
	}
	return true
}

// Sentinel refusals shared by service adapters. Every implementation returns
// them wrapped so callers can use errors.Is without depending on a message.
var (
	// ErrStaleVersion reports a current request version behind the scanned one.
	ErrStaleVersion = errors.New("assignment version is stale")
	// ErrChangedContent reports a retry whose idempotency content changed.
	ErrChangedContent = errors.New("idempotency content changed")
	// ErrDisabled reports that no service adapter was configured. It is a clean
	// refusal, never a silently idle process.
	ErrDisabled = errors.New("no service adapter is configured")
)

// Clock is the injected time boundary.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production time boundary.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// Assignment is one explicit request assignment to a service or human role.
type Assignment struct {
	TenantID      id.TenantID     `json:"tenant_id"`
	ProjectID     id.ProjectID    `json:"project_id"`
	ExecutionID   id.ExecutionID  `json:"execution_id"`
	ActionID      id.ActionID     `json:"action_id"`
	ServiceID     id.PrincipalID  `json:"service_id,omitempty"`
	Request       string          `json:"request"`
	Version       uint64          `json:"version"`
	SourceEventID id.EventID      `json:"source_event_id,omitempty"`
	At            time.Time       `json:"at"`
	Payload       json.RawMessage `json:"payload,omitempty"`
}

// humanRoleNames is the human-role vocabulary. A human role is never a
// service identity and a service adapter never answers one.
var humanRoleNames = []string{"admin", "engineer", "reviewer", "operator", "viewer"}

// validHumanRoleName reports whether value is a declared human role name.
func validHumanRoleName(value string) bool {
	for _, candidate := range humanRoleNames {
		if candidate == value {
			return true
		}
	}
	return false
}

// Validate checks the identities, assignee grammar, version, time and payload.
func (assignment Assignment) Validate() error {
	if _, err := id.ParseTenantID(string(assignment.TenantID)); err != nil {
		return fmt.Errorf("assignment tenant %q is invalid: %w", assignment.TenantID, err)
	}
	if _, err := id.ParseProjectID(string(assignment.ProjectID)); err != nil {
		return fmt.Errorf("assignment project %q is invalid: %w", assignment.ProjectID, err)
	}
	if _, err := id.ParseExecutionID(string(assignment.ExecutionID)); err != nil {
		return fmt.Errorf("assignment execution %q is invalid: %w", assignment.ExecutionID, err)
	}
	if _, err := id.ParseActionID(string(assignment.ActionID)); err != nil {
		return fmt.Errorf("assignment action %q is invalid: %w", assignment.ActionID, err)
	}
	if err := validateAssignee(assignment); err != nil {
		return err
	}
	if assignment.Version == 0 {
		return fmt.Errorf("assignment version is zero")
	}
	if assignment.At.IsZero() {
		return fmt.Errorf("assignment time is zero")
	}
	if assignment.SourceEventID != "" {
		if _, err := id.ParseEventID(string(assignment.SourceEventID)); err != nil {
			return fmt.Errorf("assignment source event %q is invalid: %w", assignment.SourceEventID, err)
		}
	}
	if len(assignment.Payload) > 0 {
		trimmed := strings.TrimSpace(string(assignment.Payload))
		if trimmed == "" || trimmed[0] != '{' || !json.Valid(assignment.Payload) {
			return fmt.Errorf("assignment payload is not a JSON object")
		}
	}
	return nil
}

// validateAssignee checks exactly `service/<service id>` or `role/<role>`.
func validateAssignee(assignment Assignment) error {
	const servicePrefix = "service/"
	const rolePrefix = "role/"
	switch {
	case strings.HasPrefix(assignment.Request, servicePrefix):
		if assignment.ServiceID == "" {
			return fmt.Errorf("service assignment has no service identity")
		}
		if _, err := id.ParsePrincipalID(string(assignment.ServiceID)); err != nil {
			return fmt.Errorf("assignment service %q is invalid: %w", assignment.ServiceID, err)
		}
		if assignment.Request != servicePrefix+string(assignment.ServiceID) {
			return fmt.Errorf("assignment request %q does not name its service %q", assignment.Request, assignment.ServiceID)
		}
		return nil
	case strings.HasPrefix(assignment.Request, rolePrefix):
		if assignment.ServiceID != "" {
			return fmt.Errorf("human-role assignment carries a service identity")
		}
		role := strings.TrimPrefix(assignment.Request, rolePrefix)
		if !validHumanRoleName(role) {
			return fmt.Errorf("assignment role %q is not a frozen human role", role)
		}
		return nil
	default:
		return fmt.Errorf("assignment request %q is not a service or role assignment", assignment.Request)
	}
}

// Notification is one retained hint that an assignment may exist. Delivery may
// repeat or reorder; a notification grants neither authority nor exclusivity.
type Notification struct {
	EventID   id.EventID     `json:"event_id"`
	Type      string         `json:"type"`
	TenantID  id.TenantID    `json:"tenant_id"`
	ProjectID id.ProjectID   `json:"project_id"`
	ServiceID id.PrincipalID `json:"service_id,omitempty"`
	At        time.Time      `json:"at"`
}

// NotificationPage is one page of retained notifications.
type NotificationPage struct {
	Notifications []Notification `json:"notifications"`
	Next          api.PageInfo   `json:"next"`
}

// EventSource is the retained-notification boundary. Notifications are hints
// only; they never cause a resolve and never own a grant.
type EventSource interface {
	Poll(context.Context, string) (NotificationPage, error)
}

// Public operations and service decisions used by the adapters.
const (
	OperationInbox   = "task.inbox"
	OperationInspect = "execution.inspect"
	OperationResolve = "execution.request.resolve"
	DecisionPass     = "pass"
	DecisionFail     = "fail"
)

// The adapter wire types are aliases of the served public declarations, never
// private lookalikes. A value the servicekit package hands to a public codec or
// handler is therefore the exact type the public edge decodes, so a private
// member such as the legacy tenant_id/limit/cursor inbox shape cannot reappear.
type (
	// InboxInput is the served task.inbox request: an optional project_id, the
	// optional kinds list and the optional page.
	InboxInput = api.TaskInboxRequest
	// InboxResult is the served task.inbox result.
	InboxResult = api.TaskInboxResult
	// InspectInput is the served execution.inspect request.
	InspectInput = api.ExecutionInspectRequest
	// InspectResult is the served execution.inspect result.
	InspectResult = api.ExecutionInspectResult
	// ResolveInput is the served action-only execution.request.resolve input.
	ResolveInput = api.ExecutionRequestResolveRequest
)

// RequestResolveFunc settles the currently revalidated request with the
// adapter's observed versions and idempotency key as a passing decision.
type RequestResolveFunc func(ctx context.Context, outputs map[string]string) error

// RequestFailFunc settles the currently revalidated request with the adapter's
// observed versions and idempotency key as a failing decision carrying a
// sanitized diagnostic note and no downstream outputs.
type RequestFailFunc func(ctx context.Context, note string) error

// RequestResolver settles one currently revalidated request as either pass or
// fail using the adapter's observed versions and idempotency key.
type RequestResolver struct {
	Pass RequestResolveFunc
	Fail RequestFailFunc
}

// RequestHandlerFunc overrides the default output-free resolution for an
// in-scope, revalidated running request.
type RequestHandlerFunc func(
	ctx context.Context,
	scanned api.InboxItem,
	current api.ExecutionInspectResult,
	action api.ExecutionAction,
	resolve RequestResolver,
) error

// AdapterConfig is the complete independent-service adapter configuration.
type AdapterConfig struct {
	Kind           ServiceKind
	ServiceID      id.PrincipalID
	TenantID       id.TenantID
	ProjectID      id.ProjectID
	Handler        api.Handler
	Events         EventSource
	Stream         client.EventStreamer
	Clock          Clock
	Schedule       time.Duration
	RescanInterval time.Duration

	// PreScan optionally runs before each inbox scan (for example, to reconcile
	// durable pending assignments whose resolve response was lost).
	PreScan func(context.Context) error
	// HandleRequest optionally overrides the default output-free resolution
	// for an in-scope, revalidated running request.
	HandleRequest RequestHandlerFunc
	// SkipExpired, when true, skips inbox items whose At deadline is not in the
	// future before inspecting the execution.
	SkipExpired bool
}

// Adapter is one independent service adapter.
type Adapter struct {
	kind          ServiceKind
	serviceID     id.PrincipalID
	tenant        id.TenantID
	project       id.ProjectID
	handler       api.Handler
	events        EventSource
	stream        client.EventStreamer
	wakeCh        chan struct{}
	clock         Clock
	schedule      time.Duration
	rescan        time.Duration
	preScan       func(context.Context) error
	handleRequest RequestHandlerFunc
	skipExpired   bool
}
