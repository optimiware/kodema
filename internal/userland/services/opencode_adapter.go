package services

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/id"
)

const (
	defaultOpenCodeMaxConcurrency = 4
	maxOpenCodePromptBlobBytes    = 256 << 10
	maxOpenCodeResolveNoteBytes   = 1024
)

// OpenCodeAdapter is the per-project OpenCode brain service adapter.
// It uses ONLY public Kodema APIs (task.inbox, execution.inspect, task.read,
// kv.get, kv.put, event.publish, execution.request.resolve, and the project
// HTTP SSE event stream), maintains persistent task_id -> opencode_session_id
// mappings and isolated per-task local Git worktrees, and aborts active
// OpenCode sessions immediately on cancellation or timeout.
type OpenCodeAdapter struct {
	serviceID      id.PrincipalID
	tenant         id.TenantID
	project        id.ProjectID
	handler        api.Handler
	stream         client.EventStreamer
	events         EventSource
	opencode       OpenCodeClient
	store          OpenCodeStateStore
	worktrees      OpenCodeWorktreeManager
	proxy          *OpenCodeLLMProxy
	clock          Clock
	rescan         time.Duration
	maxConcurrency int
	model          string
	agent          string

	turnSlots chan struct{}

	mu         sync.Mutex
	active     map[string]*activeOpenCodeTurn // "execID/actionID" -> turn
	taskLocks  map[string]chan struct{}       // taskID -> 1-buffered semaphore
	turnWG     sync.WaitGroup
	scanSignal chan struct{}
}

type activeOpenCodeTurn struct {
	executionID string
	actionID    string
	taskID      string
	cancel      context.CancelFunc

	mu        sync.Mutex
	sessionID string
	aborted   bool
	abortOnce sync.Once
}

func (t *activeOpenCodeTurn) setSessionID(sessionID string) {
	t.mu.Lock()
	t.sessionID = sessionID
	t.mu.Unlock()
}

func (t *activeOpenCodeTurn) getSessionID() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sessionID
}

func (t *activeOpenCodeTurn) cancelAndAbort(opencode OpenCodeClient) {
	t.cancel()
	sessionID := t.getSessionID()
	if sessionID == "" || opencode == nil {
		return
	}
	t.abortOnce.Do(func() {
		t.mu.Lock()
		t.aborted = true
		t.mu.Unlock()
		_ = opencode.AbortSession(context.Background(), sessionID)
	})
}

var _ ServiceAdapter = (*OpenCodeAdapter)(nil)

// NewOpenCodeAdapter validates config and constructs an OpenCodeAdapter.
// It starts no background goroutines until Run is called.
func NewOpenCodeAdapter(config OpenCodeAdapterConfig) (*OpenCodeAdapter, error) {
	if _, err := id.ParsePrincipalID(string(config.ServiceID)); err != nil {
		return nil, fmt.Errorf("opencode adapter service identity %q is invalid: %w", config.ServiceID, err)
	}
	if _, err := id.ParseTenantID(string(config.TenantID)); err != nil {
		return nil, fmt.Errorf("opencode adapter tenant %q is invalid: %w", config.TenantID, err)
	}
	if _, err := id.ParseProjectID(string(config.ProjectID)); err != nil {
		return nil, fmt.Errorf("opencode adapter project %q is invalid: %w", config.ProjectID, err)
	}
	if config.Handler == nil {
		return nil, fmt.Errorf("opencode adapter requires a public handler")
	}
	if config.OpenCode == nil {
		return nil, fmt.Errorf("opencode adapter requires an OpenCode client")
	}
	if config.Store == nil {
		return nil, fmt.Errorf("opencode adapter requires a durable state store")
	}
	if config.Clock == nil {
		return nil, fmt.Errorf("opencode adapter requires a clock")
	}
	if config.RescanInterval < 0 {
		return nil, fmt.Errorf("opencode adapter rescan interval is negative")
	}
	rescan := config.RescanInterval
	if rescan == 0 {
		rescan = defaultRescanInterval
	}
	maxConc := config.MaxConcurrentTurns
	if maxConc <= 0 {
		maxConc = defaultOpenCodeMaxConcurrency
	}
	return &OpenCodeAdapter{
		serviceID:      config.ServiceID,
		tenant:         config.TenantID,
		project:        config.ProjectID,
		handler:        config.Handler,
		stream:         config.Stream,
		events:         config.Events,
		opencode:       config.OpenCode,
		store:          config.Store,
		worktrees:      config.Worktrees,
		proxy:          config.Proxy,
		clock:          config.Clock,
		rescan:         rescan,
		maxConcurrency: maxConc,
		model:          strings.TrimSpace(config.Model),
		agent:          strings.TrimSpace(config.Agent),
		turnSlots:      make(chan struct{}, maxConc),
		active:         make(map[string]*activeOpenCodeTurn),
		taskLocks:      make(map[string]chan struct{}),
		scanSignal:     make(chan struct{}, 1),
	}, nil
}

// Kind reports KindOpenCode.
func (a *OpenCodeAdapter) Kind() ServiceKind { return KindOpenCode }

// Run performs startup inbox reconciliation, subscribes to the project's live
// SSE event stream (when configured), and reconciles periodically until ctx is
// cancelled. On cancellation, all active turns and OpenCode sessions are
// aborted cleanly before Run returns nil.
func (a *OpenCodeAdapter) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	defer func() {
		cancelRun()
		a.abortAllActive()
		a.turnWG.Wait()
	}()

	// Initial startup reconciliation so pending requests are never stranded.
	if err := a.reconcileAsync(runCtx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	var streamWG sync.WaitGroup
	if a.stream != nil {
		streamWG.Add(1)
		go func() {
			defer streamWG.Done()
			a.runEventStreamLoop(runCtx)
		}()
	}
	defer streamWG.Wait()

	ticker := time.NewTicker(a.rescan)
	defer ticker.Stop()

	for {
		select {
		case <-runCtx.Done():
			return nil
		case <-a.scanSignal:
			_ = a.reconcileAsync(runCtx)
		case <-ticker.C:
			_ = a.reconcileAsync(runCtx)
		}
	}
}

// RunOnce performs one synchronous inbox scan, executing every eligible
// request assigned to this service and waiting for those turns to finish.
// It is useful for deterministic tests and single-pass reconciliation.
func (a *OpenCodeAdapter) RunOnce(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	items, err := a.scanAssignedRequests(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, item := range items {
		if !a.clock.Now().Before(item.At) {
			continue
		}
		if err := a.executeTurn(ctx, item); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// reconcileAsync scans assigned inbox requests and dispatches non-active ones
// into bounded background turn goroutines.
func (a *OpenCodeAdapter) reconcileAsync(ctx context.Context) error {
	if a.events != nil && a.stream == nil {
		_ = a.drainEvents(ctx)
	}
	items, err := a.scanAssignedRequests(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if !a.clock.Now().Before(item.At) {
			continue
		}
		a.dispatchTurnAsync(ctx, item)
	}
	return nil
}

func (a *OpenCodeAdapter) dispatchTurnAsync(ctx context.Context, item api.InboxItem) {
	key := turnKey(item.ExecutionID, item.ActionID)
	a.mu.Lock()
	if _, exists := a.active[key]; exists {
		a.mu.Unlock()
		return
	}
	turnCtx, cancel := context.WithCancel(ctx)
	turn := &activeOpenCodeTurn{
		executionID: item.ExecutionID,
		actionID:    item.ActionID,
		taskID:      item.TaskID,
		cancel:      cancel,
	}
	a.active[key] = turn
	a.turnWG.Add(1)
	a.mu.Unlock()

	go func() {
		defer a.turnWG.Done()
		defer func() {
			cancel()
			a.mu.Lock()
			if a.active[key] == turn {
				delete(a.active, key)
			}
			a.mu.Unlock()
		}()

		select {
		case a.turnSlots <- struct{}{}:
			defer func() { <-a.turnSlots }()
		case <-turnCtx.Done():
			return
		}

		_ = a.executeRegisteredTurn(turnCtx, turn, item)
	}()
}

// runEventStreamLoop maintains the live SSE subscription with reconnect backoff
// and Last-Event-ID resume provided by client.EventStreamer.
func (a *OpenCodeAdapter) runEventStreamLoop(ctx context.Context) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	var lastCursor string
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := a.stream.StreamEvents(ctx, client.EventStreamRequest{
			ProjectID: string(a.project),
			Since:     lastCursor,
			Types: []string{
				"action.requested",
				"action.transition",
				"request.resolved",
				"execution.cancelled",
				"execution.failed",
				"execution.succeeded",
				"task.cancelled",
				"task.completed",
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = 100 * time.Millisecond

		// Reconcile once upon (re)connect in case events arrived during gap.
		a.triggerScan()

		for {
			evt, nextErr := stream.Next(ctx)
			if nextErr != nil {
				_ = stream.Close()
				break
			}
			if evt.Cursor != "" {
				lastCursor = evt.Cursor
			}
			a.handleStreamEvent(evt)
		}
	}
}

// handleStreamEvent reacts immediately to new requests and cancellations.
func (a *OpenCodeAdapter) handleStreamEvent(evt api.StreamEvent) {
	switch evt.Type {
	case "action.requested":
		a.triggerScan()
	case "action.cancelled":
		actionID := evt.SubjectID
		var data struct {
			ActionID    string `json:"action_id"`
			ExecutionID string `json:"execution_id"`
		}
		if len(evt.Data) > 0 && json.Unmarshal(evt.Data, &data) == nil {
			if data.ActionID != "" {
				actionID = data.ActionID
			}
		}
		a.cancelByAction(actionID)
		if data.ExecutionID != "" && actionID == "" {
			a.cancelByExecution(data.ExecutionID)
		}
	case "action.transition":
		var data struct {
			ActionID string `json:"action_id"`
			State    string `json:"state"`
		}
		if len(evt.Data) > 0 && json.Unmarshal(evt.Data, &data) == nil {
			if data.State == "cancelled" || data.State == "failed" || data.State == "succeeded" {
				actionID := data.ActionID
				if actionID == "" {
					actionID = evt.SubjectID
				}
				a.cancelByAction(actionID)
			}
		}
	case "request.resolved":
		var data struct {
			ActionID    string `json:"action_id"`
			ExecutionID string `json:"execution_id"`
		}
		_ = json.Unmarshal(evt.Data, &data)
		if data.ActionID != "" {
			a.cancelByAction(data.ActionID)
		} else if data.ExecutionID != "" {
			a.cancelByExecution(data.ExecutionID)
		} else if evt.SubjectID != "" {
			a.cancelByExecution(evt.SubjectID)
			a.cancelByAction(evt.SubjectID)
		}
	case "execution.cancelled", "execution.failed", "execution.succeeded":
		execID := evt.SubjectID
		var data struct {
			ExecutionID string `json:"execution_id"`
		}
		if len(evt.Data) > 0 && json.Unmarshal(evt.Data, &data) == nil && data.ExecutionID != "" {
			execID = data.ExecutionID
		}
		a.cancelByExecution(execID)
	case "task.cancelled", "task.completed":
		taskID := evt.SubjectID
		var data struct {
			TaskID string `json:"task_id"`
		}
		if len(evt.Data) > 0 && json.Unmarshal(evt.Data, &data) == nil && data.TaskID != "" {
			taskID = data.TaskID
		}
		a.cancelByTask(taskID)
	}
}

func (a *OpenCodeAdapter) triggerScan() {
	select {
	case a.scanSignal <- struct{}{}:
	default:
	}
}

func (a *OpenCodeAdapter) cancelByAction(actionID string) {
	if actionID == "" {
		return
	}
	var matched []*activeOpenCodeTurn
	a.mu.Lock()
	for _, t := range a.active {
		if t.actionID == actionID {
			matched = append(matched, t)
		}
	}
	a.mu.Unlock()
	for _, t := range matched {
		go t.cancelAndAbort(a.opencode)
	}
}

func (a *OpenCodeAdapter) cancelByExecution(executionID string) {
	if executionID == "" {
		return
	}
	var matched []*activeOpenCodeTurn
	a.mu.Lock()
	for _, t := range a.active {
		if t.executionID == executionID {
			matched = append(matched, t)
		}
	}
	a.mu.Unlock()
	for _, t := range matched {
		go t.cancelAndAbort(a.opencode)
	}
}

func (a *OpenCodeAdapter) cancelByTask(taskID string) {
	if taskID == "" {
		return
	}
	var matched []*activeOpenCodeTurn
	a.mu.Lock()
	for _, t := range a.active {
		if t.taskID == taskID {
			matched = append(matched, t)
		}
	}
	a.mu.Unlock()
	for _, t := range matched {
		go t.cancelAndAbort(a.opencode)
	}
}

func (a *OpenCodeAdapter) abortAllActive() {
	a.mu.Lock()
	all := make([]*activeOpenCodeTurn, 0, len(a.active))
	for _, t := range a.active {
		all = append(all, t)
	}
	a.mu.Unlock()
	for _, t := range all {
		t.cancelAndAbort(a.opencode)
	}
}

// executeTurn executes a single turn synchronously (used by RunOnce).
func (a *OpenCodeAdapter) executeTurn(ctx context.Context, scanned api.InboxItem) error {
	key := turnKey(scanned.ExecutionID, scanned.ActionID)
	a.mu.Lock()
	if _, exists := a.active[key]; exists {
		a.mu.Unlock()
		return nil
	}
	turnCtx, cancel := context.WithCancel(ctx)
	turn := &activeOpenCodeTurn{
		executionID: scanned.ExecutionID,
		actionID:    scanned.ActionID,
		taskID:      scanned.TaskID,
		cancel:      cancel,
	}
	a.active[key] = turn
	a.mu.Unlock()
	defer func() {
		cancel()
		a.mu.Lock()
		if a.active[key] == turn {
			delete(a.active, key)
		}
		a.mu.Unlock()
	}()

	return a.executeRegisteredTurn(turnCtx, turn, scanned)
}

func (a *OpenCodeAdapter) executeRegisteredTurn(turnCtx context.Context, turn *activeOpenCodeTurn, scanned api.InboxItem) error {
	// 1. Revalidate current execution and action state before any side effect.
	current, err := a.inspectExecution(turnCtx, scanned.ExecutionID)
	if err != nil {
		var publicErr *api.Error
		if errors.As(err, &publicErr) && publicErr.Code == api.CodeNotFound {
			return nil
		}
		return err
	}
	if current.ExecutionID != scanned.ExecutionID || current.State != "running" {
		return nil
	}
	action, ok := currentAction(current, scanned.ActionID)
	if !ok || action.Kind != "request" || action.State != "running" || action.Decision != "" {
		return nil
	}
	if action.Assignment != "" && action.Assignment != "service/"+string(a.serviceID) {
		return nil
	}
	if action.Version < scanned.Version {
		return fmt.Errorf("%w: current action version %d is behind scanned version %d", ErrStaleVersion, action.Version, scanned.Version)
	}
	deadline := scanned.At
	if action.Deadline != nil && !action.Deadline.IsZero() {
		deadline = *action.Deadline
	}
	if !a.clock.Now().Before(deadline) {
		return nil
	}

	// Update turn.taskID from authoritative execution.inspect so task.cancelled
	// matches even if inbox omitted TaskID.
	taskID := current.TaskID
	if taskID == "" {
		taskID = scanned.TaskID
	}
	if taskID == "" {
		taskID = "exec-" + scanned.ExecutionID
	}
	a.mu.Lock()
	turn.taskID = taskID
	a.mu.Unlock()

	// 2. Check durable per-action idempotency record to avoid double-pushes or
	// re-running after a crash between push and resolve.
	if existing, found, err := a.store.LoadAction(turnCtx, a.tenant, a.project, scanned.ExecutionID, scanned.ActionID); err == nil && found {
		if existing.Resolved {
			return nil
		}
		if existing.Pushed && len(existing.Outputs) > 0 {
			if err := a.resolveRequest(turnCtx, scanned, current, action, DecisionPass, existing.Outputs, ""); err != nil {
				return err
			}
			existing.Resolved = true
			existing.UpdatedAt = a.clock.Now()
			_ = a.store.SaveAction(context.WithoutCancel(turnCtx), existing)
			return nil
		}
	}

	// 3. Parse and validate the typed request subject.
	rawSubject := scanned.Subject
	if rawSubject == nil {
		rawSubject = action.Subject
	}
	subject, err := ParseOpenCodeSubject(rawSubject)
	if err != nil {
		return err
	}

	// 4. Derive a deadline-bounded context for this turn and register with
	// the local attribution LLM proxy (if configured).
	remaining := deadline.Sub(a.clock.Now())
	if remaining <= 0 {
		return nil
	}
	callCtx, cancelCall := context.WithTimeout(turnCtx, remaining)
	defer cancelCall()

	unregisterProxy := a.proxy.RegisterTurn(callCtx, scanned.ExecutionID, scanned.ActionID)
	defer unregisterProxy()

	// 5. Serialize concurrent turns for the same task so worktree & OpenCode
	// session state are never mutated concurrently.
	releaseTask, err := a.acquireTaskLock(callCtx, taskID)
	if err != nil {
		return err
	}
	defer releaseTask()

	// 6. Fetch task spec/conversation and referenced KV blobs.
	var taskInfo *api.TaskReadResult
	if current.TaskID != "" {
		readTask, taskErr := a.readTask(callCtx, current.TaskID)
		if taskErr == nil {
			taskInfo = &readTask
		}
	}
	var planContent, feedbackContent string
	if subject.PlanKey != "" {
		if obj, kvErr := a.kvGet(callCtx, subject.PlanKey); kvErr == nil {
			planContent = truncateString(string(obj.Data), maxOpenCodePromptBlobBytes)
		}
	}
	if subject.FeedbackKey != "" {
		if obj, kvErr := a.kvGet(callCtx, subject.FeedbackKey); kvErr == nil {
			feedbackContent = truncateString(string(obj.Data), maxOpenCodePromptBlobBytes)
		}
	}

	// 7. Prepare isolated local Git worktree if a repository is named.
	var checkout WorktreeCheckout
	if subject.Repository != "" {
		if a.worktrees == nil {
			return fmt.Errorf("opencode request names repository %q but no worktree manager is configured", subject.Repository)
		}
		checkout, err = a.worktrees.Prepare(callCtx, subject.Repository, taskID, subject.BaseRef)
		if err != nil {
			if callCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("prepare task worktree: %w", err)
		}
	}

	// 8. Reuse existing persistent OpenCode session for this task_id, or create
	// and persist a new one atomically before sending the turn prompt.
	sessionID, err := a.ensureTaskSession(callCtx, taskID, taskInfo, subject, checkout)
	if err != nil {
		if callCtx.Err() != nil {
			return nil
		}
		return err
	}
	turn.setSessionID(sessionID)
	if callCtx.Err() != nil {
		turn.cancelAndAbort(a.opencode)
		return nil
	}

	// 9. Publish best-effort started progress event.
	a.publishProgress(callCtx, scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "started", "")

	// 10. Build turn prompt and dispatch to OpenCode.
	prompt := buildOpenCodePrompt(subject, taskInfo, current, checkout, planContent, feedbackContent)
	resp, err := a.opencode.SendMessage(callCtx, sessionID, OpenCodeMessageRequest{
		Directory:   checkout.WorktreePath,
		Prompt:      prompt,
		Mode:        subject.Mode,
		Model:       a.model,
		Agent:       a.agent,
		ExecutionID: scanned.ExecutionID,
		ActionID:    scanned.ActionID,
		TaskID:      taskID,
	})
	if err != nil {
		if callCtx.Err() != nil {
			turn.cancelAndAbort(a.opencode)
			return nil
		}
		if errors.Is(err, ErrOpenCodeBudgetExceeded) {
			a.publishProgress(context.WithoutCancel(callCtx), scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "budget_exceeded", err.Error())
			note := truncateString("budget_exceeded: "+err.Error(), maxOpenCodeResolveNoteBytes)
			if resolveErr := a.resolveRequest(callCtx, scanned, current, action, "fail", nil, note); resolveErr == nil {
				_ = a.store.SaveAction(context.WithoutCancel(callCtx), OpenCodeActionRecord{
					TenantID:    a.tenant,
					ProjectID:   a.project,
					ExecutionID: scanned.ExecutionID,
					ActionID:    scanned.ActionID,
					TaskID:      taskID,
					SessionID:   sessionID,
					Resolved:    true,
					UpdatedAt:   a.clock.Now(),
				})
				return nil
			}
			return err
		}
		a.publishProgress(context.WithoutCancel(callCtx), scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "failed", err.Error())
		return err
	}
	if callCtx.Err() != nil {
		turn.cancelAndAbort(a.opencode)
		return nil
	}

	// 11. For coding modes, commit & push changes to refs/heads/work/<task-id>.
	var branch, candidate string
	if subject.Mode.MutatesCode() && checkout.WorktreePath != "" {
		commitMsg := fmt.Sprintf("opencode(%s): %s", subject.Mode, taskID)
		if taskInfo != nil && strings.TrimSpace(taskInfo.Title) != "" {
			commitMsg = fmt.Sprintf("opencode(%s): %s (%s)", subject.Mode, strings.TrimSpace(taskInfo.Title), taskID)
		}
		pushRes, pushErr := a.worktrees.CommitAndPush(callCtx, checkout, commitMsg)
		if pushErr != nil {
			if callCtx.Err() != nil {
				turn.cancelAndAbort(a.opencode)
				return nil
			}
			return fmt.Errorf("commit and push task branch: %w", pushErr)
		}
		branch = pushRes.Branch
		candidate = pushRes.Candidate

		// Persist pushed checkpoint immediately so a crash before KV/resolve
		// never double-pushes on retry.
		_ = a.store.SaveAction(context.WithoutCancel(callCtx), OpenCodeActionRecord{
			TenantID:    a.tenant,
			ProjectID:   a.project,
			ExecutionID: scanned.ExecutionID,
			ActionID:    scanned.ActionID,
			TaskID:      taskID,
			SessionID:   sessionID,
			Branch:      branch,
			Candidate:   candidate,
			Pushed:      true,
			UpdatedAt:   a.clock.Now(),
		})
	} else if checkout.Branch != "" {
		branch = checkout.Branch
	}

	// 12. Write Markdown summary/plan/review to Kodema KV.
	markdown := formatOpenCodeMarkdown(subject.Mode, taskID, sessionID, branch, candidate, resp.Text)
	var kvKey string
	if needsKVOutput(scanned.Outputs) || strings.TrimSpace(resp.Text) != "" {
		kvRes, kvErr := a.kvPut(callCtx, []byte(markdown), "text/markdown; charset=utf-8")
		if kvErr != nil {
			if callCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("store opencode summary in KV: %w", kvErr)
		}
		kvKey = kvRes.Key
	}

	// 13. Map outputs strictly to the request's declared outputs.
	outputs := buildOpenCodeOutputs(scanned.Outputs, subject, sessionID, branch, candidate, kvKey)

	actionRecord := OpenCodeActionRecord{
		TenantID:    a.tenant,
		ProjectID:   a.project,
		ExecutionID: scanned.ExecutionID,
		ActionID:    scanned.ActionID,
		TaskID:      taskID,
		SessionID:   sessionID,
		Branch:      branch,
		Candidate:   candidate,
		SummaryKey:  kvKey,
		PlanKey:     subject.PlanKey,
		Outputs:     outputs,
		Pushed:      true,
		UpdatedAt:   a.clock.Now(),
	}
	if subject.Mode == OpenCodeModePlan && kvKey != "" {
		actionRecord.PlanKey = kvKey
	}
	if err := a.store.SaveAction(context.WithoutCancel(callCtx), actionRecord); err != nil {
		return err
	}

	// 14. Publish best-effort completion progress event.
	a.publishProgress(callCtx, scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "completed", candidate)

	// 15. Resolve the request idempotently with expected execution & action versions.
	if err := a.resolveRequest(callCtx, scanned, current, action, DecisionPass, outputs, ""); err != nil {
		return err
	}
	actionRecord.Resolved = true
	actionRecord.UpdatedAt = a.clock.Now()
	_ = a.store.SaveAction(context.WithoutCancel(callCtx), actionRecord)
	return nil
}

func (a *OpenCodeAdapter) ensureTaskSession(
	ctx context.Context,
	taskID string,
	taskInfo *api.TaskReadResult,
	subject OpenCodeSubject,
	checkout WorktreeCheckout,
) (string, error) {
	existing, found, err := a.store.LoadTask(ctx, a.tenant, a.project, taskID)
	if err != nil {
		return "", fmt.Errorf("load opencode task state: %w", err)
	}
	if found && strings.TrimSpace(existing.OpenCodeSessionID) != "" {
		if (checkout.WorktreePath != "" && existing.WorktreePath != checkout.WorktreePath) ||
			(checkout.Branch != "" && existing.Branch != checkout.Branch) ||
			(subject.Repository != "" && existing.Repository != subject.Repository) {
			existing.Repository = subject.Repository
			existing.WorktreePath = checkout.WorktreePath
			existing.Branch = checkout.Branch
			existing.UpdatedAt = a.clock.Now()
			_ = a.store.SaveTask(context.WithoutCancel(ctx), existing)
		}
		return existing.OpenCodeSessionID, nil
	}

	title := "kodema:" + taskID
	if taskInfo != nil && strings.TrimSpace(taskInfo.Title) != "" {
		title = fmt.Sprintf("kodema:%s %s", taskID, strings.TrimSpace(taskInfo.Title))
	}
	created, err := a.opencode.CreateSession(ctx, checkout.WorktreePath, title)
	if err != nil {
		return "", fmt.Errorf("create opencode session: %w", err)
	}
	state := OpenCodeTaskState{
		TenantID:          a.tenant,
		ProjectID:         a.project,
		TaskID:            taskID,
		OpenCodeSessionID: created.ID,
		Repository:        subject.Repository,
		WorktreePath:      checkout.WorktreePath,
		Branch:            checkout.Branch,
		UpdatedAt:         a.clock.Now(),
	}
	if err := a.store.SaveTask(context.WithoutCancel(ctx), state); err != nil {
		return "", fmt.Errorf("persist opencode task state: %w", err)
	}
	return created.ID, nil
}

func (a *OpenCodeAdapter) acquireTaskLock(ctx context.Context, taskID string) (func(), error) {
	a.mu.Lock()
	sem, ok := a.taskLocks[taskID]
	if !ok {
		sem = make(chan struct{}, 1)
		a.taskLocks[taskID] = sem
	}
	a.mu.Unlock()

	select {
	case sem <- struct{}{}:
		return func() { <-sem }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *OpenCodeAdapter) drainEvents(ctx context.Context) error {
	cursor := ""
	seen := make(map[string]bool)
	for {
		page, err := a.events.Poll(ctx, cursor)
		if err != nil {
			return err
		}
		next := page.Next.NextCursor
		if next == "" || next == cursor || seen[next] {
			return nil
		}
		seen[next] = true
		cursor = next
	}
}

func (a *OpenCodeAdapter) scanAssignedRequests(ctx context.Context) ([]api.InboxItem, error) {
	cursor := ""
	seen := make(map[string]bool)
	var items []api.InboxItem
	wantAssignment := "service/" + string(a.serviceID)

	for {
		decoded, err := a.invoke(ctx, OperationInbox, api.TaskInboxRequest{
			ProjectID: string(a.project),
			Kinds:     []string{requestInboxKind},
			Page:      &api.Page{Limit: api.DefaultPageLimit, Cursor: cursor},
		}, nil, "")
		if err != nil {
			return nil, err
		}
		page, ok := decoded.(api.TaskInboxResult)
		if !ok {
			return nil, fmt.Errorf("task.inbox returned unexpected %T", decoded)
		}
		for _, item := range page.Items {
			if item.Kind == requestInboxKind && item.Assignment == wantAssignment {
				items = append(items, item)
			}
		}
		if page.NextCursor == "" || seen[page.NextCursor] {
			break
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
	return items, nil
}

func (a *OpenCodeAdapter) inspectExecution(ctx context.Context, executionID string) (api.ExecutionInspectResult, error) {
	decoded, err := a.invoke(ctx, OperationInspect, api.ExecutionInspectRequest{
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

func (a *OpenCodeAdapter) readTask(ctx context.Context, taskID string) (api.TaskReadResult, error) {
	decoded, err := a.invoke(ctx, OperationTaskRead, api.TaskReadRequest{
		TaskID: taskID,
	}, nil, "")
	if err != nil {
		return api.TaskReadResult{}, err
	}
	res, ok := decoded.(api.TaskReadResult)
	if !ok {
		return api.TaskReadResult{}, fmt.Errorf("task.read returned unexpected %T", decoded)
	}
	return res, nil
}

func (a *OpenCodeAdapter) kvGet(ctx context.Context, key string) (client.KVObject, error) {
	decoded, err := a.invoke(ctx, OperationKVGet, api.KVGetRequest{
		ProjectID: string(a.project),
		Key:       key,
	}, nil, "")
	if err != nil {
		return client.KVObject{}, err
	}
	out, ok := decoded.(api.KVGetResult)
	if !ok {
		return client.KVObject{}, fmt.Errorf("kv.get returned unexpected %T", decoded)
	}
	data, err := base64.StdEncoding.DecodeString(out.DataBase64)
	if err != nil {
		return client.KVObject{}, fmt.Errorf("decode kv.get base64: %w", err)
	}
	return client.KVObject{
		Key:         out.Key,
		ProjectID:   out.ProjectID,
		SizeBytes:   out.SizeBytes,
		ContentType: out.ContentType,
		CreatedBy:   out.CreatedBy,
		CreatedAt:   out.CreatedAt,
		Data:        data,
		Raw:         out,
	}, nil
}

func (a *OpenCodeAdapter) kvPut(ctx context.Context, data []byte, contentType string) (api.KVPutResult, error) {
	encoded := base64.StdEncoding.EncodeToString(data)
	decoded, err := a.invoke(ctx, OperationKVPut, api.KVPutRequest{
		ProjectID:   string(a.project),
		DataBase64:  &encoded,
		ContentType: contentType,
	}, nil, "")
	if err != nil {
		return api.KVPutResult{}, err
	}
	out, ok := decoded.(api.KVPutResult)
	if !ok {
		return api.KVPutResult{}, fmt.Errorf("kv.put returned unexpected %T", decoded)
	}
	return out, nil
}

// publishProgress emits a bounded `observation.custom` event via event.publish.
// Because strict zero-role (`roles: []`) service accounts may not hold
// `event.publish` permission in the frozen operation matrix, publish failures
// are intentionally best-effort and never abort the turn.
func (a *OpenCodeAdapter) publishProgress(ctx context.Context, executionID, actionID, taskID, sessionID, mode, phase, detail string) {
	if ctx == nil || ctx.Err() != nil {
		return
	}
	payload, err := json.Marshal(struct {
		Service     string `json:"service"`
		ExecutionID string `json:"execution_id"`
		ActionID    string `json:"action_id"`
		TaskID      string `json:"task_id,omitempty"`
		SessionID   string `json:"session_id,omitempty"`
		Mode        string `json:"mode,omitempty"`
		Phase       string `json:"phase"`
		Detail      string `json:"detail,omitempty"`
	}{
		Service:     string(a.serviceID),
		ExecutionID: executionID,
		ActionID:    actionID,
		TaskID:      taskID,
		SessionID:   sessionID,
		Mode:        mode,
		Phase:       phase,
		Detail:      truncateString(detail, 512),
	})
	if err != nil {
		return
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("opencode-progress:%s:%s:%s:%s", executionID, actionID, phase, sessionID)))
	idemKey := "opencode-evt:" + hex.EncodeToString(sum[:])[:32]
	_, _ = a.invoke(ctx, OperationEventPublish, api.EventPublishRequest{
		ProjectID:     string(a.project),
		Type:          "observation.custom",
		SubjectID:     executionID,
		CorrelationID: actionID,
		Data:          payload,
	}, nil, idemKey)
}

func (a *OpenCodeAdapter) resolveRequest(
	ctx context.Context,
	scanned api.InboxItem,
	current api.ExecutionInspectResult,
	action api.ExecutionAction,
	decision string,
	outputs map[string]string,
	note string,
) error {
	expected := current.Version
	req := api.ExecutionRequestResolveRequest{
		ExecutionID:           scanned.ExecutionID,
		ActionID:              scanned.ActionID,
		ExpectedActionVersion: action.Version,
		Decision:              decision,
		Outputs:               outputs,
		Note:                  note,
	}
	_, err := a.invoke(ctx, OperationResolve, req, &expected, a.resolveIdempotencyKey(scanned, current, action))
	if err == nil {
		return nil
	}
	var publicErr *api.Error
	if errors.As(err, &publicErr) {
		switch publicErr.Code {
		case api.CodeVersionConflict:
			return fmt.Errorf("%w: %v", ErrStaleVersion, err)
		case api.CodeChangedIdempotencyContent:
			return fmt.Errorf("%w: %v", ErrChangedContent, err)
		}
	}
	return err
}

func (a *OpenCodeAdapter) resolveIdempotencyKey(scanned api.InboxItem, current api.ExecutionInspectResult, action api.ExecutionAction) string {
	scope := strings.Join([]string{
		string(KindOpenCode),
		scanned.ExecutionID,
		scanned.ActionID,
		strconv.FormatUint(current.Version, 10),
		strconv.FormatUint(action.Version, 10),
	}, ":")
	sum := sha256.Sum256([]byte(scope))
	return fmt.Sprintf("%s:v%d:%s", KindOpenCode, action.Version, hex.EncodeToString(sum[:])[:32])
}

func (a *OpenCodeAdapter) invoke(ctx context.Context, operation string, request any, expectedVersion *uint64, idempotencyKey string) (any, error) {
	codec, ok := publicCodecs[operation]
	if !ok {
		return nil, fmt.Errorf("operation %q has no public codec", operation)
	}
	input, err := codec.EncodeRequest(request)
	if err != nil {
		return nil, fmt.Errorf("encode %s request: %w", operation, err)
	}
	result, err := a.handler.Invoke(ctx, api.Invocation{
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
	decoded, err := codec.DecodeResult(result.Body)
	if err != nil {
		return nil, fmt.Errorf("decode %s result: %w", operation, err)
	}
	return decoded, nil
}

func turnKey(executionID, actionID string) string {
	return executionID + "/" + actionID
}

func needsKVOutput(declared []string) bool {
	for _, name := range declared {
		switch name {
		case OpenCodeOutputSummaryKey, OpenCodeOutputPlanKey, OpenCodeOutputReviewKey:
			return true
		}
	}
	return false
}

func buildOpenCodeOutputs(declared []string, subject OpenCodeSubject, sessionID, branch, candidate, kvKey string) map[string]string {
	if len(declared) == 0 {
		return nil
	}
	outputs := make(map[string]string, len(declared))
	for _, name := range declared {
		switch name {
		case OpenCodeOutputBranch:
			if branch != "" {
				outputs[name] = branch
			}
		case OpenCodeOutputCandidate:
			if candidate != "" {
				outputs[name] = candidate
			}
		case OpenCodeOutputSummaryKey:
			if kvKey != "" {
				outputs[name] = kvKey
			}
		case OpenCodeOutputPlanKey:
			if subject.Mode == OpenCodeModePlan && kvKey != "" {
				outputs[name] = kvKey
			} else if subject.PlanKey != "" {
				outputs[name] = subject.PlanKey
			} else if kvKey != "" {
				outputs[name] = kvKey
			}
		case OpenCodeOutputReviewKey:
			if kvKey != "" {
				outputs[name] = kvKey
			}
		case OpenCodeOutputSessionID:
			if sessionID != "" {
				outputs[name] = sessionID
			}
		}
	}
	if len(outputs) == 0 {
		return nil
	}
	return outputs
}

func buildOpenCodePrompt(
	subject OpenCodeSubject,
	task *api.TaskReadResult,
	exec api.ExecutionInspectResult,
	checkout WorktreeCheckout,
	planContent string,
	feedbackContent string,
) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Kodema OpenCode Turn\n\n")
	fmt.Fprintf(&b, "- Mode: `%s`\n", subject.Mode)
	fmt.Fprintf(&b, "- Execution: `%s`\n", exec.ExecutionID)
	if exec.TaskID != "" {
		fmt.Fprintf(&b, "- Task: `%s`\n", exec.TaskID)
	}
	if subject.Repository != "" {
		fmt.Fprintf(&b, "- Repository: `%s`\n", subject.Repository)
	}
	if subject.BaseRef != "" {
		fmt.Fprintf(&b, "- Base Ref: `%s`\n", subject.BaseRef)
	}
	if checkout.Branch != "" {
		fmt.Fprintf(&b, "- Work Branch: `%s`\n", checkout.Branch)
	}

	if task != nil {
		fmt.Fprintf(&b, "\n## Task Specification\n\n### %s\n", task.Title)
		if strings.TrimSpace(task.Description) != "" {
			fmt.Fprintf(&b, "\n%s\n", strings.TrimSpace(task.Description))
		}
		if len(task.Messages) > 0 {
			fmt.Fprintf(&b, "\n### Conversation\n")
			for _, msg := range task.Messages {
				fmt.Fprintf(&b, "- **%s** (%s): %s\n", msg.Author, msg.At.UTC().Format(time.RFC3339), msg.Body)
			}
		}
	}

	if strings.TrimSpace(planContent) != "" {
		fmt.Fprintf(&b, "\n## Approved Plan (`%s`)\n\n%s\n", subject.PlanKey, strings.TrimSpace(planContent))
	}
	if strings.TrimSpace(feedbackContent) != "" {
		fmt.Fprintf(&b, "\n## Feedback / Failure Diagnostics (`%s`)\n\n%s\n", subject.FeedbackKey, strings.TrimSpace(feedbackContent))
	}
	if len(subject.Extra) > 0 {
		keys := make([]string, 0, len(subject.Extra))
		for k := range subject.Extra {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Fprintf(&b, "\n## Additional Context\n")
		for _, k := range keys {
			fmt.Fprintf(&b, "- `%s`: %s\n", k, subject.Extra[k])
		}
	}

	fmt.Fprintf(&b, "\n## Mode Instructions\n")
	switch subject.Mode {
	case OpenCodeModePlan:
		b.WriteString("Produce a clear, structured implementation plan in Markdown without pushing code.\n")
	case OpenCodeModeImplement:
		b.WriteString("Implement the requested changes in the checked-out worktree and provide a concise Markdown summary.\n")
	case OpenCodeModeFix:
		b.WriteString("Diagnose and fix the reported verification failures in the existing worktree and provide a concise Markdown summary.\n")
	case OpenCodeModeRevise:
		b.WriteString("Address the reviewer's requested revisions in the existing worktree and provide a concise Markdown summary.\n")
	case OpenCodeModeAIReview:
		b.WriteString("Perform a thorough code review of the candidate changes and output structured Markdown review findings.\n")
	}
	return b.String()
}

func formatOpenCodeMarkdown(mode OpenCodeMode, taskID, sessionID, branch, candidate, responseText string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# OpenCode %s Report (%s)\n\n", mode, taskID)
	fmt.Fprintf(&b, "- Session: `%s`\n", sessionID)
	if branch != "" {
		fmt.Fprintf(&b, "- Branch: `%s`\n", branch)
	}
	if candidate != "" {
		fmt.Fprintf(&b, "- Candidate: `%s`\n", candidate)
	}
	b.WriteString("\n")
	text := strings.TrimSpace(responseText)
	if text == "" {
		text = "(no textual summary returned)"
	}
	b.WriteString(text)
	b.WriteString("\n")
	return b.String()
}

func truncateString(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	return s[:maxBytes]
}
