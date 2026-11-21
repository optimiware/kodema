package services

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	defaultOpenCodeMaxConcurrency   = 4
	defaultOpenCodeAbortCallTimeout = 5 * time.Second
	maxOpenCodePromptBlobBytes      = 256 << 10
	maxOpenCodeResolveNoteBytes     = 1024
)

type openCodeTaskLockEntry struct {
	sem  chan struct{}
	refs int
}

// OpenCodeAdapter is the per-project OpenCode brain service adapter.
// It uses ONLY public Kodema APIs (task.inbox, execution.inspect, task.read,
// kv.get, kv.put, event.publish, execution.request.resolve, and the project
// HTTP SSE event stream), maintains persistent task_id -> opencode_session_id
// mappings and isolated per-task local Git worktrees, and aborts active
// OpenCode sessions immediately on cancellation or timeout.
type OpenCodeAdapter struct {
	serviceID           id.PrincipalID
	tenant              id.TenantID
	project             id.ProjectID
	executionMode       OpenCodeExecutionMode
	brain               OpenCodeBrainConfig
	handler             api.Handler
	stream              client.EventStreamer
	events              EventSource
	opencode            OpenCodeClient
	store               OpenCodeStateStore
	worktrees           OpenCodeWorktreeManager
	sandboxes           OpenCodeSandboxManager
	proxy               *OpenCodeLLMProxy
	perTurnProxyRouting bool
	sandboxRT           http.RoundTripper
	clock               Clock
	rescan              time.Duration
	maxConcurrency      int
	model               string
	agent               string

	turnSlots chan struct{}

	mu         sync.Mutex
	active     map[string]*activeOpenCodeTurn    // "execID/actionID" -> turn
	taskLocks  map[string]*openCodeTaskLockEntry // taskID -> refcounted 1-buffered semaphore
	turnWG     sync.WaitGroup
	abortWG    sync.WaitGroup
	scanSignal chan struct{}
}

type activeOpenCodeTurn struct {
	executionID string
	actionID    string
	taskID      string
	cancel      context.CancelFunc

	mu           sync.Mutex
	sessionID    string
	aborted      bool
	abortOnce    sync.Once
	sandboxLease *OpenCodeSandboxLease
	releaseOnce  sync.Once
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

func (t *activeOpenCodeTurn) setSandboxLease(lease OpenCodeSandboxLease) {
	t.mu.Lock()
	copyLease := lease
	t.sandboxLease = &copyLease
	t.mu.Unlock()
}

func (t *activeOpenCodeTurn) getSandboxLease() (OpenCodeSandboxLease, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sandboxLease == nil {
		return OpenCodeSandboxLease{}, false
	}
	return *t.sandboxLease, true
}

func (t *activeOpenCodeTurn) releaseSandbox(mgr OpenCodeSandboxManager) {
	if mgr == nil {
		return
	}
	lease, ok := t.getSandboxLease()
	if !ok {
		return
	}
	t.releaseOnce.Do(func() {
		relCtx, cancel := context.WithTimeout(context.Background(), defaultSandboxStopTimeout)
		defer cancel()
		_ = mgr.Release(relCtx, lease)
	})
}

func (t *activeOpenCodeTurn) abortSession(opencode OpenCodeClient) {
	sessionID := t.getSessionID()
	if sessionID == "" || opencode == nil {
		return
	}
	t.abortOnce.Do(func() {
		t.mu.Lock()
		t.aborted = true
		t.mu.Unlock()
		abortCtx, cancel := context.WithTimeout(context.Background(), defaultOpenCodeAbortCallTimeout)
		defer cancel()
		_ = opencode.AbortSession(abortCtx, sessionID)
	})
}

func (t *activeOpenCodeTurn) cancelAndAbort(opencode OpenCodeClient) {
	t.cancel()
	t.abortSession(opencode)
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
	execMode := config.ExecutionMode
	if execMode == "" {
		execMode = OpenCodeExecutionModeWorktree
	}
	if !execMode.Valid() {
		return nil, fmt.Errorf("opencode adapter execution mode %q is unsupported", execMode)
	}
	var brainCfg OpenCodeBrainConfig
	if execMode == OpenCodeExecutionModeBrain {
		if config.Sandboxes == nil {
			return nil, fmt.Errorf("opencode brain mode requires a sandbox manager")
		}
		var err error
		brainCfg, err = NormalizeBrainConfig(config.Brain)
		if err != nil {
			return nil, err
		}
	}
	rescan := config.RescanInterval
	if rescan == 0 {
		rescan = defaultRescanInterval
	}
	maxConc := config.MaxConcurrentTurns
	if config.Proxy != nil && !config.PerTurnProxyRouting {
		if maxConc > 1 {
			return nil, fmt.Errorf("opencode adapter with shared static-URL LLM proxy requires MaxConcurrentTurns <= 1 unless PerTurnProxyRouting is enabled")
		}
		maxConc = 1
	} else if maxConc <= 0 {
		maxConc = defaultOpenCodeMaxConcurrency
	}
	return &OpenCodeAdapter{
		serviceID:           config.ServiceID,
		tenant:              config.TenantID,
		project:             config.ProjectID,
		executionMode:       execMode,
		brain:               brainCfg,
		handler:             config.Handler,
		stream:              config.Stream,
		events:              config.Events,
		opencode:            config.OpenCode,
		store:               config.Store,
		worktrees:           config.Worktrees,
		sandboxes:           config.Sandboxes,
		proxy:               config.Proxy,
		perTurnProxyRouting: config.PerTurnProxyRouting,
		sandboxRT:           config.SandboxRoundTripper,
		clock:               config.Clock,
		rescan:              rescan,
		maxConcurrency:      maxConc,
		model:               strings.TrimSpace(config.Model),
		agent:               strings.TrimSpace(config.Agent),
		turnSlots:           make(chan struct{}, maxConc),
		active:              make(map[string]*activeOpenCodeTurn),
		taskLocks:           make(map[string]*openCodeTaskLockEntry),
		scanSignal:          make(chan struct{}, 1),
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
		a.abortWG.Wait()
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

func (a *OpenCodeAdapter) cancelTurnsAsync(matched []*activeOpenCodeTurn) {
	for _, t := range matched {
		// Cancel turn context synchronously before spawning AbortSession and
		// sandbox termination so in-flight LLM proxy calls and git commands
		// stop immediately.
		t.cancel()
		a.abortWG.Add(1)
		go func(turn *activeOpenCodeTurn) {
			defer a.abortWG.Done()
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				turn.abortSession(a.opencode)
			}()
			if a.sandboxes != nil {
				wg.Add(1)
				go func() {
					defer wg.Done()
					turn.releaseSandbox(a.sandboxes)
				}()
			}
			wg.Wait()
		}(t)
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
	a.cancelTurnsAsync(matched)
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
	a.cancelTurnsAsync(matched)
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
	a.cancelTurnsAsync(matched)
}

func (a *OpenCodeAdapter) abortAllActive() {
	a.mu.Lock()
	all := make([]*activeOpenCodeTurn, 0, len(a.active))
	for _, t := range a.active {
		all = append(all, t)
	}
	a.mu.Unlock()
	a.cancelTurnsAsync(all)
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

	// 2. Parse and validate the typed request subject before resuming or running.
	rawSubject := scanned.Subject
	if rawSubject == nil {
		rawSubject = action.Subject
	}
	subject, err := ParseOpenCodeSubject(rawSubject)
	if err != nil {
		return err
	}

	// 3. Derive a deadline-bounded context for this turn.
	remaining := deadline.Sub(a.clock.Now())
	if remaining <= 0 {
		return nil
	}
	callCtx, cancelCall := context.WithTimeout(turnCtx, remaining)
	defer cancelCall()

	// 4. Check durable per-action checkpoint (failing closed on unreadable or
	// corrupt state) so restarts never double-push or re-run settled requests.
	existing, found, err := a.store.LoadAction(callCtx, a.tenant, a.project, scanned.ExecutionID, scanned.ActionID)
	if err != nil {
		return fmt.Errorf("load opencode action checkpoint: %w", err)
	}
	if found {
		if existing.Resolved || existing.Stage == OpenCodeActionStageResolved {
			return nil
		}
		stage := existing.Stage
		if stage == "" {
			if existing.Pushed && len(existing.Outputs) > 0 {
				stage = OpenCodeActionStageOutputsReady
			} else if existing.Pushed {
				stage = OpenCodeActionStagePushed
			}
		}
		switch stage {
		case OpenCodeActionStageOutputsReady:
			if err := a.resolveRequest(callCtx, scanned, current, action, DecisionPass, existing.Outputs, ""); err != nil {
				return err
			}
			existing.Stage = OpenCodeActionStageResolved
			existing.Resolved = true
			existing.UpdatedAt = a.clock.Now()
			if err := a.store.SaveAction(context.WithoutCancel(callCtx), existing); err != nil {
				return fmt.Errorf("persist opencode resolved checkpoint: %w", err)
			}
			return nil
		case OpenCodeActionStagePushed:
			return a.finishTurnAfterPush(
				callCtx, scanned, current, action, subject,
				taskID, existing.SessionID, existing.Branch, existing.Candidate,
				existing.ResponseText, existing.Pushed,
			)
		}
	}

	// 5. Serialize concurrent turns for the same task so worktree/sandbox &
	// OpenCode session state are never mutated concurrently, and ONLY register
	// the turn with the local LLM proxy AFTER acquiring the task lock
	// (unregistering in defer before releasing the task lock).
	releaseTask, err := a.acquireTaskLock(callCtx, taskID)
	if err != nil {
		return err
	}
	defer releaseTask()

	turnRunCtx, cancelTurnRun := context.WithCancel(callCtx)
	defer cancelTurnRun()

	unregisterProxy := a.proxy.RegisterTurn(turnRunCtx, scanned.ExecutionID, scanned.ActionID)
	defer unregisterProxy()

	// 6. Fetch task spec/conversation and referenced KV blobs.
	var taskInfo *api.TaskReadResult
	if current.TaskID != "" {
		readTask, taskErr := a.readTask(turnRunCtx, current.TaskID)
		if taskErr == nil {
			taskInfo = &readTask
		}
	}
	var planContent, feedbackContent string
	if subject.PlanKey != "" {
		if obj, kvErr := a.kvGet(turnRunCtx, subject.PlanKey); kvErr == nil {
			planContent = truncateString(string(obj.Data), maxOpenCodePromptBlobBytes)
		}
	}
	if subject.FeedbackKey != "" {
		if obj, kvErr := a.kvGet(turnRunCtx, subject.FeedbackKey); kvErr == nil {
			feedbackContent = truncateString(string(obj.Data), maxOpenCodePromptBlobBytes)
		}
	}

	// 7. Prepare execution workspace:
	//   - Option 1 (worktree mode): isolated local per-task Git worktree.
	//   - Option 2 (brain mode): optional ephemeral per-task MCP sandbox
	//     (omitted for non-coding modes when no sandbox_image is requested).
	var checkout WorktreeCheckout
	var sandboxLease OpenCodeSandboxLease
	var hasSandbox bool
	var sandboxErrMu sync.Mutex
	var sandboxDeathErr error
	setSandboxDeath := func(err error) {
		sandboxErrMu.Lock()
		if sandboxDeathErr == nil {
			sandboxDeathErr = err
		}
		sandboxErrMu.Unlock()
	}
	getSandboxDeath := func() error {
		sandboxErrMu.Lock()
		defer sandboxErrMu.Unlock()
		return sandboxDeathErr
	}

	if a.executionMode == OpenCodeExecutionModeBrain {
		if subject.Repository != "" {
			checkout = WorktreeCheckout{
				Repository: subject.Repository,
				TaskID:     taskID,
				Branch:     "refs/heads/work/" + taskID,
			}
		}
		if a.needsSandbox(subject) {
			sandboxReq, reqErr := a.buildSandboxRequest(scanned, subject, taskID, remaining)
			if reqErr != nil {
				return reqErr
			}
			lease, acqErr := a.sandboxes.Acquire(turnRunCtx, sandboxReq)
			if acqErr != nil {
				if callCtx.Err() != nil {
					return nil
				}
				return fmt.Errorf("acquire ephemeral sandbox: %w", acqErr)
			}
			sandboxLease = lease
			hasSandbox = true
			turn.setSandboxLease(lease)
			defer turn.releaseSandbox(a.sandboxes)

			if lease.Done != nil {
				watchStop := make(chan struct{})
				defer close(watchStop)
				go func() {
					select {
					case <-lease.Done:
						var cause error
						if lease.Err != nil {
							cause = lease.Err()
						}
						if cause == nil {
							cause = ErrOpenCodeSandboxTerminated
						} else if !errors.Is(cause, ErrOpenCodeSandboxTerminated) {
							cause = fmt.Errorf("%w: %v", ErrOpenCodeSandboxTerminated, cause)
						}
						setSandboxDeath(cause)
						// Immediately cut off this turn's LLM proxy & in-flight
						// SendMessage and abort ONLY this task's Brain session.
						cancelTurnRun()
						turn.abortSession(a.opencode)
					case <-watchStop:
					case <-turnRunCtx.Done():
					}
				}()
			}
		}
	} else if subject.Repository != "" {
		if a.worktrees == nil {
			return fmt.Errorf("opencode request names repository %q but no worktree manager is configured", subject.Repository)
		}
		checkout, err = a.worktrees.Prepare(turnRunCtx, subject.Repository, taskID, subject.BaseRef)
		if err != nil {
			if callCtx.Err() != nil {
				return nil
			}
			return fmt.Errorf("prepare task worktree: %w", err)
		}
	}

	// 8. Reuse existing persistent OpenCode session for this task_id, or create
	// and persist a new one atomically before sending the turn prompt.
	sessionID, err := a.ensureTaskSession(turnRunCtx, taskID, taskInfo, subject, checkout)
	if err != nil {
		if deathErr := getSandboxDeath(); deathErr != nil && callCtx.Err() == nil {
			return a.handleSandboxFailure(callCtx, scanned, current, action, subject, taskID, "", deathErr)
		}
		if callCtx.Err() != nil {
			return nil
		}
		return err
	}
	turn.setSessionID(sessionID)
	if deathErr := getSandboxDeath(); deathErr != nil && callCtx.Err() == nil {
		turn.abortSession(a.opencode)
		return a.handleSandboxFailure(callCtx, scanned, current, action, subject, taskID, sessionID, deathErr)
	}
	if callCtx.Err() != nil {
		turn.cancelAndAbort(a.opencode)
		return nil
	}

	// 8b. In Brain mode with an active sandbox, attach the ephemeral sandbox
	// MCP server to the OpenCode session and detach it cleanly on exit.
	var mcpServers map[string]OpenCodeMCPServerConfig
	if hasSandbox {
		mcpCfg := OpenCodeMCPServerConfig{
			Name:    "sandbox",
			Type:    "remote",
			URL:     sandboxLease.EndpointURL,
			Headers: map[string]string{"Authorization": "Bearer " + sandboxLease.BearerToken},
			Enabled: true,
		}
		mcpServers = map[string]OpenCodeMCPServerConfig{mcpCfg.Name: mcpCfg}
		if mcpClient, ok := a.opencode.(OpenCodeMCPClient); ok {
			if attachErr := mcpClient.AttachMCPServer(turnRunCtx, sessionID, mcpCfg); attachErr != nil {
				if deathErr := getSandboxDeath(); deathErr != nil && callCtx.Err() == nil {
					turn.abortSession(a.opencode)
					return a.handleSandboxFailure(callCtx, scanned, current, action, subject, taskID, sessionID, deathErr)
				}
				if callCtx.Err() != nil {
					turn.cancelAndAbort(a.opencode)
					return nil
				}
				return fmt.Errorf("attach sandbox MCP server: %w", attachErr)
			}
			defer func() {
				_ = mcpClient.DetachMCPServer(context.WithoutCancel(callCtx), sessionID, mcpCfg.Name)
			}()
		}
	}

	// 9. Publish best-effort started progress event.
	a.publishProgress(turnRunCtx, scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "started", "")

	// 10. Build turn prompt and dispatch to OpenCode.
	prompt := buildOpenCodePrompt(subject, taskInfo, current, checkout, planContent, feedbackContent)
	msgReq := OpenCodeMessageRequest{
		Directory:   checkout.WorktreePath,
		Prompt:      prompt,
		Mode:        subject.Mode,
		Model:       a.model,
		Agent:       a.agent,
		ExecutionID: scanned.ExecutionID,
		ActionID:    scanned.ActionID,
		TaskID:      taskID,
	}
	if a.executionMode == OpenCodeExecutionModeBrain {
		// Brain mode never operates on a local filesystem directory; sandbox
		// workspace paths exist only inside the remote sandbox container.
		msgReq.Directory = ""
		if hasSandbox {
			msgReq.MCPServers = mcpServers
		}
		msgReq.DisabledTools = append([]string(nil), a.brain.DisabledLocalTools...)
	}
	if a.perTurnProxyRouting {
		msgReq.ProxyPathPrefix = fmt.Sprintf("/turn/%s/%s", scanned.ExecutionID, scanned.ActionID)
	}

	resp, err := a.opencode.SendMessage(turnRunCtx, sessionID, msgReq)
	if err != nil {
		if deathErr := getSandboxDeath(); deathErr != nil && callCtx.Err() == nil {
			turn.abortSession(a.opencode)
			return a.handleSandboxFailure(callCtx, scanned, current, action, subject, taskID, sessionID, deathErr)
		}
		if callCtx.Err() != nil {
			turn.cancelAndAbort(a.opencode)
			return nil
		}
		if errors.Is(err, ErrOpenCodeBudgetExceeded) {
			a.publishProgress(context.WithoutCancel(callCtx), scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "budget_exceeded", err.Error())
			note := truncateString("budget_exceeded: "+err.Error(), maxOpenCodeResolveNoteBytes)
			if resolveErr := a.resolveRequest(callCtx, scanned, current, action, "fail", nil, note); resolveErr == nil {
				if saveErr := a.store.SaveAction(context.WithoutCancel(callCtx), OpenCodeActionRecord{
					TenantID:    a.tenant,
					ProjectID:   a.project,
					ExecutionID: scanned.ExecutionID,
					ActionID:    scanned.ActionID,
					TaskID:      taskID,
					SessionID:   sessionID,
					Stage:       OpenCodeActionStageResolved,
					Resolved:    true,
					UpdatedAt:   a.clock.Now(),
				}); saveErr != nil {
					return fmt.Errorf("persist opencode budget-exceeded checkpoint: %w", saveErr)
				}
				return nil
			}
			return err
		}
		a.publishProgress(context.WithoutCancel(callCtx), scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "failed", err.Error())
		return err
	}
	if deathErr := getSandboxDeath(); deathErr != nil && callCtx.Err() == nil {
		turn.abortSession(a.opencode)
		return a.handleSandboxFailure(callCtx, scanned, current, action, subject, taskID, sessionID, deathErr)
	}
	if callCtx.Err() != nil {
		turn.cancelAndAbort(a.opencode)
		return nil
	}

	// 11. For coding modes, commit & push changes to refs/heads/work/<task-id>.
	var branch, candidate string
	var pushed bool
	if subject.Mode.MutatesCode() && a.executionMode == OpenCodeExecutionModeBrain && hasSandbox {
		commitMsg := fmt.Sprintf("opencode(%s): %s", subject.Mode, taskID)
		if taskInfo != nil && strings.TrimSpace(taskInfo.Title) != "" {
			commitMsg = fmt.Sprintf("opencode(%s): %s (%s)", subject.Mode, strings.TrimSpace(taskInfo.Title), taskID)
		}
		var pushRes WorktreePushResult
		var pushErr error
		if committer, ok := a.sandboxes.(OpenCodeSandboxCommitter); ok {
			pushRes, pushErr = committer.CommitAndPush(turnRunCtx, sandboxLease, subject.Repository, taskID, commitMsg)
		} else {
			pushRes, pushErr = CommitAndPushViaSandboxMCP(turnRunCtx, sandboxLease, subject.Repository, taskID, commitMsg, a.sandboxRT)
		}
		if pushErr != nil {
			if deathErr := getSandboxDeath(); deathErr != nil && callCtx.Err() == nil {
				turn.abortSession(a.opencode)
				return a.handleSandboxFailure(callCtx, scanned, current, action, subject, taskID, sessionID, deathErr)
			}
			if callCtx.Err() != nil {
				turn.cancelAndAbort(a.opencode)
				return nil
			}
			return fmt.Errorf("commit and push task branch from sandbox: %w", pushErr)
		}
		branch = pushRes.Branch
		candidate = pushRes.Candidate
		pushed = true

		if err := a.store.SaveAction(context.WithoutCancel(callCtx), OpenCodeActionRecord{
			TenantID:     a.tenant,
			ProjectID:    a.project,
			ExecutionID:  scanned.ExecutionID,
			ActionID:     scanned.ActionID,
			TaskID:       taskID,
			SessionID:    sessionID,
			Stage:        OpenCodeActionStagePushed,
			ResponseText: resp.Text,
			Branch:       branch,
			Candidate:    candidate,
			Pushed:       true,
			UpdatedAt:    a.clock.Now(),
		}); err != nil {
			return fmt.Errorf("persist opencode pushed checkpoint: %w", err)
		}
	} else if subject.Mode.MutatesCode() && checkout.WorktreePath != "" {
		commitMsg := fmt.Sprintf("opencode(%s): %s", subject.Mode, taskID)
		if taskInfo != nil && strings.TrimSpace(taskInfo.Title) != "" {
			commitMsg = fmt.Sprintf("opencode(%s): %s (%s)", subject.Mode, strings.TrimSpace(taskInfo.Title), taskID)
		}
		pushRes, pushErr := a.worktrees.CommitAndPush(turnRunCtx, checkout, commitMsg)
		if pushErr != nil {
			if callCtx.Err() != nil {
				turn.cancelAndAbort(a.opencode)
				return nil
			}
			return fmt.Errorf("commit and push task branch: %w", pushErr)
		}
		branch = pushRes.Branch
		candidate = pushRes.Candidate
		pushed = true

		// Persist pushed checkpoint immediately so a crash before KV/resolve
		// never double-pushes on retry (even when declared outputs are empty).
		if err := a.store.SaveAction(context.WithoutCancel(callCtx), OpenCodeActionRecord{
			TenantID:     a.tenant,
			ProjectID:    a.project,
			ExecutionID:  scanned.ExecutionID,
			ActionID:     scanned.ActionID,
			TaskID:       taskID,
			SessionID:    sessionID,
			Stage:        OpenCodeActionStagePushed,
			ResponseText: resp.Text,
			Branch:       branch,
			Candidate:    candidate,
			Pushed:       true,
			UpdatedAt:    a.clock.Now(),
		}); err != nil {
			return fmt.Errorf("persist opencode pushed checkpoint: %w", err)
		}
	} else if checkout.Branch != "" {
		branch = checkout.Branch
	}

	return a.finishTurnAfterPush(callCtx, scanned, current, action, subject, taskID, sessionID, branch, candidate, resp.Text, pushed)
}

func (a *OpenCodeAdapter) needsSandbox(subject OpenCodeSubject) bool {
	if subject.Mode.MutatesCode() {
		return true
	}
	return strings.TrimSpace(subject.SandboxImage) != ""
}

func (a *OpenCodeAdapter) buildSandboxRequest(
	scanned api.InboxItem,
	subject OpenCodeSubject,
	taskID string,
	timeout time.Duration,
) (OpenCodeSandboxRequest, error) {
	image := strings.TrimSpace(subject.SandboxImage)
	if image == "" {
		image = a.brain.DefaultSandboxImage
	}
	if image == "" {
		return OpenCodeSandboxRequest{}, fmt.Errorf("opencode brain mode requires subject.sandbox_image or --sandbox-default-image for mode %q", subject.Mode)
	}
	caches := subject.SandboxCaches
	if len(caches) == 0 && len(a.brain.DefaultSandboxCaches) > 0 {
		caches = append([]string(nil), a.brain.DefaultSandboxCaches...)
	}
	mem := strings.TrimSpace(subject.SandboxMemory)
	if mem == "" {
		mem = a.brain.DefaultSandboxMemory
	}
	cpu := strings.TrimSpace(subject.SandboxCPU)
	if cpu == "" {
		cpu = a.brain.DefaultSandboxCPU
	}
	workBranch := ""
	if subject.Repository != "" {
		workBranch = "refs/heads/work/" + taskID
	}
	return OpenCodeSandboxRequest{
		TenantID:    a.tenant,
		ProjectID:   a.project,
		TaskID:      taskID,
		ExecutionID: scanned.ExecutionID,
		ActionID:    scanned.ActionID,
		Image:       image,
		Repository:  subject.Repository,
		BaseRef:     subject.BaseRef,
		WorkBranch:  workBranch,
		Caches:      caches,
		Memory:      mem,
		CPU:         cpu,
		Port:        a.brain.SandboxPort,
		Timeout:     timeout,
	}, nil
}

func (a *OpenCodeAdapter) handleSandboxFailure(
	callCtx context.Context,
	scanned api.InboxItem,
	current api.ExecutionInspectResult,
	action api.ExecutionAction,
	subject OpenCodeSubject,
	taskID, sessionID string,
	deathErr error,
) error {
	a.publishProgress(context.WithoutCancel(callCtx), scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "sandbox_failed", deathErr.Error())
	note := truncateString("sandbox_terminated: "+deathErr.Error(), maxOpenCodeResolveNoteBytes)
	if resolveErr := a.resolveRequest(callCtx, scanned, current, action, "fail", nil, note); resolveErr == nil {
		if saveErr := a.store.SaveAction(context.WithoutCancel(callCtx), OpenCodeActionRecord{
			TenantID:    a.tenant,
			ProjectID:   a.project,
			ExecutionID: scanned.ExecutionID,
			ActionID:    scanned.ActionID,
			TaskID:      taskID,
			SessionID:   sessionID,
			Stage:       OpenCodeActionStageResolved,
			Resolved:    true,
			UpdatedAt:   a.clock.Now(),
		}); saveErr != nil {
			return fmt.Errorf("persist opencode sandbox-terminated checkpoint: %w", saveErr)
		}
		return nil
	}
	return deathErr
}

func (a *OpenCodeAdapter) finishTurnAfterPush(
	callCtx context.Context,
	scanned api.InboxItem,
	current api.ExecutionInspectResult,
	action api.ExecutionAction,
	subject OpenCodeSubject,
	taskID, sessionID, branch, candidate, responseText string,
	pushed bool,
) error {
	// 12. Write Markdown summary/plan/review to Kodema KV.
	markdown := formatOpenCodeMarkdown(subject.Mode, taskID, sessionID, branch, candidate, responseText)
	var kvKey string
	if needsKVOutput(scanned.Outputs) || strings.TrimSpace(responseText) != "" {
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
		TenantID:     a.tenant,
		ProjectID:    a.project,
		ExecutionID:  scanned.ExecutionID,
		ActionID:     scanned.ActionID,
		TaskID:       taskID,
		SessionID:    sessionID,
		Stage:        OpenCodeActionStageOutputsReady,
		ResponseText: responseText,
		Branch:       branch,
		Candidate:    candidate,
		SummaryKey:   kvKey,
		PlanKey:      subject.PlanKey,
		Outputs:      outputs,
		Pushed:       pushed,
		UpdatedAt:    a.clock.Now(),
	}
	if subject.Mode == OpenCodeModePlan && kvKey != "" {
		actionRecord.PlanKey = kvKey
	}
	if err := a.store.SaveAction(context.WithoutCancel(callCtx), actionRecord); err != nil {
		return fmt.Errorf("persist opencode outputs_ready checkpoint: %w", err)
	}

	// 14. Publish best-effort completion progress event.
	a.publishProgress(callCtx, scanned.ExecutionID, scanned.ActionID, taskID, sessionID, string(subject.Mode), "completed", candidate)

	// 15. Resolve the request idempotently with expected execution & action versions.
	if err := a.resolveRequest(callCtx, scanned, current, action, DecisionPass, outputs, ""); err != nil {
		return err
	}
	actionRecord.Stage = OpenCodeActionStageResolved
	actionRecord.Resolved = true
	actionRecord.UpdatedAt = a.clock.Now()
	if err := a.store.SaveAction(context.WithoutCancel(callCtx), actionRecord); err != nil {
		return fmt.Errorf("persist opencode resolved checkpoint: %w", err)
	}
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
	entry, ok := a.taskLocks[taskID]
	if !ok {
		entry = &openCodeTaskLockEntry{sem: make(chan struct{}, 1)}
		a.taskLocks[taskID] = entry
	}
	entry.refs++
	a.mu.Unlock()

	releaseRef := func() {
		a.mu.Lock()
		entry.refs--
		if entry.refs == 0 && a.taskLocks[taskID] == entry {
			delete(a.taskLocks, taskID)
		}
		a.mu.Unlock()
	}

	select {
	case entry.sem <- struct{}{}:
		return func() {
			<-entry.sem
			releaseRef()
		}, nil
	case <-ctx.Done():
		releaseRef()
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
	err := a.resolveRequestAttempt(ctx, scanned, current, action, decision, outputs, note)
	if !errors.Is(err, ErrStaleVersion) {
		return err
	}
	// Retry once by re-inspecting current execution/action versions so a
	// concurrent version increment resolves immediately instead of stalling.
	refreshed, inspectErr := a.inspectExecution(ctx, scanned.ExecutionID)
	if inspectErr != nil {
		var publicErr *api.Error
		if errors.As(inspectErr, &publicErr) && publicErr.Code == api.CodeNotFound {
			return nil
		}
		return err
	}
	if refreshed.ExecutionID != scanned.ExecutionID || refreshed.State != "running" {
		return nil
	}
	refreshedAction, ok := currentAction(refreshed, scanned.ActionID)
	if !ok || refreshedAction.Kind != "request" || refreshedAction.State != "running" || refreshedAction.Decision != "" {
		return nil
	}
	if refreshedAction.Assignment != "" && refreshedAction.Assignment != "service/"+string(a.serviceID) {
		return nil
	}
	return a.resolveRequestAttempt(ctx, scanned, refreshed, refreshedAction, decision, outputs, note)
}

func (a *OpenCodeAdapter) resolveRequestAttempt(
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
