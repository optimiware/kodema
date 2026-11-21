package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
)

type fixedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

type fakeOpenCodeClient struct {
	mu            sync.Mutex
	createCalls   int
	sendCalls     int
	abortCalls    []string
	abortHasDeadl bool
	sessionSeq    int
	onSend        func(ctx context.Context, sessionID string, req OpenCodeMessageRequest) (OpenCodeMessageResponse, error)
	onAbort       func(ctx context.Context, sessionID string) error
}

func (f *fakeOpenCodeClient) Health(context.Context) error { return nil }

func (f *fakeOpenCodeClient) CreateSession(_ context.Context, _, _ string) (OpenCodeSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCalls++
	f.sessionSeq++
	return OpenCodeSession{ID: fmt.Sprintf("oc-sess-%d", f.sessionSeq)}, nil
}

func (f *fakeOpenCodeClient) SendMessage(ctx context.Context, sessionID string, req OpenCodeMessageRequest) (OpenCodeMessageResponse, error) {
	f.mu.Lock()
	f.sendCalls++
	fn := f.onSend
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, sessionID, req)
	}
	return OpenCodeMessageResponse{Text: "implemented cleanly", MessageID: "msg-1"}, nil
}

func (f *fakeOpenCodeClient) AbortSession(ctx context.Context, sessionID string) error {
	f.mu.Lock()
	f.abortCalls = append(f.abortCalls, sessionID)
	_, hasDeadline := ctx.Deadline()
	f.abortHasDeadl = hasDeadline
	fn := f.onAbort
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, sessionID)
	}
	return nil
}

type fakeWorktreeManager struct {
	mu           sync.Mutex
	prepareCalls int
	pushCalls    int
	candidate    string
}

func (f *fakeWorktreeManager) Prepare(_ context.Context, repository, taskID, _ string) (WorktreeCheckout, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prepareCalls++
	return WorktreeCheckout{
		Repository:   repository,
		TaskID:       taskID,
		Branch:       "refs/heads/work/" + taskID,
		WorktreePath: "/tmp/wt/" + repository + "/" + taskID,
		BaseCommit:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}, nil
}

func (f *fakeWorktreeManager) CommitAndPush(_ context.Context, checkout WorktreeCheckout, _ string) (WorktreePushResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushCalls++
	cand := f.candidate
	if cand == "" {
		cand = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	}
	return WorktreePushResult{
		Branch:    checkout.Branch,
		Candidate: cand,
		Committed: true,
	}, nil
}

func (f *fakeWorktreeManager) Remove(context.Context, string, string) error { return nil }

type fakeOpenCodeHandler struct {
	mu              sync.Mutex
	inboxItems      []api.InboxItem
	executions      map[string]api.ExecutionInspectResult
	kvPutCalls      int
	resolveCalls    []api.ExecutionRequestResolveRequest
	resolveVersions []uint64
	staleOnce       bool
	staleTriggered  bool
	onResolve       func(req api.ExecutionRequestResolveRequest, expected *uint64) error
}

func (h *fakeOpenCodeHandler) Invoke(_ context.Context, inv api.Invocation) (api.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	encode := func(v any) api.Result {
		b, _ := json.Marshal(v)
		return api.Result{Operation: inv.Operation, Body: b}
	}

	switch inv.Operation {
	case OperationInbox:
		items := append([]api.InboxItem(nil), h.inboxItems...)
		return encode(api.TaskInboxResult{Items: items}), nil
	case OperationInspect:
		var req api.ExecutionInspectRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		exec, ok := h.executions[req.ExecutionID]
		if !ok {
			return api.Result{}, &api.Error{Code: api.CodeNotFound, Message: "not found"}
		}
		return encode(exec), nil
	case OperationTaskRead:
		var req api.TaskReadRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		return encode(api.TaskReadResult{
			TaskID:           req.TaskID,
			Title:            "Implement Feature",
			Description:      "Details",
			State:            "open",
			ContentVersion:   1,
			LifecycleVersion: 1,
			Initiator:        "admin-1",
		}), nil
	case OperationKVPut:
		h.kvPutCalls++
		var req api.KVPutRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		sum := sha256.Sum256([]byte(fmt.Sprintf("kv-%d", h.kvPutCalls)))
		key := api.KVKeyPrefix + hex.EncodeToString(sum[:])
		return encode(api.KVPutResult{
			Key:         key,
			ProjectID:   req.ProjectID,
			SizeBytes:   16,
			ContentType: req.ContentType,
			CreatedAt:   time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC),
		}), nil
	case OperationEventPublish:
		return encode(api.EventPublishResult{EventID: "evt-1"}), nil
	case OperationResolve:
		var req api.ExecutionRequestResolveRequest
		_ = json.Unmarshal(inv.Request.Input, &req)
		var exp uint64
		if inv.Request.ExpectedVersion != nil {
			exp = *inv.Request.ExpectedVersion
		}
		h.resolveCalls = append(h.resolveCalls, req)
		h.resolveVersions = append(h.resolveVersions, exp)
		if h.onResolve != nil {
			if err := h.onResolve(req, inv.Request.ExpectedVersion); err != nil {
				return api.Result{}, err
			}
		}
		if h.staleOnce && !h.staleTriggered {
			h.staleTriggered = true
			if exec, ok := h.executions[req.ExecutionID]; ok {
				exec.Version++
				h.executions[req.ExecutionID] = exec
			}
			return api.Result{}, &api.Error{Code: api.CodeVersionConflict, Message: "stale version"}
		}
		if exec, ok := h.executions[req.ExecutionID]; ok {
			for i := range exec.Actions {
				if exec.Actions[i].ActionID == req.ActionID {
					exec.Actions[i].Decision = req.Decision
					exec.Actions[i].State = "succeeded"
				}
			}
			h.executions[req.ExecutionID] = exec
		}
		return encode(api.ExecutionRequestResolveResult{
			ExecutionVersion: exp + 1,
			ActionVersion:    req.ExpectedActionVersion + 1,
		}), nil
	default:
		return api.Result{}, &api.Error{Code: api.CodeNotFound, Message: "unsupported op " + inv.Operation}
	}
}

type fakeEventStream struct {
	ch chan api.StreamEvent
}

func (s *fakeEventStream) StreamEvents(_ context.Context, _ client.EventStreamRequest) (client.EventStream, error) {
	return s, nil
}

func (s *fakeEventStream) Next(ctx context.Context) (api.StreamEvent, error) {
	select {
	case evt, ok := <-s.ch:
		if !ok {
			return api.StreamEvent{}, context.Canceled
		}
		return evt, nil
	case <-ctx.Done():
		return api.StreamEvent{}, ctx.Err()
	}
}

func (s *fakeEventStream) Close() error { return nil }

func makeImplementSubject(repo string) *api.Subject {
	return &api.Subject{
		Fields: []api.SubjectField{
			{Name: OpenCodeFieldMode, Kind: "string", Value: string(OpenCodeModeImplement)},
			{Name: OpenCodeFieldRepository, Kind: "string", Value: repo},
			{Name: OpenCodeFieldBaseRef, Kind: "string", Value: "refs/heads/main"},
		},
	}
}

func makeTestExecution(execID, actionID, taskID string, version uint64, deadline time.Time, subj *api.Subject) api.ExecutionInspectResult {
	return api.ExecutionInspectResult{
		ExecutionID:   execID,
		PipelineID:    "pipeline-1",
		TaskID:        taskID,
		State:         "running",
		Version:       version,
		CurrentAction: 0,
		Actions: []api.ExecutionAction{{
			ActionID:       actionID,
			Index:          0,
			Name:           "implement",
			Kind:           "request",
			State:          "running",
			Assignment:     "service/opencode-1",
			Version:        1,
			Deadline:       &deadline,
			TimeoutSeconds: 3600,
			Subject:        subj,
		}},
		Cleanup: api.ExecutionCleanup{State: "none"},
	}
}

func TestNewOpenCodeAdapterProxyConcurrencyEnforcement(t *testing.T) {
	store, err := NewFileOpenCodeStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileOpenCodeStateStore: %v", err)
	}
	proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:   "http://127.0.0.1:8080",
		Credentials: staticTestCred("kodsvc.opencode-1.tok"),
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	baseCfg := OpenCodeAdapterConfig{
		ServiceID: "opencode-1",
		TenantID:  "tenant-1",
		ProjectID: "project-1",
		Handler:   &fakeOpenCodeHandler{},
		OpenCode:  &fakeOpenCodeClient{},
		Store:     store,
		Proxy:     proxy,
		Clock:     &fixedClock{now: time.Now()},
	}

	// Default with shared static-URL proxy MUST enforce MaxConcurrentTurns = 1.
	a1, err := NewOpenCodeAdapter(baseCfg)
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter default with proxy: %v", err)
	}
	if a1.maxConcurrency != 1 {
		t.Fatalf("maxConcurrency = %d; want 1 with shared static-URL proxy", a1.maxConcurrency)
	}

	// Explicit MaxConcurrentTurns > 1 without PerTurnProxyRouting MUST fail!
	cfgBad := baseCfg
	cfgBad.MaxConcurrentTurns = 2
	if _, err := NewOpenCodeAdapter(cfgBad); err == nil {
		t.Fatal("NewOpenCodeAdapter allowed MaxConcurrentTurns=2 with shared static-URL proxy")
	}

	// Explicit PerTurnProxyRouting=true allows MaxConcurrentTurns > 1.
	cfgPerTurn := baseCfg
	cfgPerTurn.PerTurnProxyRouting = true
	cfgPerTurn.MaxConcurrentTurns = 3
	a3, err := NewOpenCodeAdapter(cfgPerTurn)
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter with PerTurnProxyRouting: %v", err)
	}
	if a3.maxConcurrency != 3 {
		t.Fatalf("maxConcurrency = %d; want 3", a3.maxConcurrency)
	}
}

func TestOpenCodeAdapterSessionReuseAcrossRestartsAndTaskLockCleanup(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)
	storeDir := t.TempDir()
	store1, err := NewFileOpenCodeStateStore(storeDir)
	if err != nil {
		t.Fatalf("NewFileOpenCodeStateStore: %v", err)
	}
	ocClient := &fakeOpenCodeClient{}
	wt := &fakeWorktreeManager{}
	handler := &fakeOpenCodeHandler{
		inboxItems: []api.InboxItem{{
			Kind:        "request",
			ExecutionID: "exec-1",
			ActionID:    "act-1",
			TaskID:      "task-persist-1",
			Assignment:  "service/opencode-1",
			Version:     1,
			At:          deadline,
			Subject:     makeImplementSubject("app"),
			Outputs:     []string{OpenCodeOutputBranch, OpenCodeOutputCandidate, OpenCodeOutputSummaryKey, OpenCodeOutputSessionID},
		}},
		executions: map[string]api.ExecutionInspectResult{
			"exec-1": makeTestExecution("exec-1", "act-1", "task-persist-1", 2, deadline, makeImplementSubject("app")),
		},
	}

	adapter1, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID: "opencode-1",
		TenantID:  "tenant-1",
		ProjectID: "project-1",
		Handler:   handler,
		OpenCode:  ocClient,
		Store:     store1,
		Worktrees: wt,
		Clock:     &fixedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter 1: %v", err)
	}
	if err := adapter1.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce turn 1: %v", err)
	}
	adapter1.mu.Lock()
	lockCount := len(adapter1.taskLocks)
	adapter1.mu.Unlock()
	if lockCount != 0 {
		t.Fatalf("adapter1.taskLocks retained %d idle entries; want 0", lockCount)
	}

	// Restart adapter with a brand-new store pointing at the same stateDir,
	// and enqueue turn 2 for the same task-persist-1.
	store2, err := NewFileOpenCodeStateStore(storeDir)
	if err != nil {
		t.Fatalf("NewFileOpenCodeStateStore 2: %v", err)
	}
	handler.inboxItems = []api.InboxItem{{
		Kind:        "request",
		ExecutionID: "exec-2",
		ActionID:    "act-2",
		TaskID:      "task-persist-1",
		Assignment:  "service/opencode-1",
		Version:     1,
		At:          deadline,
		Subject:     makeImplementSubject("app"),
		Outputs:     []string{OpenCodeOutputSessionID},
	}}
	handler.executions["exec-2"] = makeTestExecution("exec-2", "act-2", "task-persist-1", 1, deadline, makeImplementSubject("app"))
	adapter2, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID: "opencode-1",
		TenantID:  "tenant-1",
		ProjectID: "project-1",
		Handler:   handler,
		OpenCode:  ocClient,
		Store:     store2,
		Worktrees: wt,
		Clock:     &fixedClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter 2: %v", err)
	}
	if err := adapter2.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce turn 2: %v", err)
	}

	if ocClient.createCalls != 1 {
		t.Fatalf("CreateSession calls = %d; want 1 (reused across restart)", ocClient.createCalls)
	}
	if ocClient.sendCalls != 2 {
		t.Fatalf("SendMessage calls = %d; want 2", ocClient.sendCalls)
	}
	if len(handler.resolveCalls) != 2 || handler.resolveCalls[1].Outputs[OpenCodeOutputSessionID] != "oc-sess-1" {
		t.Fatalf("turn 2 did not reuse oc-sess-1: %+v", handler.resolveCalls)
	}
}

func TestOpenCodeAdapterCrashRecoveryZeroDoublePushAndCorruptFailClosed(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)

	t.Run("recover from pushed stage with zero declared outputs", func(t *testing.T) {
		store, _ := NewFileOpenCodeStateStore(t.TempDir())
		if err := store.SaveAction(context.Background(), OpenCodeActionRecord{
			TenantID:     "tenant-1",
			ProjectID:    "project-1",
			ExecutionID:  "exec-rec-1",
			ActionID:     "act-rec-1",
			TaskID:       "task-rec-1",
			SessionID:    "oc-sess-rec",
			Stage:        OpenCodeActionStagePushed,
			ResponseText: "done before crash",
			Branch:       "refs/heads/work/task-rec-1",
			Candidate:    "cccccccccccccccccccccccccccccccccccccccc",
			Pushed:       true,
			UpdatedAt:    now,
		}); err != nil {
			t.Fatalf("SaveAction seed: %v", err)
		}

		ocClient := &fakeOpenCodeClient{}
		wt := &fakeWorktreeManager{}
		handler := &fakeOpenCodeHandler{
			inboxItems: []api.InboxItem{{
				Kind:        "request",
				ExecutionID: "exec-rec-1",
				ActionID:    "act-rec-1",
				TaskID:      "task-rec-1",
				Assignment:  "service/opencode-1",
				Version:     1,
				At:          deadline,
				Subject:     makeImplementSubject("app"),
				Outputs:     nil, // ZERO declared outputs!
			}},
			executions: map[string]api.ExecutionInspectResult{
				"exec-rec-1": makeTestExecution("exec-rec-1", "act-rec-1", "task-rec-1", 1, deadline, makeImplementSubject("app")),
			},
		}

		adapter, _ := NewOpenCodeAdapter(OpenCodeAdapterConfig{
			ServiceID: "opencode-1",
			TenantID:  "tenant-1",
			ProjectID: "project-1",
			Handler:   handler,
			OpenCode:  ocClient,
			Store:     store,
			Worktrees: wt,
			Clock:     &fixedClock{now: now},
		})
		if err := adapter.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce recovery: %v", err)
		}
		if ocClient.sendCalls != 0 {
			t.Fatalf("SendMessage called %d times on pushed recovery; want 0", ocClient.sendCalls)
		}
		if wt.pushCalls != 0 {
			t.Fatalf("CommitAndPush called %d times on pushed recovery; want 0 (zero double-push)", wt.pushCalls)
		}
		if len(handler.resolveCalls) != 1 || handler.resolveCalls[0].Decision != DecisionPass {
			t.Fatalf("resolveCalls = %+v; want 1 pass resolution", handler.resolveCalls)
		}
		rec, found, err := store.LoadAction(context.Background(), "tenant-1", "project-1", "exec-rec-1", "act-rec-1")
		if err != nil || !found || !rec.Resolved || rec.Stage != OpenCodeActionStageResolved {
			t.Fatalf("post-recovery record = (%+v, found=%v, err=%v); want resolved", rec, found, err)
		}
	})

	t.Run("recover from outputs_ready stage skips KV and push", func(t *testing.T) {
		store, _ := NewFileOpenCodeStateStore(t.TempDir())
		if err := store.SaveAction(context.Background(), OpenCodeActionRecord{
			TenantID:    "tenant-1",
			ProjectID:   "project-1",
			ExecutionID: "exec-rec-2",
			ActionID:    "act-rec-2",
			TaskID:      "task-rec-2",
			SessionID:   "oc-sess-rec-2",
			Stage:       OpenCodeActionStageOutputsReady,
			Branch:      "refs/heads/work/task-rec-2",
			Candidate:   "dddddddddddddddddddddddddddddddddddddddd",
			Outputs:     map[string]string{"candidate": "dddddddddddddddddddddddddddddddddddddddd"},
			Pushed:      true,
			UpdatedAt:   now,
		}); err != nil {
			t.Fatalf("SaveAction seed: %v", err)
		}

		ocClient := &fakeOpenCodeClient{}
		wt := &fakeWorktreeManager{}
		handler := &fakeOpenCodeHandler{
			inboxItems: []api.InboxItem{{
				Kind:        "request",
				ExecutionID: "exec-rec-2",
				ActionID:    "act-rec-2",
				TaskID:      "task-rec-2",
				Assignment:  "service/opencode-1",
				Version:     1,
				At:          deadline,
				Subject:     makeImplementSubject("app"),
				Outputs:     []string{"candidate"},
			}},
			executions: map[string]api.ExecutionInspectResult{
				"exec-rec-2": makeTestExecution("exec-rec-2", "act-rec-2", "task-rec-2", 1, deadline, makeImplementSubject("app")),
			},
		}
		adapter, _ := NewOpenCodeAdapter(OpenCodeAdapterConfig{
			ServiceID: "opencode-1",
			TenantID:  "tenant-1",
			ProjectID: "project-1",
			Handler:   handler,
			OpenCode:  ocClient,
			Store:     store,
			Worktrees: wt,
			Clock:     &fixedClock{now: now},
		})
		if err := adapter.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce outputs_ready recovery: %v", err)
		}
		if ocClient.sendCalls != 0 || wt.pushCalls != 0 || handler.kvPutCalls != 0 {
			t.Fatalf("outputs_ready recovery called send=%d push=%d kvPut=%d; want all 0",
				ocClient.sendCalls, wt.pushCalls, handler.kvPutCalls)
		}
		if len(handler.resolveCalls) != 1 || handler.resolveCalls[0].Outputs["candidate"] != "dddddddddddddddddddddddddddddddddddddddd" {
			t.Fatalf("resolveCalls = %+v; want candidate output", handler.resolveCalls)
		}
	})

	t.Run("corrupt checkpoint fails closed without re-running LLM", func(t *testing.T) {
		storeDir := t.TempDir()
		store, _ := NewFileOpenCodeStateStore(storeDir)
		corruptPath := store.actionPath("tenant-1", "project-1", "exec-corrupt", "act-corrupt")
		if err := os.WriteFile(corruptPath, []byte("{broken json"), 0o600); err != nil {
			t.Fatalf("write corrupt file: %v", err)
		}
		ocClient := &fakeOpenCodeClient{}
		wt := &fakeWorktreeManager{}
		handler := &fakeOpenCodeHandler{
			inboxItems: []api.InboxItem{{
				Kind:        "request",
				ExecutionID: "exec-corrupt",
				ActionID:    "act-corrupt",
				TaskID:      "task-corrupt",
				Assignment:  "service/opencode-1",
				Version:     1,
				At:          deadline,
				Subject:     makeImplementSubject("app"),
			}},
			executions: map[string]api.ExecutionInspectResult{
				"exec-corrupt": makeTestExecution("exec-corrupt", "act-corrupt", "task-corrupt", 1, deadline, makeImplementSubject("app")),
			},
		}
		adapter, _ := NewOpenCodeAdapter(OpenCodeAdapterConfig{
			ServiceID: "opencode-1",
			TenantID:  "tenant-1",
			ProjectID: "project-1",
			Handler:   handler,
			OpenCode:  ocClient,
			Store:     store,
			Worktrees: wt,
			Clock:     &fixedClock{now: now},
		})
		if err := adapter.RunOnce(context.Background()); err == nil {
			t.Fatal("RunOnce succeeded on corrupt checkpoint; want fail-closed error")
		}
		if ocClient.sendCalls != 0 || wt.pushCalls != 0 {
			t.Fatalf("corrupt checkpoint ran send=%d push=%d; want 0", ocClient.sendCalls, wt.pushCalls)
		}
	})
}

func TestOpenCodeAdapterStaleVersionRetryAndBudgetExceededFail(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)

	t.Run("retries once immediately on ErrStaleVersion", func(t *testing.T) {
		store, _ := NewFileOpenCodeStateStore(t.TempDir())
		handler := &fakeOpenCodeHandler{
			staleOnce: true,
			inboxItems: []api.InboxItem{{
				Kind:        "request",
				ExecutionID: "exec-stale",
				ActionID:    "act-stale",
				TaskID:      "task-stale",
				Assignment:  "service/opencode-1",
				Version:     1,
				At:          deadline,
				Subject:     makeImplementSubject("app"),
				Outputs:     []string{OpenCodeOutputCandidate},
			}},
			executions: map[string]api.ExecutionInspectResult{
				"exec-stale": makeTestExecution("exec-stale", "act-stale", "task-stale", 5, deadline, makeImplementSubject("app")),
			},
		}
		adapter, _ := NewOpenCodeAdapter(OpenCodeAdapterConfig{
			ServiceID: "opencode-1",
			TenantID:  "tenant-1",
			ProjectID: "project-1",
			Handler:   handler,
			OpenCode:  &fakeOpenCodeClient{},
			Store:     store,
			Worktrees: &fakeWorktreeManager{},
			Clock:     &fixedClock{now: now},
		})
		if err := adapter.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce stale version retry failed: %v", err)
		}
		if len(handler.resolveVersions) != 2 || handler.resolveVersions[0] != 5 || handler.resolveVersions[1] != 6 {
			t.Fatalf("resolveVersions = %v; want [5 6]", handler.resolveVersions)
		}
	})

	t.Run("budget exceeded resolves fail and never pass", func(t *testing.T) {
		store, _ := NewFileOpenCodeStateStore(t.TempDir())
		ocClient := &fakeOpenCodeClient{
			onSend: func(context.Context, string, OpenCodeMessageRequest) (OpenCodeMessageResponse, error) {
				return OpenCodeMessageResponse{}, fmt.Errorf("%w: 429 limit hit", ErrOpenCodeBudgetExceeded)
			},
		}
		handler := &fakeOpenCodeHandler{
			inboxItems: []api.InboxItem{{
				Kind:        "request",
				ExecutionID: "exec-budget",
				ActionID:    "act-budget",
				TaskID:      "task-budget",
				Assignment:  "service/opencode-1",
				Version:     1,
				At:          deadline,
				Subject:     makeImplementSubject("app"),
			}},
			executions: map[string]api.ExecutionInspectResult{
				"exec-budget": makeTestExecution("exec-budget", "act-budget", "task-budget", 1, deadline, makeImplementSubject("app")),
			},
		}
		adapter, _ := NewOpenCodeAdapter(OpenCodeAdapterConfig{
			ServiceID: "opencode-1",
			TenantID:  "tenant-1",
			ProjectID: "project-1",
			Handler:   handler,
			OpenCode:  ocClient,
			Store:     store,
			Worktrees: &fakeWorktreeManager{},
			Clock:     &fixedClock{now: now},
		})
		if err := adapter.RunOnce(context.Background()); err != nil {
			t.Fatalf("RunOnce budget exceeded: %v", err)
		}
		if len(handler.resolveCalls) != 1 || handler.resolveCalls[0].Decision != "fail" {
			t.Fatalf("resolveCalls = %+v; want single fail decision", handler.resolveCalls)
		}
	})
}

func TestOpenCodeAdapterLiveSSECancellationAndProxyRegistrationAfterTaskLock(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	deadline := now.Add(time.Hour)
	store, _ := NewFileOpenCodeStateStore(t.TempDir())

	inSend := make(chan struct{})
	var ctxCancelledBeforeAbort atomic.Bool
	abortStarted := make(chan struct{})
	releaseAbort := make(chan struct{})

	ocClient := &fakeOpenCodeClient{
		onSend: func(ctx context.Context, _ string, _ OpenCodeMessageRequest) (OpenCodeMessageResponse, error) {
			close(inSend)
			<-ctx.Done()
			ctxCancelledBeforeAbort.Store(true)
			return OpenCodeMessageResponse{}, ctx.Err()
		},
		onAbort: func(ctx context.Context, _ string) error {
			close(abortStarted)
			<-releaseAbort
			return nil
		},
	}

	stream := &fakeEventStream{ch: make(chan api.StreamEvent, 4)}
	handler := &fakeOpenCodeHandler{
		inboxItems: []api.InboxItem{{
			Kind:        "request",
			ExecutionID: "exec-cancel-1",
			ActionID:    "act-cancel-1",
			TaskID:      "task-cancel-1",
			Assignment:  "service/opencode-1",
			Version:     1,
			At:          deadline,
			Subject:     makeImplementSubject("app"),
		}},
		executions: map[string]api.ExecutionInspectResult{
			"exec-cancel-1": makeTestExecution("exec-cancel-1", "act-cancel-1", "task-cancel-1", 1, deadline, makeImplementSubject("app")),
		},
	}

	proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:   "http://127.0.0.1:8080",
		Credentials: staticTestCred("kodsvc.opencode-1.tok"),
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}

	adapter, err := NewOpenCodeAdapter(OpenCodeAdapterConfig{
		ServiceID:      "opencode-1",
		TenantID:       "tenant-1",
		ProjectID:      "project-1",
		Handler:        handler,
		Stream:         stream,
		OpenCode:       ocClient,
		Store:          store,
		Worktrees:      &fakeWorktreeManager{},
		Proxy:          proxy,
		Clock:          &fixedClock{now: now},
		RescanInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeAdapter: %v", err)
	}

	// Verify RegisterTurn happens ONLY after acquireTaskLock succeeds:
	// Pre-hold the task lock before starting adapter.Run, confirm proxy has 0
	// turns while turn is blocked on task lock, then release lock -> turn starts!
	releasePreLock, err := adapter.acquireTaskLock(context.Background(), "task-cancel-1")
	if err != nil {
		t.Fatalf("acquireTaskLock pre-hold: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- adapter.Run(runCtx)
	}()

	time.Sleep(50 * time.Millisecond)
	proxy.mu.RLock()
	turnsWhileBlocked := len(proxy.turns)
	proxy.mu.RUnlock()
	if turnsWhileBlocked != 0 {
		t.Fatalf("proxy registered %d turns before task lock was acquired; want 0", turnsWhileBlocked)
	}

	// Release task lock -> turn proceeds into SendMessage.
	releasePreLock()
	select {
	case <-inSend:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SendMessage")
	}

	// Emit live SSE execution.cancelled event -> synchronously cancels turn ctx
	// and triggers bounded AbortSession tracked by abortWG.
	stream.ch <- api.StreamEvent{
		Type:      "execution.cancelled",
		SubjectID: "exec-cancel-1",
		Data:      json.RawMessage(`{"execution_id":"exec-cancel-1"}`),
	}
	select {
	case <-abortStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for AbortSession after SSE cancellation")
	}
	if !ctxCancelledBeforeAbort.Load() {
		// Give SendMessage goroutine a tiny window to observe synchronous cancel.
		deadlineWait := time.Now().Add(time.Second)
		for time.Now().Before(deadlineWait) && !ctxCancelledBeforeAbort.Load() {
			time.Sleep(5 * time.Millisecond)
		}
		if !ctxCancelledBeforeAbort.Load() {
			t.Fatal("turn context was not cancelled synchronously on SSE cancel")
		}
	}

	// Verify Run waits for in-flight AbortSession goroutine via abortWG!
	cancelRun()
	select {
	case <-runDone:
		t.Fatal("Run returned before tracked AbortSession goroutine finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseAbort)
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after AbortSession completed")
	}

	ocClient.mu.Lock()
	defer ocClient.mu.Unlock()
	if len(ocClient.abortCalls) != 1 || ocClient.abortCalls[0] != "oc-sess-1" {
		t.Fatalf("abortCalls = %v; want [oc-sess-1]", ocClient.abortCalls)
	}
	if !ocClient.abortHasDeadl {
		t.Fatal("AbortSession context must have a bounded deadline")
	}
}
