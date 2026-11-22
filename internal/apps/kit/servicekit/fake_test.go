package servicekit_test

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
)

// fakeClock is the injected time boundary. Every test controls it explicitly.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}

// resolveRecord is one observed resolve invocation.
type resolveRecord struct {
	operation string
	rawInput  json.RawMessage
	request   api.Request
	input     api.ExecutionRequestResolveRequest
}

var servedCodecs = func() map[string]api.OperationCodec {
	codecs := make(map[string]api.OperationCodec, len(api.OperationCodecs()))
	for _, codec := range api.OperationCodecs() {
		codecs[codec.Operation] = codec
	}
	return codecs
}()

type fakeHandler struct {
	log *callLog

	mu               sync.Mutex
	inboxFunc        func(api.TaskInboxRequest) (api.TaskInboxResult, error)
	inspectFunc      func(api.ExecutionInspectRequest) (api.ExecutionInspectResult, error)
	resolveFunc      func(api.ExecutionRequestResolveRequest, api.Request, int) error
	inboxInputs      []api.TaskInboxRequest
	inspectInputs    []api.ExecutionInspectRequest
	resolves         []resolveRecord
	resultOperation  string
	resultOperations map[string]string
	resultBodies     map[string]json.RawMessage
	resolved         chan struct{}
}

func (handler *fakeHandler) Invoke(_ context.Context, invocation api.Invocation) (api.Result, error) {
	if err := invocation.Validate(); err != nil {
		return api.Result{}, err
	}
	codec, ok := servedCodecs[invocation.Operation]
	if !ok {
		return api.Result{}, &api.Error{Code: api.CodeInvalidInput, Message: "unknown operation " + invocation.Operation}
	}
	decoded, err := codec.DecodeRequest(invocation.Request.Input)
	if err != nil {
		return api.Result{}, err
	}
	switch input := decoded.(type) {
	case api.TaskInboxRequest:
		handler.mu.Lock()
		handler.inboxInputs = append(handler.inboxInputs, input)
		inboxFunc := handler.inboxFunc
		handler.mu.Unlock()
		cursor := ""
		if input.Page != nil {
			cursor = input.Page.Cursor
		}
		handler.log.add("inbox:" + cursor)
		if inboxFunc == nil {
			return handler.respond(invocation.Operation, api.TaskInboxResult{})
		}
		result, err := inboxFunc(input)
		if err != nil {
			return api.Result{}, err
		}
		return handler.respond(invocation.Operation, result)

	case api.ExecutionInspectRequest:
		handler.mu.Lock()
		handler.inspectInputs = append(handler.inspectInputs, input)
		inspectFunc := handler.inspectFunc
		handler.mu.Unlock()
		handler.log.add("inspect:" + input.ExecutionID)
		if inspectFunc == nil {
			return api.Result{}, &api.Error{Code: api.CodeNotFound, Message: "no current request"}
		}
		result, err := inspectFunc(input)
		if err != nil {
			return api.Result{}, err
		}
		return handler.respond(invocation.Operation, result)

	case api.ExecutionRequestResolveRequest:
		if invocation.Request.IdempotencyKey == "" {
			return api.Result{}, &api.Error{Code: api.CodeInvalidInput, Message: "idempotency key is required"}
		}
		record := resolveRecord{
			operation: invocation.Operation,
			rawInput:  append(json.RawMessage(nil), invocation.Request.Input...),
			request:   invocation.Request,
			input:     input,
		}
		handler.mu.Lock()
		index := len(handler.resolves)
		handler.resolves = append(handler.resolves, record)
		resolveFunc := handler.resolveFunc
		resolved := handler.resolved
		handler.mu.Unlock()
		handler.log.add("resolve:" + invocation.Request.IdempotencyKey)
		var failure error
		if resolveFunc != nil {
			failure = resolveFunc(input, invocation.Request, index)
		}
		if resolved != nil {
			select {
			case resolved <- struct{}{}:
			default:
			}
		}
		if failure != nil {
			return api.Result{}, failure
		}
		executionVersion := uint64(1)
		if invocation.Request.ExpectedVersion != nil {
			executionVersion = *invocation.Request.ExpectedVersion + 1
		}
		return handler.respond(invocation.Operation, api.ExecutionRequestResolveResult{
			ExecutionVersion: executionVersion,
			ActionVersion:    input.ExpectedActionVersion + 1,
		})
	}
	return api.Result{}, &api.Error{Code: api.CodeInvalidInput, Message: "unsupported operation " + invocation.Operation}
}

func (handler *fakeHandler) respond(operation string, value any) (api.Result, error) {
	codec, ok := servedCodecs[operation]
	if !ok {
		return api.Result{}, &api.Error{Code: api.CodeInvalidInput, Message: "unknown operation " + operation}
	}
	encoded, err := codec.EncodeResult(value)
	if err != nil {
		return api.Result{}, err
	}
	handler.mu.Lock()
	identity := handler.resultOperation
	if override, ok := handler.resultOperations[operation]; ok {
		identity = override
	}
	body := encoded
	if replacement, ok := handler.resultBodies[operation]; ok {
		body = append(json.RawMessage(nil), replacement...)
	}
	handler.mu.Unlock()
	if identity == "" {
		identity = operation
	}
	return api.Result{Operation: identity, Body: body}, nil
}

func (handler *fakeHandler) inboxCalls() []api.TaskInboxRequest {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return append([]api.TaskInboxRequest(nil), handler.inboxInputs...)
}

func (handler *fakeHandler) inspectCalls() []api.ExecutionInspectRequest {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return append([]api.ExecutionInspectRequest(nil), handler.inspectInputs...)
}

func (handler *fakeHandler) resolvesSnapshot() []resolveRecord {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return append([]resolveRecord(nil), handler.resolves...)
}

func (handler *fakeHandler) resolveCount() int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return len(handler.resolves)
}

type fakeEventSource struct {
	mu      sync.Mutex
	pages   map[string]servicekit.NotificationPage
	cursors []string
	err     error
}

func (source *fakeEventSource) Poll(_ context.Context, cursor string) (servicekit.NotificationPage, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.cursors = append(source.cursors, cursor)
	if source.err != nil {
		return servicekit.NotificationPage{}, source.err
	}
	return source.pages[cursor], nil
}

func (source *fakeEventSource) polledCursors() []string {
	source.mu.Lock()
	defer source.mu.Unlock()
	return append([]string(nil), source.cursors...)
}
