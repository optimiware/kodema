package servicekit_test

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/id"
)

// The single deterministic clock instant every fixture derives from.
var testNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

var (
	testTenant  = id.TenantID("tenant-a")
	testProject = id.ProjectID("project-a")
	testService = id.PrincipalID("email-svc")
)

// callLog is the shared, ordered record of boundary interactions.
type callLog struct {
	mu      sync.Mutex
	entries []string
}

func (log *callLog) add(entry string) {
	if log == nil {
		return
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	log.entries = append(log.entries, entry)
}

func validAssignment() servicekit.Assignment {
	return servicekit.Assignment{
		TenantID:      testTenant,
		ProjectID:     testProject,
		ExecutionID:   "exec-1",
		ActionID:      "action-1",
		ServiceID:     testService,
		Request:       "service/email-svc",
		Version:       1,
		SourceEventID: "event-1",
		At:            testNow,
		Payload:       json.RawMessage(`{"summary":"ready"}`),
	}
}

// servedRequest is one request action as the served public DTOs expose it.
type servedRequest struct {
	executionID      string
	actionID         string
	assignment       string
	executionVersion uint64
	actionVersion    uint64
	at               time.Time
	state            string
	decision         string
	actor            string
	answeredAt       *time.Time
}

func validServedRequest() servedRequest {
	return servedRequest{
		executionID:      "exec-1",
		actionID:         "action-1",
		assignment:       "service/" + string(testService),
		executionVersion: 1,
		actionVersion:    1,
		at:               testNow,
		state:            "running",
	}
}

func (request servedRequest) inboxItem() api.InboxItem {
	return api.InboxItem{
		Kind:        "request",
		ExecutionID: request.executionID,
		ActionID:    request.actionID,
		Assignment:  request.assignment,
		Subject:     &api.Subject{Fields: []api.SubjectField{{Name: "seed", Kind: "string", Value: "seed-1"}}},
		Version:     request.actionVersion,
		At:          request.at,
	}
}

func (request servedRequest) inspectResult() api.ExecutionInspectResult {
	state := request.state
	if state == "" {
		state = "running"
	}
	return api.ExecutionInspectResult{
		ExecutionID:   request.executionID,
		PipelineID:    "pipeline-1",
		State:         "running",
		Version:       request.executionVersion,
		CurrentAction: 0,
		Actions: []api.ExecutionAction{{
			ActionID:       request.actionID,
			Index:          0,
			Name:           "answer",
			Kind:           "request",
			State:          state,
			Version:        request.actionVersion,
			Decision:       request.decision,
			Actor:          request.actor,
			AnsweredAt:     request.answeredAt,
			TimeoutSeconds: int64((time.Hour) / time.Second),
		}},
		Cleanup: api.ExecutionCleanup{State: "none"},
	}
}

func validAdapterConfig(handler api.Handler, events servicekit.EventSource, clock servicekit.Clock) servicekit.AdapterConfig {
	return servicekit.AdapterConfig{
		Kind:      servicekit.KindEmail,
		ServiceID: testService,
		TenantID:  testTenant,
		ProjectID: testProject,
		Handler:   handler,
		Events:    events,
		Clock:     clock,
	}
}
