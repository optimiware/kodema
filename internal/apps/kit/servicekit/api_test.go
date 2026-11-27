package servicekit_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
)

// Compile-time pins for the shared servicekit surface.
var (
	_ servicekit.ServiceAdapter = (*servicekit.Adapter)(nil)
	_ servicekit.EventSource    = (*fakeEventSource)(nil)
	_ servicekit.Clock          = (*fakeClock)(nil)
	_ api.Handler               = (*fakeHandler)(nil)
)

func TestAssignmentValidation(t *testing.T) {
	assignment := validAssignment()
	if err := assignment.Validate(); err != nil {
		t.Fatalf("valid assignment refused: %v", err)
	}

	human := validAssignment()
	human.Request = "role/reviewer"
	human.ServiceID = ""
	if err := human.Validate(); err != nil {
		t.Fatalf("valid human-role assignment refused: %v", err)
	}

	for name, invalid := range map[string]servicekit.Assignment{
		"missing tenant":       {ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, At: testNow},
		"invalid tenant":       {TenantID: "../escape", ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, At: testNow},
		"missing project":      {TenantID: testTenant, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, At: testNow},
		"missing execution":    {TenantID: testTenant, ProjectID: testProject, ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, At: testNow},
		"missing action":       {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, At: testNow},
		"missing service":      {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", Request: "service/email-svc", Version: 1, At: testNow},
		"zero version":         {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", At: testNow},
		"missing time":         {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1},
		"bad assignee":         {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "email-svc", Version: 1, At: testNow},
		"empty assignee name":  {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/", Version: 1, At: testNow},
		"unknown role":         {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "role/robot", Version: 1, At: testNow},
		"service as role":      {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "role/email-svc", Version: 1, At: testNow},
		"invalid source event": {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, SourceEventID: "event/../1", At: testNow},
		"non-object payload":   {TenantID: testTenant, ProjectID: testProject, ExecutionID: "exec-1", ActionID: "action-1", ServiceID: "email-svc", Request: "service/email-svc", Version: 1, At: testNow, Payload: json.RawMessage(`[1,2]`)},
	} {
		t.Run(name, func(t *testing.T) {
			if err := invalid.Validate(); err == nil {
				t.Fatalf("Assignment.Validate accepted %+v", invalid)
			}
		})
	}
}

func TestAdapterConfigRejectsIncompleteConfiguration(t *testing.T) {
	base := servicekit.AdapterConfig{
		Kind:      servicekit.KindEmail,
		ServiceID: testService,
		TenantID:  testTenant,
		ProjectID: testProject,
		Handler:   &fakeHandler{log: &callLog{}},
		Events:    &fakeEventSource{},
		Clock:     &fakeClock{now: testNow},
	}
	if _, err := servicekit.NewAdapter(base); err != nil {
		t.Fatalf("NewAdapter with a complete config: %v", err)
	}

	for name, mutate := range map[string]func(*servicekit.AdapterConfig){
		"invalid kind":         func(config *servicekit.AdapterConfig) { config.Kind = "artifact_publisher" },
		"missing service id":   func(config *servicekit.AdapterConfig) { config.ServiceID = "" },
		"invalid service id":   func(config *servicekit.AdapterConfig) { config.ServiceID = "svc/../1" },
		"invalid tenant":       func(config *servicekit.AdapterConfig) { config.TenantID = "../escape" },
		"missing project":      func(config *servicekit.AdapterConfig) { config.ProjectID = "" },
		"missing handler":      func(config *servicekit.AdapterConfig) { config.Handler = nil },
		"missing events":       func(config *servicekit.AdapterConfig) { config.Events = nil },
		"missing clock":        func(config *servicekit.AdapterConfig) { config.Clock = nil },
		"negative rescan":      func(config *servicekit.AdapterConfig) { config.RescanInterval = -time.Second },
		"timer without period": func(config *servicekit.AdapterConfig) { config.Kind = servicekit.KindTimer },
		"merge without hook":   func(config *servicekit.AdapterConfig) { config.Kind = servicekit.KindMergeCoordinator },
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			if _, err := servicekit.NewAdapter(config); err == nil {
				t.Fatalf("NewAdapter accepted a config with %s", name)
			}
		})
	}

	timer := base
	timer.Kind = servicekit.KindTimer
	timer.Schedule = time.Minute
	if _, err := servicekit.NewAdapter(timer); err != nil {
		t.Fatalf("NewAdapter refused a timer with a schedule: %v", err)
	}

	mergeHook := base
	mergeHook.Kind = servicekit.KindMergeCoordinator
	mergeHook.HandleRequest = func(context.Context, api.InboxItem, api.ExecutionInspectResult, api.ExecutionAction, servicekit.RequestResolver) error {
		return nil
	}
	if _, err := servicekit.NewAdapter(mergeHook); err != nil {
		t.Fatalf("NewAdapter refused a merge coordinator with a request handler: %v", err)
	}
}
