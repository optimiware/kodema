package workspace_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func TestEnvironmentMergesLiteralEnvThenReservedContext(t *testing.T) {
	request := testPrepareRequest()
	got, err := workspace.Environment(request, "https://kodema.example")
	if err != nil {
		t.Fatalf("Environment: %v", err)
	}
	want := []string{
		"B=2",
		"A=1",
		"KODEMA_URL=https://kodema.example",
		"KODEMA_EXECUTION=exec-1",
		"KODEMA_ACTION=build",
		"KODEMA_TASK=task-1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("environment = %v; want %v", got, want)
	}
}

func TestEnvironmentOmitsTaskWhenTaskFree(t *testing.T) {
	request := testPrepareRequest()
	request.TaskID = ""
	request.Task = nil
	got, err := workspace.Environment(request, "https://kodema.example")
	if err != nil {
		t.Fatalf("Environment: %v", err)
	}
	want := []string{
		"B=2",
		"A=1",
		"KODEMA_URL=https://kodema.example",
		"KODEMA_EXECUTION=exec-1",
		"KODEMA_ACTION=build",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("environment = %v; want %v", got, want)
	}
	for _, entry := range got {
		if strings.HasPrefix(entry, id.EnvTask+"=") {
			t.Fatalf("task-free environment contains %q", entry)
		}
	}
}

func TestEnvironmentRefusesReservedShadowAndMissingContext(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*reconcile.PrepareRequest)
		url    string
	}{
		{
			name: "literal shadows reserved URL",
			mutate: func(request *reconcile.PrepareRequest) {
				request.Container.Env = append(request.Container.Env, definition.EnvVar{Name: id.EnvName(id.EnvURL), Value: "spoofed"})
			},
			url: "https://kodema.example",
		},
		{
			name: "empty URL",
			mutate: func(*reconcile.PrepareRequest) {
			},
			url: "",
		},
		{
			name: "empty execution id",
			mutate: func(request *reconcile.PrepareRequest) {
				request.Identity.ExecutionID = ""
			},
			url: "https://kodema.example",
		},
		{
			name: "empty action slug",
			mutate: func(request *reconcile.PrepareRequest) {
				request.ActionName = ""
			},
			url: "https://kodema.example",
		},
		{
			name: "invalid action slug",
			mutate: func(request *reconcile.PrepareRequest) {
				request.ActionName = id.ActionName("Not-A-Slug")
			},
			url: "https://kodema.example",
		},
		{
			name: "invalid task id",
			mutate: func(request *reconcile.PrepareRequest) {
				request.TaskID = id.TaskID("bad/name")
			},
			url: "https://kodema.example",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := testPrepareRequest()
			testCase.mutate(&request)
			_, err := workspace.Environment(request, testCase.url)
			requireClass(t, err, fault.InvalidInput)
		})
	}
}

func TestEnvironmentDoesNotLeakSecretsOrCredential(t *testing.T) {
	request := testPrepareRequest()
	request.Secrets = []reconcile.SecretValue{{Name: "DB_PASSWORD", Value: "secret-value"}}
	request.Credential = "kodex-credential-token"

	got, err := workspace.Environment(request, "https://kodema.example")
	if err != nil {
		t.Fatalf("Environment: %v", err)
	}
	joined := strings.Join(got, "\n")
	if strings.Contains(joined, "secret-value") {
		t.Fatal("environment contains a secret value")
	}
	if strings.Contains(joined, "kodex-credential-token") {
		t.Fatal("environment contains the execution credential")
	}
	if strings.Contains(joined, id.SecretTokenName+"=") {
		t.Fatal("environment contains a KODEMA_TOKEN entry; the credential is a file")
	}
}
