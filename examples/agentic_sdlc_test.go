package examples_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/apps/kit/sandbox"
	"github.com/optimiware/kodema/internal/apps/merge"
	"github.com/optimiware/kodema/internal/apps/opencode"
	"github.com/optimiware/kodema/internal/control/identity"
	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	kernelengine "github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/installation"
	shellapp "github.com/optimiware/kodema/internal/server"
)

// requiredAgenticSDLCPipelines is the exact manifest of pipeline YAMLs shipped
// under examples/agentic-sdlc/ (excluding the Helm values snippet).
var requiredAgenticSDLCPipelines = []string{
	"pattern-a-implement.yaml",
	"pattern-a-verify-review.yaml",
	"pattern-a-merge.yaml",
	"pattern-b-worktree-sdlc.yaml",
	"pattern-c-brain-polyglot.yaml",
}

const agenticSDLCBrainValuesFile = "opencode-brain-values.yaml"

// TestAgenticSDLCExamplePipelinesValidate proves every shipped Agentic SDLC
// pipeline under examples/agentic-sdlc/ decodes cleanly through Kodema's real
// admission parser, pins container images, uses real kodema CLI subcommands,
// and satisfies the OpenCode and merge-coordinator request subject contracts.
func TestAgenticSDLCExamplePipelinesValidate(t *testing.T) {
	allYAML := sortedExampleYAML(t, "agentic-sdlc")
	var pipelines []string
	foundBrainValues := false
	for _, name := range allYAML {
		if name == agenticSDLCBrainValuesFile {
			foundBrainValues = true
			continue
		}
		pipelines = append(pipelines, name)
	}
	if !foundBrainValues {
		t.Fatalf("examples/agentic-sdlc is missing %s", agenticSDLCBrainValuesFile)
	}
	requireSameExampleInventory(t, "examples/agentic-sdlc pipelines", pipelines, requiredAgenticSDLCPipelines)

	for _, name := range pipelines {
		rel := "agentic-sdlc/" + name
		t.Run(name, func(t *testing.T) {
			decoded := decodeExample(t, rel)
			if len(decoded.Actions) == 0 {
				t.Fatalf("%s declares no actions", rel)
			}
			assertPinnedImages(t, rel, decoded)
			for _, action := range decoded.Actions {
				if action.Request != nil {
					assertAssignment(t, rel, action)
				}
				if action.Container != nil {
					for _, command := range action.Container.Commands {
						for _, match := range kodemaInvocation.FindAllStringSubmatch(command, -1) {
							subcommand := match[1]
							if !realKodemaSubcommand(t, subcommand) {
								t.Fatalf("%s action %q invokes `kodema %s`; the real CLI has no %q subcommand", rel, action.Name, subcommand, subcommand)
							}
						}
					}
				}
			}
		})
	}

	t.Run("pattern-a contracts", func(t *testing.T) {
		impl := decodeExample(t, "agentic-sdlc/pattern-a-implement.yaml")
		if impl.Budget == nil || impl.Budget.MaxTokens <= 0 || impl.Budget.MaxCostUSD <= 0 {
			t.Fatalf("pattern-a-implement.yaml must declare pipeline budget max_tokens and max_cost_usd")
		}
		assertKinds(t, "agentic-sdlc/pattern-a-implement.yaml", impl, "container")
		act := impl.Actions[0].Container
		if len(act.Caches) != 2 || act.Caches[0] != "go-mod" || act.Caches[1] != "go-build" {
			t.Fatalf("pattern-a-implement.yaml caches = %v; want [go-mod go-build]", act.Caches)
		}
		if act.Budget == nil || act.Budget.MaxTokens <= 0 {
			t.Fatalf("pattern-a-implement.yaml implement action must declare action budget")
		}
		cmds := strings.Join(act.Commands, "\n")
		for _, want := range []string{"kodema harness exec --", "kodema kv get", "kodema kv put"} {
			if !strings.Contains(cmds, want) {
				t.Fatalf("pattern-a-implement.yaml must demonstrate %q", want)
			}
		}

		verifyReview := decodeExample(t, "agentic-sdlc/pattern-a-verify-review.yaml")
		assertKinds(t, "agentic-sdlc/pattern-a-verify-review.yaml", verifyReview, "container", "request")
		if verifyReview.Actions[1].Request.Assignment != "role/reviewer" {
			t.Fatalf("pattern-a-verify-review.yaml review assignment = %q; want role/reviewer", verifyReview.Actions[1].Request.Assignment)
		}

		merge := decodeExample(t, "agentic-sdlc/pattern-a-merge.yaml")
		assertKinds(t, "agentic-sdlc/pattern-a-merge.yaml", merge, "request")
		validateMergeActionContract(t, "agentic-sdlc/pattern-a-merge.yaml", merge.Actions[0], true)
	})

	t.Run("pattern-b worktree opencode contracts", func(t *testing.T) {
		decoded := decodeExample(t, "agentic-sdlc/pattern-b-worktree-sdlc.yaml")
		assertKinds(t, "agentic-sdlc/pattern-b-worktree-sdlc.yaml", decoded, "request", "request", "container", "request", "request")
		planSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-b-worktree-sdlc.yaml", decoded.Actions[0])
		if planSubj.Mode != opencode.OpenCodeModePlan {
			t.Fatalf("plan mode = %q; want plan", planSubj.Mode)
		}
		implSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-b-worktree-sdlc.yaml", decoded.Actions[1])
		if implSubj.Mode != opencode.OpenCodeModeImplement || implSubj.PlanKey == "" {
			t.Fatalf("implement subject = %+v; want mode=implement with plan_key", implSubj)
		}
		aiRevSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-b-worktree-sdlc.yaml", decoded.Actions[3])
		if aiRevSubj.Mode != opencode.OpenCodeModeAIReview {
			t.Fatalf("ai-review mode = %q; want ai_review", aiRevSubj.Mode)
		}
	})

	t.Run("pattern-c brain polyglot opencode contracts", func(t *testing.T) {
		decoded := decodeExample(t, "agentic-sdlc/pattern-c-brain-polyglot.yaml")
		assertKinds(t, "agentic-sdlc/pattern-c-brain-polyglot.yaml", decoded, "request", "request", "request", "request", "request")

		planSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-c-brain-polyglot.yaml", decoded.Actions[0])
		if planSubj.Mode != opencode.OpenCodeModePlan || planSubj.SandboxImage != "" {
			t.Fatalf("brain plan turn must be sandbox-free (mode=plan, sandbox_image empty); got %+v", planSubj)
		}

		goSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-c-brain-polyglot.yaml", decoded.Actions[1])
		if goSubj.SandboxImage != "docker.io/library/golang:1.24" || len(goSubj.SandboxCaches) != 2 {
			t.Fatalf("backend sandbox subject = %+v; want golang:1.24 with 2 caches", goSubj)
		}

		nodeSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-c-brain-polyglot.yaml", decoded.Actions[2])
		if nodeSubj.SandboxImage != "docker.io/library/node:22" || len(nodeSubj.SandboxCaches) != 1 || nodeSubj.SandboxCaches[0] != "npm" {
			t.Fatalf("frontend sandbox subject = %+v; want node:22 with [npm] cache", nodeSubj)
		}

		aiRevSubj := validateOpenCodeActionContract(t, "agentic-sdlc/pattern-c-brain-polyglot.yaml", decoded.Actions[3])
		if aiRevSubj.Mode != opencode.OpenCodeModeAIReview || aiRevSubj.SandboxImage != "" {
			t.Fatalf("brain ai-review turn must be sandbox-free; got %+v", aiRevSubj)
		}
	})

	t.Run("helm opencodeBrain values snippet", func(t *testing.T) {
		raw := readExample(t, "agentic-sdlc/"+agenticSDLCBrainValuesFile)
		var parsed struct {
			Tenants       []string `yaml:"tenants"`
			OpenCodeBrain struct {
				Enabled     bool `yaml:"enabled"`
				SandboxPort int  `yaml:"sandboxPort"`
				Projects    []struct {
					Name        string `yaml:"name"`
					Tenant      string `yaml:"tenant"`
					Project     string `yaml:"project"`
					Namespace   string `yaml:"namespace"`
					SandboxPort int    `yaml:"sandboxPort"`
				} `yaml:"projects"`
			} `yaml:"opencodeBrain"`
		}
		if err := yaml.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("unmarshal %s: %v", agenticSDLCBrainValuesFile, err)
		}
		if !parsed.OpenCodeBrain.Enabled || len(parsed.OpenCodeBrain.Projects) == 0 {
			t.Fatalf("%s must enable opencodeBrain with at least one project entry", agenticSDLCBrainValuesFile)
		}
		if parsed.OpenCodeBrain.SandboxPort < 1024 || parsed.OpenCodeBrain.SandboxPort > 65535 {
			t.Fatalf("%s sandboxPort = %d; want 1024..65535", agenticSDLCBrainValuesFile, parsed.OpenCodeBrain.SandboxPort)
		}
		tenantSet := make(map[string]bool, len(parsed.Tenants))
		for _, tenant := range parsed.Tenants {
			tenantSet[tenant] = true
		}
		for _, entry := range parsed.OpenCodeBrain.Projects {
			if !tenantSet[entry.Tenant] {
				t.Fatalf("%s project %q references tenant %q not in tenants %v", agenticSDLCBrainValuesFile, entry.Name, entry.Tenant, parsed.Tenants)
			}
			if entry.Project == "" || entry.Name == "" {
				t.Fatalf("%s project entry missing name or project: %+v", agenticSDLCBrainValuesFile, entry)
			}
		}

		validateHelmValuesOverlayAgainstSchema(
			t,
			raw,
			filepath.Join("..", "deploy", "charts", "kodema", "values.schema.json"),
		)
	})
}

func validateOpenCodeActionContract(t *testing.T, rel string, action definition.Action) opencode.OpenCodeSubject {
	t.Helper()
	if action.Request == nil {
		t.Fatalf("%s action %q is not a request action", rel, action.Name)
	}
	sampleValues := map[string]string{
		"repository":     "shop/web",
		"base_ref":       "refs/heads/main",
		"plan_key":       "kv:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"feedback_key":   "kv:sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		"sandbox_image":  "docker.io/library/golang:1.24",
		"sandbox_caches": "go-mod,go-build",
		"sandbox_memory": "2Gi",
		"sandbox_cpu":    "2",
	}
	resolved := &api.Subject{Fields: make([]api.SubjectField, 0, action.Request.Subject.Len())}
	for name, val := range action.Request.Subject.Fields() {
		v := val.Literal
		if val.Kind == definition.SubjectReference {
			v = sampleValues[name]
			if v == "" {
				v = "sample-" + name
			}
		}
		resolved.Fields = append(resolved.Fields, api.SubjectField{
			Name:  name,
			Kind:  "string",
			Value: v,
		})
	}
	parsed, err := opencode.ParseOpenCodeSubject(resolved)
	if err != nil {
		t.Fatalf("%s action %q subject failed ParseOpenCodeSubject: %v", rel, action.Name, err)
	}
	return parsed
}

func validateMergeActionContract(t *testing.T, rel string, action definition.Action, requireDecisionID bool) {
	t.Helper()
	if action.Request == nil {
		t.Fatalf("%s action %q is not a request action", rel, action.Name)
	}
	sampleValues := map[string]string{
		"repository":  "shop/web",
		"ref":         "refs/heads/main",
		"base":        "1111111111111111111111111111111111111111",
		"candidate":   "2222222222222222222222222222222222222222",
		"decision_id": "decision-0123456789abcdef0123456789abcdef",
	}
	resolved := &api.Subject{Fields: make([]api.SubjectField, 0, action.Request.Subject.Len())}
	for name, val := range action.Request.Subject.Fields() {
		v := val.Literal
		if val.Kind == definition.SubjectReference {
			v = sampleValues[name]
		}
		resolved.Fields = append(resolved.Fields, api.SubjectField{
			Name:  name,
			Kind:  "string",
			Value: v,
		})
	}
	parsed, err := merge.ParseMergeSubject(resolved)
	if err != nil {
		t.Fatalf("%s action %q subject failed ParseMergeSubject: %v", rel, action.Name, err)
	}
	if requireDecisionID && parsed.DecisionID == "" {
		t.Fatalf("%s action %q must carry decision_id in merge subject", rel, action.Name)
	}
	outputs := make([]string, len(action.Request.Outputs))
	for i, k := range action.Request.Outputs {
		outputs[i] = string(k)
	}
	if err := merge.ValidateMergeOutputs(outputs); err != nil {
		t.Fatalf("%s action %q outputs %v failed ValidateMergeOutputs: %v", rel, action.Name, outputs, err)
	}
}

// sdlcHermeticRuntime is a minimal in-process Kernel Runtime/Workspace that
// simulates container actions settling outputs over the real public API so
// multi-stage SDLC pipelines execute hermetically with zero external opencode.
type sdlcHermeticRuntime struct {
	mu      sync.Mutex
	started map[id.WorkloadID]bool
	byID    map[id.WorkloadID]kernelengine.PrepareRequest
	onStart func(req kernelengine.StartRequest, prep kernelengine.PrepareRequest) error
}

func newSDLCHermeticRuntime() *sdlcHermeticRuntime {
	return &sdlcHermeticRuntime{
		started: make(map[id.WorkloadID]bool),
		byID:    make(map[id.WorkloadID]kernelengine.PrepareRequest),
	}
}

var (
	_ kernelengine.Runtime   = (*sdlcHermeticRuntime)(nil)
	_ kernelengine.Workspace = (*sdlcHermeticRuntime)(nil)
)

func (r *sdlcHermeticRuntime) Capabilities(context.Context) (kernelengine.Capabilities, error) {
	return kernelengine.Capabilities{Names: []string{"process"}}, nil
}

func (r *sdlcHermeticRuntime) Prepare(_ context.Context, req kernelengine.PrepareRequest) (kernelengine.Preparation, error) {
	r.mu.Lock()
	r.byID[req.Identity.WorkloadID] = req
	r.mu.Unlock()
	return kernelengine.Preparation{WorkspaceID: req.WorkspaceID, Detail: "prepared"}, nil
}

func (r *sdlcHermeticRuntime) Start(_ context.Context, req kernelengine.StartRequest) (kernelengine.StartResult, error) {
	r.mu.Lock()
	prep := r.byID[req.Identity.WorkloadID]
	hook := r.onStart
	r.mu.Unlock()
	if hook != nil {
		if err := hook(req, prep); err != nil {
			return kernelengine.StartResult{}, err
		}
	}
	r.mu.Lock()
	r.started[req.Identity.WorkloadID] = true
	r.mu.Unlock()
	return kernelengine.StartResult{Outcome: kernelengine.StartAccepted, WorkloadID: req.Identity.WorkloadID, Detail: "started"}, nil
}

func (r *sdlcHermeticRuntime) Observe(_ context.Context, req kernelengine.ObserveRequest) (engine.Observation, error) {
	r.mu.Lock()
	wasStarted := r.started[req.Identity.WorkloadID]
	r.mu.Unlock()
	if !wasStarted {
		return engine.AbsentObservation(), nil
	}
	return engine.ExitedObservation(0, engine.ConfirmedStop("exited 0")), nil
}

func (r *sdlcHermeticRuntime) Logs(context.Context, kernelengine.LogsRequest) (kernelengine.LogBatch, error) {
	return kernelengine.LogBatch{}, nil
}

func (r *sdlcHermeticRuntime) Stop(context.Context, kernelengine.StopRequest) (engine.StopEvidence, error) {
	return engine.ConfirmedStop("stopped"), nil
}

func (r *sdlcHermeticRuntime) Remove(context.Context, kernelengine.RemoveRequest) (kernelengine.Removal, error) {
	return kernelengine.Removal{Removed: true}, nil
}

func (r *sdlcHermeticRuntime) RefreshCredential(context.Context, kernelengine.CredentialRequest) (kernelengine.CredentialRefresh, error) {
	return kernelengine.CredentialRefresh{Delivered: true}, nil
}

func (r *sdlcHermeticRuntime) Status(context.Context, id.WorkspaceID) (kernelengine.WorkspaceObservation, error) {
	return kernelengine.WorkspaceObservation{Available: true}, nil
}

func (r *sdlcHermeticRuntime) Retain(context.Context, id.WorkspaceID) error  { return nil }
func (r *sdlcHermeticRuntime) Release(context.Context, id.WorkspaceID) error { return nil }

type sdlcStaticCred string

func (c sdlcStaticCred) Credential(context.Context) (client.Credential, error) {
	return client.Credential{
		Kind:      client.KindSession,
		Token:     string(c),
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

func invokeSDLC[Req any, Res any](
	t *testing.T,
	ctx context.Context,
	tr client.Transport,
	operation string,
	req Req,
	expectedVersion *uint64,
	idemKey string,
) Res {
	t.Helper()
	input, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal %s: %v", operation, err)
	}
	raw, err := tr.Invoke(ctx, api.Invocation{
		Operation: operation,
		Request: api.Request{
			Input:           input,
			ExpectedVersion: expectedVersion,
			IdempotencyKey:  idemKey,
		},
	})
	if err != nil {
		t.Fatalf("invoke %s: %v", operation, err)
	}
	var out Res
	if err := json.Unmarshal(raw.Body, &out); err != nil {
		t.Fatalf("unmarshal %s: %v (body=%s)", operation, err, string(raw.Body))
	}
	return out
}

func waitSDLCExecution(
	t *testing.T,
	ctx context.Context,
	tr client.Transport,
	execID string,
	pred func(api.ExecutionInspectResult) bool,
	desc string,
) api.ExecutionInspectResult {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		res := invokeSDLC[api.ExecutionInspectRequest, api.ExecutionInspectResult](
			t, ctx, tr, "execution.inspect", api.ExecutionInspectRequest{ExecutionID: execID}, nil, "",
		)
		if pred(res) {
			return res
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s on %s; last state=%s actions=%+v", desc, execID, res.State, res.Actions)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func resolveSDLCRequest(
	t *testing.T,
	ctx context.Context,
	inspectTr client.Transport,
	resolveTr client.Transport,
	execID string,
	actionIndex int,
	decision string,
	note string,
	outputs map[string]string,
	idemKey string,
) api.ExecutionRequestResolveResult {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		inspected := waitSDLCExecution(t, ctx, inspectTr, execID, func(res api.ExecutionInspectResult) bool {
			return len(res.Actions) > actionIndex && res.Actions[actionIndex].State == "running"
		}, "request action running")
		input, err := json.Marshal(api.ExecutionRequestResolveRequest{
			ExecutionID:           inspected.ExecutionID,
			ActionID:              inspected.Actions[actionIndex].ActionID,
			ExpectedActionVersion: inspected.Actions[actionIndex].Version,
			Decision:              decision,
			Note:                  note,
			Outputs:               outputs,
		})
		if err != nil {
			t.Fatalf("marshal resolve: %v", err)
		}
		raw, err := resolveTr.Invoke(ctx, api.Invocation{
			Operation: "execution.request.resolve",
			Request: api.Request{
				Input:           input,
				ExpectedVersion: &inspected.Version,
				IdempotencyKey:  idemKey,
			},
		})
		if err != nil {
			if strings.Contains(err.Error(), "version_conflict") && time.Now().Before(deadline) {
				time.Sleep(15 * time.Millisecond)
				continue
			}
			t.Fatalf("invoke execution.request.resolve (%s): %v", decision, err)
		}
		var out api.ExecutionRequestResolveResult
		if err := json.Unmarshal(raw.Body, &out); err != nil {
			t.Fatalf("unmarshal resolve result: %v", err)
		}
		return out
	}
}

// TestAgenticSDLCHermeticLifecycleEndToEnd runs a real hermetic multi-stage
// Agentic SDLC lifecycle against Kodema's real Shell + Kernel composition:
//   - applies the shipped Pattern A example pipelines,
//   - stores and retrieves a spec in the immutable KV store (kv.put / kv.get),
//   - runs Stage 1 with completes_task: false (task stays open),
//   - runs Stage 2, resolves review with `revise` (feedback appended to task,
//     excluded from role/operator failed_task inbox),
//   - re-runs Stage 2 and resolves review with `pass` (mints decision_id),
//   - verifies cross-execution approval via POST /api/v1/verify with decision_id,
//   - runs Stage 3 (service/merge-coordinator) and closes via task.complete.
func TestAgenticSDLCHermeticLifecycleEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		tenantID        = "tenant-sdlc"
		projectID       = "project-sdlc"
		tenantAdminCred = "dev-tenant-admin-sdlc"
		adminCred       = "dev-admin-sdlc"
		reviewerCred    = "dev-reviewer-sdlc"
		operatorCred    = "dev-operator-sdlc"
		gitServiceID    = "git-edge"
		mergeSvcID      = "merge-coordinator"
		gitResourceID   = "git-shop-web"
		baseSHA         = "1111111111111111111111111111111111111111"
		candV1          = "2222222222222222222222222222222222222222"
		candV2          = "3333333333333333333333333333333333333333"
		mergedSHA       = "4444444444444444444444444444444444444444"
	)

	tmp := t.TempDir()
	rt := newSDLCHermeticRuntime()
	scope := tenancy.Scope{TenantID: tenantID, ProjectID: projectID}
	expiry := time.Now().Add(2 * time.Hour)

	app, err := shellapp.New(ctx, shellapp.Config{
		Profile:              shellapp.ProfileDevelopment,
		InstallationID:       "inst-sdlc-example",
		DataDir:              filepath.Join(tmp, "shell-data"),
		DefaultActionTimeout: 2 * time.Minute,
		LoopInterval:         10 * time.Millisecond,
		Installation: installation.Config{
			Environment: installation.EnvironmentDevelopment,
			State:       installation.StateMemory,
			Messaging:   installation.MessagingMemory,
			Runtime:     installation.RuntimeProcess,
			Keys:        installation.KeyEphemeral,
			PublicURL:   "http://127.0.0.1:8080",
		},
		Dev: &shellapp.DevelopmentConfig{
			Bootstrap: &shellapp.Bootstrap{
				TenantID:       tenantID,
				TenantName:     "Tenant SDLC",
				ProjectID:      projectID,
				ProjectName:    "Project SDLC",
				AdminPrincipal: "admin-1",
				AdminRoles:     []string{"admin", "engineer", "reviewer", "operator", "viewer"},
			},
			Tokens: []shellapp.DevToken{
				{
					Credential: tenantAdminCred,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID},
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: adminCred,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   scope,
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: reviewerCred,
					Principal: identity.Principal{
						ID:      "reviewer-1",
						Kind:    identity.KindHuman,
						Scope:   scope,
						Roles:   []tenancy.Role{tenancy.RoleReviewer, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: operatorCred,
					Principal: identity.Principal{
						ID:      "operator-1",
						Kind:    identity.KindHuman,
						Scope:   scope,
						Roles:   []tenancy.Role{tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
			},
		},
		Adapters: &shellapp.Adapters{
			Runtime:   rt,
			Workspace: rt,
		},
	})
	if err != nil {
		t.Fatalf("shellapp.New: %v", err)
	}

	appCtx, cancelApp := context.WithCancel(ctx)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- app.Run(appCtx)
	}()
	defer func() {
		cancelApp()
		<-runErrCh
	}()

	srv := httptest.NewServer(app.Handler())
	defer srv.Close()

	newTransport := func(cred string) *client.HTTPTransport {
		return &client.HTTPTransport{
			BaseURL:      srv.URL,
			RoundTripper: srv.Client().Transport,
			Source:       sdlcStaticCred(cred),
			Retry: client.RetryPolicy{
				MaxAttempts: 1,
				BaseDelay:   10 * time.Millisecond,
				MaxDelay:    50 * time.Millisecond,
			},
		}
	}

	tenantAdminTr := newTransport(tenantAdminCred)
	adminTr := newTransport(adminCred)
	reviewerTr := newTransport(reviewerCred)
	operatorTr := newTransport(operatorCred)

	// Container hook: when implement or verify runs, submit declared outputs
	// using the workload's real execution credential over the public API.
	rt.onStart = func(req kernelengine.StartRequest, prep kernelengine.PrepareRequest) error {
		execTr := newTransport(prep.Credential)
		switch string(prep.ActionName) {
		case "implement":
			summaryText := "1 file changed, 24 insertions(+)"
			putRes := invokeSDLC[api.KVPutRequest, api.KVPutResult](t, ctx, execTr, "kv.put", api.KVPutRequest{
				ProjectID:   projectID,
				Text:        &summaryText,
				ContentType: "text/plain",
			}, nil, "kv-summary-"+string(prep.Identity.ExecutionID))
			inspected := invokeSDLC[api.ExecutionInspectRequest, api.ExecutionInspectResult](
				t, ctx, execTr, "execution.inspect", api.ExecutionInspectRequest{ExecutionID: string(prep.Identity.ExecutionID)}, nil, "",
			)
			invokeSDLC[api.ExecutionActionOutputsRequest, api.ExecutionActionOutputsResult](
				t, ctx, execTr, "execution.action.outputs", api.ExecutionActionOutputsRequest{
					ExecutionID: string(prep.Identity.ExecutionID),
					ActionID:    string(prep.Identity.ActionID),
					Outputs: map[string]string{
						"branch":      "refs/heads/work/" + string(prep.TaskID),
						"base":        baseSHA,
						"candidate":   candV1,
						"summary_key": putRes.Key,
					},
				}, &inspected.Version, "out-"+string(prep.Identity.ActionID),
			)
		case "verify":
			reportText := "ok github.com/acme/shop/checkout 0.12s"
			putRes := invokeSDLC[api.KVPutRequest, api.KVPutResult](t, ctx, execTr, "kv.put", api.KVPutRequest{
				ProjectID:   projectID,
				Text:        &reportText,
				ContentType: "text/plain",
			}, nil, "kv-report-"+string(prep.Identity.ExecutionID))
			inspected := invokeSDLC[api.ExecutionInspectRequest, api.ExecutionInspectResult](
				t, ctx, execTr, "execution.inspect", api.ExecutionInspectRequest{ExecutionID: string(prep.Identity.ExecutionID)}, nil, "",
			)
			invokeSDLC[api.ExecutionActionOutputsRequest, api.ExecutionActionOutputsResult](
				t, ctx, execTr, "execution.action.outputs", api.ExecutionActionOutputsRequest{
					ExecutionID: string(prep.Identity.ExecutionID),
					ActionID:    string(prep.Identity.ActionID),
					Outputs: map[string]string{
						"report_key": putRes.Key,
					},
				}, &inspected.Version, "out-"+string(prep.Identity.ActionID),
			)
		}
		return nil
	}

	// 1. Grant real project memberships for reviewer-1 and operator-1, create
	// git-edge and merge-coordinator services, issue credentials, and register
	// the Git repository resource binding.
	invokeSDLC[api.MembershipGrantRequest, api.MembershipGrantResult](
		t, ctx, tenantAdminTr, "membership.grant", api.MembershipGrantRequest{
			ProjectID:   projectID,
			PrincipalID: "reviewer-1",
			Roles:       []string{"reviewer", "viewer"},
		}, nil, "grant-reviewer",
	)
	invokeSDLC[api.MembershipGrantRequest, api.MembershipGrantResult](
		t, ctx, tenantAdminTr, "membership.grant", api.MembershipGrantRequest{
			ProjectID:   projectID,
			PrincipalID: "operator-1",
			Roles:       []string{"operator", "viewer"},
		}, nil, "grant-operator",
	)
	gitSvcCreate := invokeSDLC[api.ServiceCreateRequest, api.ServiceCreateResult](
		t, ctx, adminTr, "service.create", api.ServiceCreateRequest{ProjectID: projectID, ServiceID: gitServiceID, Roles: []string{"viewer"}}, nil, "create-git-svc",
	)
	gitSvcCred := invokeSDLC[api.ServiceCredentialIssueRequest, api.ServiceCredentialIssueResult](
		t, ctx, adminTr, "service.credential.issue", api.ServiceCredentialIssueRequest{ServiceID: gitServiceID, TTLSeconds: 3600}, &gitSvcCreate.Version, "issue-git-svc",
	)
	mergeSvcCreate := invokeSDLC[api.ServiceCreateRequest, api.ServiceCreateResult](
		t, ctx, adminTr, "service.create", api.ServiceCreateRequest{ProjectID: projectID, ServiceID: mergeSvcID, Roles: []string{"engineer"}}, nil, "create-merge-svc",
	)
	mergeSvcCred := invokeSDLC[api.ServiceCredentialIssueRequest, api.ServiceCredentialIssueResult](
		t, ctx, adminTr, "service.credential.issue", api.ServiceCredentialIssueRequest{ServiceID: mergeSvcID, TTLSeconds: 3600}, &mergeSvcCreate.Version, "issue-merge-svc",
	)

	if gitSvcCred.Token == nil || mergeSvcCred.Token == nil {
		t.Fatal("service.credential.issue returned nil token")
	}
	gitTr := newTransport(*gitSvcCred.Token)
	mergeTr := newTransport(*mergeSvcCred.Token)

	invokeSDLC[api.ResourceBindingRegisterRequest, api.ResourceBindingRegisterResult](
		t, ctx, adminTr, "resource.binding.register", api.ResourceBindingRegisterRequest{
			ProjectID:        projectID,
			BindingID:        gitResourceID,
			ResourceID:       gitResourceID,
			EnforcingService: gitServiceID,
			TrustedOrigins:   []string{"https://git.example.invalid"},
			Operations:       []string{"git.read", "git.ref.update"},
			ReportableEvents: []string{"git.ref.updated"},
		}, nil, "bind-git-shop-web",
	)

	// 2. Apply all three Pattern A example pipelines verbatim.
	applyPipeline := func(name, rel string) string {
		res := invokeSDLC[api.PipelineApplyRequest, api.PipelineApplyResult](
			t, ctx, adminTr, "pipeline.apply", api.PipelineApplyRequest{
				ProjectID: projectID,
				Name:      name,
				YAML:      readExample(t, rel),
			}, nil, "apply-"+name,
		)
		return res.PipelineID
	}
	implPipelineID := applyPipeline("pattern-a-implement", "agentic-sdlc/pattern-a-implement.yaml")
	verifyPipelineID := applyPipeline("pattern-a-verify-review", "agentic-sdlc/pattern-a-verify-review.yaml")
	mergePipelineID := applyPipeline("pattern-a-merge", "agentic-sdlc/pattern-a-merge.yaml")

	// 3. Store feature spec in immutable KV store and verify roundtrip retrieval.
	specText := "# Checkout Validation\nReject empty carts with HTTP 400.\n"
	specPut := invokeSDLC[api.KVPutRequest, api.KVPutResult](t, ctx, adminTr, "kv.put", api.KVPutRequest{
		ProjectID:   projectID,
		Text:        &specText,
		ContentType: "text/markdown",
	}, nil, "put-spec")
	if !api.ValidKVKey(specPut.Key) {
		t.Fatalf("kv.put returned invalid key %q", specPut.Key)
	}
	specGet := invokeSDLC[api.KVGetRequest, api.KVGetResult](t, ctx, adminTr, "kv.get", api.KVGetRequest{
		ProjectID: projectID,
		Key:       specPut.Key,
	}, nil, "")
	if specGet.Text == nil || *specGet.Text != specText {
		t.Fatalf("kv.get text = %v; want %q", specGet.Text, specText)
	}

	// 4. Create parent epic task and start child task on Stage 1 (completes_task: false).
	epic := invokeSDLC[api.TaskCreateRequest, api.TaskCreateResult](
		t, ctx, adminTr, "task.create", api.TaskCreateRequest{
			ProjectID: projectID,
			Title:     "Q4 Checkout Hardening",
		}, nil, "create-epic",
	)
	stage1 := invokeSDLC[api.TaskCreateAndStartRequest, api.TaskCreateAndStartResult](
		t, ctx, adminTr, "task.create_and_start", api.TaskCreateAndStartRequest{
			ProjectID:  projectID,
			ParentID:   epic.TaskID,
			Title:      "Add checkout validation",
			PipelineID: implPipelineID,
			Inputs: map[string]string{
				"repository": "shop/web",
				"base_ref":   "refs/heads/main",
				"spec_key":   specPut.Key,
			},
		}, nil, "start-stage-1",
	)
	exec1 := waitSDLCExecution(t, ctx, adminTr, stage1.ExecutionID, func(res api.ExecutionInspectResult) bool {
		return res.State == "succeeded"
	}, "stage 1 succeeded")
	implOutputs := exec1.Actions[0].Outputs

	// Verify completes_task: false kept the task open (not prematurely done!).
	taskAfterStage1 := invokeSDLC[api.TaskReadRequest, api.TaskReadResult](
		t, ctx, adminTr, "task.read", api.TaskReadRequest{TaskID: stage1.TaskID}, nil, "",
	)
	if taskAfterStage1.State != "open" {
		t.Fatalf("task state after completes_task:false stage 1 = %q; want open", taskAfterStage1.State)
	}
	if taskAfterStage1.ParentID != epic.TaskID {
		t.Fatalf("task parent_id = %q; want %q", taskAfterStage1.ParentID, epic.TaskID)
	}

	// 5. Advance to Stage 2 (verify + review) and resolve review with `revise`.
	stage2v1 := invokeSDLC[api.TaskStartRequest, api.TaskStartResult](
		t, ctx, adminTr, "task.start", api.TaskStartRequest{
			TaskID:     stage1.TaskID,
			PipelineID: verifyPipelineID,
			Inputs: map[string]string{
				"repository":  "shop/web",
				"resource_id": gitResourceID,
				"target_ref":  "refs/heads/main",
				"branch":      implOutputs["branch"],
				"base":        implOutputs["base"],
				"candidate":   implOutputs["candidate"],
				"summary_key": implOutputs["summary_key"],
			},
		}, &taskAfterStage1.LifecycleVersion, "start-stage-2-v1",
	)
	const reviseFeedback = "Please add a nil-cart unit test before merging."
	resolveSDLCRequest(t, ctx, adminTr, reviewerTr, stage2v1.ExecutionID, 1, "revise", reviseFeedback, nil, "resolve-revise-v1")
	waitSDLCExecution(t, ctx, adminTr, stage2v1.ExecutionID, func(res api.ExecutionInspectResult) bool {
		return res.State == "failed"
	}, "stage 2 v1 failed with revision")

	// Assert reviewer feedback was appended to task messages and operator inbox
	// does NOT receive a failed_task alert for domain `revise` decisions.
	taskAfterRevise := invokeSDLC[api.TaskReadRequest, api.TaskReadResult](
		t, ctx, adminTr, "task.read", api.TaskReadRequest{TaskID: stage1.TaskID}, nil, "",
	)
	if taskAfterRevise.State != "open" {
		t.Fatalf("task state after revise = %q; want open", taskAfterRevise.State)
	}
	foundFeedback := false
	for _, msg := range taskAfterRevise.Messages {
		if strings.Contains(msg.Body, reviseFeedback) {
			foundFeedback = true
			break
		}
	}
	if !foundFeedback {
		t.Fatalf("task messages after revise missing feedback %q: %+v", reviseFeedback, taskAfterRevise.Messages)
	}
	operatorInbox := invokeSDLC[api.TaskInboxRequest, api.TaskInboxResult](
		t, ctx, operatorTr, "task.inbox", api.TaskInboxRequest{ProjectID: projectID}, nil, "",
	)
	for _, item := range operatorInbox.Items {
		if item.Kind == "failed_task" && item.TaskID == stage1.TaskID {
			t.Fatalf("operator inbox should exclude task %s after revise decision, got %+v", stage1.TaskID, item)
		}
	}

	// 6. Re-run Stage 2 with revised candidate v2 and resolve review with `pass`.
	stage2v2 := invokeSDLC[api.TaskStartRequest, api.TaskStartResult](
		t, ctx, adminTr, "task.start", api.TaskStartRequest{
			TaskID:     stage1.TaskID,
			PipelineID: verifyPipelineID,
			Inputs: map[string]string{
				"repository":  "shop/web",
				"resource_id": gitResourceID,
				"target_ref":  "refs/heads/main",
				"branch":      implOutputs["branch"],
				"base":        baseSHA,
				"candidate":   candV2,
				"summary_key": implOutputs["summary_key"],
			},
		}, &taskAfterRevise.LifecycleVersion, "start-stage-2-v2",
	)
	passRes := resolveSDLCRequest(
		t, ctx, adminTr, reviewerTr, stage2v2.ExecutionID, 1, "pass", "LGTM",
		map[string]string{"notes": "approved after test added"}, "resolve-pass-v2",
	)
	if passRes.DecisionID == "" {
		t.Fatal("pass resolution did not mint a cross-execution decision_id")
	}
	waitSDLCExecution(t, ctx, adminTr, stage2v2.ExecutionID, func(res api.ExecutionInspectResult) bool {
		return res.State == "succeeded"
	}, "stage 2 v2 succeeded")

	// 7. Start Stage 3 (pattern-a-merge with decision_id) so service/merge-coordinator
	// holds an active assigned merge request on the same task, then verify
	// cross-execution approval via POST /api/v1/verify using the minted Decision ID.
	taskBeforeMerge := invokeSDLC[api.TaskReadRequest, api.TaskReadResult](
		t, ctx, adminTr, "task.read", api.TaskReadRequest{TaskID: stage1.TaskID}, nil, "",
	)
	stage3 := invokeSDLC[api.TaskStartRequest, api.TaskStartResult](
		t, ctx, adminTr, "task.start", api.TaskStartRequest{
			TaskID:     stage1.TaskID,
			PipelineID: mergePipelineID,
			Inputs: map[string]string{
				"repository":  "shop/web",
				"target_ref":  "refs/heads/main",
				"base":        baseSHA,
				"candidate":   candV2,
				"decision_id": passRes.DecisionID,
			},
		}, &taskBeforeMerge.LifecycleVersion, "start-stage-3",
	)
	waitSDLCExecution(t, ctx, adminTr, stage3.ExecutionID, func(res api.ExecutionInspectResult) bool {
		return len(res.Actions) == 1 && res.Actions[0].State == "running"
	}, "stage 3 waiting at merge-coordinator")

	verifyOK := invokeSDLC[api.VerifyRequest, api.VerifyResponse](
		t, ctx, gitTr, "actor.verify", api.VerifyRequest{
			ActorCredential: *mergeSvcCred.Token,
			ResourceID:      gitResourceID,
			Operation:       "git.ref.update",
			Approval:        "role/reviewer",
			DecisionID:      passRes.DecisionID,
			Subject: api.Subject{Fields: []api.SubjectField{
				{Name: "ref", Kind: "string", Value: "refs/heads/main"},
				{Name: "expected_base", Kind: "string", Value: baseSHA},
				{Name: "candidate", Kind: "string", Value: candV2},
			}},
		}, nil, "",
	)
	if !verifyOK.Approval {
		t.Fatalf("actor.verify with matching decision_id %q returned approval=false", passRes.DecisionID)
	}
	mismatchInput, err := json.Marshal(api.VerifyRequest{
		ActorCredential: *mergeSvcCred.Token,
		ResourceID:      gitResourceID,
		Operation:       "git.ref.update",
		Approval:        "role/reviewer",
		DecisionID:      passRes.DecisionID,
		Subject: api.Subject{Fields: []api.SubjectField{
			{Name: "ref", Kind: "string", Value: "refs/heads/main"},
			{Name: "expected_base", Kind: "string", Value: baseSHA},
			{Name: "candidate", Kind: "string", Value: candV1},
		}},
	})
	if err != nil {
		t.Fatalf("marshal mismatch verify: %v", err)
	}
	mismatchRaw, err := gitTr.Invoke(ctx, api.Invocation{
		Operation: "actor.verify",
		Request:   api.Request{Input: mismatchInput},
	})
	if err != nil {
		var pubErr *api.Error
		code, hasCode := fault.CodeOf(err)
		isForbidden := (hasCode && code == fault.CodeForbidden) || (errors.As(err, &pubErr) && pubErr.Code == api.CodeForbidden)
		if !isForbidden {
			t.Fatalf("actor.verify mismatch returned unexpected error: %v (want %s)", err, api.CodeForbidden)
		}
	} else {
		var mismatchResp api.VerifyResponse
		if decErr := json.Unmarshal(mismatchRaw.Body, &mismatchResp); decErr != nil {
			t.Fatalf("unmarshal mismatch verify: %v", decErr)
		}
		if mismatchResp.Approval {
			t.Fatalf("actor.verify approved mismatched candidate %s with decision_id %q", candV1, passRes.DecisionID)
		}
	}

	// 8. Settle Stage 3's merge request as service/merge-coordinator and
	// explicitly complete the task via task.complete.
	resolveSDLCRequest(
		t, ctx, adminTr, mergeTr, stage3.ExecutionID, 0, "pass", "",
		map[string]string{
			"base":      baseSHA,
			"candidate": candV2,
			"output":    mergedSHA,
		}, "resolve-merge",
	)
	waitSDLCExecution(t, ctx, adminTr, stage3.ExecutionID, func(res api.ExecutionInspectResult) bool {
		return res.State == "succeeded"
	}, "stage 3 succeeded")

	taskBeforeComplete := invokeSDLC[api.TaskReadRequest, api.TaskReadResult](
		t, ctx, adminTr, "task.read", api.TaskReadRequest{TaskID: stage1.TaskID}, nil, "",
	)
	if taskBeforeComplete.State != "open" {
		t.Fatalf("task state before explicit task.complete = %q; want open", taskBeforeComplete.State)
	}
	invokeSDLC[api.TaskCompleteRequest, api.TaskCompleteResult](
		t, ctx, adminTr, "task.complete", api.TaskCompleteRequest{
			TaskID: stage1.TaskID,
			Reason: fmt.Sprintf("Verified, approved (%s), and merged (%s)", passRes.DecisionID, mergedSHA),
		}, &taskBeforeComplete.LifecycleVersion, "complete-task",
	)
	finalTask := invokeSDLC[api.TaskReadRequest, api.TaskReadResult](
		t, ctx, adminTr, "task.read", api.TaskReadRequest{TaskID: stage1.TaskID}, nil, "",
	)
	if finalTask.State != "done" {
		t.Fatalf("final task state after task.complete = %q; want done", finalTask.State)
	}
}

// TestAgenticSDLCSandboxServeAndPipelineAdmission verifies Option 2 Brain/Hands
// end-to-end:
//  1. Starts a real `kodema sandbox serve` HTTP MCP server jailed to a temp
//     workspace and exercises write_file, edit_file, read_file, glob, grep,
//     bash, and path-traversal refusal over authenticated HTTP MCP.
//  2. Generates ephemeral sandbox pipeline YAMLs with `BuildSandboxPipelineYAML`
//     for both Go (`golang:1.24`) and Node (`node:22`) sandboxes and proves
//     Kodema's real pipeline admission parser accepts both.
func TestAgenticSDLCSandboxServeAndPipelineAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	workspaceDir := t.TempDir()
	bearer, err := sandbox.GenerateBearerToken()
	if err != nil {
		t.Fatalf("GenerateBearerToken: %v", err)
	}
	srv, err := sandbox.NewServer(sandbox.Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: workspaceDir,
		Token:        bearer,
	})
	if err != nil {
		t.Fatalf("sandbox.NewServer: %v", err)
	}
	srvCtx, cancelSrv := context.WithCancel(ctx)
	baseURL, err := srv.Start(srvCtx)
	if err != nil {
		cancelSrv()
		t.Fatalf("sandbox.Start: %v", err)
	}
	t.Cleanup(func() {
		cancelSrv()
		_ = srv.Shutdown(context.Background())
	})

	cli, err := sandbox.NewClient(baseURL, bearer, nil)
	if err != nil {
		t.Fatalf("sandbox.NewClient: %v", err)
	}
	if err := cli.Health(ctx); err != nil {
		t.Fatalf("sandbox Health: %v", err)
	}
	if _, err := cli.Initialize(ctx); err != nil {
		t.Fatalf("sandbox Initialize: %v", err)
	}
	tools, err := cli.ListTools(ctx)
	if err != nil {
		t.Fatalf("sandbox ListTools: %v", err)
	}
	if len(tools) != 6 {
		t.Fatalf("sandbox ListTools count = %d; want 6", len(tools))
	}

	writeRes, err := cli.CallTool(ctx, "write_file", map[string]any{
		"path":    "pkg/greet.txt",
		"content": "hello world\n",
	})
	if err != nil || writeRes.IsError {
		t.Fatalf("write_file failed: err=%v res=%+v", err, writeRes)
	}
	editRes, err := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":       "pkg/greet.txt",
		"old_string": "world",
		"new_string": "kodema",
	})
	if err != nil || editRes.IsError {
		t.Fatalf("edit_file failed: err=%v res=%+v", err, editRes)
	}
	readRes, err := cli.CallTool(ctx, "read_file", map[string]any{
		"path": "pkg/greet.txt",
	})
	if err != nil || readRes.IsError || !strings.Contains(readRes.FirstText(), "hello kodema") {
		t.Fatalf("read_file failed: err=%v res=%+v", err, readRes)
	}
	globRes, err := cli.CallTool(ctx, "glob", map[string]any{
		"pattern": "**/*.txt",
	})
	if err != nil || globRes.IsError || !strings.Contains(globRes.FirstText(), "pkg/greet.txt") {
		t.Fatalf("glob failed: err=%v res=%+v", err, globRes)
	}
	grepRes, err := cli.CallTool(ctx, "grep", map[string]any{
		"pattern": "kodema",
	})
	if err != nil || grepRes.IsError || !strings.Contains(grepRes.FirstText(), "hello kodema") {
		t.Fatalf("grep failed: err=%v res=%+v", err, grepRes)
	}
	bashRes, err := cli.CallTool(ctx, "bash", map[string]any{
		"command": "cat pkg/greet.txt",
	})
	if err != nil || bashRes.IsError || !strings.Contains(bashRes.FirstText(), "hello kodema") {
		t.Fatalf("bash failed: err=%v res=%+v", err, bashRes)
	}

	// Path traversal outside /workspace must be refused by os.Root jail.
	escapeRes, err := cli.CallTool(ctx, "read_file", map[string]any{
		"path": "../outside.txt",
	})
	if err != nil {
		t.Fatalf("read_file traversal transport error: %v", err)
	}
	if !escapeRes.IsError {
		t.Fatalf("read_file traversal unexpectedly succeeded: %+v", escapeRes)
	}

	// Admit BuildSandboxPipelineYAML for both Go and Node polyglot sandboxes
	// through Kodema's real pipeline admission parser.
	sandboxCases := []struct {
		name      string
		req       opencode.OpenCodeSandboxRequest
		secretRef string
	}{
		{
			name: "golang-1.24",
			req: opencode.OpenCodeSandboxRequest{
				TenantID:    "tenant-a",
				ProjectID:   "shop",
				TaskID:      "task-polyglot-1",
				ExecutionID: "exec-1",
				ActionID:    "act-1",
				Image:       "docker.io/library/golang:1.24",
				Repository:  "shop/web",
				BaseRef:     "refs/heads/main",
				WorkBranch:  "refs/heads/work/task-polyglot-1",
				Caches:      []string{"go-mod", "go-build"},
				Memory:      "2Gi",
				CPU:         "2",
				Port:        8090,
				Timeout:     15 * time.Minute,
			},
			secretRef: "KODEMA_SBOX_TOK_0123456789ABCDEF",
		},
		{
			name: "node-22",
			req: opencode.OpenCodeSandboxRequest{
				TenantID:    "tenant-a",
				ProjectID:   "shop",
				TaskID:      "task-polyglot-1",
				ExecutionID: "exec-1",
				ActionID:    "act-2",
				Image:       "docker.io/library/node:22",
				Repository:  "shop/web",
				BaseRef:     "refs/heads/work/task-polyglot-1",
				WorkBranch:  "refs/heads/work/task-polyglot-1",
				Caches:      []string{"npm"},
				Memory:      "2Gi",
				CPU:         "2",
				Port:        8090,
				Timeout:     15 * time.Minute,
			},
			secretRef: "KODEMA_SBOX_TOK_FEDCBA9876543210",
		},
	}
	for _, tc := range sandboxCases {
		t.Run("admit-"+tc.name, func(t *testing.T) {
			yamlBytes, err := opencode.BuildSandboxPipelineYAML(tc.req, tc.secretRef)
			if err != nil {
				t.Fatalf("BuildSandboxPipelineYAML(%s): %v", tc.name, err)
			}
			decoded, err := pipeline.Decode(yamlBytes, pipeline.DefaultLimits)
			if err != nil {
				t.Fatalf("pipeline.Decode(%s) refused generated sandbox YAML: %v\n%s", tc.name, err, string(yamlBytes))
			}
			if len(decoded.Actions) != 1 || decoded.Actions[0].Container == nil {
				t.Fatalf("generated sandbox pipeline has unexpected actions: %+v", decoded.Actions)
			}
			c := decoded.Actions[0].Container
			if c.Image != tc.req.Image {
				t.Fatalf("sandbox image = %q; want %q", c.Image, tc.req.Image)
			}
			if len(c.Secrets) != 1 || string(c.Secrets[0]) != tc.secretRef {
				t.Fatalf("sandbox secrets = %v; want [%s]", c.Secrets, tc.secretRef)
			}
			if len(c.Caches) != len(tc.req.Caches) {
				t.Fatalf("sandbox caches = %v; want %v", c.Caches, tc.req.Caches)
			}
		})
	}
}

// validateHelmValuesOverlayAgainstSchema validates every top-level key and
// nested value in a Helm overlay values YAML snippet against the chart's
// values.schema.json (using stdlib only).
func validateHelmValuesOverlayAgainstSchema(t *testing.T, rawYAML []byte, schemaPath string) {
	t.Helper()
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read Helm values schema %s: %v", schemaPath, err)
	}
	var rootSchema map[string]any
	if err := json.Unmarshal(schemaBytes, &rootSchema); err != nil {
		t.Fatalf("parse Helm values schema %s: %v", schemaPath, err)
	}
	var decodedYAML any
	if err := yaml.Unmarshal(rawYAML, &decodedYAML); err != nil {
		t.Fatalf("unmarshal YAML overlay: %v", err)
	}
	jsonBytes, err := json.Marshal(decodedYAML)
	if err != nil {
		t.Fatalf("normalize YAML overlay to JSON: %v", err)
	}
	var overlay map[string]any
	if err := json.Unmarshal(jsonBytes, &overlay); err != nil {
		t.Fatalf("decode normalized JSON overlay as object: %v", err)
	}
	if len(overlay) == 0 {
		t.Fatal("Helm values overlay is empty")
	}
	rootProps, ok := rootSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("%s missing top-level properties object", schemaPath)
	}
	for key, val := range overlay {
		propSchema, exists := rootProps[key].(map[string]any)
		if !exists {
			t.Fatalf("Helm values overlay key %q is not declared in %s", key, schemaPath)
		}
		if err := validateSchemaNode(propSchema, val, "values."+key); err != nil {
			t.Fatalf("Helm values overlay violates %s: %v", schemaPath, err)
		}
	}
}

func validateSchemaNode(schema map[string]any, value any, path string) error {
	if expectedType, ok := schema["type"].(string); ok {
		switch expectedType {
		case "object":
			if _, ok := value.(map[string]any); !ok {
				return fmt.Errorf("%s: got %T, want object", path, value)
			}
		case "array":
			if _, ok := value.([]any); !ok {
				return fmt.Errorf("%s: got %T, want array", path, value)
			}
		case "string":
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s: got %T, want string", path, value)
			}
		case "boolean":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s: got %T, want boolean", path, value)
			}
		case "integer":
			n, ok := value.(float64)
			if !ok || math.Trunc(n) != n {
				return fmt.Errorf("%s: got %v (%T), want integer", path, value, value)
			}
		case "number":
			if _, ok := value.(float64); !ok {
				return fmt.Errorf("%s: got %T, want number", path, value)
			}
		}
	}
	if c, hasConst := schema["const"]; hasConst && value != c {
		return fmt.Errorf("%s: got %v, want const %v", path, value, c)
	}
	if enumVals, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enumVals {
			if value == candidate {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: value %v not in enum %v", path, value, enumVals)
		}
	}

	switch v := value.(type) {
	case map[string]any:
		props, _ := schema["properties"].(map[string]any)
		if reqList, ok := schema["required"].([]any); ok {
			for _, entry := range reqList {
				reqKey, ok := entry.(string)
				if !ok {
					continue
				}
				if _, present := v[reqKey]; !present {
					return fmt.Errorf("%s: missing required property %q", path, reqKey)
				}
			}
		}
		for k, child := range v {
			if childSchema, ok := props[k].(map[string]any); ok {
				if err := validateSchemaNode(childSchema, child, path+"."+k); err != nil {
					return err
				}
				continue
			}
			switch add := schema["additionalProperties"].(type) {
			case bool:
				if !add {
					return fmt.Errorf("%s: unexpected property %q", path, k)
				}
			case map[string]any:
				if err := validateSchemaNode(add, child, path+"."+k); err != nil {
					return err
				}
			}
		}
	case []any:
		if minItems, ok := schema["minItems"].(float64); ok && len(v) < int(minItems) {
			return fmt.Errorf("%s: array length %d < minItems %d", path, len(v), int(minItems))
		}
		if itemSchema, ok := schema["items"].(map[string]any); ok {
			for i, elem := range v {
				if err := validateSchemaNode(itemSchema, elem, fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
		}
	case string:
		runeLen := utf8.RuneCountInString(v)
		if minLen, ok := schema["minLength"].(float64); ok && runeLen < int(minLen) {
			return fmt.Errorf("%s: string %q length %d < minLength %d", path, v, runeLen, int(minLen))
		}
		if maxLen, ok := schema["maxLength"].(float64); ok && runeLen > int(maxLen) {
			return fmt.Errorf("%s: string %q length %d > maxLength %d", path, v, runeLen, int(maxLen))
		}
		if pat, ok := schema["pattern"].(string); ok {
			re, err := regexp.Compile(pat)
			if err != nil {
				return fmt.Errorf("%s: invalid schema pattern %q: %w", path, pat, err)
			}
			if !re.MatchString(v) {
				return fmt.Errorf("%s: string %q does not match pattern %q", path, v, pat)
			}
		}
	case float64:
		if minVal, ok := schema["minimum"].(float64); ok && v < minVal {
			return fmt.Errorf("%s: value %v < minimum %v", path, v, minVal)
		}
		if maxVal, ok := schema["maximum"].(float64); ok && v > maxVal {
			return fmt.Errorf("%s: value %v > maximum %v", path, v, maxVal)
		}
	}

	if allOf, ok := schema["allOf"].([]any); ok {
		for _, entry := range allOf {
			sub, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			if err := validateSchemaNode(sub, value, path); err != nil {
				return err
			}
		}
	}
	if ifSchema, ok := schema["if"].(map[string]any); ok {
		if validateSchemaNode(ifSchema, value, path) == nil {
			if thenSchema, ok := schema["then"].(map[string]any); ok {
				if err := validateSchemaNode(thenSchema, value, path); err != nil {
					return err
				}
			}
		} else if elseSchema, ok := schema["else"].(map[string]any); ok {
			if err := validateSchemaNode(elseSchema, value, path); err != nil {
				return err
			}
		}
	}
	return nil
}
