//go:build integration

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	runtimepodman "github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/control/secret"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/test/support/factory"
)

// pipelineDoc is the API-posted pipeline document this suite authors.
type pipelineDoc struct {
	Inputs  []string            `yaml:"inputs,omitempty"`
	Env     map[string]string   `yaml:"env,omitempty"`
	Actions []pipelineActionDoc `yaml:"actions"`
}

// pipelineActionDoc is one action of the API-posted pipeline. Exactly one of
// Image (container) or Request (request action) is rendered.
type pipelineActionDoc struct {
	Name     string         `yaml:"name"`
	Image    string         `yaml:"image,omitempty"`
	Commands []string       `yaml:"commands,omitempty"`
	Request  string         `yaml:"request,omitempty"`
	Subject  map[string]any `yaml:"subject,omitempty"`
	Outputs  []string       `yaml:"outputs,omitempty"`
	Secrets  []string       `yaml:"secrets,omitempty"`
	Timeout  string         `yaml:"timeout,omitempty"`
}

// marshalPipeline renders one exact pipeline YAML byte sequence.
func marshalPipeline(t *testing.T, document pipelineDoc) []byte {
	t.Helper()
	encoded, err := yaml.Marshal(document)
	if err != nil {
		t.Fatalf("marshal pipeline yaml: %v", err)
	}
	return encoded
}

// applyPipeline posts one pipeline through the public API and returns the
// stored pipeline identity and version.
func applyPipeline(t *testing.T, harness *factory.Harness, name string, document []byte) api.PipelineApplyResult {
	t.Helper()
	result := harness.MustREST(harness.ProjectToken, "pipeline.apply", api.PipelineApplyRequest{
		ProjectID: factory.ProjectID,
		Name:      name,
		YAML:      document,
	}, nil, "apply-"+name)
	var applied api.PipelineApplyResult
	harness.Decode(result, &applied)
	return applied
}

// updatePipeline replaces one stored pipeline at its expected version.
func updatePipeline(t *testing.T, harness *factory.Harness, pipelineID, name string, document []byte, expected uint64) api.PipelineUpdateResult {
	t.Helper()
	result := harness.MustREST(harness.ProjectToken, "pipeline.update", api.PipelineUpdateRequest{
		ProjectID:  factory.ProjectID,
		PipelineID: pipelineID,
		Name:       name,
		YAML:       document,
	}, &expected, "update-"+name)
	var updated api.PipelineUpdateResult
	harness.Decode(result, &updated)
	return updated
}

// readPipeline reads one stored pipeline definition.
func readPipeline(t *testing.T, harness *factory.Harness, pipelineID string) api.PipelineReadResult {
	t.Helper()
	result := harness.MustREST(harness.ProjectToken, "pipeline.read", api.PipelineReadRequest{
		ProjectID:  factory.ProjectID,
		PipelineID: pipelineID,
	}, nil, "")
	var read api.PipelineReadResult
	harness.Decode(result, &read)
	return read
}

// startPipeline starts one stored pipeline over REST at its currently observed
// stored version.
func startPipeline(t *testing.T, harness *factory.Harness, token, pipelineID string, inputs map[string]string, key string) api.PipelineStartResult {
	t.Helper()
	stored := readPipeline(t, harness, pipelineID)
	if stored.Version == 0 {
		t.Fatalf("pipeline.read %s version = 0", pipelineID)
	}
	result := harness.MustREST(token, "pipeline.start", api.PipelineStartRequest{
		ProjectID:  factory.ProjectID,
		PipelineID: pipelineID,
		Inputs:     inputs,
	}, &stored.Version, key)
	var started api.PipelineStartResult
	harness.Decode(result, &started)
	return started
}

// inspectExecution reads one execution through the public API.
func inspectExecution(t *testing.T, harness *factory.Harness, token, executionID string) api.ExecutionInspectResult {
	t.Helper()
	result := harness.MustREST(token, "execution.inspect", api.ExecutionInspectRequest{
		ExecutionID: executionID,
	}, nil, "")
	var inspected api.ExecutionInspectResult
	harness.Decode(result, &inspected)
	return inspected
}

// executionResources is the live external-resource observation used to justify
// a terminal "none" cleanup record. A "none" record claims that no external
// resource requires cleanup, so it is only accepted once the live state proves
// that claim: no run-labelled container for the execution and no retained
// workspace volume remain.
type executionResources struct {
	containers      []string
	workspaceID     string
	workspaceVolume bool
	err             error
}

// clean reports whether the live state matches a "none" cleanup record.
func (resources executionResources) clean() bool {
	return resources.err == nil && len(resources.containers) == 0 && !resources.workspaceVolume
}

// String renders the probe for a failure message. It never contains a secret.
func (resources executionResources) String() string {
	if resources.err != nil {
		return fmt.Sprintf("resource probe failed: %v (containers=%v workspace_id=%q workspace_volume_present=%t)",
			resources.err, resources.containers, resources.workspaceID, resources.workspaceVolume)
	}
	return fmt.Sprintf("containers=%v workspace_id=%q workspace_volume_present=%t",
		resources.containers, resources.workspaceID, resources.workspaceVolume)
}

// probeExecutionResources observes the live containers and retained workspace
// volume of one execution. It never fails the test: a transient probe error is
// returned as evidence so the caller keeps waiting.
func probeExecutionResources(ctx context.Context, harness *factory.Harness, executionID string) executionResources {
	containers, err := harness.Containers(ctx, runtimepodman.LabelExecution, executionID)
	if err != nil {
		return executionResources{err: fmt.Errorf("list execution containers: %w", err)}
	}
	workspaceID, err := harness.ExecutionWorkspaceID(ctx, executionID)
	if err != nil {
		return executionResources{containers: containers, err: fmt.Errorf("read execution workspace: %w", err)}
	}
	present, err := harness.WorkspaceVolumePresent(ctx, workspaceID)
	if err != nil {
		return executionResources{containers: containers, workspaceID: workspaceID, err: fmt.Errorf("probe workspace volume: %w", err)}
	}
	return executionResources{containers: containers, workspaceID: workspaceID, workspaceVolume: present}
}

// assertRequestWaitResources proves the live probes observe the real retained
// workspace and the stopped earlier workload before any absence is trusted. A
// probe that filters the wrong label or reports a missing volume would
// otherwise make every later "released"/"no running workload" assertion pass
// vacuously. The running-container check stays negative here; its positive
// control is assertRunningContainersPositiveControl, which starts its own
// running container.
func assertRequestWaitResources(t *testing.T, harness *factory.Harness, executionID string) {
	t.Helper()
	resources := probeExecutionResources(t.Context(), harness, executionID)
	if resources.err != nil {
		t.Fatalf("probe request-wait resources of execution %s: %v", executionID, resources.err)
	}
	if len(resources.containers) == 0 {
		t.Fatalf("positive control: execution %s has no stopped workload container while the request waits", executionID)
	}
	if !resources.workspaceVolume {
		t.Fatalf("positive control: execution %s has no retained workspace volume %q while the request waits", executionID, resources.workspaceID)
	}
	running, err := harness.RunningContainers(t.Context(), runtimepodman.LabelExecution, executionID)
	if err != nil {
		t.Fatalf("list running execution containers: %v", err)
	}
	if len(running) != 0 {
		t.Fatalf("request wait has running workload containers %v", running)
	}
}

// assertRunningContainersPositiveControl proves RunningContainers observes a
// real running container selected by the execution label. It starts and removes
// its own probe container, so the empty result asserted during the request wait
// is meaningful instead of the vacuous output of a wrong label filter.
func assertRunningContainersPositiveControl(t *testing.T, harness *factory.Harness, executionID string) {
	t.Helper()
	const probeBase = "running-probe"
	// The base names the probe fixture; the run scope makes it a per-run
	// Podman name so two concurrent suite runs cannot force-remove each
	// other's probe.
	probeName := factory.ProbeContainerName(probeBase)
	ctx := t.Context()
	_ = harness.RemoveProbeContainer(ctx, probeName)
	if err := harness.StartProbeContainer(ctx, probeName, runtimepodman.LabelExecution, executionID); err != nil {
		t.Fatalf("start running-container positive control: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = harness.RemoveProbeContainer(cleanup, probeName)
	})
	harness.Eventually(t, 30*time.Second, func() bool {
		running, err := harness.RunningContainers(ctx, runtimepodman.LabelExecution, executionID)
		return err == nil && len(running) == 1 && running[0] == probeName
	})
	if err := harness.RemoveProbeContainer(ctx, probeName); err != nil {
		t.Fatalf("remove running-container positive control: %v", err)
	}
	harness.Eventually(t, 30*time.Second, func() bool {
		running, err := harness.RunningContainers(ctx, runtimepodman.LabelExecution, executionID)
		return err == nil && len(running) == 0
	})
}

// assertDeadlineRelation proves the persisted deadline is exactly the activation
// time plus the admitted timeout. Both fields are persisted from one activation
// clock reading, so a different relation means a wrong persisted value.
func assertDeadlineRelation(t *testing.T, action api.ExecutionAction) {
	t.Helper()
	if action.ActivatedAt == nil || action.Deadline == nil {
		t.Fatalf("action %q activated_at/deadline = %v/%v; want both persisted", action.Name, action.ActivatedAt, action.Deadline)
	}
	if action.TimeoutSeconds <= 0 {
		t.Fatalf("action %q timeout_seconds = %d; want a positive timeout", action.Name, action.TimeoutSeconds)
	}
	want := time.Duration(action.TimeoutSeconds) * time.Second
	if got := action.Deadline.Sub(*action.ActivatedAt); got != want {
		t.Fatalf("action %q deadline - activated_at = %s; want %s (activated_at=%s deadline=%s)",
			action.Name, got, want, action.ActivatedAt.UTC(), action.Deadline.UTC())
	}
}

// cleanupNoneCanBeTrue reports whether a "none" cleanup record is consistent
// with the execution's own action history: no container action may still be
// pending or running. An activated container action owns a stable workload
// identity whose container exists until terminal cleanup removes it, so the
// record alone is never enough; the caller also probes the live state.
func cleanupNoneCanBeTrue(execution api.ExecutionInspectResult) bool {
	for _, action := range execution.Actions {
		if action.Kind != "container" {
			continue
		}
		switch action.State {
		case "pending", "running":
			return false
		}
	}
	return true
}

// waitExecutionOptions configures the unified terminal-and-cleanup wait loop.
type waitExecutionOptions struct {
	// terminalTimeout bounds reaching a terminal execution state when
	// totalTimeout is zero. Defaults to 2 minutes.
	terminalTimeout time.Duration
	// cleanupTimeout bounds cleanup settlement after reaching a terminal state
	// when totalTimeout is zero. Defaults to 1 minute.
	cleanupTimeout time.Duration
	// totalTimeout, when positive, bounds both terminal arrival and cleanup
	// settlement under one shared wall-clock deadline.
	totalTimeout time.Duration
	// pollInterval sets the delay between inspections. Defaults to 10ms.
	pollInterval time.Duration
	// allowCleanupNone permits settling on cleanup.state == "none" after a
	// grace period when the execution has no active container action and the
	// live resource probe proves nothing remains.
	allowCleanupNone bool
	// onPendingCleanup is called while cleanup.state == "pending" so callers
	// with a frozen factory clock can advance or pace durable retry backoffs.
	onPendingCleanup func(api.ExecutionCleanup)
	// onFailure is invoked in the failure defer for backend-specific evidence
	// (for example, Kubernetes Jobs/Pods/logs).
	onFailure func(context.Context, api.ExecutionInspectResult)
}

// isTerminalExecutionState reports whether state is a terminal execution state.
func isTerminalExecutionState(state string) bool {
	switch state {
	case "succeeded", "failed", "cancelled":
		return true
	default:
		return false
	}
}

// waitExecution waits for one execution to reach a terminal state and for its
// independent cleanup record to settle.
//
// "complete" is the product's own claim that owned workloads were removed and
// the retained workspace was released, so it settles immediately. "pending"
// means cleanup is still owed. "none" claims no external resource requires
// cleanup; it is accepted only after a short grace (a terminal commit and the
// cleanup intent that follows it are separate commits) and only when the
// execution has no active container action and the live state proves the claim:
// no run-labelled container for the execution and no retained workspace volume.
// Anything else keeps waiting for "complete" instead of masking a leaked
// workload or workspace behind a "none" record.
//
// When the wait fails it logs the last inspection, the live resource probe and
// the retained workload stdout/stderr through the public logs operation, so a
// red test carries its own evidence.
func waitExecution(t *testing.T, harness *factory.Harness, token, executionID string) api.ExecutionInspectResult {
	t.Helper()
	return waitExecutionWithOptions(t, harness, token, executionID, waitExecutionOptions{
		allowCleanupNone: true,
	})
}

// waitExecutionWithOptions is the unified terminal-and-cleanup barrier shared
// by Podman, Kubernetes, and paced-retry lifecycle tests.
func waitExecutionWithOptions(t *testing.T, harness *factory.Harness, token, executionID string, opts waitExecutionOptions) api.ExecutionInspectResult {
	t.Helper()
	if opts.terminalTimeout <= 0 {
		opts.terminalTimeout = 2 * time.Minute
	}
	if opts.cleanupTimeout <= 0 {
		opts.cleanupTimeout = time.Minute
	}
	if opts.pollInterval <= 0 {
		opts.pollInterval = 10 * time.Millisecond
	}

	var last api.ExecutionInspectResult
	var resources executionResources
	probed := false
	defer func() {
		if t.Failed() {
			// The test context is already cancelled when a failed wait unwinds
			// through a cleanup, so the evidence reads take their own bound.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if !probed && harness.Cluster == nil {
				resources = probeExecutionResources(ctx, harness, executionID)
			}
			t.Logf("execution %s last inspect: state=%q version=%d cleanup=%+v failure=%+v actions=%+v",
				last.ExecutionID, last.State, last.Version, last.Cleanup, last.Failure, last.Actions)
			if harness.Cluster == nil {
				t.Logf("execution %s runtime evidence: %s", executionID, harness.ExecutionRuntimeEvidence(ctx, executionID))
				t.Logf("execution %s last live resources: %s", executionID, resources)
			}
			dumpExecutionLogs(t, harness, token, executionID, last)
			if opts.onFailure != nil {
				opts.onFailure(ctx, last)
			}
		}
	}()

	const noneGrace = 2 * time.Second
	now := time.Now()
	terminalDeadline := now.Add(opts.terminalTimeout)
	var cleanupDeadline time.Time
	if opts.totalTimeout > 0 {
		shared := now.Add(opts.totalTimeout)
		terminalDeadline = shared
		cleanupDeadline = shared
	}

	terminalReached := false
	var probeAfter time.Time
	probedNone := false

	for {
		inspected := false
		result, publicError := harness.REST(token, "execution.inspect", api.ExecutionInspectRequest{
			ExecutionID: executionID,
		}, nil, "")
		if publicError.Code == "" {
			harness.Decode(result, &last)
			inspected = true
			if !terminalReached && isTerminalExecutionState(last.State) {
				terminalReached = true
				at := time.Now()
				probeAfter = at.Add(noneGrace)
				if opts.totalTimeout <= 0 {
					cleanupDeadline = at.Add(opts.cleanupTimeout)
				}
			}
		}

		if terminalReached && inspected {
			switch last.Cleanup.State {
			case "complete":
				return last
			case "none":
				if opts.allowCleanupNone && !probedNone && !time.Now().Before(probeAfter) {
					probedNone = true
					probed = true
					resources = probeExecutionResources(t.Context(), harness, executionID)
					if cleanupNoneCanBeTrue(last) && resources.clean() {
						return last
					}
				}
			}
		}

		checkNow := time.Now()
		if !terminalReached {
			if !checkNow.Before(terminalDeadline) {
				bound := opts.terminalTimeout
				if opts.totalTimeout > 0 {
					bound = opts.totalTimeout
				}
				t.Fatalf("execution %s did not reach a terminal state within %s: state=%q cleanup=%+v\ncomposition log:\n%s",
					executionID, bound, last.State, last.Cleanup, factory.SanitizeDiagnostic(harness.Logs()))
			}
		} else if !checkNow.Before(cleanupDeadline) {
			if !probed && harness.Cluster == nil {
				probed = true
				resources = probeExecutionResources(t.Context(), harness, executionID)
			}
			bound := opts.cleanupTimeout
			if opts.totalTimeout > 0 {
				bound = opts.totalTimeout
			}
			t.Fatalf("execution %s reached state %q but cleanup did not settle within %s: cleanup=%+v; live resources: %s\ncomposition log:\n%s",
				executionID, last.State, bound, last.Cleanup, resources, factory.SanitizeDiagnostic(harness.Logs()))
		}

		if inspected && last.Cleanup.State == "pending" && opts.onPendingCleanup != nil {
			opts.onPendingCleanup(last.Cleanup)
		}
		time.Sleep(opts.pollInterval)
	}
}

// waitExecutionSettled waits for one execution's terminal cleanup to complete
// and then for the served execution view to become quiescent: cleanup complete
// with the same execution version observed by two consecutive reads.
//
// The served execution view is assembled from several rows, so one inspect can
// read the execution row before a cleanup commit and the cleanup row after it.
// That read reports cleanup complete with a version one commit stale. A second
// read that starts after the first completed starts after any commit the first
// read straddled, so an equal version proves the view is no longer straddling a
// commit. This is a barrier, never a fixed delay.
func waitExecutionSettled(t *testing.T, harness *factory.Harness, token, executionID string) api.ExecutionInspectResult {
	t.Helper()
	waitExecution(t, harness, token, executionID)
	var last api.ExecutionInspectResult
	var lastVersion uint64
	haveBaseline := false
	harness.Eventually(t, 2*time.Minute, func() bool {
		result, publicError := harness.REST(token, "execution.inspect", api.ExecutionInspectRequest{
			ExecutionID: executionID,
		}, nil, "")
		if publicError.Code != "" {
			haveBaseline = false
			return false
		}
		var current api.ExecutionInspectResult
		harness.Decode(result, &current)
		if current.Cleanup.State != "complete" {
			haveBaseline = false
			last = current
			return false
		}
		if haveBaseline && current.Version == lastVersion {
			last = current
			return true
		}
		haveBaseline = true
		lastVersion = current.Version
		last = current
		return false
	})
	return last
}

// assertExecutionResourcesReleased proves a terminal execution's cleanup
// removed every run-labelled container and released the retained workspace
// volume. Cleanup is separate from the execution outcome, but the contract
// still requires owned resources to be removed and the workspace released
// after a confirmed stop; this checks those external effects independently of
// the cleanup record's own state.
func assertExecutionResourcesReleased(t *testing.T, harness *factory.Harness, executionID string) {
	t.Helper()
	resources := probeExecutionResources(t.Context(), harness, executionID)
	if resources.err != nil {
		t.Fatalf("probe released resources of execution %s: %v", executionID, resources.err)
	}
	if len(resources.containers) != 0 {
		t.Fatalf("execution %s left run-labelled containers %v after terminal cleanup", executionID, resources.containers)
	}
	if resources.workspaceVolume {
		t.Fatalf("execution %s retained workspace volume %s after terminal cleanup", executionID, resources.workspaceID)
	}
}

// dumpExecutionLogs logs the retained stdout/stderr of every action of one
// execution. It is diagnostics for a failed wait, never an assertion: a refused
// or missing log read is logged and skipped, so it cannot mask the real failure.
func dumpExecutionLogs(t *testing.T, harness *factory.Harness, token, executionID string, execution api.ExecutionInspectResult) {
	t.Helper()
	actions := execution.Actions
	if len(actions) == 0 {
		result, publicError := harness.REST(token, "execution.inspect", api.ExecutionInspectRequest{
			ExecutionID: executionID,
		}, nil, "")
		if publicError.Code != "" {
			t.Logf("execution %s retained logs unavailable: inspect refused: %s (%s)", executionID, publicError.Code, publicError.Message)
			return
		}
		var current api.ExecutionInspectResult
		harness.Decode(result, &current)
		actions = current.Actions
	}
	for _, action := range actions {
		for _, stream := range []string{"stdout", "stderr"} {
			result, publicError := harness.REST(token, "execution.logs.read", api.ExecutionLogsReadRequest{
				ExecutionID: executionID,
				ActionID:    action.ActionID,
				Stream:      stream,
			}, nil, "")
			if publicError.Code != "" {
				t.Logf("execution %s action %s %s logs refused: %s (%s)", executionID, action.Name, stream, publicError.Code, publicError.Message)
				continue
			}
			var read api.ExecutionLogsReadResult
			harness.Decode(result, &read)
			if len(read.Chunks) == 0 {
				t.Logf("execution %s action %s %s logs: (no retained chunks)", executionID, action.Name, stream)
				continue
			}
			var builder strings.Builder
			for _, chunk := range read.Chunks {
				_, _ = builder.Write(chunk.Data)
			}
			t.Logf("execution %s action %s %s logs:\n%s", executionID, action.Name, stream, factory.SanitizeDiagnostic(builder.String()))
		}
	}
}

// actionByName returns one action observation by its admitted name.
func actionByName(t *testing.T, execution api.ExecutionInspectResult, name string) api.ExecutionAction {
	t.Helper()
	for _, action := range execution.Actions {
		if action.Name == name {
			return action
		}
	}
	t.Fatalf("execution %s has no action %q; actions=%+v", execution.ExecutionID, name, execution.Actions)
	return api.ExecutionAction{}
}

// requireSucceeded fails with the typed failure when the execution did not
// succeed.
func requireSucceeded(t *testing.T, execution api.ExecutionInspectResult) {
	t.Helper()
	if execution.State != "succeeded" {
		t.Fatalf("execution %s state = %q; failure=%+v; actions=%+v",
			execution.ExecutionID, execution.State, execution.Failure, execution.Actions)
	}
}

// assertSameBody proves two results name the same operation and carry an equal
// JSON body.
func assertSameBody(t *testing.T, left, right api.Result) {
	t.Helper()
	if left.Operation != right.Operation {
		t.Fatalf("operations left=%q right=%q", left.Operation, right.Operation)
	}
	var leftBody, rightBody any
	if err := json.Unmarshal(left.Body, &leftBody); err != nil {
		t.Fatalf("decode left body: %v; body=%s", err, string(left.Body))
	}
	if err := json.Unmarshal(right.Body, &rightBody); err != nil {
		t.Fatalf("decode right body: %v; body=%s", err, string(right.Body))
	}
	if !reflect.DeepEqual(leftBody, rightBody) {
		t.Fatalf("%s bodies differ:\nleft =%s\nright=%s", left.Operation, string(left.Body), string(right.Body))
	}
}

// submitTemplate is the workload-side output submission. It reads the mounted
// execution credential, inspects its own execution to learn the current
// execution version and its action id (KODEMA_ACTION is deliberately the action
// slug, not the id), then stages the declared output with the public envelope.
// It fails the workload on any refusal so a broken submission cannot look like
// a successful action.
const submitTemplate = `TOKEN=$(cat /kodema/secrets/KODEMA_TOKEN)
ENDPOINT=${KODEMA_URL#http://}
API_HOST=${ENDPOINT%:*}
API_PORT=${ENDPOINT##*:}
# api sends one public envelope over BusyBox nc. The request is written
# first and stdin is then held open for a bounded time: nc half-closes the
# connection as soon as its stdin reaches EOF, and Go's net/http server cancels
# the request context on a client half-close. Closing immediately cancelled the
# composition's credential lookup mid-flight and surfaced as the uniform
# "presented credential is invalid" even though the mounted token was valid.
api() {
  method=$1
  path=$2
  body=$3
  length=$(printf '%s' "$body" | wc -c)
  { printf '%s %s HTTP/1.0\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %s\r\nConnection: close\r\n\r\n%s' \
      "$method" "$path" "$ENDPOINT" "$TOKEN" "$length" "$body"; sleep 2; } \
    | nc -w 10 "$API_HOST" "$API_PORT" | tr -d '\r' | sed '1,/^$/d'
}
# The admitted execution version advances whenever the composition moves the
# execution (credential issuance, activation, settlement), so a legitimate
# submission can race it. Mirror submitExecutionOutputs in
# internal/shell/app/workload_bearer_test.go: re-inspect for the current
# version and retry a version_conflict with a fresh idempotency key, because
# execution.action.outputs is not idempotent across reused keys.
attempt=0
submitted=0
while [ "$attempt" -lt 20 ]; do
  attempt=$((attempt+1))
  inspect=$(api GET "/api/v1/executions/$KODEMA_EXECUTION" '{}')
  case "$inspect" in
    *'"operation":"execution.inspect"'*) ;;
    *) echo "execution inspect failed: $inspect" >&2; exit 1 ;;
  esac
  version=$(printf '%s' "$inspect" | grep -o '"version":[0-9]*' | head -n 1 | cut -d: -f2)
  action=$(printf '%s' "$inspect" | sed 's/},{/}\n{/g' | grep -F "\"name\":\"$KODEMA_ACTION\"" | grep -o '"action_id":"[^"]*"' | head -n 1 | cut -d'"' -f4)
  test -n "$version"
  test -n "$action"
  response=$(api PUT "/api/v1/executions/$KODEMA_EXECUTION/actions/$action/outputs" "{\"input\":{\"outputs\":{\"__OUTPUT_KEY__\":\"$__OUTPUT_VAR__\"}},\"expected_version\":$version,\"idempotency_key\":\"outputs:$KODEMA_EXECUTION:$action:$attempt\"}")
  case "$response" in
    *'"operation":"execution.action.outputs"'*) submitted=1; break ;;
    *version_conflict*) echo "execution outputs hit a version conflict; retrying with a fresh key" >&2 ;;
    *) echo "output submission failed: $response" >&2; exit 1 ;;
  esac
done
if [ "$submitted" -ne 1 ]; then
  echo "output submission did not converge on a current execution version" >&2
  exit 1
fi`

// submitScript returns the submission script for one declared output.
func submitScript(outputKey, outputVariable string) string {
	return strings.NewReplacer(
		"__OUTPUT_KEY__", outputKey,
		"__OUTPUT_VAR__", outputVariable,
	).Replace(submitTemplate)
}

// prepareChecks verifies the task-free context, the admitted input, the
// materialized secret and the shared workspace, then sets the first artifact.
func prepareChecks(seed string, secretLength int, secretPrefix string) string {
	return fmt.Sprintf(`test ! -e /kodema/task.json
test -z "${KODEMA_TASK:-}"
test "$(cat /kodema/inputs/seed)" = %q
test "$(wc -c < /kodema/secrets/PIPELINE_SECRET | tr -d ' ')" = %q
test "$(head -c 8 /kodema/secrets/PIPELINE_SECRET)" = %q
printf 'shared workspace\n' > /workspace/marker
ARTIFACT="built:$(cat /kodema/inputs/seed)"`, seed, strconv.Itoa(secretLength), secretPrefix)
}

// consumeChecks verifies the ordered materialization of the earlier succeeded
// output and the shared workspace, then sets the final output.
func consumeChecks(artifact string) string {
	return fmt.Sprintf(`test ! -e /kodema/task.json
test "$(cat /workspace/marker)" = "shared workspace"
test "$(cat /kodema/outputs/prepare/artifact)" = %q
FINAL="consumed:$(cat /kodema/outputs/prepare/artifact)"`, artifact)
}

// readActionStreamLogs drains every retained log page for one action and stream
// through the public execution.logs.read operation.
func readActionStreamLogs(t *testing.T, harness *factory.Harness, token, executionID, actionID, stream string) string {
	t.Helper()
	var builder strings.Builder
	cursor := ""
	for range 64 {
		req := api.ExecutionLogsReadRequest{
			ExecutionID: executionID,
			ActionID:    actionID,
			Stream:      stream,
		}
		if cursor != "" {
			req.Page = &api.Page{Cursor: cursor}
		}
		result := harness.MustREST(token, "execution.logs.read", req, nil, "")
		var page api.ExecutionLogsReadResult
		harness.Decode(result, &page)
		for _, chunk := range page.Chunks {
			_, _ = builder.Write(chunk.Data)
		}
		if page.NextCursor == "" {
			return builder.String()
		}
		cursor = page.NextCursor
	}
	t.Fatalf("execution.logs.read for %s/%s (%s) exceeded 64 pages", executionID, actionID, stream)
	return ""
}

// executionLogs concatenates the retained stdout and stderr of one action.
func executionLogs(t *testing.T, harness *factory.Harness, token, executionID, actionName string) string {
	t.Helper()
	execution := inspectExecution(t, harness, token, executionID)
	action := actionByName(t, execution, actionName)
	return readActionStreamLogs(t, harness, token, executionID, action.ActionID, "stdout") +
		readActionStreamLogs(t, harness, token, executionID, action.ActionID, "stderr")
}

// cancelExecutionCurrent cancels one execution from the latest inspected
// version, retrying a stale expected version from a fresh inspect.
func cancelExecutionCurrent(t *testing.T, harness *factory.Harness, token, executionID, key string) api.ExecutionCancelResult {
	t.Helper()
	for range 20 {
		current := inspectExecution(t, harness, harness.ProjectToken, executionID)
		result, publicError := harness.REST(token, "execution.cancel", api.ExecutionCancelRequest{
			ExecutionID: executionID,
		}, &current.Version, key)
		if publicError.Code == api.CodeVersionConflict {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if publicError.Code != "" {
			t.Fatalf("execution.cancel refused: %s (%s)", publicError.Code, publicError.Message)
		}
		var cancelled api.ExecutionCancelResult
		harness.Decode(result, &cancelled)
		return cancelled
	}
	t.Fatalf("execution.cancel did not converge on a current execution version")
	return api.ExecutionCancelResult{}
}

// invokeStartViaClient issues one pipeline.start through the public Go client
// at an explicit expected pipeline version, returning transport or API errors
// to the caller so lost-response paths can observe them.
func invokeStartViaClient(t *testing.T, transport *client.HTTPTransport, pipelineID string, inputs map[string]string, expected uint64, key string) (api.PipelineStartResult, error) {
	t.Helper()
	input, err := json.Marshal(api.PipelineStartRequest{
		ProjectID:  factory.ProjectID,
		PipelineID: pipelineID,
		Inputs:     inputs,
	})
	if err != nil {
		t.Fatalf("marshal start input: %v", err)
	}
	result, err := transport.Invoke(t.Context(), api.Invocation{
		Operation: "pipeline.start",
		Request: api.Request{
			Input:           input,
			ExpectedVersion: &expected,
			IdempotencyKey:  key,
		},
	})
	if err != nil {
		return api.PipelineStartResult{}, err
	}
	var started api.PipelineStartResult
	if err := json.Unmarshal(result.Body, &started); err != nil {
		t.Fatalf("decode client start: %v; body=%s", err, string(result.Body))
	}
	if err := started.Validate(); err != nil {
		t.Fatalf("client start result: %v", err)
	}
	return started, nil
}

// startViaClient starts one stored pipeline through the public Go client at its
// currently observed stored version.
func startViaClient(t *testing.T, transport *client.HTTPTransport, pipelineID string, inputs map[string]string, key string) api.PipelineStartResult {
	t.Helper()
	readInput, err := json.Marshal(api.PipelineReadRequest{
		ProjectID:  factory.ProjectID,
		PipelineID: pipelineID,
	})
	if err != nil {
		t.Fatalf("marshal read input: %v", err)
	}
	read, err := transport.Invoke(t.Context(), api.Invocation{
		Operation: "pipeline.read",
		Request:   api.Request{Input: readInput},
	})
	if err != nil {
		t.Fatalf("pipeline.read over client: %v", err)
	}
	var stored api.PipelineReadResult
	if err := json.Unmarshal(read.Body, &stored); err != nil {
		t.Fatalf("decode client read: %v; body=%s", err, string(read.Body))
	}
	if stored.Version == 0 {
		t.Fatalf("client pipeline.read %s version = 0", pipelineID)
	}
	started, err := invokeStartViaClient(t, transport, pipelineID, inputs, stored.Version, key)
	if err != nil {
		t.Fatalf("pipeline.start over client: %v", err)
	}
	return started
}

// invokeCancelViaClient issues one execution.cancel through the public Go
// client at an explicit expected execution version.
func invokeCancelViaClient(t *testing.T, transport *client.HTTPTransport, executionID string, expected uint64, key string) (api.ExecutionCancelResult, error) {
	t.Helper()
	input, err := json.Marshal(api.ExecutionCancelRequest{ExecutionID: executionID})
	if err != nil {
		t.Fatalf("marshal cancel input: %v", err)
	}
	result, err := transport.Invoke(t.Context(), api.Invocation{
		Operation: "execution.cancel",
		Request: api.Request{
			Input:           input,
			ExpectedVersion: &expected,
			IdempotencyKey:  key,
		},
	})
	if err != nil {
		return api.ExecutionCancelResult{}, err
	}
	var cancelled api.ExecutionCancelResult
	if err := json.Unmarshal(result.Body, &cancelled); err != nil {
		t.Fatalf("decode client cancel: %v; body=%s", err, string(result.Body))
	}
	if err := cancelled.Validate(); err != nil {
		t.Fatalf("client cancel result: %v", err)
	}
	return cancelled, nil
}

// newDroppingClient returns a public Go client that commits its next HTTP
// request on the server and then reports a transport failure instead of
// returning the response.
func newDroppingClient(harness *factory.Harness, token string, maxAttempts int) *client.HTTPTransport {
	transport := harness.Client(token)
	base := transport.RoundTripper
	if base == nil {
		base = http.DefaultTransport
	}
	transport.RoundTripper = &dropOnceRoundTripper{base: base}
	transport.Retry = client.RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Millisecond,
		MaxDelay:    time.Millisecond,
	}
	return transport
}

// inspectViaClient reads one execution through the public Go client.
func inspectViaClient(t *testing.T, transport *client.HTTPTransport, executionID string) api.Result {
	t.Helper()
	input, err := json.Marshal(api.ExecutionInspectRequest{ExecutionID: executionID})
	if err != nil {
		t.Fatalf("marshal inspect input: %v", err)
	}
	result, err := transport.Invoke(t.Context(), api.Invocation{
		Operation: "execution.inspect",
		Request:   api.Request{Input: input},
	})
	if err != nil {
		t.Fatalf("execution.inspect over client: %v", err)
	}
	return result
}

// assertExecutionCount asserts the exact number of executions persisted for one
// pipeline.
func assertExecutionCount(t *testing.T, harness *factory.Harness, pipelineID string, want int64) {
	t.Helper()
	count, err := harness.ExecutionCount(t.Context(), pipelineID)
	if err != nil {
		t.Fatalf("count executions for %s: %v", pipelineID, err)
	}
	if count != want {
		t.Fatalf("execution count for %s = %d; want %d", pipelineID, count, want)
	}
}

// assertReceiptCount asserts the exact number of durable idempotency receipts
// stored for key.
func assertReceiptCount(t *testing.T, harness *factory.Harness, key string, want int64) {
	t.Helper()
	count, err := harness.ReceiptCount(t.Context(), key)
	if err != nil {
		t.Fatalf("count receipts for %q: %v", key, err)
	}
	if count != want {
		t.Fatalf("receipt count for %q = %d; want %d", key, count, want)
	}
}

// assertConfirmedStop asserts that action recorded confirmed stop evidence and,
// when wantDetail is non-empty, that its detail contains wantDetail.
func assertConfirmedStop(t *testing.T, action api.ExecutionAction, wantDetail string) {
	t.Helper()
	if action.Stop == nil || action.Stop.State != "confirmed" {
		t.Fatalf("action %q stop evidence = %+v; want confirmed", action.Name, action.Stop)
	}
	if wantDetail != "" && !strings.Contains(action.Stop.Detail, wantDetail) {
		t.Fatalf("action %q stop detail = %q; want substring %q", action.Name, action.Stop.Detail, wantDetail)
	}
}

// assertCancelledExecution proves an execution settled cancelled without a
// failure record and, when requireCompleteCleanup is true, with complete cleanup.
func assertCancelledExecution(t *testing.T, execution api.ExecutionInspectResult, requireCompleteCleanup bool) {
	t.Helper()
	if execution.State != "cancelled" {
		t.Fatalf("execution %s state = %q; want cancelled", execution.ExecutionID, execution.State)
	}
	if execution.Failure != nil {
		t.Fatalf("cancelled execution %s carries a failure: %+v", execution.ExecutionID, execution.Failure)
	}
	if requireCompleteCleanup && execution.Cleanup.State != "complete" {
		t.Fatalf("cancelled execution %s cleanup = %+v; want complete", execution.ExecutionID, execution.Cleanup)
	}
}

// assertSingleRunLogs proves retained logs show exactly one command execution
// ("runs=1" and no replay "runs=2") along with all required markers and none of
// the forbidden markers.
func assertSingleRunLogs(t *testing.T, logs string, required, forbidden []string) {
	t.Helper()
	if got := strings.Count(logs, "runs="); got != 1 {
		t.Fatalf("retained log has %d runs lines; want exactly one:\n%s", got, logs)
	}
	if !strings.Contains(logs, "runs=1") {
		t.Fatalf("retained log does not show exactly one command execution (runs=1):\n%s", logs)
	}
	if strings.Contains(logs, "runs=2") {
		t.Fatalf("retained log shows a replayed workload (runs=2):\n%s", logs)
	}
	for _, want := range required {
		if !strings.Contains(logs, want) {
			t.Fatalf("retained log missing expected marker %q:\n%s", want, logs)
		}
	}
	for _, reject := range forbidden {
		if strings.Contains(logs, reject) {
			t.Fatalf("retained log contains forbidden marker %q:\n%s", reject, logs)
		}
	}
}

// deniedSecretOptions configures a harness with one allowed pipeline secret and
// one stored but unallowed secret so activation fails during preparation.
func deniedSecretOptions() []factory.Option {
	return []factory.Option{
		factory.WithSecretRules(secret.NameRule{
			Scope: tenancy.Scope{
				TenantID:  id.TenantID(factory.TenantID),
				ProjectID: id.ProjectID(factory.ProjectID),
			},
			Names: []id.SecretName{pipelineSecretName},
		}),
		factory.WithSecrets(map[string]string{
			pipelineSecretName: pipelineSecretValue,
			deniedSecretName:   deniedSecretValue,
		}),
	}
}

// deniedSecretPipeline renders a single-action pipeline that requests the
// unallowed secret so activation fails closed during preparation.
func deniedSecretPipeline(t *testing.T, harness *factory.Harness, timeout string) []byte {
	t.Helper()
	return marshalPipeline(t, pipelineDoc{
		Actions: []pipelineActionDoc{{
			Name:     "denied",
			Image:    harness.Image.Reference,
			Secrets:  []string{deniedSecretName},
			Commands: []string{"true"},
			Timeout:  timeout,
		}},
	})
}

// assertPreparationFailure verifies the shared contract for an execution whose
// activation fails during preparation before Start is issued.
func assertPreparationFailure(t *testing.T, execution api.ExecutionInspectResult, actionName, wantWorkspaceID string) {
	t.Helper()
	if execution.State != "failed" {
		t.Fatalf("preparation-failure execution state = %q; want failed", execution.State)
	}
	if execution.Failure == nil || execution.Failure.Kind != "preparation" {
		t.Fatalf("preparation-failure failure = %+v; want preparation", execution.Failure)
	}
	if execution.Cleanup.State != "complete" {
		t.Fatalf("preparation-failure cleanup = %+v; want complete", execution.Cleanup)
	}
	if execution.Cleanup.WorkloadID == "" {
		t.Fatal("preparation-failure cleanup did not name the failed identity")
	}
	if wantWorkspaceID != "" {
		if execution.Cleanup.WorkspaceID != wantWorkspaceID {
			t.Fatalf("preparation-failure cleanup workspace = %q; want %q", execution.Cleanup.WorkspaceID, wantWorkspaceID)
		}
	} else if execution.Cleanup.WorkspaceID == "" {
		t.Fatal("preparation-failure cleanup did not name the retained workspace")
	}
	action := actionByName(t, execution, actionName)
	if action.State != "failed" {
		t.Fatalf("preparation-failure action %q state = %q; want failed", actionName, action.State)
	}
	if action.Stop != nil && action.Stop.State == "confirmed" {
		t.Fatalf("preparation failure fabricated stop evidence: %+v", action.Stop)
	}
}

// dropOnceRoundTripper forwards the request to the real edge, reads and closes
// the response, and then reports a transport failure exactly once. The server
// has already committed the effect, so the client's retry exercises the
// lost-response-after-effect path.
type dropOnceRoundTripper struct {
	mu      sync.Mutex
	dropped bool
	base    http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (transport *dropOnceRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.mu.Lock()
	drop := !transport.dropped
	transport.dropped = true
	transport.mu.Unlock()

	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if !drop {
		return response, nil
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return nil, errors.New("simulated lost response after the server committed the effect")
}
