package runtimesupport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/test/support/infrastructure/harness"
)

// Shared baseline values used by StandardPrepareRequest and the S07
// container-runtime conformance assertions.
const (
	StandardKodemaURL   = "https://kodema.example.com"
	StandardActionName  = id.ActionName("build")
	StandardTaskID      = id.TaskID("task-1")
	StandardSecretName  = id.SecretName("API_KEY")
	StandardSecretValue = "s3cr3t-value"
	StandardCredential  = "tok-abc"
	RefreshedCredential = "tok-new"
)

// StandardPrepareRequest builds the shared baseline PrepareRequest with
// input-a, earlier/out-a, task.json, declared secret API_KEY, execution
// credential KODEMA_TOKEN, and literal env BUILD_MODE=release.
func StandardPrepareRequest(
	identity reconcile.EffectIdentity,
	workspaceID id.WorkspaceID,
	image string,
	commands []string,
) reconcile.PrepareRequest {
	return reconcile.PrepareRequest{
		Identity:    identity,
		WorkspaceID: workspaceID,
		ActionName:  StandardActionName,
		TaskID:      StandardTaskID,
		Container: definition.Container{
			Image:    image,
			Commands: commands,
			Env:      []definition.EnvVar{{Name: "BUILD_MODE", Value: "release"}},
			Secrets:  []id.SecretName{StandardSecretName},
		},
		Secrets: []reconcile.SecretValue{
			{Name: StandardSecretName, Value: StandardSecretValue},
		},
		Inputs: map[id.InputKey]string{
			"input-a": "line1\nline2\n",
		},
		Outputs: map[id.ActionName]engine.Outputs{
			"earlier": {"out-a": "earlier-output"},
		},
		Task:       []byte(`{"id":"task-1"}`),
		Credential: StandardCredential,
	}
}

// RequireFaultClass asserts that err is non-nil and carries the stable public
// fault class want.
func RequireFaultClass(t testing.TB, err error, want fault.Class) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil; want fault class %s", want)
	}
	class, ok := fault.ClassOf(err)
	if !ok {
		t.Fatalf("error %v carries no fault class; want %s", err, want)
	}
	if class != want {
		t.Fatalf("fault class = %s; want %s (error %v)", class, want, err)
	}
}

// ObservationKindIs returns a predicate matching one ObservationKind.
func ObservationKindIs(want engine.ObservationKind) func(engine.Observation) bool {
	return func(obs engine.Observation) bool { return obs.Kind == want }
}

// WaitForObservation polls rt.Observe until pred returns true or the timeout
// expires. Zero timeout/interval fall back to harness defaults.
func WaitForObservation(
	t testing.TB,
	ctx context.Context,
	rt reconcile.Runtime,
	req reconcile.ObserveRequest,
	timeout, interval time.Duration,
	pred func(engine.Observation) bool,
) engine.Observation {
	t.Helper()
	var last engine.Observation
	probe := func(ctx context.Context) error {
		observation, err := rt.Observe(ctx, req)
		if err != nil {
			return err
		}
		last = observation
		if !pred(observation) {
			return fmt.Errorf("observation = %s; waiting for predicate", observation.Kind)
		}
		return nil
	}
	opts := waitOptions(timeout, interval, 15*time.Second)
	if err := harness.WaitReady(ctx, harness.RealClock{}, "runtime observation", probe, opts); err != nil {
		t.Fatalf("wait for observation: %v (last %+v)", err, last)
	}
	return last
}

// WaitForStopConfirmed polls rt.Stop until it reports affirmatively confirmed
// whole-workload termination. Zero timeout/interval fall back to harness defaults.
func WaitForStopConfirmed(
	t testing.TB,
	ctx context.Context,
	rt reconcile.Runtime,
	req reconcile.StopRequest,
	timeout, interval time.Duration,
) engine.StopEvidence {
	t.Helper()
	var last engine.StopEvidence
	probe := func(ctx context.Context) error {
		evidence, err := rt.Stop(ctx, req)
		last = evidence
		if err != nil {
			return err
		}
		if !evidence.IsConfirmed() {
			return fmt.Errorf("stop still pending: %s", evidence.Detail)
		}
		return nil
	}
	opts := waitOptions(timeout, interval, 20*time.Second)
	if err := harness.WaitReady(ctx, harness.RealClock{}, "runtime stop barrier", probe, opts); err != nil {
		t.Fatalf("wait for confirmed stop: %v (last %+v)", err, last)
	}
	return last
}

// ReadAllLogs pages rt.Logs to completion and returns the accumulated stdout
// and stderr streams. Any read error fails the test.
func ReadAllLogs(
	t testing.TB,
	ctx context.Context,
	rt reconcile.Runtime,
	identity reconcile.EffectIdentity,
	limit int,
) (stdout, stderr string) {
	t.Helper()
	stdout, stderr, err := ReadAllLogsErr(ctx, rt, identity, limit)
	if err != nil {
		t.Fatalf("ReadAllLogs: %v", err)
	}
	return stdout, stderr
}

// ReadAllLogsErr pages rt.Logs to completion without calling t.Fatalf, so
// callers that inspect classified log errors or poll inside WaitReady can
// handle transient failures explicitly.
func ReadAllLogsErr(
	ctx context.Context,
	rt reconcile.Runtime,
	identity reconcile.EffectIdentity,
	limit int,
) (stdout, stderr string, err error) {
	var outBuf, errBuf strings.Builder
	var cursor int64
	for range 10000 {
		batch, readErr := rt.Logs(ctx, reconcile.LogsRequest{
			Identity:    identity,
			AfterOffset: cursor,
			Limit:       limit,
		})
		if readErr != nil {
			return outBuf.String(), errBuf.String(), fmt.Errorf("log read at offset %d: %w", cursor, readErr)
		}
		for _, chunk := range batch.Chunks {
			switch chunk.Stream {
			case reconcile.LogStdout:
				outBuf.Write(chunk.Data)
			case reconcile.LogStderr:
				errBuf.Write(chunk.Data)
			}
		}
		if batch.NextOffset == cursor {
			return outBuf.String(), errBuf.String(), nil
		}
		cursor = batch.NextOffset
	}
	return outBuf.String(), errBuf.String(), errors.New("log pagination did not terminate")
}

// ContainsLine reports whether text contains want as a complete newline-delimited line.
func ContainsLine(text, want string) bool {
	for line := range strings.SplitSeq(text, "\n") {
		if line == want {
			return true
		}
	}
	return false
}

// GenerateTestCAPEM returns a fresh self-signed x509 CA certificate encoded as PEM.
func GenerateTestCAPEM(t testing.TB, commonName string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate test CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create test CA certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func waitOptions(timeout, interval, longAttemptTimeout time.Duration) harness.WaitOptions {
	opts := harness.DefaultWaitOptions()
	if timeout > 0 {
		opts.Timeout = timeout
		if timeout >= time.Minute && longAttemptTimeout > opts.AttemptTimeout {
			opts.AttemptTimeout = longAttemptTimeout
		}
	}
	if interval > 0 {
		opts.InitialBackoff = interval
		if opts.MaxBackoff < interval {
			opts.MaxBackoff = interval
		}
	}
	return opts
}

// ContainerHarness parameterizes the shared S07 container-runtime contract
// assertions so real Podman and real Kubernetes component suites verify the
// exact same contract without drifting copies.
type ContainerHarness struct {
	Runtime       reconcile.Runtime
	Workspace     reconcile.Workspace
	WorkspaceID   id.WorkspaceID
	Image         string
	KodemaURL     string
	ActionTimeout time.Duration
	PollInterval  time.Duration

	// ExpectWorkloadCA, when true, asserts that /etc/kodema/ca/ca.crt is
	// mounted read-only and KODEMA_CA_FILE=/etc/kodema/ca/ca.crt overrides any
	// spoofed literal environment entry.
	ExpectWorkloadCA bool

	// NewIdentity mints one valid, scope-isolated EffectIdentity for label.
	NewIdentity func(t testing.TB, label string) reconcile.EffectIdentity

	// ReadWorkspaceFile reads one logical file relative to /workspace.
	ReadWorkspaceFile func(t testing.TB, ctx context.Context, path string) string

	// WaitRunning optionally waits until the workload container is actually
	// running (not merely Pending/scheduled) before live credential refresh.
	WaitRunning func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity)

	// BeforeRefresh optionally captures pre-refresh runtime state (container
	// ID, Job/Pod identity) and returns a check invoked after live refresh
	// confirms the workload observed the new credential without restarting.
	BeforeRefresh func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity) func(t testing.TB, ctx context.Context)

	// AssertNoRestart is an optional post-refresh assertion invoked after
	// RefreshCredential completes and the container observes the refreshed token.
	AssertNoRestart func(t testing.TB, identity reconcile.EffectIdentity)

	// AfterExit optionally runs driver-specific assertions after the workload
	// exits 0 and logs are checked, before Remove deletes the workload object.
	AfterExit func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity)

	// AfterRemove optionally runs post-Remove synchronization (such as waiting
	// for Kubernetes Job/Pod workspace references to clear).
	AfterRemove func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity)

	// ReleaseWorkspace optionally overrides Workspace.Release (for example
	// Kubernetes eventually-consistent PVC release).
	ReleaseWorkspace func(t testing.TB, ctx context.Context, workspaceID id.WorkspaceID)
}

func (h ContainerHarness) kodemaURL() string {
	if h.KodemaURL != "" {
		return h.KodemaURL
	}
	return StandardKodemaURL
}

func (h ContainerHarness) release(t testing.TB, ctx context.Context) {
	t.Helper()
	if h.ReleaseWorkspace != nil {
		h.ReleaseWorkspace(t, ctx, h.WorkspaceID)
		return
	}
	if err := h.Workspace.Release(ctx, h.WorkspaceID); err != nil {
		t.Fatalf("Release(%s): %v", h.WorkspaceID, err)
	}
}

func (h ContainerHarness) afterRemove(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity) {
	t.Helper()
	if h.AfterRemove != nil {
		h.AfterRemove(t, ctx, identity)
	}
}

// AssertMaterializationContract verifies the S07 container materialization
// contract end to end: /kodema/inputs/*, /kodema/outputs/*, /kodema/task.json,
// /kodema/task/{title,description,metadata.json,spec.json},
// /kodema/secrets/{API_KEY,KODEMA_TOKEN}, WorkingDir == /workspace, optional
// /etc/kodema/ca/ca.crt + $KODEMA_CA_FILE, env precedence, secret redaction in
// ReadAllLogs, exit 0, and Remove.
func AssertMaterializationContract(t testing.TB, ctx context.Context, h ContainerHarness) {
	t.Helper()
	identity := h.NewIdentity(t, "s07-mat")
	taskFiles, err := workspace.BuildTaskFiles(
		StandardTaskID,
		"Contract title",
		"Line one\nLine two",
		map[string]string{"area": "runtime", "tier": "s07"},
	)
	if err != nil {
		t.Fatalf("BuildTaskFiles: %v", err)
	}

	commands := []string{
		`test "$PWD" = /workspace`,
		`test ! -w /kodema/inputs/input-a`,
		`test ! -w /kodema/outputs/earlier/out-a`,
		`test ! -w /kodema/task.json`,
		`test ! -w /kodema/task/title && test ! -w /kodema/task/spec.json`,
		`test ! -w /kodema/secrets/API_KEY && test ! -w /kodema/secrets/KODEMA_TOKEN`,
		`test "$(tr '\n' '|' < /kodema/inputs/input-a)" = "line1|line2|"`,
		`test "$(cat /kodema/outputs/earlier/out-a)" = "earlier-output"`,
		`test "$(cat /kodema/task.json)" = '{"id":"task-1"}'`,
		`test "$(cat /kodema/task/title)" = "Contract title"`,
		`test "$(cat /kodema/task/description)" = "$(printf 'Line one\nLine two')"`,
		`test "$(cat /kodema/task/metadata.json)" = '{"area":"runtime","tier":"s07"}'`,
		`test "$(cat /kodema/task/spec.json)" = '{"id":"task-1","title":"Contract title","description":"Line one\nLine two","metadata":{"area":"runtime","tier":"s07"}}'`,
		`{ printf 'PWD=%s\n' "$PWD"; printf 'INPUT=%s\n' "$(tr '\n' '|' < /kodema/inputs/input-a)"; printf 'OUTPUT=%s\n' "$(cat /kodema/outputs/earlier/out-a)"; printf 'TASK=%s\n' "$(cat /kodema/task.json)"; env | sort | sed 's/^/ENV:/'; } > /workspace/contract.txt`,
		`cat /kodema/secrets/API_KEY`,
		`cat /kodema/secrets/KODEMA_TOKEN`,
		`printf '\ncontract-ok\n'`,
	}
	if h.ExpectWorkloadCA {
		commands = append([]string{
			`test "$KODEMA_CA_FILE" = "` + container.WorkloadCAPath + `"`,
			`test -s "` + container.WorkloadCAPath + `"`,
			`test ! -w "` + container.WorkloadCAPath + `"`,
		}, commands...)
	}

	req := StandardPrepareRequest(identity, h.WorkspaceID, h.Image, commands)
	req.TaskFiles = taskFiles
	if h.ExpectWorkloadCA {
		// Spoof KODEMA_CA_FILE in literal env to prove runtime env precedence
		// overrides it with /etc/kodema/ca/ca.crt.
		req.Container.Env = append(req.Container.Env,
			definition.EnvVar{Name: workspace.EnvCAFile, Value: "/tmp/spoofed-ca.crt"})
	}

	if _, err := h.Runtime.Prepare(ctx, req); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	start, err := h.Runtime.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: h.WorkspaceID})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if start.Outcome != reconcile.StartAccepted {
		t.Fatalf("start outcome = %q; want accepted (%s)", start.Outcome, start.Detail)
	}

	obs := WaitForObservation(t, ctx, h.Runtime, reconcile.ObserveRequest{
		Identity: identity, WorkspaceID: h.WorkspaceID,
	}, h.ActionTimeout, h.PollInterval, ObservationKindIs(engine.ObservationExited))
	stdout, stderr := ReadAllLogs(t, ctx, h.Runtime, identity, container.MaxLogBytes)
	combined := stdout + stderr
	if code, ok := obs.ExitStatus(); !ok || code != 0 {
		t.Fatalf("exit status = %d/%v; want 0/true (%+v)\nlogs:\n%s", code, ok, obs, combined)
	}
	if !obs.StopConfirmed() {
		t.Fatalf("exited observation has no confirmed stop: %+v", obs)
	}
	if strings.Contains(combined, StandardSecretValue) || strings.Contains(combined, StandardCredential) {
		t.Fatalf("secret or credential leaked into logs: %q", combined)
	}
	if !strings.Contains(combined, container.Redacted) {
		t.Fatalf("redaction marker %q is absent from logs: %q", container.Redacted, combined)
	}
	if !ContainsLine(combined, "contract-ok") {
		t.Fatalf("logs missing contract-ok marker: %q", combined)
	}

	if h.AfterExit != nil {
		h.AfterExit(t, ctx, identity)
	}

	removal, err := h.Runtime.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: h.WorkspaceID})
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !removal.Removed {
		t.Fatalf("removal = %+v; want removed", removal)
	}
	h.afterRemove(t, ctx, identity)

	if h.ReadWorkspaceFile != nil {
		report := h.ReadWorkspaceFile(t, ctx, "contract.txt")
		wantLines := []string{
			"PWD=/workspace",
			"INPUT=line1|line2|",
			"OUTPUT=earlier-output",
			`TASK={"id":"task-1"}`,
			"ENV:BUILD_MODE=release",
			"ENV:KODEMA_URL=" + h.kodemaURL(),
			"ENV:KODEMA_EXECUTION=" + string(identity.ExecutionID),
			"ENV:KODEMA_ACTION=" + string(StandardActionName),
			"ENV:KODEMA_TASK=" + string(StandardTaskID),
		}
		if h.ExpectWorkloadCA {
			wantLines = append(wantLines, "ENV:"+workspace.EnvCAFile+"="+container.WorkloadCAPath)
		}
		for _, want := range wantLines {
			if !ContainsLine(report, want) {
				t.Fatalf("contract report missing %q:\n%s", want, report)
			}
		}
		if h.ExpectWorkloadCA && strings.Contains(report, "/tmp/spoofed-ca.crt") {
			t.Fatalf("spoofed %s leaked into container env:\n%s", workspace.EnvCAFile, report)
		}
		for _, forbidden := range []string{
			"KODEMA_HOST_CANARY",
			StandardSecretValue,
			StandardCredential,
			"ENV:API_KEY=",
			"ENV:KODEMA_TOKEN=",
		} {
			if strings.Contains(report, forbidden) {
				t.Fatalf("forbidden value %q appeared in container env:\n%s", forbidden, report)
			}
		}
	}
}

// AssertWorkspacePersistence verifies that two sequential actions share
// /workspace across Remove, and that Release then removes workspace
// availability.
func AssertWorkspacePersistence(t testing.TB, ctx context.Context, h ContainerHarness) {
	t.Helper()
	first := h.NewIdentity(t, "s07-ws-1")
	firstReq := StandardPrepareRequest(first, h.WorkspaceID, h.Image, []string{
		`printf 'persisted\n' > /workspace/persist.txt`,
		`printf 'first\n' >> /workspace/launch-count`,
	})
	firstReq.Secrets = nil
	firstReq.Container.Secrets = nil
	if _, err := h.Runtime.Prepare(ctx, firstReq); err != nil {
		t.Fatalf("first Prepare: %v", err)
	}
	if start, err := h.Runtime.Start(ctx, reconcile.StartRequest{Identity: first, WorkspaceID: h.WorkspaceID}); err != nil || start.Outcome != reconcile.StartAccepted {
		t.Fatalf("first Start = %+v, %v; want accepted", start, err)
	}
	obs := WaitForObservation(t, ctx, h.Runtime, reconcile.ObserveRequest{
		Identity: first, WorkspaceID: h.WorkspaceID,
	}, h.ActionTimeout, h.PollInterval, ObservationKindIs(engine.ObservationExited))
	if code, ok := obs.ExitStatus(); !ok || code != 0 {
		t.Fatalf("first exit = %d/%v; want 0/true (%+v)", code, ok, obs)
	}
	if removal, err := h.Runtime.Remove(ctx, reconcile.RemoveRequest{Identity: first, WorkspaceID: h.WorkspaceID}); err != nil || !removal.Removed {
		t.Fatalf("first Remove = %+v, %v; want removed", removal, err)
	}
	h.afterRemove(t, ctx, first)

	second := h.NewIdentity(t, "s07-ws-2")
	secondReq := StandardPrepareRequest(second, h.WorkspaceID, h.Image, []string{
		`test "$(cat /workspace/persist.txt)" = "persisted"`,
		`cat /workspace/persist.txt > /workspace/readback.txt`,
		`printf 'second\n' >> /workspace/launch-count`,
		`cat /workspace/persist.txt`,
		`cat /workspace/launch-count`,
	})
	secondReq.Secrets = nil
	secondReq.Container.Secrets = nil
	if _, err := h.Runtime.Prepare(ctx, secondReq); err != nil {
		t.Fatalf("second Prepare: %v", err)
	}
	if start, err := h.Runtime.Start(ctx, reconcile.StartRequest{Identity: second, WorkspaceID: h.WorkspaceID}); err != nil || start.Outcome != reconcile.StartAccepted {
		t.Fatalf("second Start = %+v, %v; want accepted", start, err)
	}
	obs = WaitForObservation(t, ctx, h.Runtime, reconcile.ObserveRequest{
		Identity: second, WorkspaceID: h.WorkspaceID,
	}, h.ActionTimeout, h.PollInterval, ObservationKindIs(engine.ObservationExited))
	stdout, stderr := ReadAllLogs(t, ctx, h.Runtime, second, container.MaxLogBytes)
	combined := stdout + stderr
	if code, ok := obs.ExitStatus(); !ok || code != 0 {
		t.Fatalf("second exit = %d/%v; want 0/true (%+v)\nlogs:\n%s", code, ok, obs, combined)
	}
	if !ContainsLine(combined, "persisted") || !strings.Contains(combined, "first\nsecond") {
		t.Fatalf("second workload logs did not observe persisted workspace content:\n%s", combined)
	}
	if removal, err := h.Runtime.Remove(ctx, reconcile.RemoveRequest{Identity: second, WorkspaceID: h.WorkspaceID}); err != nil || !removal.Removed {
		t.Fatalf("second Remove = %+v, %v; want removed", removal, err)
	}
	h.afterRemove(t, ctx, second)

	if h.ReadWorkspaceFile != nil {
		if got := strings.TrimSpace(h.ReadWorkspaceFile(t, ctx, "launch-count")); got != "first\nsecond" {
			t.Fatalf("workspace launch-count = %q; want first then second", got)
		}
	}

	if h.Workspace != nil {
		status, err := h.Workspace.Status(ctx, h.WorkspaceID)
		if err != nil {
			t.Fatalf("Status before Release: %v", err)
		}
		if !status.Available {
			t.Fatalf("workspace status = %+v; want available before Release", status)
		}
		h.release(t, ctx)
		status, err = h.Workspace.Status(ctx, h.WorkspaceID)
		if err != nil {
			t.Fatalf("Status after Release: %v", err)
		}
		if status.Available {
			t.Fatalf("released workspace is still available: %+v", status)
		}
	}
}

// AssertLiveCredentialRefresh verifies that RefreshCredential updates
// /kodema/secrets/KODEMA_TOKEN inside a running container without restarting it.
func AssertLiveCredentialRefresh(t testing.TB, ctx context.Context, h ContainerHarness) {
	t.Helper()
	identity := h.NewIdentity(t, "s07-cred")
	req := StandardPrepareRequest(identity, h.WorkspaceID, h.Image, []string{
		`trap 'exit 0' TERM INT`,
		`if [ "$(cat /kodema/secrets/KODEMA_TOKEN)" != "` + StandardCredential + `" ]; then exit 11; fi`,
		`printf 'cred-ready-before\n'`,
		`i=0`,
		`while [ "$i" -lt 180 ]; do`,
		`  if [ "$(cat /kodema/secrets/KODEMA_TOKEN)" = "` + RefreshedCredential + `" ]; then`,
		`    printf 'cred-ready-after\n'`,
		`    while true; do sleep 1; done`,
		`  fi`,
		`  i=$((i+1))`,
		`  sleep 1`,
		`done`,
		`exit 12`,
	})
	req.Secrets = nil
	req.Container.Secrets = nil
	req.Container.Env = nil

	if _, err := h.Runtime.Prepare(ctx, req); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if start, err := h.Runtime.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: h.WorkspaceID}); err != nil || start.Outcome != reconcile.StartAccepted {
		t.Fatalf("Start = %+v, %v; want accepted", start, err)
	}
	WaitForObservation(t, ctx, h.Runtime, reconcile.ObserveRequest{
		Identity: identity, WorkspaceID: h.WorkspaceID,
	}, h.ActionTimeout, h.PollInterval, ObservationKindIs(engine.ObservationRunning))
	if h.WaitRunning != nil {
		h.WaitRunning(t, ctx, identity)
	}

	waitForLogLine(t, ctx, h, identity, "cred-ready-before")

	var postRefreshCheck func(testing.TB, context.Context)
	if h.BeforeRefresh != nil {
		postRefreshCheck = h.BeforeRefresh(t, ctx, identity)
	}

	refresh, err := h.Runtime.RefreshCredential(ctx, reconcile.CredentialRequest{
		Identity:   identity,
		Credential: RefreshedCredential,
	})
	if err != nil {
		t.Fatalf("RefreshCredential: %v", err)
	}
	if !refresh.Delivered {
		t.Fatalf("RefreshCredential = %+v; want delivered", refresh)
	}

	waitForLogLine(t, ctx, h, identity, "cred-ready-after")

	if postRefreshCheck != nil {
		postRefreshCheck(t, ctx)
	}
	if h.AssertNoRestart != nil {
		h.AssertNoRestart(t, identity)
	}

	evidence := WaitForStopConfirmed(t, ctx, h.Runtime, reconcile.StopRequest{
		Identity: identity, WorkspaceID: h.WorkspaceID,
	}, h.ActionTimeout, h.PollInterval)
	if !evidence.IsConfirmed() {
		t.Fatalf("stop evidence = %+v; want confirmed", evidence)
	}
	if removal, err := h.Runtime.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: h.WorkspaceID}); err != nil || !removal.Removed {
		t.Fatalf("Remove = %+v, %v; want removed", removal, err)
	}
	h.afterRemove(t, ctx, identity)
}

func waitForLogLine(t testing.TB, ctx context.Context, h ContainerHarness, identity reconcile.EffectIdentity, marker string) {
	t.Helper()
	var last string
	probe := func(ctx context.Context) error {
		stdout, stderr, err := ReadAllLogsErr(ctx, h.Runtime, identity, container.MaxLogBytes)
		if err != nil {
			return err
		}
		last = stdout + stderr
		if !ContainsLine(last, marker) {
			return fmt.Errorf("log does not contain %q yet", marker)
		}
		return nil
	}
	opts := waitOptions(h.ActionTimeout, h.PollInterval, 15*time.Second)
	if err := harness.WaitReady(ctx, harness.RealClock{}, "log marker "+marker, probe, opts); err != nil {
		t.Fatalf("wait for log marker %q: %v (last logs %q)", marker, err, last)
	}
}

// AssertAbsentObservation verifies that Observe on an absent workload reports
// engine.ObservationAbsent and never collapses into ObservationNeverStarted.
func AssertAbsentObservation(t testing.TB, ctx context.Context, h ContainerHarness) {
	t.Helper()
	identity := h.NewIdentity(t, "s07-absent")
	obs, err := h.Runtime.Observe(ctx, reconcile.ObserveRequest{
		Identity:    identity,
		WorkspaceID: h.WorkspaceID,
	})
	if err != nil {
		t.Fatalf("Observe(absent): %v", err)
	}
	if obs.Kind != engine.ObservationAbsent {
		t.Fatalf("Observe(absent) = %+v; want %s", obs, engine.ObservationAbsent)
	}
	if obs.Kind == engine.ObservationNeverStarted || obs.ProofOfNonexecution() {
		t.Fatalf("Observe(absent) collapsed into nonexecution proof: %+v", obs)
	}
}
