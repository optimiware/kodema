// Package containertest holds the shared behavioral conformance suite every
// container runtime adapter (Podman, Kubernetes) must satisfy.
//
// Each adapter supplies its own deterministic fake harness through Run, so
// shared S07 §1–§3 invariants (activation materialization, environment & CA &
// Git & LLM proxy injection, WorkingDir & launcher entrypoint, labels, log
// redaction & pagination & fail-closed behavior, 5-op workspace ownership,
// absent-object observation, cancellation, live credential refresh, concurrent
// races, and named cache mounts) are tested once here instead of being
// duplicated across every driver's unit tests.
package containertest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/engine/script"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// Options configures optional installation features for a conformance fixture.
type Options struct {
	WithCA         bool
	WithToolbox    bool
	GitURL         string
	WithCacheClaim bool
}

// LaunchedWorkload is the normalized view of a launched container/Job spec and
// its materialized activation & secret files.
type LaunchedWorkload struct {
	WorkingDir      string
	Command         []string
	FrameStreams    bool
	Env             map[string]string
	EnvList         []string
	Labels          map[string]string
	ActivationFiles map[string][]byte
	SecretFiles     map[string][]byte
	HasCAMount      bool
	CAMountPath     string
	HasToolboxMount bool
	HasOutputDir    bool
	OutputDirPath   string
	CacheMounts     map[string]string
}

// Fixture adapts one concrete container runtime adapter and its deterministic
// fake to the shared conformance suite.
type Fixture interface {
	Client() runtime.Client
	Workspace() reconcile.Workspace
	KodemaURL() string
	Request() reconcile.PrepareRequest
	InspectLaunched(t *testing.T, identity reconcile.EffectIdentity) LaunchedWorkload
	SeedRunning(t *testing.T, identity reconcile.EffectIdentity)
	SeedExited(t *testing.T, identity reconcile.EffectIdentity, exitCode int)
	AppendLog(t *testing.T, identity reconcile.EffectIdentity, stream reconcile.LogStream, data []byte)
	BreakRedactionSource(t *testing.T, identity reconcile.EffectIdentity)
	RemoveWorkspace(t *testing.T, ws id.WorkspaceID)
	AddWorkspace(t *testing.T, ws id.WorkspaceID)
	ReadCredential(t *testing.T, identity reconcile.EffectIdentity) string
	LaunchCount() int
	ObjectCount() int
}

// Factory constructs one isolated conformance fixture per subtest.
type Factory func(t *testing.T, opts Options) Fixture

// Run executes the complete shared container-runtime conformance suite against
// one adapter factory.
func Run(t *testing.T, factory Factory) {
	t.Run("ActivationMaterializationAndConfinement", func(t *testing.T) {
		runActivationTests(t, factory)
	})
	t.Run("EnvironmentCAGitAndLLMProxy", func(t *testing.T) {
		runEnvironmentTests(t, factory)
	})
	t.Run("WorkingDirAndLauncherEntrypoint", func(t *testing.T) {
		runLauncherTests(t, factory)
	})
	t.Run("WorkloadLabels", func(t *testing.T) {
		runLabelTests(t, factory)
	})
	t.Run("LogRedactionOrderingCursorsAndFailClosed", func(t *testing.T) {
		runLogTests(t, factory)
	})
	t.Run("WorkspaceOwnershipAllFiveOps", func(t *testing.T) {
		runWorkspaceOwnershipTests(t, factory)
	})
	t.Run("LifecycleCancellationRefreshAndRaces", func(t *testing.T) {
		runLifecycleAndRaceTests(t, factory)
	})
	t.Run("NamedCacheMounts", func(t *testing.T) {
		runCacheTests(t, factory)
	})
	t.Run("WorkspacePort", func(t *testing.T) {
		runWorkspacePortTests(t, factory)
	})
}

func requireFault(t *testing.T, err error, want fault.Class, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: error = nil; want fault class %q", what, want)
	}
	got, ok := fault.ClassOf(err)
	if !ok || got != want {
		t.Fatalf("%s: fault class = (%q, %v) for %v; want %q", what, got, ok, err, want)
	}
}

func mustPrepare(t *testing.T, fx Fixture, ctx context.Context, req reconcile.PrepareRequest) reconcile.Preparation {
	t.Helper()
	prep, err := fx.Client().Prepare(ctx, req)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return prep
}

func mustStart(t *testing.T, fx Fixture, ctx context.Context, req reconcile.PrepareRequest) reconcile.StartResult {
	t.Helper()
	res, err := fx.Client().Start(ctx, reconcile.StartRequest{
		Identity:    req.Identity,
		WorkspaceID: req.WorkspaceID,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if res.Outcome != reconcile.StartAccepted {
		t.Fatalf("Start outcome = %q; want %q", res.Outcome, reconcile.StartAccepted)
	}
	return res
}

// ---------------------------------------------------------------------------
// 1. Activation materialization & confinement
// ---------------------------------------------------------------------------

func runActivationTests(t *testing.T, factory Factory) {
	t.Run("materializes inputs earlier outputs task files secrets credential and script", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		taskFiles, err := workspace.BuildTaskFiles(
			req.TaskID,
			"Conformance title",
			"Conformance description",
			map[string]string{"b": "2", "a": "1"},
		)
		if err != nil {
			t.Fatalf("BuildTaskFiles: %v", err)
		}
		req.Task = []byte(`{"id":"task-1"}`)
		req.TaskFiles = taskFiles

		prep := mustPrepare(t, fx, context.Background(), req)
		if prep.WorkspaceID != req.WorkspaceID {
			t.Fatalf("Preparation.WorkspaceID = %q; want %q", prep.WorkspaceID, req.WorkspaceID)
		}
		if prep.Detail == "" {
			t.Fatal("Preparation.Detail is empty")
		}
		if fx.LaunchCount() != 0 {
			t.Fatalf("Prepare launched %d workloads; want 0", fx.LaunchCount())
		}

		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		wantScript, err := script.Render(req.Container.Commands)
		if err != nil {
			t.Fatalf("script.Render: %v", err)
		}
		wantActivation := map[string]string{
			"/kodema/inputs/input-a":       req.Inputs["input-a"],
			"/kodema/outputs/clone/commit": req.Outputs["clone"]["commit"],
			workspace.TaskPath:             `{"id":"task-1"}`,
			workspace.TaskTitlePath:        "Conformance title",
			workspace.TaskDescriptionPath:  "Conformance description",
			workspace.TaskMetadataPath:     `{"a":"1","b":"2"}`,
			workspace.TaskSpecPath:         `{"id":"task-1","title":"Conformance title","description":"Conformance description","metadata":{"a":"1","b":"2"}}`,
			container.WorkloadScriptPath:   wantScript,
		}
		if len(w.ActivationFiles) != len(wantActivation) {
			t.Fatalf("activation files = %d (%v); want %d", len(w.ActivationFiles), mapKeys(w.ActivationFiles), len(wantActivation))
		}
		for path, want := range wantActivation {
			got, ok := w.ActivationFiles[path]
			if !ok {
				t.Errorf("missing activation file %q", path)
				continue
			}
			if string(got) != want {
				t.Errorf("activation file %q = %q; want %q", path, got, want)
			}
			if strings.HasPrefix(path, workspace.WorkspaceRoot) {
				t.Errorf("activation file %q lives inside /workspace", path)
			}
			if strings.Contains(string(got), req.Credential) {
				t.Errorf("activation file %q leaked credential", path)
			}
		}

		if got := string(w.SecretFiles["API_KEY"]); got != "s3cr3t" {
			t.Errorf("secret API_KEY = %q; want s3cr3t", got)
		}
		if got := string(w.SecretFiles[id.SecretTokenName]); got != req.Credential {
			t.Errorf("secret KODEMA_TOKEN = %q; want %q", got, req.Credential)
		}
		if len(w.SecretFiles) != 2 {
			t.Errorf("secret files = %v; want exactly API_KEY and KODEMA_TOKEN", mapKeys(w.SecretFiles))
		}

		// Secret values must never appear in env, command, or labels.
		serialized := strings.Join(append(append([]string{}, w.EnvList...), w.Command...), "\n")
		for _, forbidden := range []string{"s3cr3t", req.Credential} {
			if strings.Contains(serialized, forbidden) {
				t.Fatalf("launched spec leaked %q", forbidden)
			}
			for k, v := range w.Labels {
				if strings.Contains(k+v, forbidden) {
					t.Fatalf("launched labels leaked %q", forbidden)
				}
			}
		}
	})

	t.Run("task-free workload omits task files and KODEMA_TASK and refuses stray TaskFiles", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		req.TaskID = ""
		req.Task = nil
		req.TaskFiles = nil

		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		if _, ok := w.ActivationFiles[workspace.TaskPath]; ok {
			t.Fatalf("task-free workload materialized %q", workspace.TaskPath)
		}
		for path := range w.ActivationFiles {
			if strings.HasPrefix(path, workspace.TaskRoot+"/") {
				t.Fatalf("task-free workload materialized %q", path)
			}
		}
		if _, ok := w.Env[id.EnvTask]; ok {
			t.Fatalf("task-free workload injected %s", id.EnvTask)
		}

		// Stray TaskFiles on a task-free request must be refused.
		fx2 := factory(t, Options{})
		req2 := fx2.Request()
		req2.TaskID = ""
		req2.Task = nil
		taskFiles, err := workspace.BuildTaskFiles("task-1", "t", "d", nil)
		if err != nil {
			t.Fatalf("BuildTaskFiles: %v", err)
		}
		req2.TaskFiles = taskFiles
		_, err = fx2.Client().Prepare(context.Background(), req2)
		requireFault(t, err, fault.InvalidInput, "stray TaskFiles when task-free")
	})

	t.Run("idempotent Prepare retry and ChangedIdempotencyContent on mutations", func(t *testing.T) {
		fx := factory(t, Options{})
		base := fx.Request()
		taskFiles, err := workspace.BuildTaskFiles(base.TaskID, "orig-title", "orig-desc", map[string]string{"k": "v"})
		if err != nil {
			t.Fatalf("BuildTaskFiles: %v", err)
		}
		base.TaskFiles = taskFiles

		mustPrepare(t, fx, context.Background(), base)
		// Identical retry succeeds idempotently.
		mustPrepare(t, fx, context.Background(), base)

		mutations := []struct {
			name   string
			mutate func(*reconcile.PrepareRequest)
		}{
			{"mutated input", func(r *reconcile.PrepareRequest) {
				r.Inputs = map[id.InputKey]string{"input-a": "mutated\n"}
			}},
			{"mutated task files", func(r *reconcile.PrepareRequest) {
				r.TaskFiles = map[string][]byte{
					workspace.TaskTitleFile:       []byte("changed-title"),
					workspace.TaskDescriptionFile: taskFiles[workspace.TaskDescriptionFile],
					workspace.TaskMetadataFile:    taskFiles[workspace.TaskMetadataFile],
					workspace.TaskSpecFile:        taskFiles[workspace.TaskSpecFile],
				}
			}},
			{"mutated image", func(r *reconcile.PrepareRequest) {
				r.Container.Image = "registry.example.com/tools/build@sha256:" + strings.Repeat("f", 64)
			}},
			{"mutated commands", func(r *reconcile.PrepareRequest) {
				r.Container.Commands = []string{"echo changed"}
			}},
			{"mutated env", func(r *reconcile.PrepareRequest) {
				r.Container.Env = []definition.EnvVar{{Name: "BUILD_MODE", Value: "debug"}}
			}},
			{"mutated secret", func(r *reconcile.PrepareRequest) {
				r.Secrets = []reconcile.SecretValue{{Name: "API_KEY", Value: "rotated"}}
			}},
			{"mutated outputs", func(r *reconcile.PrepareRequest) {
				r.Container.Outputs = []id.OutputKey{"different-out"}
			}},
			{"mutated idempotency key", func(r *reconcile.PrepareRequest) {
				r.Identity.IdempotencyKey = "idem-changed"
			}},
		}
		for _, tc := range mutations {
			t.Run(tc.name, func(t *testing.T) {
				mutated := clonePrepareRequest(base)
				mutated.Credential = "tok-should-not-overwrite"
				tc.mutate(&mutated)
				_, err := fx.Client().Prepare(context.Background(), mutated)
				requireFault(t, err, fault.ChangedIdempotencyContent, tc.name)
				if got := fx.ReadCredential(t, base.Identity); got != base.Credential {
					t.Fatalf("refused Prepare overwrote credential: got %q, want %q", got, base.Credential)
				}
			})
		}

		// Original request still starts cleanly after refused mutations.
		mustStart(t, fx, context.Background(), base)
		if fx.ObjectCount() != 1 {
			t.Fatalf("ObjectCount = %d; want 1", fx.ObjectCount())
		}
	})

	t.Run("refuses unpinned image reserved env and invalid input", func(t *testing.T) {
		cases := []struct {
			name   string
			mutate func(*reconcile.PrepareRequest)
		}{
			{"empty image", func(r *reconcile.PrepareRequest) { r.Container.Image = "   " }},
			{"bare image", func(r *reconcile.PrepareRequest) { r.Container.Image = "busybox" }},
			{"tag-only image", func(r *reconcile.PrepareRequest) { r.Container.Image = "alpine:3.24" }},
			{"reserved env", func(r *reconcile.PrepareRequest) {
				r.Container.Env = append(r.Container.Env, definition.EnvVar{Name: id.EnvAction, Value: "spoofed"})
			}},
			{"invalid env name", func(r *reconcile.PrepareRequest) {
				r.Container.Env = []definition.EnvVar{{Name: "BAD NAME", Value: "x"}}
			}},
			{"empty commands", func(r *reconcile.PrepareRequest) { r.Container.Commands = nil }},
			{"NUL in command", func(r *reconcile.PrepareRequest) { r.Container.Commands = []string{"echo\x00bad"} }},
			{"traversal input key", func(r *reconcile.PrepareRequest) {
				r.Inputs = map[id.InputKey]string{"../../etc/passwd": "x"}
			}},
			{"empty credential", func(r *reconcile.PrepareRequest) { r.Credential = "" }},
			{"invalid action name", func(r *reconcile.PrepareRequest) { r.ActionName = "not a slug" }},
			{"invalid task id", func(r *reconcile.PrepareRequest) { r.TaskID = "../escape" }},
			{"reserved secret name", func(r *reconcile.PrepareRequest) {
				r.Secrets = append(r.Secrets, reconcile.SecretValue{Name: id.SecretTokenName, Value: "spoofed"})
			}},
			{"invalid output action", func(r *reconcile.PrepareRequest) {
				r.Outputs = map[id.ActionName]engine.Outputs{"not a slug": {"out-a": "x"}}
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fx := factory(t, Options{})
				req := fx.Request()
				tc.mutate(&req)
				_, err := fx.Client().Prepare(context.Background(), req)
				requireFault(t, err, fault.InvalidInput, tc.name)
				if fx.LaunchCount() != 0 {
					t.Fatalf("%s launched %d workloads", tc.name, fx.LaunchCount())
				}
			})
		}
	})

	t.Run("missing workspace refused with NotFound and never recreated", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		fx.RemoveWorkspace(t, req.WorkspaceID)

		_, err := fx.Client().Prepare(context.Background(), req)
		requireFault(t, err, fault.NotFound, "Prepare on missing workspace")

		obs, err := fx.Workspace().Status(context.Background(), req.WorkspaceID)
		if err != nil {
			t.Fatalf("Workspace.Status: %v", err)
		}
		if obs.Available {
			t.Fatal("Prepare recreated a missing workspace")
		}

		// Workspace vanished after Prepare must also refuse Start with NotFound.
		fx2 := factory(t, Options{})
		req2 := fx2.Request()
		mustPrepare(t, fx2, context.Background(), req2)
		fx2.RemoveWorkspace(t, req2.WorkspaceID)

		res, err := fx2.Client().Start(context.Background(), reconcile.StartRequest{
			Identity:    req2.Identity,
			WorkspaceID: req2.WorkspaceID,
		})
		requireFault(t, err, fault.NotFound, "Start after workspace removed")
		if res.Outcome != reconcile.StartUnknown {
			t.Fatalf("Start outcome = %q; want unknown", res.Outcome)
		}
		if fx2.LaunchCount() != 0 {
			t.Fatalf("Start launched %d workloads against missing workspace", fx2.LaunchCount())
		}
	})
}

// ---------------------------------------------------------------------------
// 2. Final environment, CA, Git, and LLM proxy
// ---------------------------------------------------------------------------

func runEnvironmentTests(t *testing.T, factory Factory) {
	t.Run("admitted order LLM proxy preservation and unshadowable runtime context", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		req.Container.Env = []definition.EnvVar{
			{Name: "ALPHA", Value: "1"},
			{Name: "KODEMA_LLM_ANTHROPIC_BASE_URL", Value: "https://kodema.local/llm/anthropic"},
			{Name: "KODEMA_LLM_OPENAI_BASE_URL", Value: "https://kodema.local/llm/openai/v1"},
			{Name: "BETA", Value: "2"},
		}
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		wantEnv := []string{
			"ALPHA=1",
			"KODEMA_LLM_ANTHROPIC_BASE_URL=https://kodema.local/llm/anthropic",
			"KODEMA_LLM_OPENAI_BASE_URL=https://kodema.local/llm/openai/v1",
			"BETA=2",
			id.EnvURL + "=" + fx.KodemaURL(),
			id.EnvExecution + "=" + string(req.Identity.ExecutionID),
			id.EnvAction + "=" + string(req.ActionName),
			id.EnvTask + "=" + string(req.TaskID),
		}
		if !slices.Equal(w.EnvList, wantEnv) {
			t.Fatalf("EnvList = %v\nwant      %v", w.EnvList, wantEnv)
		}
		if w.Env[id.EnvAction] == string(req.Identity.ActionID) {
			t.Fatalf("KODEMA_ACTION = %q; must be action slug, not action ID", w.Env[id.EnvAction])
		}
		if w.HasCAMount {
			t.Fatal("unconfigured workload mounted a CA bundle")
		}
		if _, ok := w.Env[workspace.EnvCAFile]; ok {
			t.Fatal("unconfigured workload injected KODEMA_CA_FILE")
		}
		for _, entry := range w.EnvList {
			if strings.HasPrefix(entry, "GIT_CONFIG_") || strings.HasPrefix(entry, workspace.EnvGitURL+"=") {
				t.Fatalf("unconfigured workload injected Git env %q", entry)
			}
		}
	})

	t.Run("canonical CA mount and KODEMA_CA_FILE override", func(t *testing.T) {
		fx := factory(t, Options{WithCA: true})
		req := fx.Request()
		req.Container.Env = append(req.Container.Env,
			definition.EnvVar{Name: workspace.EnvCAFile, Value: "/tmp/spoofed.crt"})

		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		if !w.HasCAMount || w.CAMountPath != container.WorkloadCAPath {
			t.Fatalf("CA mount = (present=%v, path=%q); want (true, %q)", w.HasCAMount, w.CAMountPath, container.WorkloadCAPath)
		}
		if got := w.Env[workspace.EnvCAFile]; got != container.WorkloadCAPath {
			t.Fatalf("KODEMA_CA_FILE = %q; want %q", got, container.WorkloadCAPath)
		}
		if slices.Contains(w.EnvList, workspace.EnvCAFile+"=/tmp/spoofed.crt") {
			t.Fatalf("EnvList allowed spoofing KODEMA_CA_FILE: %v", w.EnvList)
		}
		for _, forbidden := range []string{"SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "NODE_EXTRA_CA_CERTS"} {
			if _, ok := w.Env[forbidden]; ok {
				t.Errorf("CA configuration unexpectedly injected %s", forbidden)
			}
		}
	})

	t.Run("Git service env injection and pipeline GIT_CONFIG stripping", func(t *testing.T) {
		const gitURL = "https://git.kodema.example:8443/git"
		fx := factory(t, Options{WithToolbox: true, GitURL: gitURL})
		req := fx.Request()
		req.Container.Env = append(req.Container.Env,
			definition.EnvVar{Name: workspace.EnvGitURL, Value: "https://evil.example"},
			definition.EnvVar{Name: "GIT_CONFIG_COUNT", Value: "1"},
			definition.EnvVar{Name: "GIT_CONFIG_KEY_0", Value: "credential.helper"},
			definition.EnvVar{Name: "GIT_CONFIG_VALUE_0", Value: "store"},
			definition.EnvVar{Name: "GIT_AUTHOR_NAME", Value: "kept"},
		)
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		for _, want := range []string{
			workspace.EnvGitURL + "=" + gitURL,
			"GIT_CONFIG_COUNT=3",
			"GIT_CONFIG_KEY_0=credential.https://git.kodema.example:8443.helper",
			"GIT_CONFIG_VALUE_0=",
			"GIT_CONFIG_KEY_1=credential.https://git.kodema.example:8443.helper",
			"GIT_CONFIG_VALUE_1=/kodema/bin/kodema --trusted-origin 'https://git.kodema.example:8443' git credential",
			"GIT_CONFIG_KEY_2=safe.directory",
			"GIT_CONFIG_VALUE_2=/workspace",
			"GIT_AUTHOR_NAME=kept",
		} {
			if !slices.Contains(w.EnvList, want) {
				t.Fatalf("EnvList %v missing %q", w.EnvList, want)
			}
		}
		for _, stripped := range []string{
			workspace.EnvGitURL + "=https://evil.example",
			"GIT_CONFIG_KEY_0=credential.helper",
			"GIT_CONFIG_VALUE_0=store",
		} {
			if slices.Contains(w.EnvList, stripped) {
				t.Fatalf("EnvList kept stripped entry %q", stripped)
			}
		}
		// No duplicate env variable names.
		seen := make(map[string]bool, len(w.EnvList))
		for _, entry := range w.EnvList {
			name, _, _ := strings.Cut(entry, "=")
			if seen[name] {
				t.Fatalf("EnvList defines %q more than once: %v", name, w.EnvList)
			}
			seen[name] = true
		}
	})
}

// ---------------------------------------------------------------------------
// 3. WorkingDir & launcher entrypoint
// ---------------------------------------------------------------------------

func runLauncherTests(t *testing.T, factory Factory) {
	t.Run("without toolbox", func(t *testing.T) {
		fx := factory(t, Options{WithToolbox: false})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		if w.WorkingDir != workspace.WorkspaceRoot {
			t.Fatalf("WorkingDir = %q; want %q", w.WorkingDir, workspace.WorkspaceRoot)
		}
		want := workspace.Entrypoint(container.WorkloadScriptPath, workspace.Launch{})
		if !slices.Equal(w.Command, want) {
			t.Fatalf("Command = %q; want %q", w.Command, want)
		}
		if w.HasToolboxMount {
			t.Fatal("mounted toolbox without toolbox configuration")
		}
		if w.HasOutputDir {
			t.Fatal("mounted writable output dir without toolbox configuration")
		}
	})

	t.Run("with toolbox and no declared outputs", func(t *testing.T) {
		fx := factory(t, Options{WithToolbox: true})
		req := fx.Request()
		req.Container.Outputs = nil
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		if w.WorkingDir != workspace.WorkspaceRoot {
			t.Fatalf("WorkingDir = %q; want %q", w.WorkingDir, workspace.WorkspaceRoot)
		}
		want := workspace.Entrypoint(container.WorkloadScriptPath, workspace.Launch{
			Toolbox:      true,
			Outputs:      false,
			FrameStreams: w.FrameStreams,
		})
		if !slices.Equal(w.Command, want) {
			t.Fatalf("Command = %q; want %q", w.Command, want)
		}
		if !w.HasToolboxMount {
			t.Fatal("missing toolbox mount at /kodema/bin")
		}
		if w.HasOutputDir {
			t.Fatal("mounted output dir for action declaring no outputs")
		}
	})

	t.Run("with toolbox and declared outputs", func(t *testing.T) {
		fx := factory(t, Options{WithToolbox: true})
		req := fx.Request()
		req.Container.Outputs = []id.OutputKey{"out-a"}
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		if w.WorkingDir != workspace.WorkspaceRoot {
			t.Fatalf("WorkingDir = %q; want %q", w.WorkingDir, workspace.WorkspaceRoot)
		}
		want := workspace.Entrypoint(container.WorkloadScriptPath, workspace.Launch{
			Toolbox:      true,
			Outputs:      true,
			FrameStreams: w.FrameStreams,
		})
		if !slices.Equal(w.Command, want) {
			t.Fatalf("Command = %q; want %q", w.Command, want)
		}
		if !w.HasToolboxMount {
			t.Fatal("missing toolbox mount at /kodema/bin")
		}
		wantOutDir, _ := workspace.OutputDirectory(req.ActionName)
		if !w.HasOutputDir || w.OutputDirPath != wantOutDir {
			t.Fatalf("output dir = (present=%v, path=%q); want (true, %q)", w.HasOutputDir, w.OutputDirPath, wantOutDir)
		}
		if got := string(w.ActivationFiles["/kodema/outputs/clone/commit"]); got != "abc" {
			t.Fatalf("earlier output = %q; want abc", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 4. Labels
// ---------------------------------------------------------------------------

func runLabelTests(t *testing.T, factory Factory) {
	t.Run("regular workload carries identity and project labels without sandbox role", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		ctx := runtime.WithExecutionProject(context.Background(), "proj-alpha")
		mustPrepare(t, fx, ctx, req)
		mustStart(t, fx, ctx, req)
		w := fx.InspectLaunched(t, req.Identity)

		want := map[string]string{
			container.LabelExecution: string(req.Identity.ExecutionID),
			container.LabelAction:    string(req.Identity.ActionID),
			container.LabelWorkload:  string(req.Identity.WorkloadID),
			container.LabelWorkspace: string(req.WorkspaceID),
			container.LabelProject:   "proj-alpha",
		}
		for k, v := range want {
			if got := w.Labels[k]; got != v {
				t.Errorf("label %q = %q; want %q", k, got, v)
			}
		}
		if got, ok := w.Labels[container.LabelRole]; ok {
			t.Fatalf("regular workload unexpectedly carried %s=%q", container.LabelRole, got)
		}
	})

	t.Run("sandbox workload carries role=sandbox", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		req.ActionName = container.RoleSandbox
		req.Container.Commands = []string{"exec /kodema/bin/kodema sandbox serve --listen 0.0.0.0:8090"}
		req.Container.Secrets = []id.SecretName{"KODEMA_SBOX_TOK_1"}
		req.Secrets = []reconcile.SecretValue{{Name: "KODEMA_SBOX_TOK_1", Value: "kodsbox.test"}}
		req.Container.Outputs = nil
		ctx := runtime.WithExecutionProject(context.Background(), "proj-alpha")
		mustPrepare(t, fx, ctx, req)
		mustStart(t, fx, ctx, req)
		w := fx.InspectLaunched(t, req.Identity)

		if got := w.Labels[container.LabelProject]; got != "proj-alpha" {
			t.Errorf("label %q = %q; want proj-alpha", container.LabelProject, got)
		}
		if got := w.Labels[container.LabelRole]; got != container.RoleSandbox {
			t.Errorf("label %q = %q; want %q", container.LabelRole, got, container.RoleSandbox)
		}
	})

	t.Run("sandbox action with privileged secret or comment-only command does not get role=sandbox", func(t *testing.T) {
		t.Run("privileged secret", func(t *testing.T) {
			fx := factory(t, Options{})
			req := fx.Request()
			req.ActionName = container.RoleSandbox
			req.Container.Commands = []string{"exec /kodema/bin/kodema sandbox serve"}
			mustPrepare(t, fx, context.Background(), req)
			mustStart(t, fx, context.Background(), req)
			w := fx.InspectLaunched(t, req.Identity)
			if got, ok := w.Labels[container.LabelRole]; ok {
				t.Fatalf("privileged workload carried %s=%q", container.LabelRole, got)
			}
		})

		t.Run("comment or echo only", func(t *testing.T) {
			fx := factory(t, Options{})
			req := fx.Request()
			req.ActionName = container.RoleSandbox
			req.Container.Commands = []string{"# /kodema/bin/kodema sandbox serve\necho 'kodema sandbox serve'"}
			req.Container.Secrets = nil
			req.Secrets = nil
			mustPrepare(t, fx, context.Background(), req)
			mustStart(t, fx, context.Background(), req)
			w := fx.InspectLaunched(t, req.Identity)
			if got, ok := w.Labels[container.LabelRole]; ok {
				t.Fatalf("comment-only workload carried %s=%q", container.LabelRole, got)
			}
		})
	})
}

// ---------------------------------------------------------------------------
// 5. Log redaction, ordering, cursors, truncation, fail-closed
// ---------------------------------------------------------------------------

func collectStdout(t *testing.T, fx Fixture, identity reconcile.EffectIdentity, limit int) []byte {
	t.Helper()
	var out []byte
	cursor := int64(0)
	for step := range 1000 {
		batch, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
			Identity:    identity,
			AfterOffset: cursor,
			Limit:       limit,
		})
		if err != nil {
			t.Fatalf("step %d Logs: %v", step, err)
		}
		for _, chunk := range batch.Chunks {
			if len(chunk.Data) > limit {
				t.Fatalf("step %d chunk len %d > limit %d", step, len(chunk.Data), limit)
			}
			if chunk.Stream == reconcile.LogStdout {
				out = append(out, chunk.Data...)
			}
		}
		if batch.NextOffset == cursor {
			return out
		}
		cursor = batch.NextOffset
	}
	t.Fatal("log pagination did not terminate")
	return nil
}

func runLogTests(t *testing.T, factory Factory) {
	t.Run("redacts declared secrets initial credential and refreshed credential", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)
		launchesBefore := fx.LaunchCount()

		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("hello s3cr3t and tok-abc\n"))
		batch, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
			Identity: req.Identity,
			Limit:    256,
		})
		if err != nil {
			t.Fatalf("Logs: %v", err)
		}
		if len(batch.Chunks) == 0 {
			t.Fatal("Logs returned no chunks")
		}
		got := string(batch.Chunks[0].Data)
		if !strings.Contains(got, container.Redacted) || !strings.Contains(got, "hello") {
			t.Fatalf("redacted chunk = %q; want hello + %q", got, container.Redacted)
		}
		for _, leak := range []string{"s3cr3t", "tok-abc"} {
			if strings.Contains(got, leak) {
				t.Fatalf("log chunk leaked %q: %q", leak, got)
			}
		}
		if fx.LaunchCount() != launchesBefore {
			t.Fatal("Logs altered launch count")
		}

		// Refresh KODEMA_TOKEN and append log containing the new token; the
		// refreshed token must be redacted and delivered via cursor advancement.
		ref, err := fx.Client().RefreshCredential(context.Background(), reconcile.CredentialRequest{
			Identity:   req.Identity,
			Credential: "tok-refreshed-xyz",
		})
		if err != nil || !ref.Delivered {
			t.Fatalf("RefreshCredential = (%+v, %v); want delivered", ref, err)
		}
		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("after tok-refreshed-xyz end\n"))
		next, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
			Identity:    req.Identity,
			AfterOffset: batch.NextOffset,
			Limit:       256,
		})
		if err != nil {
			t.Fatalf("follow-up Logs: %v", err)
		}
		if len(next.Chunks) != 1 {
			t.Fatalf("follow-up chunks = %d; want 1", len(next.Chunks))
		}
		wantNext := "after " + container.Redacted + " end\n"
		if string(next.Chunks[0].Data) != wantNext {
			t.Fatalf("follow-up chunk = %q; want %q", next.Chunks[0].Data, wantNext)
		}
		// Caught-up read emits 0 chunks and keeps cursor stable.
		caughtUp, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
			Identity:    req.Identity,
			AfterOffset: next.NextOffset,
			Limit:       256,
		})
		if err != nil {
			t.Fatalf("caught-up Logs: %v", err)
		}
		if len(caughtUp.Chunks) != 0 || caughtUp.NextOffset != next.NextOffset {
			t.Fatalf("caught-up = (%d chunks, offset %d); want (0, %d)", len(caughtUp.Chunks), caughtUp.NextOffset, next.NextOffset)
		}
	})

	t.Run("redacts secrets split across small page boundaries without partial leaks", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		req.Secrets = []reconcile.SecretValue{{Name: "API_KEY", Value: "s3cr3t"}}
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)

		prefix := strings.Repeat("x", 10)
		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte(prefix+"s3cr3t-tail"))
		got := collectStdout(t, fx, req.Identity, 13)
		if want := prefix + container.Redacted + "-tail"; string(got) != want {
			t.Fatalf("paged stdout = %q; want %q", got, want)
		}
		if bytes.Contains(got, []byte("s3cr3t")) {
			t.Fatalf("paged stdout leaked secret: %q", got)
		}
	})

	t.Run("short secret expansion sets Truncated and never drops trailing bytes", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		req.Secrets = []reconcile.SecretValue{{Name: "API_KEY", Value: "s3c"}}
		req.Credential = "tok4"
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)

		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("0123456789s3c-TAIL"))
		first, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
			Identity: req.Identity,
			Limit:    18,
		})
		if err != nil {
			t.Fatalf("first Logs: %v", err)
		}
		if len(first.Chunks) != 1 || !first.Chunks[0].Truncated {
			t.Fatalf("first batch = %+v; want 1 chunk with Truncated=true when marker expansion leaves unconsumed bytes", first)
		}

		got := collectStdout(t, fx, req.Identity, 18)
		if want := "0123456789" + container.Redacted + "-TAIL"; string(got) != want {
			t.Fatalf("stdout = %q; want %q", got, want)
		}
	})

	t.Run("refuses unbounded limits negative offset and absent workload", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)

		// Absent workload (prepared, not started) -> NotFound.
		_, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{Identity: req.Identity, Limit: 64})
		requireFault(t, err, fault.NotFound, "Logs on absent workload")

		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)
		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("data"))

		for _, tc := range []struct {
			name string
			req  reconcile.LogsRequest
		}{
			{"zero limit", reconcile.LogsRequest{Identity: req.Identity, Limit: 0}},
			{"negative limit", reconcile.LogsRequest{Identity: req.Identity, Limit: -1}},
			{"oversized limit", reconcile.LogsRequest{Identity: req.Identity, Limit: container.MaxLogBytes + 1}},
			{"negative offset", reconcile.LogsRequest{Identity: req.Identity, AfterOffset: -1, Limit: 16}},
		} {
			_, err := fx.Client().Logs(context.Background(), tc.req)
			requireFault(t, err, fault.InvalidInput, tc.name)
		}
	})

	t.Run("fails closed when redaction source is unavailable", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)
		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("secret s3cr3t tok-abc\n"))

		fx.BreakRedactionSource(t, req.Identity)
		batch, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
			Identity: req.Identity,
			Limit:    256,
		})
		requireFault(t, err, fault.DependencyUnavailable, "missing redaction source")
		if !errors.Is(err, container.ErrRedactionSourceUnavailable) {
			t.Fatalf("error %v does not wrap container.ErrRedactionSourceUnavailable", err)
		}
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error %v leaked os.ErrNotExist", err)
		}
		if len(batch.Chunks) != 0 {
			t.Fatalf("emitted %d chunks without a redaction source", len(batch.Chunks))
		}
	})

	t.Run("separates stdout and stderr with independent redaction and cursors", func(t *testing.T) {
		fx := factory(t, Options{WithToolbox: true})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)

		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("out-1 s3cr3t\n"))
		fx.AppendLog(t, req.Identity, reconcile.LogStderr, []byte("err-1 tok-abc\n"))
		fx.AppendLog(t, req.Identity, reconcile.LogStdout, []byte("out-2-tail"))
		fx.AppendLog(t, req.Identity, reconcile.LogStderr, []byte("err-2-tail"))

		var gotOut, gotErr []byte
		cursor := int64(0)
		for step := range 200 {
			batch, err := fx.Client().Logs(context.Background(), reconcile.LogsRequest{
				Identity:    req.Identity,
				AfterOffset: cursor,
				Limit:       12,
			})
			if err != nil {
				t.Fatalf("step %d Logs: %v", step, err)
			}
			for _, chunk := range batch.Chunks {
				if len(chunk.Data) > 12 {
					t.Fatalf("step %d chunk len %d > limit 12", step, len(chunk.Data))
				}
				switch chunk.Stream {
				case reconcile.LogStdout:
					gotOut = append(gotOut, chunk.Data...)
				case reconcile.LogStderr:
					gotErr = append(gotErr, chunk.Data...)
				default:
					t.Fatalf("step %d unexpected stream %q", step, chunk.Stream)
				}
			}
			if batch.NextOffset == cursor {
				break
			}
			cursor = batch.NextOffset
		}

		wantOut := "out-1 " + container.Redacted + "\nout-2-tail"
		wantErr := "err-1 " + container.Redacted + "\nerr-2-tail"
		if string(gotOut) != wantOut {
			t.Fatalf("stdout = %q; want %q", gotOut, wantOut)
		}
		if string(gotErr) != wantErr {
			t.Fatalf("stderr = %q; want %q", gotErr, wantErr)
		}
		for _, leak := range []string{"s3cr3t", "tok-abc"} {
			if strings.Contains(string(gotOut), leak) || strings.Contains(string(gotErr), leak) {
				t.Fatalf("split streams leaked %q: stdout=%q stderr=%q", leak, gotOut, gotErr)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// 6. Workspace ownership across all 5 operations
// ---------------------------------------------------------------------------

func runWorkspaceOwnershipTests(t *testing.T, factory Factory) {
	fx := factory(t, Options{})
	req := fx.Request()
	wrongWS := id.WorkspaceID("workspace-other")
	fx.AddWorkspace(t, wrongWS)

	mustPrepare(t, fx, context.Background(), req)
	mustStart(t, fx, context.Background(), req)
	fx.SeedExited(t, req.Identity, 0)

	// 1. Prepare with mismatched workspace -> Forbidden.
	prepReq := clonePrepareRequest(req)
	prepReq.WorkspaceID = wrongWS
	_, err := fx.Client().Prepare(context.Background(), prepReq)
	requireFault(t, err, fault.Forbidden, "Prepare workspace mismatch")

	// 2. Start with mismatched workspace -> Forbidden.
	startRes, err := fx.Client().Start(context.Background(), reconcile.StartRequest{
		Identity:    req.Identity,
		WorkspaceID: wrongWS,
	})
	requireFault(t, err, fault.Forbidden, "Start workspace mismatch")
	if startRes.Outcome != reconcile.StartUnknown {
		t.Fatalf("Start outcome on mismatch = %q; want unknown", startRes.Outcome)
	}

	// 3. Observe with mismatched workspace -> Forbidden.
	_, err = fx.Client().Observe(context.Background(), reconcile.ObserveRequest{
		Identity:    req.Identity,
		WorkspaceID: wrongWS,
	})
	requireFault(t, err, fault.Forbidden, "Observe workspace mismatch")

	// 4. Stop with mismatched workspace -> Forbidden & unconfirmed.
	ev, err := fx.Client().Stop(context.Background(), reconcile.StopRequest{
		Identity:    req.Identity,
		WorkspaceID: wrongWS,
	})
	requireFault(t, err, fault.Forbidden, "Stop workspace mismatch")
	if ev.IsConfirmed() {
		t.Fatal("Stop confirmed on mismatched workspace")
	}

	// 5. Remove with mismatched workspace -> Forbidden & not removed.
	rem, err := fx.Client().Remove(context.Background(), reconcile.RemoveRequest{
		Identity:    req.Identity,
		WorkspaceID: wrongWS,
	})
	requireFault(t, err, fault.Forbidden, "Remove workspace mismatch")
	if rem.Removed {
		t.Fatal("Remove succeeded on mismatched workspace")
	}
	if fx.ObjectCount() != 1 {
		t.Fatalf("mismatched Remove altered workload count to %d; want 1", fx.ObjectCount())
	}
}

// ---------------------------------------------------------------------------
// 7. Absent Observe, pre-effect cancellation, RefreshCredential, races
// ---------------------------------------------------------------------------

func runLifecycleAndRaceTests(t *testing.T, factory Factory) {
	t.Run("Observe reports Absent without proof before and after Prepare", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		for _, stage := range []string{"unprepared", "prepared"} {
			if stage == "prepared" {
				mustPrepare(t, fx, context.Background(), req)
			}
			obs, err := fx.Client().Observe(context.Background(), reconcile.ObserveRequest{
				Identity:    req.Identity,
				WorkspaceID: req.WorkspaceID,
			})
			if err != nil {
				t.Fatalf("%s Observe: %v", stage, err)
			}
			if obs.Kind != engine.ObservationAbsent {
				t.Fatalf("%s Observe kind = %q; want absent", stage, obs.Kind)
			}
			if obs.ProofOfNonexecution() {
				t.Fatalf("%s Observe falsely claimed proof of nonexecution", stage)
			}
			if !obs.Valid() {
				t.Fatalf("%s Observe invalid: %+v", stage, obs)
			}
		}
	})

	t.Run("Observe running and exited plus Start reuse and Remove idempotency", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()

		// Start without Prepare refused.
		res, err := fx.Client().Start(context.Background(), reconcile.StartRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		requireFault(t, err, fault.InvalidInput, "Start without Prepare")
		if res.Outcome != reconcile.StartUnknown || fx.LaunchCount() != 0 {
			t.Fatalf("Start without Prepare = (%+v, launches=%d)", res, fx.LaunchCount())
		}

		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)

		// Idempotent Start reuse.
		mustStart(t, fx, context.Background(), req)
		if fx.ObjectCount() != 1 || fx.LaunchCount() != 1 {
			t.Fatalf("Start retry duplicated workload: objects=%d launches=%d", fx.ObjectCount(), fx.LaunchCount())
		}

		obs, err := fx.Client().Observe(context.Background(), reconcile.ObserveRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		if err != nil || obs.Kind != engine.ObservationRunning || obs.StopConfirmed() {
			t.Fatalf("running Observe = (%+v, %v); want running unconfirmed", obs, err)
		}

		// Remove while running retains workload.
		rem, err := fx.Client().Remove(context.Background(), reconcile.RemoveRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		if err != nil || rem.Removed {
			t.Fatalf("Remove while running = (%+v, %v); want Removed=false", rem, err)
		}

		// Exited observation reports exit code + confirmed stop.
		fx.SeedExited(t, req.Identity, 7)
		obs, err = fx.Client().Observe(context.Background(), reconcile.ObserveRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		if err != nil || obs.Kind != engine.ObservationExited || !obs.StopConfirmed() || !obs.Valid() {
			t.Fatalf("exited Observe = (%+v, %v); want exited confirmed", obs, err)
		}
		if code, ok := obs.ExitStatus(); !ok || code != 7 {
			t.Fatalf("ExitStatus = (%d, %v); want (7, true)", code, ok)
		}

		// Remove on exited succeeds, is idempotent, and retains the workspace.
		rem, err = fx.Client().Remove(context.Background(), reconcile.RemoveRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		if err != nil || !rem.Removed || rem.Detail == "" {
			t.Fatalf("Remove = (%+v, %v); want Removed=true", rem, err)
		}
		rem2, err := fx.Client().Remove(context.Background(), reconcile.RemoveRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		if err != nil || !rem2.Removed {
			t.Fatalf("repeated Remove = (%+v, %v); want idempotent Removed=true", rem2, err)
		}
		wsObs, err := fx.Workspace().Status(context.Background(), req.WorkspaceID)
		if err != nil || !wsObs.Available {
			t.Fatalf("Remove deleted retained workspace: (%+v, %v)", wsObs, err)
		}
	})

	t.Run("Start cancelled before call stays StartUnknown with zero effect", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		res, err := fx.Client().Start(ctx, reconcile.StartRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		})
		if err == nil {
			t.Fatal("Start hid cancelled context")
		}
		if res.Outcome != reconcile.StartUnknown {
			t.Fatalf("cancelled Start outcome = %q; want unknown", res.Outcome)
		}
		if fx.LaunchCount() != 0 || fx.ObjectCount() != 0 {
			t.Fatalf("cancelled Start had side effect: launches=%d objects=%d", fx.LaunchCount(), fx.ObjectCount())
		}
	})

	t.Run("live RefreshCredential updates token without restart and refuses invalid or removed", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)
		launchesBefore := fx.LaunchCount()

		ref, err := fx.Client().RefreshCredential(context.Background(), reconcile.CredentialRequest{
			Identity:   req.Identity,
			Credential: "tok-live-rotated",
		})
		if err != nil || !ref.Delivered || ref.Detail == "" {
			t.Fatalf("RefreshCredential = (%+v, %v); want delivered", ref, err)
		}
		if got := fx.ReadCredential(t, req.Identity); got != "tok-live-rotated" {
			t.Fatalf("ReadCredential = %q; want tok-live-rotated", got)
		}
		if fx.LaunchCount() != launchesBefore {
			t.Fatalf("RefreshCredential restarted workload: launches %d -> %d", launchesBefore, fx.LaunchCount())
		}
		// Declared secrets remain intact.
		w := fx.InspectLaunched(t, req.Identity)
		if string(w.SecretFiles["API_KEY"]) != "s3cr3t" {
			t.Fatalf("RefreshCredential disturbed API_KEY: %q", w.SecretFiles["API_KEY"])
		}

		// Empty/whitespace credential refused.
		_, err = fx.Client().RefreshCredential(context.Background(), reconcile.CredentialRequest{
			Identity:   req.Identity,
			Credential: "   ",
		})
		requireFault(t, err, fault.InvalidInput, "blank credential")

		// Refused after Remove with NotFound.
		fx.SeedExited(t, req.Identity, 0)
		if rem, err := fx.Client().Remove(context.Background(), reconcile.RemoveRequest{
			Identity:    req.Identity,
			WorkspaceID: req.WorkspaceID,
		}); err != nil || !rem.Removed {
			t.Fatalf("Remove = (%+v, %v)", rem, err)
		}
		ref, err = fx.Client().RefreshCredential(context.Background(), reconcile.CredentialRequest{
			Identity:   req.Identity,
			Credential: "tok-after-remove",
		})
		requireFault(t, err, fault.NotFound, "RefreshCredential after Remove")
		if ref.Delivered {
			t.Fatal("RefreshCredential reported Delivered after Remove")
		}
	})

	t.Run("concurrent duplicate Start converges on one workload", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)

		const workers = 8
		gate := make(chan struct{})
		results := make([]reconcile.StartResult, workers)
		errs := make([]error, workers)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-gate
				results[idx], errs[idx] = fx.Client().Start(context.Background(), reconcile.StartRequest{
					Identity:    req.Identity,
					WorkspaceID: req.WorkspaceID,
				})
			}(i)
		}
		close(gate)
		wg.Wait()

		accepted := 0
		for i := range workers {
			if errs[i] != nil {
				t.Fatalf("worker %d Start error: %v", i, errs[i])
			}
			if results[i].Outcome == reconcile.StartProvenNotStarted {
				t.Fatalf("worker %d claimed proven-not-started", i)
			}
			if results[i].Outcome == reconcile.StartAccepted {
				accepted++
			}
		}
		if accepted == 0 {
			t.Fatal("no concurrent Start was accepted")
		}
		if fx.ObjectCount() != 1 {
			t.Fatalf("concurrent Starts created %d objects; want 1", fx.ObjectCount())
		}
	})

	t.Run("concurrent RefreshCredential and Observe keep token whole without restart", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		fx.SeedRunning(t, req.Identity)
		launchesBefore := fx.LaunchCount()

		const workers = 8
		tokens := make([]string, workers)
		for i := range tokens {
			tokens[i] = fmt.Sprintf("tok-concurrent-%d-%s", i, strings.Repeat("x", i+1))
		}
		gate := make(chan struct{})
		errs := make(chan error, workers*2)
		var wg sync.WaitGroup
		for i := range workers {
			wg.Add(2)
			go func(tok string) {
				defer wg.Done()
				<-gate
				ref, err := fx.Client().RefreshCredential(context.Background(), reconcile.CredentialRequest{
					Identity:   req.Identity,
					Credential: tok,
				})
				if err == nil && !ref.Delivered {
					err = errors.New("refresh did not report delivery")
				}
				errs <- err
			}(tokens[i])
			go func() {
				defer wg.Done()
				<-gate
				obs, err := fx.Client().Observe(context.Background(), reconcile.ObserveRequest{
					Identity:    req.Identity,
					WorkspaceID: req.WorkspaceID,
				})
				if err == nil && obs.Kind != engine.ObservationRunning {
					err = fmt.Errorf("observed %q; want running", obs.Kind)
				}
				errs <- err
			}()
		}
		close(gate)
		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent refresh/observe error: %v", err)
			}
		}
		final := fx.ReadCredential(t, req.Identity)
		if !slices.Contains(tokens, final) {
			t.Fatalf("final token %q is torn or not among submitted tokens", final)
		}
		if fx.LaunchCount() != launchesBefore || fx.ObjectCount() != 1 {
			t.Fatalf("concurrent refresh restarted workload: launches %d->%d objects=%d",
				launchesBefore, fx.LaunchCount(), fx.ObjectCount())
		}
	})
}

// ---------------------------------------------------------------------------
// 8. Named cache mounts
// ---------------------------------------------------------------------------

func runCacheTests(t *testing.T, factory Factory) {
	t.Run("valid scoped caches mount at /kodema/cache/<name>", func(t *testing.T) {
		fx := factory(t, Options{WithCacheClaim: true})
		req := fx.Request()
		scope := tenancy.Scope{TenantID: "tenant-acme", ProjectID: "proj-a"}
		caches, err := workspace.BuildCacheMounts(scope, []string{"go-mod", "npm"})
		if err != nil {
			t.Fatalf("BuildCacheMounts: %v", err)
		}
		req.Container.Caches = []string{"go-mod", "npm"}
		req.Caches = caches

		mustPrepare(t, fx, context.Background(), req)
		mustStart(t, fx, context.Background(), req)
		w := fx.InspectLaunched(t, req.Identity)

		if got := w.CacheMounts["go-mod"]; got != "/kodema/cache/go-mod" {
			t.Fatalf("go-mod cache mount = %q; want /kodema/cache/go-mod (all=%v)", got, w.CacheMounts)
		}
		if got := w.CacheMounts["npm"]; got != "/kodema/cache/npm" {
			t.Fatalf("npm cache mount = %q; want /kodema/cache/npm (all=%v)", got, w.CacheMounts)
		}
	})

	t.Run("unscoped or mismatched caches refused with InvalidInput", func(t *testing.T) {
		cases := []struct {
			name     string
			declared []string
			mounts   []reconcile.CacheMount
		}{
			{"missing CacheMounts", []string{"go-mod"}, nil},
			{"empty key", []string{"go-mod"}, []reconcile.CacheMount{{Name: "go-mod", Key: ""}}},
			{"mismatched name", []string{"go-mod"}, []reconcile.CacheMount{{Name: "npm", Key: strings.Repeat("a", 32)}}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fx := factory(t, Options{})
				req := fx.Request()
				req.Container.Caches = tc.declared
				req.Caches = tc.mounts
				_, err := fx.Client().Prepare(context.Background(), req)
				requireFault(t, err, fault.InvalidInput, tc.name)
				if fx.LaunchCount() != 0 {
					t.Fatalf("refused cache request launched %d workloads", fx.LaunchCount())
				}
			})
		}
	})
}

// ---------------------------------------------------------------------------
// 9. Shared reconcile.Workspace port invariants
// ---------------------------------------------------------------------------

func runWorkspacePortTests(t *testing.T, factory Factory) {
	t.Run("Status Retain Release basic lifecycle idempotency and secret-free details", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		ws := fx.Workspace()
		ctx := context.Background()

		// Status on present workspace.
		obs, err := ws.Status(ctx, req.WorkspaceID)
		if err != nil || !obs.Available || obs.WorkspaceID != req.WorkspaceID || obs.Detail == "" {
			t.Fatalf("Status(present) = (%+v, %v); want available with detail", obs, err)
		}

		// Retain is idempotent.
		if err := ws.Retain(ctx, req.WorkspaceID); err != nil {
			t.Fatalf("first Retain: %v", err)
		}
		if err := ws.Retain(ctx, req.WorkspaceID); err != nil {
			t.Fatalf("second Retain: %v", err)
		}

		// Release refuses while workload is running and leaks no secrets.
		mustPrepare(t, fx, ctx, req)
		mustStart(t, fx, ctx, req)
		fx.SeedRunning(t, req.Identity)
		relErr := ws.Release(ctx, req.WorkspaceID)
		requireFault(t, relErr, fault.Forbidden, "Release while workload running")
		for _, secret := range []string{"s3cr3t", req.Credential} {
			if strings.Contains(obs.Detail, secret) || strings.Contains(relErr.Error(), secret) {
				t.Fatalf("workspace detail/error leaked %q", secret)
			}
		}

		// After workload exits and is removed, Release succeeds and is idempotent.
		fx.SeedExited(t, req.Identity, 0)
		if rem, err := fx.Client().Remove(ctx, reconcile.RemoveRequest{
			Identity: req.Identity, WorkspaceID: req.WorkspaceID,
		}); err != nil || !rem.Removed {
			t.Fatalf("Remove = (%+v, %v)", rem, err)
		}
		if err := ws.Release(ctx, req.WorkspaceID); err != nil {
			t.Fatalf("Release after Remove: %v", err)
		}
		if err := ws.Release(ctx, req.WorkspaceID); err != nil {
			t.Fatalf("idempotent Release: %v", err)
		}

		// Missing workspace reports unavailable, refuses Retain with NotFound, and never recreates.
		missingObs, err := ws.Status(ctx, req.WorkspaceID)
		if err != nil || missingObs.Available {
			t.Fatalf("Status(missing) = (%+v, %v); want unavailable", missingObs, err)
		}
		requireFault(t, ws.Retain(ctx, req.WorkspaceID), fault.NotFound, "Retain on missing workspace")
		if afterObs, err := ws.Status(ctx, req.WorkspaceID); err != nil || afterObs.Available {
			t.Fatalf("Retain recreated missing workspace: (%+v, %v)", afterObs, err)
		}
	})

	t.Run("invalid workspace id and cancelled context refused", func(t *testing.T) {
		fx := factory(t, Options{})
		req := fx.Request()
		ws := fx.Workspace()

		_, err := ws.Status(context.Background(), "")
		requireFault(t, err, fault.InvalidInput, "Status empty workspace")
		requireFault(t, ws.Retain(context.Background(), ""), fault.InvalidInput, "Retain empty workspace")
		requireFault(t, ws.Release(context.Background(), ""), fault.InvalidInput, "Release empty workspace")

		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := ws.Status(cancelled, req.WorkspaceID); err == nil {
			t.Fatal("Status hid cancelled context")
		}
		if err := ws.Retain(cancelled, req.WorkspaceID); err == nil {
			t.Fatal("Retain hid cancelled context")
		}
		if err := ws.Release(cancelled, req.WorkspaceID); err == nil {
			t.Fatal("Release hid cancelled context")
		}
	})
}

func clonePrepareRequest(req reconcile.PrepareRequest) reconcile.PrepareRequest {
	cp := req
	cp.Container.Commands = append([]string(nil), req.Container.Commands...)
	cp.Container.Env = append([]definition.EnvVar(nil), req.Container.Env...)
	cp.Container.Secrets = append([]id.SecretName(nil), req.Container.Secrets...)
	cp.Container.Outputs = append([]id.OutputKey(nil), req.Container.Outputs...)
	cp.Container.Caches = append([]string(nil), req.Container.Caches...)
	cp.Caches = append([]reconcile.CacheMount(nil), req.Caches...)
	cp.Secrets = append([]reconcile.SecretValue(nil), req.Secrets...)
	if req.Inputs != nil {
		cp.Inputs = make(map[id.InputKey]string, len(req.Inputs))
		for k, v := range req.Inputs {
			cp.Inputs[k] = v
		}
	}
	if req.Outputs != nil {
		cp.Outputs = make(map[id.ActionName]engine.Outputs, len(req.Outputs))
		for action, outs := range req.Outputs {
			inner := make(engine.Outputs, len(outs))
			for k, v := range outs {
				inner[k] = v
			}
			cp.Outputs[action] = inner
		}
	}
	if req.Task != nil {
		cp.Task = append([]byte(nil), req.Task...)
	}
	if req.TaskFiles != nil {
		cp.TaskFiles = make(map[string][]byte, len(req.TaskFiles))
		for k, v := range req.TaskFiles {
			cp.TaskFiles[k] = append([]byte(nil), v...)
		}
	}
	return cp
}

func mapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
