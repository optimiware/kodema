package app_test

// The concrete Git effects against the real native Git binary and a
// scratch repository. This is the fast unit-level proof that the command
// sequence is valid; the build-tagged services test proves the same effects
// through the real `kodema services` entry and the public assignment edge.
//
// It lives beside the userland app that owns the OS Git executor: only an app
// package may compose the concrete executor with the userland effects, so the
// test drives the real binary from here instead of from the userland package.

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/apps/merge"
	mergeapp "github.com/optimiware/kodema/internal/apps/merge/app"
	"github.com/optimiware/kodema/internal/id"
)

// runScratchGit runs one native Git command in one scratch directory.
func runScratchGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_AUTHOR_NAME=Test",
		"GIT_AUTHOR_EMAIL=test@example.test",
		"GIT_COMMITTER_NAME=Test",
		"GIT_COMMITTER_EMAIL=test@example.test",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

// writeScratchFile writes one file and stages it.
func writeScratchFile(t *testing.T, directory, name, content string) {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	runScratchGit(t, directory, "add", name)
}

// seedMergeRepository creates one bare repository with a main base and a
// descendant work candidate.
func seedMergeRepository(t *testing.T, root string) (repository, base, candidate string) {
	t.Helper()
	repository = "shop/web"
	bare := filepath.Join(root, repository)
	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	runScratchGit(t, filepath.Dir(bare), "init", "--bare", "--quiet", "--initial-branch=main", bare)
	seed := t.TempDir()
	runScratchGit(t, seed, "init", "--quiet", "--initial-branch=main")
	writeScratchFile(t, seed, "README.md", "base\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "base")
	base = runScratchGit(t, seed, "rev-parse", "HEAD")
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work")
	writeScratchFile(t, seed, "feature.txt", "candidate\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "candidate")
	candidate = runScratchGit(t, seed, "rev-parse", "HEAD")
	runScratchGit(t, seed, "push", "--quiet", bare, "main", "work")
	return repository, base, candidate
}

// TestGitMergeEffectsRealFastForwardMerge proves the real native Git effect on
// a scratch repository before the composed integration run.
func TestGitMergeEffectsRealFastForwardMerge(t *testing.T) {
	root := t.TempDir()
	repository, base, candidate := seedMergeRepository(t, root)
	executor, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "file://" + filepath.ToSlash(root),
		StateDirectory: t.TempDir(),
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	operation := merge.MergeOperation{
		Key:         merge.MergeKey{Repository: repository, Ref: "refs/heads/main"},
		OperationID: "merge-real-1",
		Base:        base,
		Candidate:   candidate,
	}
	observation, err := effects.Execute(context.Background(), operation)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if observation.State != merge.EffectApplied {
		t.Fatalf("observation = %+v; want applied", observation)
	}
	if observation.Base != base || observation.Candidate != candidate || observation.Output != candidate {
		t.Fatalf("observation = %+v; want the fast-forward result", observation)
	}
	bare := filepath.Join(root, repository)
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/main"); head != candidate {
		t.Fatalf("remote main = %s; want the merged candidate %s", head, candidate)
	}
	content := runScratchGit(t, bare, "show", "refs/heads/main:feature.txt")
	if content != "candidate" {
		t.Fatalf("merged feature content = %q; want candidate", content)
	}
	observed, err := effects.Observe(context.Background(), operation)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if observed.State != merge.EffectApplied || observed.Output != candidate {
		t.Fatalf("observed = %+v; want the settled fast-forward", observed)
	}
}

// TestGitMergeEffectsRealRebaseMerge proves a divergent candidate is rebased
// onto the base and the rebased commit is what the remote settles.
func TestGitMergeEffectsRealRebaseMerge(t *testing.T) {
	root := t.TempDir()
	repository, _, candidate := seedMergeRepository(t, root)
	// Advance main past the candidate's base so the candidate cannot fast
	// forward.
	bare := filepath.Join(root, repository)
	advance := t.TempDir()
	runScratchGit(t, advance, "clone", "--quiet", bare, ".")
	runScratchGit(t, advance, "checkout", "--quiet", "main")
	writeScratchFile(t, advance, "later.txt", "later\n")
	runScratchGit(t, advance, "commit", "--quiet", "-m", "later")
	newBase := runScratchGit(t, advance, "rev-parse", "HEAD")
	runScratchGit(t, advance, "push", "--quiet", "origin", "main")

	executor, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "file://" + filepath.ToSlash(root),
		StateDirectory: t.TempDir(),
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	operation := merge.MergeOperation{
		Key:         merge.MergeKey{Repository: repository, Ref: "refs/heads/main"},
		OperationID: "merge-real-2",
		Base:        newBase,
		Candidate:   candidate,
	}
	observation, err := effects.Execute(context.Background(), operation)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if observation.State != merge.EffectApplied {
		t.Fatalf("observation = %+v; want applied", observation)
	}
	if observation.Base != newBase || observation.Candidate != candidate {
		t.Fatalf("observation = %+v; want the fetched revisions", observation)
	}
	if observation.Output == candidate || observation.Output == newBase {
		t.Fatalf("observation output = %s; want the rebased commit", observation.Output)
	}
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/main"); head != observation.Output {
		t.Fatalf("remote main = %s; want the rebased result %s", head, observation.Output)
	}
	if content := runScratchGit(t, bare, "show", "refs/heads/main:feature.txt"); content != "candidate" {
		t.Fatalf("merged feature content = %q; want candidate", content)
	}
}

// TestGitMergeEffectsRealStaleBaseIsNotStarted proves a remote target ref that
// advanced past the request base is refused before any effect record or push.
// Observe then proves the effect never started, so a held assignment can be
// released instead of staying uncertain forever.
func TestGitMergeEffectsRealStaleBaseIsNotStarted(t *testing.T) {
	root := t.TempDir()
	repository, base, candidate := seedMergeRepository(t, root)
	bare := filepath.Join(root, repository)
	// Advance the target ref after the request base was computed: the push
	// lease would reject the merge, but nothing must be recorded or pushed.
	advance := t.TempDir()
	runScratchGit(t, advance, "clone", "--quiet", bare, ".")
	runScratchGit(t, advance, "checkout", "--quiet", "main")
	writeScratchFile(t, advance, "later.txt", "later\n")
	runScratchGit(t, advance, "commit", "--quiet", "-m", "later")
	advanced := runScratchGit(t, advance, "rev-parse", "HEAD")
	runScratchGit(t, advance, "push", "--quiet", "origin", "main")

	executor, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	stateDirectory := t.TempDir()
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "file://" + filepath.ToSlash(root),
		StateDirectory: stateDirectory,
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	operation := merge.MergeOperation{
		Key:         merge.MergeKey{Repository: repository, Ref: "refs/heads/main"},
		OperationID: "merge-real-stale-1",
		Base:        base,
		Candidate:   candidate,
	}
	observation, err := effects.Execute(context.Background(), operation)
	if err == nil {
		t.Fatal("Execute accepted a request whose remote ref advanced past its base")
	}
	if !errors.Is(err, merge.ErrMergeBaseMoved) {
		t.Fatalf("Execute error = %v; want ErrMergeBaseMoved", err)
	}
	if observation.State != merge.EffectNotStarted {
		t.Fatalf("observation = %+v; want not_started", observation)
	}
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/main"); head != advanced {
		t.Fatalf("remote main = %s; want the advanced commit %s (nothing pushed)", head, advanced)
	}
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/work"); head != candidate {
		t.Fatalf("remote work = %s; want the unchanged candidate %s", head, candidate)
	}
	observed, err := effects.Observe(context.Background(), operation)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if observed.State != merge.EffectNotStarted {
		t.Fatalf("observed = %+v; want not_started because no record was written", observed)
	}
	records, err := filepath.Glob(filepath.Join(stateDirectory, "*.json"))
	if err != nil {
		t.Fatalf("Glob effect records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("effect records = %v; want none before any push", records)
	}

	// The same stale request through the coordinator confirms EffectNotStarted
	// (Reconciled = true) so Settle immediately releases the claim instead of
	// wedging subsequent merges.
	store, err := merge.NewFileCoordinationStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileCoordinationStore: %v", err)
	}
	request := merge.MergeRequest{
		Key:        operation.Key,
		Assignment: merge.MergeAssignment{ExecutionID: "exec-stale-1", ActionID: "action-stale-1"},
		Base:       base,
		Candidate:  candidate,
	}
	coordinator, err := merge.NewCoordinator(merge.CoordinatorConfig{
		TenantID: id.TenantID("tenant-stale"),
		Owner:    "stale-base-test",
		Lease:    time.Minute,
		Store:    store,
		Effects:  effects,
		Clock:    servicekit.SystemClock{},
		NewOperationID: func() (id.OperationID, error) {
			return "merge-op-stale-1", nil
		},
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	result, err := coordinator.Merge(context.Background(), request)
	if !errors.Is(err, merge.ErrMergeBaseMoved) {
		t.Fatalf("Merge error = %v; want ErrMergeBaseMoved", err)
	}
	if result.State != merge.EffectNotStarted {
		t.Fatalf("result = %+v; want confirmed not_started", result)
	}
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/main"); head != advanced {
		t.Fatalf("remote main = %s; want the advanced commit %s (nothing pushed)", head, advanced)
	}
	if err := coordinator.Settle(context.Background(), request, merge.EffectNotStarted); err != nil {
		t.Fatalf("Settle(not_started): %v", err)
	}
	record, ok, err := store.Load(context.Background(), id.TenantID("tenant-stale"), request.Key)
	if err != nil {
		t.Fatalf("store.Load: %v", err)
	}
	if !ok || !record.Released || record.State != merge.EffectNotStarted {
		t.Fatalf("record = %+v; want the assignment released as not_started", record)
	}
}

// TestGitMergeEffectsRealParallelWaveDisjointRebase exercises real native Git
// over a parallel wave where tasks A, B, C, D, and E all branch from C0:
//   - Task A fast-forwards main C0 -> C1 (touching a.txt).
//   - Task B (disjoint b.txt) rebases C0..B onto C1 with kodema.approved_base
//     and kodema.approved_candidate push options and succeeds.
//   - Task C (conflicting edit to a.txt) aborts rebase with ErrMergeConflict
//     and settles immediately without wedging (repo, ref).
//   - Task D (non-conflicting edit to a.txt, rejected by pre-receive for path
//     overlap) returns ErrMergeRejected confirmed by observeRecord and settles
//     immediately without wedging (repo, ref).
//   - Task E (disjoint e.txt) then rebases onto B' and succeeds immediately.
func TestGitMergeEffectsRealParallelWaveDisjointRebase(t *testing.T) {
	root := t.TempDir()
	repository := "shop/web"
	bare := filepath.Join(root, repository)
	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	runScratchGit(t, filepath.Dir(bare), "init", "--bare", "--quiet", "--initial-branch=main", bare)
	runScratchGit(t, bare, "config", "receive.advertisePushOptions", "true")

	seed := t.TempDir()
	runScratchGit(t, seed, "init", "--quiet", "--initial-branch=main")
	writeScratchFile(t, seed, "a.txt", "line1\nline2\nline3\nline4\nline5\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "base C0")
	c0 := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch A: modifies top of a.txt.
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work/a", c0)
	writeScratchFile(t, seed, "a.txt", "line1-from-A\nline2\nline3\nline4\nline5\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "task A")
	candA := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch B: disjoint file b.txt.
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work/b", c0)
	writeScratchFile(t, seed, "b.txt", "from B\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "task B")
	candB := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch C: conflicting edit to top of a.txt.
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work/c", c0)
	writeScratchFile(t, seed, "a.txt", "line1-from-C\nline2\nline3\nline4\nline5\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "task C")
	candC := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch D: non-conflicting edit to bottom of a.txt (rebases cleanly, but
	// overlaps a.txt modified on main since C0).
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work/d", c0)
	writeScratchFile(t, seed, "a.txt", "line1\nline2\nline3\nline4\nline5-from-D\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "task D")
	candD := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch E: disjoint file e.txt.
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work/e", c0)
	writeScratchFile(t, seed, "e.txt", "from E\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "task E")
	candE := runScratchGit(t, seed, "rev-parse", "HEAD")

	runScratchGit(t, seed, "push", "--quiet", bare, "main", "work/a", "work/b", "work/c", "work/d", "work/e")

	// Install a real pre-receive hook recording push options and rejecting
	// rebased pushes whose touched paths overlap approved_base..old_oid.
	optsLog := filepath.Join(t.TempDir(), "push-options.log")
	hookScript := fmt.Sprintf(`#!/bin/sh
set -eu
count="${GIT_PUSH_OPTION_COUNT:-0}"
approved_base=""
approved_cand=""
i=0
while [ "$i" -lt "$count" ]; do
  eval "opt=\${GIT_PUSH_OPTION_${i}:-}"
  printf '%%s\n' "$opt" >> %q
  case "$opt" in
    kodema.approved_base=*) approved_base="${opt#kodema.approved_base=}" ;;
    kodema.approved_candidate=*) approved_cand="${opt#kodema.approved_candidate=}" ;;
  esac
  i=$((i + 1))
done
while read -r old new ref; do
  if [ -n "$approved_base" ] && [ "$approved_base" != "$old" ]; then
    target_files=$(git diff-tree -r --no-commit-id --name-only "$approved_base" "$old")
    cand_files=$(git diff-tree -r --no-commit-id --name-only "$old" "$new")
    for f in $cand_files; do
      for tf in $target_files; do
        if [ "$f" = "$tf" ]; then
          echo "kodema git: update rejected: disjoint rebase refused: overlapping path $f" >&2
          exit 1
        fi
      done
    done
  fi
done
exit 0
`, optsLog)
	hookPath := filepath.Join(bare, "hooks", "pre-receive")
	if err := os.WriteFile(hookPath, []byte(hookScript), 0o755); err != nil {
		t.Fatalf("write pre-receive hook: %v", err)
	}

	executor, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:     "file://" + filepath.ToSlash(root),
		StateDirectory:    t.TempDir(),
		RebaseOnMovedBase: true,
		Executor:          executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	store, err := merge.NewFileCoordinationStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileCoordinationStore: %v", err)
	}
	opCounter := 0
	coordinator, err := merge.NewCoordinator(merge.CoordinatorConfig{
		TenantID: "tenant-wave",
		Owner:    "wave-worker",
		Lease:    time.Minute,
		Store:    store,
		Effects:  effects,
		Clock:    servicekit.SystemClock{},
		NewOperationID: func() (id.OperationID, error) {
			opCounter++
			return id.OperationID(fmt.Sprintf("merge-wave-%d", opCounter)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}
	key := merge.MergeKey{Repository: repository, Ref: "refs/heads/main"}
	makeReq := func(execID, cand string, dec id.DecisionID) merge.MergeRequest {
		return merge.MergeRequest{
			Key:        key,
			Assignment: merge.MergeAssignment{ExecutionID: id.ExecutionID(execID), ActionID: "act-merge"},
			Base:       c0,
			Candidate:  cand,
			DecisionID: dec,
		}
	}

	// 1. Task A fast-forwards C0 -> candA.
	reqA := makeReq("exec-a", candA, "dec_wave_a")
	resA, err := coordinator.Merge(context.Background(), reqA)
	if err != nil || resA.State != merge.EffectApplied || resA.ObservedOutput != candA {
		t.Fatalf("Task A Merge = %+v, %v; want applied output=%s", resA, err, candA)
	}
	if err := coordinator.Settle(context.Background(), reqA, merge.EffectApplied); err != nil {
		t.Fatalf("Task A Settle: %v", err)
	}

	// 2. Task B rebases C0..candB onto candA with disjoint-rebase push options.
	reqB := makeReq("exec-b", candB, "dec_wave_b")
	resB, err := coordinator.Merge(context.Background(), reqB)
	if err != nil || resB.State != merge.EffectApplied {
		t.Fatalf("Task B Merge = %+v, %v; want applied", resB, err)
	}
	if resB.ObservedBase != candA || resB.ObservedOutput == candB || resB.ObservedOutput == candA {
		t.Fatalf("Task B result = %+v; want rebased commit onto %s", resB, candA)
	}
	if err := coordinator.Settle(context.Background(), reqB, merge.EffectApplied); err != nil {
		t.Fatalf("Task B Settle: %v", err)
	}
	loggedOpts, err := os.ReadFile(optsLog)
	if err != nil {
		t.Fatalf("read push-options.log: %v", err)
	}
	for _, want := range []string{
		"kodema.decision_id=dec_wave_b",
		"kodema.approved_base=" + c0,
		"kodema.approved_candidate=" + candB,
	} {
		if !strings.Contains(string(loggedOpts), want) {
			t.Fatalf("push options log missing %q:\n%s", want, loggedOpts)
		}
	}

	// 3. Task C hits rebase conflict -> confirmed not_started -> immediate settle.
	reqC := makeReq("exec-c", candC, "dec_wave_c")
	resC, err := coordinator.Merge(context.Background(), reqC)
	if !errors.Is(err, merge.ErrMergeConflict) || resC.State != merge.EffectNotStarted {
		t.Fatalf("Task C Merge = %+v, %v; want not_started ErrMergeConflict", resC, err)
	}
	if err := coordinator.Settle(context.Background(), reqC, merge.EffectNotStarted); err != nil {
		t.Fatalf("Task C Settle: %v", err)
	}

	// 4. Task D rebases cleanly locally, but pre-receive rejects overlap ->
	//    confirmed not_started -> immediate settle.
	reqD := makeReq("exec-d", candD, "dec_wave_d")
	resD, err := coordinator.Merge(context.Background(), reqD)
	if !errors.Is(err, merge.ErrMergeRejected) || resD.State != merge.EffectNotStarted {
		t.Fatalf("Task D Merge = %+v, %v; want not_started ErrMergeRejected", resD, err)
	}
	if err := coordinator.Settle(context.Background(), reqD, merge.EffectNotStarted); err != nil {
		t.Fatalf("Task D Settle: %v", err)
	}
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/main"); head != resB.ObservedOutput {
		t.Fatalf("main mutated after rejected Task D: got %s, want %s", head, resB.ObservedOutput)
	}

	// 5. Task E (disjoint e.txt) succeeds immediately after C and D settled.
	reqE := makeReq("exec-e", candE, "dec_wave_e")
	resE, err := coordinator.Merge(context.Background(), reqE)
	if err != nil || resE.State != merge.EffectApplied {
		t.Fatalf("Task E Merge = %+v, %v; want applied", resE, err)
	}
	if err := coordinator.Settle(context.Background(), reqE, merge.EffectApplied); err != nil {
		t.Fatalf("Task E Settle: %v", err)
	}
	if content := runScratchGit(t, bare, "show", "refs/heads/main:b.txt"); content != "from B" {
		t.Fatalf("main:b.txt = %q; want 'from B'", content)
	}
	if content := runScratchGit(t, bare, "show", "refs/heads/main:e.txt"); content != "from E" {
		t.Fatalf("main:e.txt = %q; want 'from E'", content)
	}
}

// TestGitMergeEffectsRealNoOpRebaseRefusesWithoutBypassingPreReceive proves
// with real native Git that when a candidate duplicates a patch already landed
// on a moved remote ref, `git rebase --onto` drops the commit (HEAD == remote)
// and Execute refuses with (EffectNotStarted, ErrMergeBaseMoved) without
// writing a record or pushing a local no-op that would bypass pre-receive.
func TestGitMergeEffectsRealNoOpRebaseRefusesWithoutBypassingPreReceive(t *testing.T) {
	root := t.TempDir()
	repository := "shop/web"
	bare := filepath.Join(root, repository)
	if err := os.MkdirAll(filepath.Dir(bare), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	runScratchGit(t, filepath.Dir(bare), "init", "--bare", "--quiet", "--initial-branch=main", bare)
	runScratchGit(t, bare, "config", "receive.advertisePushOptions", "true")

	seed := t.TempDir()
	runScratchGit(t, seed, "init", "--quiet", "--initial-branch=main")
	writeScratchFile(t, seed, "README.md", "base\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "base C0")
	c0 := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch A lands duplicate.txt on main.
	writeScratchFile(t, seed, "duplicate.txt", "same content\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "landed on main")
	c1 := runScratchGit(t, seed, "rev-parse", "HEAD")

	// Branch B from C0 makes the identical change so rebase --onto c1 c0 candB
	// skips the commit and leaves HEAD == c1.
	runScratchGit(t, seed, "checkout", "--quiet", "-b", "work/dup", c0)
	writeScratchFile(t, seed, "duplicate.txt", "same content\n")
	runScratchGit(t, seed, "commit", "--quiet", "-m", "duplicate patch")
	candB := runScratchGit(t, seed, "rev-parse", "HEAD")

	runScratchGit(t, seed, "push", "--quiet", bare, "main", "work/dup")

	executor, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	stateDir := t.TempDir()
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:     "file://" + filepath.ToSlash(root),
		StateDirectory:    stateDir,
		RebaseOnMovedBase: true,
		Executor:          executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	obs, err := effects.Execute(context.Background(), merge.MergeOperation{
		Key:         merge.MergeKey{Repository: repository, Ref: "refs/heads/main"},
		OperationID: "merge-noop-1",
		Base:        c0,
		Candidate:   candB,
	})
	if !errors.Is(err, merge.ErrMergeBaseMoved) {
		t.Fatalf("Execute error = %v; want ErrMergeBaseMoved", err)
	}
	if obs.State != merge.EffectNotStarted {
		t.Fatalf("observation = %+v; want not_started", obs)
	}
	if head := runScratchGit(t, bare, "rev-parse", "refs/heads/main"); head != c1 {
		t.Fatalf("remote main = %s; want unchanged %s", head, c1)
	}
	records, err := filepath.Glob(filepath.Join(stateDir, "*.json"))
	if err != nil {
		t.Fatalf("Glob effect records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("effect records = %v; want none on refused no-op rebase", records)
	}
}

func gitPktLine(payload string) string {
	return fmt.Sprintf("%04x%s", len(payload)+4, payload)
}

// TestGitMergeEffectsRealHTTPSPrivateCA proves OSGitExecutor fails closed
// against a private-CA HTTPS Git server when CAFile is unset, and succeeds for
// both ls-remote (Observe) and fetch (Execute) when CAFile is configured with
// either an absolute or relative PEM bundle path.
func TestGitMergeEffectsRealHTTPSPrivateCA(t *testing.T) {
	const (
		token     = "test-merge-token"
		targetRef = "refs/heads/main"
	)
	baseCommit := strings.Repeat("a", 40)
	candidateCommit := strings.Repeat("b", 40)
	resultCommit := strings.Repeat("c", 40)

	var authenticatedRequests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "kodema" || pass != token {
			w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet || r.URL.Query().Get("service") != "git-upload-pack" {
			http.NotFound(w, r)
			return
		}
		authenticatedRequests.Add(1)
		w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
		w.Header().Set("Cache-Control", "no-cache")
		switch r.URL.Path {
		case "/shop/web/info/refs":
			_, _ = w.Write([]byte(
				gitPktLine("# service=git-upload-pack\n") +
					"0000" +
					gitPktLine(resultCommit+" "+targetRef+"\x00multi_ack_detailed side-band-64k\n") +
					"0000",
			))
		case "/shop/empty/info/refs":
			_, _ = w.Write([]byte(
				gitPktLine("# service=git-upload-pack\n") +
					"0000" +
					"0000",
			))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	caDir := t.TempDir()
	caPath := filepath.Join(caDir, "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: server.Certificate().Raw,
	})
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("WriteFile CA bundle: %v", err)
	}

	credPath := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile credential: %v", err)
	}

	executor, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}

	seedEffectRecord := func(t *testing.T, stateDir string, opID id.OperationID) {
		t.Helper()
		payload, err := json.Marshal(struct {
			Base      string `json:"base"`
			Candidate string `json:"candidate"`
			Result    string `json:"result"`
		}{
			Base:      baseCommit,
			Candidate: candidateCommit,
			Result:    resultCommit,
		})
		if err != nil {
			t.Fatalf("json.Marshal effect record: %v", err)
		}
		if err := os.WriteFile(filepath.Join(stateDir, string(opID)+".json"), payload, 0o600); err != nil {
			t.Fatalf("WriteFile effect record: %v", err)
		}
	}

	op := merge.MergeOperation{
		Key:         merge.MergeKey{Repository: "shop/web", Ref: targetRef},
		OperationID: "merge-tls-1",
		Base:        baseCommit,
		Candidate:   candidateCommit,
	}

	// 1. Without CAFile, real git fails closed with a TLS certificate error.
	t.Run("without CAFile fails closed on TLS", func(t *testing.T) {
		stateDir := t.TempDir()
		seedEffectRecord(t, stateDir, op.OperationID)
		effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
			RemoteBaseURL:  server.URL,
			StateDirectory: stateDir,
			CredentialFile: credPath,
			Executor:       executor,
		})
		if err != nil {
			t.Fatalf("NewGitMergeEffects: %v", err)
		}
		before := authenticatedRequests.Load()
		observation, err := effects.Observe(context.Background(), op)
		if err == nil {
			t.Fatalf("Observe succeeded without CAFile: %+v", observation)
		}
		if observation.State != merge.EffectUncertain {
			t.Fatalf("observation state = %q; want uncertain", observation.State)
		}
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "certificate") && !strings.Contains(lower, "ssl") && !strings.Contains(lower, "tls") {
			t.Fatalf("Observe error = %q; want a TLS/certificate failure", err)
		}
		if authenticatedRequests.Load() != before {
			t.Fatalf("server received an HTTP request despite missing CAFile trust")
		}
	})

	// 2. With absolute CAFile, Observe completes TLS + Basic Auth and reads the ref.
	t.Run("with absolute CAFile observes advertised ref", func(t *testing.T) {
		stateDir := t.TempDir()
		seedEffectRecord(t, stateDir, op.OperationID)
		effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
			RemoteBaseURL:  server.URL,
			StateDirectory: stateDir,
			CredentialFile: credPath,
			CAFile:         caPath,
			Executor:       executor,
		})
		if err != nil {
			t.Fatalf("NewGitMergeEffects: %v", err)
		}
		before := authenticatedRequests.Load()
		observation, err := effects.Observe(context.Background(), op)
		if err != nil {
			t.Fatalf("Observe with absolute CAFile: %v", err)
		}
		if observation.State != merge.EffectApplied || observation.Output != resultCommit {
			t.Fatalf("observation = %+v; want applied with output %s", observation, resultCommit)
		}
		if authenticatedRequests.Load() <= before {
			t.Fatalf("expected authenticated HTTPS request to reach test server")
		}
	})

	// 3. With relative CAFile, git -C <scratchWorkDir> still resolves the CA bundle
	// because NewGitMergeEffects normalizes CAFile to an absolute path.
	t.Run("with relative CAFile observes advertised ref and fetches over TLS", func(t *testing.T) {
		t.Chdir(caDir)
		stateDir := t.TempDir()
		seedEffectRecord(t, stateDir, op.OperationID)
		effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
			RemoteBaseURL:  server.URL,
			StateDirectory: stateDir,
			CredentialFile: credPath,
			CAFile:         "ca.pem",
			Executor:       executor,
		})
		if err != nil {
			t.Fatalf("NewGitMergeEffects: %v", err)
		}
		observation, err := effects.Observe(context.Background(), op)
		if err != nil {
			t.Fatalf("Observe with relative CAFile: %v", err)
		}
		if observation.State != merge.EffectApplied || observation.Output != resultCommit {
			t.Fatalf("observation = %+v; want applied with output %s", observation, resultCommit)
		}

		// Also verify `git -C <workDir> fetch` succeeds over HTTPS with
		// http.<origin>.sslCAInfo against an empty smart-HTTP repository
		// (Execute then fails locally at resolveCommit, proving fetch succeeded).
		fetchOp := merge.MergeOperation{
			Key:         merge.MergeKey{Repository: "shop/empty", Ref: targetRef},
			OperationID: "merge-tls-fetch-1",
			Base:        baseCommit,
			Candidate:   candidateCommit,
		}
		before := authenticatedRequests.Load()
		fetchObs, fetchErr := effects.Execute(context.Background(), fetchOp)
		if fetchErr == nil {
			t.Fatalf("Execute unexpectedly succeeded on empty repository: %+v", fetchObs)
		}
		if !strings.Contains(fetchErr.Error(), "resolve merge base") {
			t.Fatalf("Execute error = %q; want git fetch to succeed over TLS and fail locally at resolve merge base", fetchErr)
		}
		if authenticatedRequests.Load() <= before {
			t.Fatalf("expected authenticated HTTPS fetch request to reach test server")
		}
	})
}
