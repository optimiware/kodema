package publiccontract

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/git"
	gitapp "github.com/optimiware/kodema/internal/apps/git/app"
	"github.com/optimiware/kodema/internal/apps/git/app/actors"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/apps/merge"
	mergeapp "github.com/optimiware/kodema/internal/apps/merge/app"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

type waveDecisionRecord struct {
	resourceID string
	ref        string
	base       string
	candidate  string
	role       string
}

type waveGitVerifier struct {
	mu        sync.Mutex
	decisions map[string]waveDecisionRecord
	records   map[string]api.VerifyRequest
	nextID    int
}

func newWaveGitVerifier() *waveGitVerifier {
	return &waveGitVerifier{
		decisions: make(map[string]waveDecisionRecord),
		records:   make(map[string]api.VerifyRequest),
	}
}

func (v *waveGitVerifier) register(decisionID string, rec waveDecisionRecord) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.decisions[decisionID] = rec
}

func (v *waveGitVerifier) Verify(_ context.Context, req api.VerifyRequest) (api.VerifyResponse, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	var (
		principal api.VerifiedPrincipal
		roles     []string
	)
	switch req.ActorCredential {
	case "admin-token":
		principal = api.VerifiedPrincipal{ID: "usr-admin", Kind: "human"}
		roles = []string{"admin"}
	case "exec-seed-token":
		principal = api.VerifiedPrincipal{ID: "exe-seed", Kind: "execution"}
	case "merge-coord-token":
		principal = api.VerifiedPrincipal{ID: "merge-coordinator", Kind: "service"}
	default:
		return api.VerifyResponse{}, fault.New(fault.Unauthenticated, "invalid token")
	}
	approved := false
	if req.Approval != "" {
		if req.DecisionID != "" {
			if dec, ok := v.decisions[req.DecisionID]; ok && "role/"+dec.role == req.Approval && dec.resourceID == req.ResourceID {
				fields := make(map[string]string, len(req.Subject.Fields))
				for _, f := range req.Subject.Fields {
					fields[f.Name] = f.Value
				}
				if fields["ref"] == dec.ref && fields["expected_base"] == dec.base && fields["candidate"] == dec.candidate {
					approved = true
				}
			}
		}
	}
	v.nextID++
	verID := fmt.Sprintf("ver-wave-%d", v.nextID)
	v.records[verID] = req
	return api.VerifyResponse{
		Principal:      principal,
		Roles:          roles,
		Approval:       approved,
		VerificationID: verID,
	}, nil
}

func (v *waveGitVerifier) Report(_ context.Context, req api.ReportRequest) (api.ReportResponse, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.records[req.VerificationID]; !ok {
		return api.ReportResponse{}, fault.New(fault.NotFound, "verification not found")
	}
	return api.ReportResponse{
		VerificationID: req.VerificationID,
		EventID:        "evt-" + req.VerificationID,
	}, nil
}

type wavePublicHandler struct {
	mu       sync.Mutex
	items    []api.InboxItem
	resolves []api.ExecutionRequestResolveRequest
	settled  map[string]api.ExecutionRequestResolveRequest
}

func (h *wavePublicHandler) Invoke(_ context.Context, inv api.Invocation) (api.Result, error) {
	switch inv.Operation {
	case servicekit.OperationInbox:
		h.mu.Lock()
		items := append([]api.InboxItem(nil), h.items...)
		h.mu.Unlock()
		body, err := json.Marshal(api.TaskInboxResult{Items: items})
		return api.Result{Operation: inv.Operation, Body: body}, err
	case servicekit.OperationInspect:
		var req api.ExecutionInspectRequest
		if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
			return api.Result{}, err
		}
		h.mu.Lock()
		dec, isSettled := h.settled[req.ExecutionID]
		h.mu.Unlock()
		action := api.ExecutionAction{
			ActionID: "act-merge",
			Index:    0,
			Name:     "merge",
			Kind:     "request",
			State:    "running",
			Version:  1,
		}
		if isSettled {
			action.Decision = dec.Decision
			action.Version = 2
			if dec.Decision == servicekit.DecisionPass {
				action.State = "succeeded"
			} else {
				action.State = "failed"
			}
		}
		body, err := json.Marshal(api.ExecutionInspectResult{
			ExecutionID:   req.ExecutionID,
			PipelineID:    "pipe-merge",
			State:         "running",
			Version:       1,
			CurrentAction: 0,
			Actions:       []api.ExecutionAction{action},
			Cleanup:       api.ExecutionCleanup{State: "none"},
		})
		return api.Result{Operation: inv.Operation, Body: body}, err
	case servicekit.OperationResolve:
		var req api.ExecutionRequestResolveRequest
		if err := json.Unmarshal(inv.Request.Input, &req); err != nil {
			return api.Result{}, err
		}
		h.mu.Lock()
		h.resolves = append(h.resolves, req)
		if h.settled == nil {
			h.settled = make(map[string]api.ExecutionRequestResolveRequest)
		}
		h.settled[req.ExecutionID] = req
		h.mu.Unlock()
		body, err := json.Marshal(api.ExecutionRequestResolveResult{
			ExecutionVersion: 2,
			ActionVersion:    2,
		})
		return api.Result{Operation: inv.Operation, Body: body}, err
	default:
		return api.Result{}, fmt.Errorf("unexpected operation %q", inv.Operation)
	}
}

type waveStaticEvents struct{}

func (waveStaticEvents) Poll(context.Context, string) (servicekit.NotificationPage, error) {
	return servicekit.NotificationPage{}, nil
}

type waveStaticClock struct{ now time.Time }

func (c waveStaticClock) Now() time.Time { return c.now }

func runWaveGit(t *testing.T, dir, token string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"LC_ALL=C",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=WaveTest",
		"GIT_AUTHOR_EMAIL=wave@example.invalid",
		"GIT_COMMITTER_NAME=WaveTest",
		"GIT_COMMITTER_EMAIL=wave@example.invalid",
	}
	if token != "" {
		basic := base64.StdEncoding.EncodeToString([]byte("kodema:" + token))
		env = append(env,
			"GIT_CONFIG_COUNT=1",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0=Authorization: Basic "+basic,
		)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// TestMergeCoordinatorParallelWaveAgainstRealGitServeDisjointRebase proves end
// to end that `merge.Adapter` + `merge.Coordinator` + `merge.GitMergeEffects`
// interoperates with real `kodema git serve` (`disjoint_rebase.go` pre-receive):
//   - Task A fast-forwards main C0 -> C1 (a.txt).
//   - Task B (disjoint b.txt) rebases C0..B onto C1 with
//     -o kodema.approved_base -o kodema.approved_candidate -o kodema.decision_id
//     and is accepted by real pre-receive.
//   - Task C (conflicting a.txt) aborts rebase -> resolved `fail` + settled.
//   - Task D (overlapping non-conflicting a.txt) is rejected by real
//     disjoint_rebase.go pre-receive -> resolved `fail` with sanitized note + settled.
//   - Task E (disjoint e.txt) then rebases onto B' and succeeds immediately in
//     the very same scan pass without wedging.
func TestMergeCoordinatorParallelWaveAgainstRealGitServeDisjointRebase(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary unavailable")
	}
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	kodemaBin := filepath.Join(t.TempDir(), "kodema")
	buildCmd := exec.Command("go", "build", "-o", kodemaBin, "./cmd/kodema")
	buildCmd.Dir = repoRoot
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build kodema: %v\n%s", err, out)
	}

	const tenantID = id.TenantID("tenant-wave")
	storageRoot := filepath.Join(t.TempDir(), "repos")
	journalDir := filepath.Join(t.TempDir(), "journal")
	socketBase, err := os.MkdirTemp("", "k-wave-")
	if err != nil {
		t.Fatalf("MkdirTemp socket: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketBase) })

	if _, err := gitapp.ProvisionTenant(context.Background(), gitapp.ProvisionConfig{
		TenantID:         tenantID,
		StorageRoot:      storageRoot,
		JournalDirectory: journalDir,
		HookBinary:       kodemaBin,
	}); err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if _, err := gitapp.ProvisionRepository(context.Background(), gitapp.RepositoryProvisionConfig{
		TenantID:    tenantID,
		Repository:  "shop/web",
		ResourceID:  "res-shop-web",
		StorageRoot: storageRoot,
		HookBinary:  kodemaBin,
	}); err != nil {
		t.Fatalf("ProvisionRepository: %v", err)
	}

	verifier := newWaveGitVerifier()
	readVerifier, err := git.NewReadVerifier(verifier, func(repository string) (string, error) {
		return "", git.UnregisteredRepositoryError(repository)
	})
	if err != nil {
		t.Fatalf("NewReadVerifier: %v", err)
	}
	registry, err := actors.NewRegistry(actors.Config{TenantID: tenantID})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	t.Cleanup(func() { _ = registry.Close() })

	gitServer, err := gitapp.New(gitapp.Config{
		TenantID:            tenantID,
		StorageRoot:         storageRoot,
		JournalDirectory:    journalDir,
		ResourceIDs:         map[string]string{"admin": "res-admin"},
		HookBinary:          kodemaBin,
		HookSocketDirectory: socketBase,
		Verify:              verifier,
		Actors:              readVerifier,
		ActorHandles:        registry,
		HookActors:          registry,
	})
	if err != nil {
		t.Fatalf("gitapp.New: %v", err)
	}
	if err := gitServer.StartHooks(context.Background()); err != nil {
		t.Fatalf("StartHooks: %v", err)
	}
	t.Cleanup(func() { _ = gitServer.Close() })

	httpServer := httptest.NewServer(gitServer.Handler())
	t.Cleanup(httpServer.Close)

	// Configure shop/web with approve + approve-rebase = disjoint and merge-coordinator write rule.
	adminWork := filepath.Join(t.TempDir(), "admin")
	runWaveGit(t, "", "admin-token", "clone", "--quiet", httpServer.URL+"/admin", adminWork)
	gitConf := strings.Join([]string{
		"repo admin",
		"    RW = @admin",
		"",
		"repo shop/web",
		"    option resource = res-shop-web",
		"    R = @all",
		"    RW+ refs/heads/work/ = @execution",
		"    RW refs/heads/main = @execution merge-coordinator",
		"    option approve.refs/heads/main = reviewer",
		"    option approve-rebase.refs/heads/main = disjoint",
		"",
	}, "\n")
	if err := os.WriteFile(filepath.Join(adminWork, "conf", "git.conf"), []byte(gitConf), 0o644); err != nil {
		t.Fatalf("write git.conf: %v", err)
	}
	runWaveGit(t, adminWork, "", "add", "conf/git.conf")
	runWaveGit(t, adminWork, "", "commit", "-q", "-m", "enable disjoint rebase for merge-coordinator")
	runWaveGit(t, adminWork, "admin-token", "push", "-q", "origin", "HEAD:refs/heads/main")

	// Seed shop/web with C0 and five parallel branches A, B, C, D, E.
	repoURL := httpServer.URL + "/shop/web"
	seed := filepath.Join(t.TempDir(), "seed")
	runWaveGit(t, "", "exec-seed-token", "clone", "--quiet", repoURL, seed)
	if err := os.WriteFile(filepath.Join(seed, "a.txt"), []byte("line1\nline2\nline3\nline4\nline5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runWaveGit(t, seed, "", "add", "a.txt")
	runWaveGit(t, seed, "", "commit", "-q", "-m", "base C0")
	c0 := runWaveGit(t, seed, "", "rev-parse", "HEAD")
	verifier.register("dec_seed_c0", waveDecisionRecord{
		resourceID: "res-shop-web", ref: "refs/heads/main", base: "", candidate: c0, role: "reviewer",
	})
	runWaveGit(t, seed, "exec-seed-token", "push", "-q", "-o", "kodema.decision_id=dec_seed_c0", "origin", "HEAD:refs/heads/main")

	makeBranch := func(branch, file, content, msg string) string {
		runWaveGit(t, seed, "", "checkout", "-q", "-b", branch, c0)
		if err := os.WriteFile(filepath.Join(seed, file), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		runWaveGit(t, seed, "", "add", file)
		runWaveGit(t, seed, "", "commit", "-q", "-m", msg)
		sha := runWaveGit(t, seed, "", "rev-parse", "HEAD")
		runWaveGit(t, seed, "exec-seed-token", "push", "-q", "origin", "HEAD:refs/heads/"+branch)
		return sha
	}

	candA := makeBranch("work/a", "a.txt", "line1-A\nline2\nline3\nline4\nline5\n", "task A")
	candB := makeBranch("work/b", "b.txt", "from B\n", "task B")
	candC := makeBranch("work/c", "a.txt", "line1-C\nline2\nline3\nline4\nline5\n", "task C conflict")
	candD := makeBranch("work/d", "a.txt", "line1\nline2\nline3\nline4\nline5-D\n", "task D overlap")
	candE := makeBranch("work/e", "e.txt", "from E\n", "task E")

	for decID, cand := range map[string]string{
		"dec_wave_a": candA,
		"dec_wave_b": candB,
		"dec_wave_c": candC,
		"dec_wave_d": candD,
		"dec_wave_e": candE,
	} {
		verifier.register(decID, waveDecisionRecord{
			resourceID: "res-shop-web", ref: "refs/heads/main", base: c0, candidate: cand, role: "reviewer",
		})
	}

	// Wire real merge.GitMergeEffects + merge.Coordinator + merge.Adapter.
	credFile := filepath.Join(t.TempDir(), "merge-token")
	if err := os.WriteFile(credFile, []byte("merge-coord-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitExec, err := mergeapp.NewOSGitExecutor(mergeapp.OSGitExecutorConfig{})
	if err != nil {
		t.Fatalf("NewOSGitExecutor: %v", err)
	}
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:     httpServer.URL,
		StateDirectory:    filepath.Join(t.TempDir(), "effects"),
		CredentialFile:    credFile,
		RebaseOnMovedBase: true,
		Executor:          gitExec,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	store, err := merge.NewFileCoordinationStore(filepath.Join(t.TempDir(), "coord"))
	if err != nil {
		t.Fatalf("NewFileCoordinationStore: %v", err)
	}
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	opNum := 0
	coordinator, err := merge.NewCoordinator(merge.CoordinatorConfig{
		TenantID: tenantID,
		Owner:    "wave-merge-coordinator",
		Lease:    2 * time.Minute,
		Store:    store,
		Effects:  effects,
		Clock:    waveStaticClock{now: now},
		NewOperationID: func() (id.OperationID, error) {
			opNum++
			return id.OperationID(fmt.Sprintf("op-wave-%d", opNum)), nil
		},
	})
	if err != nil {
		t.Fatalf("NewCoordinator: %v", err)
	}

	makeInboxItem := func(execID, cand, decID string) api.InboxItem {
		return api.InboxItem{
			Kind:        servicekit.RequestInboxKind,
			ExecutionID: execID,
			ActionID:    "act-merge",
			Assignment:  "service/merge-coordinator",
			Subject: &api.Subject{Fields: []api.SubjectField{
				{Name: merge.MergeFieldRepository, Kind: "string", Value: "shop/web"},
				{Name: merge.MergeFieldRef, Kind: "string", Value: "refs/heads/main"},
				{Name: merge.MergeFieldBase, Kind: "string", Value: c0},
				{Name: merge.MergeFieldCandidate, Kind: "string", Value: cand},
				{Name: merge.MergeFieldDecisionID, Kind: "string", Value: decID},
			}},
			Outputs: []string{merge.MergeOutputBase, merge.MergeOutputCandidate, merge.MergeOutputResult},
			Version: 1,
			At:      now.Add(time.Hour),
		}
	}

	pubHandler := &wavePublicHandler{
		items: []api.InboxItem{
			makeInboxItem("exec-a", candA, "dec_wave_a"),
			makeInboxItem("exec-b", candB, "dec_wave_b"),
			makeInboxItem("exec-c", candC, "dec_wave_c"),
			makeInboxItem("exec-d", candD, "dec_wave_d"),
			makeInboxItem("exec-e", candE, "dec_wave_e"),
		},
	}

	adapter, err := merge.NewAdapter(merge.AdapterConfig{
		ServiceID: "merge-coordinator",
		TenantID:  tenantID,
		ProjectID: "proj-wave",
		Handler:   pubHandler,
		Events:    waveStaticEvents{},
		Clock:     waveStaticClock{now: now},
		Merges:    coordinator,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}

	// All 5 wave requests process cleanly in a single RunOnce pass!
	if err := adapter.RunOnce(context.Background()); err != nil {
		t.Fatalf("adapter.RunOnce: %v", err)
	}

	if len(pubHandler.resolves) != 5 {
		t.Fatalf("resolves = %d; want all 5 wave requests settled (got %+v)", len(pubHandler.resolves), pubHandler.resolves)
	}

	byExec := make(map[string]api.ExecutionRequestResolveRequest, 5)
	for _, r := range pubHandler.resolves {
		byExec[r.ExecutionID] = r
	}

	// A passed (fast-forward).
	if got := byExec["exec-a"]; got.Decision != servicekit.DecisionPass || got.Outputs[merge.MergeOutputResult] != candA {
		t.Fatalf("exec-a resolve = %+v; want pass with output %s", got, candA)
	}
	// B passed (disjoint rebase accepted by real disjoint_rebase.go pre-receive).
	if got := byExec["exec-b"]; got.Decision != servicekit.DecisionPass || got.Outputs[merge.MergeOutputBase] != candA || got.Outputs[merge.MergeOutputResult] == candB {
		t.Fatalf("exec-b resolve = %+v; want pass with rebased commit onto %s", got, candA)
	}
	// C failed (rebase conflict) with sanitized note and no leaked token.
	if got := byExec["exec-c"]; got.Decision != servicekit.DecisionFail || got.Note == "" || strings.Contains(got.Note, "merge-coord-token") {
		t.Fatalf("exec-c resolve = %+v; want fail with sanitized note", got)
	}
	// D failed (real disjoint_rebase.go pre-receive overlap rejection) with sanitized note.
	if got := byExec["exec-d"]; got.Decision != servicekit.DecisionFail || !strings.Contains(got.Note, "disjoint rebase refused") || strings.Contains(got.Note, "merge-coord-token") {
		t.Fatalf("exec-d resolve = %+v; want fail citing disjoint rebase refused without secrets", got)
	}
	// E passed immediately after C and D settled without waiting for request expiry!
	if got := byExec["exec-e"]; got.Decision != servicekit.DecisionPass || got.Outputs[merge.MergeOutputBase] != byExec["exec-b"].Outputs[merge.MergeOutputResult] {
		t.Fatalf("exec-e resolve = %+v; want pass rebased onto B' (%s)", got, byExec["exec-b"].Outputs[merge.MergeOutputResult])
	}

	// Verify final repository contents on refs/heads/main contain A, B, and E (and not C or D).
	webBare := filepath.Join(storageRoot, string(tenantID), "shop", "web")
	if got := runWaveGit(t, webBare, "", "show", "refs/heads/main:a.txt"); got != "line1-A\nline2\nline3\nline4\nline5" {
		t.Fatalf("main:a.txt = %q; want Task A content only", got)
	}
	if got := runWaveGit(t, webBare, "", "show", "refs/heads/main:b.txt"); got != "from B" {
		t.Fatalf("main:b.txt = %q; want 'from B'", got)
	}
	if got := runWaveGit(t, webBare, "", "show", "refs/heads/main:e.txt"); got != "from E" {
		t.Fatalf("main:e.txt = %q; want 'from E'", got)
	}
}
