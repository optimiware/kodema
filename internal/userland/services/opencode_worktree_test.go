package services

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// realTestGitExecutor runs native git commands without a shell, matching
// OSGitExecutor's control-character refusal and minimal environment.
type realTestGitExecutor struct {
	mu       sync.Mutex
	commands []GitCommand
}

func (e *realTestGitExecutor) Run(ctx context.Context, cmd GitCommand) (GitCommandResult, error) {
	args := make([]string, 0, len(cmd.Args)+2)
	if cmd.Directory != "" {
		args = append(args, "-C", cmd.Directory)
	}
	args = append(args, cmd.Args...)
	for _, a := range args {
		if strings.ContainsAny(a, "\x00\n\r") {
			return GitCommandResult{}, fmt.Errorf("native git argument contains a control character")
		}
	}
	e.mu.Lock()
	e.commands = append(e.commands, GitCommand{
		Directory: cmd.Directory,
		Args:      append([]string(nil), cmd.Args...),
		Env:       append([]string(nil), cmd.Env...),
	})
	e.mu.Unlock()

	c := exec.CommandContext(ctx, "git", args...)
	c.Env = append([]string{
		"LC_ALL=C",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"PATH=" + os.Getenv("PATH"),
	}, cmd.Env...)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	err := c.Run()
	res := GitCommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			res.ExitCode = exitErr.ExitCode()
			return res, nil
		}
		return res, err
	}
	return res, nil
}

func (e *realTestGitExecutor) snapshot() []GitCommand {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]GitCommand(nil), e.commands...)
}

func runSeedGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = []string{
		"LC_ALL=C",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=Test",
		"GIT_AUTHOR_EMAIL=test@kodema.invalid",
		"GIT_COMMITTER_NAME=Test",
		"GIT_COMMITTER_EMAIL=test@kodema.invalid",
		"PATH=" + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

func initBareRemoteRepo(t *testing.T, root, name string) (string, string) {
	t.Helper()
	bare := filepath.Join(root, name)
	if err := os.MkdirAll(bare, 0o755); err != nil {
		t.Fatalf("mkdir bare: %v", err)
	}
	runSeedGit(t, bare, "init", "--bare", "--initial-branch=main")

	seed := t.TempDir()
	runSeedGit(t, seed, "init", "--initial-branch=main")
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("# "+name+"\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runSeedGit(t, seed, "add", "README.md")
	runSeedGit(t, seed, "commit", "-m", "initial "+name)
	sha := runSeedGit(t, seed, "rev-parse", "HEAD")
	runSeedGit(t, seed, "remote", "add", "origin", bare)
	runSeedGit(t, seed, "push", "origin", "HEAD:refs/heads/main")
	return bare, sha
}

func TestGitOpenCodeWorktreeValidationAndFlagInjectionRejection(t *testing.T) {
	execFake := &realTestGitExecutor{}
	stateDir := t.TempDir()

	// Invalid RemoteBaseURLs must be rejected.
	badBaseURLs := []string{
		"",
		"-oProxyCommand=evil",
		"ext::sh -c evil",
		"ftp://example.com/git",
		"http://user:pass@example.com/git",
		"https://example.com/git?token=1",
		"https://example.com/git#frag",
		"http://",
		"file://",
		"https://example.com/git\nnewline",
	}
	for _, raw := range badBaseURLs {
		_, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
			RemoteBaseURL:  raw,
			StateDirectory: stateDir,
			Executor:       execFake,
		})
		if err == nil {
			t.Fatalf("NewGitOpenCodeWorktreeManager(%q) succeeded; want error", raw)
		}
	}

	// Valid schemes accepted.
	for _, raw := range []string{
		"http://git.local:8080/org",
		"https://git.local/org",
		"file:///srv/git",
		"ssh://git.local/org",
	} {
		if _, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
			RemoteBaseURL:  raw,
			StateDirectory: stateDir,
			Executor:       execFake,
		}); err != nil {
			t.Fatalf("NewGitOpenCodeWorktreeManager(%q) failed: %v", raw, err)
		}
	}

	mgr, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
		RemoteBaseURL:  "file://" + t.TempDir(),
		StateDirectory: stateDir,
		Executor:       execFake,
	})
	if err != nil {
		t.Fatalf("NewGitOpenCodeWorktreeManager: %v", err)
	}

	// Invalid repositories (flag injection / traversal / bad segments).
	for _, badRepo := range []string{"", "-evil", "org/-evil", "../escape", "a/../b", "a/./b", "a//b", ".hidden/.."} {
		if _, err := mgr.Prepare(context.Background(), badRepo, "task-1", "refs/heads/main"); err == nil {
			t.Fatalf("Prepare(repo=%q) succeeded; want error", badRepo)
		}
	}
	// Invalid task IDs.
	for _, badTask := range []string{"", "-flag", ".dot", "../escape", "a/b"} {
		if _, err := mgr.Prepare(context.Background(), "app", badTask, "refs/heads/main"); err == nil {
			t.Fatalf("Prepare(task=%q) succeeded; want error", badTask)
		}
	}
	// Invalid baseRef.
	for _, badRef := range []string{"-flag", "refs/heads/..", "refs/heads/main.lock", "main@{0}", "main*"} {
		if _, err := mgr.Prepare(context.Background(), "app", "task-1", badRef); err == nil {
			t.Fatalf("Prepare(baseRef=%q) succeeded; want error", badRef)
		}
	}

	// Collision-free safeRepoSlug: "a/b" and "a__b" must not collide.
	if safeRepoSlug("a/b") == safeRepoSlug("a__b") {
		t.Fatalf("safeRepoSlug collided for a/b and a__b: %q", safeRepoSlug("a/b"))
	}
}

func TestGitOpenCodeWorktreeHardeningNeutralizesHooksConfigAndOriginHijack(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	gitRoot := filepath.Join(tmp, "origin")
	bareRepo, _ := initBareRemoteRepo(t, gitRoot, "app")
	trapRepo, _ := initBareRemoteRepo(t, tmp, "trap")

	stateDir := filepath.Join(tmp, "state")
	execReal := &realTestGitExecutor{}
	mgr, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
		RemoteBaseURL:  "file://" + gitRoot,
		StateDirectory: stateDir,
		Executor:       execReal,
	})
	if err != nil {
		t.Fatalf("NewGitOpenCodeWorktreeManager: %v", err)
	}

	checkout, err := mgr.Prepare(ctx, "app", "task-hardened", "refs/heads/main")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	// Plant malicious hooks, core.hooksPath, core.fsmonitor, http.proxy,
	// credential.helper, AND mutate origin URL to trapRepo in mirror config!
	mirrorPath := filepath.Join(stateDir, "repos", safeRepoSlug("app")+".git")
	evilHooksDir := filepath.Join(tmp, "evil-hooks")
	if err := os.MkdirAll(evilHooksDir, 0o755); err != nil {
		t.Fatalf("mkdir evil-hooks: %v", err)
	}
	markerFile := filepath.Join(tmp, "hook-executed-marker")
	hookScript := fmt.Sprintf("#!/bin/sh\necho PWNED > %q\nexit 1\n", markerFile)
	for _, dir := range []string{filepath.Join(mirrorPath, "hooks"), evilHooksDir} {
		_ = os.MkdirAll(dir, 0o755)
		for _, hook := range []string{"pre-commit", "pre-push", "post-commit"} {
			p := filepath.Join(dir, hook)
			if err := os.WriteFile(p, []byte(hookScript), 0o755); err != nil {
				t.Fatalf("write hook %s: %v", p, err)
			}
		}
	}
	runSeedGit(t, mirrorPath, "config", "core.hooksPath", evilHooksDir)
	runSeedGit(t, mirrorPath, "config", "core.fsmonitor", "/bin/false")
	runSeedGit(t, mirrorPath, "config", "http.proxy", "http://127.0.0.1:1")
	runSeedGit(t, mirrorPath, "config", "credential.helper", "!sh -c 'echo PWNED > "+markerFile+"'")
	runSeedGit(t, mirrorPath, "remote", "set-url", "origin", "file://"+trapRepo)

	// Make a change and commit/push with a multiline & NUL-containing commit message!
	if err := os.WriteFile(filepath.Join(checkout.WorktreePath, "change.txt"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write change.txt: %v", err)
	}
	pushRes, err := mgr.CommitAndPush(ctx, checkout, "opencode(implement): title\nwith newline\r\nand\x00nul")
	if err != nil {
		t.Fatalf("CommitAndPush failed under malicious hooks/origin: %v", err)
	}
	if !pushRes.Committed || len(pushRes.Candidate) != 40 {
		t.Fatalf("unexpected pushRes: %+v", pushRes)
	}

	// Malicious hook must NEVER have executed.
	if _, err := os.Stat(markerFile); !os.IsNotExist(err) {
		t.Fatalf("malicious hook/helper executed and created %s", markerFile)
	}

	// Legitimate bareRepo MUST have the pushed commit, and trapRepo MUST NOT!
	gotTip := runSeedGit(t, bareRepo, "rev-parse", "refs/heads/work/task-hardened")
	if gotTip != pushRes.Candidate {
		t.Fatalf("bareRepo tip = %q; want %q", gotTip, pushRes.Candidate)
	}
	cmdCheckTrap := exec.Command("git", "-C", trapRepo, "rev-parse", "--verify", "refs/heads/work/task-hardened")
	if err := cmdCheckTrap.Run(); err == nil {
		t.Fatal("push leaked to mutated origin trapRepo instead of validated remoteURL")
	}

	// Verify sanitized commit message has no newlines or NULs.
	commitBody := runSeedGit(t, bareRepo, "log", "-1", "--format=%B", pushRes.Candidate)
	if strings.ContainsAny(commitBody, "\r\n\x00") {
		t.Fatalf("commit message still contains control chars: %q", commitBody)
	}
	if !strings.Contains(commitBody, "title with newline and nul") {
		t.Fatalf("sanitized commit message = %q; want sanitized text", commitBody)
	}

	// Verify EVERY executed git command included explicit hardening -c flags.
	var sawCommitNoVerify, sawPushExplicitURL bool
	for _, c := range execReal.snapshot() {
		joined := strings.Join(c.Args, " ")
		for _, required := range []string{
			"-c core.hooksPath=" + os.DevNull,
			"-c core.fsmonitor=false",
			"-c core.sshCommand=/bin/false",
			"-c http.proxy=",
			"-c credential.helper=",
		} {
			if !strings.Contains(joined, required) {
				t.Fatalf("git command missing hardening override %q: %v", required, c.Args)
			}
		}
		if strings.Contains(joined, "commit --no-verify -m") {
			sawCommitNoVerify = true
		}
		wantPush := "push --porcelain --no-verify -- file://" + gitRoot + "/app HEAD:refs/heads/work/task-hardened"
		if strings.Contains(joined, wantPush) {
			sawPushExplicitURL = true
		}
	}
	if !sawCommitNoVerify {
		t.Fatal("did not observe git commit --no-verify")
	}
	if !sawPushExplicitURL {
		t.Fatal("did not observe git push --porcelain --no-verify -- <explicit remoteURL>")
	}
}

func TestGitOpenCodeWorktreeMultiRepoIsolationHealthyCheckAndConcurrentMirrorLock(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	gitRoot := filepath.Join(tmp, "origin")
	initBareRemoteRepo(t, gitRoot, "repo-a")
	initBareRemoteRepo(t, gitRoot, "repo-b")

	mgr, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
		RemoteBaseURL:  "file://" + gitRoot,
		StateDirectory: filepath.Join(tmp, "state"),
		Executor:       &realTestGitExecutor{},
	})
	if err != nil {
		t.Fatalf("NewGitOpenCodeWorktreeManager: %v", err)
	}

	// Same taskID across two repositories must have isolated worktrees.
	coA, err := mgr.Prepare(ctx, "repo-a", "task-shared", "refs/heads/main")
	if err != nil {
		t.Fatalf("Prepare repo-a: %v", err)
	}
	coB, err := mgr.Prepare(ctx, "repo-b", "task-shared", "refs/heads/main")
	if err != nil {
		t.Fatalf("Prepare repo-b: %v", err)
	}
	if coA.WorktreePath == coB.WorktreePath {
		t.Fatalf("repo-a and repo-b collided on worktree path %q", coA.WorktreePath)
	}
	dataA, _ := os.ReadFile(filepath.Join(coA.WorktreePath, "README.md"))
	dataB, _ := os.ReadFile(filepath.Join(coB.WorktreePath, "README.md"))
	if !strings.Contains(string(dataA), "repo-a") || !strings.Contains(string(dataB), "repo-b") {
		t.Fatalf("cross-repo isolation broken: repo-a=%q repo-b=%q", string(dataA), string(dataB))
	}

	// Detach HEAD in repo-a worktree -> worktreeHealthy must detect mismatch
	// and Prepare must repair it back onto refs/heads/work/task-shared!
	runSeedGit(t, coA.WorktreePath, "checkout", "--detach", "HEAD")
	coARepaired, err := mgr.Prepare(ctx, "repo-a", "task-shared", "refs/heads/main")
	if err != nil {
		t.Fatalf("Prepare repair: %v", err)
	}
	headRef := runSeedGit(t, coARepaired.WorktreePath, "symbolic-ref", "HEAD")
	if headRef != "refs/heads/work/task-shared" {
		t.Fatalf("repaired HEAD = %q; want refs/heads/work/task-shared", headRef)
	}

	// Concurrent CommitAndPush (task-shared) and Prepare (task-other-i) on the
	// same repository must serialize cleanly via mirrorLock under -race.
	if err := os.WriteFile(filepath.Join(coARepaired.WorktreePath, "concurrent.txt"), []byte("v1\n"), 0o644); err != nil {
		t.Fatalf("write concurrent.txt: %v", err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 6)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := mgr.CommitAndPush(ctx, coARepaired, "concurrent push"); err != nil {
			errCh <- fmt.Errorf("CommitAndPush: %w", err)
		}
	}()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		taskName := fmt.Sprintf("task-concurrent-%d", i)
		go func() {
			defer wg.Done()
			if _, err := mgr.Prepare(ctx, "repo-a", taskName, "refs/heads/main"); err != nil {
				errCh <- fmt.Errorf("Prepare %s: %w", taskName, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestGitOpenCodeWorktreeManagerCAFileValidationAndOriginScope(t *testing.T) {
	t.Run("rejects control characters in CAFile", func(t *testing.T) {
		for name, bad := range map[string]string{
			"newline":         "/etc/ssl/ca.pem\nhttp.proxy=http://evil",
			"carriage return": "/etc/ssl/ca.pem\r",
			"nul":             "/etc/ssl/ca\x00.pem",
			"del":             "/etc/ssl/ca\x7f.pem",
		} {
			_, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
				RemoteBaseURL:  "https://git.example.test:8443",
				StateDirectory: t.TempDir(),
				CAFile:         bad,
				Executor:       &realTestGitExecutor{},
			})
			if err == nil {
				t.Fatalf("NewGitOpenCodeWorktreeManager accepted CAFile with %s", name)
			}
		}
	})

	t.Run("absolutizes relative CAFile and scopes sslCAInfo to HTTPS origin", func(t *testing.T) {
		credFile := filepath.Join(t.TempDir(), "cred")
		if err := os.WriteFile(credFile, []byte("kodsvc.tok\n"), 0o600); err != nil {
			t.Fatalf("WriteFile cred: %v", err)
		}
		relCA := filepath.Join("certs", "private-ca.pem")
		wantAbsCA, err := filepath.Abs(relCA)
		if err != nil {
			t.Fatalf("filepath.Abs: %v", err)
		}
		mgr, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
			RemoteBaseURL:  "https://git.example.test:8443",
			StateDirectory: t.TempDir(),
			CredentialFile: credFile,
			CAFile:         "  " + relCA + "  ",
			Executor:       &realTestGitExecutor{},
		})
		if err != nil {
			t.Fatalf("NewGitOpenCodeWorktreeManager: %v", err)
		}
		env, _, err := mgr.remoteEnvironment("https://git.example.test:8443/team/app")
		if err != nil {
			t.Fatalf("remoteEnvironment(https): %v", err)
		}
		kv := make(map[string]string, len(env))
		for _, entry := range env {
			k, v, ok := strings.Cut(entry, "=")
			if ok {
				kv[k] = v
			}
		}
		if kv["GIT_CONFIG_KEY_0"] != "http.https://git.example.test:8443.sslCAInfo" {
			t.Fatalf("GIT_CONFIG_KEY_0 = %q; want origin-scoped sslCAInfo", kv["GIT_CONFIG_KEY_0"])
		}
		if kv["GIT_CONFIG_VALUE_0"] != wantAbsCA {
			t.Fatalf("GIT_CONFIG_VALUE_0 = %q; want %q", kv["GIT_CONFIG_VALUE_0"], wantAbsCA)
		}
	})

	t.Run("plain HTTP remote omits sslCAInfo", func(t *testing.T) {
		credFile := filepath.Join(t.TempDir(), "cred")
		if err := os.WriteFile(credFile, []byte("kodsvc.tok\n"), 0o600); err != nil {
			t.Fatalf("WriteFile cred: %v", err)
		}
		mgr, err := NewGitOpenCodeWorktreeManager(GitOpenCodeWorktreeConfig{
			RemoteBaseURL:  "http://git.example.test:8080",
			StateDirectory: t.TempDir(),
			CredentialFile: credFile,
			CAFile:         filepath.Join(t.TempDir(), "ca.pem"),
			Executor:       &realTestGitExecutor{},
		})
		if err != nil {
			t.Fatalf("NewGitOpenCodeWorktreeManager: %v", err)
		}
		env, _, err := mgr.remoteEnvironment("http://git.example.test:8080/team/app")
		if err != nil {
			t.Fatalf("remoteEnvironment(http): %v", err)
		}
		for _, entry := range env {
			if strings.Contains(entry, "sslCAInfo") {
				t.Fatalf("plain HTTP remote unexpectedly included sslCAInfo: %q", entry)
			}
		}
	})
}
