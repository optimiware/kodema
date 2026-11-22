package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// GitOpenCodeWorktreeConfig configures the project-scoped local Git mirror and
// isolated per-task worktree manager.
type GitOpenCodeWorktreeConfig struct {
	// RemoteBaseURL is the Git server base URL. Each repository is appended as
	// one path segment (never embedding credentials in the URL).
	RemoteBaseURL string
	// StateDirectory is the service's local state directory. Mirrors live under
	// <StateDirectory>/repos/<repo-hash>.git and per-task worktrees under
	// <StateDirectory>/worktrees/<repo-hash>/<safe-task-id>.
	StateDirectory string
	// CredentialFile is the mounted kodsvc. credential file used to
	// authenticate HTTP(S) Git operations via GIT_CONFIG_* environment headers
	// (never written into .git/config or URLs).
	CredentialFile string
	// CredentialHelper optionally configures a git credential helper command
	// (e.g. "kodema git credential") scoped to the trusted remote origin when
	// CredentialFile is empty.
	CredentialHelper string
	// CAFile optionally configures http.<origin>.sslCAInfo via GIT_CONFIG_* for
	// HTTPS Git servers using a private CA bundle.
	CAFile string
	// Executor runs native git commands without a shell and with process-group
	// cancellation so cancelling a turn immediately terminates active git
	// subprocesses in that worktree.
	Executor servicekit.GitCommandExecutor
}

// GitOpenCodeWorktreeManager implements OpenCodeWorktreeManager over a local
// bare mirror per repository and an isolated `git worktree` per (repo, task)
// on `refs/heads/work/<task-id>`.
type GitOpenCodeWorktreeManager struct {
	remoteBaseURL    string
	reposDir         string
	worktreesDir     string
	credentialFile   string
	credentialHelper string
	caFile           string
	executor         servicekit.GitCommandExecutor

	repoMu sync.Map // repo hash -> *sync.Mutex
}

var _ OpenCodeWorktreeManager = (*GitOpenCodeWorktreeManager)(nil)

// NewGitOpenCodeWorktreeManager validates configuration and creates the local
// mirror and worktree parent directories.
func NewGitOpenCodeWorktreeManager(config GitOpenCodeWorktreeConfig) (*GitOpenCodeWorktreeManager, error) {
	base, err := validateOpenCodeRemoteBaseURL(config.RemoteBaseURL)
	if err != nil {
		return nil, err
	}
	if config.Executor == nil {
		return nil, fmt.Errorf("opencode git worktree manager requires a git executor")
	}
	stateDir := strings.TrimSpace(config.StateDirectory)
	if stateDir == "" {
		return nil, fmt.Errorf("opencode git worktree state directory is empty")
	}
	absState, err := filepath.Abs(filepath.Clean(stateDir))
	if err != nil {
		return nil, fmt.Errorf("resolve opencode state directory: %w", err)
	}
	caFile, err := servicekit.NormalizeGitCAFile(config.CAFile, "opencode git worktree")
	if err != nil {
		return nil, err
	}
	reposDir := filepath.Join(absState, "repos")
	worktreesDir := filepath.Join(absState, "worktrees")
	if err := os.MkdirAll(reposDir, 0o700); err != nil {
		return nil, fmt.Errorf("create opencode repos directory: %w", err)
	}
	if err := os.MkdirAll(worktreesDir, 0o700); err != nil {
		return nil, fmt.Errorf("create opencode worktrees directory: %w", err)
	}
	return &GitOpenCodeWorktreeManager{
		remoteBaseURL:    strings.TrimRight(base, "/"),
		reposDir:         reposDir,
		worktreesDir:     worktreesDir,
		credentialFile:   strings.TrimSpace(config.CredentialFile),
		credentialHelper: strings.TrimSpace(config.CredentialHelper),
		caFile:           caFile,
		executor:         config.Executor,
	}, nil
}

func validateOpenCodeRemoteBaseURL(raw string) (string, error) {
	base := strings.TrimSpace(raw)
	if base == "" {
		return "", fmt.Errorf("opencode git remote base URL is empty")
	}
	if strings.HasPrefix(base, "-") {
		return "", fmt.Errorf("opencode git remote base URL must not start with '-'")
	}
	for i := 0; i < len(base); i++ {
		if base[i] < 0x20 || base[i] == 0x7f {
			return "", fmt.Errorf("opencode git remote base URL contains a control character")
		}
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse opencode git remote base URL: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "ssh":
		if parsed.Host == "" {
			return "", fmt.Errorf("opencode git remote base URL with scheme %q requires a host", parsed.Scheme)
		}
	case "file":
		if parsed.Path == "" {
			return "", fmt.Errorf("opencode git remote base URL with scheme file requires a path")
		}
	default:
		return "", fmt.Errorf("opencode git remote base URL scheme %q is unsupported (allowed: http, https, file, ssh)", parsed.Scheme)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("opencode git remote base URL must not embed credentials")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("opencode git remote base URL must not contain a query or fragment")
	}
	return base, nil
}

// Prepare ensures the project-scoped bare mirror is up to date and checks out
// (or resumes) an isolated per-(repository, task) git worktree on
// refs/heads/work/<task-id>.
func (m *GitOpenCodeWorktreeManager) Prepare(ctx context.Context, repository, taskID, baseRef string) (WorktreeCheckout, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !servicekit.ValidRepository(repository) {
		return WorktreeCheckout{}, fmt.Errorf("repository %q is invalid", repository)
	}
	if !validSafePathSlug(taskID) {
		return WorktreeCheckout{}, fmt.Errorf("task id %q is invalid for worktree", taskID)
	}
	if baseRef == "" {
		baseRef = DefaultOpenCodeBaseRef
	}
	if !validOpenCodeRef(baseRef) {
		return WorktreeCheckout{}, fmt.Errorf("base_ref %q is invalid", baseRef)
	}

	repoSlug := safeRepoSlug(repository)
	mirrorPath := filepath.Join(m.reposDir, repoSlug+".git")
	repoWorktreesDir := filepath.Join(m.worktreesDir, repoSlug)
	worktreePath := filepath.Join(repoWorktreesDir, taskID)
	if !isContainedPath(m.reposDir, mirrorPath) ||
		!isContainedPath(m.worktreesDir, repoWorktreesDir) ||
		!isContainedPath(repoWorktreesDir, worktreePath) {
		return WorktreeCheckout{}, fmt.Errorf("derived worktree or mirror path escapes state directory")
	}

	remoteURL := m.repositoryURL(repository)
	env, remoteArgs, err := m.remoteEnvironment(remoteURL)
	if err != nil {
		return WorktreeCheckout{}, err
	}

	branchShort := "work/" + taskID
	branchRef := "refs/heads/" + branchShort

	// Serialize mirror fetch + worktree add/prune per repository so concurrent
	// tasks on the same repo never race mirror refs or worktree registration.
	mu := m.mirrorLock(repoSlug)
	mu.Lock()
	defer mu.Unlock()

	if err := m.ensureMirror(ctx, mirrorPath, remoteURL, env, remoteArgs); err != nil {
		return WorktreeCheckout{}, err
	}
	baseCommit, err := m.resolveMirrorRef(ctx, mirrorPath, baseRef)
	if err != nil {
		return WorktreeCheckout{}, fmt.Errorf("resolve base_ref %q: %w", baseRef, err)
	}

	// If worktree already exists and is healthy (for example a fix/revise turn
	// on the same task), resume it in place without clobbering prior work.
	if m.worktreeHealthy(ctx, worktreePath, mirrorPath, branchRef) {
		return WorktreeCheckout{
			Repository:   repository,
			TaskID:       taskID,
			Branch:       branchRef,
			WorktreePath: worktreePath,
			BaseCommit:   baseCommit,
		}, nil
	}

	// Remove any unhealthy/partial worktree directory FIRST, then prune stale
	// worktree registrations in the mirror so `git worktree add` never collides
	// with a previously registered entry.
	_, _ = m.command(ctx, mirrorPath, nil, nil, "worktree", "remove", "--force", worktreePath)
	if err := os.RemoveAll(worktreePath); err != nil {
		return WorktreeCheckout{}, fmt.Errorf("clean stale worktree directory: %w", err)
	}
	_, _ = m.command(ctx, mirrorPath, nil, nil, "worktree", "prune")
	if err := os.MkdirAll(repoWorktreesDir, 0o700); err != nil {
		return WorktreeCheckout{}, fmt.Errorf("create repository worktrees directory: %w", err)
	}

	// If origin already has work/<task-id> (e.g. after service host rebuild),
	// resume from origin/work/<task-id>; otherwise branch from baseCommit.
	startCommit := baseCommit
	if existing, err := m.resolveMirrorRef(ctx, mirrorPath, "refs/remotes/origin/"+branchShort); err == nil && existing != "" {
		startCommit = existing
	}

	if _, err := m.require(ctx, mirrorPath, nil, nil, "worktree", "add", "-B", branchShort, worktreePath, startCommit); err != nil {
		return WorktreeCheckout{}, fmt.Errorf("create task worktree: %w", err)
	}

	return WorktreeCheckout{
		Repository:   repository,
		TaskID:       taskID,
		Branch:       branchRef,
		WorktreePath: worktreePath,
		BaseCommit:   baseCommit,
	}, nil
}

// CommitAndPush commits any uncommitted changes in the task worktree and pushes
// HEAD directly to the validated explicit remote URL at refs/heads/work/<task-id>.
func (m *GitOpenCodeWorktreeManager) CommitAndPush(ctx context.Context, checkout WorktreeCheckout, commitMessage string) (WorktreePushResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !servicekit.ValidRepository(checkout.Repository) {
		return WorktreePushResult{}, fmt.Errorf("checkout repository %q is invalid", checkout.Repository)
	}
	if !validSafePathSlug(checkout.TaskID) {
		return WorktreePushResult{}, fmt.Errorf("checkout task id %q is invalid", checkout.TaskID)
	}
	expectedBranch := "refs/heads/work/" + checkout.TaskID
	if checkout.Branch != expectedBranch {
		return WorktreePushResult{}, fmt.Errorf("checkout branch %q does not match %q", checkout.Branch, expectedBranch)
	}
	repoSlug := safeRepoSlug(checkout.Repository)
	expectedWorktree := filepath.Join(m.worktreesDir, repoSlug, checkout.TaskID)
	if !isContainedPath(m.worktreesDir, checkout.WorktreePath) || filepath.Clean(checkout.WorktreePath) != expectedWorktree {
		return WorktreePushResult{}, fmt.Errorf("checkout worktree path escapes expected repository task worktree")
	}

	remoteURL := m.repositoryURL(checkout.Repository)
	env, remoteArgs, err := m.remoteEnvironment(remoteURL)
	if err != nil {
		return WorktreePushResult{}, err
	}

	// Serialize mirror ref updates and remote push against concurrent Prepare
	// calls on the same repository mirror.
	mu := m.mirrorLock(repoSlug)
	mu.Lock()
	defer mu.Unlock()

	status, err := m.require(ctx, checkout.WorktreePath, nil, nil, "status", "--porcelain")
	if err != nil {
		return WorktreePushResult{}, fmt.Errorf("inspect task worktree status: %w", err)
	}
	committed := false
	if strings.TrimSpace(string(status.Stdout)) != "" {
		if _, err := m.require(ctx, checkout.WorktreePath, nil, nil, "add", "-A"); err != nil {
			return WorktreePushResult{}, fmt.Errorf("stage task worktree changes: %w", err)
		}
		msg := sanitizeCommitMessage(commitMessage, "opencode: apply changes for "+checkout.TaskID)
		commitEnv := []string{
			"GIT_AUTHOR_NAME=Kodema OpenCode Service",
			"GIT_AUTHOR_EMAIL=opencode@kodema.invalid",
			"GIT_COMMITTER_NAME=Kodema OpenCode Service",
			"GIT_COMMITTER_EMAIL=opencode@kodema.invalid",
		}
		if _, err := m.require(ctx, checkout.WorktreePath, commitEnv, nil, "commit", "--no-verify", "-m", msg); err != nil {
			return WorktreePushResult{}, fmt.Errorf("commit task worktree changes: %w", err)
		}
		committed = true
	}

	headResult, err := m.require(ctx, checkout.WorktreePath, nil, nil, "rev-parse", "HEAD")
	if err != nil {
		return WorktreePushResult{}, fmt.Errorf("resolve task HEAD commit: %w", err)
	}
	candidate := strings.TrimSpace(string(headResult.Stdout))
	if candidate == "" {
		return WorktreePushResult{}, fmt.Errorf("task worktree HEAD resolved to empty commit")
	}

	if _, err := m.require(ctx, checkout.WorktreePath, env, remoteArgs, "push", "--porcelain", "--no-verify", "--", remoteURL, "HEAD:"+expectedBranch); err != nil {
		return WorktreePushResult{}, fmt.Errorf("push task branch %s: %w", expectedBranch, err)
	}

	return WorktreePushResult{
		Branch:    expectedBranch,
		Candidate: candidate,
		Committed: committed,
	}, nil
}

// Remove deletes an isolated task worktree cleanly.
func (m *GitOpenCodeWorktreeManager) Remove(ctx context.Context, repository, taskID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !servicekit.ValidRepository(repository) || !validSafePathSlug(taskID) {
		return fmt.Errorf("invalid repository or task id")
	}
	repoSlug := safeRepoSlug(repository)
	mirrorPath := filepath.Join(m.reposDir, repoSlug+".git")
	repoWorktreesDir := filepath.Join(m.worktreesDir, repoSlug)
	worktreePath := filepath.Join(repoWorktreesDir, taskID)
	if !isContainedPath(m.reposDir, mirrorPath) ||
		!isContainedPath(m.worktreesDir, repoWorktreesDir) ||
		!isContainedPath(repoWorktreesDir, worktreePath) {
		return fmt.Errorf("derived path escapes state directory")
	}
	mu := m.mirrorLock(repoSlug)
	mu.Lock()
	defer mu.Unlock()
	if _, err := os.Stat(mirrorPath); err == nil {
		_, _ = m.command(ctx, mirrorPath, nil, nil, "worktree", "remove", "--force", worktreePath)
		_, _ = m.command(ctx, mirrorPath, nil, nil, "worktree", "prune")
	}
	return os.RemoveAll(worktreePath)
}

func (m *GitOpenCodeWorktreeManager) ensureMirror(ctx context.Context, mirrorPath, remoteURL string, env, remoteArgs []string) error {
	if _, err := os.Stat(filepath.Join(mirrorPath, "HEAD")); os.IsNotExist(err) {
		if err := os.MkdirAll(mirrorPath, 0o700); err != nil {
			return fmt.Errorf("create mirror directory: %w", err)
		}
		if _, err := m.require(ctx, mirrorPath, nil, nil, "init", "--bare", "--quiet"); err != nil {
			return fmt.Errorf("init bare mirror: %w", err)
		}
		if _, err := m.require(ctx, mirrorPath, nil, nil, "remote", "add", "origin", remoteURL); err != nil {
			return fmt.Errorf("add mirror origin remote: %w", err)
		}
	} else {
		_, _ = m.require(ctx, mirrorPath, nil, nil, "remote", "set-url", "origin", remoteURL)
	}
	if _, err := m.require(ctx, mirrorPath, env, remoteArgs, "fetch", "--no-tags", "--prune", "--", remoteURL, "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return fmt.Errorf("fetch mirror origin: %w", err)
	}
	return nil
}

func (m *GitOpenCodeWorktreeManager) resolveMirrorRef(ctx context.Context, mirrorPath, ref string) (string, error) {
	candidates := []string{ref}
	if strings.HasPrefix(ref, "refs/heads/") {
		short := strings.TrimPrefix(ref, "refs/heads/")
		candidates = append([]string{"refs/remotes/origin/" + short}, candidates...)
	} else if !strings.HasPrefix(ref, "refs/") {
		candidates = append([]string{"refs/remotes/origin/" + ref}, candidates...)
	}
	for _, candidate := range candidates {
		res, err := m.command(ctx, mirrorPath, nil, nil, "rev-parse", "--verify", candidate+"^{commit}")
		if err == nil && res.ExitCode == 0 {
			sha := strings.TrimSpace(string(res.Stdout))
			if sha != "" {
				return sha, nil
			}
		}
	}
	return "", fmt.Errorf("could not resolve ref %q in mirror", ref)
}

func (m *GitOpenCodeWorktreeManager) worktreeHealthy(ctx context.Context, worktreePath, mirrorPath, expectedBranch string) bool {
	if _, err := os.Stat(filepath.Join(worktreePath, ".git")); err != nil {
		return false
	}
	res, err := m.command(ctx, worktreePath, nil, nil, "rev-parse", "--is-inside-work-tree")
	if err != nil || res.ExitCode != 0 || strings.TrimSpace(string(res.Stdout)) != "true" {
		return false
	}
	commonRes, err := m.command(ctx, worktreePath, nil, nil, "rev-parse", "--git-common-dir")
	if err != nil || commonRes.ExitCode != 0 {
		return false
	}
	commonDir := strings.TrimSpace(string(commonRes.Stdout))
	if commonDir == "" {
		return false
	}
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(worktreePath, commonDir)
	}
	if canonicalPath(commonDir) != canonicalPath(mirrorPath) {
		return false
	}
	headRes, err := m.command(ctx, worktreePath, nil, nil, "symbolic-ref", "--quiet", "HEAD")
	if err != nil || headRes.ExitCode != 0 {
		return false
	}
	return strings.TrimSpace(string(headRes.Stdout)) == expectedBranch
}

func canonicalPath(p string) string {
	cleaned := filepath.Clean(p)
	if abs, err := filepath.Abs(cleaned); err == nil {
		cleaned = abs
	}
	if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
		return filepath.Clean(resolved)
	}
	return cleaned
}

func (m *GitOpenCodeWorktreeManager) mirrorLock(slug string) *sync.Mutex {
	val, _ := m.repoMu.LoadOrStore(slug, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func (m *GitOpenCodeWorktreeManager) repositoryURL(repository string) string {
	return m.remoteBaseURL + "/" + repository
}

// remoteEnvironment injects HTTP(S) credentials and optional CA bundle purely
// via GIT_CONFIG_* environment variables (never writing secrets to .git/config
// or command arguments), and optionally returns origin-scoped credential helper
// `-c` overrides when CredentialHelper is used without CredentialFile.
func (m *GitOpenCodeWorktreeManager) remoteEnvironment(remoteURL string) ([]string, []string, error) {
	return servicekit.BuildGitRemoteEnv(remoteURL, m.credentialFile, m.credentialHelper, m.caFile)
}

// gitHardeningArgs returns explicit `-c` overrides prepended to EVERY native
// git invocation so attacker-controlled repository configs, hooks, fsmonitors,
// ssh commands, proxies, or credential helpers can never execute or exfiltrate
// credentials.
func gitHardeningArgs() []string {
	return []string{
		"-c", "core.hooksPath=" + os.DevNull,
		"-c", "core.fsmonitor=false",
		"-c", "core.sshCommand=/bin/false",
		"-c", "http.proxy=",
		"-c", "credential.helper=",
	}
}

func (m *GitOpenCodeWorktreeManager) command(ctx context.Context, dir string, env []string, extraConfigArgs []string, args ...string) (servicekit.GitCommandResult, error) {
	if len(args) == 0 {
		return servicekit.GitCommandResult{}, fmt.Errorf("git command has no arguments")
	}
	if err := ctx.Err(); err != nil {
		return servicekit.GitCommandResult{}, err
	}
	hardening := gitHardeningArgs()
	fullArgs := make([]string, 0, len(hardening)+len(extraConfigArgs)+len(args))
	fullArgs = append(fullArgs, hardening...)
	fullArgs = append(fullArgs, extraConfigArgs...)
	fullArgs = append(fullArgs, args...)
	res, err := m.executor.Run(ctx, servicekit.GitCommand{Directory: dir, Args: fullArgs, Env: env})
	if err != nil {
		return res, fmt.Errorf("git %s: %w", args[0], err)
	}
	return res, nil
}

func (m *GitOpenCodeWorktreeManager) require(ctx context.Context, dir string, env []string, extraConfigArgs []string, args ...string) (servicekit.GitCommandResult, error) {
	res, err := m.command(ctx, dir, env, extraConfigArgs, args...)
	if err != nil {
		return res, err
	}
	if res.ExitCode != 0 {
		return res, fmt.Errorf("git %s exited %d: %s", args[0], res.ExitCode, servicekit.CommandDetail(res))
	}
	return res, nil
}

func sanitizeCommitMessage(raw, fallback string) string {
	var b strings.Builder
	b.Grow(len(raw))
	lastSpace := true
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		if ch < 0x20 || ch == 0x7f {
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		if ch == ' ' {
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		b.WriteByte(ch)
		lastSpace = false
	}
	cleaned := strings.TrimSpace(b.String())
	if cleaned == "" {
		return fallback
	}
	return cleaned
}

// safeRepoSlug computes a collision-free filesystem-safe hash for repository
// names so repositories such as "a/b" and "a__b" never collide.
func safeRepoSlug(repository string) string {
	sum := sha256.Sum256([]byte(repository))
	return hex.EncodeToString(sum[:16])
}

func isContainedPath(baseDir, candidate string) bool {
	rel, err := filepath.Rel(baseDir, candidate)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
