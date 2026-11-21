package services

import (
	"context"
	"encoding/base64"
	"fmt"
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
	// <StateDirectory>/repos/<safe-repo-slug>.git and per-task worktrees under
	// <StateDirectory>/worktrees/<safe-task-id>.
	StateDirectory string
	// CredentialFile is the mounted kodsvc. credential file used to
	// authenticate HTTP(S) Git operations via GIT_CONFIG_* environment headers
	// (never written into .git/config or URLs).
	CredentialFile string
	// CredentialHelper optionally configures a git credential helper command
	// (e.g. "kodema git credential") via GIT_CONFIG_* when CredentialFile is empty.
	CredentialHelper string
	// CAFile optionally configures http.sslCAInfo via GIT_CONFIG_* for HTTPS
	// Git servers using a private CA bundle.
	CAFile string
	// Executor runs native git commands without a shell and with process-group
	// cancellation so cancelling a turn immediately terminates active git
	// subprocesses in that worktree.
	Executor GitCommandExecutor
}

// GitOpenCodeWorktreeManager implements OpenCodeWorktreeManager over a local
// bare mirror per repository and an isolated `git worktree` per task on
// `refs/heads/work/<task-id>`.
type GitOpenCodeWorktreeManager struct {
	remoteBaseURL    string
	reposDir         string
	worktreesDir     string
	credentialFile   string
	credentialHelper string
	caFile           string
	executor         GitCommandExecutor

	repoMu sync.Map // repo slug -> *sync.Mutex
}

var _ OpenCodeWorktreeManager = (*GitOpenCodeWorktreeManager)(nil)

// NewGitOpenCodeWorktreeManager validates configuration and creates the local
// mirror and worktree parent directories.
func NewGitOpenCodeWorktreeManager(config GitOpenCodeWorktreeConfig) (*GitOpenCodeWorktreeManager, error) {
	base := strings.TrimSpace(config.RemoteBaseURL)
	if base == "" {
		return nil, fmt.Errorf("opencode git remote base URL is empty")
	}
	if strings.ContainsAny(base, "\x00\n\r") {
		return nil, fmt.Errorf("opencode git remote base URL contains a control character")
	}
	if parsed, err := url.Parse(base); err == nil && parsed.User != nil {
		return nil, fmt.Errorf("opencode git remote base URL must not embed credentials")
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
		caFile:           strings.TrimSpace(config.CAFile),
		executor:         config.Executor,
	}, nil
}

// Prepare ensures the project-scoped bare mirror is up to date and checks out
// (or resumes) an isolated per-task git worktree on refs/heads/work/<task-id>.
func (m *GitOpenCodeWorktreeManager) Prepare(ctx context.Context, repository, taskID, baseRef string) (WorktreeCheckout, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !validRepository(repository) {
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
	worktreePath := filepath.Join(m.worktreesDir, taskID)
	if !isContainedPath(m.reposDir, mirrorPath) || !isContainedPath(m.worktreesDir, worktreePath) {
		return WorktreeCheckout{}, fmt.Errorf("derived worktree or mirror path escapes state directory")
	}

	remoteURL := m.repositoryURL(repository)
	env, err := m.remoteEnvironment(remoteURL)
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

	if err := m.ensureMirror(ctx, mirrorPath, remoteURL, env); err != nil {
		return WorktreeCheckout{}, err
	}
	baseCommit, err := m.resolveMirrorRef(ctx, mirrorPath, baseRef)
	if err != nil {
		return WorktreeCheckout{}, fmt.Errorf("resolve base_ref %q: %w", baseRef, err)
	}

	// If worktree already exists and is healthy (for example a fix/revise turn
	// on the same task), resume it in place without clobbering prior work.
	if m.worktreeHealthy(ctx, worktreePath) {
		return WorktreeCheckout{
			Repository:   repository,
			TaskID:       taskID,
			Branch:       branchRef,
			WorktreePath: worktreePath,
			BaseCommit:   baseCommit,
		}, nil
	}

	// Prune any stale worktree metadata and clean partial directory before adding.
	_, _ = m.command(ctx, mirrorPath, nil, "worktree", "prune")
	if err := os.RemoveAll(worktreePath); err != nil {
		return WorktreeCheckout{}, fmt.Errorf("clean stale worktree directory: %w", err)
	}

	// If origin already has work/<task-id> (e.g. after service host rebuild),
	// resume from origin/work/<task-id>; otherwise branch from baseCommit.
	startCommit := baseCommit
	if existing, err := m.resolveMirrorRef(ctx, mirrorPath, "refs/remotes/origin/"+branchShort); err == nil && existing != "" {
		startCommit = existing
	}

	if _, err := m.require(ctx, mirrorPath, nil, "worktree", "add", "-B", branchShort, worktreePath, startCommit); err != nil {
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
// HEAD to refs/heads/work/<task-id> on origin.
func (m *GitOpenCodeWorktreeManager) CommitAndPush(ctx context.Context, checkout WorktreeCheckout, commitMessage string) (WorktreePushResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !validRepository(checkout.Repository) {
		return WorktreePushResult{}, fmt.Errorf("checkout repository %q is invalid", checkout.Repository)
	}
	if !validSafePathSlug(checkout.TaskID) {
		return WorktreePushResult{}, fmt.Errorf("checkout task id %q is invalid", checkout.TaskID)
	}
	expectedBranch := "refs/heads/work/" + checkout.TaskID
	if checkout.Branch != expectedBranch {
		return WorktreePushResult{}, fmt.Errorf("checkout branch %q does not match %q", checkout.Branch, expectedBranch)
	}
	if !isContainedPath(m.worktreesDir, checkout.WorktreePath) {
		return WorktreePushResult{}, fmt.Errorf("checkout worktree path escapes worktrees directory")
	}

	status, err := m.require(ctx, checkout.WorktreePath, nil, "status", "--porcelain")
	if err != nil {
		return WorktreePushResult{}, fmt.Errorf("inspect task worktree status: %w", err)
	}
	committed := false
	if strings.TrimSpace(string(status.Stdout)) != "" {
		if _, err := m.require(ctx, checkout.WorktreePath, nil, "add", "-A"); err != nil {
			return WorktreePushResult{}, fmt.Errorf("stage task worktree changes: %w", err)
		}
		msg := strings.TrimSpace(commitMessage)
		if msg == "" {
			msg = "opencode: apply changes for " + checkout.TaskID
		}
		commitEnv := []string{
			"GIT_AUTHOR_NAME=Kodema OpenCode Service",
			"GIT_AUTHOR_EMAIL=opencode@kodema.invalid",
			"GIT_COMMITTER_NAME=Kodema OpenCode Service",
			"GIT_COMMITTER_EMAIL=opencode@kodema.invalid",
		}
		if _, err := m.require(ctx, checkout.WorktreePath, commitEnv, "commit", "-m", msg); err != nil {
			return WorktreePushResult{}, fmt.Errorf("commit task worktree changes: %w", err)
		}
		committed = true
	}

	headResult, err := m.require(ctx, checkout.WorktreePath, nil, "rev-parse", "HEAD")
	if err != nil {
		return WorktreePushResult{}, fmt.Errorf("resolve task HEAD commit: %w", err)
	}
	candidate := strings.TrimSpace(string(headResult.Stdout))
	if candidate == "" {
		return WorktreePushResult{}, fmt.Errorf("task worktree HEAD resolved to empty commit")
	}

	remoteURL := m.repositoryURL(checkout.Repository)
	env, err := m.remoteEnvironment(remoteURL)
	if err != nil {
		return WorktreePushResult{}, err
	}
	if _, err := m.require(ctx, checkout.WorktreePath, env, "push", "--porcelain", "origin", "HEAD:"+expectedBranch); err != nil {
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
	if !validRepository(repository) || !validSafePathSlug(taskID) {
		return fmt.Errorf("invalid repository or task id")
	}
	repoSlug := safeRepoSlug(repository)
	mirrorPath := filepath.Join(m.reposDir, repoSlug+".git")
	worktreePath := filepath.Join(m.worktreesDir, taskID)
	if !isContainedPath(m.reposDir, mirrorPath) || !isContainedPath(m.worktreesDir, worktreePath) {
		return fmt.Errorf("derived path escapes state directory")
	}
	mu := m.mirrorLock(repoSlug)
	mu.Lock()
	defer mu.Unlock()
	if _, err := os.Stat(mirrorPath); err == nil {
		_, _ = m.command(ctx, mirrorPath, nil, "worktree", "remove", "--force", worktreePath)
		_, _ = m.command(ctx, mirrorPath, nil, "worktree", "prune")
	}
	return os.RemoveAll(worktreePath)
}

func (m *GitOpenCodeWorktreeManager) ensureMirror(ctx context.Context, mirrorPath, remoteURL string, env []string) error {
	if _, err := os.Stat(filepath.Join(mirrorPath, "HEAD")); os.IsNotExist(err) {
		if err := os.MkdirAll(mirrorPath, 0o700); err != nil {
			return fmt.Errorf("create mirror directory: %w", err)
		}
		if _, err := m.require(ctx, mirrorPath, nil, "init", "--bare", "--quiet"); err != nil {
			return fmt.Errorf("init bare mirror: %w", err)
		}
		if _, err := m.require(ctx, mirrorPath, nil, "remote", "add", "origin", remoteURL); err != nil {
			return fmt.Errorf("add mirror origin remote: %w", err)
		}
	} else {
		_, _ = m.require(ctx, mirrorPath, nil, "remote", "set-url", "origin", remoteURL)
	}
	if _, err := m.require(ctx, mirrorPath, env, "fetch", "--no-tags", "--prune", "origin", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
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
		res, err := m.command(ctx, mirrorPath, nil, "rev-parse", "--verify", candidate+"^{commit}")
		if err == nil && res.ExitCode == 0 {
			sha := strings.TrimSpace(string(res.Stdout))
			if sha != "" {
				return sha, nil
			}
		}
	}
	return "", fmt.Errorf("could not resolve ref %q in mirror", ref)
}

func (m *GitOpenCodeWorktreeManager) worktreeHealthy(ctx context.Context, worktreePath string) bool {
	if _, err := os.Stat(filepath.Join(worktreePath, ".git")); err != nil {
		return false
	}
	res, err := m.command(ctx, worktreePath, nil, "rev-parse", "--is-inside-work-tree")
	return err == nil && res.ExitCode == 0 && strings.TrimSpace(string(res.Stdout)) == "true"
}

func (m *GitOpenCodeWorktreeManager) mirrorLock(slug string) *sync.Mutex {
	val, _ := m.repoMu.LoadOrStore(slug, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func (m *GitOpenCodeWorktreeManager) repositoryURL(repository string) string {
	return m.remoteBaseURL + "/" + repository
}

// remoteEnvironment injects HTTP(S) credentials and optional CA bundle purely
// via GIT_CONFIG_* environment variables, never writing secrets to .git/config
// or command arguments.
func (m *GitOpenCodeWorktreeManager) remoteEnvironment(remoteURL string) ([]string, error) {
	parsed, err := url.Parse(remoteURL)
	if err != nil {
		return nil, fmt.Errorf("parse opencode git remote URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, nil
	}
	type cfgPair struct{ k, v string }
	var pairs []cfgPair
	if m.caFile != "" {
		pairs = append(pairs, cfgPair{k: "http.sslCAInfo", v: m.caFile})
	}
	switch {
	case m.credentialFile != "":
		data, err := os.ReadFile(m.credentialFile)
		if err != nil {
			return nil, fmt.Errorf("read opencode git credential: %w", err)
		}
		tok := strings.TrimSpace(string(data))
		if tok == "" {
			return nil, fmt.Errorf("opencode git credential file is empty")
		}
		basic := base64.StdEncoding.EncodeToString([]byte("kodema:" + tok))
		pairs = append(pairs, cfgPair{
			k: "http." + remoteURL + ".extraHeader",
			v: "Authorization: Basic " + basic,
		})
	case m.credentialHelper != "":
		pairs = append(pairs, cfgPair{
			k: "credential.helper",
			v: m.credentialHelper,
		})
	default:
		return nil, fmt.Errorf("an HTTP(S) git remote requires a credential file or credential helper")
	}
	env := make([]string, 0, 1+2*len(pairs))
	env = append(env, fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(pairs)))
	for i, pair := range pairs {
		env = append(env,
			fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, pair.k),
			fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, pair.v),
		)
	}
	return env, nil
}

func (m *GitOpenCodeWorktreeManager) command(ctx context.Context, dir string, env []string, args ...string) (GitCommandResult, error) {
	if len(args) == 0 {
		return GitCommandResult{}, fmt.Errorf("git command has no arguments")
	}
	if err := ctx.Err(); err != nil {
		return GitCommandResult{}, err
	}
	res, err := m.executor.Run(ctx, GitCommand{Directory: dir, Args: args, Env: env})
	if err != nil {
		return res, fmt.Errorf("git %s: %w", args[0], err)
	}
	return res, nil
}

func (m *GitOpenCodeWorktreeManager) require(ctx context.Context, dir string, env []string, args ...string) (GitCommandResult, error) {
	res, err := m.command(ctx, dir, env, args...)
	if err != nil {
		return res, err
	}
	if res.ExitCode != 0 {
		return res, fmt.Errorf("git %s exited %d: %s", args[0], res.ExitCode, commandDetail(res))
	}
	return res, nil
}

func safeRepoSlug(repository string) string {
	return strings.ReplaceAll(repository, "/", "__")
}

func isContainedPath(baseDir, candidate string) bool {
	rel, err := filepath.Rel(baseDir, candidate)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
