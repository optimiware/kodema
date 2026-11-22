package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/userland/git"
)

// ObserverConfig configures the native ref observer.
type ObserverConfig struct {
	TenantID id.TenantID
	Storage  git.Storage
	Executor git.CommandExecutor
}

// Observer implements git.RefObserver with `git for-each-ref`. A missing ref is
// reported as an empty commit, never as an error; an invalid repository name or
// a failed command is an error.
type Observer struct {
	tenant   id.TenantID
	storage  git.Storage
	executor git.CommandExecutor
}

var _ git.RefObserver = (*Observer)(nil)

// NewObserver validates the boundaries and returns the observer.
func NewObserver(config ObserverConfig) (*Observer, error) {
	if _, err := id.ParseTenantID(string(config.TenantID)); err != nil {
		return nil, fmt.Errorf("ref observer tenant: %w", err)
	}
	if config.Storage == nil {
		return nil, fmt.Errorf("ref observer storage is missing")
	}
	if config.Executor == nil {
		return nil, fmt.Errorf("ref observer executor is missing")
	}
	return &Observer{tenant: config.TenantID, storage: config.Storage, executor: config.Executor}, nil
}

// Observe returns the current commit of one repository ref.
func (observer *Observer) Observe(ctx context.Context, repository, ref string) (git.RefState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return git.RefState{}, err
	}
	path, err := observer.storage.RepositoryPath(observer.tenant, repository)
	if err != nil {
		return git.RefState{}, err
	}
	result, err := observer.executor.Run(ctx, git.Command{
		Name: "git",
		Args: []string{"-C", path, "for-each-ref", "--format=%(refname) %(objectname)", ref},
	})
	if err != nil {
		return git.RefState{}, err
	}
	if result.ExitCode != 0 {
		return git.RefState{}, fmt.Errorf("for-each-ref exited %d", result.ExitCode)
	}
	if hasRelevantForEachRefStderr(string(result.Stderr), ref) {
		return git.RefState{}, fmt.Errorf("for-each-ref reported stderr: %s", strings.TrimSpace(string(result.Stderr)))
	}
	var commit string
	for _, line := range strings.Split(strings.TrimSpace(string(result.Stdout)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return git.RefState{}, fmt.Errorf("for-each-ref reported a malformed ref line")
		}
		if fields[0] != ref {
			continue
		}
		if commit != "" {
			return git.RefState{}, fmt.Errorf("for-each-ref resolved %q more than once", ref)
		}
		commit = fields[1]
	}
	return git.RefState{Ref: ref, Commit: commit}, nil
}

// hasRelevantForEachRefStderr reports whether stderr from `git for-each-ref`
// contains anything other than broken-ref warnings for unrelated refs that
// happen to share a name prefix with ref (e.g. refs/heads/main-backup when
// observing refs/heads/main).
func hasRelevantForEachRefStderr(stderr, ref string) bool {
	for _, rawLine := range strings.Split(stderr, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if brokenRef, ok := parseBrokenRefWarning(line); ok && brokenRef != ref {
			continue
		}
		return true
	}
	return false
}

func parseBrokenRefWarning(line string) (string, bool) {
	for _, prefix := range []string{
		"warning: ignoring broken ref ",
		"warning: ignoring ref with broken name ",
	} {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			refName := strings.TrimSpace(rest)
			if refName != "" {
				return refName, true
			}
		}
	}
	return "", false
}
