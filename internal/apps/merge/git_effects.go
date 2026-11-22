package merge

// The concrete Git merge/rebase boundary.
//
// Execute fetches the exact base and candidate from the configured remote, then
// fast-forwards when the candidate already descends from the base and rebases
// otherwise. It records the durable effect identity locally before pushing, so
// Observe can classify a lost push response from the remote ref instead of
// repeating the merge. The native Git process is injected: this package owns
// the merge policy, the composition entry owns the operating-system executor.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/id"
)

// ErrMergeBaseMoved reports that the remote target ref no longer points at the
// request base. Nothing was recorded and nothing was pushed.
var ErrMergeBaseMoved = errors.New("merge base moved on the remote")

// GitMergeEffectsConfig is the complete concrete Git effects configuration.
type GitMergeEffectsConfig struct {
	// RemoteBaseURL is the Git server/base URL. The repository of each
	// assignment is appended as one path segment.
	RemoteBaseURL string
	// StateDirectory keeps the durable per-operation effect record used by
	// Observe. It is required.
	StateDirectory string
	// WorkDirectory is where scratch clones live. A zero value uses
	// <StateDirectory>/work.
	WorkDirectory string
	// CredentialFile is the mounted service credential. It is read only for an
	// HTTP(S) remote, sent as an Authorization header to the configured Git
	// remote, and never placed in command arguments.
	CredentialFile string
	// CAFile optionally configures http.<origin>.sslCAInfo via GIT_CONFIG_*
	// for HTTPS Git remotes using a private CA bundle.
	CAFile string
	// Executor is the injected native Git boundary. It is required.
	Executor GitCommandExecutor
}

// GitMergeEffects is the concrete merge/rebase effect boundary.
type GitMergeEffects struct {
	remoteBaseURL string
	stateDir      string
	workDir       string
	credential    string
	caFile        string
	executor      GitCommandExecutor
}

var _ MergeEffects = (*GitMergeEffects)(nil)

// gitEffectRecord is the durable evidence that this operation computed one
// result and was about to push it. Absence of a record proves the operation
// never reached the remote push.
type gitEffectRecord struct {
	Base      string `json:"base"`
	Candidate string `json:"candidate"`
	Result    string `json:"result"`
}

// NewGitMergeEffects validates the configuration and creates its directories.
// It starts no process and opens no remote connection.
func NewGitMergeEffects(config GitMergeEffectsConfig) (*GitMergeEffects, error) {
	base := strings.TrimSpace(config.RemoteBaseURL)
	if base == "" {
		return nil, fmt.Errorf("git merge effects remote base URL is empty")
	}
	if strings.ContainsAny(base, "\x00\n\r") {
		return nil, fmt.Errorf("git merge effects remote base URL contains a control character")
	}
	// The mounted credential file is the only credential source. A URL that
	// embeds userinfo would leak into diagnostics, so it is refused without
	// ever echoing the URL.
	if parsed, err := url.Parse(base); err == nil {
		if parsed.User != nil {
			return nil, fmt.Errorf("git merge effects remote base URL must not embed credentials")
		}
		if (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host == "" {
			return nil, fmt.Errorf("git merge effects remote base URL with scheme %q requires a host", parsed.Scheme)
		}
	}
	if config.Executor == nil {
		return nil, fmt.Errorf("git merge effects executor is required")
	}
	stateDir := strings.TrimSpace(config.StateDirectory)
	if stateDir == "" {
		return nil, fmt.Errorf("git merge effects state directory is empty")
	}
	stateDir = filepath.Clean(stateDir)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create git merge effects state directory: %w", err)
	}
	workDir := strings.TrimSpace(config.WorkDirectory)
	if workDir == "" {
		workDir = filepath.Join(stateDir, "work")
	}
	workDir = filepath.Clean(workDir)
	if err := os.MkdirAll(workDir, 0o700); err != nil {
		return nil, fmt.Errorf("create git merge effects work directory: %w", err)
	}
	caFile, err := servicekit.NormalizeGitCAFile(config.CAFile, "git merge effects")
	if err != nil {
		return nil, err
	}
	return &GitMergeEffects{
		remoteBaseURL: strings.TrimRight(base, "/"),
		stateDir:      stateDir,
		workDir:       workDir,
		credential:    strings.TrimSpace(config.CredentialFile),
		caFile:        caFile,
		executor:      config.Executor,
	}, nil
}

// Execute performs at most one merge/rebase and push. A failure before the push
// is not_started because no remote mutation happened; a failed or lost push is
// classified by observing the remote ref, never by repeating the operation.
func (effects *GitMergeEffects) Execute(ctx context.Context, operation MergeOperation) (MergeObservation, error) {
	if err := effects.validateOperation(operation); err != nil {
		return MergeObservation{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	remoteURL := effects.repositoryURL(operation.Key.Repository)
	env, err := effects.remoteEnvironment(remoteURL)
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, err
	}
	workDir, err := os.MkdirTemp(effects.workDir, "merge-")
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, fmt.Errorf("create merge work tree: %w", err)
	}
	defer os.RemoveAll(workDir)

	if _, err := effects.require(ctx, workDir, nil, "init", "--quiet"); err != nil {
		return MergeObservation{State: EffectNotStarted}, err
	}
	if _, err := effects.require(ctx, workDir, nil, "remote", "add", "origin", remoteURL); err != nil {
		return MergeObservation{State: EffectNotStarted}, err
	}
	if _, err := effects.require(ctx, workDir, env, "fetch", "--no-tags", "--prune", "origin", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return MergeObservation{State: EffectNotStarted}, fmt.Errorf("fetch merge revisions: %w", err)
	}
	baseCommit, err := effects.resolveCommit(ctx, workDir, operation.Base)
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, fmt.Errorf("resolve merge base: %w", err)
	}
	// Refuse a stale request before computing or recording an effect. A
	// remote that already moved past the request base would only be rejected
	// by the push lease after the durable record was written, and that record
	// would then observe as uncertain forever. Nothing has started here.
	remoteCommit, err := effects.remoteRef(ctx, workDir, env, operation.Key.Ref)
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, fmt.Errorf("observe remote target: %w", err)
	}
	if remoteCommit != "" && remoteCommit != baseCommit {
		return MergeObservation{State: EffectNotStarted}, fmt.Errorf(
			"%w: %s is %s but the request base is %s",
			ErrMergeBaseMoved, operation.Key.Ref, remoteCommit, baseCommit)
	}
	candidateCommit, err := effects.resolveCommit(ctx, workDir, operation.Candidate)
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, fmt.Errorf("resolve merge candidate: %w", err)
	}
	candidateInBase, err := effects.isAncestor(ctx, workDir, candidateCommit, baseCommit)
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, err
	}
	baseInCandidate, err := effects.isAncestor(ctx, workDir, baseCommit, candidateCommit)
	if err != nil {
		return MergeObservation{State: EffectNotStarted}, err
	}
	resultCommit := ""
	switch {
	case candidateInBase:
		// The candidate is already contained in the base: there is nothing to
		// merge and the base is the settled result.
		resultCommit = baseCommit
	case baseInCandidate:
		// The base is an ancestor of the candidate: fast-forward.
		resultCommit = candidateCommit
	default:
		if _, err := effects.require(ctx, workDir, nil, "checkout", "--quiet", "--detach", candidateCommit); err != nil {
			return MergeObservation{State: EffectNotStarted}, err
		}
		if _, err := effects.require(ctx, workDir, nil, "rebase", baseCommit); err != nil {
			_, _ = effects.require(ctx, workDir, nil, "rebase", "--abort")
			return MergeObservation{State: EffectNotStarted}, fmt.Errorf("rebase merge candidate: %w", err)
		}
		rebased, err := effects.require(ctx, workDir, nil, "rev-parse", "HEAD")
		if err != nil {
			return MergeObservation{State: EffectNotStarted}, err
		}
		resultCommit = strings.TrimSpace(string(rebased.Stdout))
		if resultCommit == "" {
			return MergeObservation{State: EffectNotStarted}, fmt.Errorf("rebase merge produced no commit")
		}
	}

	record := gitEffectRecord{Base: baseCommit, Candidate: candidateCommit, Result: resultCommit}
	if err := effects.writeRecord(operation.OperationID, record); err != nil {
		return MergeObservation{State: EffectNotStarted}, err
	}
	expected := baseCommit
	if remoteCommit == "" {
		expected = ""
	}
	pushArgs := []string{"push", "--porcelain"}
	if operation.DecisionID != "" {
		pushArgs = append(pushArgs, "-o", "kodema.decision_id="+string(operation.DecisionID))
	}
	pushArgs = append(pushArgs,
		"--force-with-lease="+operation.Key.Ref+":"+expected,
		"origin", resultCommit+":"+operation.Key.Ref)
	pushResult, pushErr := effects.command(ctx, workDir, env, pushArgs...)
	pushFailure := pushErr
	if pushFailure == nil && pushResult.ExitCode != 0 {
		pushFailure = fmt.Errorf("git push exited %d: %s", pushResult.ExitCode, servicekit.CommandDetail(pushResult))
	}
	if pushFailure == nil {
		observation, observeErr := effects.observeRecord(ctx, workDir, env, operation.Key.Ref, record)
		if observeErr != nil {
			return MergeObservation{State: EffectUncertain}, observeErr
		}
		if observation.State == EffectApplied {
			return observation, nil
		}
		return MergeObservation{State: EffectUncertain}, fmt.Errorf("push did not settle the remote ref")
	}
	observation, observeErr := effects.observeRecord(ctx, workDir, env, operation.Key.Ref, record)
	if observation.State == EffectApplied {
		return observation, nil
	}
	if observation.State == EffectNotStarted {
		return observation, pushFailure
	}
	if observeErr != nil {
		return MergeObservation{State: EffectUncertain}, fmt.Errorf("%w: %v", pushFailure, observeErr)
	}
	return MergeObservation{State: EffectUncertain}, pushFailure
}

// Observe reports what the remote ref settled as without executing. It never
// fabricates: a missing operation record proves no push was attempted; with a
// record only the exact result or the exact base is conclusive.
func (effects *GitMergeEffects) Observe(ctx context.Context, operation MergeOperation) (MergeObservation, error) {
	if err := effects.validateOperation(operation); err != nil {
		return MergeObservation{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	record, ok, err := effects.readRecord(operation.OperationID)
	if err != nil {
		return MergeObservation{State: EffectUncertain}, err
	}
	if !ok {
		return MergeObservation{State: EffectNotStarted}, nil
	}
	remoteURL := effects.repositoryURL(operation.Key.Repository)
	env, err := effects.remoteEnvironment(remoteURL)
	if err != nil {
		return MergeObservation{State: EffectUncertain}, err
	}
	workDir, err := os.MkdirTemp(effects.workDir, "observe-")
	if err != nil {
		return MergeObservation{State: EffectUncertain}, fmt.Errorf("create observation work tree: %w", err)
	}
	defer os.RemoveAll(workDir)
	if _, err := effects.require(ctx, workDir, nil, "init", "--quiet"); err != nil {
		return MergeObservation{State: EffectUncertain}, err
	}
	if _, err := effects.require(ctx, workDir, nil, "remote", "add", "origin", remoteURL); err != nil {
		return MergeObservation{State: EffectUncertain}, err
	}
	return effects.observeRecord(ctx, workDir, env, operation.Key.Ref, record)
}

func (effects *GitMergeEffects) observeRecord(ctx context.Context, workDir string, env []string, ref string, record gitEffectRecord) (MergeObservation, error) {
	remoteCommit, err := effects.remoteRef(ctx, workDir, env, ref)
	if err != nil {
		return MergeObservation{State: EffectUncertain}, err
	}
	switch remoteCommit {
	case record.Result:
		return MergeObservation{
			State:     EffectApplied,
			Base:      record.Base,
			Candidate: record.Candidate,
			Output:    record.Result,
		}, nil
	case "", record.Base:
		return MergeObservation{State: EffectNotStarted}, nil
	default:
		return MergeObservation{State: EffectUncertain}, nil
	}
}

func (effects *GitMergeEffects) validateOperation(operation MergeOperation) error {
	if effects == nil || effects.executor == nil {
		return fmt.Errorf("git merge effects are not configured")
	}
	if err := operation.Key.Validate(); err != nil {
		return err
	}
	if _, err := id.ParseOperationID(string(operation.OperationID)); err != nil {
		return fmt.Errorf("merge operation identity %q is invalid: %w", operation.OperationID, err)
	}
	if strings.TrimSpace(operation.Base) == "" || strings.TrimSpace(operation.Candidate) == "" {
		return fmt.Errorf("merge operation needs a base and candidate")
	}
	if operation.DecisionID != "" {
		if _, err := id.ParseDecisionID(string(operation.DecisionID)); err != nil {
			return fmt.Errorf("merge operation decision identity %q is invalid: %w", operation.DecisionID, err)
		}
	}
	return nil
}

func (effects *GitMergeEffects) repositoryURL(repository string) string {
	return effects.remoteBaseURL + "/" + repository
}

func (effects *GitMergeEffects) remoteEnvironment(remoteURL string) ([]string, error) {
	env, _, err := servicekit.BuildGitRemoteEnv(remoteURL, effects.credential, "", effects.caFile)
	return env, err
}

func (effects *GitMergeEffects) resolveCommit(ctx context.Context, workDir, revision string) (string, error) {
	result, err := effects.require(ctx, workDir, nil, "rev-parse", "--verify", revision+"^{commit}")
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(result.Stdout))
	if commit == "" {
		return "", fmt.Errorf("revision %q resolved to no commit", revision)
	}
	return commit, nil
}

func (effects *GitMergeEffects) isAncestor(ctx context.Context, workDir, ancestor, descendant string) (bool, error) {
	result, err := effects.command(ctx, workDir, nil, "merge-base", "--is-ancestor", ancestor, descendant)
	if err != nil {
		return false, err
	}
	switch result.ExitCode {
	case 0:
		return true, nil
	case 1:
		return false, nil
	default:
		return false, fmt.Errorf("git merge-base exited %d: %s", result.ExitCode, servicekit.CommandDetail(result))
	}
}

func (effects *GitMergeEffects) remoteRef(ctx context.Context, workDir string, env []string, ref string) (string, error) {
	result, err := effects.require(ctx, workDir, env, "ls-remote", "--refs", "origin", ref)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(result.Stdout), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == ref {
			return fields[0], nil
		}
	}
	return "", nil
}

func (effects *GitMergeEffects) command(ctx context.Context, workDir string, env []string, args ...string) (GitCommandResult, error) {
	if len(args) == 0 {
		return GitCommandResult{}, fmt.Errorf("native git command has no arguments")
	}
	if err := ctx.Err(); err != nil {
		return GitCommandResult{}, err
	}
	result, err := effects.executor.Run(ctx, GitCommand{Directory: workDir, Args: args, Env: env})
	if err != nil {
		return result, fmt.Errorf("git %s: %w", args[0], err)
	}
	return result, nil
}

func (effects *GitMergeEffects) require(ctx context.Context, workDir string, env []string, args ...string) (GitCommandResult, error) {
	result, err := effects.command(ctx, workDir, env, args...)
	if err != nil {
		return result, err
	}
	if result.ExitCode != 0 {
		return result, fmt.Errorf("git %s exited %d: %s", args[0], result.ExitCode, servicekit.CommandDetail(result))
	}
	return result, nil
}

func (effects *GitMergeEffects) recordPath(operationID id.OperationID) string {
	return filepath.Join(effects.stateDir, string(operationID)+".json")
}

func (effects *GitMergeEffects) writeRecord(operationID id.OperationID, record gitEffectRecord) error {
	return servicekit.WriteAtomicJSON(effects.stateDir, ".merge-effect-*", effects.recordPath(operationID), record, servicekit.SyncDirectory)
}

func (effects *GitMergeEffects) readRecord(operationID id.OperationID) (gitEffectRecord, bool, error) {
	data, err := os.ReadFile(effects.recordPath(operationID))
	if errors.Is(err, os.ErrNotExist) {
		return gitEffectRecord{}, false, nil
	}
	if err != nil {
		return gitEffectRecord{}, false, fmt.Errorf("read merge effect record: %w", err)
	}
	var record gitEffectRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return gitEffectRecord{}, false, fmt.Errorf("decode merge effect record: %w", err)
	}
	if record.Base == "" || record.Candidate == "" || record.Result == "" {
		return gitEffectRecord{}, false, fmt.Errorf("merge effect record is incomplete")
	}
	return record, true, nil
}
