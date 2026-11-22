package merge_test

// The concrete Git merge effects.
//
// The effects fetch the exact base and candidate, choose a fast-forward or a
// rebase, push the result to the assignment ref with the expected base as the
// lease, and observe only what the remote repository reports. The executor is
// injected: these unit tests script the native Git exchanges, while the real
// services-binary test runs the same effects against a scratch repository.

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/optimiware/kodema/internal/apps/merge"
)

var (
	gitTestBase      = strings.Repeat("1", 40)
	gitTestCandidate = strings.Repeat("2", 40)
	gitTestRebased   = strings.Repeat("3", 40)
)

// scriptedGitExecutor is an injected native Git boundary. The handler decides
// each exchange and records the exact command for the assertions.
type scriptedGitExecutor struct {
	mu     sync.Mutex
	calls  []merge.GitCommand
	handle func(merge.GitCommand) (merge.GitCommandResult, error)
	remote string
}

func (executor *scriptedGitExecutor) Run(_ context.Context, command merge.GitCommand) (merge.GitCommandResult, error) {
	executor.mu.Lock()
	executor.calls = append(executor.calls, command)
	handler := executor.handle
	executor.mu.Unlock()
	if handler != nil {
		return handler(command)
	}
	return merge.GitCommandResult{}, nil
}

func (executor *scriptedGitExecutor) called(prefix ...string) bool {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	for _, call := range executor.calls {
		if len(call.Args) < len(prefix) {
			continue
		}
		matches := true
		for index, value := range prefix {
			if call.Args[index] != value {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func okGit() (merge.GitCommandResult, error) {
	return merge.GitCommandResult{ExitCode: 0}, nil
}

func gitOutput(value string) (merge.GitCommandResult, error) {
	return merge.GitCommandResult{ExitCode: 0, Stdout: []byte(value + "\n")}, nil
}

// newScriptedMergeEffects wires one effects instance over the scripted
// executor and a scratch state directory.
func newScriptedMergeEffects(t *testing.T, executor merge.GitCommandExecutor) *merge.GitMergeEffects {
	t.Helper()
	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "file:///srv/git",
		StateDirectory: t.TempDir(),
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}
	return effects
}

func gitTestOperation() merge.MergeOperation {
	return merge.MergeOperation{
		Key:         merge.MergeKey{Repository: "shop/web", Ref: "refs/heads/main"},
		OperationID: "merge-op-1",
		Base:        gitTestBase,
		Candidate:   gitTestCandidate,
	}
}

// TestGitMergeEffectsFastForwardPushesExpectedBase proves the fast-forward path:
// the fetched candidate is the result, the push leases the exact expected base,
// and the applied observation reports only the fetched revisions and result.
func TestGitMergeEffectsFastForwardPushesExpectedBase(t *testing.T) {
	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = func(command merge.GitCommand) (merge.GitCommandResult, error) {
		switch command.Args[0] {
		case "init", "remote", "fetch", "checkout":
			return okGit()
		case "rev-parse":
			switch {
			case strings.HasPrefix(command.Args[len(command.Args)-1], gitTestBase):
				return gitOutput(gitTestBase)
			case strings.HasPrefix(command.Args[len(command.Args)-1], gitTestCandidate):
				return gitOutput(gitTestCandidate)
			}
		case "merge-base":
			if command.Args[2] == gitTestCandidate && command.Args[3] == gitTestBase {
				return merge.GitCommandResult{ExitCode: 1}, nil
			}
			return okGit()
		case "push":
			executor.mu.Lock()
			executor.remote = gitTestCandidate
			executor.mu.Unlock()
			return okGit()
		case "ls-remote":
			executor.mu.Lock()
			remote := executor.remote
			executor.mu.Unlock()
			return gitOutput(remote + "\trefs/heads/main")
		}
		t.Fatalf("unexpected git command %+v", command.Args)
		return merge.GitCommandResult{}, nil
	}

	effects := newScriptedMergeEffects(t, executor)
	observation, err := effects.Execute(context.Background(), gitTestOperation())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if observation.State != merge.EffectApplied {
		t.Fatalf("observation state = %q; want applied", observation.State)
	}
	if observation.Base != gitTestBase || observation.Candidate != gitTestCandidate || observation.Output != gitTestCandidate {
		t.Fatalf("observation = %+v; want the observed fast-forward result", observation)
	}
	if !executor.called("push", "--porcelain", "--force-with-lease=refs/heads/main:"+gitTestBase) {
		t.Fatalf("git calls = %+v; want a push leased on the expected base", executor.calls)
	}
	if executor.called("rebase") {
		t.Fatal("a fast-forward candidate was rebased")
	}

	observed, err := effects.Observe(context.Background(), gitTestOperation())
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if observed.State != merge.EffectApplied || observed.Output != gitTestCandidate {
		t.Fatalf("observed = %+v; want the applied fast-forward result", observed)
	}
}

// TestGitMergeEffectsRebasesDivergentCandidate proves the rebase path and that
// the observed output is the rebased commit, not the original candidate.
func TestGitMergeEffectsRebasesDivergentCandidate(t *testing.T) {
	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = func(command merge.GitCommand) (merge.GitCommandResult, error) {
		switch command.Args[0] {
		case "init", "remote", "fetch":
			return okGit()
		case "checkout", "rebase":
			return okGit()
		case "rev-parse":
			switch command.Args[len(command.Args)-1] {
			case "HEAD":
				return gitOutput(gitTestRebased)
			case gitTestBase + "^{commit}":
				return gitOutput(gitTestBase)
			case gitTestCandidate + "^{commit}":
				return gitOutput(gitTestCandidate)
			}
		case "merge-base":
			return merge.GitCommandResult{ExitCode: 1}, nil
		case "push":
			executor.mu.Lock()
			executor.remote = gitTestRebased
			executor.mu.Unlock()
			return okGit()
		case "ls-remote":
			executor.mu.Lock()
			remote := executor.remote
			executor.mu.Unlock()
			return gitOutput(remote + "\trefs/heads/main")
		}
		t.Fatalf("unexpected git command %+v", command.Args)
		return merge.GitCommandResult{}, nil
	}

	effects := newScriptedMergeEffects(t, executor)
	observation, err := effects.Execute(context.Background(), gitTestOperation())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if observation.State != merge.EffectApplied || observation.Output != gitTestRebased {
		t.Fatalf("observation = %+v; want the applied rebased result", observation)
	}
	if !executor.called("rebase", gitTestBase) {
		t.Fatalf("git calls = %+v; want the candidate rebased onto the base", executor.calls)
	}
	if !executor.called("push", "--porcelain", "--force-with-lease=refs/heads/main:"+gitTestBase) {
		t.Fatalf("git calls = %+v; want the rebased result pushed with the expected base", executor.calls)
	}
}

// TestGitMergeEffectsLocalFailureIsNotStarted proves a failure before the push
// mutates nothing and is reported as not_started.
func TestGitMergeEffectsLocalFailureIsNotStarted(t *testing.T) {
	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = func(command merge.GitCommand) (merge.GitCommandResult, error) {
		switch command.Args[0] {
		case "init", "remote", "fetch":
			return okGit()
		case "rev-parse":
			if strings.HasPrefix(command.Args[len(command.Args)-1], gitTestCandidate) {
				return merge.GitCommandResult{ExitCode: 128, Stderr: []byte("unknown revision")}, nil
			}
			return gitOutput(gitTestBase)
		}
		return okGit()
	}

	effects := newScriptedMergeEffects(t, executor)
	observation, err := effects.Execute(context.Background(), gitTestOperation())
	if err == nil {
		t.Fatal("Execute reported success for an unfetchable candidate")
	}
	if observation.State != merge.EffectNotStarted {
		t.Fatalf("observation = %+v; want not_started", observation)
	}
	if executor.called("push") {
		t.Fatal("a local failure still pushed")
	}
}

// TestGitMergeEffectsLostPushResponseIsObserved proves a lost push response is
// classified from the remote ref, never repeated and never guessed.
func TestGitMergeEffectsLostPushResponseIsObserved(t *testing.T) {
	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = func(command merge.GitCommand) (merge.GitCommandResult, error) {
		switch command.Args[0] {
		case "init", "remote", "fetch", "checkout":
			return okGit()
		case "rev-parse":
			if strings.HasPrefix(command.Args[len(command.Args)-1], gitTestCandidate) {
				return gitOutput(gitTestCandidate)
			}
			return gitOutput(gitTestBase)
		case "merge-base":
			if command.Args[2] == gitTestCandidate {
				return merge.GitCommandResult{ExitCode: 1}, nil
			}
			return okGit()
		case "push":
			// The remote applied the push but the local process saw an error.
			executor.mu.Lock()
			executor.remote = gitTestCandidate
			executor.mu.Unlock()
			return merge.GitCommandResult{ExitCode: 128, Stderr: []byte("connection reset")}, errors.New("connection reset")
		case "ls-remote":
			executor.mu.Lock()
			remote := executor.remote
			executor.mu.Unlock()
			return gitOutput(remote + "\trefs/heads/main")
		}
		t.Fatalf("unexpected git command %+v", command.Args)
		return merge.GitCommandResult{}, nil
	}

	effects := newScriptedMergeEffects(t, executor)
	observation, err := effects.Execute(context.Background(), gitTestOperation())
	if err != nil {
		t.Fatalf("Execute: %v; want the observed applied result", err)
	}
	if observation.State != merge.EffectApplied || observation.Output != gitTestCandidate {
		t.Fatalf("observation = %+v; want applied", observation)
	}
}

// TestNewGitMergeEffectsRefusesURLCredentials proves an embedded URL userinfo
// is refused at construction without echoing the credential anywhere.
func TestNewGitMergeEffectsRefusesURLCredentials(t *testing.T) {
	for _, remote := range []string{
		"https://user:secret@example.test/git",
		"https://token@example.test/git",
	} {
		_, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
			RemoteBaseURL:  remote,
			StateDirectory: t.TempDir(),
			Executor:       &scriptedGitExecutor{},
		})
		if err == nil {
			t.Fatalf("NewGitMergeEffects accepted %q", remote)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "token") || strings.Contains(err.Error(), remote) {
			t.Fatalf("error %q echoes the URL credential", err)
		}
	}
}

// TestNewGitMergeEffectsRejectsControlCharsInCAFile proves CAFile refuses
// ASCII control characters that could corrupt GIT_CONFIG_* environment entries.
func TestNewGitMergeEffectsRejectsControlCharsInCAFile(t *testing.T) {
	for name, bad := range map[string]string{
		"newline":         "/etc/ssl/ca\n.pem",
		"carriage return": "/etc/ssl/ca\r.pem",
		"null byte":       "/etc/ssl/ca\x00.pem",
		"escape":          "/etc/ssl/ca\x1b.pem",
		"del":             "/etc/ssl/ca\x7f.pem",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
				RemoteBaseURL:  "https://git.example.test/git",
				StateDirectory: t.TempDir(),
				CAFile:         bad,
				Executor:       &scriptedGitExecutor{},
			})
			if err == nil {
				t.Fatalf("NewGitMergeEffects accepted CAFile with %s", name)
			}
		})
	}
}

// TestNewGitMergeEffectsRejectsHostlessHTTPRemoteBaseURL proves HTTP(S) remote
// base URLs without a host are refused at construction.
func TestNewGitMergeEffectsRejectsHostlessHTTPRemoteBaseURL(t *testing.T) {
	for _, remote := range []string{
		"https:///git",
		"http:///git",
	} {
		t.Run(remote, func(t *testing.T) {
			_, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
				RemoteBaseURL:  remote,
				StateDirectory: t.TempDir(),
				Executor:       &scriptedGitExecutor{},
			})
			if err == nil {
				t.Fatalf("NewGitMergeEffects accepted hostless remote %q", remote)
			}
		})
	}
}

func scriptedFastForwardHandler(executor *scriptedGitExecutor, t *testing.T) func(merge.GitCommand) (merge.GitCommandResult, error) {
	t.Helper()
	return func(command merge.GitCommand) (merge.GitCommandResult, error) {
		switch command.Args[0] {
		case "init", "remote", "fetch", "checkout":
			return okGit()
		case "rev-parse":
			switch {
			case strings.HasPrefix(command.Args[len(command.Args)-1], gitTestBase):
				return gitOutput(gitTestBase)
			case strings.HasPrefix(command.Args[len(command.Args)-1], gitTestCandidate):
				return gitOutput(gitTestCandidate)
			}
		case "merge-base":
			if command.Args[2] == gitTestCandidate && command.Args[3] == gitTestBase {
				return merge.GitCommandResult{ExitCode: 1}, nil
			}
			return okGit()
		case "push":
			executor.mu.Lock()
			executor.remote = gitTestCandidate
			executor.mu.Unlock()
			return okGit()
		case "ls-remote":
			executor.mu.Lock()
			remote := executor.remote
			executor.mu.Unlock()
			return gitOutput(remote + "\trefs/heads/main")
		}
		t.Fatalf("unexpected git command %+v", command.Args)
		return merge.GitCommandResult{}, nil
	}
}

// TestGitMergeEffectsRemoteEnvironmentWithCAFile proves CAFile is normalized
// to an absolute path and emitted as origin-scoped http.<origin>.sslCAInfo
// alongside the scoped Authorization extraHeader in Execute and Observe.
func TestGitMergeEffectsRemoteEnvironmentWithCAFile(t *testing.T) {
	workRoot := t.TempDir()
	t.Chdir(workRoot)

	relCAFile := filepath.Join("certs", "ca.pem")
	absCAFile, err := filepath.Abs(relCAFile)
	if err != nil {
		t.Fatalf("filepath.Abs: %v", err)
	}
	credFile := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatalf("WriteFile credential: %v", err)
	}

	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = scriptedFastForwardHandler(executor, t)

	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "https://git.example.test:8443/git",
		StateDirectory: t.TempDir(),
		CredentialFile: credFile,
		CAFile:         "  " + relCAFile + "  ",
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}

	op := gitTestOperation()
	if _, err := effects.Execute(context.Background(), op); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	executeCallCount := len(executor.calls)

	if _, err := effects.Observe(context.Background(), op); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	wantBasic := base64.StdEncoding.EncodeToString([]byte("kodema:secret-token"))
	wantEnv := []string{
		"GIT_CONFIG_COUNT=2",
		"GIT_CONFIG_KEY_0=http.https://git.example.test:8443.sslCAInfo",
		"GIT_CONFIG_VALUE_0=" + absCAFile,
		"GIT_CONFIG_KEY_1=http.https://git.example.test:8443/git/shop/web.extraHeader",
		"GIT_CONFIG_VALUE_1=Authorization: Basic " + wantBasic,
	}

	seenExecuteRemote := 0
	seenObserveRemote := 0
	for index, call := range executor.calls {
		switch call.Args[0] {
		case "fetch", "ls-remote", "push":
			if !slices.Equal(call.Env, wantEnv) {
				t.Fatalf("call %d (%s) env = %v; want %v", index, call.Args[0], call.Env, wantEnv)
			}
			if index < executeCallCount {
				seenExecuteRemote++
			} else {
				seenObserveRemote++
			}
		default:
			if len(call.Env) != 0 {
				t.Fatalf("local call %d (%s) env = %v; want empty", index, call.Args[0], call.Env)
			}
		}
	}
	if seenExecuteRemote < 3 {
		t.Fatalf("seenExecuteRemote = %d; want fetch, ls-remote, and push", seenExecuteRemote)
	}
	if seenObserveRemote != 1 {
		t.Fatalf("seenObserveRemote = %d; want 1 ls-remote", seenObserveRemote)
	}
}

// TestGitMergeEffectsRemoteEnvironmentWithoutCAFile proves that leaving CAFile
// empty emits only the Authorization extraHeader with GIT_CONFIG_COUNT=1.
func TestGitMergeEffectsRemoteEnvironmentWithoutCAFile(t *testing.T) {
	credFile := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatalf("WriteFile credential: %v", err)
	}

	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = scriptedFastForwardHandler(executor, t)

	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "https://git.example.test:8443/git",
		StateDirectory: t.TempDir(),
		CredentialFile: credFile,
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}

	op := gitTestOperation()
	if _, err := effects.Execute(context.Background(), op); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := effects.Observe(context.Background(), op); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	wantBasic := base64.StdEncoding.EncodeToString([]byte("kodema:secret-token"))
	wantEnv := []string{
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=http.https://git.example.test:8443/git/shop/web.extraHeader",
		"GIT_CONFIG_VALUE_0=Authorization: Basic " + wantBasic,
	}

	remoteCalls := 0
	for index, call := range executor.calls {
		switch call.Args[0] {
		case "fetch", "ls-remote", "push":
			remoteCalls++
			if !slices.Equal(call.Env, wantEnv) {
				t.Fatalf("call %d (%s) env = %v; want %v", index, call.Args[0], call.Env, wantEnv)
			}
			for _, entry := range call.Env {
				if strings.Contains(entry, "sslCAInfo") {
					t.Fatalf("call %d (%s) unexpectedly contains sslCAInfo: %q", index, call.Args[0], entry)
				}
			}
		}
	}
	if remoteCalls < 4 {
		t.Fatalf("remoteCalls = %d; want at least 4 (Execute + Observe)", remoteCalls)
	}
}

// TestGitMergeEffectsRemoteEnvironmentPlainHTTPOmitsCAFile proves that a plain
// http:// remote omits sslCAInfo even when CAFile is configured, keeping
// GIT_CONFIG_COUNT=1 for the Authorization extraHeader.
func TestGitMergeEffectsRemoteEnvironmentPlainHTTPOmitsCAFile(t *testing.T) {
	credFile := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credFile, []byte("secret-token"), 0o600); err != nil {
		t.Fatalf("WriteFile credential: %v", err)
	}

	executor := &scriptedGitExecutor{remote: gitTestBase}
	executor.handle = scriptedFastForwardHandler(executor, t)

	effects, err := merge.NewGitMergeEffects(merge.GitMergeEffectsConfig{
		RemoteBaseURL:  "http://git.example.test:8080/git",
		StateDirectory: t.TempDir(),
		CredentialFile: credFile,
		CAFile:         filepath.Join(t.TempDir(), "ca.pem"),
		Executor:       executor,
	})
	if err != nil {
		t.Fatalf("NewGitMergeEffects: %v", err)
	}

	op := gitTestOperation()
	if _, err := effects.Execute(context.Background(), op); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := effects.Observe(context.Background(), op); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	wantBasic := base64.StdEncoding.EncodeToString([]byte("kodema:secret-token"))
	wantEnv := []string{
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=http.http://git.example.test:8080/git/shop/web.extraHeader",
		"GIT_CONFIG_VALUE_0=Authorization: Basic " + wantBasic,
	}

	remoteCalls := 0
	for index, call := range executor.calls {
		switch call.Args[0] {
		case "fetch", "ls-remote", "push":
			remoteCalls++
			if !slices.Equal(call.Env, wantEnv) {
				t.Fatalf("call %d (%s) env = %v; want %v", index, call.Args[0], call.Env, wantEnv)
			}
			for _, entry := range call.Env {
				if strings.Contains(entry, "sslCAInfo") {
					t.Fatalf("call %d (%s) unexpectedly contains sslCAInfo: %q", index, call.Args[0], entry)
				}
			}
		}
	}
	if remoteCalls < 4 {
		t.Fatalf("remoteCalls = %d; want at least 4 (Execute + Observe)", remoteCalls)
	}
}
