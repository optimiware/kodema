package servicekit

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// DefaultGitOutputBytes bounds one native Git stream when the configuration
// leaves the bound unset.
const DefaultGitOutputBytes = 1 << 20

// gitCancelWaitDelay bounds how long Wait keeps reading the output pipes after
// the process group was killed on cancellation. A lingering git-remote-* child
// inherited from the killed Git process can otherwise hold the pipes open.
const gitCancelWaitDelay = 5 * time.Second

// GitCommand is one native Git invocation. Directory is the working tree and
// Args are passed verbatim; no shell ever sees them. Env carries explicitly
// composed child variables such as a scoped HTTP credential header.
type GitCommand struct {
	Directory string
	Args      []string
	Env       []string
}

// GitCommandResult is one native Git outcome. A non-zero ExitCode is a command
// result, not a transport failure.
type GitCommandResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
}

// GitCommandExecutor runs one native Git command without a shell.
type GitCommandExecutor interface {
	Run(context.Context, GitCommand) (GitCommandResult, error)
}

// OSGitExecutorConfig configures the native Git command executor.
type OSGitExecutorConfig struct {
	// GitPath is the native git binary. It defaults to "git".
	GitPath string
	// MaxOutputBytes bounds stdout and stderr independently.
	MaxOutputBytes int
}

// OSGitExecutor implements GitCommandExecutor over os/exec. It never runs a
// shell and never inherits ambient Git configuration or credentials: only an
// explicit minimal environment and the command's own variables reach the child.
type OSGitExecutor struct {
	path    string
	maxOut  int
	command func(context.Context, string, ...string) *exec.Cmd
}

var _ GitCommandExecutor = (*OSGitExecutor)(nil)

// NewOSGitExecutor validates the configuration and returns the executor.
func NewOSGitExecutor(config OSGitExecutorConfig) (*OSGitExecutor, error) {
	path := strings.TrimSpace(config.GitPath)
	if path == "" {
		path = "git"
	}
	if strings.ContainsAny(path, " \t\n;|&$`'\"<>") {
		return nil, fmt.Errorf("git path %q is not a plain executable path", path)
	}
	maxOut := config.MaxOutputBytes
	if maxOut <= 0 {
		maxOut = DefaultGitOutputBytes
	}
	return &OSGitExecutor{path: path, maxOut: maxOut, command: exec.CommandContext}, nil
}

// Run executes one native Git command. Arguments are passed verbatim. A
// non-zero exit is a command result; only a process or context failure is an
// error.
func (executor *OSGitExecutor) Run(ctx context.Context, command GitCommand) (GitCommandResult, error) {
	if executor == nil {
		return GitCommandResult{}, fmt.Errorf("git executor is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return GitCommandResult{}, err
	}
	args := make([]string, 0, len(command.Args)+2)
	if command.Directory != "" {
		args = append(args, "-C", command.Directory)
	}
	args = append(args, command.Args...)
	for _, argument := range args {
		if strings.ContainsAny(argument, "\x00\n\r") {
			return GitCommandResult{}, fmt.Errorf("native git argument contains a control character")
		}
	}

	child := executor.command(ctx, executor.path, args...)
	child.Env = append(gitBaseEnvironment(), command.Env...)
	child.Stdin = nil
	// Run Git in its own process group so cancellation kills the whole tree,
	// including a git-remote-* helper that would otherwise outlive the direct
	// child and keep the output pipes open. WaitDelay then bounds the wait for
	// those pipes after the kill.
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	child.Cancel = func() error {
		if child.Process == nil {
			return nil
		}
		if err := syscall.Kill(-child.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
	child.WaitDelay = gitCancelWaitDelay
	stdout := newBoundedBuffer(executor.maxOut)
	stderr := newBoundedBuffer(executor.maxOut)
	child.Stdout = stdout
	child.Stderr = stderr
	err := child.Run()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return GitCommandResult{}, ctxErr
	}
	if stdout.exceeded() || stderr.exceeded() {
		return GitCommandResult{}, fmt.Errorf("native git output exceeds the configured bound")
	}
	result := GitCommandResult{Stdout: stdout.bytes(), Stderr: stderr.bytes()}
	if err != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
			result.ExitCode = exitErr.ExitCode()
			return result, nil
		}
		return result, err
	}
	return result, nil
}

// NormalizeGitCAFile validates and absolutizes an optional CA bundle path so
// child Git invocations running inside scratch directories still resolve it.
func NormalizeGitCAFile(raw, label string) (string, error) {
	for i := 0; i < len(raw); i++ {
		if ch := raw[i]; ch < 0x20 || ch == 0x7f {
			return "", fmt.Errorf("%s CA file contains a control character", label)
		}
	}
	caFile := strings.TrimSpace(raw)
	if caFile == "" {
		return "", nil
	}
	absCAFile, err := filepath.Abs(filepath.Clean(caFile))
	if err != nil {
		return "", fmt.Errorf("resolve %s CA file: %w", label, err)
	}
	return absCAFile, nil
}

// BuildGitRemoteEnv injects HTTP(S) credentials and an optional origin-scoped
// CA bundle purely via GIT_CONFIG_* environment variables (never writing
// secrets to .git/config or command arguments), and optionally returns
// origin-scoped credential helper `-c` overrides when credentialHelper is used
// without credentialFile.
func BuildGitRemoteEnv(remoteURL, credentialFile, credentialHelper, caFile string) ([]string, []string, error) {
	parsed, err := url.Parse(remoteURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse git remote URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, nil, nil
	}
	type cfgPair struct {
		k, v string
	}
	pairs := make([]cfgPair, 0, 2)
	var extraArgs []string
	if caFile != "" && parsed.Scheme == "https" && parsed.Host != "" {
		origin := parsed.Scheme + "://" + parsed.Host
		pairs = append(pairs, cfgPair{
			k: "http." + origin + ".sslCAInfo",
			v: caFile,
		})
	}
	switch {
	case credentialFile != "":
		data, err := os.ReadFile(credentialFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read git credential: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return nil, nil, fmt.Errorf("git credential file is empty")
		}
		basic := base64.StdEncoding.EncodeToString([]byte("kodema:" + token))
		pairs = append(pairs, cfgPair{
			k: "http." + remoteURL + ".extraHeader",
			v: "Authorization: Basic " + basic,
		})
	case credentialHelper != "":
		origin := parsed.Scheme + "://" + parsed.Host
		extraArgs = append(extraArgs, "-c", "credential."+origin+".helper="+credentialHelper)
	default:
		return nil, nil, fmt.Errorf("an HTTP(S) git remote requires a credential file or credential helper")
	}
	if len(pairs) == 0 {
		return nil, extraArgs, nil
	}
	env := make([]string, 0, 1+2*len(pairs))
	env = append(env, fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(pairs)))
	for i, pair := range pairs {
		env = append(env,
			fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, pair.k),
			fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, pair.v),
		)
	}
	return env, extraArgs, nil
}

// CommandDetail bounds the command diagnostic so a remote failure cannot echo
// an unbounded or secret-bearing stream.
func CommandDetail(result GitCommandResult) string {
	detail := strings.TrimSpace(string(result.Stderr))
	if detail == "" {
		detail = strings.TrimSpace(string(result.Stdout))
	}
	detail = strings.Join(strings.Fields(detail), " ")
	return TruncateString(detail, 512)
}

// gitBaseEnvironment returns the explicit child environment. Only PATH and HOME
// are selected from the parent; every ambient Git setting is dropped so a
// caller cannot smuggle policy or credentials through the environment.
func gitBaseEnvironment() []string {
	environment := []string{
		"LC_ALL=C",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=" + os.DevNull,
		"GIT_AUTHOR_NAME=Kodema Merge Coordinator",
		"GIT_AUTHOR_EMAIL=merge-coordinator@kodema.invalid",
		"GIT_COMMITTER_NAME=Kodema Merge Coordinator",
		"GIT_COMMITTER_EMAIL=merge-coordinator@kodema.invalid",
	}
	if path, ok := os.LookupEnv("PATH"); ok && path != "" {
		environment = append(environment, "PATH="+path)
	}
	if home, ok := os.LookupEnv("HOME"); ok && home != "" {
		environment = append(environment, "HOME="+home)
	}
	return environment
}

// boundedBuffer keeps at most limit bytes and records that the bound was
// exceeded without blocking the child.
type boundedBuffer struct {
	mutex    sync.Mutex
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (bounded *boundedBuffer) Write(data []byte) (int, error) {
	bounded.mutex.Lock()
	defer bounded.mutex.Unlock()
	remaining := bounded.limit - bounded.buffer.Len()
	if remaining < len(data) {
		bounded.overflow = true
		if remaining > 0 {
			_, _ = bounded.buffer.Write(data[:remaining])
		}
		return len(data), nil
	}
	_, _ = bounded.buffer.Write(data)
	return len(data), nil
}

func (bounded *boundedBuffer) exceeded() bool {
	bounded.mutex.Lock()
	defer bounded.mutex.Unlock()
	return bounded.overflow
}

func (bounded *boundedBuffer) bytes() []byte {
	bounded.mutex.Lock()
	defer bounded.mutex.Unlock()
	return append([]byte(nil), bounded.buffer.Bytes()...)
}
