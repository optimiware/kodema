package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ToolDefinition describes one MCP tool exposed by `tools/list`.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// ToolCallResult is the MCP `tools/call` result payload.
type ToolCallResult struct {
	Content []ToolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// ToolContent is one content item inside ToolCallResult.
type ToolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func textResult(text string) ToolCallResult {
	return ToolCallResult{
		Content: []ToolContent{{Type: "text", Text: text}},
	}
}

func errorResult(err error) ToolCallResult {
	return ToolCallResult{
		Content: []ToolContent{{Type: "text", Text: err.Error()}},
		IsError: true,
	}
}

// ToolDefinitions returns the 6 /workspace-jailed MCP tools.
func ToolDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{
			Name:        "read_file",
			Description: "Read a UTF-8 text file inside /workspace with optional 1-based line offset and line limit.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"path"},
				"properties": map[string]any{
					"path":   map[string]any{"type": "string", "description": "Path relative to /workspace (or inside /workspace)."},
					"offset": map[string]any{"type": "integer", "minimum": 1, "description": "1-based starting line number (default 1)."},
					"limit":  map[string]any{"type": "integer", "minimum": 1, "description": "Maximum number of lines to return."},
				},
			},
		},
		{
			Name:        "write_file",
			Description: "Create or overwrite a file strictly inside /workspace (atomically creating missing parent directories inside /workspace).",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"path", "content"},
				"properties": map[string]any{
					"path":    map[string]any{"type": "string", "description": "Path relative to /workspace (or inside /workspace)."},
					"content": map[string]any{"type": "string", "description": "Complete file contents to write."},
				},
			},
		},
		{
			Name:        "edit_file",
			Description: "Exact find-and-replace inside an existing /workspace file.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"path", "old_string", "new_string"},
				"properties": map[string]any{
					"path":        map[string]any{"type": "string", "description": "Path relative to /workspace (or inside /workspace)."},
					"old_string":  map[string]any{"type": "string", "description": "Exact text to find."},
					"new_string":  map[string]any{"type": "string", "description": "Replacement text (must differ from old_string)."},
					"replace_all": map[string]any{"type": "boolean", "description": "Replace every occurrence when true; otherwise old_string must match once."},
				},
			},
		},
		{
			Name:        "glob",
			Description: "Find files under /workspace matching a glob pattern (supports **), skipping symlinks escaping /workspace.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"pattern"},
				"properties": map[string]any{
					"pattern": map[string]any{"type": "string", "description": "Glob pattern such as '**/*.go' or 'src/*.ts'."},
					"path":    map[string]any{"type": "string", "description": "Optional starting directory inside /workspace (default '.')."},
					"limit":   map[string]any{"type": "integer", "minimum": 1, "description": "Maximum number of matching file paths to return."},
				},
			},
		},
		{
			Name:        "grep",
			Description: "Search file contents under /workspace using a regex or literal string, skipping symlinks escaping /workspace.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"pattern"},
				"properties": map[string]any{
					"pattern":        map[string]any{"type": "string", "description": "Regular expression or literal string to match."},
					"path":           map[string]any{"type": "string", "description": "Optional file or directory inside /workspace (default '.')."},
					"include":        map[string]any{"type": "string", "description": "Optional filename/path glob filter (for example '*.go')."},
					"literal":        map[string]any{"type": "boolean", "description": "Treat pattern as literal text when true."},
					"case_sensitive": map[string]any{"type": "boolean", "description": "Case-sensitive search (default true)."},
					"limit":          map[string]any{"type": "integer", "minimum": 1, "description": "Maximum number of matching lines to return."},
				},
			},
		},
		{
			Name:        "bash",
			Description: "Execute a bounded shell command inside /workspace with process-group timeout/cancel isolation and truncated output.",
			InputSchema: map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"required":             []string{"command"},
				"properties": map[string]any{
					"command":         map[string]any{"type": "string", "description": "Shell command to execute."},
					"timeout_seconds": map[string]any{"type": "number", "minimum": 0.1, "description": "Optional command timeout in seconds (clamped to server max)."},
					"workdir":         map[string]any{"type": "string", "description": "Optional working directory inside /workspace (default '.')."},
				},
			},
		},
	}
}

func (s *Server) callTool(ctx context.Context, name string, rawArgs json.RawMessage) (ToolCallResult, error) {
	switch name {
	case "read_file":
		return s.toolReadFile(rawArgs), nil
	case "write_file":
		return s.toolWriteFile(rawArgs), nil
	case "edit_file":
		return s.toolEditFile(rawArgs), nil
	case "glob":
		return s.toolGlob(rawArgs), nil
	case "grep":
		return s.toolGrep(rawArgs), nil
	case "bash":
		return s.toolBash(ctx, rawArgs), nil
	default:
		return ToolCallResult{}, fmt.Errorf("unknown tool %q", name)
	}
}

func decodeStrictArgs(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage(`{}`)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid tool arguments: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("invalid tool arguments: trailing content")
	}
	return nil
}

func (s *Server) toolReadFile(raw json.RawMessage) ToolCallResult {
	var args struct {
		Path   string `json:"path"`
		Offset int    `json:"offset"`
		Limit  int    `json:"limit"`
	}
	if err := decodeStrictArgs(raw, &args); err != nil {
		return errorResult(err)
	}
	if args.Offset < 0 || args.Limit < 0 {
		return errorResult(errors.New("offset and limit must not be negative"))
	}
	f, _, _, err := s.jail.OpenRead(args.Path, false)
	if err != nil {
		return errorResult(err)
	}
	defer func() { _ = f.Close() }()

	data, err := ReadBoundedRegular(f, s.cfg.MaxFileBytes)
	if err != nil {
		return errorResult(err)
	}
	if isLikelyBinary(data) {
		return errorResult(errors.New("refusing to read binary file"))
	}

	content := string(data)
	if content == "" {
		return textResult("")
	}
	// Strip a single trailing newline before splitting so trailing-newline files
	// don't produce a phantom extra empty line.
	trimmed := strings.TrimSuffix(content, "\n")
	lines := strings.Split(trimmed, "\n")

	start := 0
	if args.Offset > 1 {
		start = args.Offset - 1
	}
	if start >= len(lines) {
		return textResult("")
	}
	limit := args.Limit
	if limit <= 0 || limit > DefaultReadFileLineLimit {
		limit = DefaultReadFileLineLimit
	}
	end := min(start+limit, len(lines))
	return textResult(strings.Join(lines[start:end], "\n"))
}

func (s *Server) toolWriteFile(raw json.RawMessage) ToolCallResult {
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := decodeStrictArgs(raw, &args); err != nil {
		return errorResult(err)
	}
	if int64(len(args.Content)) > s.cfg.MaxFileBytes {
		return errorResult(fmt.Errorf("content exceeds maximum file size of %d bytes", s.cfg.MaxFileBytes))
	}
	rel, err := s.jail.AtomicWrite(args.Path, []byte(args.Content))
	if err != nil {
		return errorResult(err)
	}
	return textResult(fmt.Sprintf("wrote %d bytes to %s", len(args.Content), rel))
}

func (s *Server) toolEditFile(raw json.RawMessage) ToolCallResult {
	var args struct {
		Path       string `json:"path"`
		OldString  string `json:"old_string"`
		NewString  string `json:"new_string"`
		ReplaceAll bool   `json:"replace_all"`
	}
	if err := decodeStrictArgs(raw, &args); err != nil {
		return errorResult(err)
	}
	if args.OldString == "" {
		return errorResult(errors.New("old_string is required"))
	}
	if args.OldString == args.NewString {
		return errorResult(errors.New("new_string must differ from old_string"))
	}
	// Resolve with allowMissing=false AND refuseTargetSymlink=true strictly through os.Root.
	f, _, rel, err := s.jail.OpenRead(args.Path, true)
	if err != nil {
		return errorResult(err)
	}
	data, readErr := ReadBoundedRegular(f, s.cfg.MaxFileBytes)
	_ = f.Close()
	if readErr != nil {
		return errorResult(readErr)
	}
	if isLikelyBinary(data) {
		return errorResult(errors.New("refusing to edit binary file"))
	}

	original := string(data)
	count := strings.Count(original, args.OldString)
	if count == 0 {
		return errorResult(errors.New("old_string not found in file"))
	}
	if count > 1 && !args.ReplaceAll {
		return errorResult(fmt.Errorf("old_string matches %d locations; provide more context or set replace_all=true", count))
	}

	var updated string
	var replaced int
	if args.ReplaceAll {
		updated = strings.ReplaceAll(original, args.OldString, args.NewString)
		replaced = count
	} else {
		updated = strings.Replace(original, args.OldString, args.NewString, 1)
		replaced = 1
	}
	if int64(len(updated)) > s.cfg.MaxFileBytes {
		return errorResult(fmt.Errorf("edited file exceeds maximum file size of %d bytes", s.cfg.MaxFileBytes))
	}
	if err := s.jail.AtomicWriteRel(rel, []byte(updated)); err != nil {
		return errorResult(err)
	}
	return textResult(fmt.Sprintf("replaced %d occurrence(s) in %s", replaced, rel))
}

func (s *Server) toolGlob(raw json.RawMessage) ToolCallResult {
	var args struct {
		Pattern string `json:"pattern"`
		Path    string `json:"path"`
		Limit   int    `json:"limit"`
	}
	if err := decodeStrictArgs(raw, &args); err != nil {
		return errorResult(err)
	}
	if strings.TrimSpace(args.Pattern) == "" {
		return errorResult(errors.New("pattern is required"))
	}
	if strings.ContainsRune(args.Pattern, 0) {
		return errorResult(fmt.Errorf("%w: glob pattern contains NUL byte", ErrPathEscape))
	}
	for _, seg := range strings.Split(filepath.ToSlash(args.Pattern), "/") {
		if seg == ".." {
			return errorResult(fmt.Errorf("%w: glob pattern must not contain '..'", ErrPathEscape))
		}
	}
	// Validate pattern syntax upfront.
	if _, err := matchGlobstar(args.Pattern, "probe"); err != nil {
		return errorResult(err)
	}

	baseRel, err := s.jail.ResolveDirRel(args.Path)
	if err != nil {
		return errorResult(err)
	}

	limit := args.Limit
	if limit <= 0 || limit > s.cfg.MaxGlobResults {
		limit = s.cfg.MaxGlobResults
	}

	var matches []string
	truncated := false
	err = s.jail.walkJailedFiles(baseRel, func(relFromRoot, relFromStart string, _ os.FileInfo) error {
		matched, matchErr := matchGlobstar(args.Pattern, relFromStart)
		if matchErr != nil {
			return matchErr
		}
		if !matched {
			return nil
		}
		if len(matches) >= limit {
			truncated = true
			return errStopWalk
		}
		matches = append(matches, relFromRoot)
		return nil
	})
	if err != nil {
		return errorResult(err)
	}
	if len(matches) == 0 {
		return textResult("")
	}
	out := strings.Join(matches, "\n")
	if truncated {
		out += fmt.Sprintf("\n[truncated at %d matches]", limit)
	}
	return textResult(out)
}

func (s *Server) toolGrep(raw json.RawMessage) ToolCallResult {
	var args struct {
		Pattern       string `json:"pattern"`
		Path          string `json:"path"`
		Include       string `json:"include"`
		Literal       bool   `json:"literal"`
		CaseSensitive *bool  `json:"case_sensitive"`
		Limit         int    `json:"limit"`
	}
	if err := decodeStrictArgs(raw, &args); err != nil {
		return errorResult(err)
	}
	if args.Pattern == "" {
		return errorResult(errors.New("pattern is required"))
	}
	if strings.ContainsRune(args.Include, 0) {
		return errorResult(fmt.Errorf("%w: include filter contains NUL byte", ErrPathEscape))
	}
	if args.Include != "" {
		if _, err := matchGlobstar(args.Include, "probe"); err != nil {
			return errorResult(err)
		}
	}

	caseSensitive := true
	if args.CaseSensitive != nil {
		caseSensitive = *args.CaseSensitive
	}
	expr := args.Pattern
	if args.Literal {
		expr = regexp.QuoteMeta(expr)
	}
	if !caseSensitive {
		expr = "(?i)" + expr
	}
	re, err := regexp.Compile(expr)
	if err != nil {
		return errorResult(fmt.Errorf("invalid grep pattern: %w", err))
	}

	targetRel, targetInfo, err := s.jail.StatExisting(args.Path)
	if err != nil {
		return errorResult(err)
	}
	limit := args.Limit
	if limit <= 0 || limit > s.cfg.MaxGrepMatches {
		limit = s.cfg.MaxGrepMatches
	}

	var results []string
	truncated := false

	scanFile := func(relFromRoot, relFromBase string) error {
		if args.Include != "" {
			matchedBase, _ := matchGlobstar(args.Include, path.Base(relFromBase))
			matchedFull, _ := matchGlobstar(args.Include, relFromBase)
			if !matchedBase && !matchedFull {
				return nil
			}
		}
		f, info, openErr := s.jail.openReadRel(relFromRoot, false)
		if openErr != nil {
			return nil
		}
		defer func() { _ = f.Close() }()
		if info.Size() > s.cfg.MaxFileBytes {
			return nil
		}
		data, readErr := ReadBoundedRegular(f, s.cfg.MaxFileBytes)
		if readErr != nil || isLikelyBinary(data) {
			return nil
		}

		displayPath := relFromRoot
		scanner := bufio.NewScanner(bytes.NewReader(data))
		// Allow scanning lines up to MaxFileBytes (bounded).
		maxBuf := min(int(s.cfg.MaxFileBytes)+1, 4<<20)
		scanner.Buffer(make([]byte, 0, 64<<10), maxBuf)
		lineNo := 0
		for scanner.Scan() {
			lineNo++
			line := scanner.Text()
			if !re.MatchString(line) {
				continue
			}
			if len(results) >= limit {
				truncated = true
				return errStopWalk
			}
			if len(line) > s.cfg.MaxGrepLineBytes {
				line = line[:s.cfg.MaxGrepLineBytes] + "...[truncated]"
			}
			results = append(results, fmt.Sprintf("%s:%d:%s", displayPath, lineNo, line))
		}
		return nil
	}

	if targetInfo.IsDir() {
		err = s.jail.walkJailedFiles(targetRel, func(relFromRoot, relFromStart string, _ os.FileInfo) error {
			return scanFile(relFromRoot, relFromStart)
		})
	} else {
		err = scanFile(targetRel, path.Base(targetRel))
		if errors.Is(err, errStopWalk) {
			err = nil
		}
	}
	if err != nil {
		return errorResult(err)
	}
	if len(results) == 0 {
		return textResult("")
	}
	out := strings.Join(results, "\n")
	if truncated {
		out += fmt.Sprintf("\n[truncated at %d matches]", limit)
	}
	return textResult(out)
}

func (s *Server) toolBash(ctx context.Context, raw json.RawMessage) ToolCallResult {
	var args struct {
		Command        string  `json:"command"`
		TimeoutSeconds float64 `json:"timeout_seconds"`
		Workdir        string  `json:"workdir"`
	}
	if err := decodeStrictArgs(raw, &args); err != nil {
		return errorResult(err)
	}
	if strings.TrimSpace(args.Command) == "" {
		return errorResult(errors.New("command is required"))
	}
	if strings.ContainsRune(args.Command, 0) {
		return errorResult(errors.New("command contains NUL byte"))
	}
	timeout, err := clampBashTimeout(args.TimeoutSeconds, s.cfg.DefaultCmdTimeout, s.cfg.MaxCmdTimeout)
	if err != nil {
		return errorResult(err)
	}

	workdir, err := s.jail.ResolveWorkdir(args.Workdir)
	if err != nil {
		return errorResult(err)
	}

	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	shellPath := s.resolveShell()
	cmd := exec.Command(shellPath, "-c", args.Command)
	cmd.Dir = workdir
	cmd.Env = s.sanitizedBashEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdoutBuf := newCappedBuffer(s.cfg.MaxOutputBytes)
	stderrBuf := newCappedBuffer(s.cfg.MaxOutputBytes)
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	if err := cmd.Start(); err != nil {
		return errorResult(fmt.Errorf("start shell command: %w", err))
	}

	pgid := cmd.Process.Pid
	s.registerPGID(pgid)
	defer func() {
		// Always terminate any lingering background children in this process group.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		s.unregisterPGID(pgid)
	}()

	done := make(chan struct{})
	var timedOut atomic.Bool
	var cancelled atomic.Bool
	go func() {
		select {
		case <-cmdCtx.Done():
			if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
				timedOut.Store(true)
			} else {
				cancelled.Store(true)
			}
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		case <-done:
		}
	}()

	waitErr := cmd.Wait()
	close(done)

	exitCode := 0
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "exit_code: %d\n", exitCode)
	if timedOut.Load() {
		fmt.Fprintf(&b, "timed_out: true (limit %s)\n", timeout)
	} else if cancelled.Load() {
		fmt.Fprintf(&b, "cancelled: true\n")
	}
	stdoutStr, stdoutTrunc := stdoutBuf.Result()
	stderrStr, stderrTrunc := stderrBuf.Result()
	if stdoutStr != "" || stdoutTrunc {
		b.WriteString("stdout:\n")
		b.WriteString(stdoutStr)
		if !strings.HasSuffix(stdoutStr, "\n") && stdoutStr != "" {
			b.WriteByte('\n')
		}
		if stdoutTrunc {
			fmt.Fprintf(&b, "[stdout truncated at %d bytes]\n", s.cfg.MaxOutputBytes)
		}
	}
	if stderrStr != "" || stderrTrunc {
		b.WriteString("stderr:\n")
		b.WriteString(stderrStr)
		if !strings.HasSuffix(stderrStr, "\n") && stderrStr != "" {
			b.WriteByte('\n')
		}
		if stderrTrunc {
			fmt.Fprintf(&b, "[stderr truncated at %d bytes]\n", s.cfg.MaxOutputBytes)
		}
	}

	res := textResult(strings.TrimRight(b.String(), "\n"))
	if timedOut.Load() || cancelled.Load() || exitCode != 0 {
		res.IsError = true
	}
	return res
}

func clampBashTimeout(timeoutSeconds float64, defaultTimeout, maxTimeout time.Duration) (time.Duration, error) {
	if math.IsNaN(timeoutSeconds) || math.IsInf(timeoutSeconds, 0) {
		return 0, errors.New("timeout_seconds must be a finite number")
	}
	if timeoutSeconds < 0 {
		return 0, errors.New("timeout_seconds must not be negative")
	}
	if timeoutSeconds == 0 {
		return defaultTimeout, nil
	}
	maxSec := maxTimeout.Seconds()
	clampedSec := timeoutSeconds
	if clampedSec > maxSec {
		clampedSec = maxSec
	}
	requested := time.Duration(clampedSec * float64(time.Second))
	if requested < 100*time.Millisecond {
		requested = 100 * time.Millisecond
	}
	return min(requested, maxTimeout), nil
}

func (s *Server) resolveShell() string {
	if strings.TrimSpace(s.cfg.ShellPath) != "" {
		return s.cfg.ShellPath
	}
	if bashPath, err := exec.LookPath("bash"); err == nil && bashPath != "" {
		return bashPath
	}
	return "/bin/sh"
}

// sanitizedBashEnv preserves /kodema/cache/*, $KODEMA_GIT_URL, GIT_CONFIG_*,
// PATH, and standard toolchain env vars while strictly stripping
// KODEMA_SANDBOX_TOKEN, KODEMA_SANDBOX_TOKEN_FILE, and any custom --token-env
// so untrusted shell commands can never read or leak the sandbox MCP bearer
// token.
func (s *Server) sanitizedBashEnv() []string {
	raw := os.Environ()
	out := make([]string, 0, len(raw))
	for _, kv := range raw {
		name, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if name == DefaultTokenEnv || name == DefaultTokenFileEnv || name == s.cfg.TokenEnv {
			continue
		}
		out = append(out, kv)
	}
	return out
}

func isLikelyBinary(data []byte) bool {
	sample := data
	if len(sample) > 8192 {
		sample = sample[:8192]
	}
	return bytes.IndexByte(sample, 0) >= 0
}

// cappedBuffer captures up to limit bytes and discards any excess without error
// so child processes never block on a full pipe and never OOM the sandbox server.
type cappedBuffer struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	limit     int
	truncated bool
}

func newCappedBuffer(limit int) *cappedBuffer {
	return &cappedBuffer{limit: limit}
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := len(p)
	remaining := c.limit - c.buf.Len()
	if remaining > 0 {
		if len(p) <= remaining {
			_, _ = c.buf.Write(p)
		} else {
			_, _ = c.buf.Write(p[:remaining])
			c.truncated = true
		}
	} else if len(p) > 0 {
		c.truncated = true
	}
	return total, nil
}

func (c *cappedBuffer) Result() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String(), c.truncated
}
