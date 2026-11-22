package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func startTestServer(t *testing.T, cfgMut func(*Config)) (*Server, *Client, string, string) {
	t.Helper()
	ws := t.TempDir()
	token, err := GenerateBearerToken()
	if err != nil {
		t.Fatalf("GenerateBearerToken: %v", err)
	}
	cfg := Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: ws,
		Token:        token,
	}
	if cfgMut != nil {
		cfgMut(&cfg)
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	baseURL, err := srv.Start(ctx)
	if err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = srv.Shutdown(context.Background())
	})
	cli, err := NewClient(baseURL, token, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return srv, cli, baseURL, token
}

func TestResolveTokenFailClosedAndGenerate(t *testing.T) {
	t.Run("fails closed without token", func(t *testing.T) {
		_, err := ResolveToken(TokenConfig{
			LookupEnv: func(string) (string, bool) { return "", false },
		})
		if err == nil {
			t.Fatal("ResolveToken succeeded without any token source")
		}
	})

	t.Run("generates 256-bit token with 0600 permissions", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "tok")
		tok, err := ResolveToken(TokenConfig{
			GenerateTokenFile: path,
			LookupEnv:         func(string) (string, bool) { return "", false },
		})
		if err != nil {
			t.Fatalf("ResolveToken generate: %v", err)
		}
		if !strings.HasPrefix(tok, "kodsbox.") || len(tok) < 72 {
			t.Fatalf("generated token shape unexpected (len=%d)", len(tok))
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat generated token: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			t.Fatalf("generated token perms = %o; want 0600", perm)
		}
		readBack, err := ResolveToken(TokenConfig{TokenFile: path})
		if err != nil {
			t.Fatalf("ResolveToken read back: %v", err)
		}
		if readBack != tok {
			t.Fatal("read-back token does not match generated token")
		}
	})

	t.Run("refuses symlink for generate-token-file", func(t *testing.T) {
		dir := t.TempDir()
		outside := filepath.Join(t.TempDir(), "outside")
		link := filepath.Join(dir, "tok-link")
		if err := os.Symlink(outside, link); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		if _, err := ResolveToken(TokenConfig{GenerateTokenFile: link}); err == nil {
			t.Fatal("ResolveToken allowed generating token to symlink path")
		}
	})
}

func TestMCPAuthAndHealthz(t *testing.T) {
	_, cli, baseURL, token := startTestServer(t, nil)

	// /healthz works without auth and never leaks token or workspace.
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz status = %d; want 200", resp.StatusCode)
	}
	if strings.Contains(string(body), token) {
		t.Fatal("/healthz leaked bearer token")
	}
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Fatalf("/healthz body = %s; want status ok", string(body))
	}

	// Auth failure modes must all return 401.
	badHeaders := []struct {
		name   string
		header []string
	}{
		{"missing", nil},
		{"wrong token", []string{"Bearer wrong-token-value-1234567890"}},
		{"malformed scheme", []string{"Basic dXNlcjpwYXNz"}},
		{"empty bearer", []string{"Bearer "}},
		{"duplicate headers", []string{"Bearer " + token, "Bearer " + token}},
	}
	for _, tc := range badHeaders {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)))
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			for _, h := range tc.header {
				req.Header.Add("Authorization", h)
			}
			r, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			_ = r.Body.Close()
			if r.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d; want 401", r.StatusCode)
			}
		})
	}

	// Valid handshake + notifications/initialized + ping + tools/list.
	initRes, err := cli.Initialize(context.Background())
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if initRes["protocolVersion"] == "" {
		t.Fatalf("Initialize missing protocolVersion: %+v", initRes)
	}

	notifReq, _ := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader([]byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)))
	notifReq.Header.Set("Authorization", "Bearer "+token)
	notifResp, err := http.DefaultClient.Do(notifReq)
	if err != nil {
		t.Fatalf("notifications/initialized: %v", err)
	}
	_ = notifResp.Body.Close()
	if notifResp.StatusCode != http.StatusAccepted {
		t.Fatalf("notifications/initialized status = %d; want 202", notifResp.StatusCode)
	}

	tools, err := cli.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(tools) != 6 {
		t.Fatalf("tools count = %d; want 6", len(tools))
	}
	gotNames := map[string]bool{}
	for _, td := range tools {
		gotNames[td.Name] = true
	}
	for _, want := range []string{"read_file", "write_file", "edit_file", "glob", "grep", "bash"} {
		if !gotNames[want] {
			t.Errorf("missing expected MCP tool %q", want)
		}
	}
}

func TestSixMCPToolsEndToEnd(t *testing.T) {
	srv, cli, _, _ := startTestServer(t, nil)
	ctx := context.Background()

	// 1. write_file (nested parent directories created automatically).
	wRes, err := cli.CallTool(ctx, "write_file", map[string]any{
		"path":    "pkg/hello/hello.go",
		"content": "package hello\n\nfunc Greeting() string {\n\treturn \"hello\"\n}\n",
	})
	if err != nil || wRes.IsError {
		t.Fatalf("write_file failed: err=%v res=%+v", err, wRes)
	}

	// 2. read_file with offset & limit.
	rRes, err := cli.CallTool(ctx, "read_file", map[string]any{
		"path":   "pkg/hello/hello.go",
		"offset": 3,
		"limit":  2,
	})
	if err != nil || rRes.IsError {
		t.Fatalf("read_file failed: err=%v res=%+v", err, rRes)
	}
	if got := rRes.FirstText(); got != "func Greeting() string {\n\treturn \"hello\"" {
		t.Fatalf("read_file slice = %q", got)
	}

	// 3. edit_file (fails on missing, fails on ambiguous without replace_all, succeeds on unique & replace_all).
	eMissing, err := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":       "pkg/hello/hello.go",
		"old_string": "not-present",
		"new_string": "other",
	})
	if err != nil || !eMissing.IsError {
		t.Fatalf("edit_file missing expected IsError=true, got err=%v res=%+v", err, eMissing)
	}

	eOK, err := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":       "pkg/hello/hello.go",
		"old_string": `return "hello"`,
		"new_string": `return "world"`,
	})
	if err != nil || eOK.IsError {
		t.Fatalf("edit_file failed: err=%v res=%+v", err, eOK)
	}

	// Write a second file with repeated tokens to test replace_all guard.
	_, _ = cli.CallTool(ctx, "write_file", map[string]any{
		"path":    "notes.txt",
		"content": "foo bar\nfoo baz\n",
	})
	eAmb, err := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":       "notes.txt",
		"old_string": "foo",
		"new_string": "qux",
	})
	if err != nil || !eAmb.IsError {
		t.Fatalf("ambiguous edit_file expected IsError=true, got err=%v res=%+v", err, eAmb)
	}
	eAll, err := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":        "notes.txt",
		"old_string":  "foo",
		"new_string":  "qux",
		"replace_all": true,
	})
	if err != nil || eAll.IsError {
		t.Fatalf("edit_file replace_all failed: err=%v res=%+v", err, eAll)
	}
	rNotes, _ := cli.CallTool(ctx, "read_file", map[string]any{"path": "notes.txt"})
	if rNotes.FirstText() != "qux bar\nqux baz" {
		t.Fatalf("notes.txt after replace_all = %q", rNotes.FirstText())
	}

	// 4. glob with ** recursive pattern.
	gRes, err := cli.CallTool(ctx, "glob", map[string]any{"pattern": "**/*.go"})
	if err != nil || gRes.IsError {
		t.Fatalf("glob failed: err=%v res=%+v", err, gRes)
	}
	if strings.TrimSpace(gRes.FirstText()) != "pkg/hello/hello.go" {
		t.Fatalf("glob **/*.go = %q; want pkg/hello/hello.go", gRes.FirstText())
	}

	// 5. grep regex & include filter.
	grRes, err := cli.CallTool(ctx, "grep", map[string]any{
		"pattern": `return "world"`,
		"include": "*.go",
	})
	if err != nil || grRes.IsError {
		t.Fatalf("grep failed: err=%v res=%+v", err, grRes)
	}
	if !strings.Contains(grRes.FirstText(), `pkg/hello/hello.go:4:	return "world"`) {
		t.Fatalf("grep result = %q", grRes.FirstText())
	}

	// 6. bash runs in workspace root.
	bRes, err := cli.CallTool(ctx, "bash", map[string]any{"command": "pwd && ls pkg/hello/hello.go"})
	if err != nil || bRes.IsError {
		t.Fatalf("bash failed: err=%v res=%+v", err, bRes)
	}
	if !strings.Contains(bRes.FirstText(), srv.WorkspaceRoot()) {
		t.Fatalf("bash pwd output %q does not contain workspace root %q", bRes.FirstText(), srv.WorkspaceRoot())
	}
}

func TestWorkspaceJailBlocksTraversalsAndSymlinkEscapes(t *testing.T) {
	srv, cli, _, _ := startTestServer(t, nil)
	ctx := context.Background()
	ws := srv.WorkspaceRoot()

	// Create outside secret file and directory.
	outsideDir := t.TempDir()
	outsideFile := filepath.Join(outsideDir, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("TOP_SECRET_OUTSIDE_DATA\n"), 0o600); err != nil {
		t.Fatalf("WriteFile outside: %v", err)
	}

	// Create symlinks inside workspace pointing outside, broken symlink outside, and symlink cycle inside.
	if err := os.Symlink(outsideFile, filepath.Join(ws, "escape-file.txt")); err != nil {
		t.Fatalf("Symlink escape-file: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(ws, "escape-dir")); err != nil {
		t.Fatalf("Symlink escape-dir: %v", err)
	}
	if err := os.Symlink(filepath.Join(outsideDir, "does-not-exist.txt"), filepath.Join(ws, "broken-escape.txt")); err != nil {
		t.Fatalf("Symlink broken-escape: %v", err)
	}
	if err := os.WriteFile(filepath.Join(ws, "inside.txt"), []byte("safe inside\n"), 0o644); err != nil {
		t.Fatalf("WriteFile inside: %v", err)
	}
	// Cycle symlink inside workspace to verify glob/grep never loop forever.
	if err := os.MkdirAll(filepath.Join(ws, "sub"), 0o755); err != nil {
		t.Fatalf("MkdirAll sub: %v", err)
	}
	if err := os.Symlink(ws, filepath.Join(ws, "sub", "loop")); err != nil {
		t.Fatalf("Symlink loop: %v", err)
	}

	badReadPaths := []string{
		"",
		"../etc/passwd",
		"sub/../../etc/passwd",
		"/etc/passwd",
		outsideFile,
		"escape-file.txt",
		"escape-dir/secret.txt",
		"escape-dir/../secret.txt",
		"broken-escape.txt",
		"inside.txt\x00.go",
	}
	for _, p := range badReadPaths {
		res, err := cli.CallTool(ctx, "read_file", map[string]any{"path": p})
		if err != nil {
			t.Fatalf("read_file(%q) rpc error: %v", p, err)
		}
		if !res.IsError {
			t.Errorf("read_file(%q) unexpectedly succeeded: %q", p, res.FirstText())
		}
		if strings.Contains(res.FirstText(), "TOP_SECRET_OUTSIDE_DATA") {
			t.Errorf("read_file(%q) leaked outside data!", p)
		}
	}

	badWritePaths := []string{
		"",
		"../pwned.txt",
		"sub/../../pwned.txt",
		filepath.Join(outsideDir, "pwned.txt"),
		"escape-file.txt",
		"escape-dir/pwned.txt",
		"escape-dir/nested/pwned.txt",
		"broken-escape.txt",
		"evil\x00name.txt",
	}
	for _, p := range badWritePaths {
		res, err := cli.CallTool(ctx, "write_file", map[string]any{"path": p, "content": "pwned"})
		if err != nil {
			t.Fatalf("write_file(%q) rpc error: %v", p, err)
		}
		if !res.IsError {
			t.Errorf("write_file(%q) unexpectedly succeeded: %q", p, res.FirstText())
		}
	}
	if _, err := os.Stat(filepath.Join(outsideDir, "pwned.txt")); !os.IsNotExist(err) {
		t.Fatal("write_file escaped jail and created outside file!")
	}
	if data, _ := os.ReadFile(outsideFile); string(data) != "TOP_SECRET_OUTSIDE_DATA\n" {
		t.Fatal("write_file modified outside symlink target!")
	}

	// edit_file must also refuse symlink escape.
	eRes, _ := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":       "escape-file.txt",
		"old_string": "TOP_SECRET",
		"new_string": "HACKED",
	})
	if !eRes.IsError {
		t.Fatal("edit_file allowed editing escaping symlink")
	}

	// glob & grep must skip escaping symlinks AND reject explicit escaping path AND survive cycles.
	globRes, err := cli.CallTool(ctx, "glob", map[string]any{"pattern": "**/*"})
	if err != nil || globRes.IsError {
		t.Fatalf("glob failed: err=%v res=%+v", err, globRes)
	}
	if strings.Contains(globRes.FirstText(), "secret.txt") || strings.Contains(globRes.FirstText(), "escape-file") {
		t.Fatalf("glob exposed escaping symlink target: %q", globRes.FirstText())
	}

	globEsc, _ := cli.CallTool(ctx, "glob", map[string]any{"pattern": "*", "path": "escape-dir"})
	if !globEsc.IsError {
		t.Fatalf("glob accepted escaping symlink directory: %+v", globEsc)
	}

	grepRes, err := cli.CallTool(ctx, "grep", map[string]any{"pattern": "TOP_SECRET"})
	if err != nil || grepRes.IsError {
		t.Fatalf("grep failed: err=%v res=%+v", err, grepRes)
	}
	if strings.TrimSpace(grepRes.FirstText()) != "" {
		t.Fatalf("grep leaked outside secret via symlink: %q", grepRes.FirstText())
	}

	grepEsc, _ := cli.CallTool(ctx, "grep", map[string]any{"pattern": "TOP_SECRET", "path": "escape-dir"})
	if !grepEsc.IsError {
		t.Fatalf("grep accepted escaping symlink directory: %+v", grepEsc)
	}

	// bash workdir must also reject escaping symlink directory.
	bashEsc, _ := cli.CallTool(ctx, "bash", map[string]any{"command": "pwd", "workdir": "escape-dir"})
	if !bashEsc.IsError {
		t.Fatalf("bash accepted escaping workdir: %+v", bashEsc)
	}
}

func TestWorkspaceJailBlocksMissingParentPopSymlinkEscape(t *testing.T) {
	srv, cli, _, _ := startTestServer(t, nil)
	ctx := context.Background()
	ws := srv.WorkspaceRoot()

	outsideDir := t.TempDir()
	outsideSecret := filepath.Join(outsideDir, "secret")
	if err := os.WriteFile(outsideSecret, []byte("TOP_SECRET_ORACLE_DATA\n"), 0o600); err != nil {
		t.Fatalf("WriteFile outside secret: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(ws, "dirlink")); err != nil {
		t.Fatalf("Symlink dirlink: %v", err)
	}

	const bypassPath = "missing/../dirlink/secret"

	// Direct WorkspaceJail assertions: must return ErrPathEscape on read and write.
	if _, _, _, err := srv.jail.OpenRead(bypassPath, false); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("OpenRead(%q, false) err = %v; want ErrPathEscape", bypassPath, err)
	}
	if _, _, _, err := srv.jail.OpenRead(bypassPath, true); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("OpenRead(%q, true) err = %v; want ErrPathEscape", bypassPath, err)
	}
	if _, err := srv.jail.AtomicWrite(bypassPath, []byte("pwned")); !errors.Is(err, ErrPathEscape) {
		t.Fatalf("AtomicWrite(%q) err = %v; want ErrPathEscape", bypassPath, err)
	}

	// End-to-end MCP tool assertions: read_file, write_file, edit_file must all fail with ErrPathEscape.
	rRes, err := cli.CallTool(ctx, "read_file", map[string]any{"path": bypassPath})
	if err != nil {
		t.Fatalf("read_file rpc error: %v", err)
	}
	if !rRes.IsError || !strings.Contains(rRes.FirstText(), ErrPathEscape.Error()) {
		t.Fatalf("read_file(%q) = %+v; want ErrPathEscape error", bypassPath, rRes)
	}

	wRes, err := cli.CallTool(ctx, "write_file", map[string]any{"path": bypassPath, "content": "pwned"})
	if err != nil {
		t.Fatalf("write_file rpc error: %v", err)
	}
	if !wRes.IsError || !strings.Contains(wRes.FirstText(), ErrPathEscape.Error()) {
		t.Fatalf("write_file(%q) = %+v; want ErrPathEscape error", bypassPath, wRes)
	}

	eRes, err := cli.CallTool(ctx, "edit_file", map[string]any{
		"path":       bypassPath,
		"old_string": "TOP_SECRET_ORACLE_DATA",
		"new_string": "HACKED",
	})
	if err != nil {
		t.Fatalf("edit_file rpc error: %v", err)
	}
	if !eRes.IsError || !strings.Contains(eRes.FirstText(), ErrPathEscape.Error()) {
		t.Fatalf("edit_file(%q) = %+v; want ErrPathEscape error", bypassPath, eRes)
	}

	// Confirm the outside secret was never modified.
	if got, _ := os.ReadFile(outsideSecret); string(got) != "TOP_SECRET_ORACLE_DATA\n" {
		t.Fatalf("outside secret modified: %q", string(got))
	}
}

func TestResolveTokenRejectsWorkspacePathAndUnlinksTokenFile(t *testing.T) {
	ws := t.TempDir()
	wsTok := filepath.Join(ws, "tok")
	tok, err := GenerateBearerToken()
	if err != nil {
		t.Fatalf("GenerateBearerToken: %v", err)
	}
	if err := os.WriteFile(wsTok, []byte(tok+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	t.Run("rejects token-file inside workspace", func(t *testing.T) {
		if _, err := ResolveToken(TokenConfig{TokenFile: wsTok, WorkspaceDir: ws}); err == nil {
			t.Fatal("ResolveToken accepted token-file inside workspace")
		}
	})

	t.Run("rejects symlink token-file targeting workspace", func(t *testing.T) {
		linkPath := filepath.Join(t.TempDir(), "link-to-ws-tok")
		if err := os.Symlink(wsTok, linkPath); err != nil {
			t.Fatalf("Symlink: %v", err)
		}
		if _, err := ResolveToken(TokenConfig{TokenFile: linkPath, WorkspaceDir: ws}); err == nil {
			t.Fatal("ResolveToken accepted symlink token-file pointing into workspace")
		}
	})

	t.Run("rejects generate-token-file inside workspace", func(t *testing.T) {
		if _, err := ResolveToken(TokenConfig{
			GenerateTokenFile: filepath.Join(ws, "nested", "gen.tok"),
			WorkspaceDir:      ws,
		}); err == nil {
			t.Fatal("ResolveToken accepted generate-token-file inside workspace")
		}
	})

	t.Run("unlinks token-file immediately when UnlinkTokenFile is true", func(t *testing.T) {
		outTok := filepath.Join(t.TempDir(), "ephemeral.tok")
		if err := os.WriteFile(outTok, []byte(tok+"\n"), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		got, err := ResolveToken(TokenConfig{
			TokenFile:       outTok,
			WorkspaceDir:    ws,
			UnlinkTokenFile: true,
		})
		if err != nil {
			t.Fatalf("ResolveToken unlink: %v", err)
		}
		if got != tok {
			t.Fatal("token mismatch")
		}
		if _, err := os.Stat(outTok); !os.IsNotExist(err) {
			t.Fatalf("expected unlinked token file to be removed, stat err = %v", err)
		}
	})
}

func TestBashTimeoutValidatesNonFiniteAndClampsOverflow(t *testing.T) {
	srv, cli, _, _ := startTestServer(t, func(c *Config) {
		c.MaxCmdTimeout = 2 * time.Second
	})
	if srv.httpServer.IdleTimeout != 60*time.Second {
		t.Fatalf("httpServer.IdleTimeout = %s; want 60s", srv.httpServer.IdleTimeout)
	}

	// NaN and Inf must be rejected directly by toolBash (JSON cannot encode them,
	// so test both overflow via JSON and NaN/Inf via internal call helper).
	hugeRes, err := cli.CallTool(context.Background(), "bash", map[string]any{
		"command":         "echo ok",
		"timeout_seconds": 1e15,
	})
	if err != nil || hugeRes.IsError || !strings.Contains(hugeRes.FirstText(), "ok") {
		t.Fatalf("bash with huge timeout_seconds failed: err=%v res=%+v", err, hugeRes)
	}

	// Verify clamp logic on non-finite floats directly via helper.
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := clampBashTimeout(bad, time.Second, 2*time.Second); err == nil {
			t.Fatalf("clampBashTimeout(%v) unexpectedly succeeded", bad)
		}
	}
	got, err := clampBashTimeout(1e15, time.Second, 2*time.Second)
	if err != nil || got != 2*time.Second {
		t.Fatalf("clampBashTimeout(1e15) = (%s, %v); want (2s, nil)", got, err)
	}
}

func TestBashStripsSandboxTokenAndPreservesGitAndCacheEnv(t *testing.T) {
	t.Setenv("KODEMA_SANDBOX_TOKEN", "kodsbox.supersecrettokenvalue1234567890")
	t.Setenv("KODEMA_SANDBOX_TOKEN_FILE", "/tmp/secret-sandbox.token")
	t.Setenv("CUSTOM_SANDBOX_TOK", "kodsbox.customsecrettokenvalue1234567890")
	t.Setenv("KODEMA_GIT_URL", "https://git.kodema.local")
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GOMODCACHE", "/kodema/cache/go-mod")

	_, cli, _, _ := startTestServer(t, func(c *Config) {
		c.TokenEnv = "CUSTOM_SANDBOX_TOK"
	})

	res, err := cli.CallTool(context.Background(), "bash", map[string]any{
		"command": `printf "tok1=%s\ntok2=%s\ntokfile=%s\ngit=%s\ngitcfg=%s\ncache=%s\n" "${KODEMA_SANDBOX_TOKEN:-}" "${CUSTOM_SANDBOX_TOK:-}" "${KODEMA_SANDBOX_TOKEN_FILE:-}" "${KODEMA_GIT_URL:-}" "${GIT_CONFIG_COUNT:-}" "${GOMODCACHE:-}"`,
	})
	if err != nil || res.IsError {
		t.Fatalf("bash failed: err=%v res=%+v", err, res)
	}
	out := res.FirstText()
	if strings.Contains(out, "supersecrettokenvalue") || strings.Contains(out, "customsecrettokenvalue") || strings.Contains(out, "secret-sandbox.token") {
		t.Fatalf("bash leaked sandbox token or token file in environment: %q", out)
	}
	if !strings.Contains(out, "tok1=\n") || !strings.Contains(out, "tok2=\n") || !strings.Contains(out, "tokfile=\n") {
		t.Fatalf("expected stripped token env vars to be empty: %q", out)
	}
	for _, want := range []string{"git=https://git.kodema.local", "gitcfg=1", "cache=/kodema/cache/go-mod"} {
		if !strings.Contains(out, want) {
			t.Fatalf("bash dropped expected env %q; got: %q", want, out)
		}
	}
}

func TestBashTimeoutKillsEntireProcessGroupAndTruncatesOutput(t *testing.T) {
	srv, cli, _, _ := startTestServer(t, func(c *Config) {
		c.MaxCmdTimeout = 2 * time.Second
		c.MaxOutputBytes = 128
	})
	ctx := context.Background()

	// 1. Output truncation test.
	truncRes, err := cli.CallTool(ctx, "bash", map[string]any{
		"command": `python3 -c 'import sys; sys.stdout.write("A"*2000); sys.stderr.write("B"*2000)' 2>&1 || awk 'BEGIN{for(i=0;i<2000;i++)printf "A"}'`,
	})
	if err != nil {
		t.Fatalf("bash truncation rpc error: %v", err)
	}
	if !strings.Contains(truncRes.FirstText(), "[stdout truncated at 128 bytes]") {
		t.Fatalf("expected stdout truncation notice, got: %q", truncRes.FirstText())
	}

	// 2. Process-group kill on timeout: spawn a background sleeper child, record its PID, then wait.
	pidFile := filepath.Join(srv.WorkspaceRoot(), "child.pid")
	cmd := `sleep 60 & echo $! > child.pid; wait`
	start := time.Now()
	tRes, err := cli.CallTool(ctx, "bash", map[string]any{
		"command":         cmd,
		"timeout_seconds": 0.4,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("bash timeout rpc error: %v", err)
	}
	if !tRes.IsError || !strings.Contains(tRes.FirstText(), "timed_out: true") {
		t.Fatalf("expected timed_out error result, got: %+v", tRes)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("bash timeout took too long (%s); child pipe may have hung", elapsed)
	}

	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read child.pid: %v", err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(string(rawPID)))
	if err != nil || childPID <= 0 {
		t.Fatalf("invalid child pid %q: %v", string(rawPID), err)
	}

	// Verify the background child process is completely dead (ESRCH).
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := syscall.Kill(childPID, 0)
		if err == syscall.ESRCH {
			break
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
			t.Fatalf("background child process %d survived bash timeout (err=%v)", childPID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestServerShutdownKillsActiveProcessGroup(t *testing.T) {
	ws := t.TempDir()
	token, _ := GenerateBearerToken()
	srv, err := NewServer(Config{
		ListenAddr:    "127.0.0.1:0",
		WorkspaceDir:  ws,
		Token:         token,
		MaxCmdTimeout: 30 * time.Second,
		ShutdownGrace: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	baseURL, err := srv.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cli, err := NewClient(baseURL, token, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	pidFile := filepath.Join(srv.WorkspaceRoot(), "shutdown.pid")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = cli.CallTool(context.Background(), "bash", map[string]any{
			"command": `sleep 60 & echo $! > shutdown.pid; wait`,
		})
	}()

	// Wait until child.pid is written.
	var childPID int
	waitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitDeadline) {
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
				childPID = pid
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if childPID == 0 {
		cancel()
		t.Fatal("child process did not write shutdown.pid in time")
	}

	// Cancel server context -> must shut down cleanly and kill the active child process group.
	cancel()
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight bash tool call did not terminate on server shutdown")
	}

	killDeadline := time.Now().Add(2 * time.Second)
	for {
		if err := syscall.Kill(childPID, 0); err == syscall.ESRCH {
			break
		}
		if time.Now().After(killDeadline) {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
			t.Fatalf("child pid %d survived server shutdown", childPID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestRequestBodySizeLimitEnforced(t *testing.T) {
	_, _, baseURL, token := startTestServer(t, func(c *Config) {
		c.MaxRequestBodyBytes = 256
	})
	bigPayload := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "write_file",
			"arguments": map[string]any{"path": "a.txt", "content": strings.Repeat("x", 1024)},
		},
	}
	raw, _ := json.Marshal(bigPayload)
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/mcp", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /mcp: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d; want 413", resp.StatusCode)
	}
}

func TestServerStartTwiceReturnsErrorAndShutdownExitsWatcher(t *testing.T) {
	ws := t.TempDir()
	token, err := GenerateBearerToken()
	if err != nil {
		t.Fatalf("GenerateBearerToken: %v", err)
	}

	// Double Start / Serve on the same Server must fail cleanly.
	srv, err := NewServer(Config{
		ListenAddr:   "127.0.0.1:0",
		WorkspaceDir: ws,
		Token:        token,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if _, err := srv.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if _, err := srv.Start(context.Background()); err == nil {
		t.Fatal("second Start succeeded; want error")
	}
	if err := srv.Serve(context.Background()); err == nil {
		t.Fatal("Serve after Start succeeded; want error")
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	// Starting with a non-cancelled context and shutting down directly must not
	// leak the Start context-watcher goroutine.
	runtime.GC()
	before := runtime.NumGoroutine()
	const iterations = 20
	for range iterations {
		s, err := NewServer(Config{
			ListenAddr:   "127.0.0.1:0",
			WorkspaceDir: ws,
			Token:        token,
		})
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		if _, err := s.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+5 && time.Now().Before(deadline) {
		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+5 {
		t.Fatalf("goroutines leaked across Start/Shutdown: before=%d after=%d (iterations=%d)", before, after, iterations)
	}
}

func TestToolBashWatchdogConsistencyUnderRace(t *testing.T) {
	srv, _, _, _ := startTestServer(t, func(c *Config) {
		c.MaxCmdTimeout = 5 * time.Second
		c.DefaultCmdTimeout = 2 * time.Second
	})

	// Exercise timeout and cancellation boundaries repeatedly under -race to
	// confirm the watchdog goroutine is joined before reading timedOut/cancelled
	// and that formatted output always agrees with IsError.
	for i := range 8 {
		ctx, cancel := context.WithCancel(context.Background())
		if i%2 == 0 {
			go func() {
				time.Sleep(15 * time.Millisecond)
				cancel()
			}()
		}
		raw, _ := json.Marshal(map[string]any{
			"command":         "sleep 0.05",
			"timeout_seconds": 0.1,
		})
		res := srv.toolBash(ctx, raw)
		cancel()
		if len(res.Content) != 1 {
			t.Fatalf("unexpected content len %d", len(res.Content))
		}
		text := res.Content[0].Text
		hasTimeout := strings.Contains(text, "timed_out: true")
		hasCancel := strings.Contains(text, "cancelled: true")
		hasZeroExit := strings.HasPrefix(text, "exit_code: 0\n") || text == "exit_code: 0"
		wantErr := hasTimeout || hasCancel || !hasZeroExit
		if res.IsError != wantErr {
			t.Fatalf("iter %d: IsError=%v inconsistent with output:\n%s", i, res.IsError, text)
		}
	}
}

func TestToolGrepMatchesLineExceeding4MiB(t *testing.T) {
	srv, cli, _, _ := startTestServer(t, func(c *Config) {
		c.MaxFileBytes = 8 << 20
	})
	const bigLen = (4 << 20) + 1024
	var content strings.Builder
	content.Grow(bigLen + 64)
	content.WriteString("first line\r\n")
	content.WriteString(strings.Repeat("a", bigLen))
	content.WriteString("NEEDLE_BEYOND_4MIB\r\n")
	content.WriteString("last line\n")

	if err := os.WriteFile(filepath.Join(srv.WorkspaceRoot(), "huge.txt"), []byte(content.String()), 0o600); err != nil {
		t.Fatalf("write huge.txt: %v", err)
	}
	res, err := cli.CallTool(context.Background(), "grep", map[string]any{
		"pattern": "NEEDLE_BEYOND_4MIB",
		"path":    "huge.txt",
	})
	if err != nil {
		t.Fatalf("grep: %v", err)
	}
	if res.IsError {
		t.Fatalf("grep returned error: %+v", res)
	}
	got := res.Content[0].Text
	if !strings.HasPrefix(got, "huge.txt:2:") || !strings.HasSuffix(got, "...[truncated]") {
		t.Fatalf("grep huge line result = %q; want line 2 match truncated", got)
	}

	// Verify CRLF stripping on normal lines too.
	res2, err := cli.CallTool(context.Background(), "grep", map[string]any{
		"pattern": "^first line$",
		"path":    "huge.txt",
	})
	if err != nil {
		t.Fatalf("grep first line: %v", err)
	}
	if res2.Content[0].Text != "huge.txt:1:first line" {
		t.Fatalf("grep CRLF line = %q; want %q", res2.Content[0].Text, "huge.txt:1:first line")
	}
}
