package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSandboxServeFailsClosedWithoutToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := RunWithDeps(
		context.Background(),
		Config{},
		[]string{"sandbox", "serve", "--workspace", t.TempDir(), "--listen", "127.0.0.1:0"},
		Ports{Out: &stdout, Err: &stderr},
		CommandDeps{Env: func(string) string { return "" }},
	)
	if code != 2 {
		t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "authentication token is required") {
		t.Fatalf("stderr = %q; want token required error", stderr.String())
	}
}

func TestSandboxServeHelpAndGenerateToken(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := RunWithDeps(
		context.Background(),
		Config{},
		[]string{"sandbox", "serve", "--help"},
		Ports{Out: &stdout, Err: &stderr},
		CommandDeps{},
	)
	if code != 0 {
		t.Fatalf("help exit code = %d; stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "--generate-token-file") || !strings.Contains(stdout.String(), "--workspace") {
		t.Fatalf("help stdout missing flags: %q", stdout.String())
	}

	// Start with --generate-token-file and cancel context promptly.
	ws := t.TempDir()
	tokPath := filepath.Join(t.TempDir(), "sandbox.tok")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	stdout.Reset()
	stderr.Reset()
	code = RunWithDeps(
		ctx,
		Config{},
		[]string{
			"sandbox", "serve",
			"--listen", "127.0.0.1:0",
			"--workspace", ws,
			"--generate-token-file", tokPath,
		},
		Ports{Out: &stdout, Err: &stderr},
		CommandDeps{Env: func(string) string { return "" }},
	)
	if code != 0 {
		t.Fatalf("sandbox serve exit code = %d; stderr=%q", code, stderr.String())
	}
	info, err := os.Stat(tokPath)
	if err != nil {
		t.Fatalf("Stat generated token: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("generated token perms = %o; want 0600", info.Mode().Perm())
	}
	tokBytes, _ := os.ReadFile(tokPath)
	tok := strings.TrimSpace(string(tokBytes))
	if tok == "" {
		t.Fatal("generated token file is empty")
	}
	if strings.Contains(stdout.String(), tok) || strings.Contains(stderr.String(), tok) {
		t.Fatal("sandbox serve logged bearer token to stdout/stderr")
	}
}
