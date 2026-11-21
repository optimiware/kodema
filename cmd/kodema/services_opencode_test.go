package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServicesOpenCodeRequiresStateGitAndServerURL(t *testing.T) {
	credential := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credential, []byte("kodsvc.opencode-proj.tok"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	base := []string{
		"--kind", "opencode",
		"--service", "opencode-project-1",
		"--tenant", "tenant-1",
		"--project", "project-1",
		"--base-url", "http://127.0.0.1:1",
		"--credential-file", credential,
	}
	cases := map[string][]string{
		"missing state-dir":    append(append([]string(nil), base...), "--git-url", "file:///srv/git", "--opencode-url", "http://127.0.0.1:4096"),
		"missing git-url":      append(append([]string(nil), base...), "--state-dir", t.TempDir(), "--opencode-url", "http://127.0.0.1:4096"),
		"missing opencode-url": append(append([]string(nil), base...), "--state-dir", t.TempDir(), "--git-url", "file:///srv/git"),
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stderr lockedBuffer
			code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
			if code != 2 {
				t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "opencode service requires --state-dir, --git-url and --opencode-url") {
				t.Fatalf("stderr = %q; want opencode required flags error", stderr.String())
			}
		})
	}
}

func TestServicesOpenCodeRefusesMultiTurnConcurrencyWithStaticLLMProxy(t *testing.T) {
	credential := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credential, []byte("kodsvc.opencode-proj.tok"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	args := []string{
		"--kind", "opencode",
		"--service", "opencode-project-1",
		"--tenant", "tenant-1",
		"--project", "project-1",
		"--base-url", "http://127.0.0.1:1",
		"--credential-file", credential,
		"--state-dir", t.TempDir(),
		"--git-url", "file:///srv/git",
		"--opencode-url", "http://127.0.0.1:4096",
		"--llm-proxy-listen", "127.0.0.1:0",
		"--max-concurrency", "2",
	}
	var stderr lockedBuffer
	code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--max-concurrency <= 1") {
		t.Fatalf("stderr = %q; want --max-concurrency <= 1 refusal", stderr.String())
	}
}

func TestServicesOpenCodeBrainModeFlagsValidation(t *testing.T) {
	credential := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credential, []byte("kodsvc.opencode-brain.tok"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	base := []string{
		"--kind", "opencode",
		"--mode", "brain",
		"--service", "opencode-brain-1",
		"--tenant", "tenant-1",
		"--project", "project-1",
		"--base-url", "http://127.0.0.1:1",
		"--credential-file", credential,
	}

	t.Run("missing opencode-url fails with brain message", func(t *testing.T) {
		var stderr lockedBuffer
		args := append(append([]string(nil), base...), "--state-dir", t.TempDir())
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "opencode brain service requires --state-dir and --opencode-url") {
			t.Fatalf("stderr = %q", stderr.String())
		}
	})

	t.Run("invalid mode refused", func(t *testing.T) {
		var stderr lockedBuffer
		args := []string{
			"--kind", "opencode",
			"--mode", "invalid-mode",
			"--service", "opencode-brain-1",
			"--tenant", "tenant-1",
			"--project", "project-1",
			"--base-url", "http://127.0.0.1:1",
			"--credential-file", credential,
			"--state-dir", t.TempDir(),
			"--opencode-url", "http://127.0.0.1:4096",
		}
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "unsupported") {
			t.Fatalf("stderr = %q", stderr.String())
		}
	})

	t.Run("invalid sandbox-port refused", func(t *testing.T) {
		var stderr lockedBuffer
		args := append(append([]string(nil), base...),
			"--state-dir", t.TempDir(),
			"--opencode-url", "http://127.0.0.1:4096",
			"--sandbox-port", "80",
		)
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "sandbox port") {
			t.Fatalf("stderr = %q", stderr.String())
		}
	})

	t.Run("brain mode refuses multi-turn concurrency with llm-proxy-listen unless per-turn-proxy-routing is explicit", func(t *testing.T) {
		var stderr lockedBuffer
		args := append(append([]string(nil), base...),
			"--state-dir", t.TempDir(),
			"--opencode-url", "http://127.0.0.1:4096",
			"--llm-proxy-listen", "127.0.0.1:0",
			"--max-concurrency", "2",
		)
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "--max-concurrency <= 1") {
			t.Fatalf("stderr = %q; want --max-concurrency <= 1 refusal", stderr.String())
		}
	})

	t.Run("brain mode allows multi-turn concurrency when per-turn-proxy-routing is explicitly enabled", func(t *testing.T) {
		var stderr lockedBuffer
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		args := append(append([]string(nil), base...),
			"--state-dir", t.TempDir(),
			"--opencode-url", "http://127.0.0.1:4096",
			"--llm-proxy-listen", "127.0.0.1:0",
			"--per-turn-proxy-routing=true",
			"--max-concurrency", "2",
		)
		code := runServicesEntry(ctx, args, &lockedBuffer{}, &stderr)
		if code == 2 {
			t.Fatalf("explicit --per-turn-proxy-routing=true was unexpectedly refused as usage error (stderr=%q)", stderr.String())
		}
	})
}
