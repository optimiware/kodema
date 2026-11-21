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

func TestServicesOpenCodePasswordFlagsAndEnvFallbacks(t *testing.T) {
	credential := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credential, []byte("kodsvc.opencode-proj.tok"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	pwFile := filepath.Join(t.TempDir(), "opencode.pw")
	if err := os.WriteFile(pwFile, []byte("file-secret\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	emptyPwFile := filepath.Join(t.TempDir(), "empty.pw")
	if err := os.WriteFile(emptyPwFile, []byte("  \n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	base := []string{
		"--kind", "opencode",
		"--service", "opencode-project-1",
		"--tenant", "tenant-1",
		"--project", "project-1",
		"--base-url", "http://127.0.0.1:1",
		"--credential-file", credential,
		"--git-url", "file:///srv/git",
		"--opencode-url", "http://127.0.0.1:4096",
	}

	t.Run("rejects both --opencode-password and --opencode-password-file", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", "")
		t.Setenv("OPENCODE_PASSWORD", "")
		var stderr lockedBuffer
		args := append(append([]string(nil), base...),
			"--state-dir", t.TempDir(),
			"--opencode-password", "inline-pw",
			"--opencode-password-file", pwFile,
		)
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "cannot use both --opencode-password and --opencode-password-file") {
			t.Fatalf("stderr = %q", stderr.String())
		}
	})

	t.Run("rejects both KODEMA_OPENCODE_PASSWORD and KODEMA_OPENCODE_PASSWORD_FILE env vars", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "env-pw")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", pwFile)
		t.Setenv("OPENCODE_PASSWORD", "")
		var stderr lockedBuffer
		args := append(append([]string(nil), base...), "--state-dir", t.TempDir())
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "KODEMA_OPENCODE_PASSWORD and KODEMA_OPENCODE_PASSWORD_FILE") {
			t.Fatalf("stderr = %q", stderr.String())
		}
	})

	t.Run("rejects unreadable or empty --opencode-password-file", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", "")
		t.Setenv("OPENCODE_PASSWORD", "")
		for name, p := range map[string]string{
			"missing": filepath.Join(t.TempDir(), "missing.pw"),
			"empty":   emptyPwFile,
		} {
			t.Run(name, func(t *testing.T) {
				var stderr lockedBuffer
				args := append(append([]string(nil), base...),
					"--state-dir", t.TempDir(),
					"--opencode-password-file", p,
				)
				code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
				if code != 2 {
					t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
				}
			})
		}
	})

	t.Run("rejects newline in --opencode-password", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", "")
		t.Setenv("OPENCODE_PASSWORD", "")
		var stderr lockedBuffer
		args := append(append([]string(nil), base...),
			"--state-dir", t.TempDir(),
			"--opencode-password", "bad\npassword",
		)
		code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
		if code != 2 {
			t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
		}
	})

	t.Run("accepts --opencode-password-file even when ambient OPENCODE_PASSWORD is set", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", "")
		t.Setenv("OPENCODE_PASSWORD", "ambient-pw")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var stderr lockedBuffer
		args := append(append([]string(nil), base...),
			"--state-dir", t.TempDir(),
			"--opencode-password-file", pwFile,
		)
		code := runServicesEntry(ctx, args, &lockedBuffer{}, &stderr)
		if code == 2 {
			t.Fatalf("--opencode-password-file with ambient OPENCODE_PASSWORD was unexpectedly rejected as usage error (stderr=%q)", stderr.String())
		}
	})

	t.Run("accepts OPENCODE_PASSWORD fallback when KODEMA_OPENCODE_PASSWORD is unset", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", "")
		t.Setenv("OPENCODE_PASSWORD", "fallback-pw")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var stderr lockedBuffer
		args := append(append([]string(nil), base...), "--state-dir", t.TempDir())
		code := runServicesEntry(ctx, args, &lockedBuffer{}, &stderr)
		if code == 2 {
			t.Fatalf("OPENCODE_PASSWORD fallback was unexpectedly rejected as usage error (stderr=%q)", stderr.String())
		}
	})

	t.Run("accepts KODEMA_OPENCODE_PASSWORD_FILE even when ambient OPENCODE_PASSWORD is set", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", pwFile)
		t.Setenv("OPENCODE_PASSWORD", "ambient-pw")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var stderr lockedBuffer
		args := append(append([]string(nil), base...), "--state-dir", t.TempDir())
		code := runServicesEntry(ctx, args, &lockedBuffer{}, &stderr)
		if code == 2 {
			t.Fatalf("KODEMA_OPENCODE_PASSWORD_FILE with ambient OPENCODE_PASSWORD was unexpectedly rejected (stderr=%q)", stderr.String())
		}
	})

	t.Run("rejects explicit empty --opencode-password-file", func(t *testing.T) {
		t.Setenv("KODEMA_OPENCODE_PASSWORD", "")
		t.Setenv("KODEMA_OPENCODE_PASSWORD_FILE", "")
		t.Setenv("OPENCODE_PASSWORD", "")
		for _, emptyVal := range []string{"", "   "} {
			var stderr lockedBuffer
			args := append(append([]string(nil), base...),
				"--state-dir", t.TempDir(),
				"--opencode-password-file", emptyVal,
			)
			code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
			if code != 2 {
				t.Fatalf("empty --opencode-password-file %q exit code = %d; want 2 (stderr=%q)", emptyVal, code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "--opencode-password-file requires a non-empty path") {
				t.Fatalf("stderr = %q; want non-empty path usage error", stderr.String())
			}
		}
	})

	t.Run("help and invalid flag never leak secret env vars via flag defaults", func(t *testing.T) {
		const (
			secretAmbient = "super-secret-opencode-pw"
			secretKodema  = "super-secret-kodema-pw"
			secretProxy   = "super-secret-llm-proxy-key"
		)
		t.Setenv("OPENCODE_PASSWORD", secretAmbient)
		t.Setenv("KODEMA_OPENCODE_PASSWORD", secretKodema)
		t.Setenv("KODEMA_OPENCODE_LLM_PROXY_API_KEY", secretProxy)

		for _, flagArgs := range [][]string{{"--help"}, {"--definitely-unknown-flag"}} {
			var stdout, stderr lockedBuffer
			code := runServicesEntry(context.Background(), flagArgs, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("args %v exit code = %d; want 2", flagArgs, code)
			}
			combined := stdout.String() + "\n" + stderr.String()
			for _, secret := range []string{secretAmbient, secretKodema, secretProxy} {
				if strings.Contains(combined, secret) {
					t.Fatalf("args %v leaked secret %q in output:\n%s", flagArgs, secret, combined)
				}
			}
		}
	})
}
