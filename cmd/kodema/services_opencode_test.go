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
