package install

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type releaseVersionFixture struct {
	t          *testing.T
	scriptPath string
	workDir    string
	remoteDir  string
	fakeBinDir string
}

func newReleaseVersionFixture(t *testing.T, initialVersion string) *releaseVersionFixture {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(repoRoot, "scripts", "release-version")

	base := t.TempDir()
	remoteDir := filepath.Join(base, "remote.git")
	workDir := filepath.Join(base, "work")
	fakeBinDir := filepath.Join(base, "bin")

	if err := os.MkdirAll(fakeBinDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fakeGH := `#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${FAKE_GH_ERROR:-}" ]]; then
  printf '%s\n' "$FAKE_GH_ERROR" >&2
  exit 1
fi
# Expect: gh api repos/<owner>/<repo>/releases/tags/<tag> --jq .tag_name
endpoint="${2:-}"
tag="${endpoint##*/tags/}"
for rel in ${FAKE_GH_RELEASES:-}; do
  if [[ "$rel" == "$tag" ]]; then
    printf '%s\n' "$tag"
    exit 0
  fi
done
printf 'gh: Not Found (HTTP 404)\n' >&2
exit 1
`
	if err := os.WriteFile(filepath.Join(fakeBinDir, "gh"), []byte(fakeGH), 0o755); err != nil {
		t.Fatal(err)
	}

	f := &releaseVersionFixture{
		t:          t,
		scriptPath: scriptPath,
		workDir:    workDir,
		remoteDir:  remoteDir,
		fakeBinDir: fakeBinDir,
	}
	f.git("", "init", "--bare", remoteDir)
	f.git("", "init", workDir)
	f.git(workDir, "config", "user.name", "Test")
	f.git(workDir, "config", "user.email", "test@example.com")
	f.git(workDir, "remote", "add", "origin", remoteDir)
	f.commitVersion(initialVersion, "initial "+initialVersion)
	return f
}

func (f *releaseVersionFixture) git(dir string, args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

func (f *releaseVersionFixture) commitVersion(version, msg string) string {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.workDir, "VERSION"), []byte(version+"\n"), 0o644); err != nil {
		f.t.Fatal(err)
	}
	f.git(f.workDir, "add", "VERSION")
	f.git(f.workDir, "commit", "--allow-empty", "-m", msg)
	return f.git(f.workDir, "rev-parse", "HEAD")
}

func (f *releaseVersionFixture) commitPatch(file, content, msg string) string {
	f.t.Helper()
	if err := os.WriteFile(filepath.Join(f.workDir, file), []byte(content), 0o644); err != nil {
		f.t.Fatal(err)
	}
	f.git(f.workDir, "add", file)
	f.git(f.workDir, "commit", "-m", msg)
	return f.git(f.workDir, "rev-parse", "HEAD")
}

func (f *releaseVersionFixture) pushTag(tag, commit string, annotated bool) {
	f.t.Helper()
	if annotated {
		f.git(f.workDir, "tag", "-a", tag, "-m", "release "+tag, commit)
	} else {
		f.git(f.workDir, "tag", tag, commit)
	}
	f.git(f.workDir, "push", "origin", "refs/tags/"+tag)
}

func (f *releaseVersionFixture) run(releases []string, ghErr string, extraArgs ...string) (map[string]string, string, error) {
	f.t.Helper()
	args := append([]string{
		"--repo-root", f.workDir,
		"--repo", "optimiware/kodema",
	}, extraArgs...)
	cmd := exec.Command(f.scriptPath, args...)
	cmd.Env = append(os.Environ(),
		"PATH="+f.fakeBinDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_GH_RELEASES="+strings.Join(releases, " "),
		"FAKE_GH_ERROR="+ghErr,
	)
	out, err := cmd.CombinedOutput()
	text := string(out)
	if err != nil {
		return nil, text, err
	}
	values := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if line == "" {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			f.t.Fatalf("unexpected output line %q in:\n%s", line, text)
		}
		values[key] = val
	}
	return values, text, nil
}

func assertResolved(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for k, wantVal := range want {
		if got[k] != wantVal {
			t.Errorf("%s = %q, want %q (all=%v)", k, got[k], wantVal, got)
		}
	}
}

func TestReleaseVersionScript(t *testing.T) {
	t.Run("automatic patch increments sequentially without release commit", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		baseCommit := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", baseCommit, false)

		// First patch merge: VERSION stays 0.8.2 -> resolves 0.8.3.
		patch1 := f.commitPatch("a.txt", "fix 1\n", "fix: first patch")
		got, out, err := f.run([]string{"v0.8.2"}, "", "--commit", patch1, "--before", baseCommit, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":          "0.8.3",
			"BASE_VERSION":     "0.8.2",
			"PREVIOUS_VERSION": "0.8.2",
			"RELEASE_MODE":     "patch",
			"PUBLISH":          "true",
			"PRERELEASE":       "false",
		})
		// Simulate GitHub creating an annotated tag v0.8.3 on patch1.
		f.pushTag("v0.8.3", patch1, true)

		// Second patch merge: VERSION still 0.8.2 -> resolves 0.8.4.
		patch2 := f.commitPatch("b.txt", "fix 2\n", "fix: second patch")
		got, out, err = f.run([]string{"v0.8.2", "v0.8.3"}, "", "--commit", patch2, "--before", patch1, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":          "0.8.4",
			"BASE_VERSION":     "0.8.2",
			"PREVIOUS_VERSION": "0.8.3",
			"RELEASE_MODE":     "patch",
			"PUBLISH":          "true",
			"PRERELEASE":       "false",
		})
	})

	t.Run("numeric patch comparison handles double-digit patches and ignores prerelease tags", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)
		c9 := f.commitPatch("p9.txt", "9\n", "patch 9")
		f.pushTag("v0.8.9", c9, false)
		c10 := f.commitPatch("p10.txt", "10\n", "patch 10")
		f.pushTag("v0.8.10", c10, true)
		f.pushTag("v0.8.99-rc.1", c10, false)
		f.pushTag("v0.80.1", c10, false)

		c11 := f.commitPatch("p11.txt", "11\n", "patch 11")
		got, out, err := f.run([]string{"v0.8.2", "v0.8.9", "v0.8.10"}, "", "--commit", c11, "--before", c10, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":          "0.8.11",
			"PREVIOUS_VERSION": "0.8.10",
			"RELEASE_MODE":     "patch",
			"PUBLISH":          "true",
		})
	})

	t.Run("manual minor and major releases publish exact VERSION then resume auto-patching", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)
		p1 := f.commitPatch("p1.txt", "1\n", "patch")
		f.pushTag("v0.8.3", p1, false)

		// Manual minor release commit: 0.8.2 -> 0.9.0.
		minor := f.commitVersion("0.9.0", "Release 0.9.0")
		got, out, err := f.run([]string{"v0.8.2", "v0.8.3"}, "", "--commit", minor, "--before", p1, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":      "0.9.0",
			"BASE_VERSION": "0.9.0",
			"RELEASE_MODE": "manual",
			"PUBLISH":      "true",
			"PRERELEASE":   "false",
		})
		f.pushTag("v0.9.0", minor, false)

		// Patch after 0.9.0 -> 0.9.1.
		p2 := f.commitPatch("p2.txt", "2\n", "fix after 0.9.0")
		got, out, err = f.run([]string{"v0.8.2", "v0.8.3", "v0.9.0"}, "", "--commit", p2, "--before", minor, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":          "0.9.1",
			"PREVIOUS_VERSION": "0.9.0",
			"RELEASE_MODE":     "patch",
			"PUBLISH":          "true",
		})

		// Manual major release commit: 0.9.0 -> 1.0.0.
		major := f.commitVersion("1.0.0", "Release 1.0.0")
		got, out, err = f.run([]string{"v0.9.0", "v0.9.1"}, "", "--commit", major, "--before", p2, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":      "1.0.0",
			"RELEASE_MODE": "manual",
			"PUBLISH":      "true",
			"PRERELEASE":   "false",
		})
	})

	t.Run("follow-up fix on unreleased manual release finishes that manual version", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.9.0")
		first := f.git(f.workDir, "rev-parse", "HEAD")
		// v0.9.0 failed CI before publishing; a follow-up fix lands without touching VERSION.
		fix := f.commitPatch("fix.txt", "stabilize test\n", "fix: stabilize test")
		got, out, err := f.run(nil, "", "--commit", fix, "--before", first, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":      "0.9.0",
			"RELEASE_MODE": "manual",
			"PUBLISH":      "true",
		})
	})

	t.Run("refuses to re-release an already-released commit when publishing", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)
		patch1 := f.commitPatch("a.txt", "fix\n", "fix")
		f.pushTag("v0.8.3", patch1, true)

		_, out, err := f.run([]string{"v0.8.2", "v0.8.3"}, "", "--commit", patch1, "--explicit-publish")
		if err == nil {
			t.Fatalf("expected error for already-released commit, got:\n%s", out)
		}
		if !strings.Contains(out, "already released as v0.8.3") {
			t.Errorf("unexpected error message: %s", out)
		}

		// Rehearsal (without --publish) succeeds without publishing.
		got, out, err := f.run([]string{"v0.8.2", "v0.8.3"}, "", "--commit", patch1)
		if err != nil {
			t.Fatalf("rehearsal on released commit failed: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION": "0.8.3",
			"PUBLISH": "false",
		})
	})

	t.Run("resumes incomplete publication when tag is on commit without GitHub release", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)
		patch1 := f.commitPatch("a.txt", "fix\n", "fix")
		f.pushTag("v0.8.3", patch1, false)

		got, out, err := f.run([]string{"v0.8.2"}, "", "--commit", patch1, "--explicit-publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":          "0.8.3",
			"PREVIOUS_VERSION": "0.8.2",
			"RELEASE_MODE":     "patch",
			"PUBLISH":          "true",
		})
	})

	t.Run("refuses explicit VERSION change to an already-released version", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)
		p1 := f.commitPatch("a.txt", "fix\n", "fix")
		f.pushTag("v0.8.3", p1, false)

		bad := f.commitVersion("0.8.3", "stale release commit for 0.8.3")
		_, out, err := f.run([]string{"v0.8.2", "v0.8.3"}, "", "--commit", bad, "--before", p1, "--publish")
		if err == nil {
			t.Fatalf("expected error when VERSION explicitly changed to already-released 0.8.3, got:\n%s", out)
		}
		if !strings.Contains(out, "already released") {
			t.Errorf("unexpected error message: %s", out)
		}
	})

	t.Run("development versions skip on push and fail on explicit publish", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.9.0-dev")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")

		got, out, err := f.run(nil, "", "--commit", c0, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":      "0.9.0-dev",
			"RELEASE_MODE": "dev",
			"PUBLISH":      "false",
			"PRERELEASE":   "true",
		})

		_, out, err = f.run(nil, "", "--commit", c0, "--explicit-publish")
		if err == nil {
			t.Fatalf("expected error on explicit publish of -dev, got:\n%s", out)
		}
		if !strings.Contains(out, "development version") {
			t.Errorf("unexpected error message: %s", out)
		}
	})

	t.Run("prerelease publishes manually and skips unchanged follow-up commits", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)

		rc1 := f.commitVersion("0.9.0-rc.1", "Release 0.9.0-rc.1")
		got, out, err := f.run([]string{"v0.8.2"}, "", "--commit", rc1, "--before", c0, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":      "0.9.0-rc.1",
			"RELEASE_MODE": "manual",
			"PUBLISH":      "true",
			"PRERELEASE":   "true",
		})
		f.pushTag("v0.9.0-rc.1", rc1, false)

		// Follow-up commit without changing VERSION skips instead of auto-patching an rc.
		followup := f.commitPatch("rc.txt", "tweak\n", "fix during rc")
		got, out, err = f.run([]string{"v0.8.2", "v0.9.0-rc.1"}, "", "--commit", followup, "--before", rc1, "--publish")
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":      "0.9.0-rc.1",
			"RELEASE_MODE": "prerelease-unchanged",
			"PUBLISH":      "false",
			"PRERELEASE":   "true",
		})

		// Explicit publish still fails loudly when rc is already released.
		_, out, err = f.run([]string{"v0.8.2", "v0.9.0-rc.1"}, "", "--commit", followup, "--explicit-publish")
		if err == nil {
			t.Fatalf("expected error on explicit publish of released rc, got:\n%s", out)
		}
	})

	t.Run("rehearsal on a patch branch resolves next patch with publish false", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		f.pushTag("v0.8.2", c0, false)
		pr := f.commitPatch("pr.txt", "change\n", "fix: pr")

		got, out, err := f.run([]string{"v0.8.2"}, "", "--commit", pr)
		if err != nil {
			t.Fatalf("unexpected error: %v\n%s", err, out)
		}
		assertResolved(t, got, map[string]string{
			"VERSION":          "0.8.3",
			"PREVIOUS_VERSION": "0.8.2",
			"RELEASE_MODE":     "patch",
			"PUBLISH":          "false",
			"PRERELEASE":       "false",
		})
	})

	t.Run("fails closed when gh api returns a non-404 error", func(t *testing.T) {
		f := newReleaseVersionFixture(t, "0.8.2")
		c0 := f.git(f.workDir, "rev-parse", "HEAD")
		_, out, err := f.run(nil, fmt.Sprintf("gh: Bad Gateway (HTTP 502)"), "--commit", c0, "--publish")
		if err == nil {
			t.Fatalf("expected error on gh api 502, got:\n%s", out)
		}
		if !strings.Contains(out, "cannot tell whether") {
			t.Errorf("unexpected error message: %s", out)
		}
	})
}
