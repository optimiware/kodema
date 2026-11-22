package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestAppsImportGraphIsPublicOnly proves that every first-party app package
// under internal/apps uses only neutral packages, the public API/client
// contracts and shared apps/kit helpers. Even when linked into the same
// `kodema` binary as the Control Plane composition, they never import engine,
// control, adapter, transport, server or installation code.
func TestAppsImportGraphIsPublicOnly(t *testing.T) {
	root := repositoryRoot(t)
	command := exec.Command("go", "list", "-deps", "./internal/apps/...")
	command.Dir = root
	command.Env = os.Environ()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, output)
	}
	forbidden := []string{
		"github.com/optimiware/kodema/internal/engine",
		"github.com/optimiware/kodema/internal/control",
		"github.com/optimiware/kodema/internal/adapter",
		"github.com/optimiware/kodema/internal/transport",
		"github.com/optimiware/kodema/internal/server",
		"github.com/optimiware/kodema/internal/installation",
	}
	found := make(map[string]bool)
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		found[line] = true
		for _, prefix := range forbidden {
			if line == prefix || strings.HasPrefix(line, prefix+"/") {
				t.Errorf("apps import graph contains forbidden package %q", line)
			}
		}
	}
	for _, expected := range []string{
		"github.com/optimiware/kodema/client",
		"github.com/optimiware/kodema/internal/apps/auth",
		"github.com/optimiware/kodema/internal/apps/git",
		"github.com/optimiware/kodema/internal/apps/agent",
		"github.com/optimiware/kodema/internal/apps/cli",
	} {
		if !found[expected] {
			t.Errorf("go list did not report expected package %q", expected)
		}
	}
}

// TestKodemaProductDoesNotImportTestSupport proves the product binary,
// including the durable test profile, never links a test harness package:
// the test entry is composed from product adapters only.
func TestKodemaProductDoesNotImportTestSupport(t *testing.T) {
	root := repositoryRoot(t)
	command := exec.Command("go", "list", "-deps", "./cmd/kodema")
	command.Dir = root
	command.Env = os.Environ()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, output)
	}
	found := false
	for line := range strings.SplitSeq(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if line == "github.com/optimiware/kodema/test" || strings.HasPrefix(line, "github.com/optimiware/kodema/test/") {
			t.Errorf("kodema imports test package %q", line)
		}
		if line == "github.com/optimiware/kodema/internal/server/drivers/podman" {
			found = true
		}
	}
	if !found {
		t.Error("go list did not report the Podman driver the test profile composes")
	}
}

// repositoryRoot walks up from the test working directory to the module root.
func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find the repository root")
		}
		directory = parent
	}
}
