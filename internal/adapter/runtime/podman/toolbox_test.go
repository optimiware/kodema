// Podman-specific toolbox behavior: shared <root>/.toolbox/<digest>/kodema
// host installation (0555), empty /kodema/bin and /kodema/outputs/<action>
// mount points, bounded tmpfs output mount, tamper detection, configuration
// validation, and survival after workload Remove.
//
// Shared WorkingDir, launcher Entrypoint, and Git env invariants are verified
// in TestPodmanConformance.
package podman

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

const testGitURL = "https://host.containers.internal:8443/git"

func toolboxFixture(t *testing.T, binary string) (*fixture, string) {
	t.Helper()
	source := filepath.Join(t.TempDir(), "kodema")
	if err := os.WriteFile(source, []byte(binary), 0o755); err != nil {
		t.Fatal(err)
	}
	fx := newFixture(t)
	fx.request.Outputs = map[id.ActionName]engine.Outputs{"clone": {"commit": "abc"}}
	fx.adapter = fx.newToolboxAdapter(t, source)
	return fx, source
}

func (f *fixture) newToolboxAdapter(t *testing.T, source string) *Adapter {
	t.Helper()
	adapter, err := New(Config{
		Libpod:      f.libpod,
		Files:       f.files,
		Root:        f.root,
		KodemaURL:   testKodemaURL,
		ToolboxFile: source,
		GitURL:      testGitURL,
		Cleanup:     CleanupPolicy{Attempts: 2},
		Clock:       f.clock,
	})
	if err != nil {
		t.Fatalf("New with a toolbox: %v", err)
	}
	return adapter
}

func (f *fixture) createdSpec(t *testing.T) ContainerSpec {
	t.Helper()
	specs := f.libpod.createSpecs()
	if len(specs) != 1 {
		t.Fatalf("created containers = %d; want 1", len(specs))
	}
	return specs[0]
}

func mountAt(spec ContainerSpec, destination string) (Mount, bool) {
	for _, mount := range spec.Mounts {
		if mount.Destination == destination {
			return mount, true
		}
	}
	return Mount{}, false
}

func TestToolboxIsInstalledOnceAndMountedReadOnly(t *testing.T) {
	fx, _ := toolboxFixture(t, "toolbox-v1")
	fx.prepare(t)
	fx.start(t)
	spec := fx.createdSpec(t)

	mount, ok := mountAt(spec, "/kodema/bin")
	if !ok || mount.Type != "bind" || !mount.ReadOnly {
		t.Fatalf("toolbox mount = %+v (ok=%v); want read-only bind", mount, ok)
	}
	if filepath.Dir(mount.Source) != filepath.Join(fx.root, ".toolbox") {
		t.Fatalf("toolbox source %q is not below %q/.toolbox", mount.Source, fx.root)
	}
	installed := filepath.Join(mount.Source, "kodema")
	if got := readHostFile(t, installed); string(got) != "toolbox-v1" {
		t.Fatalf("installed toolbox = %q; want toolbox-v1", got)
	}
	if mode := hostFileMode(t, installed); mode != 0o555 {
		t.Fatalf("installed toolbox mode = %o; want 0555", mode)
	}
	if info, err := os.Stat(fx.hostPath("/kodema/bin")); err != nil || !info.IsDir() {
		t.Fatalf("missing /kodema/bin mount point: %v", err)
	}
	if entries, _ := os.ReadDir(fx.hostPath("/kodema/bin")); len(entries) != 0 {
		t.Fatalf("/kodema/bin mount point holds %d entries; want empty", len(entries))
	}

	// Second workload reuses the shared copy without rewriting it.
	writes := fx.files.countPath("WriteFile", installed)
	second := fx.request
	second.Identity = reconcile.EffectIdentity{ExecutionID: "exec-1", ActionID: "action-2", WorkloadID: "workload-2", IdempotencyKey: "idem-2"}
	if _, err := fx.adapter.Prepare(context.Background(), second); err != nil {
		t.Fatalf("Prepare of second workload: %v", err)
	}
	if got := fx.files.countPath("WriteFile", installed); got != writes {
		t.Fatalf("toolbox was written %d more time(s) for a second workload", got-writes)
	}
}

func TestToolboxOutputTmpfsMount(t *testing.T) {
	fx, _ := toolboxFixture(t, "toolbox-v1")
	fx.prepare(t)
	fx.start(t)
	spec := fx.createdSpec(t)

	outputs, ok := mountAt(spec, "/kodema/outputs/build")
	if !ok || outputs.Type != "tmpfs" || outputs.ReadOnly || outputs.SizeBytes != 1<<20 {
		t.Fatalf("output mount = %+v (ok=%v); want rw tmpfs bounded to 1 MiB", outputs, ok)
	}
	if info, err := os.Stat(fx.hostPath("/kodema/outputs/build")); err != nil || !info.IsDir() {
		t.Fatalf("missing /kodema/outputs/build mount point: %v", err)
	}
}

func TestPrepareRefusesToolboxReplacedSinceStartup(t *testing.T) {
	fx, source := toolboxFixture(t, "before")
	if err := os.WriteFile(source, []byte("after"), 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := fx.adapter.Prepare(t.Context(), fx.request)
	assertFaultClass(t, err, fault.DependencyUnavailable)
	if hostFileExists(t, filepath.Join(fx.adapter.toolboxDir, "kodema")) {
		t.Fatal("installed replacement bytes under the previous digest")
	}
}

func TestToolboxConfigurationIsRefusedWhenUnusable(t *testing.T) {
	directory := t.TempDir()
	usable := filepath.Join(directory, "kodema")
	if err := os.WriteFile(usable, []byte("toolbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, testCase := range []struct {
		name    string
		toolbox string
		gitURL  string
	}{
		{name: "relative toolbox path", toolbox: "kodema"},
		{name: "missing toolbox file", toolbox: filepath.Join(directory, "absent")},
		{name: "toolbox directory", toolbox: directory},
		{name: "plain-http Git URL", toolbox: usable, gitURL: "http://git.example.com"},
		{name: "Git URL with user info", toolbox: usable, gitURL: "https://user@git.example.com"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := New(Config{
				Libpod:      newFakeLibpod(),
				Files:       newFakeFiles(),
				Root:        t.TempDir(),
				KodemaURL:   testKodemaURL,
				ToolboxFile: testCase.toolbox,
				GitURL:      testCase.gitURL,
				Cleanup:     CleanupPolicy{Attempts: 1},
				Clock:       newFakeClock(),
			})
			assertFaultClass(t, err, fault.InvalidInput)
		})
	}
}

func TestRemoveKeepsTheInstalledToolbox(t *testing.T) {
	fx, _ := toolboxFixture(t, "toolbox-v1")
	fx.prepare(t)
	installed := filepath.Join(fx.adapter.toolboxDir, "kodema")
	if _, err := fx.adapter.Remove(context.Background(), reconcile.RemoveRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if hostFileExists(t, fx.workloadRoot()) {
		t.Fatal("Remove left the workload's materialization")
	}
	if !hostFileExists(t, installed) {
		t.Fatal("Remove deleted the installed toolbox")
	}
}
