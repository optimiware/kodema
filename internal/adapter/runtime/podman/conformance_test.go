package podman

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/adapter/runtime/containertest"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
)

func TestPodmanConformance(t *testing.T) {
	containertest.Run(t, newPodmanConformanceFixture)
}

type podmanConformanceFixture struct {
	fx   *fixture
	logs map[reconcile.LogStream][]byte
}

func newPodmanConformanceFixture(t *testing.T, opts containertest.Options) containertest.Fixture {
	t.Helper()
	fx := newFixture(t)
	fx.request.Outputs = map[id.ActionName]engine.Outputs{"clone": {"commit": "abc"}}

	cfg := Config{
		Libpod:    fx.libpod,
		Files:     fx.files,
		Root:      fx.root,
		KodemaURL: testKodemaURL,
		GitURL:    opts.GitURL,
		Cleanup:   CleanupPolicy{Attempts: 2, Backoff: time.Second},
		Clock:     fx.clock,
	}
	if opts.WithCA {
		caFile := filepath.Join(t.TempDir(), "ca.crt")
		if err := os.WriteFile(caFile, generateTestCAPEM(t), 0o600); err != nil {
			t.Fatalf("writing test CA: %v", err)
		}
		cfg.WorkloadCAFile = caFile
	}
	if opts.WithToolbox {
		toolboxFile := filepath.Join(t.TempDir(), "kodema")
		if err := os.WriteFile(toolboxFile, []byte("toolbox-v1"), 0o755); err != nil {
			t.Fatalf("writing test toolbox: %v", err)
		}
		cfg.ToolboxFile = toolboxFile
	}
	adapter, err := New(cfg)
	if err != nil {
		t.Fatalf("New conformance podman adapter: %v", err)
	}
	fx.adapter = adapter
	return &podmanConformanceFixture{
		fx:   fx,
		logs: map[reconcile.LogStream][]byte{},
	}
}

func (p *podmanConformanceFixture) Client() runtime.Client         { return p.fx.adapter }
func (p *podmanConformanceFixture) Workspace() reconcile.Workspace { return p.fx.adapter }
func (p *podmanConformanceFixture) KodemaURL() string              { return testKodemaURL }
func (p *podmanConformanceFixture) Request() reconcile.PrepareRequest {
	req := p.fx.request
	return req
}

func (p *podmanConformanceFixture) InspectLaunched(t *testing.T, identity reconcile.EffectIdentity) containertest.LaunchedWorkload {
	t.Helper()
	specs := p.fx.libpod.createSpecs()
	if len(specs) == 0 {
		t.Fatal("no container has been created")
	}
	spec := specs[len(specs)-1]

	envMap := make(map[string]string, len(spec.Env))
	for _, entry := range spec.Env {
		k, v, _ := strings.Cut(entry, "=")
		envMap[k] = v
	}

	activationFiles := map[string][]byte{}
	kodemaRoot := activationDir(p.fx.root, identity.WorkloadID)
	_ = filepath.WalkDir(kodemaRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(kodemaRoot, path)
		if relErr != nil {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("reading activation file %q: %v", path, readErr)
		}
		activationFiles["/kodema/"+filepath.ToSlash(rel)] = data
		return nil
	})

	secretFiles := map[string][]byte{}
	secRoot := secretsDir(p.fx.root, identity.WorkloadID)
	if entries, err := os.ReadDir(secRoot); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			data, readErr := os.ReadFile(filepath.Join(secRoot, entry.Name()))
			if readErr != nil {
				t.Fatalf("reading secret file %q: %v", entry.Name(), readErr)
			}
			secretFiles[entry.Name()] = data
		}
	}

	out := containertest.LaunchedWorkload{
		WorkingDir:      spec.WorkingDir,
		Command:         append([]string(nil), spec.Entrypoint...),
		Env:             envMap,
		EnvList:         append([]string(nil), spec.Env...),
		Labels:          spec.Labels,
		ActivationFiles: activationFiles,
		SecretFiles:     secretFiles,
		CacheMounts:     map[string]string{},
	}
	for _, m := range spec.Mounts {
		switch {
		case m.Destination == WorkloadCAMountPath && m.ReadOnly:
			out.HasCAMount = true
			out.CAMountPath = WorkloadCAPath
		case m.Destination == workspace.ToolboxRoot && m.ReadOnly:
			out.HasToolboxMount = true
		case strings.HasPrefix(m.Destination, workspace.OutputRoot+"/") && !m.ReadOnly:
			out.HasOutputDir = true
			out.OutputDirPath = m.Destination
		case strings.HasPrefix(m.Destination, workspace.CacheRoot+"/"):
			name := strings.TrimPrefix(m.Destination, workspace.CacheRoot+"/")
			out.CacheMounts[name] = m.Destination
		}
	}
	return out
}

func (p *podmanConformanceFixture) SeedRunning(t *testing.T, identity reconcile.EffectIdentity) {
	t.Helper()
	name, err := ContainerName(identity.WorkloadID)
	if err != nil {
		t.Fatalf("ContainerName: %v", err)
	}
	if cur, ok := p.fx.libpod.container(name); ok {
		cur.Status = ContainerStatusRunning
		cur.Started = true
		p.fx.libpod.setContainer(cur)
		return
	}
	p.fx.seedRunning(t)
}

func (p *podmanConformanceFixture) SeedExited(t *testing.T, identity reconcile.EffectIdentity, exitCode int) {
	t.Helper()
	name, err := ContainerName(identity.WorkloadID)
	if err != nil {
		t.Fatalf("ContainerName: %v", err)
	}
	if cur, ok := p.fx.libpod.container(name); ok {
		cur.Status = ContainerStatusExited
		cur.Started = true
		cur.ExitCode = exitCode
		cur.WholeStopped = true
		p.fx.libpod.setContainer(cur)
		return
	}
	p.fx.seedExited(t, exitCode, true)
}

func (p *podmanConformanceFixture) AppendLog(t *testing.T, identity reconcile.EffectIdentity, stream reconcile.LogStream, data []byte) {
	t.Helper()
	name, err := ContainerName(identity.WorkloadID)
	if err != nil {
		t.Fatalf("ContainerName: %v", err)
	}
	p.logs[stream] = append(p.logs[stream], data...)
	p.fx.libpod.setLogs(name, stream, p.logs[stream])
}

func (p *podmanConformanceFixture) BreakRedactionSource(t *testing.T, identity reconcile.EffectIdentity) {
	t.Helper()
	if err := os.RemoveAll(secretsParentDir(p.fx.root, identity.WorkloadID)); err != nil {
		t.Fatalf("removing secrets dir: %v", err)
	}
}

func (p *podmanConformanceFixture) RemoveWorkspace(t *testing.T, ws id.WorkspaceID) {
	t.Helper()
	vol, err := WorkspaceVolumeName(ws)
	if err != nil {
		t.Fatalf("WorkspaceVolumeName: %v", err)
	}
	p.fx.libpod.deleteVolume(vol)
}

func (p *podmanConformanceFixture) AddWorkspace(t *testing.T, ws id.WorkspaceID) {
	t.Helper()
	vol, err := WorkspaceVolumeName(ws)
	if err != nil {
		t.Fatalf("WorkspaceVolumeName: %v", err)
	}
	p.fx.libpod.addVolume(vol, map[string]string{LabelWorkspace: string(ws)})
}

func (p *podmanConformanceFixture) ReadCredential(t *testing.T, identity reconcile.EffectIdentity) string {
	t.Helper()
	return string(readHostFile(t, credentialHostPath(p.fx.root, identity.WorkloadID)))
}

func (p *podmanConformanceFixture) LaunchCount() int {
	return p.fx.libpod.callsFor("CreateContainer")
}

func (p *podmanConformanceFixture) ObjectCount() int {
	return p.fx.libpod.containerCount()
}
