package kube

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/adapter/runtime/containertest"
	"github.com/optimiware/kodema/internal/adapter/runtime/kube/logframe"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
)

func TestKubeConformance(t *testing.T) {
	containertest.Run(t, newKubeConformanceFixture)
}

type kubeConformanceFixture struct {
	fake    *fakeCluster
	clock   *fakeClock
	adapter *Adapter
	logs    []byte
}

func newKubeConformanceFixture(t *testing.T, opts containertest.Options) containertest.Fixture {
	t.Helper()
	fake := newFakeCluster()
	addTestNamespace(fake)
	addTestClaim(t, fake, testWorkspaceID())
	clock := newFakeClock()

	cfg := testConfig(fake, clock)
	if opts.WithCA {
		cfg.WorkloadCA = WorkloadCAConfig{SecretName: testWorkloadCASecret}
	}
	if opts.WithToolbox {
		cfg.ToolboxImage = "registry.example.com/tools/kodema@sha256:" + strings.Repeat("a", 64)
	}
	if opts.GitURL != "" {
		cfg.GitURL = opts.GitURL
	}
	if opts.WithCacheClaim {
		fake.addClaim(Claim{Name: "kodema-shared-cache", Namespace: testNamespace})
		cfg.CacheClaimName = "kodema-shared-cache"
	}
	adapter, err := New(cfg)
	if err != nil {
		t.Fatalf("New conformance kube adapter: %v", err)
	}
	return &kubeConformanceFixture{
		fake:    fake,
		clock:   clock,
		adapter: adapter,
	}
}

func (k *kubeConformanceFixture) Client() runtime.Client         { return k.adapter }
func (k *kubeConformanceFixture) Workspace() reconcile.Workspace { return k.adapter }
func (k *kubeConformanceFixture) KodemaURL() string              { return testKodemaURL }
func (k *kubeConformanceFixture) Request() reconcile.PrepareRequest {
	req := testPrepareRequest()
	req.Outputs = map[id.ActionName]engine.Outputs{"clone": {"commit": "abc"}}
	req.Container.Outputs = []id.OutputKey{"out-a"}
	return req
}

func (k *kubeConformanceFixture) InspectLaunched(t *testing.T, identity reconcile.EffectIdentity) containertest.LaunchedWorkload {
	t.Helper()
	job, ok := k.fake.jobSnapshot(testNamespace, workloadJobName(identity.WorkloadID))
	if !ok {
		t.Fatalf("job %q is absent", workloadJobName(identity.WorkloadID))
	}
	spec := job.Spec

	envMap := make(map[string]string, len(spec.Env))
	envList := make([]string, len(spec.Env))
	for i, e := range spec.Env {
		envMap[e.Name] = e.Value
		envList[i] = e.Name + "=" + e.Value
	}

	activationFiles := map[string][]byte{}
	if cm, err := k.fake.GetConfigMap(context.Background(), testNamespace, activationConfigMapName(identity.WorkloadID)); err == nil {
		for path, data := range cm.Data {
			if path == toolboxPlanKey {
				continue
			}
			activationFiles[path] = append([]byte(nil), data...)
		}
	}

	secretFiles := map[string][]byte{}
	if sec, err := k.fake.GetSecret(context.Background(), testNamespace, credentialSecretName(identity.WorkloadID)); err == nil {
		for name, data := range sec.Data {
			secretFiles[name] = append([]byte(nil), data...)
		}
	}

	for key, want := range job.Labels {
		if got := spec.PodLabels[key]; got != want {
			t.Fatalf("Job.Spec.PodLabels[%q] = %q; want %q (matching Job.Labels)", key, got, want)
		}
	}
	if len(spec.PodLabels) != len(job.Labels) {
		t.Fatalf("Job.Spec.PodLabels = %v; want same keys as Job.Labels = %v", spec.PodLabels, job.Labels)
	}

	out := containertest.LaunchedWorkload{
		WorkingDir:      spec.WorkingDir,
		Command:         append([]string(nil), spec.Command...),
		FrameStreams:    spec.ToolboxImage != "",
		Env:             envMap,
		EnvList:         envList,
		Labels:          job.Labels,
		ActivationFiles: activationFiles,
		SecretFiles:     secretFiles,
		CacheMounts:     map[string]string{},
	}
	for _, v := range spec.Volumes {
		switch {
		case v.MountPath == WorkloadCAMountPath && v.ReadOnly && len(v.Items) == 1:
			out.HasCAMount = true
			out.CAMountPath = v.Items[0].Path
		case v.MountPath == workspace.ToolboxRoot && v.ReadOnly:
			out.HasToolboxMount = true
		case strings.HasPrefix(v.MountPath, workspace.OutputRoot+"/") && !v.ReadOnly:
			out.HasOutputDir = true
			out.OutputDirPath = v.MountPath
		case strings.HasPrefix(v.MountPath, workspace.CacheRoot+"/"):
			name := strings.TrimPrefix(v.MountPath, workspace.CacheRoot+"/")
			out.CacheMounts[name] = v.MountPath
		}
	}
	return out
}

func (k *kubeConformanceFixture) SeedRunning(t *testing.T, identity reconcile.EffectIdentity) {
	t.Helper()
	k.fake.setPods([]Pod{testRunningPod(identity)})
}

func (k *kubeConformanceFixture) SeedExited(t *testing.T, identity reconcile.EffectIdentity, exitCode int) {
	t.Helper()
	phase := "Succeeded"
	if exitCode != 0 {
		phase = "Failed"
	}
	k.fake.setPods([]Pod{testTerminatedPod(identity, phase, exitCode)})
}

func (k *kubeConformanceFixture) AppendLog(t *testing.T, identity reconcile.EffectIdentity, stream reconcile.LogStream, data []byte) {
	t.Helper()
	pods := k.fake.podsSnapshot()
	if len(pods) == 0 {
		t.Fatal("AppendLog called with no pod seeded")
	}
	wireStream := logframe.StreamStdout
	if stream == reconcile.LogStderr {
		wireStream = logframe.StreamStderr
	}
	var buf bytes.Buffer
	w := logframe.NewWriter(&buf)
	if _, err := w.WriteStream(wireStream, data); err != nil {
		t.Fatalf("logframe.WriteStream: %v", err)
	}
	k.logs = append(k.logs, buf.Bytes()...)
	k.fake.setPodLogs(pods[0].Name, LogStreamCombined, k.logs)
}

func (k *kubeConformanceFixture) BreakRedactionSource(t *testing.T, identity reconcile.EffectIdentity) {
	t.Helper()
	if err := k.fake.DeleteSecret(context.Background(), testNamespace, credentialSecretName(identity.WorkloadID)); err != nil {
		t.Fatalf("DeleteSecret: %v", err)
	}
}

func (k *kubeConformanceFixture) RemoveWorkspace(t *testing.T, ws id.WorkspaceID) {
	t.Helper()
	k.fake.removeClaim(testNamespace, testClaimName(t, ws))
}

func (k *kubeConformanceFixture) AddWorkspace(t *testing.T, ws id.WorkspaceID) {
	t.Helper()
	addTestClaim(t, k.fake, ws)
}

func (k *kubeConformanceFixture) ReadCredential(t *testing.T, identity reconcile.EffectIdentity) string {
	t.Helper()
	sec, err := k.fake.GetSecret(context.Background(), testNamespace, credentialSecretName(identity.WorkloadID))
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	return string(sec.Data[id.SecretTokenName])
}

func (k *kubeConformanceFixture) LaunchCount() int {
	return k.fake.callCount("CreateJob")
}

func (k *kubeConformanceFixture) ObjectCount() int {
	return k.fake.jobCount()
}
