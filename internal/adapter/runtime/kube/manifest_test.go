// Kubernetes-specific manifest and preparation behavior: immutable activation
// ConfigMap, mutable credential Secret, tenant namespace ownership, post-restart
// conflict detection, controller-upgrade toolbox plan persistence, Job security
// context/policy defaults, and projected volume wiring (PVC workspace,
// ConfigMap/previous-outputs split, memory emptyDir action outputs, init-container
// toolbox).
//
// Shared container-runtime preparation invariants (activation bytes, task files,
// idempotency, ChangedIdempotencyContent, invalid input refusal, missing
// workspace refusal, WorkingDir, Entrypoint, env, labels) are verified in
// TestKubeConformance.
package kube

import (
	"context"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

func testSetup(t *testing.T) (*fakeCluster, *fakeClock, *Adapter) {
	t.Helper()
	fake := newFakeCluster()
	addTestNamespace(fake)
	addTestClaim(t, fake, testWorkspaceID())
	clock := newFakeClock()
	return fake, clock, newTestAdapter(t, fake, clock)
}

func prepareTestWorkload(t *testing.T, adapter *Adapter, request reconcile.PrepareRequest) reconcile.Preparation {
	t.Helper()
	preparation, err := adapter.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare returned error: %v", err)
	}
	return preparation
}

func startTestWorkload(t *testing.T, adapter *Adapter, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID) reconcile.StartResult {
	t.Helper()
	result, err := adapter.Start(context.Background(), reconcile.StartRequest{Identity: identity, WorkspaceID: workspaceID})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	return result
}

func TestToolboxPreparationSurvivesControllerUpgrade(t *testing.T) {
	fake, clock, _ := testSetup(t)
	config := testConfig(fake, clock)
	config.ToolboxImage = "registry.example/kodema@sha256:" + strings.Repeat("a", 64)
	config.GitURL = "https://git-a.example"
	before, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	request := testPrepareRequest()
	request.Outputs = map[id.ActionName]engine.Outputs{"clone": {"commit": "abc"}}
	request.Container.Outputs = []id.OutputKey{"result"}
	prepareTestWorkload(t, before, request)

	config.ToolboxImage = "registry.example/kodema@sha256:" + strings.Repeat("b", 64)
	config.GitURL = "https://git-b.example"
	after, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	prepareTestWorkload(t, after, request)
	startTestWorkload(t, after, request.Identity, request.WorkspaceID)
	spec := fake.jobsSnapshot()[0].Spec
	if spec.ToolboxImage != before.toolboxImage || spec.WorkingDir != "/workspace" {
		t.Fatalf("recovered Job toolbox=%q working directory=%q", spec.ToolboxImage, spec.WorkingDir)
	}
	if value, _ := testEnvValue(spec.Env, workspace.EnvGitURL); value != "https://git-a.example" {
		t.Errorf("prepared Git endpoint changed: %q", value)
	}
	// Verify init-container toolbox volume, memory emptyDir output volume, and
	// previous-outputs ConfigMap split.
	toolboxVol := testVolumeAt(t, spec, workspace.ToolboxRoot)
	if !toolboxVol.EmptyDir || !toolboxVol.ReadOnly {
		t.Errorf("toolbox volume = %+v; want read-only emptyDir", toolboxVol)
	}
	actionOutVol := testVolumeAt(t, spec, "/kodema/outputs/build")
	if !actionOutVol.EmptyDir || actionOutVol.ReadOnly || actionOutVol.MemoryBytes != workspace.OutputDirectoryBytes {
		t.Errorf("action-outputs volume = %+v; want rw memory emptyDir (%d bytes)", actionOutVol, workspace.OutputDirectoryBytes)
	}
	prevOutVol := testVolumeAt(t, spec, workspace.OutputRoot)
	if prevOutVol.ConfigMapName == "" || !prevOutVol.ReadOnly || len(prevOutVol.Items) != 1 || prevOutVol.Items[0].Path != "/kodema/outputs/clone/commit" {
		t.Errorf("previous-outputs volume = %+v; want read-only ConfigMap split", prevOutVol)
	}

	request.Container.Outputs = nil
	if _, err := after.Prepare(t.Context(), request); err == nil {
		t.Fatal("rebound a prepared action to different outputs")
	}
	config.ToolboxImage = "registry.example/kodema:latest"
	if _, err := New(config); err == nil {
		t.Fatal("accepted mutable toolbox image")
	}
	if _, terminated := terminatedExit(Pod{Containers: []ContainerStatus{{Name: "install-toolbox", Terminated: true}}}); terminated {
		t.Fatal("installer termination was interpreted as workload exit")
	}
}

func TestPrepareCreatesImmutableConfigMapAndMutableSecret(t *testing.T) {
	fake, _, adapter := testSetup(t)
	request := testPrepareRequest()
	prepareTestWorkload(t, adapter, request)

	configMaps := fake.configMapsSnapshot()
	if len(configMaps) != 1 {
		t.Fatalf("prepared %d configmaps; want 1", len(configMaps))
	}
	cm := configMaps[0]
	if cm.Namespace != testNamespace || !cm.Immutable {
		t.Fatalf("configmap namespace=%q immutable=%v; want (%q, true)", cm.Namespace, cm.Immutable, testNamespace)
	}
	if cm.Labels[LabelTenant] != string(testTenant()) || cm.Labels[LabelWorkspace] != string(request.WorkspaceID) || len(cm.Labels[LabelPreparation]) != 48 {
		t.Fatalf("configmap labels = %v; want tenant, workspace, and 48-char preparation digest", cm.Labels)
	}

	secrets := fake.secretsSnapshot()
	if len(secrets) != 1 {
		t.Fatalf("prepared %d secrets; want 1", len(secrets))
	}
	if secrets[0].Immutable {
		t.Fatal("credential secret is immutable; live refresh requires a mutable Secret")
	}
	if secrets[0].Labels[LabelWorkspace] != string(request.WorkspaceID) {
		t.Fatalf("credential secret workspace label = %q", secrets[0].Labels[LabelWorkspace])
	}
}

func TestPrepareRefusesForeignTenantNamespace(t *testing.T) {
	fake := newFakeCluster()
	fake.addNamespace(Namespace{Name: testNamespace, Labels: map[string]string{LabelTenant: "tenant-other"}})
	addTestClaim(t, fake, testWorkspaceID())
	adapter := newTestAdapter(t, fake, newFakeClock())

	_, err := adapter.Prepare(context.Background(), testPrepareRequest())
	class, ok := fault.ClassOf(err)
	if !ok || class != fault.Forbidden {
		t.Fatalf("foreign namespace refusal class = (%v, %v) for %v; want %q", class, ok, err, fault.Forbidden)
	}
	if len(fake.configMapsSnapshot()) != 0 || len(fake.secretsSnapshot()) != 0 || fake.jobCount() != 0 {
		t.Fatal("Prepare wrote resources into a foreign tenant namespace")
	}
}

func TestPreparePostRestartConflictAndRemoveCleanup(t *testing.T) {
	mutations := []struct {
		name      string
		wantClass fault.Class
		mutate    func(*fakeCluster, *reconcile.PrepareRequest)
	}{
		{
			name:      "changed commands",
			wantClass: fault.ChangedIdempotencyContent,
			mutate:    func(_ *fakeCluster, req *reconcile.PrepareRequest) { req.Container.Commands = []string{"echo changed"} },
		},
		{
			name:      "changed env",
			wantClass: fault.ChangedIdempotencyContent,
			mutate: func(_ *fakeCluster, req *reconcile.PrepareRequest) {
				req.Container.Env = []definition.EnvVar{{Name: "BUILD_MODE", Value: "debug"}}
			},
		},
		{
			name:      "changed workspace",
			wantClass: fault.Forbidden,
			mutate: func(cluster *fakeCluster, req *reconcile.PrepareRequest) {
				other := id.WorkspaceID("workspace-other")
				addTestClaim(t, cluster, other)
				req.WorkspaceID = other
			},
		},
	}
	for _, tc := range mutations {
		t.Run("after_restart_"+tc.name, func(t *testing.T) {
			cluster, clock, first := testSetup(t)
			base := testPrepareRequest()
			prepareTestWorkload(t, first, base)

			reconstructed := newTestAdapter(t, cluster, clock)
			secretPutsBefore := cluster.callCount("PutSecret")

			mutated := testPrepareRequest()
			mutated.Credential = "tok-rotated-attempt"
			tc.mutate(cluster, &mutated)

			_, retryErr := reconstructed.Prepare(context.Background(), mutated)
			if gotClass, ok := fault.ClassOf(retryErr); !ok || gotClass != tc.wantClass {
				t.Fatalf("Prepare retry class = (%v, %v) for %v; want %q", gotClass, ok, retryErr, tc.wantClass)
			}
			if cluster.callCount("PutSecret") != secretPutsBefore {
				t.Fatal("refused post-restart Prepare mutated credential Secret")
			}
		})
	}

	t.Run("confirmed Remove clears Adapter.prepared and deletes ConfigMap", func(t *testing.T) {
		cluster, _, adapter := testSetup(t)
		req := testPrepareRequest()
		prepareTestWorkload(t, adapter, req)

		removal, err := adapter.Remove(context.Background(), reconcile.RemoveRequest{
			Identity: req.Identity, WorkspaceID: req.WorkspaceID,
		})
		if err != nil || !removal.Removed {
			t.Fatalf("Remove = (%+v, %v); want Removed=true", removal, err)
		}
		adapter.mu.Lock()
		_, stillPrepared := adapter.prepared[req.Identity.WorkloadID]
		adapter.mu.Unlock()
		if stillPrepared || len(cluster.configMapsSnapshot()) != 0 {
			t.Fatalf("Remove left prepared=%v configMaps=%d", stillPrepared, len(cluster.configMapsSnapshot()))
		}
	})
}

func TestStartBuildsKubeJobSpecSecurityAndVolumes(t *testing.T) {
	fake, _, adapter := testSetup(t)
	request := testPrepareRequest()
	prepareTestWorkload(t, adapter, request)
	startTestWorkload(t, adapter, request.Identity, request.WorkspaceID)

	job := onlyJob(t, fake)
	spec := job.Spec

	if job.Namespace != testNamespace || job.Name == "" {
		t.Fatalf("job identity = (%q, %q)", job.Namespace, job.Name)
	}
	if job.Labels[LabelTenant] != string(testTenant()) || spec.PodLabels[LabelTenant] != string(testTenant()) {
		t.Errorf("tenant labels = job:%q pod:%q; want %q", job.Labels[LabelTenant], spec.PodLabels[LabelTenant], testTenant())
	}
	if got := job.Annotations[AnnotationIdempotencyKey]; got != request.Identity.IdempotencyKey {
		t.Errorf("job idempotency annotation = %q; want %q", got, request.Identity.IdempotencyKey)
	}
	if spec.Image != testImage {
		t.Errorf("job image = %q; want %q", spec.Image, testImage)
	}
	if spec.RestartPolicy != "Never" || spec.BackoffLimit != 0 || spec.PodReplacementPolicy != "Failed" {
		t.Errorf("replay policy = restart:%q backoff:%d replacement:%q; want Never/0/Failed",
			spec.RestartPolicy, spec.BackoffLimit, spec.PodReplacementPolicy)
	}
	if spec.TTLSecondsAfterFinished == nil || *spec.TTLSecondsAfterFinished != 600 {
		t.Errorf("job TTL = %v; want 600", spec.TTLSecondsAfterFinished)
	}
	if spec.AutomountServiceAccountToken || spec.HostNetwork {
		t.Errorf("security flags = automountSA:%v hostNetwork:%v; want both false",
			spec.AutomountServiceAccountToken, spec.HostNetwork)
	}
	wantResources := Resources{CPURequest: "100m", CPULimit: "1", MemoryRequest: "64Mi", MemoryLimit: "256Mi"}
	if spec.Resources != wantResources {
		t.Errorf("job resources = %+v; want %+v", spec.Resources, wantResources)
	}

	wsVol := testVolumeAt(t, spec, workspace.WorkspaceRoot)
	if wsVol.ClaimName != testClaimName(t, request.WorkspaceID) || wsVol.ReadOnly {
		t.Errorf("workspace volume = %+v; want rw PVC", wsVol)
	}

	cm := fake.configMapsSnapshot()[0]
	actVol := testVolumeAt(t, spec, container.ActivationRoot)
	if !actVol.ReadOnly || actVol.ConfigMapName != cm.Name || len(actVol.Items) != len(cm.Data) {
		t.Fatalf("activation volume = %+v; want ro ConfigMap %q with %d items", actVol, cm.Name, len(cm.Data))
	}
	for _, item := range actVol.Items {
		if item.Mode&0o222 != 0 {
			t.Errorf("activation item %q mode %#o is writable", item.Path, item.Mode)
		}
	}

	sec := fake.secretsSnapshot()[0]
	credVol := testVolumeAt(t, spec, workspace.SecretRoot)
	if !credVol.ReadOnly || credVol.SecretName != sec.Name || len(credVol.Items) != 2 {
		t.Fatalf("credential volume = %+v; want ro Secret %q with 2 items", credVol, sec.Name)
	}
	for _, item := range credVol.Items {
		if item.Mode&0o222 != 0 {
			t.Errorf("credential item %q mode %#o is writable", item.Key, item.Mode)
		}
	}
}

func testEnvValue(env []EnvVar, name string) (string, bool) {
	for _, entry := range env {
		if entry.Name == name {
			return entry.Value, true
		}
	}
	return "", false
}

func testVolumeAt(t *testing.T, spec JobSpec, mountPath string) Volume {
	t.Helper()
	for _, volume := range spec.Volumes {
		if volume.MountPath == mountPath {
			return volume
		}
	}
	t.Fatalf("job has no volume mounted at %q: %+v", mountPath, spec.Volumes)
	return Volume{}
}
