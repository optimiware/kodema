//go:build component

package kubernetesconformance

import (
	"fmt"
	"strings"
	"testing"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
	kubesupport "github.com/optimiware/kodema/test/support/infrastructure/kubernetes"
	runtimesupport "github.com/optimiware/kodema/test/support/infrastructure/runtime"
)

func cacheClaimYAML(name, cacheKey, slug string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
  labels:
    %s: %s
    %s: %s
    %s: %s
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: %s
  resources:
    requests:
      storage: 64Mi
`, name, testNamespace,
		runtimekube.LabelTenant, testTenant(),
		runtimekube.LabelCacheKey, cacheKey,
		runtimekube.LabelCache, slug,
		kubesupport.StorageClassName)
}

// TestJobDedicatedCachePVCPersistsAcrossExecutions proves that a dedicated
// per-(tenant, project, slug) cache PVC (kodema-cache-<key>) is mounted at its
// root without subPath, persists cached files across two distinct executions
// with separate workspaces, and survives workload Remove and Workspace.Release.
func TestJobDedicatedCachePVCPersistsAcrossExecutions(t *testing.T) {
	t.Parallel()
	ws1 := id.WorkspaceID("workspace-cache-1")
	ws2 := id.WorkspaceID("workspace-cache-2")
	fx, ctx := newFixture(t, ws1, fixtureOptions{withClaim: true})

	ws2ClaimName, err := runtimekube.WorkspaceClaimName(ws2)
	if err != nil {
		t.Fatalf("WorkspaceClaimName(ws2): %v", err)
	}
	if _, err := fx.Cluster.Apply(ctx, claimYAML(ws2ClaimName, ws2, "")); err != nil {
		t.Fatalf("apply ws2 claim: %v", err)
	}

	scope := tenancy.Scope{TenantID: testTenant(), ProjectID: id.ProjectID("proj-cache-comp")}
	caches, err := workspace.BuildCacheMounts(scope, []string{"go-mod"})
	if err != nil {
		t.Fatalf("BuildCacheMounts: %v", err)
	}
	cacheClaimName, err := runtimekube.CacheClaimName(caches[0].Key)
	if err != nil {
		t.Fatalf("CacheClaimName: %v", err)
	}
	if _, err := fx.Cluster.Apply(ctx, cacheClaimYAML(cacheClaimName, caches[0].Key, "go-mod")); err != nil {
		t.Fatalf("apply cache claim: %v", err)
	}

	// Execution 1 writes a marker file into /kodema/cache/go-mod.
	id1 := testIdentity("exec-cache-1", "act-cache-1", "workload-cache-1", "idem-cache-1")
	req1 := workloadRequest(id1, ws1, []string{
		`printf 'cached-from-exec-1\n' > /kodema/cache/go-mod/stamp`,
		`cat /kodema/cache/go-mod/stamp`,
	}, "tok-cache-1")
	req1.Container.Caches = []string{"go-mod"}
	req1.Caches = caches
	startPrepared(t, ctx, fx, req1)

	obs1 := waitForObservation(t, ctx, fx.Adapter, reconcile.ObserveRequest{Identity: id1, WorkspaceID: ws1}, engine.ObservationExited)
	if code, ok := obs1.ExitStatus(); !ok || code != 0 {
		t.Fatalf("exec 1 exit = (%d, %v); want (0, true)", code, ok)
	}
	if rem, err := fx.Adapter.Remove(ctx, reconcile.RemoveRequest{Identity: id1, WorkspaceID: ws1}); err != nil || !rem.Removed {
		t.Fatalf("Remove exec 1 = (%+v, %v); want removed", rem, err)
	}
	if err := fx.Adapter.Release(ctx, ws1); err != nil {
		t.Fatalf("Release ws1: %v", err)
	}
	// Persistent cache PVC must survive Remove and Workspace.Release.
	if _, err := fx.Client.GetClaim(ctx, testNamespace, cacheClaimName); err != nil {
		t.Fatalf("cache PVC %q disappeared after Remove/Release: %v", cacheClaimName, err)
	}

	// Execution 2 uses a fresh workspace and reads the persisted cache marker.
	id2 := testIdentity("exec-cache-2", "act-cache-2", "workload-cache-2", "idem-cache-2")
	req2 := workloadRequest(id2, ws2, []string{
		`cat /kodema/cache/go-mod/stamp`,
	}, "tok-cache-2")
	req2.Container.Caches = []string{"go-mod"}
	req2.Caches = caches
	startPrepared(t, ctx, fx, req2)

	obs2 := waitForObservation(t, ctx, fx.Adapter, reconcile.ObserveRequest{Identity: id2, WorkspaceID: ws2}, engine.ObservationExited)
	if code, ok := obs2.ExitStatus(); !ok || code != 0 {
		t.Fatalf("exec 2 exit = (%d, %v); want (0, true)", code, ok)
	}
	stdout, _ := runtimesupport.ReadAllLogs(t, ctx, fx.Adapter, id2, 1<<20)
	if !strings.Contains(stdout, "cached-from-exec-1") {
		t.Fatalf("exec 2 stdout = %q; want persisted cache marker", stdout)
	}
}
