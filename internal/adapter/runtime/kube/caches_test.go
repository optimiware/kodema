package kube

import (
	"context"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
)

func TestKubeAdapter_NamedCacheMountsEmptyDirPVCAndEvictionDegradation(t *testing.T) {
	ctx := context.Background()
	projA := tenancy.Scope{TenantID: testTenant(), ProjectID: id.ProjectID("proj-a")}
	cachesA, err := workspace.BuildCacheMounts(projA, []string{"go-mod"})
	if err != nil {
		t.Fatalf("BuildCacheMounts: %v", err)
	}

	t.Run("default emptyDir mount at /kodema/cache/<name>", func(t *testing.T) {
		cluster := newFakeCluster()
		addTestNamespace(cluster)
		addTestClaim(t, cluster, testWorkspaceID())
		adapter := newTestAdapter(t, cluster, newFakeClock())

		req := testPrepareRequest()
		req.Container.Caches = []string{"go-mod"}
		req.Caches = cachesA

		if _, err := adapter.Prepare(ctx, req); err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		job := cacheTestOnlyJob(t, cluster)
		found := false
		for _, v := range job.Spec.Volumes {
			if v.MountPath == "/kodema/cache/go-mod" {
				if !v.EmptyDir || v.ReadOnly || v.ClaimName != "" {
					t.Fatalf("unexpected default cache volume: %+v", v)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("missing /kodema/cache/go-mod volume in job spec: %+v", job.Spec.Volumes)
		}
	})

	t.Run("configured shared cache PVC mounts with tenant/project subPath", func(t *testing.T) {
		cluster := newFakeCluster()
		addTestNamespace(cluster)
		addTestClaim(t, cluster, testWorkspaceID())
		cluster.addClaim(Claim{
			Name:      "kodema-shared-cache",
			Namespace: testNamespace,
		})
		cfg := testConfig(cluster, newFakeClock())
		cfg.CacheClaimName = "kodema-shared-cache"
		adapter, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		req := testPrepareRequest()
		req.Container.Caches = []string{"go-mod"}
		req.Caches = cachesA

		if _, err := adapter.Prepare(ctx, req); err != nil {
			t.Fatalf("Prepare with shared cache PVC: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID}); err != nil {
			t.Fatalf("Start with shared cache PVC: %v", err)
		}
		job := cacheTestOnlyJob(t, cluster)
		found := false
		for _, v := range job.Spec.Volumes {
			if v.MountPath == "/kodema/cache/go-mod" {
				if v.ClaimName != "kodema-shared-cache" || v.SubPath != cachesA[0].Key || v.ReadOnly || v.EmptyDir {
					t.Fatalf("unexpected shared PVC cache volume: %+v", v)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("missing shared PVC cache volume in job spec: %+v", job.Spec.Volumes)
		}
	})

	t.Run("missing/evicted cache PVC degrades cleanly to emptyDir without failing", func(t *testing.T) {
		cluster := newFakeCluster()
		addTestNamespace(cluster)
		addTestClaim(t, cluster, testWorkspaceID())
		// Deliberately do NOT seed kodema-evicted-cache.
		cfg := testConfig(cluster, newFakeClock())
		cfg.CacheClaimName = "kodema-evicted-cache"
		adapter, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		req := testPrepareRequest()
		req.Container.Caches = []string{"go-mod"}
		req.Caches = cachesA

		if _, err := adapter.Prepare(ctx, req); err != nil {
			t.Fatalf("Prepare failed on missing non-authoritative cache PVC: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID}); err != nil {
			t.Fatalf("Start failed on missing non-authoritative cache PVC: %v", err)
		}
		job := cacheTestOnlyJob(t, cluster)
		found := false
		for _, v := range job.Spec.Volumes {
			if v.MountPath == "/kodema/cache/go-mod" {
				if !v.EmptyDir || v.ClaimName != "" || v.ReadOnly {
					t.Fatalf("expected emptyDir fallback for missing cache PVC, got: %+v", v)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("missing fallback cache volume in job spec: %+v", job.Spec.Volumes)
		}
	})

	t.Run("dedicated owned per-slug cache PVC mounts at root without subPath", func(t *testing.T) {
		cluster := newFakeCluster()
		addTestNamespace(cluster)
		addTestClaim(t, cluster, testWorkspaceID())
		claimName, err := CacheClaimName(cachesA[0].Key)
		if err != nil {
			t.Fatalf("CacheClaimName: %v", err)
		}
		cluster.addClaim(Claim{
			Name:      claimName,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelTenant:   string(testTenant()),
				LabelCacheKey: cachesA[0].Key,
				LabelCache:    "go-mod",
			},
		})
		adapter := newTestAdapter(t, cluster, newFakeClock())

		req := testPrepareRequest()
		req.Container.Caches = []string{"go-mod"}
		req.Caches = cachesA

		if _, err := adapter.Prepare(ctx, req); err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		job := cacheTestOnlyJob(t, cluster)
		found := false
		for _, v := range job.Spec.Volumes {
			if v.MountPath == "/kodema/cache/go-mod" {
				if v.ClaimName != claimName || v.SubPath != "" || v.ReadOnly || v.EmptyDir {
					t.Fatalf("unexpected dedicated PVC cache volume: %+v; want ClaimName=%q SubPath='' ReadOnly=false EmptyDir=false", v, claimName)
				}
				found = true
			}
		}
		if !found {
			t.Fatalf("missing dedicated PVC cache volume in job spec: %+v", job.Spec.Volumes)
		}
	})

	t.Run("distinct projects declaring the same slug mount distinct cache PVCs", func(t *testing.T) {
		projB := tenancy.Scope{TenantID: testTenant(), ProjectID: id.ProjectID("proj-b")}
		cachesB, err := workspace.BuildCacheMounts(projB, []string{"go-mod"})
		if err != nil {
			t.Fatalf("BuildCacheMounts(proj-b): %v", err)
		}
		claimA, err := CacheClaimName(cachesA[0].Key)
		if err != nil {
			t.Fatalf("CacheClaimName A: %v", err)
		}
		claimB, err := CacheClaimName(cachesB[0].Key)
		if err != nil {
			t.Fatalf("CacheClaimName B: %v", err)
		}
		if claimA == claimB {
			t.Fatalf("distinct projects produced identical cache claim name %q", claimA)
		}

		cluster := newFakeCluster()
		addTestNamespace(cluster)
		wsA := id.WorkspaceID("workspace-proj-a")
		wsB := id.WorkspaceID("workspace-proj-b")
		addTestClaim(t, cluster, wsA)
		addTestClaim(t, cluster, wsB)
		cluster.addClaim(Claim{
			Name:      claimA,
			Namespace: testNamespace,
			Labels:    map[string]string{LabelTenant: string(testTenant()), LabelCacheKey: cachesA[0].Key},
		})
		cluster.addClaim(Claim{
			Name:      claimB,
			Namespace: testNamespace,
			Labels:    map[string]string{LabelTenant: string(testTenant()), LabelCacheKey: cachesB[0].Key},
		})
		adapter := newTestAdapter(t, cluster, newFakeClock())

		reqA := testPrepareRequest()
		reqA.WorkspaceID = wsA
		reqA.Identity.WorkloadID = "workload-proj-a"
		reqA.Container.Caches = []string{"go-mod"}
		reqA.Caches = cachesA

		reqB := testPrepareRequest()
		reqB.WorkspaceID = wsB
		reqB.Identity.WorkloadID = "workload-proj-b"
		reqB.Container.Caches = []string{"go-mod"}
		reqB.Caches = cachesB

		if _, err := adapter.Prepare(ctx, reqA); err != nil {
			t.Fatalf("Prepare A: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: reqA.Identity, WorkspaceID: wsA}); err != nil {
			t.Fatalf("Start A: %v", err)
		}
		if _, err := adapter.Prepare(ctx, reqB); err != nil {
			t.Fatalf("Prepare B: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: reqB.Identity, WorkspaceID: wsB}); err != nil {
			t.Fatalf("Start B: %v", err)
		}

		jobA := cluster.jobMustGet(t, testNamespace, workloadJobName(reqA.Identity.WorkloadID))
		jobB := cluster.jobMustGet(t, testNamespace, workloadJobName(reqB.Identity.WorkloadID))
		volA := testVolumeAt(t, jobA.Spec, "/kodema/cache/go-mod")
		volB := testVolumeAt(t, jobB.Spec, "/kodema/cache/go-mod")
		if volA.ClaimName != claimA || volA.SubPath != "" || volB.ClaimName != claimB || volB.SubPath != "" {
			t.Fatalf("project cache mounts = (%+v, %+v); want claims (%q, %q) with empty SubPath", volA, volB, claimA, claimB)
		}
	})

	t.Run("unowned workspace-labelled or terminating PVC at kodema-cache-<key> degrades to emptyDir", func(t *testing.T) {
		claimName, err := CacheClaimName(cachesA[0].Key)
		if err != nil {
			t.Fatalf("CacheClaimName: %v", err)
		}
		now := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
		badClaims := []struct {
			name  string
			claim Claim
		}{
			{
				name: "foreign tenant",
				claim: Claim{
					Name:      claimName,
					Namespace: testNamespace,
					Labels:    map[string]string{LabelTenant: "tenant-other", LabelCacheKey: cachesA[0].Key},
				},
			},
			{
				name: "wrong cache key",
				claim: Claim{
					Name:      claimName,
					Namespace: testNamespace,
					Labels:    map[string]string{LabelTenant: string(testTenant()), LabelCacheKey: "deadbeefdeadbeefdeadbeefdeadbeef"},
				},
			},
			{
				name: "workspace-labelled claim",
				claim: Claim{
					Name:      claimName,
					Namespace: testNamespace,
					Labels: map[string]string{
						LabelTenant:    string(testTenant()),
						LabelCacheKey:  cachesA[0].Key,
						LabelWorkspace: string(testWorkspaceID()),
					},
				},
			},
			{
				name: "terminating claim",
				claim: Claim{
					Name:              claimName,
					Namespace:         testNamespace,
					Labels:            map[string]string{LabelTenant: string(testTenant()), LabelCacheKey: cachesA[0].Key},
					DeletionTimestamp: &now,
				},
			},
		}
		for _, tc := range badClaims {
			t.Run(tc.name, func(t *testing.T) {
				cluster := newFakeCluster()
				addTestNamespace(cluster)
				addTestClaim(t, cluster, testWorkspaceID())
				cluster.addClaim(tc.claim)
				adapter := newTestAdapter(t, cluster, newFakeClock())

				req := testPrepareRequest()
				req.Container.Caches = []string{"go-mod"}
				req.Caches = cachesA

				if _, err := adapter.Prepare(ctx, req); err != nil {
					t.Fatalf("Prepare(%s): %v", tc.name, err)
				}
				if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID}); err != nil {
					t.Fatalf("Start(%s): %v", tc.name, err)
				}
				job := cacheTestOnlyJob(t, cluster)
				v := testVolumeAt(t, job.Spec, "/kodema/cache/go-mod")
				if !v.EmptyDir || v.ClaimName != "" || v.SubPath != "" || v.ReadOnly {
					t.Fatalf("%s mounted forbidden PVC instead of emptyDir fallback: %+v", tc.name, v)
				}
			})
		}
	})

	t.Run("Remove and Workspace.Release never delete dedicated cache PVCs", func(t *testing.T) {
		cluster := newFakeCluster()
		addTestNamespace(cluster)
		addTestClaim(t, cluster, testWorkspaceID())
		claimName, err := CacheClaimName(cachesA[0].Key)
		if err != nil {
			t.Fatalf("CacheClaimName: %v", err)
		}
		cluster.addClaim(Claim{
			Name:      claimName,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelTenant:   string(testTenant()),
				LabelCacheKey: cachesA[0].Key,
			},
		})
		adapter := newTestAdapter(t, cluster, newFakeClock())

		req := testPrepareRequest()
		req.Container.Caches = []string{"go-mod"}
		req.Caches = cachesA
		if _, err := adapter.Prepare(ctx, req); err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		if _, err := adapter.Start(ctx, reconcile.StartRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		cluster.setPods([]Pod{testTerminatedPod(req.Identity, "Succeeded", 0)})

		removal, err := adapter.Remove(ctx, reconcile.RemoveRequest{Identity: req.Identity, WorkspaceID: req.WorkspaceID})
		if err != nil || !removal.Removed {
			t.Fatalf("Remove = (%+v, %v); want Removed=true", removal, err)
		}
		if !cluster.claimExists(testNamespace, claimName) {
			t.Fatalf("Remove deleted cache PVC %q", claimName)
		}
		if err := adapter.Release(ctx, req.WorkspaceID); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if cluster.claimExists(testNamespace, testClaimName(t, req.WorkspaceID)) {
			t.Fatal("Release did not delete workspace claim")
		}
		if !cluster.claimExists(testNamespace, claimName) {
			t.Fatalf("Workspace.Release deleted persistent cache PVC %q", claimName)
		}
	})
}

func cacheTestOnlyJob(t *testing.T, cluster *fakeCluster) Job {
	t.Helper()
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if len(cluster.jobs) != 1 {
		t.Fatalf("jobs count = %d; want 1", len(cluster.jobs))
	}
	for _, job := range cluster.jobs {
		return job
	}
	t.Fatal("unreachable")
	return Job{}
}
