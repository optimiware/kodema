package podman

import (
	"context"
	"testing"

	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
)

// TestPodmanCacheVolumeNamingAndMounts pins Podman-specific named volume
// mounting (Type: "volume", Source: CacheVolumeName(key), rw) and cross-project
// cache volume name isolation.
func TestPodmanCacheVolumeNamingAndMounts(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)

	projA := tenancy.Scope{TenantID: id.TenantID("t1"), ProjectID: id.ProjectID("proj-a")}
	projB := tenancy.Scope{TenantID: id.TenantID("t1"), ProjectID: id.ProjectID("proj-b")}

	cachesA, err := workspace.BuildCacheMounts(projA, []string{"go-mod"})
	if err != nil {
		t.Fatalf("BuildCacheMounts(projA): %v", err)
	}
	cachesB, err := workspace.BuildCacheMounts(projB, []string{"go-mod"})
	if err != nil {
		t.Fatalf("BuildCacheMounts(projB): %v", err)
	}

	fx.request.Container.Caches = []string{"go-mod"}
	fx.request.Caches = cachesA
	if _, err := fx.adapter.Prepare(ctx, fx.request); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if _, err := fx.adapter.Start(ctx, reconcile.StartRequest{
		Identity:    fx.request.Identity,
		WorkspaceID: fx.request.WorkspaceID,
	}); err != nil {
		t.Fatalf("Start: %v", err)
	}

	wantVolA, _ := CacheVolumeName(cachesA[0].Key)
	wantVolB, _ := CacheVolumeName(cachesB[0].Key)
	if wantVolA == wantVolB {
		t.Fatalf("cross-project Podman cache volume collision: %q", wantVolA)
	}

	mount, ok := mountAt(fx.createdSpec(t), "/kodema/cache/go-mod")
	if !ok || mount.Type != "volume" || mount.ReadOnly || mount.Source != wantVolA {
		t.Fatalf("cache mount = %+v (ok=%v); want rw volume %q", mount, ok, wantVolA)
	}
}
