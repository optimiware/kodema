package server

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	shellruntime "github.com/optimiware/kodema/internal/adapter/runtime"
	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/adapter/store/memory"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/engine"
	kernelengine "github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/installation"
	driverkube "github.com/optimiware/kodema/internal/server/drivers/kube"
)

type recordingCacheClaimProvisioner struct {
	recordingClaimProvisioner

	cacheMu     sync.Mutex
	cacheSpecs  []driverkube.CacheClaimSpec
	cacheLog    *[]string
	createCache func(context.Context, driverkube.CacheClaimSpec) (runtimekube.Claim, error)
}

func (p *recordingCacheClaimProvisioner) CreateCacheClaim(ctx context.Context, spec driverkube.CacheClaimSpec) (runtimekube.Claim, error) {
	p.cacheMu.Lock()
	p.cacheSpecs = append(p.cacheSpecs, spec)
	if p.cacheLog != nil {
		*p.cacheLog = append(*p.cacheLog, "create-cache:"+spec.CacheName)
	}
	fn := p.createCache
	p.cacheMu.Unlock()
	if fn != nil {
		return fn(ctx, spec)
	}
	name, _ := runtimekube.CacheClaimName(spec.CacheKey)
	return runtimekube.Claim{
		Name:      name,
		Namespace: spec.Namespace,
		Labels: map[string]string{
			runtimekube.LabelTenant:   string(spec.TenantID),
			runtimekube.LabelCacheKey: spec.CacheKey,
		},
	}, nil
}

func (p *recordingCacheClaimProvisioner) cacheRequested() []driverkube.CacheClaimSpec {
	p.cacheMu.Lock()
	defer p.cacheMu.Unlock()
	return append([]driverkube.CacheClaimSpec(nil), p.cacheSpecs...)
}

type spyRuntime struct {
	mu           sync.Mutex
	log          *[]string
	prepareCalls int
	lastPrepare  kernelengine.PrepareRequest
	lastProject  id.ProjectID
	delegated    []string
}

var _ kernelengine.Runtime = (*spyRuntime)(nil)

func (s *spyRuntime) Capabilities(context.Context) (kernelengine.Capabilities, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "Capabilities")
	s.mu.Unlock()
	return kernelengine.Capabilities{Names: []string{"container"}}, nil
}

func (s *spyRuntime) Prepare(ctx context.Context, req kernelengine.PrepareRequest) (kernelengine.Preparation, error) {
	s.mu.Lock()
	s.prepareCalls++
	s.lastPrepare = req
	s.lastProject = shellruntime.ExecutionProject(ctx)
	if s.log != nil {
		*s.log = append(*s.log, "inner-prepare")
	}
	s.mu.Unlock()
	return kernelengine.Preparation{WorkspaceID: req.WorkspaceID, Detail: "prepared"}, nil
}

func (s *spyRuntime) Start(_ context.Context, req kernelengine.StartRequest) (kernelengine.StartResult, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "Start")
	s.mu.Unlock()
	return kernelengine.StartResult{Outcome: kernelengine.StartAccepted, WorkloadID: req.Identity.WorkloadID}, nil
}

func (s *spyRuntime) Observe(context.Context, kernelengine.ObserveRequest) (engine.Observation, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "Observe")
	s.mu.Unlock()
	return engine.Observation{Kind: engine.ObservationRunning}, nil
}

func (s *spyRuntime) Logs(context.Context, kernelengine.LogsRequest) (kernelengine.LogBatch, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "Logs")
	s.mu.Unlock()
	return kernelengine.LogBatch{}, nil
}

func (s *spyRuntime) Stop(context.Context, kernelengine.StopRequest) (engine.StopEvidence, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "Stop")
	s.mu.Unlock()
	return engine.StopEvidence{}, nil
}

func (s *spyRuntime) Remove(context.Context, kernelengine.RemoveRequest) (kernelengine.Removal, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "Remove")
	s.mu.Unlock()
	return kernelengine.Removal{Removed: true}, nil
}

func (s *spyRuntime) RefreshCredential(context.Context, kernelengine.CredentialRequest) (kernelengine.CredentialRefresh, error) {
	s.mu.Lock()
	s.delegated = append(s.delegated, "RefreshCredential")
	s.mu.Unlock()
	return kernelengine.CredentialRefresh{Delivered: true}, nil
}

func testCachePrepareRequest(caches ...kernelengine.CacheMount) kernelengine.PrepareRequest {
	return kernelengine.PrepareRequest{
		Identity: kernelengine.EffectIdentity{
			ExecutionID:    "exec-cache-1",
			ActionID:       "act-cache-1",
			WorkloadID:     "wl-cache-1",
			IdempotencyKey: "idem-cache-1",
		},
		WorkspaceID: "ws-cache-1",
		ActionName:  "build",
		Caches:      caches,
	}
}

func TestCacheProvisioningRuntimeProvisionsAllCachesBeforeInnerPrepare(t *testing.T) {
	var order []string
	inner := &spyRuntime{log: &order}
	provisioner := &recordingCacheClaimProvisioner{cacheLog: &order}
	template := kubeTestTemplate()

	wrapped, err := newCacheProvisioningRuntime(inner, provisioner, cachePVCConfigFromWorkspace(template), nil)
	if err != nil {
		t.Fatalf("newCacheProvisioningRuntime: %v", err)
	}

	keyGo := "0123456789abcdef0123456789abcdef"
	keyNpm := "fedcba9876543210fedcba9876543210"
	req := testCachePrepareRequest(
		kernelengine.CacheMount{Name: "go-mod", Key: keyGo},
		kernelengine.CacheMount{Name: "npm", Key: keyNpm},
	)
	ctx := shellruntime.WithExecutionProject(context.Background(), "proj-alpha")

	prep, err := wrapped.Prepare(ctx, req)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if prep.WorkspaceID != req.WorkspaceID {
		t.Fatalf("Preparation = %+v; want workspace %q", prep, req.WorkspaceID)
	}

	wantOrder := []string{"create-cache:go-mod", "create-cache:npm", "inner-prepare"}
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("call order = %v; want %v", order, wantOrder)
	}

	wantSpecs := []driverkube.CacheClaimSpec{
		{
			Namespace:    template.Namespace,
			TenantID:     template.TenantID,
			ProjectID:    "proj-alpha",
			CacheName:    "go-mod",
			CacheKey:     keyGo,
			StorageClass: template.StorageClass,
			Size:         template.Size,
			AccessMode:   template.AccessMode,
		},
		{
			Namespace:    template.Namespace,
			TenantID:     template.TenantID,
			ProjectID:    "proj-alpha",
			CacheName:    "npm",
			CacheKey:     keyNpm,
			StorageClass: template.StorageClass,
			Size:         template.Size,
			AccessMode:   template.AccessMode,
		},
	}
	if got := provisioner.cacheRequested(); !reflect.DeepEqual(got, wantSpecs) {
		t.Fatalf("CreateCacheClaim specs = %#v; want %#v", got, wantSpecs)
	}
	if inner.lastProject != "proj-alpha" {
		t.Fatalf("inner saw project = %q; want proj-alpha", inner.lastProject)
	}
}

func TestCacheProvisioningRuntimeSkipsProvisionerWhenNoCaches(t *testing.T) {
	inner := &spyRuntime{}
	provisioner := &recordingCacheClaimProvisioner{}
	wrapped, err := newCacheProvisioningRuntime(inner, provisioner, cachePVCConfigFromWorkspace(kubeTestTemplate()), nil)
	if err != nil {
		t.Fatalf("newCacheProvisioningRuntime: %v", err)
	}

	if _, err := wrapped.Prepare(context.Background(), testCachePrepareRequest()); err != nil {
		t.Fatalf("Prepare without caches: %v", err)
	}
	if got := len(provisioner.cacheRequested()); got != 0 {
		t.Fatalf("CreateCacheClaim calls = %d; want 0 when no caches declared", got)
	}
	if inner.prepareCalls != 1 {
		t.Fatalf("inner Prepare calls = %d; want 1", inner.prepareCalls)
	}
}

func TestCacheProvisioningRuntimeDegradesGracefullyOnNonAuthoritativeFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "terminating cache PVC (VersionConflict)",
			err:  fault.Wrap(fault.VersionConflict, "cache claim is terminating", runtimekube.ErrConflict),
		},
		{
			name: "foreign/unowned PVC (Forbidden)",
			err:  fault.New(fault.Forbidden, "cache claim held by another owner"),
		},
		{
			name: "API quota or unavailable (DependencyUnavailable)",
			err:  fault.New(fault.DependencyUnavailable, "kubernetes rejected with status 403"),
		},
		{
			name: "transient ambiguous failure (UncertainExternalEffect)",
			err:  fault.Wrap(fault.UncertainExternalEffect, "transient API failure", driverkube.ErrAmbiguous),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			inner := &spyRuntime{}
			calls := 0
			provisioner := &recordingCacheClaimProvisioner{
				createCache: func(_ context.Context, spec driverkube.CacheClaimSpec) (runtimekube.Claim, error) {
					calls++
					if spec.CacheName == "go-mod" {
						return runtimekube.Claim{}, tc.err
					}
					return runtimekube.Claim{Name: "kodema-cache-" + spec.CacheKey, Namespace: spec.Namespace}, nil
				},
			}
			wrapped, err := newCacheProvisioningRuntime(inner, provisioner, cachePVCConfigFromWorkspace(kubeTestTemplate()), logger)
			if err != nil {
				t.Fatalf("newCacheProvisioningRuntime: %v", err)
			}

			req := testCachePrepareRequest(
				kernelengine.CacheMount{Name: "go-mod", Key: "0123456789abcdef0123456789abcdef"},
				kernelengine.CacheMount{Name: "npm", Key: "fedcba9876543210fedcba9876543210"},
			)
			if _, err := wrapped.Prepare(context.Background(), req); err != nil {
				t.Fatalf("Prepare failed on non-authoritative cache error: %v", err)
			}
			if calls != 2 {
				t.Fatalf("CreateCacheClaim calls = %d; want 2 (second cache still provisioned)", calls)
			}
			if inner.prepareCalls != 1 {
				t.Fatalf("inner Prepare calls = %d; want 1", inner.prepareCalls)
			}
			if !strings.Contains(logs.String(), "go-mod") {
				t.Fatalf("warning log = %q; want degraded cache name go-mod", logs.String())
			}
		})
	}
}

func TestCacheProvisioningRuntimePropagatesContextCancellation(t *testing.T) {
	t.Run("cancelled before Prepare", func(t *testing.T) {
		inner := &spyRuntime{}
		provisioner := &recordingCacheClaimProvisioner{}
		wrapped, err := newCacheProvisioningRuntime(inner, provisioner, cachePVCConfigFromWorkspace(kubeTestTemplate()), nil)
		if err != nil {
			t.Fatalf("newCacheProvisioningRuntime: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req := testCachePrepareRequest(kernelengine.CacheMount{Name: "go-mod", Key: "0123456789abcdef0123456789abcdef"})
		_, err = wrapped.Prepare(ctx, req)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Prepare error = %v; want context.Canceled", err)
		}
		if len(provisioner.cacheRequested()) != 0 {
			t.Fatalf("CreateCacheClaim called on already-cancelled context")
		}
		if inner.prepareCalls != 0 {
			t.Fatalf("inner Prepare called after cancellation")
		}
	})

	t.Run("cancelled during CreateCacheClaim", func(t *testing.T) {
		inner := &spyRuntime{}
		ctx, cancel := context.WithCancel(context.Background())
		provisioner := &recordingCacheClaimProvisioner{
			createCache: func(_ context.Context, _ driverkube.CacheClaimSpec) (runtimekube.Claim, error) {
				cancel()
				return runtimekube.Claim{}, fault.Wrap(fault.UncertainExternalEffect, "cancelled", context.Canceled)
			},
		}
		wrapped, err := newCacheProvisioningRuntime(inner, provisioner, cachePVCConfigFromWorkspace(kubeTestTemplate()), nil)
		if err != nil {
			t.Fatalf("newCacheProvisioningRuntime: %v", err)
		}

		req := testCachePrepareRequest(
			kernelengine.CacheMount{Name: "go-mod", Key: "0123456789abcdef0123456789abcdef"},
			kernelengine.CacheMount{Name: "npm", Key: "fedcba9876543210fedcba9876543210"},
		)
		_, err = wrapped.Prepare(ctx, req)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Prepare error = %v; want context.Canceled", err)
		}
		if got := len(provisioner.cacheRequested()); got != 1 {
			t.Fatalf("CreateCacheClaim calls = %d; want 1 (stopped on cancel)", got)
		}
		if inner.prepareCalls != 0 {
			t.Fatalf("inner Prepare called after context cancelled")
		}
	})
}

func TestCacheProvisioningRuntimeDelegatesOtherMethods(t *testing.T) {
	inner := &spyRuntime{}
	provisioner := &recordingCacheClaimProvisioner{}
	wrapped, err := newCacheProvisioningRuntime(inner, provisioner, cachePVCConfigFromWorkspace(kubeTestTemplate()), nil)
	if err != nil {
		t.Fatalf("newCacheProvisioningRuntime: %v", err)
	}

	ctx := context.Background()
	if caps, err := wrapped.Capabilities(ctx); err != nil || !caps.Supports("container") {
		t.Fatalf("Capabilities = (%v, %v)", caps, err)
	}
	if _, err := wrapped.Start(ctx, kernelengine.StartRequest{}); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := wrapped.Observe(ctx, kernelengine.ObserveRequest{}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if _, err := wrapped.Logs(ctx, kernelengine.LogsRequest{}); err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if _, err := wrapped.Stop(ctx, kernelengine.StopRequest{}); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, err := wrapped.Remove(ctx, kernelengine.RemoveRequest{}); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := wrapped.RefreshCredential(ctx, kernelengine.CredentialRequest{}); err != nil {
		t.Fatalf("RefreshCredential: %v", err)
	}

	want := []string{"Capabilities", "Start", "Observe", "Logs", "Stop", "Remove", "RefreshCredential"}
	if !reflect.DeepEqual(inner.delegated, want) {
		t.Fatalf("delegated methods = %v; want %v", inner.delegated, want)
	}
}

func TestCacheProvisioningRuntimeValidation(t *testing.T) {
	valid := cachePVCConfigFromWorkspace(kubeTestTemplate())
	if _, err := newCacheProvisioningRuntime(nil, &recordingCacheClaimProvisioner{}, valid, nil); err == nil {
		t.Fatal("expected refusal for nil inner runtime")
	}
	if _, err := newCacheProvisioningRuntime(&spyRuntime{}, nil, valid, nil); err == nil {
		t.Fatal("expected refusal for nil cache provisioner")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*CachePVCConfig)
	}{
		{"empty namespace", func(c *CachePVCConfig) { c.Namespace = "" }},
		{"invalid tenant", func(c *CachePVCConfig) { c.TenantID = "tenant bad" }},
		{"blank storage class", func(c *CachePVCConfig) { c.StorageClass = "   " }},
		{"empty size", func(c *CachePVCConfig) { c.Size = "" }},
		{"empty access mode", func(c *CachePVCConfig) { c.AccessMode = "" }},
		{"unsupported access mode", func(c *CachePVCConfig) { c.AccessMode = "Invalid" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			_, err := newCacheProvisioningRuntime(&spyRuntime{}, &recordingCacheClaimProvisioner{}, cfg, nil)
			if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("error = %v; want InvalidInput", err)
			}
		})
	}
}

func TestWireKubernetesRuntimeProvisionsCachePVCs(t *testing.T) {
	t.Run("single-tenant wraps runtime when ClaimProvisioner implements CacheClaimProvisioner", func(t *testing.T) {
		mem := memory.New()
		innerRuntime := &spyRuntime{}
		innerWorkspace := &recordingWorkspace{}
		provisioner := &recordingCacheClaimProvisioner{}
		template := kubeTestTemplate()

		rt, ws, err := wireKubernetesRuntime(mem.Controller(), innerRuntime, innerWorkspace, nil, provisioner, &template)
		if err != nil {
			t.Fatalf("wireKubernetesRuntime: %v", err)
		}
		if _, ok := rt.(*cacheProvisioningRuntime); !ok {
			t.Fatalf("runtime = %T; want *cacheProvisioningRuntime", rt)
		}
		if _, ok := ws.(*provisioningWorkspace); !ok {
			t.Fatalf("workspace = %T; want *provisioningWorkspace", ws)
		}

		ctx := shellruntime.WithExecutionProject(context.Background(), "proj-single")
		req := testCachePrepareRequest(kernelengine.CacheMount{Name: "go-mod", Key: "0123456789abcdef0123456789abcdef"})
		if _, err := rt.Prepare(ctx, req); err != nil {
			t.Fatalf("Prepare: %v", err)
		}
		specs := provisioner.cacheRequested()
		if len(specs) != 1 || specs[0].Namespace != template.Namespace || specs[0].TenantID != template.TenantID || specs[0].ProjectID != "proj-single" {
			t.Fatalf("CreateCacheClaim specs = %#v; want single spec for %s/%s proj-single", specs, template.Namespace, template.TenantID)
		}
	})

	t.Run("single-tenant leaves runtime unwrapped when ClaimProvisioner does not implement CacheClaimProvisioner", func(t *testing.T) {
		mem := memory.New()
		innerRuntime := &spyRuntime{}
		innerWorkspace := &recordingWorkspace{}
		provisioner := &recordingClaimProvisioner{}
		template := kubeTestTemplate()

		rt, _, err := wireKubernetesRuntime(mem.Controller(), innerRuntime, innerWorkspace, nil, provisioner, &template)
		if err != nil {
			t.Fatalf("wireKubernetesRuntime: %v", err)
		}
		if rt != innerRuntime {
			t.Fatalf("runtime = %T; want original unwrapped runtime", rt)
		}
	})

	t.Run("multi-tenant wraps each tenant runtime with its own WorkspacePVC", func(t *testing.T) {
		controller := &activationFactsStore{rows: []store.Row{
			{Table: "executions", Values: map[string]any{
				"id":           "exec-tenant-b",
				"tenant_id":    "tenant-b",
				"project_id":   "proj-beta",
				"workspace_id": "ws-tenant-b",
			}},
		}}

		tpl := func(tenant id.TenantID, ns, size string) *WorkspacePVC {
			v := kubeTestTemplate()
			v.TenantID = tenant
			v.Namespace = ns
			v.Size = size
			return &v
		}
		rtA, rtB := &spyRuntime{}, &spyRuntime{}
		wsA, wsB := &recordingWorkspace{}, &recordingWorkspace{}
		provisioner := &recordingCacheClaimProvisioner{}
		tenants := []ProductionTenant{
			{TenantID: "tenant-a", Runtime: rtA, Workspace: wsA, WorkspacePVC: tpl("tenant-a", "ns-tenant-a", "5Gi")},
			{TenantID: "tenant-b", Runtime: rtB, Workspace: wsB, WorkspacePVC: tpl("tenant-b", "ns-tenant-b", "10Gi")},
		}

		rt, _, err := wireKubernetesRuntime(controller, nil, nil, tenants, provisioner, nil)
		if err != nil {
			t.Fatalf("wireKubernetesRuntime multi-tenant: %v", err)
		}
		dispatcher, ok := rt.(*tenantRuntime)
		if !ok {
			t.Fatalf("runtime = %T; want *tenantRuntime", rt)
		}
		for _, tid := range []id.TenantID{"tenant-a", "tenant-b"} {
			if _, ok := dispatcher.tenants[tid].(*cacheProvisioningRuntime); !ok {
				t.Fatalf("tenant %s runtime = %T; want *cacheProvisioningRuntime", tid, dispatcher.tenants[tid])
			}
		}

		req := testCachePrepareRequest(kernelengine.CacheMount{Name: "cargo", Key: "0123456789abcdef0123456789abcdef"})
		req.Identity.ExecutionID = "exec-tenant-b"
		if _, err := rt.Prepare(context.Background(), req); err != nil {
			t.Fatalf("multi-tenant Prepare: %v", err)
		}
		specs := provisioner.cacheRequested()
		if len(specs) != 1 {
			t.Fatalf("CreateCacheClaim calls = %d; want 1", len(specs))
		}
		got := specs[0]
		if got.Namespace != "ns-tenant-b" || got.TenantID != "tenant-b" || got.ProjectID != "proj-beta" || got.Size != "10Gi" || got.CacheName != "cargo" {
			t.Fatalf("multi-tenant CreateCacheClaim spec = %+v; want ns-tenant-b/tenant-b/proj-beta/10Gi/cargo", got)
		}
		if rtB.prepareCalls != 1 || rtA.prepareCalls != 0 {
			t.Fatalf("inner prepares = (A:%d, B:%d); want (0, 1)", rtA.prepareCalls, rtB.prepareCalls)
		}
	})
}

func TestComposeKubernetesWiresCacheProvisioningRuntime(t *testing.T) {
	config := fullTestProfileConfig(t)
	config.Installation.Environment = installation.EnvironmentTest
	config.Installation.Runtime = installation.RuntimeKubernetes
	innerRuntime := &spyRuntime{}
	innerWorkspace := &recordingWorkspace{}
	provisioner := &recordingCacheClaimProvisioner{}
	template := kubeTestTemplate()

	config.Adapters.Runtime = innerRuntime
	config.Adapters.Workspace = innerWorkspace
	config.Adapters.WorkspaceActivation = &fakeActivation{tenant: kubeTestTenant}
	config.Adapters.ClaimProvisioner = provisioner
	config.WorkspacePVC = &template

	env, err := buildEnvironment(context.Background(), &builder{cfg: config, logger: config.logger()})
	if err != nil {
		t.Fatalf("buildEnvironment: %v", err)
	}
	if _, ok := env.runtime.(*cacheProvisioningRuntime); !ok {
		t.Fatalf("compose runtime = %T; want *cacheProvisioningRuntime", env.runtime)
	}

	ctx := shellruntime.WithExecutionProject(context.Background(), "proj-compose")
	req := testCachePrepareRequest(kernelengine.CacheMount{Name: "pip", Key: "0123456789abcdef0123456789abcdef"})
	if _, err := env.runtime.Prepare(ctx, req); err != nil {
		t.Fatalf("compose Prepare: %v", err)
	}
	specs := provisioner.cacheRequested()
	if len(specs) != 1 || specs[0].CacheName != "pip" || specs[0].ProjectID != "proj-compose" {
		t.Fatalf("compose CreateCacheClaim specs = %#v; want pip for proj-compose", specs)
	}
	if innerRuntime.prepareCalls != 1 {
		t.Fatalf("inner Prepare calls = %d; want 1", innerRuntime.prepareCalls)
	}
}
