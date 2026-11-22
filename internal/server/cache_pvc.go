// Persistent named cache PVC provisioning for the Kubernetes runtime port.
//
// Like retained workspace claims, the runtimekube.Cluster seam deliberately has
// no claim-create method: the Kubernetes runtime adapter only inspects and
// mounts a cache PVC that already exists. This decorator provisions the
// deterministic owned cache PersistentVolumeClaim for each declared cache
// before delegating Prepare.
//
// Caches are explicitly non-authoritative (KODEMA.md): provisioning failures
// such as a terminating PVC, a foreign claim at the target name, or a transient
// API error never fail preparation or execution. The decorator logs a safe
// non-secret warning and delegates to the runtime adapter, which verifies
// ownership and degrades any unusable cache cleanly to emptyDir. Context
// cancellation and caller deadlines are always respected and propagated.
package server

import (
	"context"
	"log/slog"
	"strings"

	shellruntime "github.com/optimiware/kodema/internal/adapter/runtime"
	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/engine"
	kernelengine "github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	driverkube "github.com/optimiware/kodema/internal/server/drivers/kube"
)

// CacheClaimProvisioner is the one external effect the Kubernetes cache PVC
// runtime decorator adds: the idempotent creation of deterministic owned
// persistent cache PersistentVolumeClaims. It is satisfied by the real
// *drivers/kube.Cluster.
type CacheClaimProvisioner interface {
	CreateCacheClaim(ctx context.Context, spec driverkube.CacheClaimSpec) (runtimekube.Claim, error)
}

var _ CacheClaimProvisioner = (*driverkube.Cluster)(nil)

// CachePVCConfig is the trusted persistent cache claim template for one
// Kubernetes tenant namespace. Every value comes from composition configuration
// and is never derived from a request, a principal, or a rendered workload.
type CachePVCConfig struct {
	// Namespace is the tenant namespace that owns cache claims. Required.
	Namespace string
	// TenantID is the tenant ownership label value. Required and validated.
	TenantID id.TenantID
	// StorageClass is the requested StorageClass. Empty selects the cluster's
	// default StorageClass.
	StorageClass string
	// Size is the requested storage quantity, for example "2Gi". Required.
	Size string
	// AccessMode is one of the four Kubernetes PVC access modes. Required.
	AccessMode string
}

// cachePVCConfigFromWorkspace derives a persistent cache claim template from an
// existing WorkspacePVC template so every Kubernetes installation automatically
// provisions per-cache PVCs using its already-configured namespace, tenant,
// storage class, size, and access mode.
func cachePVCConfigFromWorkspace(template WorkspacePVC) CachePVCConfig {
	return CachePVCConfig{
		Namespace:    template.Namespace,
		TenantID:     template.TenantID,
		StorageClass: template.StorageClass,
		Size:         template.Size,
		AccessMode:   template.AccessMode,
	}
}

// validateCachePVCConfig refuses an incomplete or unusable trusted cache claim
// template before any adapter or external effect exists.
func validateCachePVCConfig(config CachePVCConfig) error {
	if strings.TrimSpace(config.Namespace) == "" {
		return fault.New(fault.InvalidInput, "cache claim template requires a namespace")
	}
	if _, err := id.ParseTenantID(string(config.TenantID)); err != nil {
		return fault.New(fault.InvalidInput, "cache claim template tenant id is invalid")
	}
	if config.StorageClass != "" && strings.TrimSpace(config.StorageClass) == "" {
		return fault.New(fault.InvalidInput, "cache claim template storage class must not be blank")
	}
	if strings.TrimSpace(config.Size) == "" {
		return fault.New(fault.InvalidInput, "cache claim template requires a storage size")
	}
	if strings.TrimSpace(config.AccessMode) == "" {
		return fault.New(fault.InvalidInput, "cache claim template requires an access mode")
	}
	if !workspaceClaimAccessModes[config.AccessMode] {
		return fault.New(fault.InvalidInput, "cache claim template access mode is not supported")
	}
	return nil
}

// cacheProvisioningRuntime decorates a Kubernetes kernelengine.Runtime so every
// declared cache in Prepare has its deterministic PersistentVolumeClaim
// provisioned idempotently before the adapter prepares the Job. All other
// Runtime methods delegate unchanged.
type cacheProvisioningRuntime struct {
	inner       kernelengine.Runtime
	provisioner CacheClaimProvisioner
	config      CachePVCConfig
	logger      *slog.Logger
}

var _ kernelengine.Runtime = (*cacheProvisioningRuntime)(nil)

// newCacheProvisioningRuntime validates every dependency and the trusted cache
// PVC template and returns the runtime decorator.
func newCacheProvisioningRuntime(
	inner kernelengine.Runtime,
	provisioner CacheClaimProvisioner,
	config CachePVCConfig,
	logger *slog.Logger,
) (*cacheProvisioningRuntime, error) {
	if inner == nil {
		return nil, fault.New(fault.InvalidInput, "cache provisioning runtime requires a runtime adapter")
	}
	if provisioner == nil {
		return nil, fault.New(fault.InvalidInput, "cache provisioning runtime requires a cache claim provisioner")
	}
	if err := validateCachePVCConfig(config); err != nil {
		return nil, err
	}
	return &cacheProvisioningRuntime{
		inner:       inner,
		provisioner: provisioner,
		config:      config,
		logger:      logger,
	}, nil
}

func (r *cacheProvisioningRuntime) Capabilities(ctx context.Context) (kernelengine.Capabilities, error) {
	return r.inner.Capabilities(ctx)
}

func (r *cacheProvisioningRuntime) Prepare(ctx context.Context, request kernelengine.PrepareRequest) (kernelengine.Preparation, error) {
	if len(request.Caches) > 0 {
		if err := ctx.Err(); err != nil {
			return kernelengine.Preparation{}, err
		}
		projectID := shellruntime.ExecutionProject(ctx)
		for _, cache := range request.Caches {
			if err := ctx.Err(); err != nil {
				return kernelengine.Preparation{}, err
			}
			spec := driverkube.CacheClaimSpec{
				Namespace:    r.config.Namespace,
				TenantID:     r.config.TenantID,
				ProjectID:    projectID,
				CacheName:    cache.Name,
				CacheKey:     cache.Key,
				StorageClass: r.config.StorageClass,
				Size:         r.config.Size,
				AccessMode:   r.config.AccessMode,
			}
			if _, err := r.provisioner.CreateCacheClaim(ctx, spec); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return kernelengine.Preparation{}, ctxErr
				}
				// Caches are explicitly non-authoritative: degrade cleanly to
				// emptyDir in the adapter instead of failing preparation.
				if r.logger != nil {
					class, _ := fault.ClassOf(err)
					r.logger.WarnContext(ctx, "persistent cache claim provisioning degraded to emptyDir",
						slog.String("cache", cache.Name),
						slog.String("fault", string(class)),
					)
				}
			}
		}
		if err := ctx.Err(); err != nil {
			return kernelengine.Preparation{}, err
		}
	}
	return r.inner.Prepare(ctx, request)
}

func (r *cacheProvisioningRuntime) Start(ctx context.Context, request kernelengine.StartRequest) (kernelengine.StartResult, error) {
	return r.inner.Start(ctx, request)
}

func (r *cacheProvisioningRuntime) Observe(ctx context.Context, request kernelengine.ObserveRequest) (engine.Observation, error) {
	return r.inner.Observe(ctx, request)
}

func (r *cacheProvisioningRuntime) Logs(ctx context.Context, request kernelengine.LogsRequest) (kernelengine.LogBatch, error) {
	return r.inner.Logs(ctx, request)
}

func (r *cacheProvisioningRuntime) Stop(ctx context.Context, request kernelengine.StopRequest) (engine.StopEvidence, error) {
	return r.inner.Stop(ctx, request)
}

func (r *cacheProvisioningRuntime) Remove(ctx context.Context, request kernelengine.RemoveRequest) (kernelengine.Removal, error) {
	return r.inner.Remove(ctx, request)
}

func (r *cacheProvisioningRuntime) RefreshCredential(ctx context.Context, request kernelengine.CredentialRequest) (kernelengine.CredentialRefresh, error) {
	return r.inner.RefreshCredential(ctx, request)
}
