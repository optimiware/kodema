package kube

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// CacheClaimSpec is the driver-owned provisioning request for one persistent
// named cache PersistentVolumeClaim.
//
// Like ClaimSpec for retained workspaces, this concrete, seam-external
// primitive lives strictly on *Cluster outside the runtimekube.Cluster seam:
// the runtime adapter only observes and mounts a cache claim that already
// exists (or degrades cleanly to emptyDir when absent or unusable).
//
// The object name is derived from CacheKey with the same deterministic
// CacheClaimName the adapter uses, so a retry or concurrent activation
// addresses the identical PVC. Workspace ownership and retention labels are
// never stamped on a cache claim.
type CacheClaimSpec struct {
	// Namespace is the tenant namespace that owns the claim. Required.
	Namespace string
	// TenantID is the tenant ownership label value. Required and validated.
	TenantID id.TenantID
	// ProjectID optionally identifies the owning project. When non-empty it is
	// validated as a project identity and stamped as LabelProject when it is
	// also a valid Kubernetes label value.
	ProjectID id.ProjectID
	// CacheName is the declared cache slug validated via
	// definition.ValidateCacheName. When it is also a valid Kubernetes label
	// value, it is stamped as the informational LabelCache label.
	CacheName string
	// CacheKey is the scoped cache key (32-hex digest) validated via
	// runtimekube.CacheClaimName. Required.
	CacheKey string
	// StorageClass is the requested StorageClass. Empty selects the cluster's
	// default StorageClass (storageClassName is omitted from the PVC spec).
	StorageClass string
	// Size is the requested storage quantity, for example "2Gi". Required.
	Size string
	// AccessMode is one of the four Kubernetes PVC access modes. Required.
	AccessMode string
}

// CreateCacheClaim provisions the deterministic PersistentVolumeClaim for one
// scoped named dependency cache.
//
// The driver creates first and never pre-reads, and never waits for the claim
// to become Bound (WaitForFirstConsumer StorageClasses bind only when a Job
// pod schedules). A 409 is reconciled with a GET and the claim is adopted only
// when its identity, tenant, and cache-key labels match exactly, it carries no
// workspace ownership label, and it is not terminating. A foreign,
// differently-owned, workspace-owned, or unlabeled claim is Forbidden and is
// never overwritten; a terminating claim is a VersionConflict. A 2xx response
// whose identity or ownership labels do not describe the requested cache claim
// stays ErrAmbiguous.
func (c *Cluster) CreateCacheClaim(ctx context.Context, spec CacheClaimSpec) (runtimekube.Claim, error) {
	manifest, claimName, err := buildCacheClaimManifest(spec)
	if err != nil {
		return runtimekube.Claim{}, err
	}
	if err := ctx.Err(); err != nil {
		return runtimekube.Claim{}, ambiguous(
			"kubernetes create cache claim was cancelled before any request", err)
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return runtimekube.Claim{}, fault.New(fault.InternalFailure,
			"kubernetes cache claim manifest is not encodable")
	}
	data, status, err := c.exchange(ctx, http.MethodPost, claimsPath(spec.Namespace), nil, payload, jsonContentType)
	if err != nil {
		return runtimekube.Claim{}, err
	}
	switch {
	case status == http.StatusConflict:
		// The object exists. Never let statusError classify this as
		// ErrAlreadyExists: the retry must reconcile the existing claim.
		return c.adoptCacheClaim(ctx, claimName, spec)
	case status < http.StatusOK || status >= http.StatusMultipleChoices:
		return runtimekube.Claim{}, statusError("create cache claim", status, false)
	}
	claim, err := decodeClaim(data, "create cache claim")
	if err != nil {
		return runtimekube.Claim{}, err
	}
	if reason := cacheClaimOwnershipReason(claim, claimName, spec); reason != "" {
		return runtimekube.Claim{}, ambiguous(
			"kubernetes create cache claim returned a different claim: " + reason)
	}
	return claim, nil
}

// adoptCacheClaim reconciles one cache claim create conflict. The existing
// claim is adopted only when it is owned by the requested tenant and cache key,
// does not carry a workspace ownership label, and is not terminating.
func (c *Cluster) adoptCacheClaim(ctx context.Context, claimName string, spec CacheClaimSpec) (runtimekube.Claim, error) {
	observed, err := c.GetClaim(ctx, spec.Namespace, claimName)
	if err != nil {
		if errors.Is(err, ErrAmbiguous) {
			return runtimekube.Claim{}, err
		}
		return runtimekube.Claim{}, ambiguous(
			"kubernetes create cache claim conflicted and the existing claim is unreadable")
	}
	if reason := cacheClaimOwnershipReason(observed, claimName, spec); reason != "" {
		return runtimekube.Claim{}, fault.New(fault.Forbidden,
			"kubernetes cache claim is already held by another owner: "+reason)
	}
	if observed.DeletionTimestamp != nil {
		return runtimekube.Claim{}, fault.Wrap(fault.VersionConflict,
			"kubernetes cache claim is terminating and cannot be adopted", runtimekube.ErrConflict)
	}
	return observed, nil
}

// buildCacheClaimManifest validates one cache provisioning request and renders
// the exact REST body before any API request is sent.
func buildCacheClaimManifest(spec CacheClaimSpec) (claimCreateBody, string, error) {
	if strings.TrimSpace(spec.Namespace) == "" {
		return claimCreateBody{}, "", fault.New(fault.InvalidInput,
			"kubernetes cache claim requires a namespace")
	}
	if _, err := id.ParseTenantID(string(spec.TenantID)); err != nil {
		return claimCreateBody{}, "", fault.Wrap(fault.InvalidInput,
			"kubernetes cache claim tenant id is invalid", err)
	}
	if spec.ProjectID != "" {
		if _, err := id.ParseProjectID(string(spec.ProjectID)); err != nil {
			return claimCreateBody{}, "", fault.Wrap(fault.InvalidInput,
				"kubernetes cache claim project id is invalid", err)
		}
	}
	if err := definition.ValidateCacheName(spec.CacheName); err != nil {
		return claimCreateBody{}, "", err
	}
	claimName, err := runtimekube.CacheClaimName(spec.CacheKey)
	if err != nil {
		return claimCreateBody{}, "", err
	}
	if spec.StorageClass != "" && strings.TrimSpace(spec.StorageClass) == "" {
		return claimCreateBody{}, "", fault.New(fault.InvalidInput,
			"kubernetes cache claim storage class must not be blank")
	}
	if strings.TrimSpace(spec.Size) == "" {
		return claimCreateBody{}, "", fault.New(fault.InvalidInput,
			"kubernetes cache claim requires a storage size")
	}
	if !claimAccessModes[spec.AccessMode] {
		return claimCreateBody{}, "", fault.New(fault.InvalidInput,
			"kubernetes cache claim access mode is not supported")
	}

	labels := map[string]string{
		runtimekube.LabelTenant:   string(spec.TenantID),
		runtimekube.LabelCacheKey: spec.CacheKey,
	}
	if spec.ProjectID != "" && validKubeLabelValue(string(spec.ProjectID)) {
		labels[runtimekube.LabelProject] = string(spec.ProjectID)
	}
	if validKubeLabelValue(spec.CacheName) {
		labels[runtimekube.LabelCache] = spec.CacheName
	}

	manifest := claimCreateBody{
		APIVersion: "v1",
		Kind:       "PersistentVolumeClaim",
		Metadata: objectMetaBody{
			Name:      claimName,
			Namespace: spec.Namespace,
			Labels:    labels,
		},
		Spec: claimSpecBody{
			AccessModes: []string{spec.AccessMode},
			Resources: claimResourcesBody{
				Requests: map[string]string{"storage": spec.Size},
			},
			StorageClassName: spec.StorageClass,
			VolumeMode:       "Filesystem",
		},
	}
	return manifest, claimName, nil
}

// cacheClaimOwnershipReason explains why a returned claim does not describe
// the requested cache claim. An empty reason means it matches.
func cacheClaimOwnershipReason(claim runtimekube.Claim, claimName string, spec CacheClaimSpec) string {
	switch {
	case claim.Name != claimName:
		return "claim name does not match the requested claim"
	case claim.Namespace != spec.Namespace:
		return "claim namespace does not match the requested namespace"
	case claim.Labels[runtimekube.LabelTenant] != string(spec.TenantID):
		return "claim is not owned by this tenant"
	case claim.Labels[runtimekube.LabelCacheKey] != spec.CacheKey:
		return "claim does not carry the requested cache key"
	case hasLabel(claim.Labels, runtimekube.LabelWorkspace):
		return "claim is owned by a workspace"
	default:
		return ""
	}
}

func hasLabel(labels map[string]string, key string) bool {
	_, ok := labels[key]
	return ok
}

// validKubeLabelValue reports whether value satisfies the Kubernetes label
// value grammar: 1..63 chars matching ^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$.
func validKubeLabelValue(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	isAlnum := func(c byte) bool {
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}
	if !isAlnum(value[0]) || !isAlnum(value[len(value)-1]) {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		c := value[index]
		if !isAlnum(c) && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}
