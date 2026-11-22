package server

// Fresh-only workspace provisioning tests for both Podman volume and
// Kubernetes PVC backends.
//
// The Kernel calls engine.Workspace.Retain before it commits an activation
// (internal/kernel/engine/reconcile.go activate). Both runtime adapters refuse
// an absent volume/claim ("retention never recreates it"), so the composition
// wraps engine.Workspace with a single fresh-only provisioningWorkspace
// decorator guarded by durable WorkspaceActivation facts.
//
// Shared decorator invariants are verified once across both backends via
// provisioningBackends(); driver-specific behaviors (Podman volume labels and
// foreign-volume refusal, Kubernetes PVC template validation, durable tenant
// ownership checks, no-wait-for-Bound, foreign/terminating claim refusal, and
// composition wiring) are tested right below.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	runtimepodman "github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/engine"
	kernelengine "github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/installation"
	driverkube "github.com/optimiware/kodema/internal/server/drivers/kube"
	podmandriver "github.com/optimiware/kodema/internal/server/drivers/podman"
)

const (
	kubeTestTenant       = id.TenantID("tenant-1")
	kubeTestNamespace    = "tenant-namespace-1"
	kubeTestStorageClass = "standard"
	kubeTestSize         = "64Mi"
	kubeTestAccessMode   = "ReadWriteOnce"
)

// kubeTestTemplate is the trusted claim template the composition configures.
func kubeTestTemplate() WorkspacePVC {
	return WorkspacePVC{
		Namespace:    kubeTestNamespace,
		TenantID:     kubeTestTenant,
		StorageClass: kubeTestStorageClass,
		Size:         kubeTestSize,
		AccessMode:   kubeTestAccessMode,
	}
}

// withKubeTemplate copies the valid template and applies one mutation.
func withKubeTemplate(mutate func(*WorkspacePVC)) WorkspacePVC {
	template := kubeTestTemplate()
	mutate(&template)
	return template
}

// expectedWorkspaceVolume is the exact volume the Podman adapter's Retain
// validates: the deterministic name and the single workspace ownership label.
func expectedWorkspaceVolume(t *testing.T, workspaceID id.WorkspaceID) runtimepodman.Volume {
	t.Helper()
	name, err := runtimepodman.WorkspaceVolumeName(workspaceID)
	if err != nil {
		t.Fatalf("WorkspaceVolumeName(%q): %v", workspaceID, err)
	}
	return runtimepodman.Volume{
		Name:   name,
		Labels: map[string]string{runtimepodman.LabelWorkspace: string(workspaceID)},
	}
}

// expectedKubeClaimSpec is the exact request the Kubernetes decorator must
// send: every field from trusted configuration plus the requested workspace id.
func expectedKubeClaimSpec(template WorkspacePVC, workspaceID id.WorkspaceID) driverkube.ClaimSpec {
	return driverkube.ClaimSpec{
		Namespace:    template.Namespace,
		TenantID:     template.TenantID,
		WorkspaceID:  workspaceID,
		StorageClass: template.StorageClass,
		Size:         template.Size,
		AccessMode:   template.AccessMode,
	}
}

// callLog records the order of fake external effects.
type callLog struct {
	mu     sync.Mutex
	events []string
}

func (log *callLog) add(event string) {
	log.mu.Lock()
	log.events = append(log.events, event)
	log.mu.Unlock()
}

func (log *callLog) snapshot() []string {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.events...)
}

// recordingWorkspace is a kernelengine.Workspace fake with no host effect.
type recordingWorkspace struct {
	log *callLog

	mu           sync.Mutex
	statusCalls  int
	retainCalls  int
	releaseCalls int
	observation  kernelengine.WorkspaceObservation
	statusErr    error
	retainErr    error
	releaseErr   error
}

func (workspace *recordingWorkspace) record(event string) {
	if workspace.log != nil {
		workspace.log.add(event)
	}
}

func (workspace *recordingWorkspace) Status(_ context.Context, workspaceID id.WorkspaceID) (kernelengine.WorkspaceObservation, error) {
	workspace.mu.Lock()
	workspace.statusCalls++
	observation := workspace.observation
	err := workspace.statusErr
	workspace.mu.Unlock()
	if err != nil {
		return kernelengine.WorkspaceObservation{}, err
	}
	if observation.WorkspaceID == "" {
		observation.WorkspaceID = workspaceID
	}
	return observation, nil
}

func (workspace *recordingWorkspace) Retain(_ context.Context, _ id.WorkspaceID) error {
	workspace.mu.Lock()
	workspace.retainCalls++
	err := workspace.retainErr
	workspace.mu.Unlock()
	workspace.record("retain")
	return err
}

func (workspace *recordingWorkspace) Release(_ context.Context, _ id.WorkspaceID) error {
	workspace.mu.Lock()
	workspace.releaseCalls++
	err := workspace.releaseErr
	workspace.mu.Unlock()
	workspace.record("release")
	return err
}

func (workspace *recordingWorkspace) retainCount() int {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return workspace.retainCalls
}

func (workspace *recordingWorkspace) statusCount() int {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return workspace.statusCalls
}

func (workspace *recordingWorkspace) releaseCount() int {
	workspace.mu.Lock()
	defer workspace.mu.Unlock()
	return workspace.releaseCalls
}

// recordingProvisioner is a VolumeProvisioner fake that records every requested
// volume and delegates to an optional create function.
type recordingProvisioner struct {
	log *callLog

	mu      sync.Mutex
	volumes []runtimepodman.Volume
	create  func(context.Context, runtimepodman.Volume) (runtimepodman.Volume, error)
}

func (provisioner *recordingProvisioner) CreateVolume(ctx context.Context, volume runtimepodman.Volume) (runtimepodman.Volume, error) {
	provisioner.mu.Lock()
	provisioner.volumes = append(provisioner.volumes, volume)
	create := provisioner.create
	provisioner.mu.Unlock()
	if provisioner.log != nil {
		provisioner.log.add("create")
	}
	if create != nil {
		return create(ctx, volume)
	}
	return volume, nil
}

func (provisioner *recordingProvisioner) count() int {
	provisioner.mu.Lock()
	defer provisioner.mu.Unlock()
	return len(provisioner.volumes)
}

func (provisioner *recordingProvisioner) requested() []runtimepodman.Volume {
	provisioner.mu.Lock()
	defer provisioner.mu.Unlock()
	return append([]runtimepodman.Volume(nil), provisioner.volumes...)
}

// recordingClaimProvisioner is a ClaimProvisioner fake that records every
// requested spec and delegates to an optional create function.
type recordingClaimProvisioner struct {
	log *callLog

	mu     sync.Mutex
	specs  []driverkube.ClaimSpec
	create func(context.Context, driverkube.ClaimSpec) (runtimekube.Claim, error)
}

func (provisioner *recordingClaimProvisioner) CreateClaim(ctx context.Context, spec driverkube.ClaimSpec) (runtimekube.Claim, error) {
	provisioner.mu.Lock()
	provisioner.specs = append(provisioner.specs, spec)
	create := provisioner.create
	provisioner.mu.Unlock()
	if provisioner.log != nil {
		provisioner.log.add("create")
	}
	if create != nil {
		return create(ctx, spec)
	}
	return runtimekube.Claim{Name: "unbound-workspace-claim", Namespace: spec.Namespace}, nil
}

func (provisioner *recordingClaimProvisioner) count() int {
	provisioner.mu.Lock()
	defer provisioner.mu.Unlock()
	return len(provisioner.specs)
}

func (provisioner *recordingClaimProvisioner) requested() []driverkube.ClaimSpec {
	provisioner.mu.Lock()
	defer provisioner.mu.Unlock()
	return append([]driverkube.ClaimSpec(nil), provisioner.specs...)
}

// fakeActivation is a WorkspaceActivation fake keyed by workspace identity.
// The Podman decorator never calls OwningTenant, so the zero tenant is
// harmless there.
type fakeActivation struct {
	mu          sync.Mutex
	previously  map[id.WorkspaceID]bool
	err         error
	calls       []id.WorkspaceID
	tenant      id.TenantID
	tenants     map[id.WorkspaceID]id.TenantID
	tenantErr   error
	tenantCalls []id.WorkspaceID
}

func (activation *fakeActivation) PreviouslyActivated(_ context.Context, workspaceID id.WorkspaceID) (bool, error) {
	activation.mu.Lock()
	defer activation.mu.Unlock()
	activation.calls = append(activation.calls, workspaceID)
	if activation.err != nil {
		return false, activation.err
	}
	return activation.previously[workspaceID], nil
}

func (activation *fakeActivation) OwningTenant(_ context.Context, workspaceID id.WorkspaceID) (id.TenantID, error) {
	activation.mu.Lock()
	defer activation.mu.Unlock()
	activation.tenantCalls = append(activation.tenantCalls, workspaceID)
	if activation.tenantErr != nil {
		return "", activation.tenantErr
	}
	if tenant, ok := activation.tenants[workspaceID]; ok {
		return tenant, nil
	}
	return activation.tenant, nil
}

func (activation *fakeActivation) count() int {
	activation.mu.Lock()
	defer activation.mu.Unlock()
	return len(activation.calls)
}

func (activation *fakeActivation) tenantCount() int {
	activation.mu.Lock()
	defer activation.mu.Unlock()
	return len(activation.tenantCalls)
}

// provisioningFixtureOptions configures a backend fixture for the shared
// decorator contract suite.
type provisioningFixtureOptions struct {
	retainErr     error
	observation   kernelengine.WorkspaceObservation
	previously    map[id.WorkspaceID]bool
	activationErr error
	onCreate      func(attempt int) error
	template      *WorkspacePVC
}

type provisioningFixture struct {
	decorated       kernelengine.Workspace
	inner           *recordingWorkspace
	activation      *fakeActivation
	log             *callLog
	provisionCount  func() int
	assertRequested func(t *testing.T, workspaceID id.WorkspaceID, calls int)
	forbiddenDetail func(t *testing.T, workspaceID id.WorkspaceID) []string
}

type provisioningBackend struct {
	name             string
	checksTenant     bool
	ambiguousErr     error
	newFixture       func(t *testing.T, opts provisioningFixtureOptions) provisioningFixture
	constructMissing func(inner kernelengine.Workspace, hasProvisioner bool, activation WorkspaceActivation) error
}

// provisioningBackends returns the Podman and Kubernetes backends that share
// the fresh-only provisioningWorkspace decorator contract.
func provisioningBackends() []provisioningBackend {
	newCommon := func(opts provisioningFixtureOptions, tenant id.TenantID) (*callLog, *recordingWorkspace, *fakeActivation) {
		log := &callLog{}
		inner := &recordingWorkspace{log: log, retainErr: opts.retainErr, observation: opts.observation}
		activation := &fakeActivation{previously: opts.previously, err: opts.activationErr, tenant: tenant}
		return log, inner, activation
	}

	return []provisioningBackend{
		{
			name:         "podman",
			ambiguousErr: podmandriver.ErrAmbiguous,
			newFixture: func(t *testing.T, opts provisioningFixtureOptions) provisioningFixture {
				t.Helper()
				log, inner, activation := newCommon(opts, "")
				attempts := 0
				provisioner := &recordingProvisioner{log: log}
				if opts.onCreate != nil {
					provisioner.create = func(_ context.Context, volume runtimepodman.Volume) (runtimepodman.Volume, error) {
						attempts++
						if err := opts.onCreate(attempts); err != nil {
							return runtimepodman.Volume{}, err
						}
						return volume, nil
					}
				}
				decorated, err := newProvisioningWorkspace(inner, provisioner, activation)
				if err != nil {
					t.Fatalf("newProvisioningWorkspace: %v", err)
				}
				return provisioningFixture{
					decorated:      decorated,
					inner:          inner,
					activation:     activation,
					log:            log,
					provisionCount: provisioner.count,
					assertRequested: func(t *testing.T, workspaceID id.WorkspaceID, calls int) {
						t.Helper()
						want := expectedWorkspaceVolume(t, workspaceID)
						got := provisioner.requested()
						if len(got) != calls {
							t.Fatalf("CreateVolume calls = %d; want %d", len(got), calls)
						}
						for i, volume := range got {
							if !reflect.DeepEqual(volume, want) {
								t.Fatalf("CreateVolume[%d] = %#v; want %#v", i, volume, want)
							}
						}
					},
					forbiddenDetail: func(t *testing.T, workspaceID id.WorkspaceID) []string {
						t.Helper()
						return []string{runtimepodman.LabelWorkspace, expectedWorkspaceVolume(t, workspaceID).Name}
					},
				}
			},
			constructMissing: func(inner kernelengine.Workspace, hasProvisioner bool, activation WorkspaceActivation) error {
				var provisioner VolumeProvisioner
				if hasProvisioner {
					provisioner = &recordingProvisioner{}
				}
				_, err := newProvisioningWorkspace(inner, provisioner, activation)
				return err
			},
		},
		{
			name:         "kubernetes",
			checksTenant: true,
			ambiguousErr: driverkube.ErrAmbiguous,
			newFixture: func(t *testing.T, opts provisioningFixtureOptions) provisioningFixture {
				t.Helper()
				template := kubeTestTemplate()
				if opts.template != nil {
					template = *opts.template
				}
				log, inner, activation := newCommon(opts, template.TenantID)
				attempts := 0
				provisioner := &recordingClaimProvisioner{log: log}
				if opts.onCreate != nil {
					provisioner.create = func(_ context.Context, spec driverkube.ClaimSpec) (runtimekube.Claim, error) {
						attempts++
						if err := opts.onCreate(attempts); err != nil {
							return runtimekube.Claim{}, err
						}
						return runtimekube.Claim{Name: "kodema-workspace-claim", Namespace: spec.Namespace}, nil
					}
				}
				decorated, err := newKubeProvisioningWorkspace(inner, provisioner, activation, template)
				if err != nil {
					t.Fatalf("newKubeProvisioningWorkspace: %v", err)
				}
				return provisioningFixture{
					decorated:      decorated,
					inner:          inner,
					activation:     activation,
					log:            log,
					provisionCount: provisioner.count,
					assertRequested: func(t *testing.T, workspaceID id.WorkspaceID, calls int) {
						t.Helper()
						want := expectedKubeClaimSpec(template, workspaceID)
						got := provisioner.requested()
						if len(got) != calls {
							t.Fatalf("CreateClaim calls = %d; want %d", len(got), calls)
						}
						for i, spec := range got {
							if !reflect.DeepEqual(spec, want) {
								t.Fatalf("CreateClaim[%d] = %#v; want %#v", i, spec, want)
							}
						}
					},
					forbiddenDetail: func(t *testing.T, workspaceID id.WorkspaceID) []string {
						t.Helper()
						claimName, err := runtimekube.WorkspaceClaimName(workspaceID)
						if err != nil {
							t.Fatalf("WorkspaceClaimName(%q): %v", workspaceID, err)
						}
						return []string{template.Namespace, string(template.TenantID), template.StorageClass, template.Size, template.AccessMode, claimName}
					},
				}
			},
			constructMissing: func(inner kernelengine.Workspace, hasProvisioner bool, activation WorkspaceActivation) error {
				var provisioner ClaimProvisioner
				if hasProvisioner {
					provisioner = &recordingClaimProvisioner{}
				}
				_, err := newKubeProvisioningWorkspace(inner, provisioner, activation, kubeTestTemplate())
				return err
			},
		},
	}
}

// TestProvisioningWorkspaceFreshOnly pins the central safety rule across both
// backends: provision only for a never-activated workspace; never recreate the
// lost resource of an execution that already ran.
func TestProvisioningWorkspaceFreshOnly(t *testing.T) {
	const workspaceID = id.WorkspaceID("workspace-fresh-1")

	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			t.Run("fresh workspace is provisioned once then retained", func(t *testing.T) {
				fx := backend.newFixture(t, provisioningFixtureOptions{})
				if err := fx.decorated.Retain(context.Background(), workspaceID); err != nil {
					t.Fatalf("Retain: %v", err)
				}
				fx.assertRequested(t, workspaceID, 1)
				if got := fx.inner.retainCount(); got != 1 {
					t.Fatalf("Retain calls = %d; want 1", got)
				}
				if got := fx.log.snapshot(); !reflect.DeepEqual(got, []string{"create", "retain"}) {
					t.Fatalf("effect order = %v; want [create retain]", got)
				}
				if got := fx.activation.count(); got != 1 {
					t.Fatalf("activation lookups = %d; want 1", got)
				}
				wantTenants := 0
				if backend.checksTenant {
					wantTenants = 1
				}
				if got := fx.activation.tenantCount(); got != wantTenants {
					t.Fatalf("tenant lookups = %d; want %d", got, wantTenants)
				}
			})

			t.Run("already-activated workspace with missing resource is never recreated", func(t *testing.T) {
				fx := backend.newFixture(t, provisioningFixtureOptions{
					retainErr:  fault.New(fault.NotFound, "retained workspace resource is missing; never recreated"),
					previously: map[id.WorkspaceID]bool{workspaceID: true},
				})
				err := fx.decorated.Retain(context.Background(), workspaceID)
				if class, ok := fault.ClassOf(err); !ok || class != fault.NotFound {
					t.Fatalf("Retain error = %v; want not_found", err)
				}
				if got := fx.provisionCount(); got != 0 {
					t.Fatalf("provision calls = %d; previously activated workspace must never be recreated", got)
				}
				if got := fx.activation.tenantCount(); got != 0 {
					t.Fatalf("tenant lookups = %d; already-activated workspace must not read tenant", got)
				}
				if got := fx.inner.retainCount(); got != 1 {
					t.Fatalf("Retain calls = %d; want 1", got)
				}
			})

			t.Run("already-activated workspace with present resource retains without provisioning", func(t *testing.T) {
				fx := backend.newFixture(t, provisioningFixtureOptions{
					previously: map[id.WorkspaceID]bool{workspaceID: true},
				})
				if err := fx.decorated.Retain(context.Background(), workspaceID); err != nil {
					t.Fatalf("Retain: %v", err)
				}
				if got := fx.provisionCount(); got != 0 {
					t.Fatalf("provision calls = %d; want 0", got)
				}
				if got := fx.inner.retainCount(); got != 1 {
					t.Fatalf("Retain calls = %d; want 1", got)
				}
			})
		})
	}
}

// TestProvisioningWorkspaceCancellationBeforeEffect proves a cancelled context
// reads no fact and performs no external effect across both backends.
func TestProvisioningWorkspaceCancellationBeforeEffect(t *testing.T) {
	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fx := backend.newFixture(t, provisioningFixtureOptions{})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := fx.decorated.Retain(ctx, id.WorkspaceID("workspace-cancelled"))
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled Retain error = %v; want context.Canceled", err)
			}
			if got := fx.provisionCount(); got != 0 {
				t.Fatalf("provision calls = %d; cancelled Retain must not provision", got)
			}
			if got := fx.inner.retainCount(); got != 0 {
				t.Fatalf("Retain calls = %d; cancelled Retain must not delegate", got)
			}
			if got := fx.activation.count(); got != 0 {
				t.Fatalf("activation lookups = %d; cancelled Retain must not read facts", got)
			}
		})
	}
}

// TestProvisioningWorkspaceActivationFactFailure proves the decorator fails
// closed when the durable activation fact cannot be read.
func TestProvisioningWorkspaceActivationFactFailure(t *testing.T) {
	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fx := backend.newFixture(t, provisioningFixtureOptions{
				activationErr: fault.New(fault.DependencyUnavailable, "workspace activation state is unreachable"),
			})
			err := fx.decorated.Retain(context.Background(), id.WorkspaceID("workspace-unknown"))
			if class, ok := fault.ClassOf(err); !ok || class != fault.DependencyUnavailable {
				t.Fatalf("Retain error = %v; want dependency_unavailable", err)
			}
			if got := fx.provisionCount(); got != 0 {
				t.Fatalf("provision calls = %d; unknown activation fact must not provision", got)
			}
			if got := fx.inner.retainCount(); got != 0 {
				t.Fatalf("Retain calls = %d; unknown activation fact must not delegate", got)
			}
		})
	}
}

// TestProvisioningWorkspaceUncertainCreateIsNotRetained proves each driver's
// ambiguous transport sentinel stays reachable as UncertainExternalEffect and
// never delegates Retain.
func TestProvisioningWorkspaceUncertainCreateIsNotRetained(t *testing.T) {
	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fx := backend.newFixture(t, provisioningFixtureOptions{
				onCreate: func(int) error {
					return fmt.Errorf("%w: response was lost", backend.ambiguousErr)
				},
			})
			err := fx.decorated.Retain(context.Background(), id.WorkspaceID("workspace-uncertain"))
			if !errors.Is(err, backend.ambiguousErr) {
				t.Fatalf("Retain error = %v; want %v reachable", err, backend.ambiguousErr)
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.UncertainExternalEffect {
				t.Fatalf("Retain error class = %v/%v; want uncertain_external_effect", class, ok)
			}
			if got := fx.inner.retainCount(); got != 0 {
				t.Fatalf("Retain calls = %d; uncertain create must not delegate", got)
			}
		})
	}
}

// TestProvisioningWorkspaceIdempotentRetryAfterLostResponse proves a retry
// after a lost response re-sends the exact deterministic resource request and
// only delegates Retain once creation succeeds.
func TestProvisioningWorkspaceIdempotentRetryAfterLostResponse(t *testing.T) {
	const workspaceID = id.WorkspaceID("workspace-retry")

	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fx := backend.newFixture(t, provisioningFixtureOptions{
				onCreate: func(attempt int) error {
					if attempt == 1 {
						return fmt.Errorf("%w: response was lost", backend.ambiguousErr)
					}
					return nil
				},
			})

			firstErr := fx.decorated.Retain(context.Background(), workspaceID)
			if !errors.Is(firstErr, backend.ambiguousErr) {
				t.Fatalf("first Retain error = %v; want %v", firstErr, backend.ambiguousErr)
			}
			if got := fx.provisionCount(); got != 1 {
				t.Fatalf("provision calls after lost response = %d; want 1", got)
			}
			if got := fx.inner.retainCount(); got != 0 {
				t.Fatalf("Retain calls after lost response = %d; want 0", got)
			}

			if err := fx.decorated.Retain(context.Background(), workspaceID); err != nil {
				t.Fatalf("retry Retain: %v", err)
			}
			fx.assertRequested(t, workspaceID, 2)
			if got := fx.inner.retainCount(); got != 1 {
				t.Fatalf("Retain calls after retry = %d; want 1", got)
			}
		})
	}
}

// TestProvisioningWorkspaceErrorsRenderNoResourceDetails proves the decorator
// never leaks resource names, labels, namespaces, tenants, or template details
// into errors while keeping the driver cause reachable.
func TestProvisioningWorkspaceErrorsRenderNoResourceDetails(t *testing.T) {
	const workspaceID = id.WorkspaceID("workspace-detail-free")
	customTemplate := WorkspacePVC{
		Namespace:    "namespace-detail-9",
		TenantID:     "tenant-detail-9",
		StorageClass: "class-detail-9",
		Size:         "999Mi",
		AccessMode:   "ReadWriteMany",
	}

	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			plainCause := errors.New("provision create failed")
			cases := []struct {
				name  string
				err   error
				cause error
			}{
				{name: "plain driver cause", err: plainCause, cause: plainCause},
				{name: "ambiguous outcome", err: fmt.Errorf("%w: response was lost", backend.ambiguousErr), cause: backend.ambiguousErr},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					template := customTemplate
					fx := backend.newFixture(t, provisioningFixtureOptions{
						template: &template,
						onCreate: func(int) error { return tc.err },
					})
					err := fx.decorated.Retain(context.Background(), workspaceID)
					if err == nil {
						t.Fatal("Retain succeeded despite a failed create")
					}
					if !errors.Is(err, tc.cause) {
						t.Fatalf("Retain error = %v; want cause %v reachable", err, tc.cause)
					}
					for _, detail := range fx.forbiddenDetail(t, workspaceID) {
						if strings.Contains(err.Error(), detail) {
							t.Fatalf("error renders resource detail %q: %v", detail, err)
						}
					}
				})
			}
		})
	}
}

// TestProvisioningWorkspaceDelegatesStatusAndRelease proves Status and Release
// delegate unchanged, never provision, and consult no activation or tenant fact.
func TestProvisioningWorkspaceDelegatesStatusAndRelease(t *testing.T) {
	const workspaceID = id.WorkspaceID("workspace-delegated")

	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fx := backend.newFixture(t, provisioningFixtureOptions{
				observation: kernelengine.WorkspaceObservation{Available: true},
			})

			observation, err := fx.decorated.Status(context.Background(), workspaceID)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if !observation.Available || observation.WorkspaceID != workspaceID {
				t.Fatalf("Status observation = %+v; want available for %q", observation, workspaceID)
			}
			if err := fx.decorated.Release(context.Background(), workspaceID); err != nil {
				t.Fatalf("Release: %v", err)
			}
			if got := fx.inner.statusCount(); got != 1 {
				t.Fatalf("inner Status calls = %d; want 1", got)
			}
			if got := fx.inner.releaseCount(); got != 1 {
				t.Fatalf("inner Release calls = %d; want 1", got)
			}
			if got := fx.provisionCount(); got != 0 {
				t.Fatalf("provision calls = %d; Status/Release must never provision", got)
			}
			if got := fx.activation.count(); got != 0 {
				t.Fatalf("activation lookups = %d; Status/Release must not consult activation", got)
			}
			if got := fx.activation.tenantCount(); got != 0 {
				t.Fatalf("tenant lookups = %d; Status/Release must not read tenant", got)
			}
		})
	}
}

// TestProvisioningWorkspaceRequiresDependencies proves incomplete decorators
// are refused at construction with InvalidInput across both backends.
func TestProvisioningWorkspaceRequiresDependencies(t *testing.T) {
	inner := &recordingWorkspace{}
	activation := &fakeActivation{tenant: kubeTestTenant}
	cases := []struct {
		name           string
		inner          kernelengine.Workspace
		hasProvisioner bool
		activation     WorkspaceActivation
	}{
		{name: "missing workspace", inner: nil, hasProvisioner: true, activation: activation},
		{name: "missing provisioner", inner: inner, hasProvisioner: false, activation: activation},
		{name: "missing activation fact", inner: inner, hasProvisioner: true, activation: nil},
	}

	for _, backend := range provisioningBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					err := backend.constructMissing(tc.inner, tc.hasProvisioner, tc.activation)
					if err == nil {
						t.Fatal("constructor accepted an incomplete decorator")
					}
					if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
						t.Fatalf("constructor error = %v; want invalid_input", err)
					}
				})
			}
		})
	}
}

// --- Podman-specific decorator tests ---

// TestPodmanProvisioningRefusesForeignVolume proves a foreign-owner refusal
// propagates as Forbidden and Retain is not called.
func TestPodmanProvisioningRefusesForeignVolume(t *testing.T) {
	inner := &recordingWorkspace{}
	provisioner := &recordingProvisioner{create: func(context.Context, runtimepodman.Volume) (runtimepodman.Volume, error) {
		return runtimepodman.Volume{}, fault.New(fault.Forbidden,
			"podman volume create found an existing volume with different ownership labels")
	}}
	decorated, err := newProvisioningWorkspace(inner, provisioner, &fakeActivation{})
	if err != nil {
		t.Fatalf("newProvisioningWorkspace: %v", err)
	}

	err = decorated.Retain(context.Background(), id.WorkspaceID("workspace-foreign"))
	if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
		t.Fatalf("Retain error = %v; want forbidden", err)
	}
	if got := inner.retainCount(); got != 0 {
		t.Fatalf("Retain calls = %d; a refused provision must not be retained", got)
	}
}

// --- Kubernetes-specific decorator tests ---

// TestKubernetesProvisioningDoesNotWaitForClaimBinding proves the decorator
// treats the create response as a provisioned claim and delegates Retain
// immediately. A WaitForFirstConsumer StorageClass only binds the claim when
// the workload Job runs, so waiting for Bound would deadlock activation.
func TestKubernetesProvisioningDoesNotWaitForClaimBinding(t *testing.T) {
	inner := &recordingWorkspace{}
	provisioner := &recordingClaimProvisioner{create: func(_ context.Context, spec driverkube.ClaimSpec) (runtimekube.Claim, error) {
		return runtimekube.Claim{Name: "kodema-workspace-unbound", Namespace: spec.Namespace}, nil
	}}
	decorated, err := newKubeProvisioningWorkspace(inner, provisioner, &fakeActivation{tenant: kubeTestTenant}, kubeTestTemplate())
	if err != nil {
		t.Fatalf("newKubeProvisioningWorkspace: %v", err)
	}

	if err := decorated.Retain(context.Background(), id.WorkspaceID("workspace-kube-unbound")); err != nil {
		t.Fatalf("Retain: %v", err)
	}
	if got := provisioner.count(); got != 1 {
		t.Fatalf("CreateClaim calls = %d; want exactly 1 with no polling", got)
	}
	if got := inner.retainCount(); got != 1 {
		t.Fatalf("Retain calls = %d; want the claim retained without waiting for binding", got)
	}
}

// TestKubernetesProvisioningTenantChecks proves durable tenant ownership is
// enforced before any external effect: an unreadable tenant fails closed and a
// mismatched tenant is refused as Forbidden without rendering tenant/namespace.
func TestKubernetesProvisioningTenantChecks(t *testing.T) {
	t.Run("unreadable tenant fact fails closed", func(t *testing.T) {
		inner := &recordingWorkspace{}
		provisioner := &recordingClaimProvisioner{}
		activation := &fakeActivation{
			tenant:    kubeTestTenant,
			tenantErr: fault.New(fault.DependencyUnavailable, "workspace execution tenant is unreachable"),
		}
		decorated, err := newKubeProvisioningWorkspace(inner, provisioner, activation, kubeTestTemplate())
		if err != nil {
			t.Fatalf("newKubeProvisioningWorkspace: %v", err)
		}

		err = decorated.Retain(context.Background(), id.WorkspaceID("workspace-kube-tenant-unknown"))
		if class, ok := fault.ClassOf(err); !ok || class != fault.DependencyUnavailable {
			t.Fatalf("Retain error = %v; want dependency_unavailable", err)
		}
		if got := provisioner.count(); got != 0 {
			t.Fatalf("CreateClaim calls = %d; an unknown tenant fact must not provision", got)
		}
		if got := inner.retainCount(); got != 0 {
			t.Fatalf("Retain calls = %d; an unknown tenant fact must not delegate", got)
		}
	})

	t.Run("durable tenant mismatch is forbidden without leaking values", func(t *testing.T) {
		const foreignTenant = id.TenantID("tenant-foreign")
		inner := &recordingWorkspace{}
		provisioner := &recordingClaimProvisioner{}
		activation := &fakeActivation{tenant: foreignTenant}
		decorated, err := newKubeProvisioningWorkspace(inner, provisioner, activation, kubeTestTemplate())
		if err != nil {
			t.Fatalf("newKubeProvisioningWorkspace: %v", err)
		}

		err = decorated.Retain(context.Background(), id.WorkspaceID("workspace-kube-tenant"))
		if err == nil {
			t.Fatal("Retain succeeded for a workspace owned by another tenant")
		}
		if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
			t.Fatalf("Retain error = %v; want forbidden", err)
		}
		if got := provisioner.count(); got != 0 {
			t.Fatalf("CreateClaim calls = %d; a tenant mismatch must not provision", got)
		}
		if got := inner.retainCount(); got != 0 {
			t.Fatalf("Retain calls = %d; a tenant mismatch must not delegate", got)
		}
		for _, forbidden := range []string{string(foreignTenant), string(kubeTestTenant), kubeTestNamespace} {
			if strings.Contains(err.Error(), forbidden) {
				t.Fatalf("error renders a tenant or namespace value %q: %v", forbidden, err)
			}
		}
	})
}

// TestKubernetesProvisioningRefusalsDoNotRetain proves a foreign claim and a
// terminating-claim conflict propagate and are never collapsed into a retained
// workspace.
func TestKubernetesProvisioningRefusalsDoNotRetain(t *testing.T) {
	cases := []struct {
		name   string
		create func(context.Context, driverkube.ClaimSpec) (runtimekube.Claim, error)
		class  fault.Class
		cause  error
	}{
		{
			name: "foreign claim is forbidden",
			create: func(context.Context, driverkube.ClaimSpec) (runtimekube.Claim, error) {
				return runtimekube.Claim{}, fault.New(fault.Forbidden,
					"kubernetes workspace claim is already held by another owner")
			},
			class: fault.Forbidden,
		},
		{
			name: "terminating claim is a version conflict",
			create: func(context.Context, driverkube.ClaimSpec) (runtimekube.Claim, error) {
				return runtimekube.Claim{}, fault.Wrap(fault.VersionConflict,
					"kubernetes workspace claim is terminating and cannot be adopted",
					runtimekube.ErrConflict)
			},
			class: fault.VersionConflict,
			cause: runtimekube.ErrConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &recordingWorkspace{}
			provisioner := &recordingClaimProvisioner{create: tc.create}
			decorated, err := newKubeProvisioningWorkspace(inner, provisioner, &fakeActivation{tenant: kubeTestTenant}, kubeTestTemplate())
			if err != nil {
				t.Fatalf("newKubeProvisioningWorkspace: %v", err)
			}

			err = decorated.Retain(context.Background(), id.WorkspaceID("workspace-kube-refused"))
			if class, ok := fault.ClassOf(err); !ok || class != tc.class {
				t.Fatalf("Retain error = %v; want class %v", err, tc.class)
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatalf("Retain error = %v; want cause %v reachable", err, tc.cause)
			}
			if got := inner.retainCount(); got != 0 {
				t.Fatalf("Retain calls = %d; a refused provision must not be retained", got)
			}
		})
	}
}

// TestWorkspacePVCValidation pins the WorkspacePVC template validation matrix
// for both Config.validate and newKubeProvisioningWorkspace construction.
func TestWorkspacePVCValidation(t *testing.T) {
	ptr := func(mutate func(*WorkspacePVC)) *WorkspacePVC {
		template := withKubeTemplate(mutate)
		return &template
	}
	valid := kubeTestTemplate()
	cases := []struct {
		name     string
		template *WorkspacePVC
		want     string
	}{
		{name: "unset template", template: nil},
		{name: "valid template", template: &valid},
		{name: "cluster default storage class", template: ptr(func(tpl *WorkspacePVC) { tpl.StorageClass = "" })},
		{name: "empty namespace", template: ptr(func(tpl *WorkspacePVC) { tpl.Namespace = "" }), want: "namespace"},
		{name: "blank namespace", template: ptr(func(tpl *WorkspacePVC) { tpl.Namespace = "   " }), want: "namespace"},
		{name: "blank storage class", template: ptr(func(tpl *WorkspacePVC) { tpl.StorageClass = "   " }), want: "storage class"},
		{name: "empty size", template: ptr(func(tpl *WorkspacePVC) { tpl.Size = "" }), want: "storage size"},
		{name: "empty access mode", template: ptr(func(tpl *WorkspacePVC) { tpl.AccessMode = "" }), want: "access mode"},
		{name: "invalid tenant", template: ptr(func(tpl *WorkspacePVC) { tpl.TenantID = "tenant bad" }), want: "tenant"},
		{name: "unsupported access mode", template: ptr(func(tpl *WorkspacePVC) { tpl.AccessMode = "ReadWriteSometimes" }), want: "access mode"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := fullTestProfileConfig(t)
			config.Installation.Environment = installation.EnvironmentTest
			config.Installation.Runtime = installation.RuntimeKubernetes
			config.WorkspacePVC = tc.template
			err := config.resolved().validate()

			if tc.want == "" {
				if err != nil {
					t.Fatalf("config.validate refused usable template: %v", err)
				}
				if tc.template != nil {
					if _, err := newKubeProvisioningWorkspace(&recordingWorkspace{}, &recordingClaimProvisioner{}, &fakeActivation{tenant: kubeTestTenant}, *tc.template); err != nil {
						t.Fatalf("newKubeProvisioningWorkspace refused usable template: %v", err)
					}
				}
				return
			}

			if err == nil {
				t.Fatal("config.validate accepted unusable template")
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("config.validate error = %v; want invalid_input", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("config.validate refusal = %q; want substring %q", err.Error(), tc.want)
			}

			_, ctorErr := newKubeProvisioningWorkspace(&recordingWorkspace{}, &recordingClaimProvisioner{}, &fakeActivation{tenant: kubeTestTenant}, *tc.template)
			if ctorErr == nil {
				t.Fatal("newKubeProvisioningWorkspace accepted unusable template")
			}
			if class, ok := fault.ClassOf(ctorErr); !ok || class != fault.InvalidInput {
				t.Fatalf("constructor error = %v; want invalid_input", ctorErr)
			}
		})
	}
}

// --- Composition wiring tests ---

// TestProvisioningWorkspaceWiring verifies buildEnvironment installs the right
// decorator for Podman, Kubernetes, and Process test profiles.
func TestProvisioningWorkspaceWiring(t *testing.T) {
	cases := []struct {
		name               string
		runtime            installation.RuntimeAdapter
		withVolume         bool
		withClaim          bool
		withTemplate       bool
		wantBuildErrSubstr string
		wantVolumeCalls    int
		wantClaimCalls     int
		wantActivationHits int
		checkClaimSpec     bool
	}{
		{
			name:               "podman profile installs volume decorator and ignores claim provisioner",
			runtime:            installation.RuntimePodman,
			withVolume:         true,
			withClaim:          true,
			withTemplate:       true,
			wantVolumeCalls:    1,
			wantActivationHits: 1,
		},
		{
			name:         "process profile leaves workspace unwrapped even when provisioners are supplied",
			runtime:      installation.RuntimeProcess,
			withVolume:   true,
			withClaim:    true,
			withTemplate: true,
		},
		{
			name:               "kubernetes profile installs claim decorator with configured template",
			runtime:            installation.RuntimeKubernetes,
			withClaim:          true,
			withTemplate:       true,
			wantClaimCalls:     1,
			wantActivationHits: 1,
			checkClaimSpec:     true,
		},
		{
			name:               "kubernetes profile refuses claim provisioner without template",
			runtime:            installation.RuntimeKubernetes,
			withClaim:          true,
			wantBuildErrSubstr: "workspace claim template",
		},
		{
			name:         "kubernetes profile leaves workspace unwrapped without claim provisioner",
			runtime:      installation.RuntimeKubernetes,
			withTemplate: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := fullTestProfileConfig(t)
			config.Installation.Environment = installation.EnvironmentTest
			config.Installation.Runtime = tc.runtime
			inner := &recordingWorkspace{}
			volumeProvisioner := &recordingProvisioner{}
			claimProvisioner := &recordingClaimProvisioner{}
			activation := &fakeActivation{tenant: kubeTestTenant}
			template := kubeTestTemplate()

			config.Adapters.Workspace = inner
			config.Adapters.WorkspaceActivation = activation
			if tc.withVolume {
				config.Adapters.VolumeProvisioner = volumeProvisioner
			}
			if tc.withClaim {
				config.Adapters.ClaimProvisioner = claimProvisioner
			}
			if tc.withTemplate {
				config.WorkspacePVC = &template
			}

			environment, err := buildEnvironment(context.Background(), &builder{cfg: config, logger: config.logger()})
			if tc.wantBuildErrSubstr != "" {
				if err == nil {
					t.Fatal("buildEnvironment succeeded; want error")
				}
				if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
					t.Fatalf("buildEnvironment error = %v; want invalid_input", err)
				}
				if !strings.Contains(err.Error(), tc.wantBuildErrSubstr) {
					t.Fatalf("refusal = %q; want substring %q", err.Error(), tc.wantBuildErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildEnvironment: %v", err)
			}

			const workspaceID = id.WorkspaceID("workspace-wired")
			if err := environment.workspace.Retain(context.Background(), workspaceID); err != nil {
				t.Fatalf("Retain: %v", err)
			}
			if got := volumeProvisioner.count(); got != tc.wantVolumeCalls {
				t.Fatalf("CreateVolume calls = %d; want %d", got, tc.wantVolumeCalls)
			}
			if got := claimProvisioner.count(); got != tc.wantClaimCalls {
				t.Fatalf("CreateClaim calls = %d; want %d", got, tc.wantClaimCalls)
			}
			if tc.checkClaimSpec {
				want := expectedKubeClaimSpec(template, workspaceID)
				if got := claimProvisioner.requested(); len(got) != 1 || !reflect.DeepEqual(got[0], want) {
					t.Fatalf("CreateClaim specs = %#v; want %#v", got, want)
				}
			}
			if got := activation.count(); got != tc.wantActivationHits {
				t.Fatalf("activation lookups = %d; want %d", got, tc.wantActivationHits)
			}
			if got := inner.retainCount(); got != 1 {
				t.Fatalf("inner Retain calls = %d; want 1", got)
			}
		})
	}
}

// TestKubernetesClaimProvisioningUsesDurableTenantByDefault proves the wired
// decorator takes the tenant from the persisted execution row and refuses a
// workspace whose durable tenant differs from the trusted template.
func TestKubernetesClaimProvisioningUsesDurableTenantByDefault(t *testing.T) {
	const (
		freshWorkspace = id.WorkspaceID("ws-kube-wiring-fresh")
		freshExecution = id.ExecutionID("execution-kube-wiring-fresh")
	)

	build := func(t *testing.T, tenant id.TenantID) (Config, *recordingWorkspace, *recordingClaimProvisioner) {
		t.Helper()
		config := fullTestProfileConfig(t)
		config.Installation.Environment = installation.EnvironmentTest
		config.Installation.Runtime = installation.RuntimeKubernetes
		inner := &recordingWorkspace{}
		provisioner := &recordingClaimProvisioner{}
		template := kubeTestTemplate()
		config.Adapters.Workspace = inner
		config.Adapters.ClaimProvisioner = provisioner
		config.Adapters.Controller = &activationFactsStore{rows: []store.Row{
			{Table: "executions", Values: map[string]any{
				"id":           string(freshExecution),
				"workspace_id": string(freshWorkspace),
				"tenant_id":    string(tenant),
			}},
			activationActionRow("act-kube-wiring-fresh-0", freshExecution, 0, engine.ActionPending),
		}}
		config.Adapters.WorkspaceActivation = nil
		config.WorkspacePVC = &template
		return config, inner, provisioner
	}

	t.Run("durable tenant matches the template", func(t *testing.T) {
		config, inner, provisioner := build(t, kubeTestTenant)
		environment, err := buildEnvironment(context.Background(), &builder{cfg: config, logger: config.logger()})
		if err != nil {
			t.Fatalf("buildEnvironment: %v", err)
		}
		if err := environment.workspace.Retain(context.Background(), freshWorkspace); err != nil {
			t.Fatalf("fresh Retain: %v", err)
		}
		if got := provisioner.count(); got != 1 {
			t.Fatalf("CreateClaim calls = %d; want 1", got)
		}
		if got := provisioner.requested()[0]; got.TenantID != kubeTestTenant {
			t.Fatalf("CreateClaim tenant = %q; want %q", got.TenantID, kubeTestTenant)
		}
		if got := inner.retainCount(); got != 1 {
			t.Fatalf("inner Retain calls = %d; want 1", got)
		}
	})

	t.Run("durable tenant mismatch is refused", func(t *testing.T) {
		config, inner, provisioner := build(t, id.TenantID("tenant-other"))
		environment, err := buildEnvironment(context.Background(), &builder{cfg: config, logger: config.logger()})
		if err != nil {
			t.Fatalf("buildEnvironment: %v", err)
		}
		err = environment.workspace.Retain(context.Background(), freshWorkspace)
		if class, ok := fault.ClassOf(err); !ok || class != fault.Forbidden {
			t.Fatalf("Retain error = %v; want forbidden", err)
		}
		if got := provisioner.count(); got != 0 {
			t.Fatalf("CreateClaim calls = %d; durable tenant mismatch must not provision", got)
		}
		if got := inner.retainCount(); got != 0 {
			t.Fatalf("Retain calls = %d; durable tenant mismatch must not delegate", got)
		}
	})
}
