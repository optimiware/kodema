package adaptercontract

import (
	"context"

	"github.com/optimiware/kodema/internal/adapter/integration/nats"
	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/control/authorize"
	"github.com/optimiware/kodema/internal/control/event"
	"github.com/optimiware/kodema/internal/control/identity"
	"github.com/optimiware/kodema/internal/control/log"
	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/control/secret"
	"github.com/optimiware/kodema/internal/control/store"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/control/token"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/installation"
)

// The fake adapters below are compile-only conformance consumers. They prove
// each declared port is implementable without a real database, process,
// container, broker, identity provider, registry or KMS. No fake is started by
// a test.

type fakeRows struct{}

func (fakeRows) Next() bool        { return false }
func (fakeRows) Scan(...any) error { return nil }
func (fakeRows) Err() error        { return nil }
func (fakeRows) Close() error      { return nil }

type fakeTransaction struct{}

func (fakeTransaction) Exec(context.Context, store.Statement) (store.Result, error) {
	return store.Result{}, nil
}
func (fakeTransaction) Query(context.Context, store.Query) (store.Rows, error) {
	return fakeRows{}, nil
}
func (fakeTransaction) Commit(context.Context) error   { return nil }
func (fakeTransaction) Rollback(context.Context) error { return nil }

type fakeScopedConnection struct{}

func (fakeScopedConnection) Begin(context.Context, tenancy.Scope) (store.Transaction, error) {
	return fakeTransaction{}, nil
}

// fakeScanner is a compile-only Scanner consumer. It proves the separate
// scoped read seam is implementable without a database and shares the
// declaration-level validation.
type fakeScanner struct{}

func (fakeScanner) Query(_ context.Context, _ tenancy.Scope, query store.ScanQuery) (store.QueryPage, error) {
	if err := query.Validate(); err != nil {
		return store.QueryPage{}, err
	}
	return store.QueryPage{}, nil
}

type fakeMaterializer struct{}

func (fakeMaterializer) Materialize(context.Context, workspace.Materialization) (workspace.Preparation, error) {
	return workspace.Preparation{Complete: true}, nil
}

type fakeRetainedSink struct{}

func (fakeRetainedSink) Append(_ context.Context, scope tenancy.Scope, _ log.Chunk) (log.AppendResult, error) {
	if err := log.ValidateScope(scope); err != nil {
		return log.AppendResult{}, err
	}
	return log.AppendResult{}, nil
}
func (fakeRetainedSink) Read(_ context.Context, request log.ReadRequest) (log.ReadResult, error) {
	if err := request.Validate(); err != nil {
		return log.ReadResult{}, err
	}
	return log.ReadResult{}, nil
}

type fakeLivePublisher struct{}

func (fakeLivePublisher) PublishLive(_ context.Context, scope tenancy.Scope, _ log.Chunk) error {
	return log.ValidateScope(scope)
}

type fakeLiveSubscription struct{}

func (fakeLiveSubscription) Next(context.Context) (log.Chunk, error) { return log.Chunk{}, nil }
func (fakeLiveSubscription) Close(context.Context) error             { return nil }

type fakeLiveSubscriber struct{}

func (fakeLiveSubscriber) SubscribeLive(_ context.Context, scope tenancy.Scope, _ id.ExecutionID) (log.LiveSubscription, error) {
	if err := log.ValidateScope(scope); err != nil {
		return nil, err
	}
	return fakeLiveSubscription{}, nil
}

type fakeSecretResolver struct{}

func (fakeSecretResolver) Resolve(context.Context, secret.ResolveRequest) ([]secret.Resolved, error) {
	return nil, nil
}

type fakeKeyProvider struct{}

func (fakeKeyProvider) Wrap(context.Context, secret.WrapRequest) (secret.WrappedKey, error) {
	return secret.WrappedKey{}, nil
}
func (fakeKeyProvider) Unwrap(context.Context, secret.UnwrapRequest) ([]byte, error) {
	return nil, nil
}

type fakeRuntimeClient struct{}

func (fakeRuntimeClient) Capabilities(context.Context) (reconcile.Capabilities, error) {
	return reconcile.Capabilities{}, nil
}
func (fakeRuntimeClient) Prepare(context.Context, reconcile.PrepareRequest) (reconcile.Preparation, error) {
	return reconcile.Preparation{}, nil
}
func (fakeRuntimeClient) Start(context.Context, reconcile.StartRequest) (reconcile.StartResult, error) {
	return reconcile.StartResult{}, nil
}
func (fakeRuntimeClient) Observe(context.Context, reconcile.ObserveRequest) (engine.Observation, error) {
	return engine.UnknownObservation(), nil
}
func (fakeRuntimeClient) Logs(context.Context, reconcile.LogsRequest) (reconcile.LogBatch, error) {
	return reconcile.LogBatch{}, nil
}
func (fakeRuntimeClient) Stop(context.Context, reconcile.StopRequest) (engine.StopEvidence, error) {
	return engine.PendingStop("pending"), nil
}
func (fakeRuntimeClient) Remove(context.Context, reconcile.RemoveRequest) (reconcile.Removal, error) {
	return reconcile.Removal{}, nil
}
func (fakeRuntimeClient) RefreshCredential(context.Context, reconcile.CredentialRequest) (reconcile.CredentialRefresh, error) {
	return reconcile.CredentialRefresh{}, nil
}
func (fakeRuntimeClient) Version(context.Context) (runtime.APIVersion, error) {
	return runtime.APIVersion{}, nil
}

type fakeEventPublisher struct{}

func (fakeEventPublisher) Publish(context.Context, event.Envelope) error { return nil }

type fakeInvalidationSink struct{}

func (fakeInvalidationSink) Invalidate(context.Context, token.Invalidation) error { return nil }

type fakeNATSPublisher struct{}

func (fakeNATSPublisher) PublishEvent(context.Context, event.Envelope) error { return nil }
func (fakeNATSPublisher) PublishLive(_ context.Context, scope tenancy.Scope, chunk log.Chunk) error {
	_, err := nats.LiveLogSubject(scope, chunk)
	return err
}

type fakeNATSSubscription struct{}

func (fakeNATSSubscription) Close(context.Context) error { return nil }

type fakeNATSSubscriber struct{}

func (fakeNATSSubscriber) SubscribeInvalidations(context.Context, tenancy.Scope, nats.InvalidationHandler) (nats.Subscription, error) {
	return fakeNATSSubscription{}, nil
}

type fakeOIDCVerifier struct{}

func (fakeOIDCVerifier) Verify(context.Context, identity.OIDCRequest) (identity.Principal, error) {
	return identity.Principal{}, nil
}

type fakeHostedAuthority struct{}

func (fakeHostedAuthority) Check(_ context.Context, request authorize.HostedAuthorityRequest) (authorize.HostedAuthorityResponse, error) {
	return authorize.HostedAuthorityResponse{
		Protocol: request.Protocol,
		Decision: authorize.HostedAuthorityDecision{
			Allowed: true,
			Role:    request.Claims.Role,
			Access:  append([]string(nil), request.RequiredAccess...),
		},
	}, nil
}

type fakeImageResolver struct{}

func (fakeImageResolver) Resolve(context.Context, pipeline.ResolveRequest) (pipeline.ResolvedImage, error) {
	return pipeline.ResolvedImage{}, nil
}

type fakeBackupPort struct{}

func (fakeBackupPort) Backup(context.Context, installation.BackupPlan) (installation.BackupResult, error) {
	return installation.BackupResult{}, nil
}

type fakeRestorePort struct{}

func (fakeRestorePort) Preflight(context.Context, installation.BackupIdentity) (installation.BackupEvidence, error) {
	return installation.BackupEvidence{}, nil
}

func (fakeRestorePort) Restore(context.Context, installation.RestorePlan) (installation.RestoreResult, error) {
	return installation.RestoreResult{}, nil
}

func (fakeRestorePort) Reconcile(context.Context) (installation.ReconcileResult, error) {
	return installation.ReconcileResult{}, nil
}

type fakeFencePort struct{}

func (fakeFencePort) Fence(context.Context) error { return nil }

type fakeDispatchGate struct{}

func (fakeDispatchGate) EnableDispatch(context.Context) error { return nil }

var (
	_ store.ScopedConnection    = fakeScopedConnection{}
	_ store.Transaction         = fakeTransaction{}
	_ store.Scanner             = fakeScanner{}
	_ store.Rows                = fakeRows{}
	_ workspace.Materializer    = fakeMaterializer{}
	_ log.RetainedSink          = fakeRetainedSink{}
	_ log.LivePublisher         = fakeLivePublisher{}
	_ log.LiveSubscriber        = fakeLiveSubscriber{}
	_ log.LiveSubscription      = fakeLiveSubscription{}
	_ secret.Resolver           = fakeSecretResolver{}
	_ secret.KeyProvider        = fakeKeyProvider{}
	_ runtime.Client            = fakeRuntimeClient{}
	_ reconcile.Runtime         = (runtime.Client)(nil)
	_ event.Publisher           = fakeEventPublisher{}
	_ token.InvalidationPort    = fakeInvalidationSink{}
	_ nats.Publisher            = fakeNATSPublisher{}
	_ nats.Subscriber           = fakeNATSSubscriber{}
	_ nats.Subscription         = fakeNATSSubscription{}
	_ identity.OIDCVerifier     = fakeOIDCVerifier{}
	_ authorize.HostedAuthority = fakeHostedAuthority{}
	_ pipeline.Resolver         = fakeImageResolver{}
	_ installation.BackupPort   = fakeBackupPort{}
	_ installation.RestorePort  = fakeRestorePort{}
	_ installation.FencePort    = fakeFencePort{}
	_ installation.DispatchGate = fakeDispatchGate{}
)
