package testkit

import (
	"context"
	"fmt"
	"sync"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// MaxLogChunkBytes bounds every chunk the fake runtime stores and returns. A
// larger append is split into ordered chunks; a read never returns more than
// this many bytes in one chunk.
const MaxLogChunkBytes = 4096

// Workload is a read-only snapshot of one fake runtime resource.
type Workload struct {
	Prepared            bool
	Started             bool
	StartCount          int
	StopRequests        int
	StopConfirmed       bool
	LeaseExpired        bool
	Removed             bool
	CredentialRefreshes int
}

// Runtime is a deterministic engine.Runtime. Resources are stable per workload
// identity: a retry observes the same object instead of creating a new one, and
// a replayed start is refused rather than silently applied twice.
//
// The fake separates an effect from its response. Failure, blocking and
// lost-response injections are one-shot and explicitly staged by the test, so
// every crash boundary in K-RECOVER can be reproduced without sleeps.
type Runtime struct {
	mu sync.Mutex

	capabilities []string
	workloads    map[id.WorkloadID]*workloadRecord
	secrets      *Secrets

	prepares int
	starts   int

	failPrepare           error
	failStartBeforeEffect error
	refuseStart           *string
	loseStartResponse     *string
	blockStartBefore      *Barrier
	blockStartAfter       *Barrier

	failStopBeforeEffect error
	loseStopResponse     *string
	blockStopAfter       *Barrier
	confirmStop          *string

	failRemove error
	removals   []reconcile.RemoveRequest

	failCredential error
}

type workloadRecord struct {
	workspaceID         id.WorkspaceID
	started             bool
	startCount          int
	stopRequests        int
	stopConfirmed       bool
	leaseExpired        bool
	removed             bool
	credentialRefreshes int
	observations        []engine.Observation
	logs                []reconcile.LogChunk
	nextOffset          int64
	nextSequence        int64
}

// NewRuntime returns an empty runtime fake with no declared capabilities.
func NewRuntime() *Runtime {
	return &Runtime{
		workloads: make(map[id.WorkloadID]*workloadRecord),
		secrets:   NewSecrets(),
	}
}

// SetCapabilities declares the adapter capability set.
func (r *Runtime) SetCapabilities(names ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capabilities = append([]string(nil), names...)
}

// SetSecrets attaches the secret-delivery fake used by Prepare. A nil fake is
// replaced by a fresh one.
func (r *Runtime) SetSecrets(secrets *Secrets) {
	if secrets == nil {
		secrets = NewSecrets()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.secrets = secrets
}

// Capabilities returns a copy of the declared capability set.
func (r *Runtime) Capabilities(ctx context.Context) (reconcile.Capabilities, error) {
	if err := ctx.Err(); err != nil {
		return reconcile.Capabilities{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return reconcile.Capabilities{Names: append([]string(nil), r.capabilities...)}, nil
}

// Prepare materializes one workload at most once. A second prepare for the same
// stable identity is idempotent; a conflicting workspace is caller misuse.
// Secret delivery happens before the resource exists, so a delivery failure
// leaves nothing to start.
func (r *Runtime) Prepare(ctx context.Context, request reconcile.PrepareRequest) (reconcile.Preparation, error) {
	if err := ctx.Err(); err != nil {
		return reconcile.Preparation{}, err
	}
	if request.Identity.WorkloadID == "" {
		return reconcile.Preparation{}, fault.New(fault.InvalidInput, "prepare requires a workload identity")
	}
	if request.WorkspaceID == "" {
		return reconcile.Preparation{}, fault.New(fault.InvalidInput, "prepare requires a workspace identity")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if record, ok := r.workloads[request.Identity.WorkloadID]; ok {
		if record.removed {
			return reconcile.Preparation{}, fault.New(fault.InvalidInput, fmt.Sprintf(
				"workload %q was removed; its stable identity cannot be reused",
				request.Identity.WorkloadID,
			))
		}
		if record.workspaceID != request.WorkspaceID {
			return reconcile.Preparation{}, fault.New(fault.InvalidInput, fmt.Sprintf(
				"workload %q is already prepared for workspace %q, not %q",
				request.Identity.WorkloadID, record.workspaceID, request.WorkspaceID,
			))
		}
		return reconcile.Preparation{WorkspaceID: record.workspaceID, Detail: "already prepared"}, nil
	}
	if r.failPrepare != nil {
		err := r.failPrepare
		r.failPrepare = nil
		return reconcile.Preparation{}, err
	}
	if len(request.Secrets) > 0 {
		if err := r.secrets.Deliver(request.Identity.WorkloadID, request.Secrets); err != nil {
			return reconcile.Preparation{}, err
		}
	}
	r.workloads[request.Identity.WorkloadID] = &workloadRecord{workspaceID: request.WorkspaceID}
	r.prepares++
	return reconcile.Preparation{WorkspaceID: request.WorkspaceID, Detail: "materialized"}, nil
}

// Start applies the start effect at most once for the stable workload identity.
// A replayed start is refused. An injected refusal reports affirmative proof of
// nonexecution without applying an effect or consuming the attempt; a pre-effect
// failure leaves no effect; a lost post-effect response reports StartUnknown
// while preserving the effect.
func (r *Runtime) Start(ctx context.Context, request reconcile.StartRequest) (reconcile.StartResult, error) {
	if err := ctx.Err(); err != nil {
		return reconcile.StartResult{}, err
	}

	r.mu.Lock()
	record, ok := r.workloads[request.Identity.WorkloadID]
	if !ok {
		r.mu.Unlock()
		return reconcile.StartResult{}, ErrNotPrepared
	}
	if request.WorkspaceID != "" && record.workspaceID != request.WorkspaceID {
		r.mu.Unlock()
		return reconcile.StartResult{}, fault.New(fault.InvalidInput, fmt.Sprintf(
			"workload %q belongs to workspace %q, not %q",
			request.Identity.WorkloadID, record.workspaceID, request.WorkspaceID,
		))
	}
	if record.started {
		r.mu.Unlock()
		return reconcile.StartResult{}, ErrReplay
	}
	if record.removed {
		r.mu.Unlock()
		return reconcile.StartResult{}, fault.New(fault.InvalidInput, fmt.Sprintf(
			"workload %q was removed; its stable identity cannot be reused",
			request.Identity.WorkloadID,
		))
	}
	if r.failStartBeforeEffect != nil {
		err := r.failStartBeforeEffect
		r.failStartBeforeEffect = nil
		r.mu.Unlock()
		return reconcile.StartResult{}, err
	}
	// An injected refusal is affirmative proof of nonexecution. It applies no
	// effect and does not mark the identity started, so a later start of the
	// same prepared identity is still permitted.
	if r.refuseStart != nil {
		detail := *r.refuseStart
		r.refuseStart = nil
		r.mu.Unlock()
		return reconcile.StartResult{
			Outcome:    reconcile.StartProvenNotStarted,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     detail,
		}, nil
	}
	barrier := r.blockStartBefore
	r.blockStartBefore = nil
	r.mu.Unlock()

	if barrier != nil {
		if err := barrier.Wait(ctx); err != nil {
			return reconcile.StartResult{}, err
		}
	}

	r.mu.Lock()
	record, ok = r.workloads[request.Identity.WorkloadID]
	if !ok {
		r.mu.Unlock()
		return reconcile.StartResult{}, ErrNotPrepared
	}
	if record.started {
		r.mu.Unlock()
		return reconcile.StartResult{}, ErrReplay
	}
	if record.removed {
		r.mu.Unlock()
		return reconcile.StartResult{}, fault.New(fault.InvalidInput, fmt.Sprintf(
			"workload %q was removed; its stable identity cannot be reused",
			request.Identity.WorkloadID,
		))
	}
	record.started = true
	record.startCount++
	r.starts++
	after := r.blockStartAfter
	r.blockStartAfter = nil
	lost := r.loseStartResponse
	r.loseStartResponse = nil
	r.mu.Unlock()

	if after != nil {
		if err := after.Wait(ctx); err != nil {
			return reconcile.StartResult{
				Outcome:    reconcile.StartUnknown,
				WorkloadID: request.Identity.WorkloadID,
				Detail:     "start response lost after the effect was applied",
			}, nil
		}
	}
	if lost != nil {
		return reconcile.StartResult{
			Outcome:    reconcile.StartUnknown,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     *lost,
		}, nil
	}
	return reconcile.StartResult{
		Outcome:    reconcile.StartAccepted,
		WorkloadID: request.Identity.WorkloadID,
		Detail:     "accepted",
	}, nil
}

// Observe returns the next queued observation for the identity, or a default
// derived from the fake's own knowledge. An unknown workload stays unknown; a
// prepared but never started workload is affirmative proof of nonexecution.
func (r *Runtime) Observe(ctx context.Context, request reconcile.ObserveRequest) (engine.Observation, error) {
	if err := ctx.Err(); err != nil {
		return engine.Observation{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.workloads[request.Identity.WorkloadID]
	if !ok {
		return engine.UnknownObservation(), nil
	}
	if len(record.observations) > 0 {
		observation := record.observations[0]
		record.observations = record.observations[1:]
		return cloneObservation(observation), nil
	}
	switch {
	case record.removed:
		return engine.AbsentObservation(), nil
	case record.started:
		return engine.RunningObservation(), nil
	default:
		return engine.NeverStarted("runtime fake: no start request was issued for this workload"), nil
	}
}

// Logs returns ordered chunks at or after AfterOffset, bounded by Limit. Reads
// carry no lifecycle authority and return defensive copies.
func (r *Runtime) Logs(ctx context.Context, request reconcile.LogsRequest) (reconcile.LogBatch, error) {
	if err := ctx.Err(); err != nil {
		return reconcile.LogBatch{}, err
	}
	if request.Limit <= 0 {
		return reconcile.LogBatch{}, fault.New(fault.InvalidInput, "logs limit must be positive")
	}
	if request.AfterOffset < 0 {
		return reconcile.LogBatch{}, fault.New(fault.InvalidInput, "logs after-offset must not be negative")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.workloads[request.Identity.WorkloadID]
	if !ok {
		return reconcile.LogBatch{}, fault.New(fault.NotFound, fmt.Sprintf("workload %q is unknown", request.Identity.WorkloadID))
	}
	batch := reconcile.LogBatch{NextOffset: request.AfterOffset}
	for _, chunk := range record.logs {
		if chunk.Offset < request.AfterOffset {
			continue
		}
		if len(batch.Chunks) >= request.Limit {
			break
		}
		copied := chunk
		copied.Data = append([]byte(nil), chunk.Data...)
		batch.Chunks = append(batch.Chunks, copied)
		batch.NextOffset = chunk.Offset + int64(len(chunk.Data))
	}
	return batch, nil
}

// Stop requests termination. An acknowledgment is never confirmation; only a
// staged confirmation produces confirmed evidence.
func (r *Runtime) Stop(ctx context.Context, request reconcile.StopRequest) (engine.StopEvidence, error) {
	if err := ctx.Err(); err != nil {
		return engine.StopEvidence{}, err
	}

	r.mu.Lock()
	record, ok := r.workloads[request.Identity.WorkloadID]
	if !ok {
		r.mu.Unlock()
		return engine.StopEvidence{}, ErrNotPrepared
	}
	if record.stopConfirmed {
		r.mu.Unlock()
		return engine.ConfirmedStop("workload stop was already confirmed"), nil
	}
	if r.failStopBeforeEffect != nil {
		err := r.failStopBeforeEffect
		r.failStopBeforeEffect = nil
		r.mu.Unlock()
		return engine.StopEvidence{}, err
	}
	record.stopRequests++
	if r.confirmStop != nil {
		detail := *r.confirmStop
		r.confirmStop = nil
		record.stopConfirmed = true
		r.mu.Unlock()
		return engine.ConfirmedStop(detail), nil
	}
	barrier := r.blockStopAfter
	r.blockStopAfter = nil
	lost := r.loseStopResponse
	r.loseStopResponse = nil
	r.mu.Unlock()

	if barrier != nil {
		if err := barrier.Wait(ctx); err != nil {
			return engine.PendingStop("stop response lost after the request; termination is not confirmed"), nil
		}
	}
	if lost != nil {
		return engine.PendingStop(*lost), nil
	}
	return engine.PendingStop("stop request acknowledged; termination is not confirmed"), nil
}

// Remove performs idempotent cleanup. A prepared but never started workload has
// no live process, so it is affirmatively removable without stop evidence. A
// started workload still requires a confirmed stop: an acknowledgment, absence
// or lease expiry never substitutes for confirmation. A failed attempt is
// recorded and stays retryable.
func (r *Runtime) Remove(ctx context.Context, request reconcile.RemoveRequest) (reconcile.Removal, error) {
	if err := ctx.Err(); err != nil {
		return reconcile.Removal{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.workloads[request.Identity.WorkloadID]
	if !ok {
		return reconcile.Removal{}, ErrNotPrepared
	}
	if record.started && !record.stopConfirmed {
		return reconcile.Removal{}, ErrStopUnconfirmed
	}
	if record.removed {
		return reconcile.Removal{Removed: true, Detail: "already removed"}, nil
	}
	r.removals = append(r.removals, request)
	if r.failRemove != nil {
		err := r.failRemove
		r.failRemove = nil
		return reconcile.Removal{}, err
	}
	record.removed = true
	return reconcile.Removal{Removed: true, Detail: "removed"}, nil
}

// RefreshCredential replaces token material without replaying or restarting the
// workload. A failed delivery is attributable and retryable.
func (r *Runtime) RefreshCredential(ctx context.Context, request reconcile.CredentialRequest) (reconcile.CredentialRefresh, error) {
	if err := ctx.Err(); err != nil {
		return reconcile.CredentialRefresh{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.workloads[request.Identity.WorkloadID]
	if !ok {
		return reconcile.CredentialRefresh{}, ErrNotPrepared
	}
	if r.failCredential != nil {
		err := r.failCredential
		r.failCredential = nil
		return reconcile.CredentialRefresh{Delivered: false}, err
	}
	record.credentialRefreshes++
	return reconcile.CredentialRefresh{Delivered: true, Detail: "delivered"}, nil
}

// AppendLog appends data to one stream, splitting it into bounded ordered
// chunks. The caller's slice is copied.
func (r *Runtime) AppendLog(workloadID id.WorkloadID, stream reconcile.LogStream, data []byte) error {
	if stream != reconcile.LogStdout && stream != reconcile.LogStderr {
		return fault.New(fault.InvalidInput, fmt.Sprintf("unknown log stream %q", stream))
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.workloads[workloadID]
	if !ok {
		return fault.New(fault.NotFound, fmt.Sprintf("workload %q is unknown", workloadID))
	}
	for len(data) > 0 {
		size := min(len(data), MaxLogChunkBytes)
		record.logs = append(record.logs, reconcile.LogChunk{
			Stream:   stream,
			Offset:   record.nextOffset,
			Sequence: record.nextSequence,
			Data:     append([]byte(nil), data[:size]...),
		})
		record.nextOffset += int64(size)
		record.nextSequence++
		data = data[size:]
	}
	return nil
}

// QueueObservation stages observations consumed in order by Observe.
func (r *Runtime) QueueObservation(workloadID id.WorkloadID, observations ...engine.Observation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.workloads[workloadID]
	if !ok {
		return
	}
	for _, observation := range observations {
		record.observations = append(record.observations, cloneObservation(observation))
	}
}

// Workload returns a snapshot of one resource record.
func (r *Runtime) Workload(workloadID id.WorkloadID) (Workload, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.workloads[workloadID]
	if !ok {
		return Workload{}, false
	}
	return Workload{
		Prepared:            true,
		Started:             record.started,
		StartCount:          record.startCount,
		StopRequests:        record.stopRequests,
		StopConfirmed:       record.stopConfirmed,
		LeaseExpired:        record.leaseExpired,
		Removed:             record.removed,
		CredentialRefreshes: record.credentialRefreshes,
	}, true
}

// PrepareCount returns the number of successful materializations.
func (r *Runtime) PrepareCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prepares
}

// StartCount returns the number of applied start effects.
func (r *Runtime) StartCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts
}

// Removals returns the recorded removal attempts in order.
func (r *Runtime) Removals() []reconcile.RemoveRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]reconcile.RemoveRequest(nil), r.removals...)
}

// OwnershipHeld reports ErrOwnershipHeld while a workload for the workspace has
// not reached a confirmed stop and has not been affirmatively removed. Lease
// expiry and absence are not confirmation; a removed workload no longer owns
// the workspace.
func (r *Runtime) OwnershipHeld(workspaceID id.WorkspaceID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.workloads {
		if record.workspaceID == workspaceID && !record.removed && !record.stopConfirmed {
			return ErrOwnershipHeld
		}
	}
	return nil
}

// ExpireLease marks the controller lease expired. It never confirms a stop.
func (r *Runtime) ExpireLease(workloadID id.WorkloadID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if record, ok := r.workloads[workloadID]; ok {
		record.leaseExpired = true
	}
}

// ConfirmNextStop stages affirmative termination evidence for the next stop.
func (r *Runtime) ConfirmNextStop(detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.confirmStop = &detail
}

// FailNextPrepare injects one materialization failure.
func (r *Runtime) FailNextPrepare(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failPrepare = injectedError(err, "prepare")
}

// FailNextStartBeforeEffect injects one start failure before the effect.
func (r *Runtime) FailNextStartBeforeEffect(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failStartBeforeEffect = injectedError(err, "start")
}

// RefuseNextStart injects one start refusal with affirmative proof that the
// command never executed. The refusal applies no effect, leaves the workload
// unstarted and does not consume the identity, so a later start is permitted.
func (r *Runtime) RefuseNextStart(detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refuseStart = &detail
}

// LoseNextStartResponse injects one lost start response after the effect.
func (r *Runtime) LoseNextStartResponse(detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loseStartResponse = &detail
}

// BlockNextStartBeforeEffect stages a barrier before the start effect.
func (r *Runtime) BlockNextStartBeforeEffect(barrier *Barrier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockStartBefore = barrier
}

// BlockNextStartAfterEffect stages a barrier after the start effect.
func (r *Runtime) BlockNextStartAfterEffect(barrier *Barrier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockStartAfter = barrier
}

// FailNextStopBeforeEffect injects one stop failure before the request effect.
func (r *Runtime) FailNextStopBeforeEffect(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failStopBeforeEffect = injectedError(err, "stop")
}

// LoseNextStopResponse injects one lost stop response after the request effect.
func (r *Runtime) LoseNextStopResponse(detail string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loseStopResponse = &detail
}

// BlockNextStopAfterEffect stages a barrier after the stop request effect.
func (r *Runtime) BlockNextStopAfterEffect(barrier *Barrier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockStopAfter = barrier
}

// FailNextRemove injects one cleanup failure. The attempt stays recorded.
func (r *Runtime) FailNextRemove(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failRemove = injectedError(err, "remove")
}

// FailNextCredential injects one credential-delivery failure.
func (r *Runtime) FailNextCredential(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failCredential = injectedError(err, "credential delivery")
}

func injectedError(err error, operation string) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("testkit: injected %s failure", operation)
}
