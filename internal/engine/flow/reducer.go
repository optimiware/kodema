package flow

// The reducer is the only place where flow facts become durable state
// changes. It is pure: it never reads a clock, allocates randomness, performs
// I/O or mutates its input. Every successful transition returns one complete
// compare-and-commit payload plus the exact post-commit projection that the
// state port would produce from that payload.
//
// Three outcomes are possible:
//
//   - (execution, nil, nil) is a converged no-op: the fact is a duplicate of
//     already recorded evidence, or it records nothing new.
//   - (execution, nil, err) is a refusal: the fact is illegal for the current
//     state, stale, malformed, late or conflicts with settled history. The
//     execution is returned unchanged and nothing is persisted.
//   - (projection, commit, nil) is one atomic transition. The projection is
//     derived from the commit alone, so a state port that applies the same
//     commit cannot diverge from the reducer.
//
// Time, identity and evidence are explicit inputs. A fact time is never used
// to re-derive a persisted deadline, and only the authoritative transaction
// time decides answer timeliness.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// Reduce applies one explicit fact to one execution.
func Reduce(execution engine.Execution, fact Fact) (engine.Execution, *Commit, error) {
	switch typed := fact.(type) {
	case nil:
		return execution, nil, refuseInvalid("flow: fact is nil")
	case ActivateFact:
		return reduceActivate(execution, typed)
	case ObserveFact:
		return reduceObserve(execution, typed)
	case OutputFact:
		return reduceOutput(execution, typed)
	case AnswerFact:
		return reduceAnswer(execution, typed)
	case CancelFact:
		return reduceCancel(execution, typed)
	case ExpireFact:
		return reduceExpire(execution, typed)
	case WorkspaceLostFact:
		return reduceWorkspaceLost(execution, typed)
	case RetiredFact:
		return reduceRetired(execution, typed)
	default:
		return execution, nil, refuseInvalid("flow: unsupported fact %T", fact)
	}
}

// ---------------------------------------------------------------------------
// Activation
// ---------------------------------------------------------------------------

func reduceActivate(execution engine.Execution, fact ActivateFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	if execution.State.Terminal() {
		return execution, nil, refuseInvalid("flow: execution %q is terminal and cannot activate new work", execution.ID)
	}

	target := execution.ActionIndex(fact.ActionID)
	if target < 0 {
		return execution, nil, refuseInvalid("flow: action %q does not exist", fact.ActionID)
	}
	action := execution.Actions[target]
	if action.State != engine.ActionPending {
		return execution, nil, refuseInvalid("flow: action %q is %q, not pending", action.ID, action.State)
	}

	// At most one action may be active, and no activation may overlap it.
	if running, err := runningActionIndex(execution); err != nil {
		return execution, nil, err
	} else if running >= 0 {
		return execution, nil, refuseInvalid("flow: action %q is already running", execution.Actions[running].ID)
	}

	// Ordered lifecycle: every earlier action must have succeeded, and every
	// earlier container workload must have confirmed its stop barrier before a
	// successor may take over the retained workspace.
	for index := range target {
		previous := execution.Actions[index]
		if previous.State != engine.ActionSucceeded {
			return execution, nil, refuseInvalid("flow: action %q cannot activate before action %q succeeds", action.ID, previous.ID)
		}
		if previous.Kind == definition.ActionKindContainer && !previous.Stop.IsConfirmed() {
			return execution, nil, refuseInvalid("flow: action %q cannot activate before the workload of %q is confirmed stopped", action.ID, previous.ID)
		}
	}

	at := fact.At
	deadline := at.UTC().Add(action.Timeout)
	nextCurrent := target
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionRunning,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionRunning,
		NextCurrentAction:        &nextCurrent,
		Output:                   OutputChange{Action: OutputNone},
		Deadline:                 deadline,
		At:                       at,
	}

	switch action.Kind {
	case definition.ActionKindContainer:
		if action.Container == nil {
			return execution, nil, refuseInvalid("flow: container action %q has no container value", action.ID)
		}
		// Persist the stable effect identity and idempotency key before any
		// start request can be issued.
		launch := newLaunchIntent(execution.ID, action.ID, "activation")
		commit.Launch = &launch
	case definition.ActionKindRequest:
		if action.Request == nil {
			return execution, nil, refuseInvalid("flow: request action %q has no request value", action.ID)
		}
		subject, err := resolveSubject(execution, target, action.Request.Definition.Subject)
		if err != nil {
			// Never expose an answerable request with an incomplete subject.
			return activationFailure(execution, action, at, err)
		}
		commit.Subject = &subject
		commit.InvalidateCredential = true
	default:
		return execution, nil, refuseInvalid("flow: action %q has unsupported kind %q", action.ID, action.Kind)
	}

	next := project(execution, *commit)
	return next, commit, nil
}

// activationFailure settles the selected action as failed before any workload
// or request became usable. The failed action, the typed preparation failure
// and every pending successor cancellation travel in one commit.
func activationFailure(execution engine.Execution, action engine.Action, at time.Time, cause error) (engine.Execution, *Commit, error) {
	nextCurrent := -1
	detail := ""
	if cause != nil {
		if msg, ok := fault.MessageOf(cause); ok {
			detail = msg
		} else {
			detail = cause.Error()
		}
	}
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionFailed,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionFailed,
		PendingCancellations:     pendingCancellations(execution, action.ID),
		NextCurrentAction:        &nextCurrent,
		Output:                   OutputChange{Action: OutputNone},
		InvalidateCredential:     true,
		Failure: &engine.Failure{
			Kind:   engine.FailurePreparation,
			Reason: "subject_resolution",
			Detail: detail,
		},
		At: at,
	}
	// A preparation failure settles the execution before any new workload or
	// request becomes usable, but earlier container workloads and the retained
	// workspace are still owed removal/release. No later Reconcile pass is
	// guaranteed, so the pending cleanup intent/progress travels in this same
	// commit, attributed exactly as the engine's effect-workload selection is.
	workload := firstEffectWorkload(execution)
	commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: "activation_failed"}
	commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}

	next := project(execution, *commit)
	return next, commit, nil
}

// resolveSubject materializes one request subject from admitted inputs and the
// settled outputs of earlier succeeded actions only. A forward or self
// reference, a missing input or a missing earlier output fails resolution.
func resolveSubject(execution engine.Execution, target int, subject definition.Subject) (definition.Subject, error) {
	var inputs map[string]string
	if execution.Snapshot != nil {
		inputs = execution.Snapshot.Inputs()
	}
	succeeded := make(map[id.ActionName]map[id.OutputKey]string, target)
	for index := range target {
		earlier := execution.Actions[index]
		if earlier.State != engine.ActionSucceeded {
			continue
		}
		succeeded[earlier.Name] = earlier.Outputs
	}
	return definition.ResolveSubject(subject, inputs, succeeded)
}

// newLaunchIntent allocates the stable workload identity and idempotency key
// of one launch attempt. The derivation is deterministic for one input, so a
// repeated reduction of the same revision yields the same identity, while a
// fresh attempt is seeded by the previous identity and therefore differs.
func newLaunchIntent(executionID id.ExecutionID, actionID id.ActionID, seed string) engine.LaunchIntent {
	identity := sha256.Sum256([]byte(string(executionID) + "\x00" + string(actionID) + "\x00workload\x00" + seed))
	key := sha256.Sum256([]byte(string(executionID) + "\x00" + string(actionID) + "\x00start\x00" + seed))
	return engine.LaunchIntent{
		WorkloadID:     id.WorkloadID("wl-" + hex.EncodeToString(identity[:16])),
		State:          engine.LaunchNotIssued,
		IdempotencyKey: "start-" + hex.EncodeToString(key[:16]),
	}
}

// relaunchSeed binds a fresh attempt to the current identity so a relaunch
// never reuses the workload identity of the attempt it replaces.
func relaunchSeed(container *engine.ContainerAction) string {
	if container == nil {
		return "relaunch"
	}
	return string(container.WorkloadID) + "\x00" + container.Launch.IdempotencyKey
}

// ---------------------------------------------------------------------------
// Observation
// ---------------------------------------------------------------------------

func reduceObserve(execution engine.Execution, fact ObserveFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	if !fact.Observation.Valid() {
		return execution, nil, refuseInvalid("flow: observation is not internally consistent")
	}
	target := execution.ActionIndex(fact.ActionID)
	if target < 0 {
		return execution, nil, refuseInvalid("flow: action %q does not exist", fact.ActionID)
	}
	action := execution.Actions[target]
	if action.Kind != definition.ActionKindContainer || action.Container == nil {
		return execution, nil, refuseInvalid("flow: action %q is not a container action", action.ID)
	}

	if execution.State.Terminal() {
		// A duplicate observation of the settled evidence converges; any other
		// observation would rewrite terminal history and is refused.
		if action.Container.Observation.Equal(fact.Observation) {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: observation conflicts with the settled state of action %q", action.ID)
	}
	if execution.State != engine.ExecutionRunning || execution.CurrentAction != target || action.State != engine.ActionRunning {
		return execution, nil, refuseInvalid("flow: action %q is not the running action", action.ID)
	}

	// A retiring identity is launch-terminal: it accepts no observation for a
	// new decision. The identical proof converges because removal is still
	// owed; every other observation would rewrite the evidence that justified
	// retirement and is refused.
	if action.Container.Launch.State == engine.LaunchRetiring {
		if action.Container.Observation.Equal(fact.Observation) {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: observation conflicts with the retiring identity of action %q", action.ID)
	}

	observation := cloneObservation(fact.Observation)
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionRunning,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionRunning,
		Output:                   OutputChange{Action: OutputNone},
		Observation:              &observation,
		At:                       fact.At,
	}

	switch observation.Kind {
	case engine.ObservationNeverStarted:
		// Retire-then-replace: affirmative proof of nonexecution only marks the
		// current identity as retiring. The same workload identity and
		// idempotency key are retained with the proof, and no replacement
		// identity exists until removal is confirmed by a retired fact.
		launch := engine.LaunchIntent{
			WorkloadID:     action.Container.WorkloadID,
			State:          engine.LaunchRetiring,
			IdempotencyKey: action.Container.Launch.IdempotencyKey,
		}
		commit.Launch = &launch
		// commit.Observation already retains the never-started proof verbatim;
		// removal retries depend on it until the replacement is committed.
	case engine.ObservationExited:
		commit.StopEvidence = cloneStop(observation.Stop)
		code, ok := observation.ExitStatus()
		if !ok {
			return execution, nil, refuseInvalid("flow: exited observation of action %q has no exit status", action.ID)
		}
		// Settlement and workspace handoff wait for affirmative stop evidence.
		// An exit with a pending stop barrier records evidence and advances
		// nothing.
		if observation.StopConfirmed() {
			nextCurrent := -1
			commit.NextCurrentAction = &nextCurrent
			if code == 0 {
				commit.NextActionState = engine.ActionSucceeded
				commit.Output = promotedOutput(action)
				if allOtherActionsSucceeded(execution, target) {
					commit.NextExecutionState = engine.ExecutionSucceeded
					commit.InvalidateCredential = true
					// The confirmed exit is the last thing the Kernel does for
					// this execution. The terminal commit therefore carries
					// the pending cleanup intent/progress itself, or the
					// earlier workloads and retained workspace would never be
					// selected for removal and release again.
					workload := firstEffectWorkload(execution)
					commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: "execution_succeeded"}
					commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
				}
			} else {
				commit.NextActionState = engine.ActionFailed
				commit.NextExecutionState = engine.ExecutionFailed
				commit.Output = OutputChange{Action: OutputDiscard}
				commit.PendingCancellations = pendingCancellations(execution, action.ID)
				commit.InvalidateCredential = true
				commit.Failure = workloadExitFailure(code, observation.OOMKilled, observation.MemoryLimit)
				// A failed exit settles the execution without a later
				// Reconcile pass, so the pending cleanup intent/progress must
				// travel in this same commit. The just-exited workload counts
				// as an effect workload: its launch was durably issued before
				// the command ran, and an unknown identity may have applied it.
				workload := firstEffectWorkload(execution)
				commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: commit.Failure.Reason}
				commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
			}
		}
	}

	next := project(execution, *commit)
	return next, commit, nil
}

// promotedOutput promotes immutable staging after a runtime-confirmed success.
// No submission means nothing to promote.
func promotedOutput(action engine.Action) OutputChange {
	if !action.StagedOutputsSet && action.StagedOutputs == nil {
		return OutputChange{Action: OutputNone}
	}
	return OutputChange{Action: OutputPromote, Values: action.StagedOutputs.Clone()}
}

// ---------------------------------------------------------------------------
// Container output submission
// ---------------------------------------------------------------------------

func reduceOutput(execution engine.Execution, fact OutputFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	target := execution.ActionIndex(fact.ActionID)
	if target < 0 {
		return execution, nil, refuseInvalid("flow: action %q does not exist", fact.ActionID)
	}
	action := execution.Actions[target]
	if action.Kind != definition.ActionKindContainer || action.Container == nil {
		return execution, nil, refuseInvalid("flow: action %q is not a container action", action.ID)
	}
	if err := fact.Outputs.Validate(action.Container.Definition.Outputs); err != nil {
		return execution, nil, err
	}

	// A settled action's identical retry returns the recorded result, whether or
	// not the execution itself has already reached a terminal state. New content
	// never rewrites settled history.
	if action.State == engine.ActionSucceeded {
		if outputsEqual(action.Outputs, fact.Outputs) {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: output submission conflicts with the settled outputs of action %q", action.ID)
	}
	if action.StagedOutputsSet || action.StagedOutputs != nil {
		if outputsEqual(action.StagedOutputs, fact.Outputs) {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: staged output content for action %q changed", action.ID)
	}
	if execution.State.Terminal() {
		return execution, nil, refuseChanged("flow: output submission conflicts with the settled state of action %q", action.ID)
	}
	if execution.State != engine.ExecutionRunning || execution.CurrentAction != target || action.State != engine.ActionRunning {
		return execution, nil, refuseInvalid("flow: action %q is not the running action", action.ID)
	}

	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionRunning,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionRunning,
		Output:                   OutputChange{Action: OutputStage, Values: fact.Outputs.Clone()},
		At:                       fact.At,
	}
	next := project(execution, *commit)
	return next, commit, nil
}

// ---------------------------------------------------------------------------
// Request answer settlement
// ---------------------------------------------------------------------------

func reduceAnswer(execution engine.Execution, fact AnswerFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	target := execution.ActionIndex(fact.ActionID)
	if target < 0 {
		return execution, nil, refuseInvalid("flow: action %q does not exist", fact.ActionID)
	}
	action := execution.Actions[target]
	if action.Kind != definition.ActionKindRequest || action.Request == nil {
		return execution, nil, refuseInvalid("flow: action %q is not a request action", action.ID)
	}
	if !fact.Answer.Decision.Valid() {
		return execution, nil, refuseInvalid("flow: answer decision %q is not supported", fact.Answer.Decision)
	}
	if !utf8.ValidString(fact.Answer.Note) {
		return execution, nil, refuseInvalid("flow: answer note is not valid UTF-8")
	}
	if len(fact.Answer.Note) > engine.MaxNoteBytes {
		return execution, nil, refuseInvalid("flow: answer note is %d bytes; at most %d are allowed", len(fact.Answer.Note), engine.MaxNoteBytes)
	}
	if err := fact.Answer.Outputs.Validate(action.Request.Definition.Outputs); err != nil {
		return execution, nil, err
	}

	// An identical retry returns the recorded result, whether or not the
	// execution has already reached a terminal state. Replacement content never
	// rewrites a settled answer.
	if action.Request.Answer != nil {
		if answersEqual(action.Request.Answer, fact.Answer) {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: answer for action %q conflicts with the recorded decision", action.ID)
	}
	if execution.State.Terminal() {
		return execution, nil, refuseChanged("flow: answer conflicts with the settled state of action %q", action.ID)
	}
	if execution.State != engine.ExecutionRunning || execution.CurrentAction != target || action.State != engine.ActionRunning {
		return execution, nil, refuseInvalid("flow: action %q is not the running action", action.ID)
	}
	if action.Request.Subject == nil {
		return execution, nil, refuseInvalid("flow: request action %q has no resolved subject", action.ID)
	}
	// Timeliness is decided by the authoritative transaction time only; the
	// sender timestamp is retained but never used. Equality expires.
	if !fact.At.Before(action.Deadline) {
		return execution, nil, fault.New(fault.DeadlineExpired, fmt.Sprintf("flow: action %q is at or past its deadline", action.ID))
	}

	answer := cloneAnswer(fact.Answer)
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionRunning,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionRunning,
		Output:                   OutputChange{Action: OutputNone},
		Answer:                   &answer,
		At:                       fact.At,
	}

	nextCurrent := -1
	if answer.Decision == engine.DecisionPass {
		commit.NextActionState = engine.ActionSucceeded
		commit.NextCurrentAction = &nextCurrent
		commit.Output = OutputChange{Action: OutputPromote, Values: answer.Outputs.Clone()}
		if allOtherActionsSucceeded(execution, target) {
			commit.NextExecutionState = engine.ExecutionSucceeded
			commit.InvalidateCredential = true
			// The answer is the last thing the Kernel does for this
			// execution: no later Reconcile pass follows it. The terminal
			// commit therefore carries the pending cleanup intent/progress
			// itself, or the earlier workloads and retained workspace would
			// never be selected for removal and release again.
			workload := firstEffectWorkload(execution)
			commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: "answer_succeeded"}
			commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
		}
	} else {
		commit.NextActionState = engine.ActionFailed
		commit.NextExecutionState = engine.ExecutionFailed
		commit.NextCurrentAction = &nextCurrent
		commit.Output = OutputChange{Action: OutputDiscard}
		commit.PendingCancellations = pendingCancellations(execution, action.ID)
		commit.InvalidateCredential = true
		reason := "answer_rejected"
		detail := "request action was answered with a failing decision"
		if answer.Decision == engine.DecisionRevise {
			reason = engine.FailureReasonNeedsRevision
			detail = "request action requested revision"
		}
		if trimmed := strings.TrimSpace(answer.Note); trimmed != "" {
			detail = trimmed
		} else if feedback := strings.TrimSpace(answer.Outputs[id.OutputKey("feedback")]); feedback != "" {
			detail = feedback
		} else if outReason := strings.TrimSpace(answer.Outputs[id.OutputKey("reason")]); outReason != "" {
			detail = outReason
		} else if comment := strings.TrimSpace(answer.Outputs[id.OutputKey("comment")]); comment != "" {
			detail = comment
		}
		commit.Failure = &engine.Failure{
			Kind:   engine.FailureRuntime,
			Reason: reason,
			Detail: detail,
		}
		// A failing or revision answer settles the execution without a later
		// Reconcile pass, so the pending cleanup intent/progress must travel in
		// this same commit. The answered action is a request, so this commit
		// requests no new stop; earlier container workloads keep their own
		// persisted stop evidence and cleanup confirms termination before
		// removal.
		workload := firstEffectWorkload(execution)
		commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: reason}
		commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
	}

	next := project(execution, *commit)
	return next, commit, nil
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

func reduceCancel(execution engine.Execution, fact CancelFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	if !fact.Reason.Valid() {
		return execution, nil, refuseInvalid("flow: cancel reason %q is not supported", fact.Reason)
	}
	if execution.State.Terminal() {
		// Explicit cancellation and superseded cancellation converge on an
		// identical retry; every other fact conflicts with terminal history.
		if execution.State == engine.ExecutionCancelled && fact.Reason == CancelRequested && execution.Failure == nil {
			return execution, nil, nil
		}
		if execution.State == engine.ExecutionCancelled && fact.Reason == CancelSuperseded &&
			execution.Failure != nil &&
			execution.Failure.Kind == engine.FailureCancelled &&
			execution.Failure.Reason == string(CancelSuperseded) &&
			execution.Failure.Detail == fact.Detail {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: cancellation conflicts with the settled state %q", execution.State)
	}
	if execution.State != engine.ExecutionQueued && execution.State != engine.ExecutionRunning {
		return execution, nil, refuseInvalid("flow: execution %q cannot be cancelled from state %q", execution.ID, execution.State)
	}

	running, err := runningActionIndex(execution)
	if err != nil {
		return execution, nil, err
	}

	nextCurrent := -1
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionCancelled,
		NextCurrentAction:        &nextCurrent,
		Output:                   OutputChange{Action: OutputDiscard},
		InvalidateCredential:     true,
		At:                       fact.At,
	}
	cleanupReason := "cancelled"
	if fact.Reason == CancelSuperseded {
		commit.Failure = &Failure{
			Kind:   engine.FailureCancelled,
			Reason: string(CancelSuperseded),
			Detail: fact.Detail,
		}
		cleanupReason = string(CancelSuperseded)
	}
	if running >= 0 {
		primary := execution.Actions[running]
		commit.ActionID = primary.ID
		commit.ExpectedActionVersion = primary.Version
		commit.NextActionState = engine.ActionCancelled
	}
	commit.PendingCancellations = pendingCancellations(execution, commit.ActionID)

	workload := id.WorkloadID("")
	if running >= 0 {
		primary := execution.Actions[running]
		workload = containerWorkload(primary)
		if containerNeedsStop(primary) {
			stop := StopIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
			commit.Stop = &stop
		}
	}
	commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: cleanupReason}
	commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}

	next := project(execution, *commit)
	return next, commit, nil
}

// ---------------------------------------------------------------------------
// Deadline expiry
// ---------------------------------------------------------------------------

func reduceExpire(execution engine.Execution, fact ExpireFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	target := execution.ActionIndex(fact.ActionID)
	if target < 0 {
		return execution, nil, refuseInvalid("flow: action %q does not exist", fact.ActionID)
	}
	action := execution.Actions[target]

	if execution.State.Terminal() {
		// Re-observing the same expired deadline converges. Any other expiry
		// would replace the recorded terminal cause and is refused.
		if execution.State == engine.ExecutionFailed &&
			execution.Failure != nil && execution.Failure.Kind == engine.FailureTimeout &&
			action.State == engine.ActionFailed && !action.Deadline.IsZero() &&
			!fact.At.Before(action.Deadline) {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: expiry conflicts with the settled state of action %q", action.ID)
	}
	if execution.State != engine.ExecutionRunning || execution.CurrentAction != target || action.State != engine.ActionRunning {
		return execution, nil, refuseInvalid("flow: action %q is not the running action", action.ID)
	}
	if action.Deadline.IsZero() {
		return execution, nil, refuseInvalid("flow: running action %q has no persisted deadline", action.ID)
	}
	// The persisted deadline is authoritative and is never recalculated from a
	// changed timeout. Reaching it expires; it is not strictly-before.
	if fact.At.Before(action.Deadline) {
		return execution, nil, refuseInvalid("flow: deadline %s of action %q has not been reached", action.Deadline.UTC().Format(time.RFC3339Nano), action.ID)
	}

	nextCurrent := -1
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionFailed,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionFailed,
		PendingCancellations:     pendingCancellations(execution, action.ID),
		NextCurrentAction:        &nextCurrent,
		Output:                   OutputChange{Action: OutputDiscard},
		InvalidateCredential:     true,
		Failure: &engine.Failure{
			Kind:   engine.FailureTimeout,
			Reason: "deadline_reached",
			Detail: fmt.Sprintf("action %q reached its persisted deadline", action.ID),
		},
		Deadline: action.Deadline,
		At:       fact.At,
	}

	workload := containerWorkload(action)
	if containerNeedsStop(action) {
		stop := StopIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
		commit.Stop = &stop
	}
	commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: "deadline_expired"}
	commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}

	next := project(execution, *commit)
	return next, commit, nil
}

// ---------------------------------------------------------------------------
// Workspace loss
// ---------------------------------------------------------------------------

func reduceWorkspaceLost(execution engine.Execution, fact WorkspaceLostFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	if fact.WorkspaceID != execution.WorkspaceID {
		return execution, nil, refuseInvalid("flow: workspace %q is not the retained workspace of execution %q", fact.WorkspaceID, execution.ID)
	}
	if execution.State.Terminal() {
		if execution.State == engine.ExecutionFailed &&
			execution.Failure != nil && execution.Failure.Kind == engine.FailureWorkspaceLost {
			return execution, nil, nil
		}
		return execution, nil, refuseChanged("flow: workspace loss conflicts with the settled state %q", execution.State)
	}
	if execution.State != engine.ExecutionQueued && execution.State != engine.ExecutionRunning {
		return execution, nil, refuseInvalid("flow: execution %q cannot observe workspace loss from state %q", execution.ID, execution.State)
	}

	running, err := runningActionIndex(execution)
	if err != nil {
		return execution, nil, err
	}

	nextCurrent := -1
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionFailed,
		NextCurrentAction:        &nextCurrent,
		Output:                   OutputChange{Action: OutputDiscard},
		InvalidateCredential:     true,
		Failure: &engine.Failure{
			Kind:   engine.FailureWorkspaceLost,
			Reason: "workspace_missing",
			Detail: fmt.Sprintf("retained workspace %q is no longer available", execution.WorkspaceID),
		},
		At: fact.At,
	}
	if running >= 0 {
		primary := execution.Actions[running]
		commit.ActionID = primary.ID
		commit.ExpectedActionVersion = primary.Version
		commit.NextActionState = engine.ActionFailed
	}
	commit.PendingCancellations = pendingCancellations(execution, commit.ActionID)

	// Losing the workspace is not evidence that the workload stopped. When the
	// failed action owns a live container workload whose stop is unconfirmed,
	// the same failure commit must request the stop so termination stays
	// attributable; the workspace is never reconstructed. Cleanup progress is
	// recorded as pending exactly as cancellation and expiry do, with the
	// workload identity left empty when no container workload is involved.
	workload := id.WorkloadID("")
	if running >= 0 {
		primary := execution.Actions[running]
		workload = containerWorkload(primary)
		if containerNeedsStop(primary) {
			stop := StopIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID}
			commit.Stop = &stop
		}
	}
	commit.Cleanup = &CleanupIntent{WorkloadID: workload, WorkspaceID: execution.WorkspaceID, Reason: "workspace_lost"}
	commit.CleanupUpdate = &CleanupUpdate{State: engine.CleanupPending, WorkloadID: workload, WorkspaceID: execution.WorkspaceID}

	next := project(execution, *commit)
	return next, commit, nil
}

// ---------------------------------------------------------------------------
// Retirement
// ---------------------------------------------------------------------------

// reduceRetired consumes the confirmed removal of the retiring identity and
// commits its deterministic replacement. Removal is confirmed separately from
// the retirement decision, so this is the only place a replacement identity is
// allocated. A terminal execution never receives one, and nothing but the
// launch identity and the new identity's unknown observation changes: the
// persisted deadline, activation time, outputs and stop evidence are untouched.
func reduceRetired(execution engine.Execution, fact RetiredFact) (engine.Execution, *Commit, error) {
	if err := checkExecutionID(execution, fact.ExecutionID); err != nil {
		return execution, nil, err
	}
	if execution.State.Terminal() {
		return execution, nil, refuseChanged("flow: retired fact conflicts with the settled state %q of execution %q", execution.State, execution.ID)
	}
	target := execution.ActionIndex(fact.ActionID)
	if target < 0 {
		return execution, nil, refuseInvalid("flow: action %q does not exist", fact.ActionID)
	}
	action := execution.Actions[target]
	if action.Kind != definition.ActionKindContainer || action.Container == nil {
		return execution, nil, refuseInvalid("flow: action %q is not a container action", action.ID)
	}
	if execution.State != engine.ExecutionRunning || execution.CurrentAction != target || action.State != engine.ActionRunning {
		return execution, nil, refuseInvalid("flow: action %q is not the running action", action.ID)
	}
	if action.Container.Launch.State != engine.LaunchRetiring {
		return execution, nil, refuseInvalid("flow: action %q is not retiring a workload identity", action.ID)
	}
	if fact.WorkloadID != action.Container.WorkloadID {
		return execution, nil, refuseInvalid("flow: workload %q is not the retiring identity %q of action %q", fact.WorkloadID, action.Container.WorkloadID, action.ID)
	}
	// A replacement is justified only by the persisted affirmative proof that
	// made removal safe. A malformed aggregate without it is refused.
	if !action.Container.Observation.ProofOfNonexecution() {
		return execution, nil, refuseInvalid("flow: retiring action %q has no persisted proof of nonexecution", action.ID)
	}

	// The replacement is derived deterministically from the retired identity,
	// so a retried retired fact on the same revision yields the same identity
	// and never reuses the retired workload id or key.
	launch := newLaunchIntent(execution.ID, action.ID, relaunchSeed(action.Container))
	unknown := engine.UnknownObservation()
	commit := &Commit{
		ExecutionID:              execution.ID,
		ExpectedExecutionVersion: execution.Version,
		NextExecutionState:       engine.ExecutionRunning,
		ActionID:                 action.ID,
		ExpectedActionVersion:    action.Version,
		NextActionState:          engine.ActionRunning,
		Output:                   OutputChange{Action: OutputNone},
		Launch:                   &launch,
		Observation:              &unknown,
		At:                       fact.At,
	}
	next := project(execution, *commit)
	return next, commit, nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

func checkExecutionID(execution engine.Execution, executionID id.ExecutionID) error {
	if executionID != execution.ID {
		return refuseInvalid("flow: fact execution %q does not match execution %q", executionID, execution.ID)
	}
	return nil
}

// runningActionIndex returns the single active action index, or -1. Two running
// actions are an illegal aggregate and are refused rather than guessed.
func runningActionIndex(execution engine.Execution) (int, error) {
	found := -1
	for index, action := range execution.Actions {
		if action.State != engine.ActionRunning {
			continue
		}
		if found >= 0 {
			return -1, refuseInvalid("flow: execution %q has more than one running action", execution.ID)
		}
		found = index
	}
	return found, nil
}

// pendingCancellations cancels every pending action except the primary one.
// A failure, cancellation, timeout or workspace loss therefore never leaves a
// pending successor behind.
func pendingCancellations(execution engine.Execution, primary id.ActionID) []ActionUpdate {
	var updates []ActionUpdate
	for _, action := range execution.Actions {
		if action.ID == primary || action.State != engine.ActionPending {
			continue
		}
		updates = append(updates, ActionUpdate{
			ActionID:        action.ID,
			ExpectedVersion: action.Version,
			NextState:       engine.ActionCancelled,
		})
	}
	return updates
}

func allOtherActionsSucceeded(execution engine.Execution, target int) bool {
	for index, action := range execution.Actions {
		if index == target {
			continue
		}
		if action.State != engine.ActionSucceeded {
			return false
		}
	}
	return true
}

func containerWorkload(action engine.Action) id.WorkloadID {
	if action.Kind != definition.ActionKindContainer || action.Container == nil {
		return ""
	}
	return action.Container.WorkloadID
}

// containerNeedsStop reports whether settling an active action requires a stop
// intent. Only an unconfirmed issued or unknown launch may have started a
// command; a not_issued or retiring identity has no live workload to stop.
func containerNeedsStop(action engine.Action) bool {
	if action.Kind != definition.ActionKindContainer ||
		action.Container == nil ||
		action.Container.WorkloadID == "" ||
		action.Stop.IsConfirmed() {
		return false
	}
	switch action.Container.Launch.State {
	case engine.LaunchIssued, engine.LaunchUnknown:
		return true
	default:
		return false
	}
}

// firstEffectWorkload returns the workload identity that a terminal answer must
// attribute cleanup to. It selects the first container action whose start may
// have been applied, exactly as the engine's cleanup selection does: a
// container action owning a recorded identity whose launch is not affirmatively
// never-issued. Issued, unknown and retiring identities all stay owed removal
// after the execution settles, including an earlier action that already
// succeeded. A request-only execution owns no workload, so the cleanup intent
// releases the retained workspace alone.
func firstEffectWorkload(execution engine.Execution) id.WorkloadID {
	for _, action := range execution.Actions {
		if action.Kind != definition.ActionKindContainer || action.Container == nil {
			continue
		}
		if action.Container.WorkloadID == "" {
			continue
		}
		if action.Container.Launch.State == engine.LaunchNotIssued {
			continue
		}
		return action.Container.WorkloadID
	}
	return ""
}

// project applies one commit to an execution exactly as the state port
// must: the expected versions are incremented once, the primary action and
// every pending cancellation are settled, the current-action index, evidence,
// output change, failure and cleanup progress move in the same step. The
// reducer returns this projection so a caller never needs to re-derive it.
func project(execution engine.Execution, commit Commit) engine.Execution {
	next := execution
	next.Version = commit.ExpectedExecutionVersion + 1
	next.State = commit.NextExecutionState
	next.Actions = append([]engine.Action(nil), execution.Actions...)
	if commit.Failure != nil {
		failure := *commit.Failure
		next.Failure = &failure
	}
	if update := commit.CleanupUpdate; update != nil {
		next.Cleanup = engine.Cleanup{
			State:       update.State,
			WorkloadID:  update.WorkloadID,
			WorkspaceID: update.WorkspaceID,
			Attempts:    update.Attempts,
			LastError:   update.LastError,
		}
	}
	if commit.NextCurrentAction != nil {
		next.CurrentAction = *commit.NextCurrentAction
	}
	if commit.ActionID != "" {
		index := next.ActionIndex(commit.ActionID)
		if index >= 0 {
			action := &next.Actions[index]
			if action.Container != nil {
				container := *action.Container
				action.Container = &container
			}
			if action.Request != nil {
				request := *action.Request
				action.Request = &request
			}
			action.Version = commit.ExpectedActionVersion + 1
			action.State = commit.NextActionState
			if !commit.Deadline.IsZero() {
				action.Deadline = commit.Deadline
			}
			if action.State == engine.ActionRunning && action.ActivatedAt.IsZero() {
				action.ActivatedAt = commit.At
			}
			switch commit.Output.Action {
			case OutputStage:
				action.StagedOutputs = commit.Output.Values.Clone()
				action.StagedOutputsSet = true
			case OutputPromote:
				action.Outputs = commit.Output.Values.Clone()
				action.StagedOutputs = nil
				action.StagedOutputsSet = false
			case OutputDiscard:
				action.StagedOutputs = nil
				action.StagedOutputsSet = false
			}
			if commit.Subject != nil {
				if action.Request == nil {
					action.Request = &engine.RequestAction{}
				}
				subject := *commit.Subject
				action.Request.Subject = &subject
			}
			if commit.Answer != nil {
				if action.Request == nil {
					action.Request = &engine.RequestAction{}
				}
				answer := cloneAnswer(*commit.Answer)
				action.Request.Answer = &answer
			}
			if commit.Observation != nil {
				if action.Container == nil {
					action.Container = &engine.ContainerAction{}
				}
				action.Container.Observation = cloneObservation(*commit.Observation)
			}
			if commit.StopEvidence != nil {
				action.Stop = *commit.StopEvidence
			}
			if commit.Launch != nil {
				if action.Container == nil {
					action.Container = &engine.ContainerAction{}
				}
				action.Container.Launch = *commit.Launch
				action.Container.WorkloadID = commit.Launch.WorkloadID
				if action.Container.WorkspaceID == "" {
					action.Container.WorkspaceID = next.WorkspaceID
				}
			}
		}
	}
	for _, update := range commit.PendingCancellations {
		index := next.ActionIndex(update.ActionID)
		if index < 0 {
			continue
		}
		next.Actions[index].Version = update.ExpectedVersion + 1
		next.Actions[index].State = update.NextState
	}
	return next
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

func refuseInvalid(format string, args ...any) error {
	return fault.New(fault.InvalidInput, fmt.Sprintf(format, args...))
}

func refuseChanged(format string, args ...any) error {
	return fault.New(fault.ChangedIdempotencyContent, fmt.Sprintf(format, args...))
}

func outputsEqual(left, right engine.Outputs) bool {
	if (left == nil) != (right == nil) {
		return false
	}
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		other, ok := right[key]
		if !ok || other != value {
			return false
		}
	}
	return true
}

func cloneAnswer(answer engine.Answer) engine.Answer {
	cloned := answer
	cloned.Outputs = answer.Outputs.Clone()
	cloned.Provenance = append([]byte(nil), answer.Provenance...)
	return cloned
}

// answersEqual compares the settlement content a retry must reproduce: the
// decision, the declared outputs, the decision note and the authorized
// principal. Sender timestamps and transport provenance are ignored metadata,
// so an identical retry carrying a different Answer.At or different Provenance
// still converges on the recorded result. A different decision, output map,
// note or principal remains a replacement settlement and is refused.
func answersEqual(recorded *engine.Answer, candidate engine.Answer) bool {
	if recorded == nil {
		return false
	}
	return recorded.Decision == candidate.Decision &&
		outputsEqual(recorded.Outputs, candidate.Outputs) &&
		recorded.Note == candidate.Note &&
		recorded.Actor == candidate.Actor
}

func cloneObservation(observation engine.Observation) engine.Observation {
	cloned := observation
	if observation.ExitCode != nil {
		code := *observation.ExitCode
		cloned.ExitCode = &code
	}
	cloned.Stop = cloneStop(observation.Stop)
	return cloned
}

func cloneStop(stop *engine.StopEvidence) *engine.StopEvidence {
	if stop == nil {
		return nil
	}
	cloned := *stop
	return &cloned
}

// workloadExitFailure builds the typed terminal diagnostic for a nonzero
// workload exit, surfacing OOMKilled and the configured memory limit when the
// runtime observed an out-of-memory kill.
func workloadExitFailure(code int, oomKilled bool, memoryLimit string) *engine.Failure {
	if oomKilled {
		detail := fmt.Sprintf("OOMKilled: workload exceeded its memory limit (exit %d)", code)
		if memoryLimit != "" {
			detail = fmt.Sprintf("OOMKilled: workload exceeded its %s memory limit (exit %d)", memoryLimit, code)
		}
		return &engine.Failure{
			Kind:   engine.FailureRuntime,
			Reason: "oom_killed",
			Detail: detail,
		}
	}
	return &engine.Failure{
		Kind:   engine.FailureRuntime,
		Reason: "workload_exited",
		Detail: fmt.Sprintf("workload exited with status %d", code),
	}
}
