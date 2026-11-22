// Observation, stop and log evidence for the Process runtime adapter.
//
// The rules here keep uncertainty visible: absence and a signal acknowledgment
// never collapse into exit or stop confirmation, and only affirmative
// never-created evidence reports never_started.
package process

import (
	"fmt"
	"strings"

	"github.com/optimiware/kodema/internal/kernel"
)

// observationFor maps persisted spawn state and process-group evidence to the
// declared Kernel vocabulary. Only affirmative evidence is proof.
func observationFor(state groupState, status GroupStatus) kernel.Observation {
	switch state.State {
	case groupStarted:
		return observationForStatus(status)
	case groupNeverStarted:
		return kernel.NeverStarted(nonEmptyProof(state.Detail))
	case groupSpawning, groupUnknown:
		return observationForUncertainSpawn(state, status)
	}
	if status.NeverCreated {
		return kernel.NeverStarted(nonEmptyProof(state.Detail))
	}
	return observationForStatus(status)
}

// observationForUncertainSpawn reports what the observer affirmatively knows
// about a persisted spawn intent or lost spawn response. A running group is
// running, an affirmatively terminated group reports its leader exit, and only
// affirmative never-created evidence proves nonexecution. Zero or ambiguous
// evidence stays unknown and never collapses into absence.
func observationForUncertainSpawn(state groupState, status GroupStatus) kernel.Observation {
	if status.NeverCreated {
		return kernel.NeverStarted(nonEmptyProof(state.Detail))
	}
	if !status.Present && !status.MembersAlive && !status.LeaderExited && !status.GroupGone {
		return kernel.UnknownObservation()
	}
	return observationForStatus(status)
}

// observationForStatus reports exit only for affirmative whole-group
// termination with a leader exit status.
func observationForStatus(status GroupStatus) kernel.Observation {
	switch {
	case status.GroupGone && status.LeaderExited:
		return kernel.ExitedObservation(status.ExitCode, kernel.ConfirmedStop(
			fmt.Sprintf("process group affirmatively terminated with exit code %d", status.ExitCode),
		))
	case status.GroupGone:
		// Whole-group termination without a leader status is still not an
		// exit status.
		return kernel.UnknownObservation()
	case status.MembersAlive || status.Present || status.LeaderExited:
		return kernel.RunningObservation()
	default:
		return kernel.AbsentObservation()
	}
}

// nonEmptyProof never fabricates an empty never-started proof.
func nonEmptyProof(detail string) string {
	if proof := strings.TrimSpace(detail); proof != "" {
		return proof
	}
	return "the process executor affirmatively created no process"
}
