// Observation, stop and redaction evidence for the Kubernetes runtime adapter.
//
// The rules here keep uncertainty visible: absence, node loss, a terminating
// pod without terminated-container evidence and a Job without a pod never
// collapse into exit or stop confirmation.
package kube

import (
	"fmt"
	"strings"

	"github.com/optimiware/kodema/internal/adapter/runtime/internal/adapterutil"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
)

// workloadSelector is the only pod selector the adapter uses. The workload
// label is the stable effect identity; namespace ownership provides the tenant
// boundary.
func workloadSelector(identity reconcile.EffectIdentity) map[string]string {
	return map[string]string{LabelWorkload: string(identity.WorkloadID)}
}

// workloadContainerName is the single workload container the adapter creates.
const workloadContainerName = "workload"

// terminatedContainer returns the status of the workload container only when it
// is affirmatively terminated. A pod phase alone is not termination evidence,
// and a terminated sidecar must not confirm the workload's exit.
func terminatedContainer(pod Pod) (ContainerStatus, bool) {
	for _, container := range pod.Containers {
		if container.Name == workloadContainerName && container.Terminated {
			return container, true
		}
	}
	// The seam may omit container names; accept the unambiguous single
	// container case but never guess among several.
	if len(pod.Containers) == 1 && pod.Containers[0].Name == "" && pod.Containers[0].Terminated {
		return pod.Containers[0], true
	}
	return ContainerStatus{}, false
}

// terminatedExit returns the exit code of the workload container only when it
// is affirmatively terminated.
func terminatedExit(pod Pod) (int, bool) {
	status, ok := terminatedContainer(pod)
	return status.ExitCode, ok
}

// exitedObservationFor builds an exited or OOM-killed Kernel observation from
// one affirmatively terminated workload container.
func exitedObservationFor(podName string, status ContainerStatus, fallbackMemoryLimit string, terminating bool) engine.Observation {
	if status.Reason == "OOMKilled" {
		memLimit := strings.TrimSpace(status.MemoryLimit)
		if memLimit == "" {
			memLimit = strings.TrimSpace(fallbackMemoryLimit)
		}
		detail := fmt.Sprintf("pod %q was OOMKilled (exit code %d)", podName, status.ExitCode)
		if terminating {
			detail = fmt.Sprintf("pod %q was OOMKilled (exit code %d) while terminating", podName, status.ExitCode)
		}
		return engine.OOMKilledObservation(status.ExitCode, memLimit, engine.ConfirmedStop(detail))
	}
	detail := fmt.Sprintf("pod %q terminated with exit code %d", podName, status.ExitCode)
	if terminating {
		detail = fmt.Sprintf("pod %q terminated with exit code %d while terminating", podName, status.ExitCode)
	}
	return engine.ExitedObservation(status.ExitCode, engine.ConfirmedStop(detail))
}

// observationFor maps Job and pod evidence to the declared Kernel vocabulary.
func observationFor(jobFound bool, pods []Pod, fallbackMemoryLimit string) engine.Observation {
	switch {
	case len(pods) == 0:
		if jobFound {
			// The Job exists but has not scheduled a pod yet: work is still
			// outstanding, not absent.
			return engine.RunningObservation()
		}
		return engine.AbsentObservation()
	case len(pods) > 1:
		// Two pods for one stable identity mean a replay or replacement. Never
		// silently pick one.
		return engine.UnknownObservation()
	}
	pod := pods[0]
	if pod.DeletionTimestamp != nil {
		// A deletion request alone is not termination proof, but a pod that is
		// terminating after its workload container affirmatively terminated
		// (for example TTL cleanup of a finished Job) still reports that exit.
		if status, ok := terminatedContainer(pod); ok {
			return exitedObservationFor(pod.Name, status, fallbackMemoryLimit, true)
		}
		// Termination was requested but no container termination was observed:
		// still running work.
		return engine.RunningObservation()
	}
	switch pod.Phase {
	case "Running", "Pending":
		return engine.RunningObservation()
	case "Succeeded", "Failed":
		if status, ok := terminatedContainer(pod); ok {
			return exitedObservationFor(pod.Name, status, fallbackMemoryLimit, false)
		}
		// A terminal phase without terminated-container evidence is not proof.
		return engine.UnknownObservation()
	default:
		// Unknown node, empty phase and any other state stay uncertain.
		return engine.UnknownObservation()
	}
}

// stopEvidenceFor reports a confirmed stop only for one affirmatively
// terminated pod. Job deletion, an absent pod, a terminating pod whose workload
// container has not terminated, node loss and elapsed time all stay pending.
func stopEvidenceFor(pods []Pod) engine.StopEvidence {
	if len(pods) != 1 {
		return engine.PendingStop(fmt.Sprintf("stop unconfirmed: %d workload pods observed", len(pods)))
	}
	pod := pods[0]
	if pod.DeletionTimestamp != nil {
		// A terminating pod confirms the barrier only when its workload
		// container affirmatively terminated; otherwise it may still be running.
		if code, ok := terminatedExit(pod); ok {
			return engine.ConfirmedStop(fmt.Sprintf("pod %q terminated with exit code %d while terminating", pod.Name, code))
		}
		return engine.PendingStop(fmt.Sprintf("stop unconfirmed: pod %q is terminating without terminated-container evidence", pod.Name))
	}
	switch pod.Phase {
	case "Succeeded", "Failed":
		if code, ok := terminatedExit(pod); ok {
			return engine.ConfirmedStop(fmt.Sprintf("pod %q terminated with exit code %d", pod.Name, code))
		}
	}
	return engine.PendingStop(fmt.Sprintf("stop unconfirmed: pod %q phase %q has no terminated container", pod.Name, pod.Phase))
}

// jobConditionTrue reports whether one Job condition carries the exact
// affirmative server status. A "False" or "Unknown" status never counts.
func jobConditionTrue(job Job, kind string) bool {
	for _, condition := range job.Status.Conditions {
		if condition.Type == kind && condition.Status == "True" {
			return true
		}
	}
	return false
}

// stopJobIdentityMismatch names the first way an observed Job differs from the
// persisted identity Stop intends to suspend. It returns "" when the Job is the
// intended object.
func stopJobIdentityMismatch(job Job, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, namespace, tenant string) string {
	if job.Namespace != namespace || job.Name != workloadJobName(identity.WorkloadID) {
		return "Job is not the intended object in this namespace"
	}
	if job.Labels[LabelTenant] != tenant {
		return "Job is not owned by this tenant"
	}
	if job.Labels[LabelWorkload] != string(identity.WorkloadID) {
		return "Job is bound to a different workload identity"
	}
	if job.Labels[LabelWorkspace] != string(workspaceID) {
		return "Job is bound to a different workspace"
	}
	if job.Annotations[AnnotationIdempotencyKey] != identity.IdempotencyKey {
		return "Job carries a different idempotency key"
	}
	if job.UID == "" {
		return "Job has no immutable UID"
	}
	return ""
}

// suspensionConfirmed is the fully guarded Job-status branch of the stop
// barrier. All three fresh Job observations must name the same expected UID and
// the intended owned object with no deletion timestamp; the final observation
// must additionally show the suspension intent, the controller's
// acknowledgment (or an already Complete/Failed Job), zero active pods and a
// present zero terminating count. It is only consulted when a successful fresh
// workload-pod listing observed no pod at all, so a potentially live pod can
// never satisfy it.
func suspensionConfirmed(before, mid, after Job, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, namespace, tenant, expectedUID string) bool {
	if expectedUID == "" {
		return false
	}
	for _, job := range []Job{before, mid, after} {
		if job.UID != expectedUID {
			return false
		}
		if stopJobIdentityMismatch(job, identity, workspaceID, namespace, tenant) != "" {
			return false
		}
		if job.DeletionTimestamp != nil {
			return false
		}
	}
	if after.Spec.Suspend == nil || !*after.Spec.Suspend {
		return false
	}
	acknowledged := jobConditionTrue(after, "Suspended") ||
		jobConditionTrue(after, "Complete") ||
		jobConditionTrue(after, "Failed")
	if !(acknowledged) {
		return false
	}
	if after.Status.Active != 0 {
		return false
	}
	if after.Status.Terminating == nil || *after.Status.Terminating != 0 {
		return false
	}
	return true
}

// podTerminated reports whether a single pod's workload container has
// affirmatively terminated and can no longer write to a retained workspace. A
// terminating pod without terminated-container evidence is not terminated.
func podTerminated(pod Pod) bool {
	if pod.DeletionTimestamp != nil {
		// TTL cleanup of a finished Job sets a deletion timestamp on an already
		// terminated pod; a live terminating container is still not releasable.
		_, terminated := terminatedExit(pod)
		return terminated
	}
	switch pod.Phase {
	case "Succeeded", "Failed":
		_, terminated := terminatedExit(pod)
		return terminated
	default:
		return false
	}
}

// workspaceReleasable reports whether no live workload can still write to the
// retained workspace for one workload identity. Only no pod, or exactly one pod
// whose workload container affirmatively terminated, qualifies; multiple pods
// for one workload identity stay unreleasable.
func workspaceReleasable(pods []Pod) bool {
	if len(pods) == 0 {
		return true
	}
	if len(pods) > 1 {
		return false
	}
	return podTerminated(pods[0])
}

// redactionValues returns every configured value that must be removed from log
// output, longest first so a shorter value cannot partially obscure a longer
// one.
func redactionValues(secret Secret) []string {
	values := make([]string, 0, len(secret.Data))
	for _, value := range secret.Data {
		if len(value) == 0 {
			continue
		}
		values = append(values, string(value))
	}
	return adapterutil.SortRedactionValues(values)
}
