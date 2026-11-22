//go:build integration

package lifecycle

// Regression: a Kubernetes preparation failure must not strand the
// unconsumed workspace claim. The workspace is retained (and its PVC created)
// before the workload is prepared; when preparation fails before Start is
// issued, cleanup must remove the failed identity's partial resources, release
// the claim and record the attribution instead of waiting forever on an absent
// object and an unconfirmed Stop.
//
// This test runs on the real kind composition and is intentionally not part of
// the unit baseline.

import (
	"errors"
	"testing"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/test/support/factory"
)

// TestKubernetesPreparationFailureCleanup proves that boundary on a real
// kind cluster: an activation secret refused during preparation fails the
// execution, and cleanup converges to complete with the failed identity named,
// the unconsumed claim released and no workload ephemera left behind.
func TestKubernetesPreparationFailureCleanup(t *testing.T) {
	acquireLifecycleKindSlot(t)
	harness := factory.StartKubernetes(t, deniedSecretOptions()...)

	document := deniedSecretPipeline(t, harness, kubeActionTimeout)
	applied := applyPipeline(t, harness, "preparation-failure", document)
	started := startPipeline(t, harness, harness.ProjectToken, applied.PipelineID, nil, "start-preparation-failure")
	if started.ExecutionID == "" {
		t.Fatalf("pipeline.start returned an empty execution id: %+v", started)
	}

	// The workspace is retained before preparation, so its deterministic claim
	// name is available from the durable execution row even though the workload
	// never started.
	workspaceID := waitWorkspaceID(t, harness, started.ExecutionID)
	claimName, err := runtimekube.WorkspaceClaimName(id.WorkspaceID(workspaceID))
	if err != nil {
		t.Fatalf("WorkspaceClaimName(%q): %v", workspaceID, err)
	}

	execution := waitKubernetesExecution(t, harness, started.ExecutionID)
	assertPreparationFailure(t, execution, "denied", workspaceID)

	// No partial Job, Pod, Secret or ConfigMap survives the failed identity, and
	// the unconsumed claim is gone. Both are bounded observable barriers.
	waitJobsGone(t, harness, started.ExecutionID)
	waitExecutionEphemeraGone(t, harness, started.ExecutionID)
	waitClaimGone(t, harness, claimName)
	if _, err := harness.KubernetesClient.GetClaim(t.Context(), factory.KubernetesNamespace, claimName); !errors.Is(err, runtimekube.ErrNotFound) {
		t.Fatalf("unconsumed workspace claim read = %v; want ErrNotFound", err)
	}
}
