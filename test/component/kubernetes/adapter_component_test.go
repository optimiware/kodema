//go:build component

package kubernetesconformance

import (
	"context"
	"strings"
	"testing"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
	runtimesupport "github.com/optimiware/kodema/test/support/infrastructure/runtime"
)

// TestAdapterActionEndToEnd proves one complete adapter action on a real
// cluster satisfies the S07 materialization contract, the pinned Job security
// policy, combined-log redaction, absent-workload observation, retained cleanup
// and release.
func TestAdapterActionEndToEnd(t *testing.T) {
	t.Parallel()
	workspaceID := id.WorkspaceID("workspace-kube-9")
	fx, ctx := newFixture(t, workspaceID, fixtureOptions{withClaim: true})
	claimName, err := runtimekube.WorkspaceClaimName(workspaceID)
	if err != nil {
		t.Fatalf("WorkspaceClaimName: %v", err)
	}

	h := fx.harness(t, workspaceID)
	h.AfterExit = func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity) {
		t.Helper()
		job := oneJob(t, ctx, fx.Client, identity)
		assertJobSecurityPolicy(t, ctx, fx, job.Name)
		// Stock clusters cannot split the streams: stderr stays empty and all
		// output arrives in stdout.
		stdout, stderr := runtimesupport.ReadAllLogs(t, ctx, fx.Adapter, identity, 1<<20)
		if !strings.Contains(stdout, "[REDACTED][REDACTED]") || len(stderr) != 0 {
			t.Fatalf("combined log = %q, stderr = %q; want redacted combined log with empty stderr", stdout, stderr)
		}
	}

	runtimesupport.AssertMaterializationContract(t, ctx, h)
	runtimesupport.AssertAbsentObservation(t, ctx, h)

	releaseWorkspaceEventually(t, ctx, fx, workspaceID)
	if err := waitForClaimGone(ctx, fx.Client, claimName); err != nil {
		t.Fatalf("released claim is still present: %v", err)
	}
}

// assertJobSecurityPolicy reads the live Job through kubectl and proves the
// no-replay, no-token, no-host-network and bounded-resource policy survived the
// API server.
func assertJobSecurityPolicy(t testing.TB, ctx context.Context, fx *fixture, jobName string) {
	t.Helper()
	document, err := fx.Cluster.GetJSON(ctx, "get", "job", jobName, "-n", testNamespace)
	if err != nil {
		t.Fatalf("read live Job: %v", err)
	}
	spec, _ := document["spec"].(map[string]any)
	if value, _ := spec["backoffLimit"].(float64); value != 0 {
		t.Fatalf("backoffLimit = %v; want 0 (never retried)", spec["backoffLimit"])
	}
	if value, _ := spec["podReplacementPolicy"].(string); value != "Failed" {
		t.Fatalf("podReplacementPolicy = %q; want Failed", value)
	}
	if value, _ := spec["ttlSecondsAfterFinished"].(float64); value != 120 {
		t.Fatalf("ttlSecondsAfterFinished = %v; want 120", spec["ttlSecondsAfterFinished"])
	}
	template, _ := spec["template"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	if value, _ := podSpec["restartPolicy"].(string); value != "Never" {
		t.Fatalf("restartPolicy = %q; want Never", value)
	}
	if automount, ok := podSpec["automountServiceAccountToken"]; !ok || automount != false {
		t.Fatalf("automountServiceAccountToken = %v (present %v); want an explicit false", automount, ok)
	}
	if hostNetwork, ok := podSpec["hostNetwork"]; ok && hostNetwork != false {
		t.Fatalf("hostNetwork = %v; want false", hostNetwork)
	}
	if serviceAccount, ok := podSpec["serviceAccountName"]; ok && serviceAccount != "" {
		t.Fatalf("serviceAccountName = %v; want the default account", serviceAccount)
	}

	containers, _ := podSpec["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("containers = %d; want exactly 1", len(containers))
	}
	container, _ := containers[0].(map[string]any)
	if name, _ := container["name"].(string); name != "workload" {
		t.Fatalf("container name = %q; want workload", name)
	}
	resources, _ := container["resources"].(map[string]any)
	limits, _ := resources["limits"].(map[string]any)
	if cpu, _ := limits["cpu"].(string); cpu != "200m" {
		t.Fatalf("cpu limit = %q; want 200m", cpu)
	}
	if memory, _ := limits["memory"].(string); memory != "128Mi" {
		t.Fatalf("memory limit = %q; want 128Mi", memory)
	}

	volumes, _ := podSpec["volumes"].([]any)
	var hasClaim, hasConfigMap, hasSecret bool
	for _, raw := range volumes {
		volume, _ := raw.(map[string]any)
		name, _ := volume["name"].(string)
		if strings.HasPrefix(name, "kube-api-access-") {
			t.Fatalf("Job has a service-account token volume %q", name)
		}
		switch name {
		case "workspace":
			claim, _ := volume["persistentVolumeClaim"].(map[string]any)
			if claimName, _ := claim["claimName"].(string); claimName == "" {
				t.Fatalf("workspace volume has no claim name: %v", volume)
			}
			hasClaim = true
		case "activation":
			if configMap, ok := volume["configMap"].(map[string]any); ok {
				if name, _ := configMap["name"].(string); name != "" {
					hasConfigMap = true
				}
			}
		case "credential":
			if secret, ok := volume["secret"].(map[string]any); ok {
				if name, _ := secret["secretName"].(string); name != "" {
					hasSecret = true
				}
			}
		}
	}
	if !hasClaim || !hasConfigMap || !hasSecret {
		t.Fatalf("Job volumes = %v; want the workspace claim, activation snapshot and credential secret", volumes)
	}
}
