//go:build component

package runtimeconformance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
	runtimesupport "github.com/optimiware/kodema/test/support/infrastructure/runtime"
)

// TestPodmanRuntimeManifestEnvironmentPermissionsAndExit proves the real
// Podman runtime satisfies the S07 §3 materialization contract: read-only
// activation bind, isolated 0700 host secrets directory, shared /etc/kodema/ca
// CA bind with env precedence, WorkingDir == /workspace, cgroup resource
// defaults, redacted logs, absent-workload observation, and retained workspace
// separation across Remove and Release.
func TestPodmanRuntimeManifestEnvironmentPermissionsAndExit(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-1")
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, runtimesupport.GenerateTestCAPEM(t, "kodema-podman-component-ca"), 0o600); err != nil {
		t.Fatalf("write workload CA bundle: %v", err)
	}
	fixture.caFile = caFile
	fixture.libpod, fixture.files, fixture.adapter = newPodmanAdapter(t, fixture, nil)

	t.Setenv("KODEMA_HOST_CANARY", "must-not-leak")

	h := fixture.harness(t)
	h.AfterExit = func(t testing.TB, ctx context.Context, identity reconcile.EffectIdentity) {
		t.Helper()
		// Host secret confinement: <root>/<workload>/secrets is 0700 with 0444
		// files, and <root>/<workload>/kodema/secrets is an empty mount point.
		assertPodmanSecretConfinement(t, fixture.root, identity.WorkloadID,
			runtimesupport.StandardCredential,
			map[string]string{string(runtimesupport.StandardSecretName): runtimesupport.StandardSecretValue})

		// Activation files are explicit 0444 and CA is never copied into kodema/.
		for _, rel := range []string{"inputs/input-a", "script"} {
			host := filepath.Join(fixture.root, string(identity.WorkloadID), "kodema", rel)
			if info, err := os.Lstat(host); err != nil || info.Mode().Perm() != 0o444 {
				t.Fatalf("activation file %s mode = %v, %v; want 0444", rel, info, err)
			}
		}
		if _, err := os.Lstat(filepath.Join(fixture.root, string(identity.WorkloadID), "kodema", podman.WorkloadCAKey)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("CA bundle was copied into <root>/<workload>/kodema: err = %v", err)
		}

		// Container inspection: WorkingDir == /workspace and default 2 CPU / 2 GiB limits.
		containerName, err := podman.ContainerName(identity.WorkloadID)
		if err != nil {
			t.Fatalf("ContainerName: %v", err)
		}
		wd, err := runtimesupport.RunPodman(ctx, "inspect", "--format", "{{.Config.WorkingDir}}", containerName)
		if err != nil {
			t.Fatalf("inspect WorkingDir: %v", err)
		}
		if got := strings.TrimSpace(string(wd.Stdout)); got != "/workspace" {
			t.Fatalf("container WorkingDir = %q; want /workspace", got)
		}
		limits, err := runtimesupport.RunPodman(ctx, "inspect", "--format",
			"{{.HostConfig.CpuQuota}} {{.HostConfig.CpuPeriod}} {{.HostConfig.Memory}}", containerName)
		if err != nil {
			t.Fatalf("inspect resource limits: %v", err)
		}
		wantQuota := (podman.DefaultCPUMillicores * podman.DefaultCPUPeriod) / 1000
		wantLimits := fmt.Sprintf("%d %d %d", wantQuota, podman.DefaultCPUPeriod, podman.DefaultMemoryBytes)
		if got := strings.TrimSpace(string(limits.Stdout)); got != wantLimits {
			t.Fatalf("container resource limits = %q; want %q (default 2 CPUs / 2 GiB)", got, wantLimits)
		}

		evidence, err := fixture.adapter.Stop(ctx, reconcile.StopRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if !evidence.IsConfirmed() {
			t.Fatalf("stop evidence = %+v; want confirmed", evidence)
		}
	}

	runtimesupport.AssertMaterializationContract(t, ctx, h)
	runtimesupport.AssertAbsentObservation(t, ctx, h)

	if !runtimesupport.VolumeExists(ctx, fixture.volumeName) {
		t.Fatal("Remove touched the retained workspace volume")
	}
	if err := fixture.adapter.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if runtimesupport.VolumeExists(ctx, fixture.volumeName) {
		t.Fatal("workspace volume still exists after Release")
	}
	runtimesupport.RecordResult(t, runtimesupport.ResultRecord{
		Adapter: "podman", Scenario: "manifest-environment-permissions-exit",
		ImageTag: fixture.image.Tag, ImageDigest: fixture.image.Digest,
		Podman: runtimesupport.PodmanVersion(ctx),
	})
}

// assertPodmanSecretConfinement verifies the S07 §3 host secret layout:
// <root>/<workload>/secrets has 0700 mode, contains 0444 secret/credential
// files with expected contents, and <root>/<workload>/kodema/secrets is an
// empty mount-point directory with no host secret files.
func assertPodmanSecretConfinement(
	t testing.TB,
	root string,
	workload id.WorkloadID,
	wantToken string,
	wantSecrets map[string]string,
) {
	t.Helper()
	secretsDir := filepath.Join(root, string(workload), "secrets")
	dirInfo, err := os.Lstat(secretsDir)
	if err != nil {
		t.Fatalf("stat isolated secrets dir %s: %v", secretsDir, err)
	}
	if !dirInfo.IsDir() || dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("secrets dir %s mode = %o (dir=%t); want 0700 directory", secretsDir, dirInfo.Mode().Perm(), dirInfo.IsDir())
	}

	mountPoint := filepath.Join(root, string(workload), "kodema", "secrets")
	entries, err := os.ReadDir(mountPoint)
	if err != nil {
		t.Fatalf("read /kodema/secrets mount point %s: %v", mountPoint, err)
	}
	if len(entries) != 0 {
		t.Fatalf("host %s holds %d entries; want empty mount point with no secret files", mountPoint, len(entries))
	}

	checkFile := func(name, want string) {
		t.Helper()
		path := filepath.Join(secretsDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read isolated secret %s: %v", path, err)
		}
		if string(data) != want {
			t.Fatalf("isolated secret %s = %q; want %q", name, data, want)
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != 0o444 {
			t.Fatalf("isolated secret %s mode = %v, %v; want 0444", name, info, err)
		}
	}
	checkFile(string(id.SecretTokenName), wantToken)
	for name, want := range wantSecrets {
		checkFile(name, want)
	}
}

// readVolume reads one file from the fixture's retained workspace volume.
func readVolume(t testing.TB, ctx context.Context, fixture *podmanFixture, logical string) []byte {
	t.Helper()
	data, found := runtimesupport.ReadVolumeFile(ctx, t, fixture.volumeName, logical)
	if !found {
		t.Fatalf("workspace file %q is missing", logical)
	}
	return data
}
