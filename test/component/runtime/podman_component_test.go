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

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/adapter/runtime/podman"
	"github.com/optimiware/kodema/internal/engine"
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
		// Host secret confinement: <root>/<workload>/secrets is 0700, its
		// bind-mounted child <root>/<workload>/secrets/data is 0755 with 0444
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

// TestPodmanRuntimeNonRootSecretsAndCredentialRefresh proves a container image
// configured with a non-root USER (1000:1000) can traverse /kodema/secrets,
// read declared secrets, KODEMA_TOKEN, activation inputs, and the shared CA
// bundle, and observe live RefreshCredential updates while host confinement
// (<root>/<workload>/secrets mode 0700) still holds.
func TestPodmanRuntimeNonRootSecretsAndCredentialRefresh(t *testing.T) {
	ctx := t.Context()
	fixture := newPodmanFixture(t, "workspace-podman-nonroot")
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caFile, runtimesupport.GenerateTestCAPEM(t, "kodema-podman-nonroot-ca"), 0o600); err != nil {
		t.Fatalf("write workload CA bundle: %v", err)
	}
	fixture.caFile = caFile
	fixture.libpod, fixture.files, fixture.adapter = newPodmanAdapter(t, fixture, nil)

	nonRootImage := buildNonRootImage(t, ctx, fixture.image.Tag)
	identity := testIdentity("exec-nonroot", "action-nonroot", "workload-nonroot", "idem-nonroot")

	commands := []string{
		`test "$(id -u)" = 1000`,
		`test "$(cat /kodema/secrets/API_KEY)" = "` + runtimesupport.StandardSecretValue + `"`,
		`test "$(cat /kodema/secrets/KODEMA_TOKEN)" = "` + runtimesupport.StandardCredential + `"`,
		`test "$(tr '\n' '|' < /kodema/inputs/input-a)" = "line1|line2|"`,
		`test -r "` + container.WorkloadCAPath + `" && test -s "` + container.WorkloadCAPath + `"`,
		`if ( : >> /kodema/secrets/KODEMA_TOKEN ) 2>/dev/null; then exit 1; fi`,
		`if ( : >> /kodema/secrets/API_KEY ) 2>/dev/null; then exit 1; fi`,
		`printf 'nonroot-ready\n'`,
		`i=0; while [ "$i" -lt 200 ]; do if [ "$(cat /kodema/secrets/KODEMA_TOKEN)" = "` + runtimesupport.RefreshedCredential + `" ]; then cat /kodema/secrets/API_KEY; cat /kodema/secrets/KODEMA_TOKEN; printf '\nnonroot-refreshed-ok\n'; exit 0; fi; i=$((i+1)); sleep 0.1; done; exit 1`,
	}
	req := runtimesupport.StandardPrepareRequest(identity, fixture.workspaceID, nonRootImage.Reference, commands)
	if _, err := fixture.adapter.Prepare(ctx, req); err != nil {
		t.Fatalf("Prepare(non-root): %v", err)
	}
	start, err := fixture.adapter.Start(ctx, reconcile.StartRequest{Identity: identity, WorkspaceID: fixture.workspaceID})
	if err != nil || start.Outcome != reconcile.StartAccepted {
		t.Fatalf("Start(non-root) = %+v, %v; want accepted", start, err)
	}

	waitObservation(t, ctx, fixture.adapter, reconcile.ObserveRequest{
		Identity: identity, WorkspaceID: fixture.workspaceID,
	}, engine.ObservationRunning)
	assertPodmanSecretConfinement(t, fixture.root, identity.WorkloadID,
		runtimesupport.StandardCredential,
		map[string]string{string(runtimesupport.StandardSecretName): runtimesupport.StandardSecretValue})

	refresh, err := fixture.adapter.RefreshCredential(ctx, reconcile.CredentialRequest{
		Identity:   identity,
		Credential: runtimesupport.RefreshedCredential,
	})
	if err != nil || !refresh.Delivered {
		t.Fatalf("RefreshCredential(non-root) = %+v, %v; want delivered", refresh, err)
	}

	obs := waitObservation(t, ctx, fixture.adapter, reconcile.ObserveRequest{
		Identity: identity, WorkspaceID: fixture.workspaceID,
	}, engine.ObservationExited)
	stdout, stderr := runtimesupport.ReadAllLogs(t, ctx, fixture.adapter, identity, container.MaxLogBytes)
	combined := stdout + stderr
	if code, ok := obs.ExitStatus(); !ok || code != 0 {
		t.Fatalf("non-root exit status = %d/%v; want 0/true (%+v)\nlogs:\n%s", code, ok, obs, combined)
	}
	if !runtimesupport.ContainsLine(combined, "nonroot-refreshed-ok") {
		t.Fatalf("non-root logs missing nonroot-refreshed-ok:\n%s", combined)
	}
	for _, secret := range []string{runtimesupport.StandardSecretValue, runtimesupport.StandardCredential, runtimesupport.RefreshedCredential} {
		if strings.Contains(combined, secret) {
			t.Fatalf("secret or token leaked into non-root logs: %q", combined)
		}
	}
	if !strings.Contains(combined, container.Redacted) {
		t.Fatalf("redaction marker absent from non-root logs: %q", combined)
	}

	if removal, err := fixture.adapter.Remove(ctx, reconcile.RemoveRequest{Identity: identity, WorkspaceID: fixture.workspaceID}); err != nil || !removal.Removed {
		t.Fatalf("Remove(non-root) = %+v, %v; want removed", removal, err)
	}
	if err := fixture.adapter.Release(ctx, fixture.workspaceID); err != nil {
		t.Fatalf("Release(non-root): %v", err)
	}
}

// buildNonRootImage builds a derived local image from baseTag setting
// USER 1000:1000 and returns its digest-pinned reference.
func buildNonRootImage(t *testing.T, ctx context.Context, baseTag string) runtimesupport.PinnedImage {
	t.Helper()
	buildDir := t.TempDir()
	containerfile := fmt.Sprintf("FROM %s\nUSER 1000:1000\n", baseTag)
	if err := os.WriteFile(filepath.Join(buildDir, "Containerfile"), []byte(containerfile), 0o600); err != nil {
		t.Fatalf("write non-root Containerfile: %v", err)
	}
	repo := "localhost/" + runtimesupport.RunScoped("kodema-nonroot")
	tag := repo + ":1000"
	if res, err := runtimesupport.RunPodman(ctx, "build", "-q", "-t", tag, buildDir); err != nil {
		t.Fatalf("podman build non-root image: %v (stderr: %s)", err, string(res.Stderr))
	}
	t.Cleanup(func() {
		_, _ = runtimesupport.RunPodman(context.Background(), "rmi", "-f", tag)
	})
	inspected, err := runtimesupport.RunPodman(ctx, "inspect", "--format", "{{.Digest}}", tag)
	if err != nil {
		t.Fatalf("podman inspect non-root image digest: %v (stderr: %s)", err, string(inspected.Stderr))
	}
	digest := strings.TrimSpace(string(inspected.Stdout))
	if digest == "" {
		t.Fatalf("non-root image %q returned empty digest", tag)
	}
	return runtimesupport.PinnedImage{
		Tag:       tag,
		Digest:    digest,
		Reference: repo + "@" + digest,
	}
}

// assertPodmanSecretConfinement verifies the S07 §3 host secret layout:
// <root>/<workload>/secrets has 0700 mode (host confinement), its bind-mounted
// child <root>/<workload>/secrets/data has 0755 mode containing 0444
// secret/credential files with expected contents, and
// <root>/<workload>/kodema/secrets is an empty mount-point directory with no
// host secret files.
func assertPodmanSecretConfinement(
	t testing.TB,
	root string,
	workload id.WorkloadID,
	wantToken string,
	wantSecrets map[string]string,
) {
	t.Helper()
	secretsParent := filepath.Join(root, string(workload), "secrets")
	parentInfo, err := os.Lstat(secretsParent)
	if err != nil {
		t.Fatalf("stat isolated secrets parent %s: %v", secretsParent, err)
	}
	if !parentInfo.IsDir() || parentInfo.Mode().Perm() != 0o700 {
		t.Fatalf("secrets parent %s mode = %o (dir=%t); want 0700 directory", secretsParent, parentInfo.Mode().Perm(), parentInfo.IsDir())
	}

	secretsDir := filepath.Join(secretsParent, "data")
	dirInfo, err := os.Lstat(secretsDir)
	if err != nil {
		t.Fatalf("stat isolated secrets bind dir %s: %v", secretsDir, err)
	}
	if !dirInfo.IsDir() || dirInfo.Mode().Perm() != 0o755 {
		t.Fatalf("secrets bind dir %s mode = %o (dir=%t); want 0755 directory", secretsDir, dirInfo.Mode().Perm(), dirInfo.IsDir())
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
