//go:build component

package deploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestK3sReferenceValuesRenderCleanly proves deploy/k3s/values.yaml renders a
// complete, PodSecurity-hardened kodema-stack release with environment=test,
// KODEMA_RUNTIME=kubernetes, keyfile staging, neutral StorageClass, and
// in-cluster OAuthSonas, with zero cloud credentials.
func TestK3sReferenceValuesRenderCleanly(t *testing.T) {
	helm, stack := stageStackChart(t)
	valuesFile := filepath.Join(repoRoot(t), "deploy", "k3s", "values.yaml")

	rendered, err := renderChartArgs(t, helm, "kodema", stack, "--namespace", "kodema-system", "-f", valuesFile)
	if err != nil {
		t.Fatalf("helm template kodema-stack with deploy/k3s/values.yaml: %v\n%s", err, rendered)
	}

	assertRenderedPodSecurity(t, rendered)
	assertNoRenderedJob(t, rendered)
	assertNoCredentialSecretReference(t, rendered)

	if count := renderedKindCount(rendered, "CiliumNetworkPolicy"); count != 0 {
		t.Errorf("k3s values rendered %d CiliumNetworkPolicy objects; default k3s has no Cilium CRD", count)
	}

	deployment := renderedDocument(t, rendered, "Deployment", "kodema")
	for _, want := range []struct {
		name  string
		value string
	}{
		{"KODEMA_PROFILE", "test"},
		{"KODEMA_RUNTIME", "kubernetes"},
		{"KODEMA_KEY_ADAPTER", "keyfile"},
		{"KODEMA_KEY_REF", "keyfile:kek-v1"},
		{"KODEMA_KEYFILE", "/etc/kodema/keyfile/kek-v1.key"},
		{"KODEMA_TOOLBOX_IMAGE", "ghcr.io/optimiware/kodema/kodema:0.8.2"},
		{"KODEMA_SECRET_RULES_FILE", "/etc/kodema/secret-rules/secret-rules.json"},
		{"KODEMA_MODELS_FILE", "/etc/kodema/models/models.json"},
		{"KODEMA_WORKSPACE_PVC_STORAGE_CLASS", ""},
	} {
		pattern := regexp.MustCompile(`(?ms)- name: ` + regexp.QuoteMeta(want.name) + `\s*\r?\n\s*value: "` + regexp.QuoteMeta(want.value) + `"`)
		if !pattern.MatchString(deployment) {
			t.Errorf("k3s Deployment missing %s=%q:\n%s", want.name, want.value, deployment)
		}
	}
	if !strings.Contains(deployment, "name: prepare-keyfile") {
		t.Error("k3s Deployment does not include prepare-keyfile initContainer")
	}
	if !strings.Contains(deployment, "medium: Memory") {
		t.Error("k3s Deployment does not stage keyfile on a memory-backed emptyDir")
	}

	if count := renderedKindCount(rendered, "Deployment"); count < 2 {
		t.Errorf("k3s stack rendered %d Deployments; expected kodema + oauthsonas", count)
	}
	_ = renderedObjectNamed(t, rendered, "ConfigMap", "kodema-secret-rules")
	_ = renderedObjectNamed(t, rendered, "ConfigMap", "kodema-models")

	// Passing an explicit image.digest still pins KODEMA_TOOLBOX_IMAGE by digest.
	digest := "sha256:" + strings.Repeat("b", 64)
	pinned, err := renderChartArgs(t, helm, "kodema", stack, "--namespace", "kodema-system", "-f", valuesFile,
		"--set", "kodema.image.digest="+digest)
	if err != nil {
		t.Fatalf("helm template kodema-stack with digest override: %v\n%s", err, pinned)
	}
	pinnedDep := renderedObjectNamed(t, pinned, "Deployment", "kodema")
	if got := stringAtKey(t, renderedEnvEntry(t, pinnedDep, "KODEMA_TOOLBOX_IMAGE"), "value"); got != "ghcr.io/optimiware/kodema/kodema@"+digest {
		t.Errorf("k3s KODEMA_TOOLBOX_IMAGE with digest = %q, want ghcr.io/optimiware/kodema/kodema@%s", got, digest)
	}
}

// TestChartsDefaultWorkspaceStorageClassIsNeutral pins the cloud-neutral
// default StorageClass ("") in both charts so clusters such as k3s bind
// workspace PVCs to their default StorageClass without Scaleway overrides.
func TestChartsDefaultWorkspaceStorageClassIsNeutral(t *testing.T) {
	for _, relative := range []string{
		filepath.Join("deploy", "charts", "kodema", "values.yaml"),
		filepath.Join("deploy", "charts", "kodema-stack", "values.yaml"),
	} {
		body := readFileForTest(t, filepath.Join(repoRoot(t), relative))
		if !regexp.MustCompile(`(?ms)workspacePVC:.*?storageClass:[ \t]*""`).MatchString(body) {
			t.Errorf("%s does not default workspacePVC.storageClass to empty string", relative)
		}
		if regexp.MustCompile(`(?m)^[ \t]*storageClass:[ \t]*"?sbs-default"?[ \t]*$`).MatchString(body) {
			t.Errorf("%s still sets an uncommented Scaleway storageClass sbs-default", relative)
		}
	}
}

// TestK3sInstallDryRunGeneratesLocalAssets verifies deploy/k3s/install.sh
// --dry-run runs without contacting Kubernetes, generates the local PKI,
// 32-byte keyfile, NATS NKey pair and Helm values, and passes helm template.
func TestK3sInstallDryRunGeneratesLocalAssets(t *testing.T) {
	_ = helmBinary(t)
	root := repoRoot(t)
	stateDir := filepath.Join(t.TempDir(), "k3s-state")

	cmd := exec.Command(filepath.Join(root, "deploy", "k3s", "install.sh"), "--dry-run")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "KODEMA_K3S_STATE_DIR="+stateDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deploy/k3s/install.sh --dry-run failed: %v\n%s", err, out)
	}

	for _, rel := range []string{
		".kodema-k3s",
		"pki/ca.crt",
		"pki/kodema.crt",
		"pki/nats-server.crt",
		"pki/nats-client.crt",
		"pki/postgres.crt",
		"pki/oauthsonas.crt",
		"nats-auth/issuer.seed",
		"nats-auth/issuer.pub",
		"secrets/kek-v1.key",
		"secrets/database_url",
		"values.yaml",
	} {
		path := filepath.Join(stateDir, rel)
		info, statErr := os.Stat(path)
		if statErr != nil || info.Size() == 0 {
			t.Errorf("dry-run did not produce non-empty %s (err=%v)", rel, statErr)
		}
	}

	keyBytes, err := os.ReadFile(filepath.Join(stateDir, "secrets", "kek-v1.key"))
	if err != nil {
		t.Fatalf("read kek-v1.key: %v", err)
	}
	if len(keyBytes) != 32 {
		t.Fatalf("kek-v1.key length = %d; want 32", len(keyBytes))
	}
}
