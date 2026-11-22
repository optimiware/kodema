// Kubernetes-specific workload CA Secret projection: Secret volume shape
// (literal ca.crt key only, 0444, confined outside reserved roots), DNS-1123
// Secret name validation, and isolation from control-plane TLS Secrets and
// service-account tokens.
//
// Shared CA mount path and KODEMA_CA_FILE environment invariants are verified
// in TestKubeConformance.
package kube

import (
	"context"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/fault"
)

const (
	testWorkloadCASecret = "kodema-workload-ca"
	testCAPEM            = "-----BEGIN CERTIFICATE-----\nMIIB-fake-workload-ca\n-----END CERTIFICATE-----\n"
	testPrivateKeyPEM    = "-----BEGIN PRIVATE KEY-----\nMIIE-fake-serving-key\n-----END PRIVATE KEY-----\n"
)

func testSetupWithWorkloadCA(t *testing.T, secretName string) (*fakeCluster, *fakeClock, *Adapter) {
	t.Helper()
	fake := newFakeCluster()
	addTestNamespace(fake)
	addTestClaim(t, fake, testWorkspaceID())
	clock := newFakeClock()
	config := testConfig(fake, clock)
	config.WorkloadCA = WorkloadCAConfig{SecretName: secretName}
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("New with workload CA %q returned error: %v", secretName, err)
	}
	return fake, clock, adapter
}

func TestWorkloadCASecretVolumeProjectionAndConfinement(t *testing.T) {
	fake, _, adapter := testSetupWithWorkloadCA(t, testWorkloadCASecret)
	request := testPrepareRequest()
	prepareTestWorkload(t, adapter, request)
	startTestWorkload(t, adapter, request.Identity, request.WorkspaceID)
	job := onlyJob(t, fake)

	volume := testVolumeAt(t, job.Spec, WorkloadCAMountPath)
	if volume.SecretName != testWorkloadCASecret || volume.ConfigMapName != "" || volume.ClaimName != "" || !volume.ReadOnly {
		t.Fatalf("CA volume = %+v; want read-only Secret %q", volume, testWorkloadCASecret)
	}
	if len(volume.Items) != 1 || volume.Items[0].Key != WorkloadCAKey || volume.Items[0].Path != WorkloadCAPath || volume.Items[0].Mode != container.ReadOnlyFileMode {
		t.Fatalf("CA volume items = %+v; want single literal %q at %q mode %#o", volume.Items, WorkloadCAKey, WorkloadCAPath, container.ReadOnlyFileMode)
	}

	// Confined outside workspace, activation, and credential roots; unique volume names.
	for _, reserved := range []string{workspace.WorkspaceRoot, container.ActivationRoot, workspace.SecretRoot, workspace.ReservedCredentialPath} {
		if pathContains(volume.MountPath, reserved) || pathContains(reserved, volume.MountPath) {
			t.Errorf("CA mount %q overlaps reserved path %q", volume.MountPath, reserved)
		}
	}
	seen := map[string]bool{}
	for _, v := range job.Spec.Volumes {
		if v.Name == "" || seen[v.Name] {
			t.Errorf("invalid or duplicate volume name %q", v.Name)
		}
		seen[v.Name] = true
	}
}

func TestWorkloadCARefusesInvalidSecretNamesAndAcceptsDNS1123(t *testing.T) {
	invalidNames := []string{
		"../escape", "Bad Name", "ca_secret", "/etc/passwd", "   ", " a", "a\n", "é", "CA-Secret",
		"-ca", "ca-", "a..b", ".a", "a.", "a.-b", "a-.b", "..",
		strings.Repeat("a", 254),
		strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("a", 63), strings.Repeat("a", 63), strings.Repeat("a", 62)}, "."),
		strings.Repeat("a", 64),
	}
	for _, name := range invalidNames {
		invalidFake := newFakeCluster()
		config := testConfig(invalidFake, newFakeClock())
		config.WorkloadCA = WorkloadCAConfig{SecretName: name}
		_, err := New(config)
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("New(%q) class = (%v, %v) for %v; want %q", name, class, ok, err, fault.InvalidInput)
		}
		if invalidFake.callTotal() != 0 {
			t.Fatalf("New(%q) made cluster calls", name)
		}
	}

	validNames := []string{
		"a", "0a", "a.b", "ca.example", "kodema-workload-ca",
		strings.Repeat("a", 63),
		strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("a", 63), strings.Repeat("a", 63), strings.Repeat("a", 61)}, "."),
	}
	for _, name := range validNames {
		fake, _, adapter := testSetupWithWorkloadCA(t, name)
		req := testPrepareRequest()
		prepareTestWorkload(t, adapter, req)
		startTestWorkload(t, adapter, req.Identity, req.WorkspaceID)
		if vol := testVolumeAt(t, onlyJob(t, fake).Spec, WorkloadCAMountPath); vol.SecretName != name {
			t.Fatalf("CA volume secret = %q; want %q", vol.SecretName, name)
		}
	}
}

func TestWorkloadTrustDoesNotExposeControlPlaneCredentials(t *testing.T) {
	fake, _, adapter := testSetupWithWorkloadCA(t, testWorkloadCASecret)

	// Misconfigured CA Secret that also carries a serving key: only ca.crt is projected.
	if _, err := fake.PutSecret(context.Background(), Secret{
		Name:      testWorkloadCASecret,
		Namespace: testNamespace,
		Data: map[string][]byte{
			WorkloadCAKey: []byte(testCAPEM),
			"tls.key":     []byte(testPrivateKeyPEM),
			"tls.crt":     []byte("-----BEGIN CERTIFICATE-----\nMIIB-fake-serving-cert\n-----END CERTIFICATE-----\n"),
		},
	}); err != nil {
		t.Fatalf("seed CA Secret: %v", err)
	}

	const controlPlaneSecret = "kodema-control-plane-tls"
	if _, err := fake.PutSecret(context.Background(), Secret{
		Name:      controlPlaneSecret,
		Namespace: testNamespace,
		Data:      map[string][]byte{"tls.key": []byte(testPrivateKeyPEM)},
	}); err != nil {
		t.Fatalf("seed control-plane Secret: %v", err)
	}

	request := testPrepareRequest()
	prepareTestWorkload(t, adapter, request)
	startTestWorkload(t, adapter, request.Identity, request.WorkspaceID)
	job := onlyJob(t, fake)

	caVolume := testVolumeAt(t, job.Spec, WorkloadCAMountPath)
	if len(caVolume.Items) != 1 || caVolume.Items[0].Key != WorkloadCAKey {
		t.Fatalf("CA volume projected unexpected items: %+v", caVolume.Items)
	}
	for _, volume := range job.Spec.Volumes {
		if volume.SecretName == controlPlaneSecret || volume.Name == controlPlaneSecret || strings.HasPrefix(volume.Name, "kube-api-access-") {
			t.Fatalf("job referenced forbidden volume %+v", volume)
		}
	}
	if job.Spec.AutomountServiceAccountToken {
		t.Fatal("job automounted service-account token")
	}
}

func pathContains(parent, child string) bool {
	return parent == child || strings.HasPrefix(child, parent+"/")
}
