package podman

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
)

func generateTestCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kodema-local-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestPodmanWorkloadCAMountsReadOnlyCertAndExposesEnvOptIn(t *testing.T) {
	caPEM := generateTestCAPEM(t)
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	// A bundle that also carries a key must reach the workload without it.
	bundle := append(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("key")}), caPEM...)
	if err := os.WriteFile(caFile, bundle, 0o600); err != nil {
		t.Fatal(err)
	}

	fx := newFixture(t)
	adapter, err := New(Config{
		Libpod:         fx.libpod,
		Files:          fx.files,
		Root:           fx.root,
		KodemaURL:      testKodemaURL,
		WorkloadCAFile: caFile,
		Cleanup:        CleanupPolicy{Attempts: 2},
		Clock:          fx.clock,
	})
	if err != nil {
		t.Fatalf("New with WorkloadCAFile: %v", err)
	}
	fx.adapter = adapter
	// Attempt to spoof KODEMA_CA_FILE in literal env; the adapter must override
	// it with the authoritative /etc/kodema/ca/ca.crt path.
	fx.request.Container.Env = append(fx.request.Container.Env,
		definition.EnvVar{Name: EnvWorkloadCAFile, Value: "/tmp/evil.crt"})

	fx.prepare(t)

	// The CA bundle must live under <root>/.ca/<digest>/ca.crt and never be
	// copied into <root>/<workload>/kodema.
	sharedCA := filepath.Join(fx.adapter.caDir, WorkloadCAKey)
	if filepath.Dir(fx.adapter.caDir) != filepath.Join(fx.root, caDirectory) {
		t.Fatalf("caDir = %q; want under %q", fx.adapter.caDir, filepath.Join(fx.root, caDirectory))
	}
	if got := readHostFile(t, sharedCA); string(got) != string(caPEM) {
		t.Fatalf("shared CA bundle mismatch")
	}
	if mode := hostFileMode(t, sharedCA); mode != 0o444 {
		t.Fatalf("shared CA bundle mode = %o; want 0444", mode)
	}
	if hostFileExists(t, filepath.Join(fx.kodemaRoot(), WorkloadCAKey)) {
		t.Fatal("Prepare copied ca.crt into <root>/<workload>/kodema; must use shared /etc/kodema/ca bind mount")
	}

	fx.start(t)
	specs := fx.libpod.createSpecs()
	if len(specs) != 1 {
		t.Fatalf("createSpecs = %d; want 1", len(specs))
	}
	caMount, ok := mountAt(specs[0], WorkloadCAMountPath)
	if !ok {
		t.Fatalf("mounts %+v missing %s bind mount", specs[0].Mounts, WorkloadCAMountPath)
	}
	if caMount.Type != "bind" || !caMount.ReadOnly || caMount.Source != fx.adapter.caDir {
		t.Fatalf("CA mount = %+v; want read-only bind from %q", caMount, fx.adapter.caDir)
	}
	wantCAEnv := EnvWorkloadCAFile + "=" + WorkloadCAPath
	if !slices.Contains(specs[0].Env, wantCAEnv) {
		t.Fatalf("container env %v missing %q", specs[0].Env, wantCAEnv)
	}
	if slices.Contains(specs[0].Env, EnvWorkloadCAFile+"=/tmp/evil.crt") {
		t.Fatalf("container env %v allowed spoofing %s", specs[0].Env, EnvWorkloadCAFile)
	}

	// A second workload reuses the shared CA file without rewriting it, and
	// removing the first workload keeps the shared CA intact.
	writes := fx.files.countPath("WriteFile", sharedCA)
	second := fx.request
	second.Identity = reconcile.EffectIdentity{
		ExecutionID:    "exec-1",
		ActionID:       "action-2",
		WorkloadID:     "workload-2",
		IdempotencyKey: "idem-2",
	}
	if _, err := fx.adapter.Prepare(context.Background(), second); err != nil {
		t.Fatalf("Prepare of second workload: %v", err)
	}
	if got := fx.files.countPath("WriteFile", sharedCA); got != writes {
		t.Fatalf("shared CA was written %d more time(s) for a second workload", got-writes)
	}
	fx.seedExited(t, 0, true)
	if removal := fx.remove(t); !removal.Removed {
		t.Fatalf("Remove = %+v; want success", removal)
	}
	if !hostFileExists(t, sharedCA) {
		t.Fatal("Remove deleted the shared CA bundle")
	}
}

func TestPodmanWorkloadCARefusesInvalidFiles(t *testing.T) {
	badPEM := filepath.Join(t.TempDir(), "bad.crt")
	if err := os.WriteFile(badPEM, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		path string
	}{
		{"relative path", "ca.crt"},
		{"missing file", filepath.Join(t.TempDir(), "absent.crt")},
		{"non-PEM content", badPEM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(Config{
				Libpod:         newFakeLibpod(),
				Files:          newFakeFiles(),
				Root:           t.TempDir(),
				KodemaURL:      testKodemaURL,
				WorkloadCAFile: tc.path,
				Cleanup:        CleanupPolicy{Attempts: 1},
				Clock:          newFakeClock(),
			})
			assertFaultClass(t, err, fault.InvalidInput)
		})
	}
}
