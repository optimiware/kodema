package main

// The `kodema services --kind merge_coordinator` entry shape.
//
// A merge coordinator needs its durable state directory, the Git base URL and
// the mounted service credential. Every service takes the credential
// flag; the state directory and Git URL are merge-specific.

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
	"strings"
	"testing"
	"time"

	mergeapp "github.com/optimiware/kodema/internal/apps/merge/app"
)

// TestServicesMergeCoordinatorRequiresStateAndGitURL pins the merge-coordinator
// flag contract: the adapter cannot start without durable state and a Git URL.
func TestServicesMergeCoordinatorRequiresStateAndGitURL(t *testing.T) {
	credential := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credential, []byte("token"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	base := []string{
		"--kind", "merge_coordinator",
		"--service", "merge-svc",
		"--tenant", "tenant-1",
		"--project", "project-1",
		"--base-url", "http://127.0.0.1:1",
		"--credential-file", credential,
	}
	cases := map[string][]string{
		"no state directory": append(append([]string(nil), base...), "--git-url", "file:///srv/git"),
		"no Git URL":         append(append([]string(nil), base...), "--state-dir", t.TempDir()),
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			var stderr lockedBuffer
			code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
			if code != 2 {
				t.Fatalf("exit code = %d; want 2 (stderr=%q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "merge coordinator requires --state-dir and --git-url") {
				t.Fatalf("stderr = %q; want the merge coordinator requirement", stderr.String())
			}
		})
	}
}

// TestServicesMergeCoordinatorForwardsCAFile pins that `--ca-file` (and
// `KODEMA_CA_FILE`) is propagated into servicesapp.MergeConfig so child git
// commands trust the same private CA bundle as the Kodema API client.
func TestServicesMergeCoordinatorForwardsCAFile(t *testing.T) {
	dir := t.TempDir()
	credential := filepath.Join(dir, "credential")
	if err := os.WriteFile(credential, []byte("token"), 0o600); err != nil {
		t.Fatalf("WriteFile credential: %v", err)
	}
	caFile := writeTestSelfSignedCA(t, filepath.Join(dir, "ca.crt"))

	for _, tc := range []struct {
		name      string
		envCAFile string
		extraArgs []string
	}{
		{
			name:      "via flag",
			extraArgs: []string{"--ca-file", caFile},
		},
		{
			name:      "via KODEMA_CA_FILE",
			envCAFile: caFile,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KODEMA_CA_FILE", tc.envCAFile)
			var captured mergeapp.MergeConfig
			called := false
			orig := runMergeCoordinator
			runMergeCoordinator = func(_ context.Context, cfg mergeapp.MergeConfig) error {
				called = true
				captured = cfg
				return nil
			}
			t.Cleanup(func() { runMergeCoordinator = orig })

			args := append([]string{
				"--kind", "merge_coordinator",
				"--service", "merge-svc",
				"--tenant", "tenant-1",
				"--project", "project-1",
				"--base-url", "https://127.0.0.1:8443",
				"--credential-file", credential,
				"--state-dir", filepath.Join(dir, "state"),
				"--git-url", "https://127.0.0.1:8443/git",
			}, tc.extraArgs...)

			var stderr lockedBuffer
			code := runServicesEntry(context.Background(), args, &lockedBuffer{}, &stderr)
			if code != 0 {
				t.Fatalf("exit code = %d; want 0 (stderr=%q)", code, stderr.String())
			}
			if !called {
				t.Fatal("expected runMergeCoordinator to be called")
			}
			if captured.CAFile != caFile {
				t.Fatalf("MergeConfig.CAFile = %q; want %q", captured.CAFile, caFile)
			}
		})
	}
}

func writeTestSelfSignedCA(t *testing.T, path string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kodema-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, block, 0o600); err != nil {
		t.Fatalf("WriteFile CA: %v", err)
	}
	return path
}
