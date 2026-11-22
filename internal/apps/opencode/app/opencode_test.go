package app

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/apps/opencode"
	"github.com/optimiware/kodema/internal/id"
)

type noopAPIHandler struct{}

func (noopAPIHandler) Invoke(context.Context, api.Invocation) (api.Result, error) {
	return api.Result{}, nil
}

func TestHTTPRoundTripperWithCADisablesAmbientProxy(t *testing.T) {
	// Empty caFile must still clone http.DefaultTransport and set Proxy = nil.
	rt, err := httpRoundTripperWithCA("")
	if err != nil {
		t.Fatalf("httpRoundTripperWithCA empty: %v", err)
	}
	tr, ok := rt.(*http.Transport)
	if !ok {
		t.Fatalf("round tripper type = %T; want *http.Transport", rt)
	}
	if tr == http.DefaultTransport {
		t.Fatal("httpRoundTripperWithCA returned shared http.DefaultTransport instead of a clone")
	}
	if tr.Proxy != nil {
		t.Fatal("httpRoundTripperWithCA empty must set Proxy = nil")
	}

	// Valid PEM CA file must set RootCAs AND Proxy = nil.
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(caPath, pemBytes, 0o600); err != nil {
		t.Fatalf("WriteFile ca.pem: %v", err)
	}

	rtCA, err := httpRoundTripperWithCA(caPath)
	if err != nil {
		t.Fatalf("httpRoundTripperWithCA(ca.pem): %v", err)
	}
	trCA, ok := rtCA.(*http.Transport)
	if !ok {
		t.Fatalf("round tripper type = %T; want *http.Transport", rtCA)
	}
	if trCA.Proxy != nil {
		t.Fatal("httpRoundTripperWithCA with CA must set Proxy = nil")
	}
	if trCA.TLSClientConfig == nil || trCA.TLSClientConfig.RootCAs == nil {
		t.Fatal("httpRoundTripperWithCA with CA did not configure RootCAs")
	}
}

func TestRunOpenCodeWiresPasswordAndHTTPSCA(t *testing.T) {
	pwFile := filepath.Join(t.TempDir(), "opencode.pw")
	if err := os.WriteFile(pwFile, []byte("pw-from-file\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	baseCfg := OpenCodeConfig{
		ServiceID:      id.PrincipalID("opencode-svc"),
		TenantID:       id.TenantID("tenant-1"),
		ProjectID:      id.ProjectID("project-1"),
		ExecutionMode:  opencode.OpenCodeExecutionModeWorktree,
		StateDirectory: t.TempDir(),
		GitURL:         "file:///srv/git",
		OpenCodeURL:    "http://127.0.0.1:4096",
		Handler:        noopAPIHandler{},
		Clock:          servicekit.SystemClock{},
	}

	// Conflicting Password + PasswordFile must be rejected before starting.
	conflictCfg := baseCfg
	conflictCfg.OpenCodePassword = "direct"
	conflictCfg.OpenCodePasswordFile = pwFile
	if err := RunOpenCode(cancelledCtx, conflictCfg); err == nil {
		t.Fatal("RunOpenCode unexpectedly accepted both OpenCodePassword and OpenCodePasswordFile")
	}

	// Missing PasswordFile must fail fast.
	missingCfg := baseCfg
	missingCfg.OpenCodePasswordFile = filepath.Join(t.TempDir(), "missing.pw")
	if err := RunOpenCode(cancelledCtx, missingCfg); err == nil {
		t.Fatal("RunOpenCode unexpectedly accepted missing OpenCodePasswordFile")
	}

	// HTTPS OpenCodeURL with invalid CAFile must fail fast when building CA RoundTripper.
	badCACfg := baseCfg
	badCACfg.OpenCodeURL = "https://127.0.0.1:4096"
	badCACfg.CAFile = filepath.Join(t.TempDir(), "missing-ca.pem")
	if err := RunOpenCode(cancelledCtx, badCACfg); err == nil || !strings.Contains(err.Error(), "CA bundle") {
		t.Fatalf("RunOpenCode with HTTPS + missing CAFile err = %v; want CA bundle error", err)
	}

	// Valid PasswordFile constructs the client cleanly (exits nil because ctx is cancelled).
	validCfg := baseCfg
	validCfg.OpenCodePasswordFile = pwFile
	if err := RunOpenCode(cancelledCtx, validCfg); err != nil {
		t.Fatalf("RunOpenCode with valid PasswordFile: %v", err)
	}
}

func TestRunOpenCodeBrainModeRejectsMultiConcurrencyWithV2HTTPClient(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	baseBrain := OpenCodeConfig{
		ServiceID:      id.PrincipalID("opencode-brain"),
		TenantID:       id.TenantID("tenant-1"),
		ProjectID:      id.ProjectID("project-1"),
		ExecutionMode:  opencode.OpenCodeExecutionModeBrain,
		StateDirectory: t.TempDir(),
		OpenCodeURL:    "http://127.0.0.1:4096",
		Handler:        noopAPIHandler{},
		Clock:          servicekit.SystemClock{},
	}

	// Default HTTPOpenCodeClient (OpenCode == nil) in Brain mode must reject MaxConcurrentTurns > 1.
	tooMany := baseBrain
	tooMany.MaxConcurrentTurns = 2
	err := RunOpenCode(cancelledCtx, tooMany)
	if err == nil || !strings.Contains(err.Error(), "--max-concurrency 1") {
		t.Fatalf("RunOpenCode Brain MaxConcurrentTurns=2 err = %v; want --max-concurrency 1 error", err)
	}

	// Default MaxConcurrentTurns <= 0 defaults safely to 1 and succeeds (exits nil on cancelled ctx).
	defaultConc := baseBrain
	defaultConc.MaxConcurrentTurns = 0
	if err := RunOpenCode(cancelledCtx, defaultConc); err != nil {
		t.Fatalf("RunOpenCode Brain MaxConcurrentTurns=0: %v", err)
	}
}
