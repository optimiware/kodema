package keys_test

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/optimiware/kodema/internal/adapter/integration/keys"
	"github.com/optimiware/kodema/internal/control/secret"
	"github.com/optimiware/kodema/internal/fault"
)

func TestVaultTransitKMSDecryptAndWrapRoundTrip(t *testing.T) {
	kek := bytes.Repeat([]byte{0x42}, secret.DataKeyBytes)
	const (
		token     = "hvs.SENTINEL-VAULT-TOKEN"
		namespace = "ops/prod"
		cipher    = "vault:v1:abc123def456"
		keyRef    = secret.KeyRef("vault-transit:platform/transit/kodema-kek")
	)

	var gotPath, gotToken, gotNamespace, gotCiphertext string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotToken = r.Header.Get("X-Vault-Token")
		gotNamespace = r.Header.Get("X-Vault-Namespace")
		if r.Method != http.MethodPost {
			t.Errorf("method = %s; want POST", r.Method)
		}
		var req struct {
			Ciphertext string `json:"ciphertext"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		gotCiphertext = req.Ciphertext
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": map[string]any{
				"plaintext": base64.StdEncoding.EncodeToString(kek),
			},
		})
	}))
	defer server.Close()

	// Write the test server's CA cert to exercise CAFile loading.
	caFile := filepath.Join(t.TempDir(), "vault-ca.crt")
	certDER := server.Certificate().Raw
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0o600); err != nil {
		t.Fatal(err)
	}

	client, err := keys.NewVaultTransitKMS(keys.VaultTransitKMSConfig{
		Address:   server.URL,
		Token:     token,
		Namespace: namespace,
		CAFile:    caFile,
	})
	if err != nil {
		t.Fatalf("NewVaultTransitKMS: %v", err)
	}

	// Trailing newline in blob (e.g. from kubectl create secret) is trimmed cleanly.
	got, err := client.Decrypt(context.Background(), keyRef, []byte(cipher+"\n"))
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, kek) {
		t.Fatalf("plaintext mismatch")
	}
	if gotPath != "/v1/platform/transit/decrypt/kodema-kek" {
		t.Fatalf("path = %q; want /v1/platform/transit/decrypt/kodema-kek", gotPath)
	}
	if gotToken != token {
		t.Fatalf("X-Vault-Token = %q; want %q", gotToken, token)
	}
	if gotNamespace != namespace {
		t.Fatalf("X-Vault-Namespace = %q; want %q", gotNamespace, namespace)
	}
	if gotCiphertext != cipher {
		t.Fatalf("ciphertext = %q; want %q", gotCiphertext, cipher)
	}

	// Verify full envelope wrap/unwrap round-trip via keys.NewVaultTransit.
	provider, err := keys.NewVaultTransit(client, keys.CloudConfig{
		KeyRef:   keyRef,
		Current:  1,
		Versions: map[secret.KeyVersion][]byte{1: []byte(cipher)},
	}, nil)
	if err != nil {
		t.Fatalf("NewVaultTransit: %v", err)
	}
	dek := bytes.Repeat([]byte{0x77}, secret.DataKeyBytes)
	wrapped, err := provider.Wrap(context.Background(), secret.WrapRequest{
		KeyRef:         keyRef,
		KeyVersion:     1,
		Plaintext:      dek,
		AssociatedData: []byte("aad"),
	})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	opened, err := provider.Unwrap(context.Background(), secret.UnwrapRequest{
		KeyRef:         wrapped.KeyRef,
		KeyVersion:     wrapped.KeyVersion,
		Wrapped:        wrapped.Wrapped,
		Nonce:          wrapped.Nonce,
		AssociatedData: []byte("aad"),
	})
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(opened, dek) {
		t.Fatal("unwrapped DEK mismatch")
	}
}

func TestVaultTransitKMSConfigValidation(t *testing.T) {
	badCA := filepath.Join(t.TempDir(), "bad.crt")
	if err := os.WriteFile(badCA, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		config keys.VaultTransitKMSConfig
	}{
		{name: "empty address", config: keys.VaultTransitKMSConfig{Token: "tok"}},
		{name: "http address", config: keys.VaultTransitKMSConfig{Address: "http://vault.local:8200", Token: "tok"}},
		{name: "userinfo in address", config: keys.VaultTransitKMSConfig{Address: "https://u:p@vault.local:8200", Token: "tok"}},
		{name: "query in address", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200?x=1", Token: "tok"}},
		{name: "fragment in address", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200#frag", Token: "tok"}},
		{name: "empty token", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200"}},
		{name: "newline in token", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200", Token: "tok\r\nX-Evil: 1"}},
		{name: "space in token", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200", Token: " tok"}},
		{name: "newline in namespace", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200", Token: "tok", Namespace: "ns\n"}},
		{name: "missing CAFile", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200", Token: "tok", CAFile: "/nonexistent/ca.crt"}},
		{name: "invalid CAFile", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200", Token: "tok", CAFile: badCA}},
		{name: "shared default transport", config: keys.VaultTransitKMSConfig{Address: "https://vault.local:8200", Token: "tok", HTTPClient: &http.Client{Transport: http.DefaultTransport}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := keys.NewVaultTransitKMS(tc.config)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("class = %v (%v); want InvalidInput", class, err)
			}
		})
	}
}

func TestVaultTransitKMSStatusClassificationAndNoSecretLeak(t *testing.T) {
	const secretBody = "SENSITIVE-VAULT-RESPONSE-BODY"
	cases := []struct {
		name   string
		status int
		want   fault.Class
	}{
		{name: "401 unauthenticated", status: http.StatusUnauthorized, want: fault.Unauthenticated},
		{name: "403 forbidden", status: http.StatusForbidden, want: fault.Forbidden},
		{name: "404 not found", status: http.StatusNotFound, want: fault.NotFound},
		{name: "400 invalid input", status: http.StatusBadRequest, want: fault.InvalidInput},
		{name: "429 rate limited", status: http.StatusTooManyRequests, want: fault.DependencyUnavailable},
		{name: "503 sealed", status: http.StatusServiceUnavailable, want: fault.DependencyUnavailable},
		{name: "302 redirect refused", status: http.StatusFound, want: fault.DependencyUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status == http.StatusFound {
					w.Header().Set("Location", "https://evil.example/")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(secretBody))
			}))
			defer server.Close()

			pool := x509.NewCertPool()
			pool.AddCert(server.Certificate())
			tr := &http.Transport{}
			tr.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()

			client, err := keys.NewVaultTransitKMS(keys.VaultTransitKMSConfig{
				Address:    server.URL,
				Token:      "hvs.SECRET-TOKEN",
				HTTPClient: &http.Client{Transport: tr},
			})
			if err != nil {
				t.Fatalf("NewVaultTransitKMS: %v", err)
			}
			_, err = client.Decrypt(context.Background(), "vault-transit:transit/kodema-kek", []byte("vault:v1:abc"))
			if err == nil {
				t.Fatal("expected error")
			}
			if class, ok := fault.ClassOf(err); !ok || class != tc.want {
				t.Fatalf("class = %v (%v); want %v", class, err, tc.want)
			}
			for _, forbidden := range []string{secretBody, "hvs.SECRET-TOKEN"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("error leaked %q: %v", forbidden, err)
				}
			}
		})
	}
}

func TestVaultTransitKMSContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := keys.NewVaultTransitKMS(keys.VaultTransitKMSConfig{
		Address: "https://127.0.0.1:8200",
		Token:   "hvs.token",
	})
	if err != nil {
		t.Fatalf("NewVaultTransitKMS: %v", err)
	}
	_, err = client.Decrypt(ctx, "vault-transit:transit/kodema-kek", []byte("vault:v1:abc"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
}
