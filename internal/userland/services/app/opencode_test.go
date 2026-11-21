package app

import (
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
	"testing"
	"time"
)

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
