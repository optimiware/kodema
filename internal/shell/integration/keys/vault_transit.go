package keys

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/shell/secret"
)

const (
	vaultTransitMaxResponse int64 = 64 << 10
	vaultTransitMaxToken    int   = 1024
	vaultTransitMaxNS       int   = 256
	vaultTransitTimeout           = 30 * time.Second

	vaultTransitDialTimeout           = 10 * time.Second
	vaultTransitDialKeepAlive         = 30 * time.Second
	vaultTransitIdleConnTimeout       = 90 * time.Second
	vaultTransitTLSHandshakeTimeout   = 10 * time.Second
	vaultTransitExpectContinueTimeout = time.Second
)

// VaultTransitKMSConfig configures the OpenBao / HashiCorp Vault Transit
// secrets engine client.
type VaultTransitKMSConfig struct {
	// Address is the Vault / OpenBao HTTPS origin (VAULT_ADDR). Required.
	// Only an explicit https:// URL without userinfo, query or fragment is
	// accepted.
	Address string
	// Token is the Vault / OpenBao client token (VAULT_TOKEN) with decrypt
	// permission on the configured transit key. Sent as X-Vault-Token and never
	// logged, returned or rendered in an error.
	Token string
	// Namespace optionally selects a Vault / OpenBao namespace
	// (VAULT_NAMESPACE), sent as X-Vault-Namespace when non-empty.
	Namespace string
	// CAFile optionally names a PEM certificate authority bundle (VAULT_CACERT)
	// appended to the system roots for an in-cluster or self-signed Vault /
	// OpenBao server. Ignored when HTTPClient supplies an explicit transport.
	CAFile string
	// HTTPClient optionally supplies HTTP client settings such as explicit TLS
	// roots in tests. NewVaultTransitKMS copies the client, installs a private
	// transport with Proxy=nil and always refuses redirects; the caller's client
	// is never mutated. Its transport must be an explicit *http.Transport other
	// than http.DefaultTransport.
	HTTPClient *http.Client
}

// VaultTransitKMS implements KMSClient over the OpenBao / HashiCorp Vault
// Transit decrypt API (`POST /v1/<mount>/decrypt/<key-name>`).
type VaultTransitKMS struct {
	address   string
	token     string
	namespace string
	client    *http.Client
}

var _ KMSClient = (*VaultTransitKMS)(nil)

// NewVaultTransitKMS validates the configuration and returns the client. It
// makes no network request and never contacts Vault / OpenBao.
func NewVaultTransitKMS(config VaultTransitKMSConfig) (*VaultTransitKMS, error) {
	address, err := vaultTransitAddress(config.Address)
	if err != nil {
		return nil, err
	}
	if !validVaultHeaderValue(config.Token, vaultTransitMaxToken) {
		return nil, fault.New(fault.InvalidInput, "vault transit token must be 1 to 1024 printable ASCII bytes without whitespace or control characters")
	}
	if config.Namespace != "" && !validVaultHeaderValue(config.Namespace, vaultTransitMaxNS) {
		return nil, fault.New(fault.InvalidInput, "vault transit namespace must be 1 to 256 printable ASCII bytes without whitespace or control characters")
	}

	client := &http.Client{}
	if config.HTTPClient != nil {
		copied := *config.HTTPClient
		client = &copied
	}
	owned, err := vaultTransitOwnedTransport(client.Transport, config.CAFile)
	if err != nil {
		return nil, err
	}
	client.Transport = owned
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	client.Timeout = vaultTransitTimeout

	return &VaultTransitKMS{
		address:   address,
		token:     config.Token,
		namespace: config.Namespace,
		client:    client,
	}, nil
}

// Decrypt unwraps blob with the transit key named by keyRef
// ("vault-transit:<mount>/<key-name>"). blob holds the UTF-8 Vault Transit
// ciphertext string (for example "vault:v1:..."), with at most one trailing
// newline trimmed. No response body, credential, ciphertext or plaintext enters
// an error.
func (kms *VaultTransitKMS) Decrypt(ctx context.Context, keyRef secret.KeyRef, blob []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref, err := ParseVaultTransitReference(keyRef)
	if err != nil {
		return nil, err
	}
	ciphertext := strings.TrimSuffix(strings.TrimSuffix(string(blob), "\n"), "\r")
	if !validVaultCiphertext(ciphertext) {
		return nil, fault.New(fault.InvalidInput, "vault transit encrypted key blob is malformed")
	}
	body, err := json.Marshal(struct {
		Ciphertext string `json:"ciphertext"`
	}{Ciphertext: ciphertext})
	if err != nil {
		return nil, fault.Wrap(fault.InternalFailure, "vault transit request could not be encoded", err)
	}
	endpoint := kms.address + "/v1/" + ref.Mount + "/decrypt/" + url.PathEscape(ref.KeyName)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fault.Wrap(fault.InternalFailure, "vault transit request could not be built", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Vault-Token", kms.token)
	if kms.namespace != "" {
		request.Header.Set("X-Vault-Namespace", kms.namespace)
	}

	response, err := kms.client.Do(request)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, fault.Wrap(fault.DependencyUnavailable, "vault transit is unavailable", err)
	}
	defer func() { _ = response.Body.Close() }()

	switch {
	case response.StatusCode == http.StatusOK:
	case response.StatusCode == http.StatusUnauthorized:
		return nil, fault.New(fault.Unauthenticated, "vault transit rejected the token")
	case response.StatusCode == http.StatusForbidden:
		return nil, fault.New(fault.Forbidden, "vault transit denied decrypt for this token")
	case response.StatusCode == http.StatusNotFound:
		return nil, fault.New(fault.NotFound, "vault transit key was not found")
	case response.StatusCode == http.StatusTooManyRequests, response.StatusCode >= http.StatusInternalServerError:
		return nil, fault.New(fault.DependencyUnavailable, "vault transit is unavailable")
	case response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError:
		return nil, fault.New(fault.InvalidInput, "vault transit rejected the request")
	default:
		return nil, fault.New(fault.DependencyUnavailable, "vault transit returned an unexpected status")
	}

	payload, err := readVaultTransitResponse(response.Body)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, err
	}
	var decoded struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded.Data.Plaintext == "" {
		return nil, fault.New(fault.DependencyUnavailable, "vault transit returned no plaintext")
	}
	plaintext, err := base64.StdEncoding.DecodeString(decoded.Data.Plaintext)
	if err != nil {
		return nil, fault.New(fault.DependencyUnavailable, "vault transit returned malformed plaintext")
	}
	return plaintext, nil
}

func validVaultHeaderValue(value string, maxLen int) bool {
	if value == "" || len(value) > maxLen {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] <= ' ' || value[index] >= 0x7f {
			return false
		}
	}
	return true
}

func validVaultCiphertext(value string) bool {
	if value == "" {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] <= ' ' || value[index] >= 0x7f {
			return false
		}
	}
	return true
}

func vaultTransitAddress(value string) (string, error) {
	base := strings.TrimRight(value, "/")
	if base == "" {
		return "", fault.New(fault.InvalidInput, "vault transit address (VAULT_ADDR) is required")
	}
	if strings.ContainsAny(base, " \t\r\n") {
		return "", fault.New(fault.InvalidInput, "vault transit address contains whitespace")
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return "", fault.New(fault.InvalidInput, "vault transit address is not a valid URL")
	}
	if parsed.Scheme != "https" || parsed.Host == "" {
		return "", fault.New(fault.InvalidInput, "vault transit address must be an explicit https URL")
	}
	if parsed.User != nil {
		return "", fault.New(fault.InvalidInput, "vault transit address must not carry userinfo")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return "", fault.New(fault.InvalidInput, "vault transit address must not carry a query or fragment")
	}
	return base, nil
}

func vaultTransitOwnedTransport(transport http.RoundTripper, caFile string) (*http.Transport, error) {
	if transport == nil {
		return newVaultTransitTransport(caFile)
	}
	caller, ok := transport.(*http.Transport)
	if !ok {
		return nil, fault.New(fault.InvalidInput, "vault transit client must use an explicit HTTP transport")
	}
	if caller == nil {
		return nil, fault.New(fault.InvalidInput, "vault transit client supplies a nil HTTP transport")
	}
	if http.DefaultTransport != nil && caller == http.DefaultTransport {
		return nil, fault.New(fault.InvalidInput, "vault transit client must not use the shared default HTTP transport")
	}
	owned := caller.Clone()
	owned.Proxy = nil
	return owned, nil
}

func newVaultTransitTransport(caFile string) (*http.Transport, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if trimmed := strings.TrimSpace(caFile); trimmed != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		pem, err := os.ReadFile(trimmed)
		if err != nil {
			return nil, fault.New(fault.InvalidInput, "vault transit CA bundle (VAULT_CACERT) is unreadable")
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fault.New(fault.InvalidInput, "vault transit CA bundle (VAULT_CACERT) has no usable certificate")
		}
		tlsConfig.RootCAs = pool
	}
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   vaultTransitDialTimeout,
			KeepAlive: vaultTransitDialKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       vaultTransitIdleConnTimeout,
		TLSHandshakeTimeout:   vaultTransitTLSHandshakeTimeout,
		ExpectContinueTimeout: vaultTransitExpectContinueTimeout,
		TLSClientConfig:       tlsConfig,
	}, nil
}

func readVaultTransitResponse(body io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(body, vaultTransitMaxResponse+1))
	if err != nil {
		return nil, fault.Wrap(fault.DependencyUnavailable, "vault transit response could not be read", err)
	}
	if int64(len(payload)) > vaultTransitMaxResponse {
		return nil, fault.New(fault.DependencyUnavailable, "vault transit response exceeds the configured bound")
	}
	return payload, nil
}
