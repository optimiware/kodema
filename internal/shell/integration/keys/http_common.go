package keys

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/optimiware/kodema/internal/fault"
)

// Shared HTTP client and validation helpers for cloud KMS adapters (AWS, GCP,
// and Scaleway). Every cloud client owns a private transport with Proxy=nil,
// refuses redirects, validates explicit HTTPS origins without userinfo/query/
// fragment, requires printable ASCII tokens for headers/forms, and bounds all
// response body reads.

const (
	kmsDialTimeout           = 10 * time.Second
	kmsDialKeepAlive         = 30 * time.Second
	kmsIdleConnTimeout       = 90 * time.Second
	kmsTLSHandshakeTimeout   = 10 * time.Second
	kmsExpectContinueTimeout = time.Second
)

// newOwnedKMSTransport builds a private transport with explicit defaults and
// no proxy function. TLS verification stays at the system default; it
// deliberately does not clone a process-wide transport.
func newOwnedKMSTransport() *http.Transport {
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   kmsDialTimeout,
			KeepAlive: kmsDialKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       kmsIdleConnTimeout,
		TLSHandshakeTimeout:   kmsTLSHandshakeTimeout,
		ExpectContinueTimeout: kmsExpectContinueTimeout,
	}
}

// ownedKMSTransport returns the private transport a cloud KMS client installs.
// A nil transport gets explicit defaults with no proxy function; a
// caller-supplied *http.Transport is cloned so its explicit TLS, dial and
// HTTP/2 settings are preserved and its proxy is cleared. An opaque
// RoundTripper, a typed nil and the shared http.DefaultTransport are refused
// rather than silently trusted.
func ownedKMSTransport(transport http.RoundTripper, clientName string) (*http.Transport, error) {
	if transport == nil {
		return newOwnedKMSTransport(), nil
	}
	caller, ok := transport.(*http.Transport)
	if !ok {
		return nil, fault.New(fault.InvalidInput, clientName+" must use an explicit HTTP transport")
	}
	if caller == nil {
		return nil, fault.New(fault.InvalidInput, clientName+" supplies a nil HTTP transport")
	}
	if http.DefaultTransport != nil && caller == http.DefaultTransport {
		return nil, fault.New(fault.InvalidInput, clientName+" must not use the shared default HTTP transport")
	}
	owned := caller.Clone()
	owned.Proxy = nil
	return owned, nil
}

// newKMSHTTPClient copies base (without mutating it), installs a private
// transport with Proxy=nil, refuses redirects and applies the request timeout.
func newKMSHTTPClient(base *http.Client, clientName string, timeout time.Duration) (*http.Client, error) {
	client := &http.Client{}
	if base != nil {
		copied := *base
		client = &copied
	}
	owned, err := ownedKMSTransport(client.Transport, clientName)
	if err != nil {
		return nil, err
	}
	client.Transport = owned
	client.CheckRedirect = refuseRedirect
	client.Timeout = timeout
	return client, nil
}

// refuseRedirect returns redirects as-is so credentials and requests are never
// replayed to another location.
func refuseRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// parseOrigin applies the refusals shared by all cloud KMS and metadata
// origins: whitespace, an unparsable URL, userinfo, a query and a fragment.
// subject names the origin in the refusal; the value itself is never rendered.
func parseOrigin(origin, subject string) (*url.URL, error) {
	if strings.ContainsAny(origin, " \t\r\n") {
		return nil, fault.New(fault.InvalidInput, subject+" contains whitespace")
	}
	parsed, err := url.Parse(origin)
	if err != nil {
		return nil, fault.New(fault.InvalidInput, subject+" is not a valid URL")
	}
	if parsed.User != nil {
		return nil, fault.New(fault.InvalidInput, subject+" must not carry userinfo")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
		return nil, fault.New(fault.InvalidInput, subject+" must not carry a query or fragment")
	}
	return parsed, nil
}

// parseHTTPSOrigin validates one cloud KMS HTTPS origin, falling back to
// fallback when value is empty or only trailing slashes. The client never
// downgrades to cleartext and never accepts embedded credentials, queries or
// fragments.
func parseHTTPSOrigin(value, fallback, subject string) (string, error) {
	origin := strings.TrimRight(value, "/")
	if origin == "" {
		return fallback, nil
	}
	parsed, err := parseOrigin(origin, subject)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "https" || parsed.Host == "" {
		return "", fault.New(fault.InvalidInput, subject+" must be an explicit https URL")
	}
	return origin, nil
}

// isPrintableASCIIToken reports whether value is a non-empty run of printable
// ASCII bytes without spaces: the only shape a credential or token that enters
// an HTTP header or form value may have.
func isPrintableASCIIToken(value string) bool {
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

// readBoundedBody reads at most bound+1 bytes from body so an oversized stream
// is recognized without buffering the entire response or truncating silently.
func readBoundedBody(body io.Reader, bound int64) (payload []byte, oversized bool, err error) {
	data, err := io.ReadAll(io.LimitReader(body, bound+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > bound {
		return nil, true, nil
	}
	return data, false, nil
}

// readBoundedResponse reads at most bound bytes from body and maps read errors
// and oversized bodies to typed dependency_unavailable faults naming subject.
func readBoundedResponse(body io.Reader, bound int64, subject string) ([]byte, error) {
	payload, oversized, err := readBoundedBody(body, bound)
	if err != nil {
		return nil, fault.Wrap(fault.DependencyUnavailable, subject+" response could not be read", err)
	}
	if oversized {
		return nil, fault.New(fault.DependencyUnavailable, subject+" response exceeds the configured bound")
	}
	return payload, nil
}
