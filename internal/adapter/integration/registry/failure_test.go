package registry_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/optimiware/kodema/internal/adapter/integration/registry"
	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/fault"
)

// TestResolveHTTPFailureClasses pins the status-to-fault mapping. A failed read
// never yields a digest, and a response body is never echoed into the error.
func TestResolveHTTPFailureClasses(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   fault.Class
	}{
		{"not found", http.StatusNotFound, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, fault.NotFound},
		{"unauthorized", http.StatusUnauthorized, `{"errors":[{"code":"UNAUTHORIZED"}]}`, fault.Unauthenticated},
		{"forbidden", http.StatusForbidden, `{"errors":[{"code":"DENIED"}]}`, fault.Forbidden},
		{"rate limited", http.StatusTooManyRequests, "", fault.DependencyUnavailable},
		{"server error", http.StatusInternalServerError, "backend internal detail", fault.DependencyUnavailable},
		{"bad gateway", http.StatusBadGateway, "", fault.DependencyUnavailable},
		{"service unavailable", http.StatusServiceUnavailable, "", fault.DependencyUnavailable},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			transport := &fakeTransport{handle: func(_ int, request *http.Request) (*http.Response, error) {
				return registryResponse(request, testCase.status, "application/json", testCase.body, ""), nil
			}}
			resolver := newResolver(t, registry.Config{Transport: transport})

			image, err := resolveWith(t, resolver, tagReference)
			requireFault(t, err, testCase.want)
			requireNoDigest(t, image)
			requireNoLeak(t, err, "backend internal detail", "MANIFEST_UNKNOWN")
		})
	}
}

// TestResolveTransportError pins that a transport failure is a retryable
// dependency fault that keeps the private cause reachable.
func TestResolveTransportError(t *testing.T) {
	cause := errors.New("dial registry.example.com: i/o timeout")
	transport := &fakeTransport{handle: func(_ int, request *http.Request) (*http.Response, error) {
		return nil, cause
	}}
	resolver := newResolver(t, registry.Config{Transport: transport})

	image, err := resolveWith(t, resolver, tagReference)
	requireFault(t, err, fault.DependencyUnavailable)
	if !errors.Is(err, cause) {
		t.Fatalf("dependency fault lost its private cause: %v", err)
	}
	requireNoDigest(t, image)
}

// TestResolveCanceledBeforeRequest pins cancellation propagation without a
// registry call and the deadline_expired public class.
func TestResolveCanceledBeforeRequest(t *testing.T) {
	transport := &fakeTransport{}
	resolver := newResolver(t, registry.Config{Transport: transport})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	image, err := resolver.Resolve(ctx, pipeline.ResolveRequest{Reference: tagReference})
	requireFault(t, err, fault.DeadlineExpired)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation fault lost context.Canceled: %v", err)
	}
	requireNoDigest(t, image)
}

// TestResolveDeadlineExceededBeforeRequest pins deadline propagation and keeps
// context.DeadlineExceeded reachable through the fault.
func TestResolveDeadlineExceededBeforeRequest(t *testing.T) {
	transport := &fakeTransport{}
	resolver := newResolver(t, registry.Config{Transport: transport})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	image, err := resolver.Resolve(ctx, pipeline.ResolveRequest{Reference: tagReference})
	requireFault(t, err, fault.DeadlineExpired)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline fault lost context.DeadlineExceeded: %v", err)
	}
	requireNoDigest(t, image)
}

// TestResolveCanceledInFlight pins that a resolve blocked on the registry
// returns promptly when its context is canceled and never produces a digest.
func TestResolveCanceledInFlight(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	transport := &fakeTransport{handle: func(_ int, request *http.Request) (*http.Response, error) {
		once.Do(func() { close(started) })
		<-request.Context().Done()
		return nil, request.Context().Err()
	}}
	resolver := newResolver(t, registry.Config{Transport: transport})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type outcome struct {
		image pipeline.ResolvedImage
		err   error
	}
	result := make(chan outcome, 1)
	go func() {
		image, err := resolver.Resolve(ctx, pipeline.ResolveRequest{Reference: tagReference})
		result <- outcome{image: image, err: err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Resolve never reached the registry transport")
	}
	cancel()

	select {
	case got := <-result:
		requireFault(t, got.err, fault.DeadlineExpired)
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cancellation fault lost context.Canceled: %v", got.err)
		}
		requireNoDigest(t, got.image)
	case <-time.After(2 * time.Second):
		t.Fatal("Resolve did not return after cancellation")
	}
}

// TestResolveFailureAfterSuccessIsNotStale pins the no-cache recovery rule: a
// later failure is an error and never the previously resolved digest.
func TestResolveFailureAfterSuccessIsNotStale(t *testing.T) {
	body := singleManifestBody
	transport := &fakeTransport{handle: func(call int, request *http.Request) (*http.Response, error) {
		if call == 0 {
			return registryResponse(request, http.StatusOK, dockerManifestMediaType, body, digestOf(body)), nil
		}
		return registryResponse(request, http.StatusServiceUnavailable, "", "", ""), nil
	}}
	resolver := newResolver(t, registry.Config{Transport: transport})

	first, err := resolveWith(t, resolver, tagReference)
	if err != nil {
		t.Fatalf("first resolution failed: %v", err)
	}
	requireResolved(t, first, repository, digestOf(body))

	second, err := resolveWith(t, resolver, tagReference)
	requireFault(t, err, fault.DependencyUnavailable)
	requireNoDigest(t, second)
	if second.Digest == first.Digest {
		t.Fatalf("failure returned the stale digest %q", first.Digest)
	}
	if requests := transport.captured(); len(requests) != 2 {
		t.Fatalf("resolver issued %d requests for two resolves, want 2", len(requests))
	}
}

// TestResolveChangedTagReResolves pins that a moved tag is looked up again and
// reports the new immutable digest.
func TestResolveChangedTagReResolves(t *testing.T) {
	bodies := []string{singleManifestBody, secondManifestBody}
	transport := &fakeTransport{handle: func(call int, request *http.Request) (*http.Response, error) {
		body := bodies[call%len(bodies)]
		return registryResponse(request, http.StatusOK, dockerManifestMediaType, body, digestOf(body)), nil
	}}
	resolver := newResolver(t, registry.Config{Transport: transport})

	first, err := resolveWith(t, resolver, tagReference)
	if err != nil {
		t.Fatalf("first resolution failed: %v", err)
	}
	requireResolved(t, first, repository, digestOf(bodies[0]))

	second, err := resolveWith(t, resolver, tagReference)
	if err != nil {
		t.Fatalf("second resolution failed: %v", err)
	}
	requireResolved(t, second, repository, digestOf(bodies[1]))
	if first.Digest == second.Digest {
		t.Fatalf("changed tag kept the old digest %q", first.Digest)
	}
	if requests := transport.captured(); len(requests) != 2 {
		t.Fatalf("changed tag issued %d requests, want 2", len(requests))
	}
}

// TestResolveFailureDiagnosticsAndLogging pins Issue #81: 429 rate limits (with
// Retry-After and mirror/credential hint), 404 missing manifests, 5xx errors,
// DNS errors, connection refused, and TLS handshake failures produce distinct,
// actionable messages without leaking response bodies or untrusted headers, and
// emit server-side warning logs.
func TestResolveFailureDiagnosticsAndLogging(t *testing.T) {
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	t.Run("429 with Retry-After includes host status retry_after and hint", func(t *testing.T) {
		logBuf.Reset()
		transport := &fakeTransport{handle: func(_ int, request *http.Request) (*http.Response, error) {
			resp := registryResponse(request, http.StatusTooManyRequests, "application/json", `{"errors":[{"code":"TOOMANYREQUESTS"}]}`, "")
			resp.Header.Set("Retry-After", "120")
			return resp, nil
		}}
		resolver := newResolver(t, registry.Config{Transport: transport})
		_, err := resolveWith(t, resolver, "docker.io/library/alpine:3.24")
		requireFault(t, err, fault.DependencyUnavailable)
		msg, _ := fault.MessageOf(err)
		for _, want := range []string{"docker.io", "429 Too Many Requests", "retry after 120s", "credentials", "mirror"} {
			if !strings.Contains(msg, want) {
				t.Fatalf("429 message = %q; want substring %q", msg, want)
			}
		}
		requireNoLeak(t, err, "TOOMANYREQUESTS")
		logged := logBuf.String()
		for _, want := range []string{"level=WARN", "registry=docker.io", "status=429", "retry_after=120s"} {
			if !strings.Contains(logged, want) {
				t.Fatalf("server log = %q; want substring %q", logged, want)
			}
		}
	})

	t.Run("429 discards untrusted Retry-After header", func(t *testing.T) {
		transport := &fakeTransport{handle: func(_ int, request *http.Request) (*http.Response, error) {
			resp := registryResponse(request, http.StatusTooManyRequests, "application/json", "", "")
			resp.Header.Set("Retry-After", "untrusted-secret-header")
			return resp, nil
		}}
		resolver := newResolver(t, registry.Config{Transport: transport})
		_, err := resolveWith(t, resolver, "docker.io/library/alpine:3.24")
		requireFault(t, err, fault.DependencyUnavailable)
		requireNoLeak(t, err, "untrusted-secret-header")
	})

	t.Run("404 names image and registry without leaking body", func(t *testing.T) {
		logBuf.Reset()
		transport := &fakeTransport{handle: func(_ int, request *http.Request) (*http.Response, error) {
			return registryResponse(request, http.StatusNotFound, "application/json", `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, ""), nil
		}}
		resolver := newResolver(t, registry.Config{Transport: transport})
		_, err := resolveWith(t, resolver, "mirror.gcr.io/library/alpine:kt-no-such-tag")
		requireFault(t, err, fault.NotFound)
		msg, _ := fault.MessageOf(err)
		for _, want := range []string{"mirror.gcr.io/library/alpine:kt-no-such-tag", "mirror.gcr.io", "404 Not Found"} {
			if !strings.Contains(msg, want) {
				t.Fatalf("404 message = %q; want substring %q", msg, want)
			}
		}
		requireNoLeak(t, err, "MANIFEST_UNKNOWN")
		if !strings.Contains(logBuf.String(), "status=404") {
			t.Fatalf("server log = %q; want status=404", logBuf.String())
		}
	})

	t.Run("transport errors distinguish DNS, connection refused, and TLS failures", func(t *testing.T) {
		cases := []struct {
			name    string
			ref     string
			err     error
			wantSub []string
		}{
			{
				name:    "dns not found",
				ref:     "registry.invalid/probe/alpine:3.24",
				err:     &net.DNSError{Err: "no such host", Name: "registry.invalid", IsNotFound: true},
				wantSub: []string{"registry.invalid", "no such host"},
			},
			{
				name:    "connection refused",
				ref:     "host.containers.internal:18953/probe/alpine:3.24",
				err:     &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
				wantSub: []string{"host.containers.internal:18953", "connection refused"},
			},
			{
				name:    "plain http tls record error",
				ref:     "host.containers.internal:18952/probe/alpine:3.24",
				err:     tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"},
				wantSub: []string{"host.containers.internal:18952", "TLS handshake failed"},
			},
			{
				name:    "unknown certificate authority",
				ref:     "registry.example.com/probe/alpine:3.24",
				err:     x509.UnknownAuthorityError{},
				wantSub: []string{"registry.example.com", "TLS certificate"},
			},
		}
		seen := make(map[string]bool)
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				logBuf.Reset()
				transport := &fakeTransport{handle: func(_ int, _ *http.Request) (*http.Response, error) {
					return nil, tc.err
				}}
				resolver := newResolver(t, registry.Config{Transport: transport})
				_, err := resolveWith(t, resolver, tc.ref)
				requireFault(t, err, fault.DependencyUnavailable)
				msg, _ := fault.MessageOf(err)
				for _, want := range tc.wantSub {
					if !strings.Contains(msg, want) {
						t.Fatalf("%s message = %q; want substring %q", tc.name, msg, want)
					}
				}
				if seen[msg] {
					t.Fatalf("duplicate transport message %q across distinct error kinds", msg)
				}
				seen[msg] = true
				if !strings.Contains(logBuf.String(), "level=WARN") {
					t.Fatalf("server log = %q; want WARN entry", logBuf.String())
				}
			})
		}
	})
}
