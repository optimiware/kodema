package server

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	opsapp "github.com/optimiware/kodema/internal/server/ops"
)

func TestGitCompanionConfigValidate(t *testing.T) {
	validBase := func() GitCompanionConfig {
		return GitCompanionConfig{
			AdminURL:  "http://127.0.0.1:8081",
			TokenFile: "/run/secrets/git-provision-token",
		}
	}

	t.Run("nil config is valid", func(t *testing.T) {
		var cfg *GitCompanionConfig
		if err := cfg.validate(); err != nil {
			t.Fatalf("nil validate() = %v; want nil", err)
		}
	})

	t.Run("valid minimal and full configs", func(t *testing.T) {
		minimal := validBase()
		if err := minimal.validate(); err != nil {
			t.Fatalf("minimal validate() = %v; want nil", err)
		}

		full := validBase()
		full.AdminURL = "https://git-{tenant}.internal:8443/prefix"
		full.PublicGitURL = "https://git.example.com/git/"
		full.ServiceID = "git-companion_1"
		full.TrustedOrigins = []string{"https://127.0.0.1:8443", "https://kodema.example.com/"}
		if err := full.validate(); err != nil {
			t.Fatalf("full validate() = %v; want nil", err)
		}

		multiTenant := GitCompanionConfig{
			AdminURL:       "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
			PublicGitURL:   "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
			ServiceID:      "kodema-git-{tenant}",
			TrustedOrigins: []string{"https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443"},
			TokenFile:      "/etc/kodema/git-provision/{tenant}/token",
		}
		if err := multiTenant.validate(); err != nil {
			t.Fatalf("multiTenant validate() = %v; want nil", err)
		}
	})

	cases := []struct {
		name   string
		mutate func(*GitCompanionConfig)
		want   string
	}{
		{
			name:   "empty admin URL",
			mutate: func(c *GitCompanionConfig) { c.AdminURL = "  " },
			want:   "admin URL",
		},
		{
			name:   "admin URL with query",
			mutate: func(c *GitCompanionConfig) { c.AdminURL = "https://git.internal?a=1" },
			want:   "admin URL",
		},
		{
			name:   "admin URL with trailing question mark",
			mutate: func(c *GitCompanionConfig) { c.AdminURL = "https://git.internal?" },
			want:   "admin URL",
		},
		{
			name:   "admin URL with non-http scheme",
			mutate: func(c *GitCompanionConfig) { c.AdminURL = "ftp://git.internal" },
			want:   "http or https",
		},
		{
			name:   "empty token file",
			mutate: func(c *GitCompanionConfig) { c.TokenFile = "   " },
			want:   "provision token file",
		},
		{
			name:   "whitespace-only public git URL",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "   " },
			want:   "public git URL",
		},
		{
			name:   "relative public git URL",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "/git" },
			want:   "public git URL",
		},
		{
			name:   "public git URL with userinfo",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "https://user:pass@git.example.com/git" },
			want:   "public git URL",
		},
		{
			name:   "public git URL with query",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "https://git.example.com/git?x=1" },
			want:   "public git URL",
		},
		{
			name:   "public git URL with fragment",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "https://git.example.com/git#frag" },
			want:   "public git URL",
		},
		{
			name:   "public git URL with non-http scheme",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "ssh://git.example.com/git" },
			want:   "http or https",
		},
		{
			name:   "whitespace-only service ID",
			mutate: func(c *GitCompanionConfig) { c.ServiceID = "   " },
			want:   "service ID",
		},
		{
			name:   "malformed service ID with spaces",
			mutate: func(c *GitCompanionConfig) { c.ServiceID = "git service" },
			want:   "service ID",
		},
		{
			name:   "malformed service ID with slash",
			mutate: func(c *GitCompanionConfig) { c.ServiceID = "git/companion" },
			want:   "service ID",
		},
		{
			name:   "empty trusted origin element",
			mutate: func(c *GitCompanionConfig) { c.TrustedOrigins = []string{""} },
			want:   "trusted origin",
		},
		{
			name:   "whitespace trusted origin element",
			mutate: func(c *GitCompanionConfig) { c.TrustedOrigins = []string{"   "} },
			want:   "trusted origin",
		},
		{
			name:   "non-https trusted origin",
			mutate: func(c *GitCompanionConfig) { c.TrustedOrigins = []string{"http://127.0.0.1:8443"} },
			want:   "trusted origin",
		},
		{
			name:   "trusted origin with query",
			mutate: func(c *GitCompanionConfig) { c.TrustedOrigins = []string{"https://kodema.example.com?foo=bar"} },
			want:   "trusted origin",
		},
		{
			name:   "trusted origin with userinfo",
			mutate: func(c *GitCompanionConfig) { c.TrustedOrigins = []string{"https://user@kodema.example.com"} },
			want:   "trusted origin",
		},
		{
			name:   "templated public git URL with query",
			mutate: func(c *GitCompanionConfig) { c.PublicGitURL = "https://kodema-git-{tenant}.svc:8443?x=1" },
			want:   "public git URL",
		},
		{
			name:   "templated service ID with slash",
			mutate: func(c *GitCompanionConfig) { c.ServiceID = "kodema-git/{tenant}" },
			want:   "service ID",
		},
		{
			name:   "templated non-https trusted origin",
			mutate: func(c *GitCompanionConfig) { c.TrustedOrigins = []string{"http://kodema-git-{tenant}.svc:8443"} },
			want:   "trusted origin",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validBase()
			tc.mutate(&cfg)
			err := cfg.validate()
			if err == nil {
				t.Fatalf("validate() = nil; want error containing %q", tc.want)
			}
			if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
				t.Fatalf("validate() err = %v (class=%v); want InvalidInput", err, class)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate() err = %q; want substring %q", err.Error(), tc.want)
			}
		})
	}
}

func TestHTTPRepositoryProvisionerRefusesRedirects(t *testing.T) {
	var targetHits atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer redirectTarget.Close()

	companion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL+"/steal-token", http.StatusFound)
	}))
	defer companion.Close()

	tokenFile := filepath.Join(t.TempDir(), "provision-token")
	if err := os.WriteFile(tokenFile, []byte("secret-provision-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	provisioner, err := newHTTPRepositoryProvisioner(Config{
		GitCompanion: &GitCompanionConfig{
			AdminURL:  companion.URL,
			TokenFile: tokenFile,
		},
	})
	if err != nil {
		t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
	}
	if provisioner.client.CheckRedirect == nil {
		t.Fatal("provisioner HTTP client has nil CheckRedirect")
	}
	if err := provisioner.client.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatalf("CheckRedirect returned %v; want http.ErrUseLastResponse", err)
	}

	_, err = provisioner.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
		TenantID:      id.TenantID("tenant-a"),
		ProjectID:     id.ProjectID("project-a"),
		Repository:    "shop/web",
		ResourceID:    "git-shop-web",
		DefaultBranch: "main",
	})
	if class, ok := fault.ClassOf(err); !ok || class != fault.DependencyUnavailable {
		t.Fatalf("ProvisionRepository on redirect err = %v; want DependencyUnavailable", err)
	}
	if hits := targetHits.Load(); hits != 0 {
		t.Fatalf("redirect target received %d requests; want 0 (redirects must never be followed)", hits)
	}
}

func TestHTTPRepositoryProvisionerMaps401ToDependencyUnavailable(t *testing.T) {
	companion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("WWW-Authenticate", `Bearer realm="kodema"`)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"authentication failed"}`))
	}))
	defer companion.Close()

	tokenFile := filepath.Join(t.TempDir(), "provision-token")
	if err := os.WriteFile(tokenFile, []byte("wrong-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	provisioner, err := newHTTPRepositoryProvisioner(Config{
		GitCompanion: &GitCompanionConfig{
			AdminURL:  companion.URL,
			TokenFile: tokenFile,
		},
	})
	if err != nil {
		t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
	}

	_, err = provisioner.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
		TenantID:      id.TenantID("tenant-a"),
		ProjectID:     id.ProjectID("project-a"),
		Repository:    "shop/web",
		ResourceID:    "git-shop-web",
		DefaultBranch: "main",
	})
	if class, ok := fault.ClassOf(err); !ok || class != fault.DependencyUnavailable {
		t.Fatalf("ProvisionRepository on HTTP 401 err = %v (class=%v); want DependencyUnavailable", err, class)
	}
}

func TestHTTPRepositoryProvisionerIgnoresEnvironmentProxies(t *testing.T) {
	var proxyHits atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyHits.Add(1)
		http.Error(w, "proxy must never be contacted", http.StatusBadGateway)
	}))
	defer proxy.Close()

	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("http_proxy", proxy.URL)
	t.Setenv("https_proxy", proxy.URL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")

	var companionHits atomic.Int32
	companion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		companionHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"repository":"shop/web","resource_id":"git-shop-web","default_branch":"main","created":true}`))
	}))
	defer companion.Close()

	tokenFile := filepath.Join(t.TempDir(), "provision-token")
	if err := os.WriteFile(tokenFile, []byte("secret-provision-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	provisioner, err := newHTTPRepositoryProvisioner(Config{
		GitCompanion: &GitCompanionConfig{
			AdminURL:  companion.URL,
			TokenFile: tokenFile,
		},
	})
	if err != nil {
		t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
	}
	t.Cleanup(func() {
		if err := provisioner.Close(context.Background()); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	transport, ok := provisioner.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport = %T; want *http.Transport", provisioner.client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("git companion transport has non-nil Proxy; environment proxies must be ignored")
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLSClientConfig = %+v; want MinVersion TLS 1.2", transport.TLSClientConfig)
	}

	res, err := provisioner.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
		TenantID:      id.TenantID("tenant-a"),
		ProjectID:     id.ProjectID("project-a"),
		Repository:    "shop/web",
		ResourceID:    "git-shop-web",
		DefaultBranch: "main",
	})
	if err != nil {
		t.Fatalf("ProvisionRepository: %v", err)
	}
	if !res.Created || res.Repository != "shop/web" {
		t.Fatalf("ProvisionRepository result = %+v; want created shop/web", res)
	}
	if got := proxyHits.Load(); got != 0 {
		t.Fatalf("proxy received %d requests; want 0", got)
	}
	if got := companionHits.Load(); got != 1 {
		t.Fatalf("companion received %d requests; want 1", got)
	}
}

type repoRoundTripFunc func(*http.Request) (*http.Response, error)

func (f repoRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestHTTPRepositoryProvisionerTenantTemplateExpansionAndBackwardCompat(t *testing.T) {
	t.Run("multi-tenant placeholders expand per tenant with deduplication and slice isolation", func(t *testing.T) {
		p, err := newHTTPRepositoryProvisioner(Config{
			GitCompanion: &GitCompanionConfig{
				AdminURL:     "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
				PublicGitURL: "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443/",
				ServiceID:    "kodema-git-{tenant}",
				TrustedOrigins: []string{
					"https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443/",
					"https://kodema-git-tenant-a.kodema-system.svc.cluster.local:8443",
					"https://kodema.example.com",
				},
				TokenFile: "/etc/kodema/git-provision/{tenant}/token",
			},
		})
		if err != nil {
			t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
		}
		t.Cleanup(func() { _ = p.Close(context.Background()) })

		if got := p.DefaultServiceID("tenant-a"); got != "kodema-git-tenant-a" {
			t.Fatalf("DefaultServiceID(tenant-a) = %q; want kodema-git-tenant-a", got)
		}
		if got := p.DefaultServiceID("tenant-b"); got != "kodema-git-tenant-b" {
			t.Fatalf("DefaultServiceID(tenant-b) = %q; want kodema-git-tenant-b", got)
		}
		if got := p.DefaultServiceID(""); got != "kodema-git-{tenant}" {
			t.Fatalf("DefaultServiceID(empty) = %q; want unexpanded template", got)
		}

		originsA := p.DefaultTrustedOrigins("tenant-a")
		wantOriginsA := []string{
			"https://kodema-git-tenant-a.kodema-system.svc.cluster.local:8443",
			"https://kodema.example.com",
		}
		if !reflect.DeepEqual(originsA, wantOriginsA) {
			t.Fatalf("DefaultTrustedOrigins(tenant-a) = %v; want %v (deduplicated)", originsA, wantOriginsA)
		}
		// Mutating returned slice must not affect subsequent calls.
		originsA[0] = "https://mutated.invalid"

		originsB := p.DefaultTrustedOrigins("tenant-b")
		wantOriginsB := []string{
			"https://kodema-git-tenant-b.kodema-system.svc.cluster.local:8443",
			"https://kodema-git-tenant-a.kodema-system.svc.cluster.local:8443",
			"https://kodema.example.com",
		}
		if !reflect.DeepEqual(originsB, wantOriginsB) {
			t.Fatalf("DefaultTrustedOrigins(tenant-b) = %v; want %v", originsB, wantOriginsB)
		}

		if got := p.CloneURL("tenant-a", "team/repo"); got != "https://kodema-git-tenant-a.kodema-system.svc.cluster.local:8443/team/repo.git" {
			t.Fatalf("CloneURL(tenant-a) = %q", got)
		}
		if got := p.CloneURL("tenant-b", "/team/repo.git"); got != "https://kodema-git-tenant-b.kodema-system.svc.cluster.local:8443/team/repo.git" {
			t.Fatalf("CloneURL(tenant-b) = %q", got)
		}
	})

	t.Run("single-tenant configs without placeholders preserve exact behavior", func(t *testing.T) {
		p, err := newHTTPRepositoryProvisioner(Config{
			GitCompanion: &GitCompanionConfig{
				AdminURL:       "http://kodema-default-git:8081",
				PublicGitURL:   "https://127.0.0.1:8443/git",
				ServiceID:      "",
				TrustedOrigins: []string{"https://127.0.0.1:8443"},
				TokenFile:      "/run/secrets/git-provision-token",
			},
		})
		if err != nil {
			t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
		}
		t.Cleanup(func() { _ = p.Close(context.Background()) })

		if got := p.DefaultServiceID("tenant-a"); got != "git" {
			t.Fatalf("DefaultServiceID = %q; want git", got)
		}
		if got := p.DefaultTrustedOrigins("tenant-a"); !reflect.DeepEqual(got, []string{"https://127.0.0.1:8443"}) {
			t.Fatalf("DefaultTrustedOrigins = %v; want [https://127.0.0.1:8443]", got)
		}
		if got := p.CloneURL("tenant-a", "shop/web"); got != "https://127.0.0.1:8443/git/shop/web.git" {
			t.Fatalf("CloneURL = %q; want https://127.0.0.1:8443/git/shop/web.git", got)
		}
	})
}

func TestHTTPRepositoryProvisionerMultiTenantTokenAndRouting(t *testing.T) {
	tmp := t.TempDir()
	for tenant, secret := range map[string]string{
		"tenant-a": "provision-token-alpha",
		"tenant-b": "provision-token-beta",
	} {
		dir := filepath.Join(tmp, "git-provision", tenant)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		if err := os.WriteFile(filepath.Join(dir, "token"), []byte(secret+"\n"), 0o600); err != nil {
			t.Fatalf("write token for %s: %v", tenant, err)
		}
	}

	type observedReq struct {
		url  string
		auth string
	}
	var (
		mu       sync.Mutex
		observed []observedReq
	)
	rt := repoRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		observed = append(observed, observedReq{
			url:  r.URL.String(),
			auth: r.Header.Get("Authorization"),
		})
		mu.Unlock()

		var expectedAuth string
		switch r.URL.Host {
		case "kodema-git-tenant-a.kodema-system.svc.cluster.local:8443":
			expectedAuth = "Bearer provision-token-alpha"
		case "kodema-git-tenant-b.kodema-system.svc.cluster.local:8443":
			expectedAuth = "Bearer provision-token-beta"
		default:
			t.Errorf("unexpected host %q", r.URL.Host)
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{}`)),
			}, nil
		}
		if r.Header.Get("Authorization") != expectedAuth {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"code":"unauthenticated","message":"wrong token"}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"created":true}`)),
		}, nil
	})

	p, err := newHTTPRepositoryProvisioner(Config{
		GitCompanion: &GitCompanionConfig{
			AdminURL:       "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
			PublicGitURL:   "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
			ServiceID:      "kodema-git-{tenant}",
			TrustedOrigins: []string{"https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443"},
			TokenFile:      filepath.Join(tmp, "git-provision", "{tenant}", "token"),
			RoundTripper:   rt,
		},
	})
	if err != nil {
		t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
	}

	resA, err := p.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
		TenantID:      id.TenantID("tenant-a"),
		ProjectID:     id.ProjectID("proj-a"),
		Repository:    "alpha/service",
		ResourceID:    "git-alpha-service",
		DefaultBranch: "main",
	})
	if err != nil {
		t.Fatalf("ProvisionRepository(tenant-a): %v", err)
	}
	if resA.CloneURL != "https://kodema-git-tenant-a.kodema-system.svc.cluster.local:8443/alpha/service.git" {
		t.Fatalf("resA.CloneURL = %q", resA.CloneURL)
	}

	resB, err := p.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
		TenantID:      id.TenantID("tenant-b"),
		ProjectID:     id.ProjectID("proj-b"),
		Repository:    "beta/worker",
		ResourceID:    "git-beta-worker",
		DefaultBranch: "main",
	})
	if err != nil {
		t.Fatalf("ProvisionRepository(tenant-b): %v", err)
	}
	if resB.CloneURL != "https://kodema-git-tenant-b.kodema-system.svc.cluster.local:8443/beta/worker.git" {
		t.Fatalf("resB.CloneURL = %q", resB.CloneURL)
	}

	wantObserved := []observedReq{
		{
			url:  "https://kodema-git-tenant-a.kodema-system.svc.cluster.local:8443/_kodema/repositories",
			auth: "Bearer provision-token-alpha",
		},
		{
			url:  "https://kodema-git-tenant-b.kodema-system.svc.cluster.local:8443/_kodema/repositories",
			auth: "Bearer provision-token-beta",
		},
	}
	if !reflect.DeepEqual(observed, wantObserved) {
		t.Fatalf("observed requests = %+v; want %+v", observed, wantObserved)
	}
}

func TestHTTPRepositoryProvisionerRejectsInvalidTenantIDForTemplates(t *testing.T) {
	tmp := t.TempDir()
	// Create a file outside the tenant directory that path traversal would reach.
	outsideDir := filepath.Join(tmp, "other")
	if err := os.MkdirAll(outsideDir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", outsideDir, err)
	}
	if err := os.WriteFile(filepath.Join(outsideDir, "token"), []byte("stolen-secret\n"), 0o600); err != nil {
		t.Fatalf("write outside token: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "git-provision"), 0o700); err != nil {
		t.Fatalf("mkdir git-provision: %v", err)
	}

	var hits atomic.Int32
	rt := repoRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		hits.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"created":true}`)),
		}, nil
	})

	p, err := newHTTPRepositoryProvisioner(Config{
		GitCompanion: &GitCompanionConfig{
			AdminURL:     "https://kodema-git.internal:8443",
			TokenFile:    filepath.Join(tmp, "git-provision", "{tenant}", "token"),
			RoundTripper: rt,
		},
	})
	if err != nil {
		t.Fatalf("newHTTPRepositoryProvisioner: %v", err)
	}

	badTenants := []id.TenantID{
		"",
		"../other",
		"../../other",
		"tenant/a",
		"tenant\\a",
		"tenant.a",
		"tenant a",
	}
	for _, bad := range badTenants {
		_, err := p.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
			TenantID:      bad,
			ProjectID:     id.ProjectID("proj-a"),
			Repository:    "shop/web",
			ResourceID:    "git-shop-web",
			DefaultBranch: "main",
		})
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("ProvisionRepository(TenantID=%q) err = %v (class=%v); want InvalidInput", bad, err, class)
		}
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("HTTP transport called %d times on invalid tenant IDs; want 0", got)
	}

	// Also verify invalid TenantID is rejected when only AdminURL contains {tenant}.
	staticToken := filepath.Join(tmp, "static-token")
	if err := os.WriteFile(staticToken, []byte("static-secret\n"), 0o600); err != nil {
		t.Fatalf("write static token: %v", err)
	}
	pAdminOnly, err := newHTTPRepositoryProvisioner(Config{
		GitCompanion: &GitCompanionConfig{
			AdminURL:     "https://kodema-git-{tenant}.internal:8443",
			TokenFile:    staticToken,
			RoundTripper: rt,
		},
	})
	if err != nil {
		t.Fatalf("newHTTPRepositoryProvisioner(admin-only): %v", err)
	}
	for _, bad := range []id.TenantID{"", "../other", "a@evil.example"} {
		_, err := pAdminOnly.ProvisionRepository(context.Background(), opsapp.RepositoryProvisionRequest{
			TenantID:      bad,
			ProjectID:     id.ProjectID("proj-a"),
			Repository:    "shop/web",
			ResourceID:    "git-shop-web",
			DefaultBranch: "main",
		})
		if class, ok := fault.ClassOf(err); !ok || class != fault.InvalidInput {
			t.Fatalf("admin-only ProvisionRepository(TenantID=%q) err = %v (class=%v); want InvalidInput", bad, err, class)
		}
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("HTTP transport called %d times on invalid admin-template tenant IDs; want 0", got)
	}
}
