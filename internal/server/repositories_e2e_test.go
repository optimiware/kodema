package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/internal/adapter/store/memory"
	"github.com/optimiware/kodema/internal/apps/git"
	gitapp "github.com/optimiware/kodema/internal/apps/git/app"
	"github.com/optimiware/kodema/internal/apps/git/app/actors"
	"github.com/optimiware/kodema/internal/control/identity"
	"github.com/optimiware/kodema/internal/control/tenancy"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/installation"
	shellapp "github.com/optimiware/kodema/internal/server"
)

type noopGitVerifier struct{}

func (noopGitVerifier) Verify(context.Context, api.VerifyRequest) (api.VerifyResponse, error) {
	return api.VerifyResponse{}, fault.New(fault.Unauthenticated, "unused")
}

func (noopGitVerifier) Report(context.Context, api.ReportRequest) (api.ReportResponse, error) {
	return api.ReportResponse{}, fault.New(fault.Unauthenticated, "unused")
}

type noopGitReadVerifier struct{}

func (noopGitReadVerifier) VerifyReadActor(context.Context, string, string) (git.Identity, error) {
	return git.Identity{}, fault.New(fault.Unauthenticated, "unused")
}

// TestRepositoryAndProjectOnboardingEndToEndRESTAndMCP wires a real Shell
// composition (REST + MCP HTTP edges) to a real Userland Git companion HTTP
// server and verifies project creation/listing (without tenant_id) and
// end-to-end repository creation/listing across REST and MCP.
func TestRepositoryAndProjectOnboardingEndToEndRESTAndMCP(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	const (
		tenantID          = "tenant-e2e"
		projectID         = "project-e2e"
		adminToken        = "dev-tenant-admin-token"
		projectAdminToken = "dev-project-admin-token"
		engToken          = "dev-project-eng-token"
		provSecret        = "e2e-git-provision-secret-token"
	)

	storageRoot := filepath.Join(tmp, "git-repos")
	journalDir := filepath.Join(tmp, "git-journal")
	tokenFile := filepath.Join(tmp, "git-provision-token")
	if err := os.WriteFile(tokenFile, []byte(provSecret+"\n"), 0o600); err != nil {
		t.Fatalf("write provision token: %v", err)
	}
	shellTokenFile := filepath.Join(tmp, "shell-git-provision-token")
	if err := os.WriteFile(shellTokenFile, []byte(provSecret+"\n"), 0o600); err != nil {
		t.Fatalf("write shell provision token: %v", err)
	}
	hookBinary := filepath.Join(tmp, "kodema-hook")
	if err := os.WriteFile(hookBinary, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write hook binary: %v", err)
	}

	if _, err := gitapp.ProvisionTenant(ctx, gitapp.ProvisionConfig{
		TenantID:         id.TenantID(tenantID),
		StorageRoot:      storageRoot,
		JournalDirectory: journalDir,
		HookBinary:       hookBinary,
	}); err != nil {
		t.Fatalf("gitapp.ProvisionTenant: %v", err)
	}
	registry, err := actors.NewRegistry(actors.Config{TenantID: id.TenantID(tenantID)})
	if err != nil {
		t.Fatalf("actors.NewRegistry: %v", err)
	}
	defer func() { _ = registry.Close() }()

	gitServer, err := gitapp.New(gitapp.Config{
		TenantID:           id.TenantID(tenantID),
		StorageRoot:        storageRoot,
		JournalDirectory:   journalDir,
		HookBinary:         hookBinary,
		ProvisionTokenFile: tokenFile,
		ResourceIDs:        map[string]string{"admin": "git-admin"},
		Verify:             noopGitVerifier{},
		Actors:             noopGitReadVerifier{},
		ActorHandles:       registry,
		HookActors:         registry,
	})
	if err != nil {
		t.Fatalf("gitapp.New: %v", err)
	}
	defer func() { _ = gitServer.Close() }()
	gitHTTP := httptest.NewServer(gitServer.Handler())
	defer gitHTTP.Close()

	expiry := time.Now().Add(time.Hour)
	app, err := shellapp.New(ctx, shellapp.Config{
		Profile:        shellapp.ProfileDevelopment,
		InstallationID: "inst-e2e",
		DataDir:        filepath.Join(tmp, "shell-data"),
		Installation: installation.Config{
			Environment: installation.EnvironmentDevelopment,
			State:       installation.StateMemory,
			Messaging:   installation.MessagingMemory,
			Runtime:     installation.RuntimeProcess,
			Keys:        installation.KeyEphemeral,
			PublicURL:   "https://127.0.0.1:8443",
		},
		Dev: &shellapp.DevelopmentConfig{
			Bootstrap: &shellapp.Bootstrap{
				TenantID:       tenantID,
				TenantName:     "Tenant E2E",
				ProjectID:      projectID,
				ProjectName:    "Project E2E",
				AdminPrincipal: "admin-1",
				AdminRoles:     []string{"admin", "engineer", "reviewer", "operator", "viewer"},
			},
			Tokens: []shellapp.DevToken{
				{
					Credential: adminToken,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID},
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: projectAdminToken,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID, ProjectID: projectID},
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: engToken,
					Principal: identity.Principal{
						ID:      "eng-1",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID, ProjectID: projectID},
						Roles:   []tenancy.Role{tenancy.RoleEngineer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
			},
		},
		GitCompanion: &shellapp.GitCompanionConfig{
			AdminURL:       gitHTTP.URL,
			PublicGitURL:   "https://127.0.0.1:8443/git",
			ServiceID:      "git",
			TrustedOrigins: []string{"https://127.0.0.1:8443"},
			TokenFile:      shellTokenFile,
		},
	})
	if err != nil {
		t.Fatalf("shellapp.New: %v", err)
	}
	defer func() { _ = app.Close(ctx) }()

	shellHTTP := httptest.NewServer(app.Handler())
	defer shellHTTP.Close()

	restDo := func(method, path, token, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, shellHTTP.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", api.MediaType)
		resp, err := shellHTTP.Client().Do(req)
		if err != nil {
			t.Fatalf("do request: %v", err)
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		return resp.StatusCode, buf.Bytes()
	}

	mcpCall := func(token, toolName string, args map[string]any) map[string]any {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      toolName,
				"arguments": args,
			},
		})
		status, raw := restDo(http.MethodPost, "/mcp", token, string(payload))
		if status != http.StatusOK {
			t.Fatalf("MCP %s status = %d body=%s", toolName, status, raw)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode MCP response: %v body=%s", err, raw)
		}
		return out
	}

	// Create the enforcing "git" service account in project-e2e and grant eng-1
	// project membership so current-authority checks pass for project reads.
	status, raw := restDo(http.MethodPost, "/api/v1/projects/"+projectID+"/services", projectAdminToken,
		`{"input":{"service_id":"git","roles":["viewer"]},"idempotency_key":"svc-git-1"}`)
	if status != http.StatusOK {
		t.Fatalf("service.create git status=%d body=%s", status, raw)
	}
	status, raw = restDo(http.MethodPut, "/api/v1/projects/"+projectID+"/memberships/eng-1", adminToken,
		`{"input":{"roles":["engineer"]},"idempotency_key":"grant-eng-1"}`)
	if status != http.StatusOK {
		t.Fatalf("membership.grant eng-1 status=%d body=%s", status, raw)
	}

	// 1. REST POST /api/v1/projects (omitting tenant_id!) creates project-2 with
	// creator membership automatically initialized.
	status, raw = restDo(http.MethodPost, "/api/v1/projects", adminToken,
		`{"input":{"project_id":"project-two","name":"Project Two"},"idempotency_key":"proj-two-1"}`)
	if status != http.StatusOK {
		t.Fatalf("POST /api/v1/projects status=%d body=%s", status, raw)
	}
	// REST GET /api/v1/projects (omitting tenant_id!) lists both projects.
	status, raw = restDo(http.MethodGet, "/api/v1/projects", adminToken, "")
	if status != http.StatusOK {
		t.Fatalf("GET /api/v1/projects status=%d body=%s", status, raw)
	}
	if !strings.Contains(string(raw), `"project-two"`) || !strings.Contains(string(raw), `"`+projectID+`"`) {
		t.Fatalf("GET /api/v1/projects body = %s; want both projects", raw)
	}

	// 2. MCP project_list without tenant_id works identically.
	mcpProjList := mcpCall(adminToken, "project_list", map[string]any{})
	mcpProjBytes, _ := json.Marshal(mcpProjList)
	if !strings.Contains(string(mcpProjBytes), "project-two") {
		t.Fatalf("MCP project_list = %s; missing project-two", mcpProjBytes)
	}

	// 3. MCP repository_create provisions bare repo + conf/git.conf + binding in one call!
	mcpRepoCreate := mcpCall(adminToken, "repository_create", map[string]any{
		"project_id":      projectID,
		"name":            "payments/api",
		"default_branch":  "main",
		"idempotency_key": "mcp-repo-1",
	})
	mcpRepoBytes, _ := json.Marshal(mcpRepoCreate)
	if !strings.Contains(string(mcpRepoBytes), "git-payments-api") ||
		!strings.Contains(string(mcpRepoBytes), "https://127.0.0.1:8443/git/payments/api.git") {
		t.Fatalf("MCP repository_create = %s", mcpRepoBytes)
	}
	// Verify the Git companion actually provisioned the bare repo and option resource!
	resolvedResource, err := gitServer.Service().ResolveResourceID(ctx, "payments/api")
	if err != nil || resolvedResource != "git-payments-api" {
		t.Fatalf("gitServer ResolveResourceID(payments/api) = %q, %v; want git-payments-api", resolvedResource, err)
	}

	// 4. REST repository.list returns the onboarded repository.
	status, raw = restDo(http.MethodGet, "/api/v1/projects/"+projectID+"/repositories", engToken, "")
	if status != http.StatusOK {
		t.Fatalf("GET repositories status=%d body=%s", status, raw)
	}
	var listEnv struct {
		Operation string                   `json:"operation"`
		Body      api.RepositoryListResult `json:"body"`
	}
	if err := json.Unmarshal(raw, &listEnv); err != nil {
		t.Fatalf("decode repository.list: %v", err)
	}
	if len(listEnv.Body.Repositories) != 1 || listEnv.Body.Repositories[0].Name != "payments/api" ||
		listEnv.Body.Repositories[0].CloneURL != "https://127.0.0.1:8443/git/payments/api.git" {
		t.Fatalf("repository.list = %+v", listEnv.Body.Repositories)
	}

	// 5. MCP repository_list returns the same repository.
	mcpRepoList := mcpCall(engToken, "repository_list", map[string]any{"project_id": projectID})
	mcpRepoListBytes, _ := json.Marshal(mcpRepoList)
	if !strings.Contains(string(mcpRepoListBytes), "payments/api") {
		t.Fatalf("MCP repository_list = %s", mcpRepoListBytes)
	}

	// 6. Both REST and MCP reject ".git" repository names ("admin.git" and
	// "shop/web.git") BEFORE committing any binding.
	for _, badName := range []string{"admin.git", "shop/web.git"} {
		status, raw = restDo(http.MethodPost, "/api/v1/projects/"+projectID+"/repositories", adminToken,
			`{"input":{"name":"`+badName+`"},"idempotency_key":"rest-bad-`+badName+`"}`)
		if status != http.StatusBadRequest {
			t.Fatalf("REST repository.create %q status=%d body=%s; want 400", badName, status, raw)
		}
		mcpBad := mcpCall(adminToken, "repository_create", map[string]any{
			"project_id":      projectID,
			"name":            badName,
			"idempotency_key": "mcp-bad-" + badName,
		})
		mcpBadBytes, _ := json.Marshal(mcpBad)
		if !strings.Contains(string(mcpBadBytes), `"isError":true`) && !strings.Contains(string(mcpBadBytes), `"error"`) {
			t.Fatalf("MCP repository_create %q = %s; want error", badName, mcpBadBytes)
		}
	}
	// Verify no orphaned bindings were committed.
	status, raw = restDo(http.MethodGet, "/api/v1/projects/"+projectID+"/repositories", engToken, "")
	if status != http.StatusOK {
		t.Fatalf("GET repositories after .git rejections status=%d body=%s", status, raw)
	}
	listEnv = struct {
		Operation string                   `json:"operation"`
		Body      api.RepositoryListResult `json:"body"`
	}{}
	if err := json.Unmarshal(raw, &listEnv); err != nil {
		t.Fatalf("decode repository.list: %v", err)
	}
	if len(listEnv.Body.Repositories) != 1 || listEnv.Body.Repositories[0].Name != "payments/api" {
		t.Fatalf("repository.list after .git rejections = %+v; want only payments/api", listEnv.Body.Repositories)
	}

	// 7. A server-to-git provision token mismatch surfaces as HTTP 503
	// dependency_unavailable (NEVER 401 unauthenticated to end users).
	if err := os.WriteFile(shellTokenFile, []byte("mismatched-provision-secret\n"), 0o600); err != nil {
		t.Fatalf("rotate shell token file to mismatched secret: %v", err)
	}
	status, raw = restDo(http.MethodPost, "/api/v1/projects/"+projectID+"/repositories", adminToken,
		`{"input":{"name":"token/mismatch"},"idempotency_key":"rest-tok-mismatch"}`)
	if status != http.StatusServiceUnavailable || !strings.Contains(string(raw), `"dependency_unavailable"`) {
		t.Fatalf("REST repository.create with mismatched git provision token status=%d body=%s; want 503 dependency_unavailable", status, raw)
	}
}

type hostRoutingRoundTripper struct {
	routes map[string]*url.URL
	base   http.RoundTripper
}

func (rt *hostRoutingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	target, ok := rt.routes[req.URL.Host]
	if !ok {
		return nil, fault.New(fault.NotFound, "no route for host "+req.URL.Host)
	}
	cloned := req.Clone(req.Context())
	cloned.URL.Scheme = target.Scheme
	cloned.URL.Host = target.Host
	cloned.Host = target.Host
	return rt.base.RoundTrip(cloned)
}

// TestMultiTenantRepositoryOnboardingEndToEndRESTAndMCP wires a single Shell
// composition with {tenant} templates across AdminURL, PublicGitURL, ServiceID,
// TrustedOrigins, and TokenFile to two independent Userland Git companions
// (tenant-a and tenant-b) with distinct provision tokens, verifying per-tenant
// token isolation, routing, service IDs, trusted origins, and clone URLs across
// REST and MCP repository creation and listing.
func TestMultiTenantRepositoryOnboardingEndToEndRESTAndMCP(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	hookBinary := filepath.Join(tmp, "kodema-hook")
	if err := os.WriteFile(hookBinary, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("write hook binary: %v", err)
	}

	type tenantGitFixture struct {
		tenantID   string
		projectID  string
		adminToken string
		projToken  string
		secret     string
		gitServer  *gitapp.Server
		gitHTTP    *httptest.Server
		host       string
	}

	fixtures := []*tenantGitFixture{
		{
			tenantID:   "tenant-a",
			projectID:  "proj-a",
			adminToken: "token-tenant-a-admin",
			projToken:  "token-tenant-a-proj-admin",
			secret:     "tenant-a-only-provision-secret",
		},
		{
			tenantID:   "tenant-b",
			projectID:  "proj-b",
			adminToken: "token-tenant-b-admin",
			projToken:  "token-tenant-b-proj-admin",
			secret:     "tenant-b-only-provision-secret",
		},
	}

	routes := make(map[string]*url.URL, len(fixtures))
	for _, f := range fixtures {
		tenantDir := filepath.Join(tmp, f.tenantID)
		if err := os.MkdirAll(tenantDir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", tenantDir, err)
		}
		storageRoot := filepath.Join(tenantDir, "git-repos")
		journalDir := filepath.Join(tenantDir, "git-journal")
		gitTokenFile := filepath.Join(tenantDir, "git-provision-token")
		if err := os.WriteFile(gitTokenFile, []byte(f.secret+"\n"), 0o600); err != nil {
			t.Fatalf("write git token for %s: %v", f.tenantID, err)
		}
		shellTenantDir := filepath.Join(tmp, "shell-provision", f.tenantID)
		if err := os.MkdirAll(shellTenantDir, 0o700); err != nil {
			t.Fatalf("mkdir shell token dir for %s: %v", f.tenantID, err)
		}
		if err := os.WriteFile(filepath.Join(shellTenantDir, "token"), []byte(f.secret+"\n"), 0o600); err != nil {
			t.Fatalf("write shell token for %s: %v", f.tenantID, err)
		}

		if _, err := gitapp.ProvisionTenant(ctx, gitapp.ProvisionConfig{
			TenantID:         id.TenantID(f.tenantID),
			StorageRoot:      storageRoot,
			JournalDirectory: journalDir,
			HookBinary:       hookBinary,
		}); err != nil {
			t.Fatalf("gitapp.ProvisionTenant(%s): %v", f.tenantID, err)
		}
		registry, err := actors.NewRegistry(actors.Config{TenantID: id.TenantID(f.tenantID)})
		if err != nil {
			t.Fatalf("actors.NewRegistry(%s): %v", f.tenantID, err)
		}
		t.Cleanup(func() { _ = registry.Close() })

		gitServer, err := gitapp.New(gitapp.Config{
			TenantID:           id.TenantID(f.tenantID),
			StorageRoot:        storageRoot,
			JournalDirectory:   journalDir,
			HookBinary:         hookBinary,
			ProvisionTokenFile: gitTokenFile,
			ResourceIDs:        map[string]string{"admin": "git-admin"},
			Verify:             noopGitVerifier{},
			Actors:             noopGitReadVerifier{},
			ActorHandles:       registry,
			HookActors:         registry,
		})
		if err != nil {
			t.Fatalf("gitapp.New(%s): %v", f.tenantID, err)
		}
		t.Cleanup(func() { _ = gitServer.Close() })
		gitHTTP := httptest.NewServer(gitServer.Handler())
		t.Cleanup(gitHTTP.Close)

		parsed, err := url.Parse(gitHTTP.URL)
		if err != nil {
			t.Fatalf("parse gitHTTP.URL: %v", err)
		}
		f.gitServer = gitServer
		f.gitHTTP = gitHTTP
		f.host = "kodema-git-" + f.tenantID + ".kodema-system.svc.cluster.local:8443"
		routes[f.host] = parsed
	}

	expiry := time.Now().Add(time.Hour)
	allRoles := []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer}
	memStore := memory.New()
	sharedAdapters := &shellapp.Adapters{
		Store:       memStore,
		Scanner:     memStore,
		Controller:  memStore.Controller(),
		Outbox:      memStore,
		Credentials: memStore,
	}
	devTokens := []shellapp.DevToken{
		{
			Credential: "token-tenant-a-admin",
			Principal: identity.Principal{
				ID:      "admin-a",
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: "tenant-a"},
				Roles:   allRoles,
				Enabled: true,
				Version: 1,
			},
			ExpiresAt: expiry,
		},
		{
			Credential: "token-tenant-a-proj-admin",
			Principal: identity.Principal{
				ID:      "admin-a",
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: "tenant-a", ProjectID: "proj-a"},
				Roles:   allRoles,
				Enabled: true,
				Version: 1,
			},
			ExpiresAt: expiry,
		},
		{
			Credential: "token-tenant-b-admin",
			Principal: identity.Principal{
				ID:      "admin-b",
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: "tenant-b"},
				Roles:   allRoles,
				Enabled: true,
				Version: 1,
			},
			ExpiresAt: expiry,
		},
		{
			Credential: "token-tenant-b-proj-admin",
			Principal: identity.Principal{
				ID:      "admin-b",
				Kind:    identity.KindHuman,
				Scope:   tenancy.Scope{TenantID: "tenant-b", ProjectID: "proj-b"},
				Roles:   allRoles,
				Enabled: true,
				Version: 1,
			},
			ExpiresAt: expiry,
		},
	}

	// Seed tenant-a into the shared memory store via Bootstrap, then start the
	// served Shell composition bootstrapping tenant-b into the same store so
	// both tenants are active with full tenant-admin memberships.
	seedApp, err := shellapp.New(ctx, shellapp.Config{
		Profile:        shellapp.ProfileDevelopment,
		InstallationID: "inst-mt-e2e",
		DataDir:        filepath.Join(tmp, "shell-data"),
		Installation: installation.Config{
			Environment: installation.EnvironmentDevelopment,
			State:       installation.StateMemory,
			Messaging:   installation.MessagingMemory,
			Runtime:     installation.RuntimeProcess,
			Keys:        installation.KeyEphemeral,
			PublicURL:   "https://127.0.0.1:8443",
		},
		Adapters: sharedAdapters,
		Dev: &shellapp.DevelopmentConfig{
			Bootstrap: &shellapp.Bootstrap{
				TenantID:       "tenant-a",
				TenantName:     "Tenant A",
				ProjectID:      "proj-a",
				ProjectName:    "Project A",
				AdminPrincipal: "admin-a",
				AdminRoles:     []string{"admin", "engineer", "reviewer", "operator", "viewer"},
			},
			Tokens: devTokens,
		},
	})
	if err != nil {
		t.Fatalf("seed tenant-a shellapp.New: %v", err)
	}
	if err := seedApp.Close(ctx); err != nil {
		t.Fatalf("seed tenant-a shellapp.Close: %v", err)
	}

	app, err := shellapp.New(ctx, shellapp.Config{
		Profile:        shellapp.ProfileDevelopment,
		InstallationID: "inst-mt-e2e",
		DataDir:        filepath.Join(tmp, "shell-data"),
		Installation: installation.Config{
			Environment: installation.EnvironmentDevelopment,
			State:       installation.StateMemory,
			Messaging:   installation.MessagingMemory,
			Runtime:     installation.RuntimeProcess,
			Keys:        installation.KeyEphemeral,
			PublicURL:   "https://127.0.0.1:8443",
		},
		Adapters: sharedAdapters,
		Dev: &shellapp.DevelopmentConfig{
			Bootstrap: &shellapp.Bootstrap{
				TenantID:       "tenant-b",
				TenantName:     "Tenant B",
				ProjectID:      "proj-b",
				ProjectName:    "Project B",
				AdminPrincipal: "admin-b",
				AdminRoles:     []string{"admin", "engineer", "reviewer", "operator", "viewer"},
			},
			Tokens: devTokens,
		},
		GitCompanion: &shellapp.GitCompanionConfig{
			AdminURL:       "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
			PublicGitURL:   "https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443",
			ServiceID:      "kodema-git-{tenant}",
			TrustedOrigins: []string{"https://kodema-git-{tenant}.kodema-system.svc.cluster.local:8443"},
			TokenFile:      filepath.Join(tmp, "shell-provision", "{tenant}", "token"),
			RoundTripper: &hostRoutingRoundTripper{
				routes: routes,
				base:   http.DefaultTransport,
			},
		},
	})
	if err != nil {
		t.Fatalf("shellapp.New: %v", err)
	}
	defer func() { _ = app.Close(ctx) }()

	shellHTTP := httptest.NewServer(app.Handler())
	defer shellHTTP.Close()

	restDo := func(method, path, token, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, shellHTTP.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", api.MediaType)
		resp, err := shellHTTP.Client().Do(req)
		if err != nil {
			t.Fatalf("do request: %v", err)
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		return resp.StatusCode, buf.Bytes()
	}

	// Register each tenant's per-tenant enforcing git service account and create
	// + list a repository in each tenant (REST and MCP).
	for _, f := range fixtures {
		wantService := "kodema-git-" + f.tenantID
		wantOrigin := "https://" + f.host
		repoName := f.tenantID + "/service"
		wantResourceID := api.DeriveRepositoryResourceID(repoName)
		wantCloneURL := wantOrigin + "/" + repoName + ".git"

		status, raw := restDo(http.MethodPut, "/api/v1/projects/"+f.projectID+"/memberships/admin-"+strings.TrimPrefix(f.tenantID, "tenant-"), f.adminToken,
			`{"input":{"roles":["admin","engineer","reviewer","operator","viewer"]},"idempotency_key":"grant-`+f.tenantID+`"}`)
		if status != http.StatusOK {
			t.Fatalf("membership.grant %s status=%d body=%s", f.tenantID, status, raw)
		}

		status, raw = restDo(http.MethodPost, "/api/v1/projects/"+f.projectID+"/services", f.projToken,
			`{"input":{"service_id":"`+wantService+`","roles":["viewer"]},"idempotency_key":"svc-`+f.tenantID+`"}`)
		if status != http.StatusOK {
			t.Fatalf("service.create %s status=%d body=%s", wantService, status, raw)
		}

		status, raw = restDo(http.MethodPost, "/api/v1/projects/"+f.projectID+"/repositories", f.adminToken,
			`{"input":{"name":"`+repoName+`","default_branch":"main"},"idempotency_key":"repo-`+f.tenantID+`"}`)
		if status != http.StatusOK {
			t.Fatalf("repository.create %s status=%d body=%s", f.tenantID, status, raw)
		}
		var createEnv struct {
			Body api.RepositoryCreateResult `json:"body"`
		}
		if err := json.Unmarshal(raw, &createEnv); err != nil {
			t.Fatalf("decode repository.create(%s): %v", f.tenantID, err)
		}
		if createEnv.Body.EnforcingService != wantService ||
			createEnv.Body.CloneURL != wantCloneURL ||
			!reflect.DeepEqual(createEnv.Body.TrustedOrigins, []string{wantOrigin}) ||
			!createEnv.Body.Created {
			t.Fatalf("repository.create(%s) = %+v; want service=%q clone_url=%q origins=[%q] created=true",
				f.tenantID, createEnv.Body, wantService, wantCloneURL, wantOrigin)
		}

		// Verify the repository was provisioned in THIS tenant's Git companion.
		resolved, err := f.gitServer.Service().ResolveResourceID(ctx, repoName)
		if err != nil || resolved != wantResourceID {
			t.Fatalf("%s gitServer ResolveResourceID(%s) = %q, %v; want %q", f.tenantID, repoName, resolved, err, wantResourceID)
		}

		// Verify repository.list renders the tenant-expanded clone_url.
		status, raw = restDo(http.MethodGet, "/api/v1/projects/"+f.projectID+"/repositories", f.projToken, "")
		if status != http.StatusOK {
			t.Fatalf("repository.list %s status=%d body=%s", f.tenantID, status, raw)
		}
		var listEnv struct {
			Body api.RepositoryListResult `json:"body"`
		}
		if err := json.Unmarshal(raw, &listEnv); err != nil {
			t.Fatalf("decode repository.list(%s): %v", f.tenantID, err)
		}
		if len(listEnv.Body.Repositories) != 1 ||
			listEnv.Body.Repositories[0].CloneURL != wantCloneURL ||
			listEnv.Body.Repositories[0].EnforcingService != wantService ||
			!reflect.DeepEqual(listEnv.Body.Repositories[0].TrustedOrigins, []string{wantOrigin}) {
			t.Fatalf("repository.list(%s) = %+v", f.tenantID, listEnv.Body.Repositories)
		}

		// Also verify MCP repository_create and repository_list expand {tenant} identically.
		mcpRepoName := f.tenantID + "/mcp-app"
		mcpCloneURL := wantOrigin + "/" + mcpRepoName + ".git"
		mcpCreatePayload, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "tools/call",
			"params": map[string]any{
				"name": "repository_create",
				"arguments": map[string]any{
					"project_id":      f.projectID,
					"name":            mcpRepoName,
					"default_branch":  "main",
					"idempotency_key": "mcp-repo-" + f.tenantID,
				},
			},
		})
		status, raw = restDo(http.MethodPost, "/mcp", f.adminToken, string(mcpCreatePayload))
		if status != http.StatusOK || !strings.Contains(string(raw), mcpCloneURL) || !strings.Contains(string(raw), wantService) {
			t.Fatalf("MCP repository_create(%s) status=%d body=%s; want clone_url=%q service=%q",
				f.tenantID, status, raw, mcpCloneURL, wantService)
		}
		mcpListPayload, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      2,
			"method":  "tools/call",
			"params": map[string]any{
				"name": "repository_list",
				"arguments": map[string]any{
					"project_id": f.projectID,
				},
			},
		})
		status, raw = restDo(http.MethodPost, "/mcp", f.projToken, string(mcpListPayload))
		if status != http.StatusOK || !strings.Contains(string(raw), mcpCloneURL) || !strings.Contains(string(raw), wantCloneURL) {
			t.Fatalf("MCP repository_list(%s) status=%d body=%s; want both clone URLs", f.tenantID, status, raw)
		}
	}

	// Verify isolation: tenant-a's repo does NOT exist on tenant-b's Git companion
	// and vice versa.
	if _, err := fixtures[1].gitServer.Service().ResolveResourceID(ctx, "tenant-a/service"); err == nil {
		t.Fatal("tenant-b git companion unexpectedly contains tenant-a/service")
	}
	if _, err := fixtures[0].gitServer.Service().ResolveResourceID(ctx, "tenant-b/service"); err == nil {
		t.Fatal("tenant-a git companion unexpectedly contains tenant-b/service")
	}
}

// TestMultiProjectTenantScopedGitServeEndToEnd reproduces and verifies the
// Issue #111 fix end-to-end: a single tenant-scoped `kodema git serve`
// authenticating with a real HMAC `kodsvc.git.<mac>` credential serves
// repositories across two projects (project-a and project-b) on the same
// Control Plane instance, enforcing per-project actor isolation for real
// `git ls-remote`, `git clone`, and `git push` (with live pre-receive and
// post-receive hooks reporting `git.ref.updated` into each owning project).
func TestMultiProjectTenantScopedGitServeEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}
	ctx := context.Background()
	tmp := t.TempDir()

	// Compile the real kodema binary so git-receive-pack runs genuine
	// `kodema git hook pre-receive` and `post-receive` callbacks.
	kodemaBin := filepath.Join(tmp, "kodema")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", kodemaBin, "../../cmd/kodema")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build kodema hook binary: %v\n%s", err, out)
	}

	const (
		tenantID   = "tenant-mp-git"
		projectA   = "project-a"
		projectB   = "project-b"
		adminToken = "dev-mp-tenant-admin"
		engAToken  = "dev-mp-eng-a"
		engBToken  = "dev-mp-eng-b"
		provSecret = "mp-git-provision-secret"
	)

	storageRoot := filepath.Join(tmp, "git-repos")
	journalDir := filepath.Join(tmp, "git-journal")
	provTokenFile := filepath.Join(tmp, "git-provision-token")
	if err := os.WriteFile(provTokenFile, []byte(provSecret+"\n"), 0o600); err != nil {
		t.Fatalf("write provision token: %v", err)
	}
	if _, err := gitapp.ProvisionTenant(ctx, gitapp.ProvisionConfig{
		TenantID:         id.TenantID(tenantID),
		StorageRoot:      storageRoot,
		JournalDirectory: journalDir,
		HookBinary:       kodemaBin,
	}); err != nil {
		t.Fatalf("gitapp.ProvisionTenant: %v", err)
	}

	// Start the Git HTTP listener first so its concrete URL can be registered as
	// trusted origin and clone URL in Shell, then swap in the real handler once
	// the tenant-scoped git service credential is issued.
	var gitHandler atomic.Value
	gitHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, _ := gitHandler.Load().(http.Handler)
		if h == nil {
			http.Error(w, "git handler not ready", http.StatusServiceUnavailable)
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer gitHTTP.Close()

	expiry := time.Now().Add(time.Hour)
	app, err := shellapp.New(ctx, shellapp.Config{
		Profile:        shellapp.ProfileDevelopment,
		InstallationID: "inst-mp-git-e2e",
		DataDir:        filepath.Join(tmp, "shell-data"),
		Installation: installation.Config{
			Environment: installation.EnvironmentDevelopment,
			State:       installation.StateMemory,
			Messaging:   installation.MessagingMemory,
			Runtime:     installation.RuntimeProcess,
			Keys:        installation.KeyEphemeral,
			PublicURL:   "https://127.0.0.1:8443",
		},
		Dev: &shellapp.DevelopmentConfig{
			Bootstrap: &shellapp.Bootstrap{
				TenantID:       tenantID,
				TenantName:     "Tenant MP Git",
				ProjectID:      projectA,
				ProjectName:    "Project A",
				AdminPrincipal: "admin-1",
				AdminRoles:     []string{"admin", "engineer", "reviewer", "operator", "viewer"},
			},
			Tokens: []shellapp.DevToken{
				{
					Credential: adminToken,
					Principal: identity.Principal{
						ID:      "admin-1",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID},
						Roles:   []tenancy.Role{tenancy.RoleAdmin, tenancy.RoleEngineer, tenancy.RoleReviewer, tenancy.RoleOperator, tenancy.RoleViewer},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: engAToken,
					Principal: identity.Principal{
						ID:      "eng-a",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
				{
					Credential: engBToken,
					Principal: identity.Principal{
						ID:      "eng-b",
						Kind:    identity.KindHuman,
						Scope:   tenancy.Scope{TenantID: tenantID},
						Enabled: true,
						Version: 1,
					},
					ExpiresAt: expiry,
				},
			},
		},
		GitCompanion: &shellapp.GitCompanionConfig{
			AdminURL:       gitHTTP.URL,
			PublicGitURL:   gitHTTP.URL,
			ServiceID:      "git",
			TrustedOrigins: []string{"https://127.0.0.1:8443"},
			TokenFile:      provTokenFile,
		},
	})
	if err != nil {
		t.Fatalf("shellapp.New: %v", err)
	}
	defer func() { _ = app.Close(ctx) }()

	shellHTTP := httptest.NewServer(app.Handler())
	defer shellHTTP.Close()

	restDo := func(method, path, token, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, shellHTTP.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatalf("new request %s %s: %v", method, path, err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", api.MediaType)
		resp, err := shellHTTP.Client().Do(req)
		if err != nil {
			t.Fatalf("do request %s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		return resp.StatusCode, buf.Bytes()
	}

	// 1. Create project-b and grant project-isolated engineer memberships.
	status, raw := restDo(http.MethodPost, "/api/v1/projects", adminToken,
		`{"idempotency_key":"mp-git-proj-b","input":{"project_id":"`+projectB+`","name":"Project B"}}`)
	if status != http.StatusOK {
		t.Fatalf("create project-b status=%d body=%s", status, raw)
	}
	for _, grant := range []struct{ proj, user, key string }{
		{projectA, "eng-a", "mp-git-grant-a"},
		{projectB, "eng-b", "mp-git-grant-b"},
	} {
		status, raw = restDo(http.MethodPut, "/api/v1/projects/"+grant.proj+"/memberships/"+grant.user, adminToken,
			`{"idempotency_key":"`+grant.key+`","input":{"roles":["engineer"]}}`)
		if status != http.StatusOK {
			t.Fatalf("grant %s in %s status=%d body=%s", grant.user, grant.proj, status, raw)
		}
	}

	// 2. Create tenant-scoped service/git via POST /api/v1/services and issue its
	// real kodsvc.git.<mac> credential.
	status, raw = restDo(http.MethodPost, "/api/v1/services", adminToken,
		`{"idempotency_key":"mp-git-create-svc","input":{"service_id":"git","roles":["viewer"]}}`)
	if status != http.StatusOK {
		t.Fatalf("create tenant service/git status=%d body=%s", status, raw)
	}
	status, raw = restDo(http.MethodPost, "/api/v1/services/git/credentials", adminToken,
		`{"idempotency_key":"mp-git-issue-cred","expected_version":1,"input":{"ttl_seconds":3600}}`)
	if status != http.StatusOK {
		t.Fatalf("issue tenant service/git credential status=%d body=%s", status, raw)
	}
	var credEnv struct {
		Body api.ServiceCredentialIssueResult `json:"body"`
	}
	if err := json.Unmarshal(raw, &credEnv); err != nil || credEnv.Body.Token == nil || *credEnv.Body.Token == "" {
		t.Fatalf("decode service/git credential: %v (%s)", err, raw)
	}
	gitCredFile := filepath.Join(tmp, "git-service-token")
	if err := os.WriteFile(gitCredFile, []byte(*credEnv.Body.Token+"\n"), 0o600); err != nil {
		t.Fatalf("write git service token: %v", err)
	}

	// 3. Start the single tenant-scoped Git companion wired to Shell via the real
	// public HTTP transport and hook Unix socket.
	shortSocketDir, err := os.MkdirTemp("", "kodema-mp-git-")
	if err != nil {
		t.Fatalf("mkdirtemp hook socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortSocketDir) })

	gitTransport, err := gitapp.NewTransport(gitapp.TransportConfig{
		BaseURL:        shellHTTP.URL,
		CredentialFile: gitCredFile,
		RoundTripper:   shellHTTP.Client().Transport,
	})
	if err != nil {
		t.Fatalf("gitapp.NewTransport: %v", err)
	}
	verifyClient, err := gitapp.NewVerificationClient(gitTransport)
	if err != nil {
		t.Fatalf("gitapp.NewVerificationClient: %v", err)
	}
	readVerifier, err := git.NewReadVerifier(verifyClient, func(repository string) (string, error) {
		return "", git.UnregisteredRepositoryError(repository)
	})
	if err != nil {
		t.Fatalf("git.NewReadVerifier: %v", err)
	}
	registry, err := actors.NewRegistry(actors.Config{TenantID: id.TenantID(tenantID)})
	if err != nil {
		t.Fatalf("actors.NewRegistry: %v", err)
	}
	defer func() { _ = registry.Close() }()

	gitServer, err := gitapp.New(gitapp.Config{
		TenantID:           id.TenantID(tenantID),
		StorageRoot:        storageRoot,
		JournalDirectory:   journalDir,
		HookBinary:         kodemaBin,
		HookSocketPath:     filepath.Join(shortSocketDir, "h.sock"),
		ProvisionTokenFile: provTokenFile,
		ResourceIDs:        map[string]string{"admin": "git-admin"},
		Verify:             verifyClient,
		Actors:             readVerifier,
		ActorHandles:       registry,
		HookActors:         registry,
	})
	if err != nil {
		t.Fatalf("gitapp.New: %v", err)
	}
	if err := gitServer.StartHooks(ctx); err != nil {
		t.Fatalf("gitServer.StartHooks: %v", err)
	}
	defer func() { _ = gitServer.Close() }()
	gitHandler.Store(gitServer.Handler())

	// 4. Create repositories in project-a and project-b via repository.create.
	for _, repo := range []struct{ proj, name, key string }{
		{projectA, "project-a/app-a", "mp-git-repo-a"},
		{projectB, "project-b/app-b", "mp-git-repo-b"},
	} {
		status, raw = restDo(http.MethodPost, "/api/v1/projects/"+repo.proj+"/repositories", adminToken,
			`{"idempotency_key":"`+repo.key+`","input":{"name":"`+repo.name+`","default_branch":"main"}}`)
		if status != http.StatusOK {
			t.Fatalf("repository.create %s in %s status=%d body=%s", repo.name, repo.proj, status, raw)
		}
	}

	runGit := func(dir, token string, args ...string) (string, error) {
		fullArgs := []string{"-c", "http.extraHeader=Authorization: Bearer " + token}
		fullArgs = append(fullArgs, args...)
		cmd := exec.CommandContext(ctx, "git", fullArgs...)
		if dir != "" {
			cmd.Dir = dir
		}
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=" + tmp,
			"GIT_TERMINAL_PROMPT=0",
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=Kodema Test",
			"GIT_AUTHOR_EMAIL=test@kodema.local",
			"GIT_COMMITTER_NAME=Kodema Test",
			"GIT_COMMITTER_EMAIL=test@kodema.local",
		}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	repoAURL := gitHTTP.URL + "/project-a/app-a.git"
	repoBURL := gitHTTP.URL + "/project-b/app-b.git"

	// 5. Real git ls-remote, clone, and push succeed in BOTH project-a and project-b.
	for _, tc := range []struct {
		name, proj, repoURL, token, workDir string
	}{
		{"project-a", projectA, repoAURL, engAToken, filepath.Join(tmp, "clone-a")},
		{"project-b", projectB, repoBURL, engBToken, filepath.Join(tmp, "clone-b")},
	} {
		if out, err := runGit("", tc.token, "ls-remote", tc.repoURL); err != nil {
			t.Fatalf("%s git ls-remote failed: %v\n%s", tc.name, err, out)
		}
		if out, err := runGit("", tc.token, "clone", tc.repoURL, tc.workDir); err != nil {
			t.Fatalf("%s git clone failed: %v\n%s", tc.name, err, out)
		}
		if err := os.WriteFile(filepath.Join(tc.workDir, "README.md"), []byte("hello "+tc.name+"\n"), 0o644); err != nil {
			t.Fatalf("write README %s: %v", tc.name, err)
		}
		if out, err := runGit(tc.workDir, tc.token, "checkout", "-b", "work/init"); err != nil {
			t.Fatalf("%s git checkout failed: %v\n%s", tc.name, err, out)
		}
		if out, err := runGit(tc.workDir, tc.token, "add", "README.md"); err != nil {
			t.Fatalf("%s git add failed: %v\n%s", tc.name, err, out)
		}
		if out, err := runGit(tc.workDir, tc.token, "commit", "-m", "initial "+tc.name); err != nil {
			t.Fatalf("%s git commit failed: %v\n%s", tc.name, err, out)
		}
		if out, err := runGit(tc.workDir, tc.token, "push", "origin", "HEAD:refs/heads/work/init"); err != nil {
			t.Fatalf("%s git push failed: %v\n%s", tc.name, err, out)
		}
		if out, err := runGit("", tc.token, "ls-remote", "--heads", tc.repoURL, "refs/heads/work/init"); err != nil || !strings.Contains(out, "refs/heads/work/init") {
			t.Fatalf("%s git ls-remote after push = %q, err=%v; want refs/heads/work/init", tc.name, out, err)
		}

		// Confirm verified resource.report (git.ref.updated) landed in the owning project.
		status, raw = restDo(http.MethodGet, "/api/v1/projects/"+tc.proj+"/events", adminToken, "")
		if status != http.StatusOK || !strings.Contains(string(raw), `"git.ref.updated"`) {
			t.Fatalf("%s events status=%d body=%s; want git.ref.updated report", tc.name, status, raw)
		}
	}

	// 6. Cross-project actor isolation: eng-a is denied on project-b/app-b and
	// eng-b is denied on project-a/app-a.
	if out, err := runGit("", engAToken, "ls-remote", repoBURL); err == nil {
		t.Fatalf("eng-a unexpectedly read project-b repo:\n%s", out)
	}
	if out, err := runGit("", engBToken, "ls-remote", repoAURL); err == nil {
		t.Fatalf("eng-b unexpectedly read project-a repo:\n%s", out)
	}
}
