// Behavioural specification for the driver-owned persistent cache claim
// provisioning primitive (*Cluster.CreateCacheClaim).
//
// Like CreateClaim for retained workspaces, this method lives strictly on
// *Cluster outside the runtimekube.Cluster seam. It derives the deterministic
// PVC name via runtimekube.CacheClaimName, stamps tenant and cache-key
// ownership labels plus optional project and cache-name informational labels
// (only when valid Kubernetes label values), never stamps workspace/execution/
// retention labels or annotations, and never waits for the claim to become
// Bound.
package kube_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimekube "github.com/optimiware/kodema/internal/adapter/runtime/kube"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	driverkube "github.com/optimiware/kodema/internal/server/drivers/kube"
)

const (
	testCacheName = "go-mod"
	testCacheKey  = "0123456789abcdef0123456789abcdef"
	testCacheSize = "2Gi"
	testProjectID = id.ProjectID("proj-alpha")
)

func testCacheClaimSpec() driverkube.CacheClaimSpec {
	return driverkube.CacheClaimSpec{
		Namespace:    testNamespace,
		TenantID:     id.TenantID(testNamespace),
		ProjectID:    testProjectID,
		CacheName:    testCacheName,
		CacheKey:     testCacheKey,
		StorageClass: testStorageClass,
		Size:         testCacheSize,
		AccessMode:   testAccessMode,
	}
}

func cacheClaimNameFor(t *testing.T, key string) string {
	t.Helper()
	name, err := runtimekube.CacheClaimName(key)
	if err != nil {
		t.Fatalf("CacheClaimName(%q): %v", key, err)
	}
	return name
}

func ownedCacheClaimWire(name, key string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "PersistentVolumeClaim",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       testNamespace,
			"resourceVersion": "11",
			"labels": map[string]string{
				runtimekube.LabelTenant:   testNamespace,
				runtimekube.LabelCacheKey: key,
				runtimekube.LabelProject:  string(testProjectID),
				runtimekube.LabelCache:    testCacheName,
			},
			"finalizers": []string{"kubernetes.io/pvc-protection"},
		},
	}
}

func TestCreateCacheClaimSendsExactPVCBody(t *testing.T) {
	claimName := cacheClaimNameFor(t, testCacheKey)
	wantBody := fmt.Sprintf(`{
	  "apiVersion": "v1",
	  "kind": "PersistentVolumeClaim",
	  "metadata": {
	    "name": %q,
	    "namespace": %q,
	    "labels": {
	      "kodema.dev/tenant": %q,
	      "kodema.dev/cache-key": %q,
	      "kodema.dev/project": %q,
	      "kodema.dev/cache": %q
	    }
	  },
	  "spec": {
	    "accessModes": ["ReadWriteOnce"],
	    "resources": {"requests": {"storage": "2Gi"}},
	    "storageClassName": "standard",
	    "volumeMode": "Filesystem"
	  }
	}`, claimName, testNamespace, testNamespace, testCacheKey, string(testProjectID), testCacheName)

	api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != claimsPath() {
			t.Errorf("unexpected request %s %s; want POST %s", r.Method, r.URL.Path, claimsPath())
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("Content-Type = %q; want application/json", contentType)
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"apiVersion": "v1",
			"kind":       "PersistentVolumeClaim",
			"metadata": map[string]any{
				"name":            claimName,
				"namespace":       testNamespace,
				"uid":             "uid-cache-1",
				"resourceVersion": "11",
				"labels": map[string]string{
					runtimekube.LabelTenant:   testNamespace,
					runtimekube.LabelCacheKey: testCacheKey,
					runtimekube.LabelProject:  string(testProjectID),
					runtimekube.LabelCache:    testCacheName,
				},
				"annotations": map[string]string{
					"volume.kubernetes.io/storage-provisioner": "rancher.io/local-path",
				},
				"finalizers": []string{"kubernetes.io/pvc-protection"},
			},
		})
	})
	cluster := newClusterFor(t, api)

	claim, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
	if err != nil {
		t.Fatalf("CreateCacheClaim: %v", err)
	}
	if claim.Name != claimName || claim.Namespace != testNamespace {
		t.Fatalf("claim identity = %s/%s; want %s/%s", claim.Namespace, claim.Name, testNamespace, claimName)
	}
	if claim.Labels[runtimekube.LabelTenant] != testNamespace ||
		claim.Labels[runtimekube.LabelCacheKey] != testCacheKey {
		t.Fatalf("claim labels = %v; want tenant and cache-key labels", claim.Labels)
	}
	if claim.ResourceVersion != "11" {
		t.Fatalf("claim resourceVersion = %q; want 11", claim.ResourceVersion)
	}

	call := api.lastCall(t)
	assertJSONEqual(t, call.Body, wantBody)

	// Guard forbidden workspace/execution/retention labels and annotations explicitly.
	body := decodeJSON(t, call.Body).(map[string]any)
	metadata := body["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	for _, forbidden := range []string{
		runtimekube.LabelWorkspace,
		runtimekube.LabelExecution,
		runtimekube.LabelRetained,
	} {
		if _, present := labels[forbidden]; present {
			t.Fatalf("create body carries forbidden label %q: %v", forbidden, labels)
		}
	}
	if annotations, ok := metadata["annotations"]; ok {
		t.Fatalf("create body unexpectedly carries annotations: %v", annotations)
	}
}

func TestCreateCacheClaimOmitsStorageClassNameWhenEmpty(t *testing.T) {
	claimName := cacheClaimNameFor(t, testCacheKey)
	wantBody := fmt.Sprintf(`{
	  "apiVersion": "v1",
	  "kind": "PersistentVolumeClaim",
	  "metadata": {
	    "name": %q,
	    "namespace": %q,
	    "labels": {
	      "kodema.dev/tenant": %q,
	      "kodema.dev/cache-key": %q,
	      "kodema.dev/project": %q,
	      "kodema.dev/cache": %q
	    }
	  },
	  "spec": {
	    "accessModes": ["ReadWriteOnce"],
	    "resources": {"requests": {"storage": "2Gi"}},
	    "volumeMode": "Filesystem"
	  }
	}`, claimName, testNamespace, testNamespace, testCacheKey, string(testProjectID), testCacheName)

	api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusCreated, ownedCacheClaimWire(claimName, testCacheKey))
	})
	cluster := newClusterFor(t, api)
	spec := testCacheClaimSpec()
	spec.StorageClass = ""

	if _, err := cluster.CreateCacheClaim(context.Background(), spec); err != nil {
		t.Fatalf("CreateCacheClaim with empty StorageClass: %v", err)
	}
	assertJSONEqual(t, api.lastCall(t).Body, wantBody)
}

func TestCreateCacheClaimOmitsNonKubeLabelInformationalValues(t *testing.T) {
	cases := []struct {
		name           string
		cacheName      string
		projectID      id.ProjectID
		wantCacheLabel string
		wantProjLabel  string
	}{
		{
			name:           "cache slug ending with hyphen",
			cacheName:      "go-mod-",
			projectID:      testProjectID,
			wantCacheLabel: "",
			wantProjLabel:  string(testProjectID),
		},
		{
			name:           "cache slug ending with underscore",
			cacheName:      "npm_cache_",
			projectID:      testProjectID,
			wantCacheLabel: "",
			wantProjLabel:  string(testProjectID),
		},
		{
			name:           "cache slug ending with dot",
			cacheName:      "cargo.v1.",
			projectID:      testProjectID,
			wantCacheLabel: "",
			wantProjLabel:  string(testProjectID),
		},
		{
			name:           "single-character cache slug is valid kube label",
			cacheName:      "a",
			projectID:      "",
			wantCacheLabel: "a",
			wantProjLabel:  "",
		},
		{
			name:           "project id over 63 chars omits project label",
			cacheName:      "go-mod",
			projectID:      id.ProjectID(strings.Repeat("p", 64)),
			wantCacheLabel: "go-mod",
			wantProjLabel:  "",
		},
		{
			name:           "project id ending with hyphen omits project label",
			cacheName:      "go-mod",
			projectID:      id.ProjectID("proj-"),
			wantCacheLabel: "go-mod",
			wantProjLabel:  "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claimName := cacheClaimNameFor(t, testCacheKey)
			api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, http.StatusCreated, ownedCacheClaimWire(claimName, testCacheKey))
			})
			cluster := newClusterFor(t, api)
			spec := testCacheClaimSpec()
			spec.CacheName = tc.cacheName
			spec.ProjectID = tc.projectID

			if _, err := cluster.CreateCacheClaim(context.Background(), spec); err != nil {
				t.Fatalf("CreateCacheClaim: %v", err)
			}

			body := decodeJSON(t, api.lastCall(t).Body).(map[string]any)
			labels := body["metadata"].(map[string]any)["labels"].(map[string]any)
			if labels[runtimekube.LabelTenant] != testNamespace {
				t.Fatalf("LabelTenant = %v; want %q", labels[runtimekube.LabelTenant], testNamespace)
			}
			if labels[runtimekube.LabelCacheKey] != testCacheKey {
				t.Fatalf("LabelCacheKey = %v; want %q", labels[runtimekube.LabelCacheKey], testCacheKey)
			}
			gotCache, hasCache := labels[runtimekube.LabelCache]
			if tc.wantCacheLabel == "" {
				if hasCache {
					t.Fatalf("LabelCache = %v; want omitted for slug %q", gotCache, tc.cacheName)
				}
			} else if gotCache != tc.wantCacheLabel {
				t.Fatalf("LabelCache = %v; want %q", gotCache, tc.wantCacheLabel)
			}
			gotProj, hasProj := labels[runtimekube.LabelProject]
			if tc.wantProjLabel == "" {
				if hasProj {
					t.Fatalf("LabelProject = %v; want omitted for project %q", gotProj, tc.projectID)
				}
			} else if gotProj != tc.wantProjLabel {
				t.Fatalf("LabelProject = %v; want %q", gotProj, tc.wantProjLabel)
			}
		})
	}
}

func TestCreateCacheClaimAdoptsOwnedClaimIdempotently(t *testing.T) {
	claimName := cacheClaimNameFor(t, testCacheKey)
	existing := ownedCacheClaimWire(claimName, testCacheKey)
	existing["metadata"].(map[string]any)["labels"].(map[string]string)["server.example/added"] = "yes"

	var posts atomic.Int32
	var mu sync.Mutex
	var methods []string
	api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method+" "+r.URL.Path)
		mu.Unlock()
		switch {
		case r.Method == http.MethodPost && posts.Add(1) == 1:
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("fake API server cannot lose a response")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close()
		case r.Method == http.MethodPost:
			writeJSON(w, http.StatusConflict, map[string]any{"kind": "Status", "reason": "AlreadyExists"})
		case r.Method == http.MethodGet && r.URL.Path == claimPath(claimName):
			writeJSON(w, http.StatusOK, existing)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotImplemented)
		}
	})
	cluster := newClusterFor(t, api)

	_, lostErr := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
	assertAmbiguous(t, lostErr)

	claim, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
	if err != nil {
		t.Fatalf("idempotent retry after lost response: %v", err)
	}
	if claim.Name != claimName || claim.Namespace != testNamespace {
		t.Fatalf("adopted claim = %s/%s; want %s/%s", claim.Namespace, claim.Name, testNamespace, claimName)
	}
	if claim.Labels[runtimekube.LabelTenant] != testNamespace ||
		claim.Labels[runtimekube.LabelCacheKey] != testCacheKey {
		t.Fatalf("adopted claim labels = %v; want matching ownership labels", claim.Labels)
	}

	mu.Lock()
	got := append([]string(nil), methods...)
	mu.Unlock()
	want := []string{
		"POST " + claimsPath(),
		"POST " + claimsPath(),
		"GET " + claimPath(claimName),
	}
	if len(got) != len(want) {
		t.Fatalf("request sequence = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("request sequence = %v; want %v", got, want)
		}
	}
}

func TestCreateCacheClaimRefusesForeignOrWorkspaceClaimOnConflict(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
	}{
		{
			name: "different tenant",
			labels: map[string]string{
				runtimekube.LabelTenant:   "tenant-other",
				runtimekube.LabelCacheKey: testCacheKey,
			},
		},
		{
			name: "different cache key",
			labels: map[string]string{
				runtimekube.LabelTenant:   testNamespace,
				runtimekube.LabelCacheKey: "fedcba9876543210fedcba9876543210",
			},
		},
		{
			name:   "unlabeled",
			labels: nil,
		},
		{
			name: "missing cache key label",
			labels: map[string]string{
				runtimekube.LabelTenant: testNamespace,
			},
		},
		{
			name: "carries workspace label",
			labels: map[string]string{
				runtimekube.LabelTenant:    testNamespace,
				runtimekube.LabelCacheKey:  testCacheKey,
				runtimekube.LabelWorkspace: "workspace-1",
			},
		},
		{
			name: "carries empty workspace label",
			labels: map[string]string{
				runtimekube.LabelTenant:    testNamespace,
				runtimekube.LabelCacheKey:  testCacheKey,
				runtimekube.LabelWorkspace: "",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claimName := cacheClaimNameFor(t, testCacheKey)
			api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					writeJSON(w, http.StatusConflict, map[string]any{"kind": "Status", "reason": "AlreadyExists"})
				case http.MethodGet:
					writeJSON(w, http.StatusOK, map[string]any{
						"metadata": map[string]any{
							"name":            claimName,
							"namespace":       testNamespace,
							"resourceVersion": "3",
							"labels":          tc.labels,
						},
					})
				default:
					t.Errorf("unexpected request %s %s; a foreign claim must never be overwritten", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotImplemented)
				}
			})
			cluster := newClusterFor(t, api)

			_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
			if err == nil {
				t.Fatal("CreateCacheClaim adopted a foreign/workspace claim")
			}
			assertFaultClass(t, err, fault.Forbidden)
			if errors.Is(err, runtimekube.ErrAlreadyExists) {
				t.Fatalf("error %v leaked ErrAlreadyExists", err)
			}
			calls := api.snapshot()
			if len(calls) != 2 || calls[0].Method != http.MethodPost || calls[1].Method != http.MethodGet {
				t.Fatalf("calls = %s; want exactly POST then GET", formatCalls(calls))
			}
		})
	}
}

func TestCreateCacheClaimRefusesTerminatingClaimOnConflict(t *testing.T) {
	claimName := cacheClaimNameFor(t, testCacheKey)
	api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			writeJSON(w, http.StatusConflict, map[string]any{"kind": "Status", "reason": "AlreadyExists"})
		case http.MethodGet:
			writeJSON(w, http.StatusOK, map[string]any{
				"metadata": map[string]any{
					"name":              claimName,
					"namespace":         testNamespace,
					"resourceVersion":   "9",
					"deletionTimestamp": "2026-11-22T10:01:00Z",
					"labels": map[string]string{
						runtimekube.LabelTenant:   testNamespace,
						runtimekube.LabelCacheKey: testCacheKey,
					},
					"finalizers": []string{"kubernetes.io/pvc-protection"},
				},
			})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotImplemented)
		}
	})
	cluster := newClusterFor(t, api)

	claim, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
	if err == nil {
		t.Fatalf("CreateCacheClaim succeeded for terminating claim: %+v", claim)
	}
	assertFaultClass(t, err, fault.VersionConflict)
	if !errors.Is(err, runtimekube.ErrConflict) {
		t.Fatalf("error %v does not wrap ErrConflict", err)
	}
}

func TestCreateCacheClaimRefusesInvalidSpecBeforeSending(t *testing.T) {
	base := testCacheClaimSpec()
	cases := []struct {
		name   string
		mutate func(*driverkube.CacheClaimSpec)
	}{
		{name: "empty namespace", mutate: func(s *driverkube.CacheClaimSpec) { s.Namespace = "" }},
		{name: "empty tenant", mutate: func(s *driverkube.CacheClaimSpec) { s.TenantID = "" }},
		{name: "invalid tenant", mutate: func(s *driverkube.CacheClaimSpec) { s.TenantID = "tenant/acme" }},
		{name: "invalid project id", mutate: func(s *driverkube.CacheClaimSpec) { s.ProjectID = "proj/bad" }},
		{name: "empty cache name", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheName = "" }},
		{name: "uppercase cache name", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheName = "GoMod" }},
		{name: "traversal cache name", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheName = "a..b" }},
		{name: "empty cache key", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheKey = "" }},
		{name: "short cache key", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheKey = "0123456789abcde" }},
		{name: "uppercase cache key", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheKey = strings.Repeat("A", 32) }},
		{name: "non-hex cache key", mutate: func(s *driverkube.CacheClaimSpec) { s.CacheKey = strings.Repeat("g", 32) }},
		{name: "blank storage class", mutate: func(s *driverkube.CacheClaimSpec) { s.StorageClass = "   " }},
		{name: "empty size", mutate: func(s *driverkube.CacheClaimSpec) { s.Size = "" }},
		{name: "empty access mode", mutate: func(s *driverkube.CacheClaimSpec) { s.AccessMode = "" }},
		{name: "unknown access mode", mutate: func(s *driverkube.CacheClaimSpec) { s.AccessMode = "ReadWriteTwice" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("invalid spec reached API server: %s %s", r.Method, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			})
			cluster := newClusterFor(t, api)
			spec := base
			tc.mutate(&spec)
			_, err := cluster.CreateCacheClaim(context.Background(), spec)
			assertFaultClass(t, err, fault.InvalidInput)
			if calls := api.callCount(); calls != 0 {
				t.Fatalf("%d request(s) sent for invalid spec; want none", calls)
			}
		})
	}
}

func TestCreateCacheClaimConflictWithUnreadableClaimIsAmbiguous(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{name: "claim disappeared", status: http.StatusNotFound},
		{name: "claim read failed", status: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					writeJSON(w, http.StatusConflict, map[string]any{"kind": "Status", "reason": "AlreadyExists"})
				case http.MethodGet:
					writeJSON(w, tc.status, map[string]any{"kind": "Status"})
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotImplemented)
				}
			})
			cluster := newClusterFor(t, api)
			_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
			assertAmbiguous(t, err)
		})
	}
}

func TestCreateCacheClaimUncertainAndStatusOutcomes(t *testing.T) {
	claimName := cacheClaimNameFor(t, testCacheKey)

	t.Run("cancelled before send", func(t *testing.T) {
		api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
			t.Errorf("cancelled call reached API server: %s %s", r.Method, r.URL.Path)
		})
		cluster := newClusterFor(t, api)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := cluster.CreateCacheClaim(ctx, testCacheClaimSpec())
		assertAmbiguous(t, err)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error %v does not wrap context.Canceled", err)
		}
		if api.callCount() != 0 {
			t.Fatalf("%d request(s) sent after cancel; want 0", api.callCount())
		}
	})

	t.Run("4xx rejected as DependencyUnavailable", func(t *testing.T) {
		api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusForbidden, map[string]any{"kind": "Status", "message": "quota exceeded for " + testToken})
		})
		cluster := newClusterFor(t, api)
		_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
		assertFaultClass(t, err, fault.DependencyUnavailable)
		assertNoCredential(t, err, testToken)
	})

	t.Run("server failure with token in body", func(t *testing.T) {
		api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"kind":    "Status",
				"message": "internal error for " + testToken,
			})
		})
		cluster := newClusterFor(t, api)
		_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
		assertAmbiguous(t, err)
		assertNoCredential(t, err, testToken)
	})

	t.Run("mismatched returned name", func(t *testing.T) {
		api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusCreated, ownedCacheClaimWire("kodema-cache-someone-else", testCacheKey))
		})
		cluster := newClusterFor(t, api)
		_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
		assertAmbiguous(t, err)
	})

	t.Run("returned claim carries workspace label", func(t *testing.T) {
		api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
			resp := ownedCacheClaimWire(claimName, testCacheKey)
			resp["metadata"].(map[string]any)["labels"].(map[string]string)[runtimekube.LabelWorkspace] = "ws-1"
			writeJSON(w, http.StatusCreated, resp)
		})
		cluster := newClusterFor(t, api)
		_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
		assertAmbiguous(t, err)
	})

	t.Run("timeout", func(t *testing.T) {
		api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		})
		cluster := newClusterWith(t, api.kubeconfig(t, nil, nil), func(config *driverkube.ClusterConfig) {
			config.Timeout = 150 * time.Millisecond
		})
		_, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
		assertAmbiguous(t, err)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error %v does not carry deadline", err)
		}
	})
}

func TestCreateCacheClaimConcurrentConvergesOnOneClaim(t *testing.T) {
	claimName := cacheClaimNameFor(t, testCacheKey)
	existing := ownedCacheClaimWire(claimName, testCacheKey)
	var posts atomic.Int32
	api := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			if posts.Add(1) == 1 {
				writeJSON(w, http.StatusCreated, existing)
				return
			}
			writeJSON(w, http.StatusConflict, map[string]any{"kind": "Status", "reason": "AlreadyExists"})
		case http.MethodGet:
			writeJSON(w, http.StatusOK, existing)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotImplemented)
		}
	})
	cluster := newClusterFor(t, api)

	type outcome struct {
		claim runtimekube.Claim
		err   error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-start
			claim, err := cluster.CreateCacheClaim(context.Background(), testCacheClaimSpec())
			results <- outcome{claim: claim, err: err}
		}()
	}
	close(start)
	for range 2 {
		res := <-results
		if res.err != nil {
			t.Fatalf("concurrent CreateCacheClaim: %v", res.err)
		}
		if res.claim.Name != claimName || res.claim.Namespace != testNamespace {
			t.Fatalf("concurrent CreateCacheClaim = %s/%s; want %s/%s",
				res.claim.Namespace, res.claim.Name, testNamespace, claimName)
		}
	}
	if got := posts.Load(); got != 2 {
		t.Fatalf("POST count = %d; want 2", got)
	}
}
