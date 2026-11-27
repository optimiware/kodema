package opencode

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
)

type staticTestCred string

func (s staticTestCred) Credential(context.Context) (client.Credential, error) {
	return client.Credential{
		Kind:      client.KindSession,
		Token:     string(s),
		ExpiresAt: time.Now().Add(time.Hour),
	}, nil
}

func newTestProxy(t *testing.T, kodemaURL, localKey string, rt http.RoundTripper) (*OpenCodeLLMProxy, string) {
	t.Helper()
	p, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:    kodemaURL,
		Credentials:  staticTestCred("kodsvc.opencode-1.secretmac"),
		LocalAPIKey:  localKey,
		RoundTripper: rt,
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	baseURL, err := p.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = p.Close(context.Background())
	})
	return p, baseURL
}

func TestOpenCodeLLMProxyLoopbackAndNoProxyTransport(t *testing.T) {
	p, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:   "http://127.0.0.1:8080",
		Credentials: staticTestCred("kodsvc.opencode-1.tok"),
	})
	if err != nil {
		t.Fatalf("NewOpenCodeLLMProxy: %v", err)
	}
	if p.LocalAPIKey() == "" {
		t.Fatal("expected auto-generated LocalAPIKey when omitted")
	}
	tr, ok := p.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("default transport type = %T; want *http.Transport", p.client.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("default proxy transport must set Proxy = nil to disable ambient proxy inheritance")
	}

	// Non-loopback listen addresses must be refused.
	for _, badAddr := range []string{"0.0.0.0:0", "192.0.2.1:0", "[::]:0", "example.com:8080"} {
		if _, err := p.Start(badAddr); err == nil {
			t.Fatalf("Start(%q) unexpectedly succeeded; want loopback error", badAddr)
		}
	}

	// Ambient HTTP_PROXY must never hijack proxy outbound calls.
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	p2, proxyURL := newTestProxy(t, upstream.URL, "", nil)
	unreg := p2.RegisterTurn(context.Background(), "exec-1", "act-1")
	defer unreg()

	noProxyClient := &http.Client{Transport: defaultNoProxyTransport()}
	req, _ := http.NewRequest(http.MethodPost, proxyURL+"/llm/anthropic/v1/messages", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+p2.LocalAPIKey())
	resp, err := noProxyClient.Do(req)
	if err != nil {
		t.Fatalf("request failed under ambient HTTP_PROXY: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}
}

func TestOpenCodeLLMProxyLocalAuthAndHeaderStripping(t *testing.T) {
	var (
		upstreamHits atomic.Int32
		seenHeaders  http.Header
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		seenHeaders = r.Header.Clone()
		w.Header().Set("Connection", "X-Resp-Hop")
		w.Header().Set("X-Resp-Hop", "drop-me")
		w.Header().Set("X-Safe-Resp", "keep-me")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	p, proxyURL := newTestProxy(t, upstream.URL, "local-secret-token-123", upstream.Client().Transport)
	unreg := p.RegisterTurn(context.Background(), "exec-auth-1", "act-auth-1")
	defer unreg()

	clientHTTP := &http.Client{Transport: defaultNoProxyTransport()}

	// Unauthenticated / wrong / malformed / conflicting auth -> 401.
	badAuthCases := []struct {
		name    string
		headers map[string]string
	}{
		{name: "missing auth"},
		{name: "wrong bearer", headers: map[string]string{"Authorization": "Bearer wrong-key"}},
		{name: "wrong x-api-key", headers: map[string]string{"X-Api-Key": "wrong-key"}},
		{name: "non-bearer scheme", headers: map[string]string{"Authorization": "Basic dXNlcjpwYXNz"}},
		{name: "conflicting headers", headers: map[string]string{"Authorization": "Bearer local-secret-token-123", "X-Api-Key": "other"}},
	}
	for _, tc := range badAuthCases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, proxyURL+"/llm/anthropic/v1/messages", strings.NewReader(`{}`))
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			resp, err := clientHTTP.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d; want 401", resp.StatusCode)
			}
		})
	}
	if upstreamHits.Load() != 0 {
		t.Fatalf("upstream received %d hits on unauthorized requests; want 0", upstreamHits.Load())
	}

	// Valid x-api-key succeeds, strips local key + sensitive + hop-by-hop headers.
	req, _ := http.NewRequest(http.MethodPost, proxyURL+"/llm/anthropic/v1/messages", strings.NewReader(`{}`))
	req.Header.Set("X-Api-Key", "local-secret-token-123")
	req.Header.Set("Cookie", "session=secret-cookie")
	req.Header.Set("Proxy-Authorization", "Basic leak")
	req.Header.Set("Proxy-Authenticate", "Basic realm=x")
	req.Header.Set("Te", "trailers")
	req.Header.Set("Trailer", "X-Checksum")
	req.Header.Set("Connection", "X-Custom-Hop, X-Second-Hop")
	req.Header.Set("X-Custom-Hop", "must-strip")
	req.Header.Set("X-Second-Hop", "must-strip")
	req.Header.Set("X-Keep-Me", "preserved")

	resp, err := clientHTTP.Do(req)
	if err != nil {
		t.Fatalf("Do valid: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", resp.StatusCode)
	}
	if seenHeaders.Get("Authorization") != "Bearer kodsvc.opencode-1.secretmac" {
		t.Fatalf("upstream Authorization = %q; want live kodsvc bearer", seenHeaders.Get("Authorization"))
	}
	if seenHeaders.Get("X-Kodema-Execution") != "exec-auth-1" || seenHeaders.Get("X-Kodema-Action") != "act-auth-1" {
		t.Fatalf("upstream attribution = (%q, %q); want (exec-auth-1, act-auth-1)",
			seenHeaders.Get("X-Kodema-Execution"), seenHeaders.Get("X-Kodema-Action"))
	}
	if seenHeaders.Get("X-Keep-Me") != "preserved" {
		t.Fatalf("X-Keep-Me = %q; want preserved", seenHeaders.Get("X-Keep-Me"))
	}
	for _, forbidden := range []string{
		"X-Api-Key", "Cookie", "Proxy-Authorization", "Proxy-Authenticate",
		"Te", "Trailer", "X-Custom-Hop", "X-Second-Hop",
	} {
		if got := seenHeaders.Get(forbidden); got != "" {
			t.Fatalf("sensitive/hop-by-hop header %s leaked upstream: %q", forbidden, got)
		}
	}
	if resp.Header.Get("X-Resp-Hop") != "" {
		t.Fatalf("response Connection-listed header X-Resp-Hop was not stripped: %q", resp.Header.Get("X-Resp-Hop"))
	}
	if resp.Header.Get("X-Safe-Resp") != "keep-me" {
		t.Fatalf("response X-Safe-Resp = %q; want keep-me", resp.Header.Get("X-Safe-Resp"))
	}
}

func TestOpenCodeLLMProxyPathTraversalRejection(t *testing.T) {
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p, _ := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
		KodemaURL:    upstream.URL,
		Credentials:  staticTestCred("kodsvc.opencode-1.tok"),
		LocalAPIKey:  "local-key",
		RoundTripper: upstream.Client().Transport,
	})
	unreg := p.RegisterTurn(context.Background(), "exec-1", "act-1")
	defer unreg()

	badPaths := []struct {
		name     string
		path     string
		rawPath  string
		rawQuery string
		fragment string
	}{
		{name: "dotdot after llm", path: "/llm/../v1/messages"},
		{name: "dotdot in upstream", path: "/llm/anthropic/v1/../../admin"},
		{name: "dot segment", path: "/llm/anthropic/./v1/messages"},
		{name: "empty segment", path: "/llm/anthropic//v1/messages"},
		{name: "backslash", path: `/llm/anthropic/v1\messages`},
		{name: "at sign", path: "/llm/anthropic/v1@evil/messages"},
		{name: "encoded slash", path: "/llm/anthropic/v1/messages", rawPath: "/llm/anthropic/v1%2fmessages"},
		{name: "encoded uppercase slash", path: "/llm/anthropic/v1/messages", rawPath: "/llm/anthropic/v1%2Fmessages"},
		{name: "encoded backslash", path: "/llm/anthropic/v1/messages", rawPath: "/llm/anthropic/v1%5cmessages"},
		{name: "encoded nul", path: "/llm/anthropic/v1/messages", rawPath: "/llm/anthropic/v1%00messages"},
		{name: "turn dotdot traversal", path: "/turn/exec-1/act-1/llm/../v1/messages"},
		{name: "control char in path", path: "/llm/anthropic/v1/\x07messages"},
		{name: "control char in query", path: "/llm/anthropic/v1/messages", rawQuery: "a=b\r\nX-Injected:1"},
		{name: "fragment", path: "/llm/anthropic/v1/messages", fragment: "evil"},
		{name: "missing upstream path", path: "/llm/anthropic"},
		{name: "non-llm prefix", path: "/v1/messages"},
	}
	for _, tc := range badPaths {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{
				Method: http.MethodPost,
				URL: &url.URL{
					Path:     tc.path,
					RawPath:  tc.rawPath,
					RawQuery: tc.rawQuery,
					Fragment: tc.fragment,
				},
				Header: http.Header{"Authorization": []string{"Bearer local-key"}},
				Body:   io.NopCloser(strings.NewReader(`{}`)),
			}
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d; want 400 (body=%s)", rec.Code, rec.Body.String())
			}
		})
	}
	if upstreamHits.Load() != 0 {
		t.Fatalf("upstream received %d hits on rejected paths; want 0", upstreamHits.Load())
	}
}

func TestOpenCodeLLMProxyFailClosedUnderConcurrentTurns(t *testing.T) {
	var (
		upstreamHits atomic.Int32
		lastExec     atomic.Value
		lastAction   atomic.Value
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		lastExec.Store(r.Header.Get("X-Kodema-Execution"))
		lastAction.Store(r.Header.Get("X-Kodema-Action"))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	p, proxyURL := newTestProxy(t, upstream.URL, "secret-key", upstream.Client().Transport)
	clientHTTP := &http.Client{Transport: defaultNoProxyTransport()}

	doReq := func(path string, headers map[string]string) int {
		req, _ := http.NewRequest(http.MethodPost, proxyURL+path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer secret-key")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := clientHTTP.Do(req)
		if err != nil {
			t.Fatalf("Do(%s): %v", path, err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	// 0 turns -> 403 Forbidden.
	if got := doReq("/llm/anthropic/v1/messages", nil); got != http.StatusForbidden {
		t.Fatalf("0 active turns status = %d; want 403", got)
	}

	// 1 active turn -> implicit /llm/* allowed, partial header rejected.
	unreg1 := p.RegisterTurn(context.Background(), "exec-1", "act-1")
	if got := doReq("/llm/anthropic/v1/messages", map[string]string{"X-Kodema-Execution": "exec-1"}); got != http.StatusForbidden {
		t.Fatalf("partial header status = %d; want 403", got)
	}
	if got := doReq("/llm/anthropic/v1/messages", nil); got != http.StatusOK {
		t.Fatalf("1 active turn implicit status = %d; want 200", got)
	}

	// 2 active turns -> unattributed /llm/* MUST fail closed with 409 Conflict!
	unreg2 := p.RegisterTurn(context.Background(), "exec-2", "act-2")
	hitsBefore := upstreamHits.Load()
	if got := doReq("/llm/anthropic/v1/messages", nil); got != http.StatusConflict {
		t.Fatalf("2 active turns unattributed status = %d; want 409", got)
	}
	if upstreamHits.Load() != hitsBefore {
		t.Fatal("unattributed request under 2 active turns reached upstream")
	}

	// Explicit /turn/{exec}/{action}/llm/* and explicit headers still work accurately.
	if got := doReq("/turn/exec-2/act-2/llm/anthropic/v1/messages", nil); got != http.StatusOK {
		t.Fatalf("explicit turn-scoped path status = %d; want 200", got)
	}
	if lastExec.Load() != "exec-2" || lastAction.Load() != "act-2" {
		t.Fatalf("attributed (%v, %v); want (exec-2, act-2)", lastExec.Load(), lastAction.Load())
	}
	if got := doReq("/llm/anthropic/v1/messages", map[string]string{
		"X-Kodema-Execution": "exec-1",
		"X-Kodema-Action":    "act-1",
	}); got != http.StatusOK {
		t.Fatalf("explicit attribution headers status = %d; want 200", got)
	}
	if lastExec.Load() != "exec-1" || lastAction.Load() != "act-1" {
		t.Fatalf("attributed (%v, %v); want (exec-1, act-1)", lastExec.Load(), lastAction.Load())
	}

	// After unregistering turn 1, 1 turn remains -> implicit /llm/* works for turn 2.
	unreg1()
	if got := doReq("/llm/anthropic/v1/messages", nil); got != http.StatusOK {
		t.Fatalf("after unreg1 status = %d; want 200", got)
	}
	if lastExec.Load() != "exec-2" || lastAction.Load() != "act-2" {
		t.Fatalf("attributed (%v, %v); want (exec-2, act-2)", lastExec.Load(), lastAction.Load())
	}
	unreg2()
	if got := doReq("/llm/anthropic/v1/messages", nil); got != http.StatusForbidden {
		t.Fatalf("after unreg2 status = %d; want 403", got)
	}
}

func TestOpenCodeLLMProxyMidStreamAbort(t *testing.T) {
	firstChunkSent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: chunk-1\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		close(firstChunkSent)
		<-r.Context().Done()
	}))
	defer upstream.Close()

	p, proxyURL := newTestProxy(t, upstream.URL, "stream-key", upstream.Client().Transport)
	turnCtx, cancelTurn := context.WithCancel(context.Background())
	unreg := p.RegisterTurn(turnCtx, "exec-stream", "act-stream")
	defer unreg()

	clientHTTP := &http.Client{Transport: defaultNoProxyTransport()}
	req, _ := http.NewRequest(http.MethodPost, proxyURL+"/llm/anthropic/v1/messages", strings.NewReader(`{"stream":true}`))
	req.Header.Set("Authorization", "Bearer stream-key")
	resp, err := clientHTTP.Do(req)
	if err != nil {
		t.Fatalf("Do stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	<-firstChunkSent
	buf := make([]byte, 64)
	n, err := resp.Body.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "chunk-1") {
		t.Fatalf("first read = (%q, %v); want chunk-1", string(buf[:n]), err)
	}

	// Cancel turn mid-stream -> proxy must abort the response (not clean EOF).
	cancelTurn()
	_, readErr := io.ReadAll(resp.Body)
	if readErr == nil {
		t.Fatal("expected aborted mid-stream read error after turn cancel, got clean EOF")
	}
}

func TestOpenCodeLLMProxyStartUsesConfiguredListenAddr(t *testing.T) {
	t.Run("Start empty uses config ListenAddr", func(t *testing.T) {
		probe, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("probe Listen: %v", err)
		}
		wantAddr := probe.Addr().String()
		_ = probe.Close()

		proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
			ListenAddr:  "  " + wantAddr + "  ",
			KodemaURL:   "http://127.0.0.1:18080",
			Credentials: staticTestCred("kodsvc.tok"),
		})
		if err != nil {
			t.Fatalf("NewOpenCodeLLMProxy: %v", err)
		}
		gotURL, err := proxy.Start("")
		if err != nil {
			t.Fatalf("Start(\"\"): %v", err)
		}
		defer func() { _ = proxy.Close(context.Background()) }()
		if gotURL != "http://"+wantAddr {
			t.Fatalf("Start(\"\") URL = %q; want %q", gotURL, "http://"+wantAddr)
		}
	})

	t.Run("Start empty rejects non-loopback config ListenAddr", func(t *testing.T) {
		proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
			ListenAddr:  "0.0.0.0:0",
			KodemaURL:   "http://127.0.0.1:18080",
			Credentials: staticTestCred("kodsvc.tok"),
		})
		if err != nil {
			t.Fatalf("NewOpenCodeLLMProxy: %v", err)
		}
		if _, err := proxy.Start(""); err == nil {
			t.Fatal("Start(\"\") accepted non-loopback configured ListenAddr")
		}
	})

	t.Run("explicit Start arg overrides config ListenAddr", func(t *testing.T) {
		proxy, err := NewOpenCodeLLMProxy(OpenCodeLLMProxyConfig{
			ListenAddr:  "0.0.0.0:0",
			KodemaURL:   "http://127.0.0.1:18080",
			Credentials: staticTestCred("kodsvc.tok"),
		})
		if err != nil {
			t.Fatalf("NewOpenCodeLLMProxy: %v", err)
		}
		gotURL, err := proxy.Start("127.0.0.1:0")
		if err != nil {
			t.Fatalf("Start(\"127.0.0.1:0\"): %v", err)
		}
		defer func() { _ = proxy.Close(context.Background()) }()
		if !strings.HasPrefix(gotURL, "http://127.0.0.1:") {
			t.Fatalf("unexpected bound URL %q", gotURL)
		}
	})
}

func TestOpenCodeLLMProxyAuthenticateLocalCallerDiagnostics(t *testing.T) {
	const (
		configuredKey = "ocproxy.configured-secret-9999"
		wrongKey      = "ocproxy.wrong-presented-1111"
		otherKey      = "ocproxy.conflicting-db-2222"
	)

	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	p, proxyURL := newTestProxy(t, upstream.URL, configuredKey, upstream.Client().Transport)
	unreg := p.RegisterTurn(context.Background(), "exec-diag-1", "act-diag-1")
	defer unreg()

	clientHTTP := &http.Client{Transport: defaultNoProxyTransport()}

	cases := []struct {
		name        string
		headers     http.Header
		wantSubstr  string
		wantSecrets []string
	}{
		{
			name:        "missing auth",
			headers:     http.Header{},
			wantSubstr:  "missing local LLM proxy API key",
			wantSecrets: []string{configuredKey},
		},
		{
			name:        "non-bearer authorization scheme",
			headers:     http.Header{"Authorization": []string{"Basic dXNlcjpzZWNyZXQ="}},
			wantSubstr:  "expected Bearer token",
			wantSecrets: []string{configuredKey, "dXNlcjpzZWNyZXQ="},
		},
		{
			name:        "empty bearer token",
			headers:     http.Header{"Authorization": []string{"Bearer   "}},
			wantSubstr:  "expected Bearer token",
			wantSecrets: []string{configuredKey},
		},
		{
			name:        "conflicting Authorization and X-Api-Key headers",
			headers:     http.Header{"Authorization": []string{"Bearer " + configuredKey}, "X-Api-Key": []string{otherKey}},
			wantSubstr:  "conflicting Authorization and X-Api-Key",
			wantSecrets: []string{configuredKey, otherKey},
		},
		{
			name:        "conflicting multiple Authorization headers",
			headers:     http.Header{"Authorization": []string{"Bearer " + configuredKey, "Bearer " + otherKey}},
			wantSubstr:  "conflicting Authorization headers",
			wantSecrets: []string{configuredKey, otherKey},
		},
		{
			name:        "conflicting multiple X-Api-Key headers",
			headers:     http.Header{"X-Api-Key": []string{configuredKey, otherKey}},
			wantSubstr:  "conflicting X-Api-Key headers",
			wantSecrets: []string{configuredKey, otherKey},
		},
		{
			name:        "mismatched bearer key",
			headers:     http.Header{"Authorization": []string{"Bearer " + wrongKey}},
			wantSubstr:  "mismatched local LLM proxy API key",
			wantSecrets: []string{configuredKey, wrongKey},
		},
		{
			name:        "mismatched x-api-key",
			headers:     http.Header{"X-Api-Key": []string{wrongKey}},
			wantSubstr:  "mismatched local LLM proxy API key",
			wantSecrets: []string{configuredKey, wrongKey},
		},
		{
			name:        "matching wrong Authorization and X-Api-Key",
			headers:     http.Header{"Authorization": []string{"Bearer " + wrongKey}, "X-Api-Key": []string{wrongKey}},
			wantSubstr:  "mismatched local LLM proxy API key",
			wantSecrets: []string{configuredKey, wrongKey},
		},
	}

	seenByCategory := make(map[string]string)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, proxyURL+"/llm/openai/v1/chat/completions", strings.NewReader(`{}`))
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			for k, vals := range tc.headers {
				for _, v := range vals {
					req.Header.Add(k, v)
				}
			}
			resp, err := clientHTTP.Do(req)
			if err != nil {
				t.Fatalf("Do: %v", err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("status = %d; want 401 (body=%s)", resp.StatusCode, string(body))
			}
			var apiErr api.Error
			if err := json.Unmarshal(body, &apiErr); err != nil {
				t.Fatalf("decode error JSON: %v (body=%s)", err, string(body))
			}
			if apiErr.Code != api.CodeUnauthenticated {
				t.Fatalf("code = %q; want %q", apiErr.Code, api.CodeUnauthenticated)
			}
			if !strings.Contains(apiErr.Message, tc.wantSubstr) {
				t.Fatalf("message = %q; want substring %q", apiErr.Message, tc.wantSubstr)
			}
			for _, secret := range tc.wantSecrets {
				if strings.Contains(string(body), secret) {
					t.Fatalf("401 response leaked secret %q in body: %s", secret, string(body))
				}
			}
			seenByCategory[tc.name] = apiErr.Message
		})
	}

	// Verify the three primary diagnostic classes are mutually distinct.
	missingMsg := seenByCategory["missing auth"]
	conflictMsg := seenByCategory["conflicting Authorization and X-Api-Key headers"]
	mismatchMsg := seenByCategory["mismatched bearer key"]
	if missingMsg == conflictMsg || missingMsg == mismatchMsg || conflictMsg == mismatchMsg {
		t.Fatalf("expected distinct 401 diagnostics, got missing=%q conflict=%q mismatch=%q", missingMsg, conflictMsg, mismatchMsg)
	}
	if upstreamHits.Load() != 0 {
		t.Fatalf("upstream received %d hits on unauthorized calls; want 0", upstreamHits.Load())
	}

	// Matching Authorization + X-Api-Key equal to configuredKey must succeed.
	req, _ := http.NewRequest(http.MethodPost, proxyURL+"/llm/openai/v1/chat/completions", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+configuredKey)
	req.Header.Set("X-Api-Key", configuredKey)
	resp, err := clientHTTP.Do(req)
	if err != nil {
		t.Fatalf("Do matching both headers: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("matching Authorization + X-Api-Key status = %d; want 200", resp.StatusCode)
	}
}

func TestOpenCodeLLMProxySessionHeaderRoutingStrippingAndFailClosed(t *testing.T) {
	var (
		mu          sync.Mutex
		seenHits    []http.Header
		upstreamCnt atomic.Int32
	)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCnt.Add(1)
		mu.Lock()
		seenHits = append(seenHits, r.Header.Clone())
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	p, proxyURL := newTestProxy(t, upstream.URL, "session-routing-key", upstream.Client().Transport)
	clientHTTP := &http.Client{Transport: defaultNoProxyTransport()}

	doReqPath := func(path string, headers http.Header) (int, string) {
		req, err := http.NewRequest(http.MethodPost, proxyURL+path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatalf("NewRequest(%s): %v", path, err)
		}
		req.Header.Set("Authorization", "Bearer session-routing-key")
		for k, vals := range headers {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
		resp, err := clientHTTP.Do(req)
		if err != nil {
			t.Fatalf("Do(%s): %v", path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, string(body)
	}
	doReq := func(headers http.Header) (int, string) {
		return doReqPath("/llm/openai/v1/chat/completions", headers)
	}

	// BindSession before RegisterTurn is a no-op; session must fail closed (403).
	noopUnbind := p.BindSession("ses-unregistered", "exec-none", "act-none")
	defer noopUnbind()
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-unregistered"}}); status != http.StatusForbidden {
		t.Fatalf("unregistered session status = %d; want 403 (body=%s)", status, body)
	}

	// Register turn 1 and bind ses-1.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	unreg1 := p.RegisterTurn(ctx1, "exec-1", "act-1")
	unbind1 := p.BindSession("ses-1", "exec-1", "act-1")

	// Even with only 1 active turn, an unknown X-OpenCode-Session MUST fail closed with 403 (never fall back to implicit attribution).
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-unknown"}}); status != http.StatusForbidden {
		t.Fatalf("unknown X-OpenCode-Session with 1 active turn status = %d; want 403 (body=%s)", status, body)
	}
	// Empty or duplicate X-OpenCode-Session headers fail closed with 403.
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"   "}}); status != http.StatusForbidden {
		t.Fatalf("blank X-OpenCode-Session status = %d; want 403 (body=%s)", status, body)
	}
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-1", "ses-1"}}); status != http.StatusForbidden {
		t.Fatalf("duplicate X-OpenCode-Session status = %d; want 403 (body=%s)", status, body)
	}

	// Register concurrent turn 2 and bind ses-2.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	unreg2 := p.RegisterTurn(ctx2, "exec-2", "act-2")
	unbind2 := p.BindSession("ses-2", "exec-2", "act-2")

	// Unattributed call during 2 concurrent turns still fails closed with 409 Conflict.
	if status, body := doReq(nil); status != http.StatusConflict {
		t.Fatalf("unattributed concurrent call status = %d; want 409 (body=%s)", status, body)
	}

	// Route turn 1 and turn 2 concurrently via X-OpenCode-Session on the static /llm/ path.
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-2"}}); status != http.StatusOK {
		t.Fatalf("ses-2 status = %d; want 200 (body=%s)", status, body)
	}
	if status, body := doReq(http.Header{"x-opencode-session": []string{"ses-1"}}); status != http.StatusOK {
		t.Fatalf("ses-1 status = %d; want 200 (body=%s)", status, body)
	}

	// Consistent X-Kodema-Execution + X-Kodema-Action + X-OpenCode-Session succeeds; mismatched fails closed (403).
	if status, body := doReq(http.Header{
		"X-Kodema-Execution": []string{"exec-1"},
		"X-Kodema-Action":    []string{"act-1"},
		"X-OpenCode-Session": []string{"ses-1"},
	}); status != http.StatusOK {
		t.Fatalf("consistent headers status = %d; want 200 (body=%s)", status, body)
	}
	if status, body := doReq(http.Header{
		"X-Kodema-Execution": []string{"exec-1"},
		"X-Kodema-Action":    []string{"act-1"},
		"X-OpenCode-Session": []string{"ses-2"},
	}); status != http.StatusForbidden {
		t.Fatalf("conflicting X-OpenCode-Session vs X-Kodema-* status = %d; want 403 (body=%s)", status, body)
	}

	// Turn-scoped /turn/{exec}/{action}/llm/* path must also enforce X-OpenCode-Session
	// fail-closed when the header is present (consistent succeeds, unknown/blank/duplicate/conflicting -> 403).
	if status, body := doReqPath("/turn/exec-1/act-1/llm/openai/v1/chat/completions", http.Header{
		"X-OpenCode-Session": []string{"ses-1"},
	}); status != http.StatusOK {
		t.Fatalf("consistent /turn path + X-OpenCode-Session status = %d; want 200 (body=%s)", status, body)
	}
	for _, tc := range []struct {
		name    string
		headers http.Header
	}{
		{name: "conflicting session", headers: http.Header{"X-OpenCode-Session": []string{"ses-2"}}},
		{name: "unknown session", headers: http.Header{"X-OpenCode-Session": []string{"ses-unknown"}}},
		{name: "blank session", headers: http.Header{"X-OpenCode-Session": []string{"   "}}},
		{name: "duplicate session", headers: http.Header{"X-OpenCode-Session": []string{"ses-1", "ses-1"}}},
	} {
		if status, body := doReqPath("/turn/exec-1/act-1/llm/openai/v1/chat/completions", tc.headers); status != http.StatusForbidden {
			t.Fatalf("/turn path with %s status = %d; want 403 (body=%s)", tc.name, status, body)
		}
	}

	// Verify exact attribution and strict header stripping on every upstream hit.
	mu.Lock()
	if len(seenHits) != 4 {
		mu.Unlock()
		t.Fatalf("upstream hit count = %d; want 4", len(seenHits))
	}
	wantPairs := [][2]string{
		{"exec-2", "act-2"},
		{"exec-1", "act-1"},
		{"exec-1", "act-1"},
		{"exec-1", "act-1"},
	}
	for i, h := range seenHits {
		if h.Get("X-Kodema-Execution") != wantPairs[i][0] || h.Get("X-Kodema-Action") != wantPairs[i][1] {
			mu.Unlock()
			t.Fatalf("hit %d attribution = (%q, %q); want (%q, %q)", i, h.Get("X-Kodema-Execution"), h.Get("X-Kodema-Action"), wantPairs[i][0], wantPairs[i][1])
		}
		if got := h.Get("X-OpenCode-Session"); got != "" {
			mu.Unlock()
			t.Fatalf("hit %d leaked X-OpenCode-Session upstream: %q", i, got)
		}
	}
	mu.Unlock()

	// Cancelling turn 1 rejects ses-1 with 408 while ses-2 remains active (200).
	cancel1()
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-1"}}); status != http.StatusRequestTimeout {
		t.Fatalf("cancelled ses-1 status = %d; want 408 (body=%s)", status, body)
	}
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-2"}}); status != http.StatusOK {
		t.Fatalf("active ses-2 after turn 1 cancel status = %d; want 200 (body=%s)", status, body)
	}

	// Unbinding ses-1 fails closed with 403 (both on /llm/* and on /turn/*);
	// unregistering turn 2 also cleans up ses-2 -> 403.
	unbind1()
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-1"}}); status != http.StatusForbidden {
		t.Fatalf("unbound ses-1 status = %d; want 403 (body=%s)", status, body)
	}
	if status, body := doReqPath("/turn/exec-1/act-1/llm/openai/v1/chat/completions", http.Header{"X-OpenCode-Session": []string{"ses-1"}}); status != http.StatusForbidden {
		t.Fatalf("/turn path with unbound ses-1 status = %d; want 403 (body=%s)", status, body)
	}
	unreg1()
	unreg2()
	if status, body := doReq(http.Header{"X-OpenCode-Session": []string{"ses-2"}}); status != http.StatusForbidden {
		t.Fatalf("unregistered turn 2 ses-2 status = %d; want 403 (body=%s)", status, body)
	}
	unbind2()
}
