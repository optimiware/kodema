package opencode

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
)

func basicAuthHeader(password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte("opencode:"+password))
}

func TestHTTPOpenCodeClientConfigValidationAndBasicAuth(t *testing.T) {
	c, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: "http://127.0.0.1:4096"})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}
	tr, ok := c.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("default transport type = %T; want *http.Transport", c.client.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("default OpenCode client transport must set Proxy = nil")
	}

	pwFile := filepath.Join(t.TempDir(), "opencode.pw")
	if err := os.WriteFile(pwFile, []byte("secret-one\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Reject configuring both Password and PasswordFile.
	if _, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      "http://127.0.0.1:4096",
		Password:     "direct",
		PasswordFile: pwFile,
	}); err == nil {
		t.Fatal("expected error when both Password and PasswordFile are set")
	}

	// Reject control chars in direct Password without leaking password material.
	for _, bad := range []string{"bad\npass", "bad\rpass", "bad\x00pass"} {
		_, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
			BaseURL:  "http://127.0.0.1:4096",
			Password: bad,
		})
		if err == nil {
			t.Fatalf("expected error for password containing control char")
		}
		if strings.Contains(err.Error(), "bad") {
			t.Fatalf("error leaked password material: %v", err)
		}
	}

	// Reject missing, empty, or multiline password files.
	if _, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      "http://127.0.0.1:4096",
		PasswordFile: filepath.Join(t.TempDir(), "does-not-exist"),
	}); err == nil {
		t.Fatal("expected error for missing password file")
	}
	emptyFile := filepath.Join(t.TempDir(), "empty.pw")
	_ = os.WriteFile(emptyFile, []byte("   \n\t "), 0o600)
	if _, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      "http://127.0.0.1:4096",
		PasswordFile: emptyFile,
	}); err == nil {
		t.Fatal("expected error for empty password file")
	}
	multiFile := filepath.Join(t.TempDir(), "multi.pw")
	_ = os.WriteFile(multiFile, []byte("line1\nline2\n"), 0o600)
	if _, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      "http://127.0.0.1:4096",
		PasswordFile: multiFile,
	}); err == nil {
		t.Fatal("expected error for multi-line password file")
	}

	// Verify Basic Auth header with direct Password, PasswordFile (including fresh
	// re-read after rotation), and 401 handling when unauthorized.
	var mu sync.Mutex
	var expectedAuth string
	var gotAuths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotAuths = append(gotAuths, r.Header.Get("Authorization"))
		want := expectedAuth
		mu.Unlock()
		if r.URL.Path != "/api/info" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != want {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"_tag":"UnauthorizedError","message":"Authentication required"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"version":"2.3.2","pid":42,"urls":["http://127.0.0.1:4096"],"paths":{"tmp":"/tmp"}}`)
	}))
	defer srv.Close()

	// 1. Direct password succeeds; wrong password returns 401 error.
	mu.Lock()
	expectedAuth = basicAuthHeader("direct-pass")
	mu.Unlock()

	cliDirect, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		Password:     "direct-pass",
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient direct: %v", err)
	}
	if err := cliDirect.Health(context.Background()); err != nil {
		t.Fatalf("Health with direct password: %v", err)
	}

	cliNoAuth, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient unauth: %v", err)
	}
	if err := cliNoAuth.Health(context.Background()); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("unauthenticated Health err = %v; want 401 error", err)
	}

	// 2. PasswordFile reads fresh on every request so rotation works cleanly.
	mu.Lock()
	expectedAuth = basicAuthHeader("secret-one")
	mu.Unlock()

	cliFile, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		PasswordFile: pwFile,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient file: %v", err)
	}
	if err := cliFile.Health(context.Background()); err != nil {
		t.Fatalf("Health with password file: %v", err)
	}

	// Rotate password on disk and verify next request uses the rotated value.
	if err := os.WriteFile(pwFile, []byte("secret-two\n"), 0o600); err != nil {
		t.Fatalf("rotate password file: %v", err)
	}
	mu.Lock()
	expectedAuth = basicAuthHeader("secret-two")
	mu.Unlock()
	if err := cliFile.Health(context.Background()); err != nil {
		t.Fatalf("Health after password file rotation: %v", err)
	}
}

func TestHTTPOpenCodeClientHealthRejectsHTMLSPAAndMissingVersion(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		status      int
		body        string
		wantErr     bool
	}{
		{
			name:        "valid v2 server info",
			contentType: "application/json",
			status:      http.StatusOK,
			body:        `{"version":"2.3.2","pid":123,"urls":["http://127.0.0.1:4096"],"paths":{"tmp":"/tmp"}}`,
			wantErr:     false,
		},
		{
			name:        "html spa fallback 200 rejected",
			contentType: "text/html; charset=utf-8",
			status:      http.StatusOK,
			body:        `<!doctype html><html><body>OpenCode SPA</body></html>`,
			wantErr:     true,
		},
		{
			name:        "json missing version rejected",
			contentType: "application/json",
			status:      http.StatusOK,
			body:        `{"ok":true}`,
			wantErr:     true,
		},
		{
			name:        "unauthorized 401 rejected",
			contentType: "application/json",
			status:      http.StatusUnauthorized,
			body:        `{"_tag":"UnauthorizedError","message":"Authentication required"}`,
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
				BaseURL:      srv.URL,
				RoundTripper: srv.Client().Transport,
			})
			if err != nil {
				t.Fatalf("NewHTTPOpenCodeClient: %v", err)
			}
			err = cli.Health(context.Background())
			if gotPath != "/api/info" {
				t.Fatalf("Health requested %q; want /api/info", gotPath)
			}
			if tc.wantErr && err == nil {
				t.Fatal("Health unexpectedly succeeded; want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Health failed: %v", err)
			}
		})
	}
}

func TestHTTPOpenCodeClientCreateSessionV2Contract(t *testing.T) {
	var mu sync.Mutex
	var gotBody map[string]any
	var gotQuery string
	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/session" {
			http.NotFound(w, r)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody = nil
		_ = json.Unmarshal(raw, &gotBody)
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"id":"ses_v2_abc123","projectID":"proj-1"}}`)
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		Password:     "pw-create",
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	sess, err := cli.CreateSession(context.Background(), "/tmp/wt/repo/task-1", "kodema:task-1")
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if sess.ID != "ses_v2_abc123" {
		t.Fatalf("sess.ID = %q; want ses_v2_abc123", sess.ID)
	}

	mu.Lock()
	if gotQuery != "" {
		t.Fatalf("CreateSession sent unexpected query %q", gotQuery)
	}
	if gotAuth != basicAuthHeader("pw-create") {
		t.Fatalf("Authorization = %q; want Basic auth", gotAuth)
	}
	if gotBody["title"] != "kodema:task-1" {
		t.Fatalf("title = %v; want kodema:task-1", gotBody["title"])
	}
	loc, _ := gotBody["location"].(map[string]any)
	if loc["directory"] != "/tmp/wt/repo/task-1" {
		t.Fatalf("location = %+v; want directory /tmp/wt/repo/task-1", gotBody["location"])
	}
	mu.Unlock()

	// Empty directory and title must omit both fields.
	if _, err := cli.CreateSession(context.Background(), "", ""); err != nil {
		t.Fatalf("CreateSession empty: %v", err)
	}
	mu.Lock()
	if len(gotBody) != 0 {
		t.Fatalf("empty CreateSession body = %+v; want {}", gotBody)
	}
	mu.Unlock()
}

func TestHTTPOpenCodeClientSendMessageV2FullFlowAndPagination(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	var moveBody map[string]any
	var agentBody map[string]any
	var modelBody map[string]any
	var patchBody map[string]any
	var promptBody map[string]any
	var promptHeaders http.Header
	var listQueries []string

	const wantPW = "v2-secret"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != basicAuthHeader(wantPW) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"_tag":"UnauthorizedError","message":"Authentication required"}`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, r.Method+" "+r.URL.Path)

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/ses_1/move":
			_ = json.Unmarshal(raw, &moveBody)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/ses_1/agent":
			_ = json.Unmarshal(raw, &agentBody)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/ses_1/model":
			_ = json.Unmarshal(raw, &modelBody)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPatch && r.URL.Path == "/api/session/ses_1":
			_ = json.Unmarshal(raw, &patchBody)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && r.URL.Path == "/api/session/ses_1/prompt":
			_ = json.Unmarshal(raw, &promptBody)
			promptHeaders = r.Header.Clone()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"data":{"id":"msg_user_2","sessionID":"ses_1","type":"user"}}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/experimental/session/ses_1/wait":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/session/ses_1/message":
			listQueries = append(listQueries, r.URL.RawQuery)
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("cursor") == "" {
				// Page 1 (newest first): idle + second assistant message
				if r.URL.Query().Get("order") != "desc" || r.URL.Query().Get("limit") != "50" {
					t.Errorf("page 1 query = %q; want order=desc&limit=50", r.URL.RawQuery)
				}
				_, _ = io.WriteString(w, `{
					"data": [
						{"id":"msg_idle_2","type":"idle","outcome":"succeeded"},
						{"id":"msg_asst_2b","type":"assistant","content":[{"type":"reasoning","text":"hidden"},{"type":"text","text":"Second part"}]}
					],
					"cursor": {"previous": null, "next": "cur_page_2"}
				}`)
			} else if r.URL.Query().Get("cursor") == "cur_page_2" {
				// Page 2: must NOT combine cursor with order!
				if r.URL.Query().Get("order") != "" {
					t.Errorf("page 2 must not combine cursor with order, got query %q", r.URL.RawQuery)
				}
				// Contains first assistant message of this turn, current user message, and older turn messages that must be ignored!
				_, _ = io.WriteString(w, `{
					"data": [
						{"id":"msg_asst_2a","type":"assistant","content":[{"type":"text","text":"First part"}]},
						{"id":"msg_user_2","type":"user"},
						{"id":"msg_asst_1_old","type":"assistant","content":[{"type":"text","text":"OLD TURN SHOULD BE IGNORED"}]}
					],
					"cursor": {"previous": null, "next": "cur_page_3"}
				}`)
			} else {
				t.Errorf("unexpected extra pagination page: %q", r.URL.RawQuery)
				w.WriteHeader(http.StatusBadRequest)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		Password:     wantPW,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	resp, err := cli.SendMessage(context.Background(), "ses_1", OpenCodeMessageRequest{
		Directory:       "/tmp/worktree/app",
		Agent:           "coder",
		Model:           "anthropic/claude-sonnet-4#high",
		DisabledTools:   []string{"bash", "write", "patch", "edit", "read", "glob", "grep"},
		Prompt:          "Implement feature X",
		ExecutionID:     "exec-99",
		ActionID:        "act-7",
		ProxyPathPrefix: "/turn/exec-99/act-7",
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	if resp.Text != "First part\n\nSecond part" {
		t.Fatalf("resp.Text = %q; want %q", resp.Text, "First part\n\nSecond part")
	}
	if resp.MessageID != "msg_asst_2b" {
		t.Fatalf("resp.MessageID = %q; want msg_asst_2b", resp.MessageID)
	}

	mu.Lock()
	defer mu.Unlock()
	wantCalls := []string{
		"POST /api/session/ses_1/move",
		"POST /api/session/ses_1/agent",
		"POST /api/session/ses_1/model",
		"PATCH /api/session/ses_1",
		"POST /api/session/ses_1/prompt",
		"POST /api/experimental/session/ses_1/wait",
		"GET /api/session/ses_1/message",
		"GET /api/session/ses_1/message",
	}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("call sequence = %v; want %v", calls, wantCalls)
	}
	if moveBody["directory"] != "/tmp/worktree/app" {
		t.Fatalf("moveBody = %+v", moveBody)
	}
	if agentBody["agent"] != "coder" {
		t.Fatalf("agentBody = %+v", agentBody)
	}
	modelObj, _ := modelBody["model"].(map[string]any)
	if modelObj["providerID"] != "anthropic" || modelObj["id"] != "claude-sonnet-4" || modelObj["variant"] != "high" {
		t.Fatalf("modelBody = %+v; want providerID/id/variant", modelBody)
	}
	if _, hasOldKey := modelObj["modelID"]; hasOldKey {
		t.Fatalf("modelBody must use 'id', not v1 'modelID': %+v", modelBody)
	}

	// Verify V1->V2 permission mapping: bash->shell, write/patch/edit->edit, deduplicated & sorted.
	rawPerms, _ := json.Marshal(patchBody["permissions"])
	var gotPerms []openCodePermissionRule
	_ = json.Unmarshal(rawPerms, &gotPerms)
	wantPerms := []openCodePermissionRule{
		{Action: "edit", Resource: "*", Effect: "deny"},
		{Action: "glob", Resource: "*", Effect: "deny"},
		{Action: "grep", Resource: "*", Effect: "deny"},
		{Action: "read", Resource: "*", Effect: "deny"},
		{Action: "shell", Resource: "*", Effect: "deny"},
	}
	if !reflect.DeepEqual(gotPerms, wantPerms) {
		t.Fatalf("permissions = %+v; want %+v", gotPerms, wantPerms)
	}

	// Prompt body must contain ONLY {"text": ...} (additionalProperties: false).
	if len(promptBody) != 1 || promptBody["text"] != "Implement feature X" {
		t.Fatalf("promptBody = %+v; want only {text: ...}", promptBody)
	}
	if promptHeaders.Get("X-Kodema-Execution") != "exec-99" ||
		promptHeaders.Get("X-Kodema-Action") != "act-7" ||
		promptHeaders.Get("X-Kodema-Proxy-Turn-Path") != "/turn/exec-99/act-7" {
		t.Fatalf("missing Kodema attribution headers: %+v", promptHeaders)
	}
	if len(listQueries) != 2 {
		t.Fatalf("listQueries = %v; want 2 pages", listQueries)
	}
}

func TestHTTPOpenCodeClientModelValidationFailsFast(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	for _, badModel := range []string{"claude-sonnet-4", "/claude-sonnet-4", "anthropic/", "anthropic/#high", "anthropic/claude#"} {
		if _, err := cli.SendMessage(context.Background(), "ses_1", OpenCodeMessageRequest{
			Model:  badModel,
			Prompt: "hi",
		}); err == nil {
			t.Errorf("SendMessage with invalid model %q unexpectedly succeeded", badModel)
		}
	}
	if hits.Load() != 0 {
		t.Fatalf("server received %d requests for invalid models; want 0", hits.Load())
	}

	// Verify model without variant omits "variant" key cleanly.
	ref, err := parseOpenCodeModelRef("openrouter/anthropic/claude-sonnet-4")
	if err != nil {
		t.Fatalf("parseOpenCodeModelRef nested slash: %v", err)
	}
	if ref.ProviderID != "openrouter" || ref.ID != "anthropic/claude-sonnet-4" || ref.Variant != "" {
		t.Fatalf("ref = %+v", ref)
	}
	encoded, _ := json.Marshal(ref)
	if strings.Contains(string(encoded), "variant") {
		t.Fatalf("empty variant should be omitted: %s", string(encoded))
	}
}

func TestHTTPOpenCodeClientSendMessageErrorsAndBudgetClassification(t *testing.T) {
	cases := []struct {
		name          string
		promptStatus  int
		promptBody    string
		messagesBody  string
		wantErr       bool
		wantBudgetErr bool
		wantErrSubstr string
		wantText      string
	}{
		{
			name:         "success with assistant text and idle succeeded",
			promptStatus: http.StatusOK,
			promptBody:   `{"data":{"id":"u-1"}}`,
			messagesBody: `{"data":[{"id":"i-1","type":"idle","outcome":"succeeded"},{"id":"a-1","type":"assistant","content":[{"type":"text","text":"all done"}]},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantText:     "all done",
		},
		{
			name:          "assistant structured error generic",
			promptStatus:  http.StatusOK,
			promptBody:    `{"data":{"id":"u-1"}}`,
			messagesBody:  `{"data":[{"id":"a-1","type":"assistant","content":[],"error":{"type":"APIError","message":"provider unavailable","status":503}},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantErr:       true,
			wantErrSubstr: "provider unavailable",
		},
		{
			name:          "assistant structured error status 429",
			promptStatus:  http.StatusOK,
			promptBody:    `{"data":{"id":"u-1"}}`,
			messagesBody:  `{"data":[{"id":"a-1","type":"assistant","content":[],"error":{"type":"APIError","message":"rate limited","status":429}},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
		{
			name:          "assistant structured error response.body budget_exceeded",
			promptStatus:  http.StatusOK,
			promptBody:    `{"data":{"id":"u-1"}}`,
			messagesBody:  `{"data":[{"id":"a-1","type":"assistant","content":[],"error":{"type":"APIError","message":"upstream error","response":{"body":"{\"code\":\"budget_exceeded\"}"}}},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
		{
			name:          "idle failed with budget_exceeded error",
			promptStatus:  http.StatusOK,
			promptBody:    `{"data":{"id":"u-1"}}`,
			messagesBody:  `{"data":[{"id":"i-1","type":"idle","outcome":"failed","error":{"type":"APIError","message":"budget_exceeded: project limit reached","status":429}},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
		{
			name:          "idle failed without error detail",
			promptStatus:  http.StatusOK,
			promptBody:    `{"data":{"id":"u-1"}}`,
			messagesBody:  `{"data":[{"id":"i-1","type":"idle","outcome":"failed"},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantErr:       true,
			wantErrSubstr: "failed",
		},
		{
			name:          "idle interrupted",
			promptStatus:  http.StatusOK,
			promptBody:    `{"data":{"id":"u-1"}}`,
			messagesBody:  `{"data":[{"id":"i-1","type":"idle","outcome":"interrupted"},{"id":"u-1","type":"user"}],"cursor":{}}`,
			wantErr:       true,
			wantErrSubstr: "interrupted",
		},
		{
			name:          "http 429 on prompt endpoint",
			promptStatus:  http.StatusTooManyRequests,
			promptBody:    `{"code":"budget_exceeded","message":"limit reached"}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/api/session/sess-1/prompt":
					w.WriteHeader(tc.promptStatus)
					_, _ = io.WriteString(w, tc.promptBody)
				case r.Method == http.MethodPost && r.URL.Path == "/api/experimental/session/sess-1/wait":
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodGet && r.URL.Path == "/api/session/sess-1/message":
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, tc.messagesBody)
				default:
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()

			cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
				BaseURL:      srv.URL,
				RoundTripper: srv.Client().Transport,
			})
			if err != nil {
				t.Fatalf("NewHTTPOpenCodeClient: %v", err)
			}
			resp, err := cli.SendMessage(context.Background(), "sess-1", OpenCodeMessageRequest{Prompt: "hi"})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("SendMessage unexpectedly succeeded with %+v; want error", resp)
				}
				if tc.wantBudgetErr && !errors.Is(err, ErrOpenCodeBudgetExceeded) {
					t.Fatalf("err = %v; want wrapped ErrOpenCodeBudgetExceeded", err)
				}
				if !tc.wantBudgetErr && errors.Is(err, ErrOpenCodeBudgetExceeded) {
					t.Fatalf("err = %v; did not expect ErrOpenCodeBudgetExceeded", err)
				}
				if tc.wantErrSubstr != "" && !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("err = %v; want substring %q", err, tc.wantErrSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("SendMessage: %v", err)
			}
			if resp.Text != tc.wantText {
				t.Fatalf("Text = %q; want %q", resp.Text, tc.wantText)
			}
		})
	}
}

func TestHTTPOpenCodeClientAbortSessionDetachedContext(t *testing.T) {
	var abortCalled atomic.Bool
	var gotAuth string
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/session/sess-abort-1/interrupt" {
			mu.Lock()
			gotAuth = r.Header.Get("Authorization")
			mu.Unlock()
			abortCalled.Store(true)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"interrupted":true}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		Password:     "abort-pw",
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := cli.AbortSession(cancelledCtx, "sess-abort-1"); err != nil {
		t.Fatalf("AbortSession with cancelled caller ctx failed: %v", err)
	}
	if !abortCalled.Load() {
		t.Fatal("AbortSession did not reach server")
	}
	mu.Lock()
	if gotAuth != basicAuthHeader("abort-pw") {
		t.Fatalf("Authorization = %q; want Basic auth", gotAuth)
	}
	mu.Unlock()
}

func TestHTTPOpenCodeClientAttachAndDetachMCPV2(t *testing.T) {
	var mu sync.Mutex
	var putBody map[string]any
	var putQuery string
	var firstDeleteQuery string
	var deleteCalls int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != basicAuthHeader("mcp-pw") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/experimental/mcp/sandbox-1":
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &putBody)
			putQuery = r.URL.Query().Get("location[directory]")
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/experimental/mcp/sandbox-1":
			deleteCalls++
			if deleteCalls == 1 {
				firstDeleteQuery = r.URL.Query().Get("location[directory]")
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		Password:     "mcp-pw",
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	err = cli.AttachMCPServer(context.Background(), "ses_1", OpenCodeMCPServerConfig{
		Name:      "sandbox-1",
		Type:      "remote",
		URL:       "http://10.0.0.5:8090/mcp",
		Headers:   map[string]string{"Authorization": "Bearer tok-123"},
		Enabled:   true,
		Directory: "/srv/brain",
	})
	if err != nil {
		t.Fatalf("AttachMCPServer: %v", err)
	}

	// Detach with a cancelled caller ctx must still succeed (detached timeout).
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cli.DetachMCPServer(cancelledCtx, "ses_1", "sandbox-1"); err != nil {
		t.Fatalf("DetachMCPServer (204): %v", err)
	}
	// 404 on second detach must also return nil cleanly.
	if err := cli.DetachMCPServer(context.Background(), "ses_1", "sandbox-1"); err != nil {
		t.Fatalf("DetachMCPServer (404): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if putQuery != "/srv/brain" {
		t.Fatalf("PUT location[directory] = %q; want /srv/brain", putQuery)
	}
	if firstDeleteQuery != "/srv/brain" {
		t.Fatalf("DELETE location[directory] = %q; want /srv/brain", firstDeleteQuery)
	}
	cfgObj, _ := putBody["config"].(map[string]any)
	if cfgObj["type"] != "remote" || cfgObj["url"] != "http://10.0.0.5:8090/mcp" || cfgObj["disabled"] != false || cfgObj["oauth"] != false {
		t.Fatalf("PUT MCP config = %+v", putBody)
	}
	headersObj, _ := cfgObj["headers"].(map[string]any)
	if headersObj["Authorization"] != "Bearer tok-123" {
		t.Fatalf("PUT MCP headers = %+v", headersObj)
	}
}

func TestHTTPOpenCodeClientMCPConcurrencyConflictAndDetachRetry(t *testing.T) {
	var mu sync.Mutex
	var putCount int
	var deleteCalls int
	var deleteQueries []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/experimental/mcp/sandbox":
			putCount++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/api/experimental/mcp/sandbox":
			deleteCalls++
			deleteQueries = append(deleteQueries, r.URL.Query().Get("location[directory]"))
			if deleteCalls == 1 {
				// First delete returns transient 500; client must preserve location[directory] and ownership.
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, `{"error":"transient failure"}`)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	// Session A attaches "sandbox" in /srv/brain.
	err = cli.AttachMCPServer(context.Background(), "ses_A", OpenCodeMCPServerConfig{
		Name:      "sandbox",
		URL:       "http://10.0.0.1:8090/mcp",
		Enabled:   true,
		Directory: "/srv/brain",
	})
	if err != nil {
		t.Fatalf("session A AttachMCPServer: %v", err)
	}

	// Session B attempting to attach "sandbox" in the same location (even with trailing slash)
	// MUST fail closed before issuing PUT!
	err = cli.AttachMCPServer(context.Background(), "ses_B", OpenCodeMCPServerConfig{
		Name:      "sandbox",
		URL:       "http://10.0.0.2:8090/mcp",
		Enabled:   true,
		Directory: "/srv/brain/",
	})
	if err == nil || !strings.Contains(err.Error(), "--max-concurrency 1") {
		t.Fatalf("session B AttachMCPServer err = %v; want conflict error mentioning --max-concurrency 1", err)
	}

	// Session B MUST NOT be allowed to detach session A's "sandbox"!
	err = cli.DetachMCPServer(context.Background(), "ses_B", "sandbox")
	if err == nil || !strings.Contains(err.Error(), "ses_A") {
		t.Fatalf("session B DetachMCPServer err = %v; want ownership refusal mentioning ses_A", err)
	}

	mu.Lock()
	if putCount != 1 {
		t.Fatalf("PUT count = %d; want 1 (session B conflict must fail before PUT)", putCount)
	}
	if deleteCalls != 0 {
		t.Fatalf("DELETE count = %d; want 0 (session B must not detach session A's MCP server)", deleteCalls)
	}
	mu.Unlock()

	// Session A's first detach hits transient 500; ownership & directory must be preserved for retry.
	if err := cli.DetachMCPServer(context.Background(), "ses_A", "sandbox"); err == nil {
		t.Fatal("first DetachMCPServer unexpectedly succeeded on HTTP 500")
	}

	// Session B still cannot attach while session A's detach has not succeeded.
	if err := cli.AttachMCPServer(context.Background(), "ses_B", OpenCodeMCPServerConfig{
		Name:      "sandbox",
		URL:       "http://10.0.0.2:8090/mcp",
		Enabled:   true,
		Directory: "/srv/brain",
	}); err == nil {
		t.Fatal("session B unexpectedly attached while session A detach had failed")
	}

	// Session A retries detach: must still send location[directory]=/srv/brain and succeed.
	if err := cli.DetachMCPServer(context.Background(), "ses_A", "sandbox"); err != nil {
		t.Fatalf("retry DetachMCPServer: %v", err)
	}

	mu.Lock()
	if !reflect.DeepEqual(deleteQueries, []string{"/srv/brain", "/srv/brain"}) {
		t.Fatalf("DELETE location[directory] queries = %v; want [/srv/brain /srv/brain]", deleteQueries)
	}
	mu.Unlock()

	// After session A's detach succeeds, session B can now attach cleanly.
	if err := cli.AttachMCPServer(context.Background(), "ses_B", OpenCodeMCPServerConfig{
		Name:      "sandbox",
		URL:       "http://10.0.0.2:8090/mcp",
		Enabled:   true,
		Directory: "/srv/brain",
	}); err != nil {
		t.Fatalf("session B AttachMCPServer after session A detach: %v", err)
	}
}

func TestHTTPOpenCodeClientPaginationAndEmptyTurnFailures(t *testing.T) {
	t.Run("duplicate cursor before reaching user message fails closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/prompt"):
				_, _ = io.WriteString(w, `{"data":{"id":"u-1"}}`)
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/wait"):
				w.WriteHeader(http.StatusNoContent)
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/message"):
				_, _ = io.WriteString(w, `{"data":[{"id":"a-1","type":"assistant","content":[{"type":"text","text":"partial"}]}],"cursor":{"next":"cur-same"}}`)
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: srv.URL, RoundTripper: srv.Client().Transport})
		if err != nil {
			t.Fatalf("NewHTTPOpenCodeClient: %v", err)
		}
		_, err = cli.SendMessage(context.Background(), "ses_1", OpenCodeMessageRequest{Prompt: "hi"})
		if err == nil || !strings.Contains(err.Error(), "duplicate cursor") {
			t.Fatalf("err = %v; want duplicate cursor error", err)
		}
	})

	t.Run("message history exhausted without user message fails closed", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/prompt"):
				_, _ = io.WriteString(w, `{"data":{"id":"u-missing"}}`)
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/wait"):
				w.WriteHeader(http.StatusNoContent)
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/message"):
				_, _ = io.WriteString(w, `{"data":[{"id":"a-1","type":"assistant","content":[{"type":"text","text":"unrelated"}]}],"cursor":{"next":null}}`)
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: srv.URL, RoundTripper: srv.Client().Transport})
		if err != nil {
			t.Fatalf("NewHTTPOpenCodeClient: %v", err)
		}
		_, err = cli.SendMessage(context.Background(), "ses_1", OpenCodeMessageRequest{Prompt: "hi"})
		if err == nil || !strings.Contains(err.Error(), "exhausted") {
			t.Fatalf("err = %v; want exhausted error", err)
		}
	})

	t.Run("exceeding maxOpenCodeMessagePages without user message fails closed", func(t *testing.T) {
		var pageNum atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/prompt"):
				_, _ = io.WriteString(w, `{"data":{"id":"u-deep"}}`)
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/wait"):
				w.WriteHeader(http.StatusNoContent)
			case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/message"):
				n := pageNum.Add(1)
				_, _ = io.WriteString(w, `{"data":[{"id":"a-`+strings.Repeat("x", int(n))+`","type":"assistant","content":[{"type":"text","text":"step"}]}],"cursor":{"next":"cur-`+strings.Repeat("n", int(n))+`"}}`)
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: srv.URL, RoundTripper: srv.Client().Transport})
		if err != nil {
			t.Fatalf("NewHTTPOpenCodeClient: %v", err)
		}
		_, err = cli.SendMessage(context.Background(), "ses_1", OpenCodeMessageRequest{Prompt: "hi"})
		if err == nil || !strings.Contains(err.Error(), "exceeded") {
			t.Fatalf("err = %v; want exceeded pages error", err)
		}
	})

	t.Run("empty post-prompt message list or no assistant/idle fails closed", func(t *testing.T) {
		for name, messagesJSON := range map[string]string{
			"only user message": `{"data":[{"id":"u-1","type":"user"}],"cursor":{}}`,
			"only tool message": `{"data":[{"id":"t-1","type":"tool"},{"id":"u-1","type":"user"}],"cursor":{}}`,
		} {
			t.Run(name, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/prompt"):
						_, _ = io.WriteString(w, `{"data":{"id":"u-1"}}`)
					case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/wait"):
						w.WriteHeader(http.StatusNoContent)
					case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/message"):
						_, _ = io.WriteString(w, messagesJSON)
					default:
						http.NotFound(w, r)
					}
				}))
				defer srv.Close()

				cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: srv.URL, RoundTripper: srv.Client().Transport})
				if err != nil {
					t.Fatalf("NewHTTPOpenCodeClient: %v", err)
				}
				_, err = cli.SendMessage(context.Background(), "ses_1", OpenCodeMessageRequest{Prompt: "hi"})
				if err == nil || !strings.Contains(err.Error(), "without an assistant or idle message") {
					t.Fatalf("err = %v; want missing assistant/idle error", err)
				}
			})
		}
	})
}

func TestHTTPOpenCodeClientPathValidationAndSingleEscaping(t *testing.T) {
	// BaseURL with query or fragment must be rejected.
	for _, badURL := range []string{
		"http://127.0.0.1:4096?foo=bar",
		"http://127.0.0.1:4096?",
		"http://127.0.0.1:4096#frag",
		"http://127.0.0.1:4096/prefix?a=1#frag",
	} {
		if _, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{BaseURL: badURL}); err == nil {
			t.Fatalf("NewHTTPOpenCodeClient(%q) unexpectedly succeeded", badURL)
		}
	}

	var mu sync.Mutex
	var gotRawPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotRawPaths = append(gotRawPaths, r.URL.EscapedPath())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}

	// "." and ".." must be rejected for sessionID and MCP server names without hitting server.
	for _, bad := range []string{".", "..", "a/b", "a\\b", ""} {
		if _, err := cli.SendMessage(context.Background(), bad, OpenCodeMessageRequest{Prompt: "hi"}); err == nil {
			t.Fatalf("SendMessage(%q) unexpectedly succeeded", bad)
		}
		if err := cli.AbortSession(context.Background(), bad); err == nil {
			t.Fatalf("AbortSession(%q) unexpectedly succeeded", bad)
		}
		if err := cli.AttachMCPServer(context.Background(), bad, OpenCodeMCPServerConfig{Name: "sandbox", URL: "http://127.0.0.1:8090/mcp"}); err == nil {
			t.Fatalf("AttachMCPServer(session=%q) unexpectedly succeeded", bad)
		}
		if err := cli.AttachMCPServer(context.Background(), "ses_1", OpenCodeMCPServerConfig{Name: bad, URL: "http://127.0.0.1:8090/mcp"}); err == nil {
			t.Fatalf("AttachMCPServer(name=%q) unexpectedly succeeded", bad)
		}
		if err := cli.DetachMCPServer(context.Background(), bad, "sandbox"); err == nil {
			t.Fatalf("DetachMCPServer(session=%q) unexpectedly succeeded", bad)
		}
		if err := cli.DetachMCPServer(context.Background(), "ses_1", bad); err == nil {
			t.Fatalf("DetachMCPServer(name=%q) unexpectedly succeeded", bad)
		}
	}
	mu.Lock()
	if len(gotRawPaths) != 0 {
		t.Fatalf("invalid segments hit server: %v", gotRawPaths)
	}
	mu.Unlock()

	// Session ID and MCP server name containing #, ?, % must arrive single-escaped (never %25-double-escaped).
	specialSession := "ses#1?q=1%20end"
	if err := cli.AbortSession(context.Background(), specialSession); err != nil {
		t.Fatalf("AbortSession special: %v", err)
	}
	specialMCP := "box#1?x%20y"
	if err := cli.AttachMCPServer(context.Background(), "ses_1", OpenCodeMCPServerConfig{
		Name:    specialMCP,
		URL:     "http://127.0.0.1:8090/mcp",
		Enabled: true,
	}); err != nil {
		t.Fatalf("AttachMCPServer special: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	wantPaths := []string{
		"/api/session/ses%231%3Fq=1%2520end/interrupt",
		"/api/experimental/mcp/box%231%3Fx%2520y",
	}
	if !reflect.DeepEqual(gotRawPaths, wantPaths) {
		t.Fatalf("EscapedPath sequence = %v; want %v", gotRawPaths, wantPaths)
	}
}

func TestHTTPOpenCodeClientStructuredErrorNoBare429FalsePositive(t *testing.T) {
	// Status 500 with a request_id containing digits "429" must NOT classify as ErrOpenCodeBudgetExceeded.
	err500 := classifyOpenCodeStructuredError(&openCodeStructuredError{
		Type:    "APIError",
		Message: "internal error",
		Status:  http.StatusInternalServerError,
		Response: &struct {
			Body string `json:"body"`
		}{Body: `{"request_id":"req_01429abc","error":"internal"}`},
	})
	if errors.Is(err500, ErrOpenCodeBudgetExceeded) {
		t.Fatalf("500 with req_01429abc falsely matched ErrOpenCodeBudgetExceeded: %v", err500)
	}

	// Status 0 with digits "429" in request_id must also NOT match.
	err0False := classifyOpenCodeStructuredError(&openCodeStructuredError{
		Type:    "APIError",
		Message: "failed request req_01429abc at 1700429000",
	})
	if errors.Is(err0False, ErrOpenCodeBudgetExceeded) {
		t.Fatalf("status 0 with req_01429abc falsely matched ErrOpenCodeBudgetExceeded: %v", err0False)
	}

	// Status 0 with explicit HTTP 429 text MUST match ErrOpenCodeBudgetExceeded.
	err0True := classifyOpenCodeStructuredError(&openCodeStructuredError{
		Type:    "APIError",
		Message: "upstream returned HTTP 429 Too Many Requests",
	})
	if !errors.Is(err0True, ErrOpenCodeBudgetExceeded) {
		t.Fatalf("status 0 with explicit HTTP 429 did not match ErrOpenCodeBudgetExceeded: %v", err0True)
	}
}

func TestHTTPOpenCodeClientPasswordFilePreservesIntentionalSpaces(t *testing.T) {
	pwFile := filepath.Join(t.TempDir(), "spaced.pw")
	if err := os.WriteFile(pwFile, []byte(" spaced-secret \r\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"version":"2.3.2"}`)
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
		PasswordFile: pwFile,
		RoundTripper: srv.Client().Transport,
	})
	if err != nil {
		t.Fatalf("NewHTTPOpenCodeClient: %v", err)
	}
	if err := cli.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
	want := basicAuthHeader(" spaced-secret ")
	if gotAuth != want {
		t.Fatalf("Authorization = %q; want %q (intentional spaces preserved)", gotAuth, want)
	}
}
