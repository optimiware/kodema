package services

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHTTPOpenCodeClientNoProxyAndInfoErrorHandling(t *testing.T) {
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

	cases := []struct {
		name          string
		status        int
		body          string
		wantErr       bool
		wantBudgetErr bool
		wantText      string
	}{
		{
			name:     "success text parts",
			status:   http.StatusOK,
			body:     `{"info":{"id":"msg-1"},"parts":[{"type":"text","text":"all done"}]}`,
			wantText: "all done",
		},
		{
			name:    "http 200 with info.error generic",
			status:  http.StatusOK,
			body:    `{"info":{"id":"msg-2","error":{"name":"APIError","message":"provider unavailable"}},"parts":[]}`,
			wantErr: true,
		},
		{
			name:          "http 200 with info.error budget_exceeded",
			status:        http.StatusOK,
			body:          `{"info":{"id":"msg-3","error":{"name":"APIError","message":"budget_exceeded: project limit reached"}},"parts":[]}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
		{
			name:          "http 200 with info.error data statusCode 429",
			status:        http.StatusOK,
			body:          `{"info":{"id":"msg-4","error":{"name":"APIError","data":{"message":"rate limited","statusCode":429}}}}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
		{
			name:          "http 429 response",
			status:        http.StatusTooManyRequests,
			body:          `{"code":"budget_exceeded","message":"limit reached"}`,
			wantErr:       true,
			wantBudgetErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/session/sess-abort-1/abort" {
			abortCalled.Store(true)
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	cli, err := NewHTTPOpenCodeClient(HTTPOpenCodeClientConfig{
		BaseURL:      srv.URL,
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
}
