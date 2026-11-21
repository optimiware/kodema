package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// maxOpenCodeResponseBytes bounds one HTTP response from opencode serve.
	maxOpenCodeResponseBytes int64 = 16 << 20
	// defaultOpenCodeAbortTimeout bounds the abort request sent when a turn is cancelled.
	defaultOpenCodeAbortTimeout = 10 * time.Second
)

// HTTPOpenCodeClientConfig configures the concrete HTTP OpenCode client.
type HTTPOpenCodeClientConfig struct {
	BaseURL      string
	RoundTripper http.RoundTripper
	AbortTimeout time.Duration
}

// HTTPOpenCodeClient talks to a local `opencode serve` instance over HTTP.
type HTTPOpenCodeClient struct {
	baseURL      *url.URL
	client       *http.Client
	abortTimeout time.Duration
}

var _ OpenCodeClient = (*HTTPOpenCodeClient)(nil)

var errOpenCodeRedirectRefused = errors.New("opencode client refuses HTTP redirects")

// NewHTTPOpenCodeClient validates BaseURL and returns an HTTP OpenCode client.
func NewHTTPOpenCodeClient(config HTTPOpenCodeClientConfig) (*HTTPOpenCodeClient, error) {
	raw := strings.TrimSpace(config.BaseURL)
	if raw == "" {
		return nil, fmt.Errorf("opencode server URL is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse opencode server URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("opencode server URL must use http or https")
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("opencode server URL has no host")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("opencode server URL must not embed credentials")
	}
	rt := config.RoundTripper
	if rt == nil {
		rt = http.DefaultTransport
	}
	abortTimeout := config.AbortTimeout
	if abortTimeout <= 0 {
		abortTimeout = defaultOpenCodeAbortTimeout
	}
	return &HTTPOpenCodeClient{
		baseURL: parsed,
		client: &http.Client{
			Transport: rt,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errOpenCodeRedirectRefused
			},
		},
		abortTimeout: abortTimeout,
	}, nil
}

// Health verifies that `opencode serve` is reachable and healthy.
func (c *HTTPOpenCodeClient) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	target := c.endpoint("/global/health", "")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build opencode health request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("opencode health check: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}
	return fmt.Errorf("opencode health returned status %d", resp.StatusCode)
}

// CreateSession creates a new durable OpenCode session bound to directory.
func (c *HTTPOpenCodeClient) CreateSession(ctx context.Context, directory string, title string) (OpenCodeSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	payload := map[string]any{}
	if strings.TrimSpace(title) != "" {
		payload["title"] = strings.TrimSpace(title)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return OpenCodeSession{}, err
	}
	target := c.endpoint("/session", directory)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
	if err != nil {
		return OpenCodeSession{}, fmt.Errorf("build opencode create session request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return OpenCodeSession{}, fmt.Errorf("opencode create session: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOpenCodeResponseBytes+1))
	if err != nil {
		return OpenCodeSession{}, fmt.Errorf("read opencode create session response: %w", err)
	}
	if int64(len(body)) > maxOpenCodeResponseBytes {
		return OpenCodeSession{}, fmt.Errorf("opencode create session response exceeds bound")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return OpenCodeSession{}, classifyOpenCodeHTTPError("create session", resp.StatusCode, body)
	}
	var session OpenCodeSession
	if err := json.Unmarshal(body, &session); err != nil {
		return OpenCodeSession{}, fmt.Errorf("decode opencode session: %w", err)
	}
	if strings.TrimSpace(session.ID) == "" {
		return OpenCodeSession{}, fmt.Errorf("opencode create session returned an empty session id")
	}
	return session, nil
}

// SendMessage submits one prompt turn to an existing OpenCode session and
// returns the assistant's textual response.
func (c *HTTPOpenCodeClient) SendMessage(ctx context.Context, sessionID string, msg OpenCodeMessageRequest) (OpenCodeMessageResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || strings.ContainsAny(sessionID, "/\\ \r\n\x00") {
		return OpenCodeMessageResponse{}, fmt.Errorf("opencode session id %q is invalid", sessionID)
	}

	payload := map[string]any{
		"parts": []map[string]any{
			{"type": "text", "text": msg.Prompt},
		},
	}
	if strings.TrimSpace(msg.Agent) != "" {
		payload["agent"] = strings.TrimSpace(msg.Agent)
	}
	if strings.TrimSpace(msg.Model) != "" {
		if providerID, modelID, ok := strings.Cut(strings.TrimSpace(msg.Model), "/"); ok && providerID != "" && modelID != "" {
			payload["model"] = map[string]string{
				"providerID": providerID,
				"modelID":    modelID,
			}
		} else {
			payload["model"] = strings.TrimSpace(msg.Model)
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return OpenCodeMessageResponse{}, err
	}

	target := c.endpoint("/session/"+url.PathEscape(sessionID)+"/message", msg.Directory)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
	if err != nil {
		return OpenCodeMessageResponse{}, fmt.Errorf("build opencode message request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if msg.ExecutionID != "" {
		req.Header.Set("X-Kodema-Execution", msg.ExecutionID)
	}
	if msg.ActionID != "" {
		req.Header.Set("X-Kodema-Action", msg.ActionID)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return OpenCodeMessageResponse{}, fmt.Errorf("opencode send message: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxOpenCodeResponseBytes+1))
	if err != nil {
		return OpenCodeMessageResponse{}, fmt.Errorf("read opencode message response: %w", err)
	}
	if int64(len(body)) > maxOpenCodeResponseBytes {
		return OpenCodeMessageResponse{}, fmt.Errorf("opencode message response exceeds bound")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("send message", resp.StatusCode, body)
	}
	return decodeOpenCodeMessageResponse(body), nil
}

// AbortSession invokes POST /session/{id}/abort so OpenCode immediately halts
// generation and kills active tools/subprocesses for that session. It uses a
// bounded non-cancelled context so abort still completes after turn cancellation.
func (c *HTTPOpenCodeClient) AbortSession(ctx context.Context, sessionID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || strings.ContainsAny(sessionID, "/\\ \r\n\x00") {
		return fmt.Errorf("opencode session id %q is invalid", sessionID)
	}
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.abortTimeout)
	defer cancel()

	target := c.endpoint("/session/"+url.PathEscape(sessionID)+"/abort", "")
	req, err := http.NewRequestWithContext(abortCtx, http.MethodPost, target, nil)
	if err != nil {
		return fmt.Errorf("build opencode abort request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("opencode abort session: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}
	return classifyOpenCodeHTTPError("abort session", resp.StatusCode, body)
}

func (c *HTTPOpenCodeClient) endpoint(path, directory string) string {
	target := *c.baseURL
	target.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	target.RawPath = ""
	if strings.TrimSpace(directory) != "" {
		q := target.Query()
		q.Set("directory", directory)
		target.RawQuery = q.Encode()
	}
	return target.String()
}

func decodeOpenCodeMessageResponse(body []byte) OpenCodeMessageResponse {
	var parsed struct {
		Text      string `json:"text"`
		MessageID string `json:"message_id"`
		Info      *struct {
			ID    string `json:"id"`
			Error *struct {
				Name    string `json:"name"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"info"`
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return OpenCodeMessageResponse{Text: strings.TrimSpace(string(body))}
	}
	out := OpenCodeMessageResponse{
		Text:      strings.TrimSpace(parsed.Text),
		MessageID: parsed.MessageID,
	}
	if parsed.Info != nil && out.MessageID == "" {
		out.MessageID = parsed.Info.ID
	}
	if len(parsed.Parts) > 0 {
		var parts []string
		for _, part := range parsed.Parts {
			if (part.Type == "" || part.Type == "text") && strings.TrimSpace(part.Text) != "" {
				parts = append(parts, strings.TrimSpace(part.Text))
			}
		}
		if len(parts) > 0 {
			out.Text = strings.Join(parts, "\n\n")
		}
	}
	return out
}

func classifyOpenCodeHTTPError(op string, status int, body []byte) error {
	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 512 {
		snippet = snippet[:512]
	}
	lower := strings.ToLower(snippet)
	if status == http.StatusTooManyRequests || strings.Contains(lower, "budget_exceeded") {
		return fmt.Errorf("%w: opencode %s returned %d: %s", ErrOpenCodeBudgetExceeded, op, status, snippet)
	}
	return fmt.Errorf("opencode %s returned status %d: %s", op, status, snippet)
}
