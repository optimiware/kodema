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
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// maxOpenCodeResponseBytes bounds one HTTP response from opencode serve.
	maxOpenCodeResponseBytes int64 = 16 << 20
	// maxOpenCodeControlBytes bounds small control/error HTTP responses.
	maxOpenCodeControlBytes int64 = 64 << 10
	// maxOpenCodePasswordFileBytes bounds the Basic Auth password file.
	maxOpenCodePasswordFileBytes int64 = 64 << 10
	// defaultOpenCodeAbortTimeout bounds interrupt/detach requests sent after turn cancellation.
	defaultOpenCodeAbortTimeout = 10 * time.Second
	// openCodeBasicAuthUsername is the fixed HTTP Basic Auth username required by OpenCode v2.
	openCodeBasicAuthUsername = "opencode"
	// openCodeMessagePageLimit is the page size used when fetching turn messages.
	openCodeMessagePageLimit = 50
	// maxOpenCodeMessagePages bounds message history pagination for a single turn.
	maxOpenCodeMessagePages = 10
)

// HTTPOpenCodeClientConfig configures the concrete HTTP OpenCode v2 client.
type HTTPOpenCodeClientConfig struct {
	BaseURL      string
	Password     string
	PasswordFile string
	RoundTripper http.RoundTripper
	AbortTimeout time.Duration
}

type mcpAttachmentKey struct {
	dir  string
	name string
}

type mcpSessionServerKey struct {
	sessionID string
	name      string
}

// HTTPOpenCodeClient talks to an OpenCode v2 `opencode serve` instance over HTTP.
type HTTPOpenCodeClient struct {
	baseURL      *url.URL
	client       *http.Client
	abortTimeout time.Duration
	password     string
	passwordFile string

	mcpMu        sync.Mutex
	mcpOwners    map[mcpAttachmentKey]string
	mcpPending   map[mcpAttachmentKey]string
	mcpBySession map[mcpSessionServerKey]string
}

var _ OpenCodeClient = (*HTTPOpenCodeClient)(nil)
var _ OpenCodeMCPClient = (*HTTPOpenCodeClient)(nil)

var errOpenCodeRedirectRefused = errors.New("opencode client refuses HTTP redirects")

// ValidateOpenCodePassword validates an OpenCode Basic Auth password without
// exposing password material in error messages.
func ValidateOpenCodePassword(password string) error {
	if strings.ContainsAny(password, "\x00\r\n") {
		return fmt.Errorf("opencode password must not contain NUL or newline characters")
	}
	return nil
}

// ValidateOpenCodePasswordFile verifies that path can be read, is bounded and
// non-empty after stripping trailing newlines, and contains no forbidden
// control characters.
func ValidateOpenCodePasswordFile(path string) error {
	_, err := readOpenCodePasswordFile(path)
	return err
}

func readOpenCodePasswordFile(path string) (string, error) {
	cleaned := strings.TrimSpace(path)
	if cleaned == "" {
		return "", fmt.Errorf("opencode password file path is empty")
	}
	f, err := os.Open(cleaned)
	if err != nil {
		return "", fmt.Errorf("read opencode password file: %w", err)
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(io.LimitReader(f, maxOpenCodePasswordFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("read opencode password file: %w", err)
	}
	if int64(len(data)) > maxOpenCodePasswordFileBytes {
		return "", fmt.Errorf("opencode password file exceeds maximum size")
	}
	pw := strings.TrimRight(string(data), "\r\n")
	if strings.TrimSpace(pw) == "" {
		return "", fmt.Errorf("opencode password file is empty")
	}
	if err := ValidateOpenCodePassword(pw); err != nil {
		return "", err
	}
	return pw, nil
}

func validateOpenCodePathSegment(kind, value string) error {
	if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/\\ \r\n\x00") {
		return fmt.Errorf("opencode %s %q is invalid", kind, value)
	}
	return nil
}

func normalizeOpenCodeMCPDir(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	return filepath.Clean(trimmed)
}

// NewHTTPOpenCodeClient validates config and returns an HTTP OpenCode v2 client.
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
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, fmt.Errorf("opencode server URL must not include a query or fragment")
	}

	password := config.Password
	passwordFile := strings.TrimSpace(config.PasswordFile)
	if password != "" && passwordFile != "" {
		return nil, fmt.Errorf("opencode client cannot configure both Password and PasswordFile")
	}
	if password != "" {
		if err := ValidateOpenCodePassword(password); err != nil {
			return nil, err
		}
	}
	if passwordFile != "" {
		if err := ValidateOpenCodePasswordFile(passwordFile); err != nil {
			return nil, err
		}
	}

	rt := config.RoundTripper
	if rt == nil {
		rt = defaultNoProxyTransport()
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
		password:     password,
		passwordFile: passwordFile,
		mcpOwners:    make(map[mcpAttachmentKey]string),
		mcpPending:   make(map[mcpAttachmentKey]string),
		mcpBySession: make(map[mcpSessionServerKey]string),
	}, nil
}

// Health verifies that `opencode serve` v2 is reachable, authenticated, and
// serving the JSON API (`GET /api/info`) rather than an HTML SPA fallback.
func (c *HTTPOpenCodeClient) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	status, body, err := c.doRequest(ctx, http.MethodGet, []string{"api", "info"}, nil, nil, nil, maxOpenCodeControlBytes, "health")
	if err != nil {
		return err
	}
	if status < 200 || status > 299 {
		return classifyOpenCodeHTTPError("health", status, body)
	}
	var info struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return fmt.Errorf("opencode health returned non-JSON response (status %d): %w", status, err)
	}
	if strings.TrimSpace(info.Version) == "" {
		return fmt.Errorf("opencode health response missing server version")
	}
	return nil
}

// CreateSession creates a new durable OpenCode v2 session bound to directory.
func (c *HTTPOpenCodeClient) CreateSession(ctx context.Context, directory string, title string) (OpenCodeSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	payload := map[string]any{}
	if t := strings.TrimSpace(title); t != "" {
		payload["title"] = t
	}
	if d := strings.TrimSpace(directory); d != "" {
		payload["location"] = map[string]string{"directory": d}
	}

	status, body, err := c.doRequest(ctx, http.MethodPost, []string{"api", "session"}, nil, payload, nil, maxOpenCodeResponseBytes, "create session")
	if err != nil {
		return OpenCodeSession{}, err
	}
	if status < 200 || status > 299 {
		return OpenCodeSession{}, classifyOpenCodeHTTPError("create session", status, body)
	}
	var envelope struct {
		Data OpenCodeSession `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return OpenCodeSession{}, fmt.Errorf("decode opencode session: %w", err)
	}
	if strings.TrimSpace(envelope.Data.ID) == "" {
		return OpenCodeSession{}, fmt.Errorf("opencode create session returned an empty session id")
	}
	return envelope.Data, nil
}

type openCodeModelRef struct {
	ProviderID string `json:"providerID"`
	ID         string `json:"id"`
	Variant    string `json:"variant,omitempty"`
}

func parseOpenCodeModelRef(raw string) (openCodeModelRef, error) {
	trimmed := strings.TrimSpace(raw)
	providerID, rest, ok := strings.Cut(trimmed, "/")
	providerID = strings.TrimSpace(providerID)
	rest = strings.TrimSpace(rest)
	if !ok || providerID == "" || rest == "" {
		return openCodeModelRef{}, fmt.Errorf("opencode model %q must be formatted as providerID/modelID[#variant]", raw)
	}
	modelID, variant, hasVariant := strings.Cut(rest, "#")
	modelID = strings.TrimSpace(modelID)
	variant = strings.TrimSpace(variant)
	if modelID == "" {
		return openCodeModelRef{}, fmt.Errorf("opencode model %q has an empty model ID", raw)
	}
	if hasVariant && variant == "" {
		return openCodeModelRef{}, fmt.Errorf("opencode model %q has an empty variant after '#'", raw)
	}
	return openCodeModelRef{
		ProviderID: providerID,
		ID:         modelID,
		Variant:    variant,
	}, nil
}

type openCodePermissionRule struct {
	Action   string `json:"action"`
	Resource string `json:"resource"`
	Effect   string `json:"effect"`
}

// mapOpenCodeDisabledToolsToPermissions maps disabled tool names (including
// OpenCode v1 aliases such as bash/write/patch) to deterministic OpenCode v2
// deny rules.
func mapOpenCodeDisabledToolsToPermissions(disabled []string) []openCodePermissionRule {
	if len(disabled) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(disabled))
	actions := make([]string, 0, len(disabled))
	for _, raw := range disabled {
		tool := strings.TrimSpace(raw)
		if tool == "" {
			continue
		}
		var mapped string
		switch tool {
		case "bash":
			mapped = "shell"
		case "write", "patch":
			mapped = "edit"
		default:
			mapped = tool
		}
		if _, exists := seen[mapped]; exists {
			continue
		}
		seen[mapped] = struct{}{}
		actions = append(actions, mapped)
	}
	if len(actions) == 0 {
		return nil
	}
	sort.Strings(actions)
	rules := make([]openCodePermissionRule, 0, len(actions))
	for _, action := range actions {
		rules = append(rules, openCodePermissionRule{
			Action:   action,
			Resource: "*",
			Effect:   "deny",
		})
	}
	return rules
}

// SendMessage submits one prompt turn to an existing OpenCode v2 session,
// waits for the session agent loop to reach idle, and returns the assembled
// assistant response for that turn.
func (c *HTTPOpenCodeClient) SendMessage(ctx context.Context, sessionID string, msg OpenCodeMessageRequest) (OpenCodeMessageResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID = strings.TrimSpace(sessionID)
	if err := validateOpenCodePathSegment("session id", sessionID); err != nil {
		return OpenCodeMessageResponse{}, err
	}

	var modelRef openCodeModelRef
	var hasModel bool
	if strings.TrimSpace(msg.Model) != "" {
		parsed, err := parseOpenCodeModelRef(msg.Model)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		modelRef = parsed
		hasModel = true
	}

	// 1. Optional pre-turn session configuration.
	if dir := strings.TrimSpace(msg.Directory); dir != "" {
		status, body, err := c.doRequest(
			ctx,
			http.MethodPost,
			[]string{"api", "session", sessionID, "move"},
			nil,
			map[string]string{"directory": dir},
			nil,
			maxOpenCodeControlBytes,
			"move session",
		)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if status < 200 || status > 299 {
			return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("move session", status, body)
		}
	}

	if agent := strings.TrimSpace(msg.Agent); agent != "" {
		status, body, err := c.doRequest(
			ctx,
			http.MethodPost,
			[]string{"api", "session", sessionID, "agent"},
			nil,
			map[string]string{"agent": agent},
			nil,
			maxOpenCodeControlBytes,
			"switch agent",
		)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if status < 200 || status > 299 {
			return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("switch agent", status, body)
		}
	}

	if hasModel {
		status, body, err := c.doRequest(
			ctx,
			http.MethodPost,
			[]string{"api", "session", sessionID, "model"},
			nil,
			map[string]openCodeModelRef{"model": modelRef},
			nil,
			maxOpenCodeControlBytes,
			"switch model",
		)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if status < 200 || status > 299 {
			return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("switch model", status, body)
		}
	}

	if perms := mapOpenCodeDisabledToolsToPermissions(msg.DisabledTools); len(perms) > 0 {
		status, body, err := c.doRequest(
			ctx,
			http.MethodPatch,
			[]string{"api", "session", sessionID},
			nil,
			map[string][]openCodePermissionRule{"permissions": perms},
			nil,
			maxOpenCodeControlBytes,
			"update session permissions",
		)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if status < 200 || status > 299 {
			return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("update session permissions", status, body)
		}
	}

	// Note: OpenCode v2 /api/session/{id}/prompt enforces additionalProperties: false
	// and does not accept inline MCP server maps; Brain mode attaches/detaches
	// MCP servers dynamically via AttachMCPServer/DetachMCPServer.

	// 2. Enqueue the prompt turn.
	extraHeaders := map[string]string{}
	if msg.ExecutionID != "" {
		extraHeaders["X-Kodema-Execution"] = msg.ExecutionID
	}
	if msg.ActionID != "" {
		extraHeaders["X-Kodema-Action"] = msg.ActionID
	}
	if msg.ProxyPathPrefix != "" {
		extraHeaders["X-Kodema-Proxy-Turn-Path"] = msg.ProxyPathPrefix
	}
	status, body, err := c.doRequest(
		ctx,
		http.MethodPost,
		[]string{"api", "session", sessionID, "prompt"},
		nil,
		struct {
			Text string `json:"text"`
		}{Text: msg.Prompt},
		extraHeaders,
		maxOpenCodeResponseBytes,
		"send prompt",
	)
	if err != nil {
		return OpenCodeMessageResponse{}, err
	}
	if status < 200 || status > 299 {
		return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("send prompt", status, body)
	}
	var promptResp struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &promptResp); err != nil {
		return OpenCodeMessageResponse{}, fmt.Errorf("decode opencode prompt response: %w", err)
	}
	userMsgID := strings.TrimSpace(promptResp.Data.ID)
	if userMsgID == "" {
		return OpenCodeMessageResponse{}, fmt.Errorf("opencode send prompt returned an empty message id")
	}

	// 3. Wait for the session agent loop to reach idle (respects ctx cancellation).
	status, body, err = c.doRequest(
		ctx,
		http.MethodPost,
		[]string{"api", "experimental", "session", sessionID, "wait"},
		nil,
		nil,
		nil,
		maxOpenCodeControlBytes,
		"wait session",
	)
	if err != nil {
		return OpenCodeMessageResponse{}, err
	}
	if status < 200 || status > 299 {
		return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("wait session", status, body)
	}

	// 4. Fetch messages newer than userMsgID (newest-first, paginated via cursor.next).
	var turnDesc []openCodeSessionMessage
	seenCursors := make(map[string]struct{})
	cursor := ""
	reachedUser := false
	dupCursor := false
	exhausted := false

	for page := 0; page < maxOpenCodeMessagePages && !reachedUser; page++ {
		query := url.Values{}
		query.Set("limit", strconv.Itoa(openCodeMessagePageLimit))
		if cursor == "" {
			query.Set("order", "desc")
		} else {
			// OpenCode v2 forbids combining cursor with order.
			query.Set("cursor", cursor)
		}

		status, body, err = c.doRequest(
			ctx,
			http.MethodGet,
			[]string{"api", "session", sessionID, "message"},
			query,
			nil,
			nil,
			maxOpenCodeResponseBytes,
			"list messages",
		)
		if err != nil {
			return OpenCodeMessageResponse{}, err
		}
		if status < 200 || status > 299 {
			return OpenCodeMessageResponse{}, classifyOpenCodeHTTPError("list messages", status, body)
		}

		var listResp openCodeMessageListResponse
		if err := json.Unmarshal(body, &listResp); err != nil {
			return OpenCodeMessageResponse{}, fmt.Errorf("decode opencode message list: %w", err)
		}

		for _, m := range listResp.Data {
			if m.ID == userMsgID {
				reachedUser = true
				break
			}
			turnDesc = append(turnDesc, m)
		}
		if reachedUser {
			break
		}
		if listResp.Cursor.Next == nil {
			exhausted = true
			break
		}
		next := strings.TrimSpace(*listResp.Cursor.Next)
		if next == "" {
			exhausted = true
			break
		}
		if _, dup := seenCursors[next]; dup {
			dupCursor = true
			break
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}

	if !reachedUser {
		switch {
		case dupCursor:
			return OpenCodeMessageResponse{}, fmt.Errorf("opencode message pagination returned duplicate cursor before reaching user message %q", userMsgID)
		case exhausted:
			return OpenCodeMessageResponse{}, fmt.Errorf("opencode message history exhausted without finding user message %q", userMsgID)
		default:
			return OpenCodeMessageResponse{}, fmt.Errorf("opencode message pagination exceeded %d pages without finding user message %q", maxOpenCodeMessagePages, userMsgID)
		}
	}

	slices.Reverse(turnDesc)
	return assembleOpenCodeTurnResponse(turnDesc)
}

type openCodeMessageListResponse struct {
	Data   []openCodeSessionMessage `json:"data"`
	Cursor struct {
		Previous *string `json:"previous"`
		Next     *string `json:"next"`
	} `json:"cursor"`
}

type openCodeSessionMessage struct {
	ID      string                     `json:"id"`
	Type    string                     `json:"type"`
	Outcome string                     `json:"outcome,omitempty"`
	Content []openCodeAssistantContent `json:"content,omitempty"`
	Error   *openCodeStructuredError   `json:"error,omitempty"`
}

type openCodeAssistantContent struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type openCodeStructuredError struct {
	Type     string `json:"type"`
	Message  string `json:"message"`
	Status   int    `json:"status,omitempty"`
	Response *struct {
		Body string `json:"body"`
	} `json:"response,omitempty"`
}

func assembleOpenCodeTurnResponse(messages []openCodeSessionMessage) (OpenCodeMessageResponse, error) {
	if len(messages) == 0 {
		return OpenCodeMessageResponse{}, fmt.Errorf("opencode turn completed without an assistant or idle message")
	}
	var (
		textParts       []string
		lastAssistantID string
		sawAssistant    bool
		sawIdle         bool
	)
	for _, m := range messages {
		switch m.Type {
		case "assistant":
			sawAssistant = true
			if strings.TrimSpace(m.ID) != "" {
				lastAssistantID = strings.TrimSpace(m.ID)
			}
			if m.Error != nil {
				return OpenCodeMessageResponse{}, classifyOpenCodeStructuredError(m.Error)
			}
			for _, part := range m.Content {
				if part.Type == "text" {
					if trimmed := strings.TrimSpace(part.Text); trimmed != "" {
						textParts = append(textParts, trimmed)
					}
				}
			}
		case "idle":
			sawIdle = true
			switch m.Outcome {
			case "failed":
				if m.Error != nil {
					return OpenCodeMessageResponse{}, classifyOpenCodeStructuredError(m.Error)
				}
				return OpenCodeMessageResponse{}, fmt.Errorf("opencode session turn failed")
			case "interrupted":
				if m.Error != nil {
					return OpenCodeMessageResponse{}, fmt.Errorf("opencode session turn interrupted: %w", classifyOpenCodeStructuredError(m.Error))
				}
				return OpenCodeMessageResponse{}, fmt.Errorf("opencode session turn interrupted")
			}
		}
	}
	if !sawAssistant && !sawIdle {
		return OpenCodeMessageResponse{}, fmt.Errorf("opencode turn completed without an assistant or idle message")
	}
	return OpenCodeMessageResponse{
		Text:      strings.Join(textParts, "\n\n"),
		MessageID: lastAssistantID,
	}, nil
}

func hasExplicitHTTP429(lower string) bool {
	for _, marker := range []string{
		"status 429",
		"status: 429",
		"status=429",
		"http 429",
		"429 too many requests",
		`"statuscode":429`,
		`"statuscode": 429`,
		`"status_code":429`,
		`"status_code": 429`,
		`"status":429`,
		`"status": 429`,
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func classifyOpenCodeStructuredError(errObj *openCodeStructuredError) error {
	if errObj == nil {
		return fmt.Errorf("opencode message error")
	}
	errType := strings.TrimSpace(errObj.Type)
	msg := strings.TrimSpace(errObj.Message)
	var respBody string
	if errObj.Response != nil {
		respBody = strings.TrimSpace(errObj.Response.Body)
	}
	detail := msg
	if errType != "" && detail != "" {
		detail = errType + ": " + detail
	} else if errType != "" {
		detail = errType
	} else if errObj.Status >= 400 {
		detail = fmt.Sprintf("status %d", errObj.Status)
	}
	if detail == "" {
		detail = "unknown error"
	}
	lowerDetail := strings.ToLower(detail)
	lowerBody := strings.ToLower(respBody)
	isBudgetExceeded := errObj.Status == http.StatusTooManyRequests ||
		strings.Contains(lowerDetail, "budget_exceeded") ||
		strings.Contains(lowerBody, "budget_exceeded") ||
		(errObj.Status == 0 && (hasExplicitHTTP429(lowerDetail) || hasExplicitHTTP429(lowerBody)))
	if isBudgetExceeded {
		return fmt.Errorf("%w: opencode message error: %s", ErrOpenCodeBudgetExceeded, detail)
	}
	return fmt.Errorf("opencode message error: %s", detail)
}

// AbortSession invokes POST /api/session/{id}/interrupt so OpenCode v2
// immediately halts generation and kills active tools/subprocesses for that
// session. It uses a bounded non-cancelled context so abort still completes
// after turn cancellation.
func (c *HTTPOpenCodeClient) AbortSession(ctx context.Context, sessionID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID = strings.TrimSpace(sessionID)
	if err := validateOpenCodePathSegment("session id", sessionID); err != nil {
		return err
	}
	abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.abortTimeout)
	defer cancel()

	status, body, err := c.doRequest(
		abortCtx,
		http.MethodPost,
		[]string{"api", "session", sessionID, "interrupt"},
		nil,
		nil,
		nil,
		maxOpenCodeControlBytes,
		"abort session",
	)
	if err != nil {
		return err
	}
	if status >= 200 && status <= 299 {
		return nil
	}
	return classifyOpenCodeHTTPError("abort session", status, body)
}

// AttachMCPServer attaches a remote MCP server (such as an ephemeral per-task
// `kodema sandbox serve` instance) via OpenCode v2 PUT /api/experimental/mcp/{server}.
func (c *HTTPOpenCodeClient) AttachMCPServer(ctx context.Context, sessionID string, cfg OpenCodeMCPServerConfig) error {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID = strings.TrimSpace(sessionID)
	if err := validateOpenCodePathSegment("session id", sessionID); err != nil {
		return err
	}
	name := strings.TrimSpace(cfg.Name)
	if err := validateOpenCodePathSegment("MCP server name", name); err != nil {
		return err
	}
	serverURL := strings.TrimSpace(cfg.URL)
	if serverURL == "" {
		return fmt.Errorf("opencode MCP server URL is required")
	}
	serverType := strings.TrimSpace(cfg.Type)
	if serverType == "" {
		serverType = "remote"
	}

	mcpConfig := map[string]any{
		"type":     serverType,
		"url":      serverURL,
		"disabled": !cfg.Enabled,
		"oauth":    false,
	}
	if len(cfg.Headers) > 0 {
		mcpConfig["headers"] = cfg.Headers
	}
	payload := map[string]any{
		"config": mcpConfig,
	}

	var query url.Values
	dir := normalizeOpenCodeMCPDir(cfg.Directory)
	if dir != "" {
		query = url.Values{}
		query.Set("location[directory]", dir)
	}

	attKey := mcpAttachmentKey{dir: dir, name: name}
	c.mcpMu.Lock()
	if owner, exists := c.mcpOwners[attKey]; exists && owner != sessionID {
		c.mcpMu.Unlock()
		return fmt.Errorf("opencode v2 MCP server %q is already attached to session %q in location %q; concurrent Brain-mode sandbox turns on a shared OpenCode v2 server require --max-concurrency 1", name, owner, dir)
	}
	if owner, exists := c.mcpPending[attKey]; exists && owner != sessionID {
		c.mcpMu.Unlock()
		return fmt.Errorf("opencode v2 MCP server %q is already attached to session %q in location %q; concurrent Brain-mode sandbox turns on a shared OpenCode v2 server require --max-concurrency 1", name, owner, dir)
	}
	c.mcpPending[attKey] = sessionID
	c.mcpMu.Unlock()

	defer func() {
		c.mcpMu.Lock()
		if c.mcpPending[attKey] == sessionID {
			delete(c.mcpPending, attKey)
		}
		c.mcpMu.Unlock()
	}()

	status, body, err := c.doRequest(
		ctx,
		http.MethodPut,
		[]string{"api", "experimental", "mcp", name},
		query,
		payload,
		nil,
		maxOpenCodeControlBytes,
		"attach MCP server",
	)
	if err != nil {
		return err
	}
	if status >= 200 && status <= 299 {
		sessKey := mcpSessionServerKey{sessionID: sessionID, name: name}
		c.mcpMu.Lock()
		if prevDir, hadPrev := c.mcpBySession[sessKey]; hadPrev && prevDir != dir {
			prevKey := mcpAttachmentKey{dir: prevDir, name: name}
			if c.mcpOwners[prevKey] == sessionID {
				delete(c.mcpOwners, prevKey)
			}
		}
		c.mcpOwners[attKey] = sessionID
		c.mcpBySession[sessKey] = dir
		c.mcpMu.Unlock()
		return nil
	}
	return classifyOpenCodeHTTPError("attach MCP server", status, body)
}

// DetachMCPServer removes a previously attached ephemeral MCP server via
// OpenCode v2 DELETE /api/experimental/mcp/{server}. Uses a bounded
// non-cancelled context so cleanup still completes after turn cancellation.
func (c *HTTPOpenCodeClient) DetachMCPServer(ctx context.Context, sessionID string, serverName string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID = strings.TrimSpace(sessionID)
	serverName = strings.TrimSpace(serverName)
	if err := validateOpenCodePathSegment("session id", sessionID); err != nil {
		return err
	}
	if err := validateOpenCodePathSegment("MCP server name", serverName); err != nil {
		return err
	}
	detachCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.abortTimeout)
	defer cancel()

	sessKey := mcpSessionServerKey{sessionID: sessionID, name: serverName}
	c.mcpMu.Lock()
	dir, ownedBySelf := c.mcpBySession[sessKey]
	if !ownedBySelf {
		for k, owner := range c.mcpOwners {
			if k.name == serverName && owner != sessionID {
				c.mcpMu.Unlock()
				return fmt.Errorf("opencode v2 MCP server %q is attached to session %q in location %q; refusing detach from session %q", serverName, owner, k.dir, sessionID)
			}
		}
		for k, owner := range c.mcpPending {
			if k.name == serverName && owner != sessionID {
				c.mcpMu.Unlock()
				return fmt.Errorf("opencode v2 MCP server %q is attached to session %q in location %q; refusing detach from session %q", serverName, owner, k.dir, sessionID)
			}
		}
	}
	c.mcpMu.Unlock()

	var query url.Values
	if dir != "" {
		query = url.Values{}
		query.Set("location[directory]", dir)
	}

	status, body, err := c.doRequest(
		detachCtx,
		http.MethodDelete,
		[]string{"api", "experimental", "mcp", serverName},
		query,
		nil,
		nil,
		maxOpenCodeControlBytes,
		"detach MCP server",
	)
	if err != nil {
		return err
	}
	if (status >= 200 && status <= 299) || status == http.StatusNotFound {
		c.mcpMu.Lock()
		if ownedDir, ok := c.mcpBySession[sessKey]; ok && ownedDir == dir {
			delete(c.mcpBySession, sessKey)
			attKey := mcpAttachmentKey{dir: dir, name: serverName}
			if c.mcpOwners[attKey] == sessionID {
				delete(c.mcpOwners, attKey)
			}
		}
		c.mcpMu.Unlock()
		return nil
	}
	return classifyOpenCodeHTTPError("detach MCP server", status, body)
}

func (c *HTTPOpenCodeClient) doRequest(
	ctx context.Context,
	method string,
	segments []string,
	query url.Values,
	payload any,
	extraHeaders map[string]string,
	maxBytes int64,
	op string,
) (int, []byte, error) {
	var bodyReader io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, fmt.Errorf("encode opencode %s request: %w", op, err)
		}
		bodyReader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpointSegments(query, segments...), bodyReader)
	if err != nil {
		return 0, nil, fmt.Errorf("build opencode %s request: %w", op, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	if err := c.applyAuth(req); err != nil {
		return 0, nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("opencode %s: %w", op, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return 0, nil, fmt.Errorf("read opencode %s response: %w", op, err)
	}
	if int64(len(body)) > maxBytes {
		return 0, nil, fmt.Errorf("opencode %s response exceeds bound", op)
	}
	return resp.StatusCode, body, nil
}

func (c *HTTPOpenCodeClient) applyAuth(req *http.Request) error {
	if c.passwordFile != "" {
		pw, err := readOpenCodePasswordFile(c.passwordFile)
		if err != nil {
			return err
		}
		req.SetBasicAuth(openCodeBasicAuthUsername, pw)
		return nil
	}
	if c.password != "" {
		req.SetBasicAuth(openCodeBasicAuthUsername, c.password)
	}
	return nil
}

func (c *HTTPOpenCodeClient) endpointSegments(query url.Values, segments ...string) string {
	basePath := strings.TrimRight(c.baseURL.Path, "/")
	baseRawPath := strings.TrimRight(c.baseURL.EscapedPath(), "/")
	decodedParts := make([]string, len(segments))
	escapedParts := make([]string, len(segments))
	for i, seg := range segments {
		decodedParts[i] = seg
		escapedParts[i] = url.PathEscape(seg)
	}
	decodedPath := basePath
	rawPath := baseRawPath
	if len(segments) > 0 {
		decodedPath += "/" + strings.Join(decodedParts, "/")
		rawPath += "/" + strings.Join(escapedParts, "/")
	}
	target := *c.baseURL
	target.Path = decodedPath
	if rawPath != decodedPath {
		target.RawPath = rawPath
	} else {
		target.RawPath = ""
	}
	if len(query) > 0 {
		target.RawQuery = query.Encode()
	} else {
		target.RawQuery = ""
	}
	return target.String()
}

func classifyOpenCodeHTTPError(op string, status int, body []byte) error {
	snippet := truncateString(strings.TrimSpace(string(body)), 512)
	lower := strings.ToLower(snippet)
	if status == http.StatusTooManyRequests || strings.Contains(lower, "budget_exceeded") {
		return fmt.Errorf("%w: opencode %s returned %d: %s", ErrOpenCodeBudgetExceeded, op, status, snippet)
	}
	return fmt.Errorf("opencode %s returned status %d: %s", op, status, snippet)
}
