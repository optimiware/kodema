package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
)

// Client is a lightweight Streamable HTTP MCP client for calling a running
// `kodema sandbox serve` instance over POST /mcp with Bearer auth.
type Client struct {
	endpointURL string
	healthURL   string
	token       string
	httpClient  *http.Client
	nextID      atomic.Int64
}

// NewClient creates a sandbox MCP client targeting endpointURL (e.g.
// "http://127.0.0.1:8090/mcp" or base URL "http://127.0.0.1:8090").
func NewClient(endpointURL, bearerToken string, rt http.RoundTripper) (*Client, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(endpointURL), "/")
	if trimmed == "" {
		return nil, errors.New("sandbox MCP endpoint URL is required")
	}
	if strings.TrimSpace(bearerToken) == "" {
		return nil, errors.New("sandbox MCP bearer token is required")
	}
	var mcpURL, healthURL string
	if strings.HasSuffix(trimmed, "/mcp") {
		mcpURL = trimmed
		healthURL = strings.TrimSuffix(trimmed, "/mcp") + "/healthz"
	} else {
		mcpURL = trimmed + "/mcp"
		healthURL = trimmed + "/healthz"
	}
	if rt == nil {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			tr := base.Clone()
			tr.Proxy = nil
			rt = tr
		} else {
			rt = &http.Transport{Proxy: nil}
		}
	}
	return &Client{
		endpointURL: mcpURL,
		healthURL:   healthURL,
		token:       bearerToken,
		httpClient: &http.Client{
			Transport: rt,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("sandbox client refuses redirects")
			},
		},
	}, nil
}

// Health calls GET /healthz on the sandbox server.
func (c *Client) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sandbox healthz returned status %d", resp.StatusCode)
	}
	return nil
}

// Initialize performs the MCP initialize handshake.
func (c *Client) Initialize(ctx context.Context) (map[string]any, error) {
	var out map[string]any
	err := c.rpc(ctx, "initialize", map[string]any{
		"protocolVersion": mcpProtocolVersion,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "kodema-opencode", "version": "1"},
	}, &out)
	return out, err
}

// ListTools calls MCP tools/list.
func (c *Client) ListTools(ctx context.Context) ([]ToolDefinition, error) {
	var out struct {
		Tools []ToolDefinition `json:"tools"`
	}
	if err := c.rpc(ctx, "tools/list", map[string]any{}, &out); err != nil {
		return nil, err
	}
	return out.Tools, nil
}

// CallTool invokes one MCP tool via tools/call and returns its ToolCallResult.
func (c *Client) CallTool(ctx context.Context, name string, arguments map[string]any) (ToolCallResult, error) {
	var out ToolCallResult
	err := c.rpc(ctx, "tools/call", map[string]any{
		"name":      name,
		"arguments": arguments,
	}, &out)
	return out, err
}

// FirstText returns the first text block of a ToolCallResult.
func (r ToolCallResult) FirstText() string {
	for _, c := range r.Content {
		if c.Type == "text" {
			return c.Text
		}
	}
	return ""
}

func (c *Client) rpc(ctx context.Context, method string, params any, dst any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	id := c.nextID.Add(1)
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
		"params":  params,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpointURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sandbox MCP %s returned HTTP %d: %s", method, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result"`
		Error   *jsonRPCError   `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode sandbox MCP %s response: %w", method, err)
	}
	if envelope.Error != nil {
		return fmt.Errorf("sandbox MCP %s error %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	if dst != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, dst); err != nil {
			return fmt.Errorf("decode sandbox MCP %s result: %w", method, err)
		}
	}
	return nil
}
