package sandbox

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	mcpProtocolVersion = "2025-03-26"
	mcpServerName      = "kodema-sandbox"
	mcpServerVersion   = "1"
)

// Server is the authenticated, /workspace-jailed Streamable HTTP MCP JSON-RPC 2.0
// server run by `kodema sandbox serve`.
type Server struct {
	cfg  Config
	jail *WorkspaceJail

	mu         sync.Mutex
	httpServer *http.Server
	runCtx     context.Context
	cancelRun  context.CancelFunc
	started    bool
	closed     bool

	pgidMu      sync.Mutex
	activePGIDs map[int]struct{}
}

// NewServer validates config, canonicalizes the workspace jail, and builds an
// unbound sandbox MCP Server.
func NewServer(cfg Config) (*Server, error) {
	normalized, err := cfg.ValidateAndNormalize()
	if err != nil {
		return nil, err
	}
	jail, err := NewWorkspaceJail(normalized.WorkspaceDir)
	if err != nil {
		return nil, err
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	s := &Server{
		cfg:         normalized,
		jail:        jail,
		runCtx:      runCtx,
		cancelRun:   cancelRun,
		activePGIDs: make(map[int]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/mcp", s.handleMCP)
	s.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       60 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return s.runCtx
		},
	}
	return s, nil
}

// WorkspaceRoot returns the canonicalized jailed workspace root directory.
func (s *Server) WorkspaceRoot() string {
	return s.jail.Root()
}

// Start binds the configured TCP listener and begins serving in the background.
// It returns the bound base URL (e.g. "http://127.0.0.1:45123").
func (s *Server) Start(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ln, err := s.bindListener()
	if err != nil {
		return "", err
	}

	go func() {
		select {
		case <-ctx.Done():
			_ = s.Shutdown(context.Background())
		case <-s.runCtx.Done():
		}
	}()
	go func() {
		_ = s.httpServer.Serve(ln)
	}()
	return "http://" + ln.Addr().String(), nil
}

// Serve binds the listener, serves until ctx is cancelled, and then performs a
// clean shutdown (killing all active child process groups).
func (s *Server) Serve(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ln, err := s.bindListener()
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		serveErr := s.httpServer.Serve(ln)
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		errCh <- serveErr
	}()

	select {
	case <-ctx.Done():
		_ = s.Shutdown(context.Background())
		return <-errCh
	case serveErr := <-errCh:
		_ = s.Shutdown(context.Background())
		return serveErr
	}
}

func (s *Server) bindListener() (net.Listener, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("sandbox server already closed")
	}
	if s.started {
		s.mu.Unlock()
		return nil, errors.New("sandbox server already started")
	}
	s.started = true
	s.mu.Unlock()

	ln, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return nil, fmt.Errorf("listen sandbox MCP server on %q: %w", s.cfg.ListenAddr, err)
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = ln.Close()
		return nil, errors.New("sandbox server already closed")
	}
	s.mu.Unlock()
	return ln, nil
}

// Shutdown stops accepting new calls, cancels all in-flight request contexts,
// kills every active child process group, and shuts down the HTTP server cleanly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancelRun()
	srv := s.httpServer
	s.mu.Unlock()

	s.killAllActivePGIDs()

	if ctx == nil {
		ctx = context.Background()
	}
	graceCtx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownGrace)
	defer cancel()
	err := srv.Shutdown(graceCtx)
	// Final sweep in case a process spawned right before Shutdown closed listeners.
	s.killAllActivePGIDs()
	if closeErr := s.jail.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *Server) registerPGID(pgid int) {
	if pgid <= 0 {
		return
	}
	s.pgidMu.Lock()
	s.activePGIDs[pgid] = struct{}{}
	s.pgidMu.Unlock()
}

func (s *Server) unregisterPGID(pgid int) {
	if pgid <= 0 {
		return
	}
	s.pgidMu.Lock()
	delete(s.activePGIDs, pgid)
	s.pgidMu.Unlock()
}

func (s *Server) killAllActivePGIDs() {
	s.pgidMu.Lock()
	pgids := make([]int, 0, len(s.activePGIDs))
	for pgid := range s.activePGIDs {
		pgids = append(pgids, pgid)
	}
	s.pgidMu.Unlock()
	for _, pgid := range pgids {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) authenticate(header http.Header) bool {
	values := header.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	raw := strings.TrimSpace(values[0])
	const prefix = "Bearer "
	if len(raw) <= len(prefix) || !strings.EqualFold(raw[:len(prefix)], prefix) {
		return false
	}
	presented := strings.TrimSpace(raw[len(prefix):])
	if presented == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(s.cfg.Token)) == 1
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      any           `json:"id"`
	Result  any           `json:"result,omitempty"`
	Error   *jsonRPCError `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	// Strict auth FIRST on every /mcp request before touching body or tools.
	if !s.authenticate(r.Header) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="kodema-sandbox"`)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      nil,
				Error:   &jsonRPCError{Code: -32600, Message: "request body exceeds maximum size"},
			})
			return
		}
		writeJSON(w, http.StatusBadRequest, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      nil,
			Error:   &jsonRPCError{Code: -32700, Message: "failed to read request body"},
		})
		return
	}

	var req jsonRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      nil,
			Error:   &jsonRPCError{Code: -32700, Message: "parse error: invalid JSON"},
		})
		return
	}
	if req.JSONRPC != "2.0" || strings.TrimSpace(req.Method) == "" {
		writeJSON(w, http.StatusBadRequest, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      decodeRPCID(req.ID),
			Error:   &jsonRPCError{Code: -32600, Message: "invalid JSON-RPC 2.0 request"},
		})
		return
	}

	// JSON-RPC notification (no id) such as notifications/initialized.
	if len(req.ID) == 0 || string(req.ID) == "null" {
		if req.Method == "notifications/initialized" || strings.HasPrefix(req.Method, "notifications/") {
			w.WriteHeader(http.StatusAccepted)
			return
		}
	}

	idVal := decodeRPCID(req.ID)
	switch req.Method {
	case "initialize":
		writeJSON(w, http.StatusOK, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      idVal,
			Result: map[string]any{
				"protocolVersion": mcpProtocolVersion,
				"capabilities": map[string]any{
					"tools": map[string]any{"listChanged": false},
				},
				"serverInfo": map[string]any{
					"name":    mcpServerName,
					"version": mcpServerVersion,
				},
			},
		})
	case "notifications/initialized":
		w.WriteHeader(http.StatusAccepted)
	case "ping":
		writeJSON(w, http.StatusOK, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      idVal,
			Result:  map[string]any{},
		})
	case "tools/list":
		writeJSON(w, http.StatusOK, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      idVal,
			Result: map[string]any{
				"tools": ToolDefinitions(),
			},
		})
	case "tools/call":
		var params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil || strings.TrimSpace(params.Name) == "" {
			writeJSON(w, http.StatusOK, jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      idVal,
				Error:   &jsonRPCError{Code: -32602, Message: "invalid params: tool name is required"},
			})
			return
		}
		res, err := s.callTool(r.Context(), params.Name, params.Arguments)
		if err != nil {
			writeJSON(w, http.StatusOK, jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      idVal,
				Error:   &jsonRPCError{Code: -32602, Message: err.Error()},
			})
			return
		}
		writeJSON(w, http.StatusOK, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      idVal,
			Result:  res,
		})
	default:
		writeJSON(w, http.StatusOK, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      idVal,
			Error:   &jsonRPCError{Code: -32601, Message: fmt.Sprintf("method %q not found", req.Method)},
		})
	}
}

func decodeRPCID(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal marshal error"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}
