package opencode

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/id"
)

// OpenCodeLLMProxyConfig configures the local loopback attribution reverse
// proxy that bridges OpenCode's LLM requests to `$KODEMA_URL/llm/{provider}/*`,
// injecting the live `kodsvc.` credential plus `X-Kodema-Execution` and
// `X-Kodema-Action`, and cancelling in-flight upstream requests immediately
// when the active turn is cancelled.
type OpenCodeLLMProxyConfig struct {
	ListenAddr   string
	KodemaURL    string
	Credentials  client.CredentialSource
	LocalAPIKey  string
	RoundTripper http.RoundTripper
	Clock        servicekit.Clock
}

type proxyTurnBinding struct {
	executionID string
	actionID    string
	ctx         context.Context
}

// OpenCodeLLMProxy is a loopback-only HTTP reverse proxy that attaches Kodema
// service credentials and execution/action attribution headers to OpenCode's
// LLM requests.
type OpenCodeLLMProxy struct {
	defaultListenAddr string
	kodemaBase        *url.URL
	creds             client.CredentialSource
	localAPIKey       string
	client            *http.Client
	clock             servicekit.Clock

	server *http.Server

	mu    sync.RWMutex
	turns map[string]*proxyTurnBinding // "exec/action" -> binding
}

var errProxyRedirectRefused = errors.New("opencode llm proxy refuses redirects")

// defaultNoProxyTransport clones http.DefaultTransport with Proxy explicitly
// disabled so ambient HTTP_PROXY/HTTPS_PROXY environment variables are never
// inherited by local loopback or Kodema service HTTP clients.
func defaultNoProxyTransport() *http.Transport {
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		tr := base.Clone()
		tr.Proxy = nil
		return tr
	}
	return &http.Transport{Proxy: nil}
}

// NewOpenCodeLLMProxy validates configuration and creates an unbound proxy.
// Call Start to begin listening on a loopback socket.
func NewOpenCodeLLMProxy(config OpenCodeLLMProxyConfig) (*OpenCodeLLMProxy, error) {
	rawBase := strings.TrimSpace(config.KodemaURL)
	if rawBase == "" {
		return nil, fmt.Errorf("opencode LLM proxy requires KodemaURL")
	}
	parsed, err := url.Parse(rawBase)
	if err != nil {
		return nil, fmt.Errorf("parse KodemaURL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return nil, fmt.Errorf("opencode LLM proxy KodemaURL must be an http(s) URL")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("opencode LLM proxy KodemaURL must not embed credentials")
	}
	if config.Credentials == nil {
		return nil, fmt.Errorf("opencode LLM proxy requires a credential source")
	}
	localKey := strings.TrimSpace(config.LocalAPIKey)
	if localKey == "" {
		var raw [32]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, fmt.Errorf("generate opencode LLM proxy local API key: %w", err)
		}
		localKey = "ocproxy." + hex.EncodeToString(raw[:])
	}
	rt := config.RoundTripper
	if rt == nil {
		rt = defaultNoProxyTransport()
	}
	p := &OpenCodeLLMProxy{
		defaultListenAddr: strings.TrimSpace(config.ListenAddr),
		kodemaBase:        parsed,
		creds:             config.Credentials,
		localAPIKey:       localKey,
		client: &http.Client{
			Transport: rt,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errProxyRedirectRefused
			},
		},
		clock: config.Clock,
		turns: make(map[string]*proxyTurnBinding),
	}
	p.server = &http.Server{
		Handler:           p,
		ReadHeaderTimeout: 15 * time.Second,
	}
	return p, nil
}

// LocalAPIKey returns the shared secret required on inbound loopback requests
// via `Authorization: Bearer <key>` or `x-api-key: <key>`.
func (p *OpenCodeLLMProxy) LocalAPIKey() string {
	if p == nil {
		return ""
	}
	return p.localAPIKey
}

// Start binds a loopback listener (requiring loopback host and verified
// loopback bound IP) and serves traffic in the background. Call Close to stop
// cleanly.
func (p *OpenCodeLLMProxy) Start(listenAddr string) (string, error) {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		listenAddr = p.defaultListenAddr
	}
	if listenAddr == "" {
		listenAddr = "127.0.0.1:0"
	}
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("invalid opencode LLM proxy listen address %q: %w", listenAddr, err)
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return "", fmt.Errorf("opencode LLM proxy must listen on loopback only, got %q", host)
		}
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return "", fmt.Errorf("listen opencode LLM proxy: %w", err)
	}
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || tcpAddr.IP == nil || !tcpAddr.IP.IsLoopback() {
		_ = ln.Close()
		return "", fmt.Errorf("opencode LLM proxy bound non-loopback address %q", ln.Addr().String())
	}
	go func() {
		_ = p.server.Serve(ln)
	}()
	return "http://" + ln.Addr().String(), nil
}

// Close shuts down the loopback reverse proxy cleanly.
func (p *OpenCodeLLMProxy) Close(ctx context.Context) error {
	if p == nil || p.server == nil {
		return nil
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	return p.server.Shutdown(ctx)
}

// RegisterTurn binds an active turn's attribution and cancellation context.
// The returned unregister func removes the binding when the turn finishes.
func (p *OpenCodeLLMProxy) RegisterTurn(turnCtx context.Context, executionID, actionID string) func() {
	if p == nil {
		return func() {}
	}
	key := executionID + "/" + actionID
	binding := &proxyTurnBinding{
		executionID: executionID,
		actionID:    actionID,
		ctx:         turnCtx,
	}
	p.mu.Lock()
	p.turns[key] = binding
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		if p.turns[key] == binding {
			delete(p.turns, key)
		}
		p.mu.Unlock()
	}
}

// ServeHTTP authenticates inbound loopback callers with the local shared
// secret, validates the path strictly against traversal, resolves attribution
// (failing closed on ambiguous multi-turn unattributed calls), and forwards to
// Kodema's `/llm/{provider}/*` endpoint with live `kodsvc.` auth and
// attribution headers.
func (p *OpenCodeLLMProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !p.authenticateLocalCaller(r.Header) {
		writeProxyJSONError(w, http.StatusUnauthorized, api.CodeUnauthenticated, "missing or invalid local LLM proxy API key")
		return
	}

	binding, upstreamPath, status, code, msg := p.resolveTurnAndPath(r)
	if status != 0 {
		writeProxyJSONError(w, status, code, msg)
		return
	}
	if binding.ctx.Err() != nil {
		writeProxyJSONError(w, http.StatusRequestTimeout, api.CodeDeadlineExpired, "attributed Kodema turn is no longer active")
		return
	}

	now := time.Now()
	if p.clock != nil {
		now = p.clock.Now()
	}
	bearer, err := client.Bearer(r.Context(), p.creds, now)
	if err != nil {
		writeProxyJSONError(w, http.StatusUnauthorized, api.CodeUnauthenticated, "service credential is unavailable")
		return
	}

	// Bind outbound request lifecycle to BOTH caller request context and active
	// turn cancellation context so turn cancel/timeout aborts in-flight calls!
	outCtx, cancelOut := context.WithCancel(r.Context())
	defer cancelOut()
	stopTurnCancel := context.AfterFunc(binding.ctx, cancelOut)
	defer stopTurnCancel()

	target := *p.kodemaBase
	target.Path = strings.TrimRight(p.kodemaBase.Path, "/") + upstreamPath
	target.RawPath = ""
	target.RawQuery = r.URL.RawQuery

	outReq, err := http.NewRequestWithContext(outCtx, r.Method, target.String(), r.Body)
	if err != nil {
		writeProxyJSONError(w, http.StatusInternalServerError, api.CodeInternalFailure, "failed to construct upstream LLM request")
		return
	}
	reqConnHeaders := parseConnectionTokens(r.Header)
	for name, values := range r.Header {
		lower := strings.ToLower(name)
		if isStrippedProxyRequestHeader(lower) || reqConnHeaders[lower] {
			continue
		}
		for _, v := range values {
			outReq.Header.Add(name, v)
		}
	}
	outReq.Header.Set("Authorization", bearer)
	outReq.Header.Set("X-Kodema-Execution", binding.executionID)
	outReq.Header.Set("X-Kodema-Action", binding.actionID)
	outReq.ContentLength = r.ContentLength

	resp, err := p.client.Do(outReq)
	if err != nil {
		if binding.ctx.Err() != nil || outCtx.Err() != nil {
			writeProxyJSONError(w, http.StatusRequestTimeout, api.CodeDeadlineExpired, "attributed LLM request cancelled")
			return
		}
		writeProxyJSONError(w, http.StatusServiceUnavailable, api.CodeDependencyUnavailable, "Kodema LLM proxy is unreachable")
		return
	}
	defer func() { _ = resp.Body.Close() }()

	respConnHeaders := parseConnectionTokens(resp.Header)
	for name, values := range resp.Header {
		lower := strings.ToLower(name)
		if isStrippedProxyResponseHeader(lower) || respConnHeaders[lower] {
			continue
		}
		for _, v := range values {
			w.Header().Add(name, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	flusher, canFlush := w.(http.Flusher)
	if canFlush {
		flusher.Flush()
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if binding.ctx.Err() != nil {
				panic(http.ErrAbortHandler)
			}
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr == io.EOF {
			if binding.ctx.Err() != nil {
				panic(http.ErrAbortHandler)
			}
			return
		}
		if readErr != nil {
			panic(http.ErrAbortHandler)
		}
	}
}

func (p *OpenCodeLLMProxy) authenticateLocalCaller(header http.Header) bool {
	if p == nil || p.localAPIKey == "" {
		return false
	}
	var bearer string
	for _, value := range header.Values("Authorization") {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if len(trimmed) < len("Bearer ") || !strings.EqualFold(trimmed[:len("Bearer ")], "Bearer ") {
			return false
		}
		tok := strings.TrimSpace(trimmed[len("Bearer "):])
		if tok == "" {
			return false
		}
		if bearer != "" && bearer != tok {
			return false
		}
		bearer = tok
	}

	var apiKey string
	for _, value := range header.Values("X-Api-Key") {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if apiKey != "" && apiKey != trimmed {
			return false
		}
		apiKey = trimmed
	}

	var presented string
	switch {
	case bearer != "" && apiKey != "":
		if bearer != apiKey {
			return false
		}
		presented = bearer
	case bearer != "":
		presented = bearer
	case apiKey != "":
		presented = apiKey
	default:
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(p.localAPIKey)) == 1
}

func (p *OpenCodeLLMProxy) resolveTurnAndPath(r *http.Request) (*proxyTurnBinding, string, int, string, string) {
	if r == nil || r.URL == nil {
		return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "invalid LLM proxy request URL"
	}
	if r.URL.Fragment != "" {
		return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "fragments are not permitted on LLM proxy paths"
	}
	if strings.ContainsAny(r.URL.RawQuery, "\r\n\x00") {
		return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "query string contains invalid control characters"
	}
	for _, encoded := range []string{r.URL.EscapedPath(), r.URL.RawPath} {
		lower := strings.ToLower(encoded)
		if strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") || strings.Contains(lower, "%00") {
			return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "LLM proxy path contains an encoded path separator or NUL byte"
		}
	}

	path := r.URL.Path
	if r.URL.RawPath != "" && hasUnsafeProxyPath(r.URL.RawPath) {
		return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "LLM proxy raw path contains invalid characters or relative segments"
	}
	if hasUnsafeProxyPath(path) {
		return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "LLM proxy path contains invalid characters or relative segments"
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// Explicit path-scoped routing: /turn/{exec}/{action}/llm/{provider}/...
	if strings.HasPrefix(path, "/turn/") {
		rest := strings.TrimPrefix(path, "/turn/")
		parts := strings.SplitN(rest, "/", 3)
		if len(parts) != 3 {
			return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "invalid turn-scoped LLM proxy path"
		}
		execID, actID, llmPath := parts[0], parts[1], "/"+parts[2]
		if _, err := id.ParseExecutionID(execID); err != nil {
			return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "invalid execution id in turn-scoped LLM proxy path"
		}
		if _, err := id.ParseActionID(actID); err != nil {
			return nil, "", http.StatusBadRequest, api.CodeInvalidInput, "invalid action id in turn-scoped LLM proxy path"
		}
		if err := validateOpenCodeProxyLLMPath(llmPath); err != nil {
			return nil, "", http.StatusBadRequest, api.CodeInvalidInput, err.Error()
		}
		binding, ok := p.turns[execID+"/"+actID]
		if !ok {
			return nil, "", http.StatusForbidden, api.CodeForbidden, "no active attributed Kodema turn for LLM proxy request"
		}
		return binding, llmPath, 0, "", ""
	}

	if err := validateOpenCodeProxyLLMPath(path); err != nil {
		return nil, "", http.StatusBadRequest, api.CodeInvalidInput, err.Error()
	}

	// Explicit header attribution override if provided (partial or conflicting
	// attribution headers always fail closed).
	execVals := r.Header.Values("X-Kodema-Execution")
	actVals := r.Header.Values("X-Kodema-Action")
	if len(execVals) > 0 || len(actVals) > 0 {
		execHdr, okExec := singleTrimmedProxyHeader(execVals)
		actHdr, okAct := singleTrimmedProxyHeader(actVals)
		if !okExec || !okAct {
			return nil, "", http.StatusForbidden, api.CodeForbidden, "explicit LLM proxy attribution requires single X-Kodema-Execution and X-Kodema-Action headers"
		}
		if binding, ok := p.turns[execHdr+"/"+actHdr]; ok {
			return binding, path, 0, "", ""
		}
		return nil, "", http.StatusForbidden, api.CodeForbidden, "no active attributed Kodema turn matches attribution headers"
	}

	// Implicit attribution is permitted ONLY when exactly one turn is active.
	// With zero active turns -> 403; with >1 concurrent turns -> fail closed
	// with 409 Conflict to eliminate cross-task misattribution & cancel bleed.
	switch len(p.turns) {
	case 0:
		return nil, "", http.StatusForbidden, api.CodeForbidden, "no active attributed Kodema turn for LLM proxy request"
	case 1:
		for _, binding := range p.turns {
			return binding, path, 0, "", ""
		}
	default:
		return nil, "", http.StatusConflict, api.CodeVersionConflict, "ambiguous LLM proxy request: multiple active Kodema turns require explicit turn routing or attribution headers"
	}
	return nil, "", http.StatusForbidden, api.CodeForbidden, "no active attributed Kodema turn for LLM proxy request"
}

func hasUnsafeProxyPath(rawPath string) bool {
	if rawPath == "" || rawPath[0] != '/' {
		return true
	}
	for i := 0; i < len(rawPath); i++ {
		ch := rawPath[i]
		if ch < 0x20 || ch == 0x7f || ch == '\\' || ch == '@' {
			return true
		}
	}
	for _, seg := range strings.Split(rawPath[1:], "/") {
		if seg == "" || seg == "." || seg == ".." {
			return true
		}
	}
	return false
}

// validateOpenCodeProxyLLMPath mirrors internal/shell/app.parseLLMProxyPath so
// path traversal, empty/relative segments, control characters, backslashes, and
// userinfo markers are refused before any upstream request is constructed.
func validateOpenCodeProxyLLMPath(rawPath string) error {
	if !strings.HasPrefix(rawPath, "/llm/") {
		return fmt.Errorf("LLM proxy path must start with /llm/")
	}
	rest := strings.TrimPrefix(rawPath, "/llm/")
	provider, upstreamRel, ok := strings.Cut(rest, "/")
	if !ok || provider == "" || upstreamRel == "" {
		return fmt.Errorf("LLM proxy path must include provider and upstream path")
	}
	if _, err := id.ParseActionName(provider); err != nil {
		return fmt.Errorf("LLM proxy provider name %q is invalid", provider)
	}
	for i := 0; i < len(upstreamRel); i++ {
		ch := upstreamRel[i]
		if ch < 0x20 || ch == 0x7f || ch == '\\' || ch == '@' {
			return fmt.Errorf("LLM proxy upstream path contains invalid characters")
		}
	}
	for _, segment := range strings.Split(upstreamRel, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("LLM proxy upstream path contains an empty or relative segment")
		}
	}
	return nil
}

func singleTrimmedProxyHeader(values []string) (string, bool) {
	var found string
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return "", false
		}
		if found != "" {
			return "", false
		}
		found = trimmed
	}
	return found, found != ""
}

func parseConnectionTokens(header http.Header) map[string]bool {
	var tokens map[string]bool
	for _, value := range header.Values("Connection") {
		for _, part := range strings.Split(value, ",") {
			token := strings.ToLower(strings.TrimSpace(part))
			if token == "" {
				continue
			}
			if tokens == nil {
				tokens = make(map[string]bool)
			}
			tokens[token] = true
		}
	}
	return tokens
}

func isStrippedProxyRequestHeader(lower string) bool {
	switch lower {
	case "authorization",
		"x-api-key",
		"x-kodema-execution",
		"x-kodema-action",
		"host",
		"cookie",
		"proxy-authorization",
		"proxy-authenticate",
		"proxy-connection",
		"connection",
		"keep-alive",
		"transfer-encoding",
		"upgrade",
		"te",
		"trailer":
		return true
	default:
		return false
	}
}

func isStrippedProxyResponseHeader(lower string) bool {
	switch lower {
	case "connection",
		"keep-alive",
		"transfer-encoding",
		"upgrade",
		"te",
		"trailer",
		"proxy-connection",
		"proxy-authenticate",
		"proxy-authorization",
		"content-length":
		return true
	default:
		return false
	}
}

func writeProxyJSONError(w http.ResponseWriter, status int, code, message string) {
	body, _ := json.Marshal(api.Error{Code: code, Message: message})
	w.Header().Set("Content-Type", api.MediaType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
