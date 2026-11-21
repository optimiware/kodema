package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
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
	RoundTripper http.RoundTripper
	Clock        Clock
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
	kodemaBase *url.URL
	creds      client.CredentialSource
	client     *http.Client
	clock      Clock

	listener net.Listener
	server   *http.Server

	mu            sync.RWMutex
	turns         map[string]*proxyTurnBinding // "exec/action" -> binding
	defaultKey    string
	defaultTurnID int
}

var errProxyRedirectRefused = errors.New("opencode llm proxy refuses redirects")

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
	rt := config.RoundTripper
	if rt == nil {
		rt = http.DefaultTransport
	}
	p := &OpenCodeLLMProxy{
		kodemaBase: parsed,
		creds:      config.Credentials,
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

// Start binds a loopback listener (requiring loopback host) and serves traffic
// in the background. Call Close to stop cleanly.
func (p *OpenCodeLLMProxy) Start(listenAddr string) (string, error) {
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
	p.mu.Lock()
	p.listener = ln
	p.mu.Unlock()
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
	p.defaultKey = key
	p.mu.Unlock()
	return func() {
		p.mu.Lock()
		if p.turns[key] == binding {
			delete(p.turns, key)
			if p.defaultKey == key {
				p.defaultKey = ""
				for otherKey := range p.turns {
					p.defaultKey = otherKey
					break
				}
			}
		}
		p.mu.Unlock()
	}
}

// ServeHTTP forwards either `/turn/{exec}/{action}/llm/{provider}/*` or
// `/llm/{provider}/*` (resolved against the active turn) to Kodema's
// `/llm/{provider}/*` endpoint with live `kodsvc.` auth and attribution headers.
func (p *OpenCodeLLMProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	binding, upstreamPath, ok := p.resolveTurnAndPath(r)
	if !ok {
		writeProxyJSONError(w, http.StatusForbidden, api.CodeForbidden, "no active attributed Kodema turn for LLM proxy request")
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
	for name, values := range r.Header {
		switch strings.ToLower(name) {
		case "authorization", "x-api-key", "x-kodema-execution", "x-kodema-action", "host", "connection", "keep-alive", "transfer-encoding", "upgrade":
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

	for name, values := range resp.Header {
		switch strings.ToLower(name) {
		case "connection", "keep-alive", "transfer-encoding", "upgrade", "content-length":
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

func (p *OpenCodeLLMProxy) resolveTurnAndPath(r *http.Request) (*proxyTurnBinding, string, bool) {
	path := r.URL.Path
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Explicit path-scoped routing: /turn/{exec}/{action}/llm/{provider}/...
	if strings.HasPrefix(path, "/turn/") {
		rest := strings.TrimPrefix(path, "/turn/")
		parts := strings.SplitN(rest, "/", 3)
		if len(parts) != 3 || !strings.HasPrefix("/"+parts[2], "/llm/") {
			return nil, "", false
		}
		key := parts[0] + "/" + parts[1]
		binding, ok := p.turns[key]
		if !ok {
			return nil, "", false
		}
		return binding, "/" + parts[2], true
	}

	if !strings.HasPrefix(path, "/llm/") {
		return nil, "", false
	}

	// Explicit header attribution override if matching an active registered turn.
	if execHdr := strings.TrimSpace(r.Header.Get("X-Kodema-Execution")); execHdr != "" {
		if actHdr := strings.TrimSpace(r.Header.Get("X-Kodema-Action")); actHdr != "" {
			if binding, ok := p.turns[execHdr+"/"+actHdr]; ok {
				return binding, path, true
			}
			return nil, "", false
		}
	}

	if p.defaultKey == "" {
		return nil, "", false
	}
	binding, ok := p.turns[p.defaultKey]
	return binding, path, ok
}

func writeProxyJSONError(w http.ResponseWriter, status int, code, message string) {
	body, _ := json.Marshal(api.Error{Code: code, Message: message})
	w.Header().Set("Content-Type", api.MediaType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
