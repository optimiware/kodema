package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/optimiware/kodema/api"
	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	"github.com/optimiware/kodema/internal/apps/opencode"
	"github.com/optimiware/kodema/internal/id"
)

// OpenCodeConfig is the complete composition input for one per-project
// OpenCode service adapter process (`kodema services --kind opencode`).
type OpenCodeConfig struct {
	ServiceID            id.PrincipalID
	TenantID             id.TenantID
	ProjectID            id.ProjectID
	ExecutionMode        opencode.OpenCodeExecutionMode
	Sandbox              opencode.OpenCodeSandboxConfig
	StateDirectory       string
	BaseURL              string
	CAFile               string
	GitURL               string
	CredentialFile       string
	CredentialHelper     string
	OpenCodeURL          string
	OpenCodePassword     string
	OpenCodePasswordFile string
	ProxyListen          string
	ProxyAPIKey          string
	PerTurnProxyRouting  bool
	Model                string
	Agent                string
	RescanInterval       time.Duration
	MaxConcurrentTurns   int

	// Transport is the public client boundary (typically *client.HTTPTransport).
	Transport client.Transport
	// Handler overrides Transport for RPC calls when injected in tests.
	Handler api.Handler
	// Stream is the live project SSE event stream source. When nil and
	// Transport implements client.EventStreamer, Transport is used automatically.
	Stream client.EventStreamer
	// Events is the optional retained-notification fallback source.
	Events servicekit.EventSource
	// OpenCode overrides the default HTTP client targeting OpenCodeURL.
	OpenCode opencode.OpenCodeClient
	// Worktrees overrides the default local Git worktree manager (worktree mode).
	Worktrees opencode.OpenCodeWorktreeManager
	// Sandboxes overrides the default KodemaExecutionSandboxManager (sandbox mode).
	Sandboxes opencode.OpenCodeSandboxManager
	// Store overrides the default FileOpenCodeStateStore.
	Store opencode.OpenCodeStateStore
	// Executor overrides the native OS Git command executor.
	Executor servicekit.GitCommandExecutor
	// GitPath selects the git binary when Executor is nil.
	GitPath string
	// RoundTripper overrides the HTTP transport used for the local LLM proxy
	// and sandbox HTTP health/MCP calls.
	RoundTripper http.RoundTripper
	// Clock is the injected time boundary. Defaults to SystemClock.
	Clock servicekit.Clock
}

// RunOpenCode composes and runs one strictly project-scoped OpenCode
// service until ctx is cancelled.
func RunOpenCode(ctx context.Context, config OpenCodeConfig) error {
	if ctx == nil {
		ctx = context.Background()
	}
	execMode := config.ExecutionMode
	if execMode == "" {
		execMode = opencode.OpenCodeExecutionModeWorktree
	}
	if !execMode.Valid() {
		return fmt.Errorf("opencode service execution mode %q is unsupported (want worktree or sandbox)", execMode)
	}
	if strings.TrimSpace(config.StateDirectory) == "" && config.Store == nil {
		return fmt.Errorf("opencode service state directory is required")
	}
	if execMode == opencode.OpenCodeExecutionModeWorktree && strings.TrimSpace(config.GitURL) == "" && config.Worktrees == nil {
		return fmt.Errorf("opencode service Git URL is required")
	}
	if strings.TrimSpace(config.OpenCodeURL) == "" && config.OpenCode == nil {
		return fmt.Errorf("opencode service server URL is required")
	}
	handler := config.Handler
	if handler == nil {
		if config.Transport == nil {
			return fmt.Errorf("opencode service requires a public transport or handler")
		}
		handler = config.Transport
	}
	stream := config.Stream
	if stream == nil && config.Transport != nil {
		if streamer, ok := config.Transport.(client.EventStreamer); ok {
			stream = streamer
		}
	}
	events := config.Events
	if events == nil && stream == nil && config.Transport != nil {
		built, err := servicekit.NewPublicEventSource(config.Transport, config.ProjectID)
		if err != nil {
			return err
		}
		events = built
	}
	clock := config.Clock
	if clock == nil {
		clock = servicekit.SystemClock{}
	}

	var stateDir string
	if strings.TrimSpace(config.StateDirectory) != "" {
		abs, err := filepath.Abs(filepath.Clean(config.StateDirectory))
		if err != nil {
			return fmt.Errorf("resolve opencode state directory: %w", err)
		}
		if err := os.MkdirAll(abs, 0o700); err != nil {
			return fmt.Errorf("create opencode state directory: %w", err)
		}
		stateDir = abs
	}

	store := config.Store
	if store == nil {
		built, err := opencode.NewFileOpenCodeStateStore(filepath.Join(stateDir, "opencode"))
		if err != nil {
			return err
		}
		store = built
	}

	var worktrees opencode.OpenCodeWorktreeManager
	var sandboxes opencode.OpenCodeSandboxManager
	var sandboxRT http.RoundTripper

	if execMode == opencode.OpenCodeExecutionModeSandbox {
		sandboxes = config.Sandboxes
		sandboxRT = config.RoundTripper
		if sandboxes == nil {
			if sandboxRT == nil {
				var err error
				sandboxRT, err = httpRoundTripperWithCA(config.CAFile)
				if err != nil {
					return err
				}
			}
			built, err := opencode.NewKodemaExecutionSandboxManager(opencode.KodemaExecutionSandboxConfig{
				TenantID:     config.TenantID,
				ProjectID:    config.ProjectID,
				Handler:      handler,
				Clock:        clock,
				RoundTripper: sandboxRT,
			})
			if err != nil {
				return err
			}
			sandboxes = built
		}
	} else {
		worktrees = config.Worktrees
		if worktrees == nil {
			executor := config.Executor
			if executor == nil {
				built, err := servicekit.NewOSGitExecutor(servicekit.OSGitExecutorConfig{GitPath: config.GitPath})
				if err != nil {
					return err
				}
				executor = built
			}
			built, err := opencode.NewGitOpenCodeWorktreeManager(opencode.GitOpenCodeWorktreeConfig{
				RemoteBaseURL:    config.GitURL,
				StateDirectory:   stateDir,
				CredentialFile:   config.CredentialFile,
				CredentialHelper: config.CredentialHelper,
				CAFile:           config.CAFile,
				Executor:         executor,
			})
			if err != nil {
				return err
			}
			worktrees = built
		}
	}

	effectiveMaxConc := config.MaxConcurrentTurns
	if execMode == opencode.OpenCodeExecutionModeSandbox && config.OpenCode == nil {
		if effectiveMaxConc > 1 {
			return fmt.Errorf("opencode sandbox mode with OpenCode v2 HTTP client requires --max-concurrency 1 because OpenCode v2 runtime MCP attachment (/api/experimental/mcp/{server}) is location-scoped (run separate OpenCode v2 server instances for parallel sandbox-mode turns)")
		}
		if effectiveMaxConc <= 0 {
			effectiveMaxConc = 1
		}
	}

	opencodeClient := config.OpenCode
	if opencodeClient == nil {
		ocRT := config.RoundTripper
		if ocRT == nil && strings.TrimSpace(config.CAFile) != "" &&
			strings.HasPrefix(strings.ToLower(strings.TrimSpace(config.OpenCodeURL)), "https://") {
			var err error
			ocRT, err = httpRoundTripperWithCA(config.CAFile)
			if err != nil {
				return err
			}
		}
		built, err := opencode.NewHTTPOpenCodeClient(opencode.HTTPOpenCodeClientConfig{
			BaseURL:      config.OpenCodeURL,
			Password:     config.OpenCodePassword,
			PasswordFile: config.OpenCodePasswordFile,
			RoundTripper: ocRT,
		})
		if err != nil {
			return err
		}
		opencodeClient = built
	}

	var proxy *opencode.OpenCodeLLMProxy
	if strings.TrimSpace(config.ProxyListen) != "" {
		if strings.TrimSpace(config.BaseURL) == "" {
			return fmt.Errorf("opencode LLM proxy requires Kodema --base-url")
		}
		if strings.TrimSpace(config.CredentialFile) == "" {
			return fmt.Errorf("opencode LLM proxy requires --credential-file")
		}
		rt := config.RoundTripper
		if rt == nil {
			var err error
			rt, err = httpRoundTripperWithCA(config.CAFile)
			if err != nil {
				return err
			}
		}
		built, err := opencode.NewOpenCodeLLMProxy(opencode.OpenCodeLLMProxyConfig{
			ListenAddr:   config.ProxyListen,
			KodemaURL:    config.BaseURL,
			Credentials:  client.FileCredentialSource{Path: config.CredentialFile},
			LocalAPIKey:  config.ProxyAPIKey,
			RoundTripper: rt,
			Clock:        clock,
		})
		if err != nil {
			return err
		}
		if _, err := built.Start(config.ProxyListen); err != nil {
			return err
		}
		proxy = built
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = proxy.Close(shutdownCtx)
		}()
	}

	adapter, err := opencode.NewOpenCodeAdapter(opencode.OpenCodeAdapterConfig{
		ServiceID:           config.ServiceID,
		TenantID:            config.TenantID,
		ProjectID:           config.ProjectID,
		ExecutionMode:       execMode,
		Sandbox:             config.Sandbox,
		Handler:             handler,
		Stream:              stream,
		Events:              events,
		OpenCode:            opencodeClient,
		Store:               store,
		Worktrees:           worktrees,
		Sandboxes:           sandboxes,
		Proxy:               proxy,
		PerTurnProxyRouting: config.PerTurnProxyRouting,
		SandboxRoundTripper: sandboxRT,
		Clock:               clock,
		RescanInterval:      config.RescanInterval,
		MaxConcurrentTurns:  effectiveMaxConc,
		Model:               config.Model,
		Agent:               config.Agent,
	})
	if err != nil {
		return err
	}
	return adapter.Run(ctx)
}

func httpRoundTripperWithCA(caFile string) (http.RoundTripper, error) {
	base, ok := http.DefaultTransport.(*http.Transport)
	var tr *http.Transport
	if ok {
		tr = base.Clone()
	} else {
		tr = &http.Transport{}
	}
	// Never inherit ambient HTTP_PROXY/HTTPS_PROXY environment variables.
	tr.Proxy = nil

	caFile = strings.TrimSpace(caFile)
	if caFile == "" {
		return tr, nil
	}
	pemBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("CA bundle %q contains no valid PEM certificates", caFile)
	}
	tr.TLSClientConfig = &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
	return tr, nil
}
