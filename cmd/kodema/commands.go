package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/optimiware/kodema/client"
	"github.com/optimiware/kodema/internal/apps/agent"
	agentapp "github.com/optimiware/kodema/internal/apps/agent/app"
	authapp "github.com/optimiware/kodema/internal/apps/auth"
	emailapp "github.com/optimiware/kodema/internal/apps/email/app"
	"github.com/optimiware/kodema/internal/apps/git"
	gitapp "github.com/optimiware/kodema/internal/apps/git/app"
	"github.com/optimiware/kodema/internal/apps/git/app/actors"
	"github.com/optimiware/kodema/internal/apps/kit/servicekit"
	mergeapp "github.com/optimiware/kodema/internal/apps/merge/app"
	"github.com/optimiware/kodema/internal/apps/opencode"
	opencodeapp "github.com/optimiware/kodema/internal/apps/opencode/app"
	"github.com/optimiware/kodema/internal/id"
)

// newReadVerifier constructs the smart-HTTP read actor verifier. It is a small
// injectable seam, following provisionTenant, so command tests can pin the
// composition without building the public client.
var newReadVerifier = func(verification git.VerificationClient, resourceID func(string) (string, error)) (git.ReadActorVerifier, error) {
	return git.NewReadVerifier(verification, resourceID)
}

// runGitApp starts the composed smart-HTTP Git service. Tests replace it to
// capture the production configuration without opening a listener.
var runGitApp = gitapp.Run

// runAuthApp starts the composed OIDC provider and social login broker. Tests
// replace it to capture the configuration without opening a listener.
var runAuthApp = authapp.Run

// runMergeCoordinator starts the composed merge coordinator service. Tests
// replace it to capture the production configuration without running the loop.
var runMergeCoordinator = mergeapp.RunMergeCoordinator

// newPublicTransport builds the public HTTP transport used by the Git and agent
// processes. The mounted credential file is read on every attempt.
func newPublicTransport(baseURL, credentialFile string) (*client.HTTPTransport, error) {
	return newPublicTransportWithCA(baseURL, credentialFile, "")
}

// newPublicTransportWithCA builds the public HTTP transport with an optional
// PEM CA bundle file.
func newPublicTransportWithCA(baseURL, credentialFile, caFile string) (*client.HTTPTransport, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("a public base URL is required")
	}
	if strings.TrimSpace(credentialFile) == "" {
		return nil, fmt.Errorf("a mounted credential file is required")
	}
	retry := client.RetryPolicy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	if err := retry.Validate(); err != nil {
		return nil, err
	}
	rt := http.DefaultTransport
	if trimmedCA := strings.TrimSpace(caFile); trimmedCA != "" {
		pemData, err := os.ReadFile(trimmedCA)
		if err != nil {
			return nil, fmt.Errorf("read CA bundle %q: %w", trimmedCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemData) {
			return nil, fmt.Errorf("CA bundle %q contains no valid PEM certificates", trimmedCA)
		}
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := base.Clone()
			cloned.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
			rt = cloned
		} else {
			rt = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
		}
	}
	return &client.HTTPTransport{
		BaseURL:      baseURL,
		RoundTripper: rt,
		Source:       client.FileCredentialSource{Path: credentialFile},
		Retry:        retry,
	}, nil
}

// runGitServeEntry runs `kodema git serve`.
func runGitServeEntry(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("git serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	tenant := flags.String("tenant", envOr("KODEMA_GIT_TENANT", ""), "tenant identity")
	listen := flags.String("listen", envOr("KODEMA_GIT_LISTEN", "127.0.0.1:8081"), "smart-HTTP listen address")
	storageRoot := flags.String("storage-root", envOr("KODEMA_GIT_STORAGE_ROOT", ""), "repository storage root")
	journalDirectory := flags.String("journal-dir", envOr("KODEMA_GIT_JOURNAL_DIR", ""), "receive journal directory")
	// The access policy is the committed conf/git.conf of the tenant's
	// provisioned admin repository. There is no --policy flag and no
	// KODEMA_GIT_POLICY environment variable: neither is read, and --policy is
	// an unknown-flag usage error.
	baseURL := flags.String("base-url", envOr("KODEMA_BASE_URL", ""), "public base URL")
	credentialFile := flags.String("credential-file", envOr("KODEMA_CREDENTIAL_FILE", ""), "mounted service credential file")
	provisionTokenFile := flags.String("provision-token-file", envOr("KODEMA_GIT_PROVISION_TOKEN_FILE", ""), "optional mounted bearer token file for HTTP repository provisioning")
	defaultMaxBodyBytes, maxBodyEnvErr := envInt64Strict("KODEMA_GIT_MAX_BODY_BYTES", 0)
	defaultMaxResponseBytes, maxResponseEnvErr := envInt64Strict("KODEMA_GIT_MAX_RESPONSE_BYTES", 0)
	maxBodyBytes := flags.Int64("max-body-bytes", defaultMaxBodyBytes, "maximum smart-HTTP request body bytes")
	maxResponseBytes := flags.Int64("max-response-bytes", defaultMaxResponseBytes, "maximum smart-HTTP response bytes")
	timeout := flags.Duration("timeout", envDuration("KODEMA_GIT_TIMEOUT", 0), "per-exchange timeout")
	resources := stringList{}
	flags.Var(&resources, "resource", "repository=resource-id (repeatable)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if maxBodyEnvErr != nil {
		usage(stderr, maxBodyEnvErr.Error())
		return 2
	}
	if maxResponseEnvErr != nil {
		usage(stderr, maxResponseEnvErr.Error())
		return 2
	}
	if *maxBodyBytes < 0 {
		usage(stderr, "--max-body-bytes must not be negative")
		return 2
	}
	if *maxResponseBytes < 0 {
		usage(stderr, "--max-response-bytes must not be negative")
		return 2
	}
	if flags.NArg() != 0 {
		usage(stderr, fmt.Sprintf("unexpected argument %q", flags.Arg(0)))
		return 2
	}
	if strings.TrimSpace(*tenant) == "" || strings.TrimSpace(*storageRoot) == "" ||
		strings.TrimSpace(*journalDirectory) == "" {
		usage(stderr, "git serve requires --tenant, --storage-root and --journal-dir")
		return 2
	}
	resourceIDs := make(map[string]string, len(resources))
	for _, resource := range resources {
		name, value, ok := strings.Cut(resource, "=")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
			usage(stderr, fmt.Sprintf("invalid --resource %q; want repository=resource-id", resource))
			return 2
		}
		resourceIDs[name] = value
	}
	// Public verification is mandatory: a Git edge that cannot verify its
	// actors and ref updates must refuse startup rather than serve unverified
	// traffic. There is no optional-transport branch.
	if strings.TrimSpace(*baseURL) == "" || strings.TrimSpace(*credentialFile) == "" {
		usage(stderr, "git serve requires --base-url and --credential-file")
		return 2
	}
	transport, err := newPublicTransport(*baseURL, *credentialFile)
	if err != nil {
		usage(stderr, err.Error())
		return 2
	}
	verification, err := gitapp.NewVerificationClient(transport)
	if err != nil {
		fmt.Fprintln(stderr, "kodema: git serve: actor verification is not configured")
		return 1
	}
	// The read verifier resolves the registered resource of the requested
	// tenant-relative repository. A repository without a mapping refuses with
	// the unauthenticated class, so the edge answers the uniform 401 challenge
	// and never reveals whether the repository exists; the repository name is
	// never substituted for its resource identity.
	resolveResource := func(repository string) (string, error) {
		resourceID, ok := resourceIDs[repository]
		if !ok || strings.TrimSpace(resourceID) == "" {
			return "", git.UnregisteredRepositoryError(repository)
		}
		return resourceID, nil
	}
	readVerifier, err := newReadVerifier(verification, resolveResource)
	if err != nil || readVerifier == nil {
		fmt.Fprintln(stderr, "kodema: git serve: actor verification is not configured")
		return 1
	}
	// One registry serves both smart-HTTP admission and the native hooks: the
	// handler places only an opaque handle in the backend environment and the
	// hook resolves it here.
	registry, err := actors.NewRegistry(actors.Config{TenantID: id.TenantID(*tenant)})
	if err != nil {
		fmt.Fprintln(stderr, "kodema: git serve: actor handle registry is not configured")
		return 1
	}
	defer func() { _ = registry.Close() }()
	logOutput := stderr
	if logOutput == nil {
		logOutput = io.Discard
	}
	config := gitapp.Config{
		TenantID:           id.TenantID(*tenant),
		ListenAddress:      *listen,
		StorageRoot:        *storageRoot,
		JournalDirectory:   *journalDirectory,
		BaseURL:            *baseURL,
		CredentialFile:     *credentialFile,
		ProvisionTokenFile: *provisionTokenFile,
		ResourceIDs:        resourceIDs,
		MaxBodyBytes:       *maxBodyBytes,
		MaxResponseBytes:   *maxResponseBytes,
		Timeout:            *timeout,
		Logger:             slog.New(slog.NewTextHandler(logOutput, nil)),
		Transport:          transport,
		Verify:             verification,
		Actors:             readVerifier,
		ActorHandles:       registry,
		HookActors:         registry,
	}
	if err := runGitApp(ctx, config); err != nil {
		fmt.Fprintf(stderr, "kodema: %v\n", err)
		return 1
	}
	return 0
}

// envInt64Strict reads one optional non-negative byte-count setting and refuses
// negative or malformed values rather than silently falling back.
func envInt64Strict(name string, fallback int64) (int64, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback, nil
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer byte count", name)
	}
	return parsed, nil
}

// runAgentEntry runs `kodema agent`.
func runAgentEntry(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("agent", flag.ContinueOnError)
	flags.SetOutput(stderr)
	baseURL := flags.String("base-url", envOr("KODEMA_BASE_URL", ""), "public base URL")
	credentialFile := flags.String("credential-file", envOr("KODEMA_CREDENTIAL_FILE", ""), "mounted execution credential file")
	project := flags.String("project", envOr("KODEMA_PROJECT", ""), "project identity")
	task := flags.String("task", envOr("KODEMA_TASK", ""), "task identity")
	execution := flags.String("execution", envOr("KODEMA_EXECUTION", ""), "execution identity")
	model := flags.String("model", envOr("KODEMA_MODEL", ""), "model identity")
	knowledgeRoot := flags.String("knowledge-root", envOr("KODEMA_KNOWLEDGE_ROOT", ""), "rooted knowledge directory")
	checkpointRoot := flags.String("checkpoint-dir", envOr("KODEMA_CHECKPOINT_DIR", ""), "durable checkpoint directory")
	restore := flags.String("restore", "", "checkpoint reference to restore")
	retainRaw := flags.Bool("retain-raw", false, "retain raw model/tool content")
	messages := stringList{}
	knowledge := stringList{}
	tools := stringList{}
	flags.Var(&messages, "message", "run message (repeatable)")
	flags.Var(&knowledge, "knowledge", "knowledge path (repeatable)")
	flags.Var(&tools, "tool", "allowed tool name (repeatable)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		usage(stderr, fmt.Sprintf("unexpected argument %q", flags.Arg(0)))
		return 2
	}
	if strings.TrimSpace(*task) == "" || strings.TrimSpace(*execution) == "" || strings.TrimSpace(*model) == "" || len(messages) == 0 {
		usage(stderr, "agent requires --task, --execution, --model and at least one --message")
		return 2
	}
	if strings.TrimSpace(*project) == "" {
		usage(stderr, "agent requires --project")
		return 2
	}
	if strings.TrimSpace(*checkpointRoot) == "" {
		usage(stderr, "agent requires --checkpoint-dir")
		return 2
	}
	if len(knowledge) > 0 && strings.TrimSpace(*knowledgeRoot) == "" {
		usage(stderr, "agent requires --knowledge-root when --knowledge is supplied")
		return 2
	}
	transport, err := newPublicTransport(*baseURL, *credentialFile)
	if err != nil {
		usage(stderr, err.Error())
		return 2
	}
	runInput := agent.RunInput{
		TaskID:      *task,
		ExecutionID: *execution,
		Model:       *model,
		Knowledge:   knowledge,
		Tools:       tools,
		RestoreRef:  *restore,
	}
	for _, message := range messages {
		runInput.Messages = append(runInput.Messages, agent.Message{Role: "user", Content: message})
	}
	config := agentapp.Config{
		Run:            runInput,
		ProjectID:      *project,
		KnowledgeRoot:  *knowledgeRoot,
		CheckpointRoot: *checkpointRoot,
		RetainRaw:      *retainRaw,
		Out:            stdout,
		Transport:      transport,
	}
	if err := agentapp.Run(ctx, config); err != nil {
		fmt.Fprintf(stderr, "kodema: %v\n", err)
		return 1
	}
	return 0
}

// runServicesEntry runs `kodema services`.
func runServicesEntry(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("services", flag.ContinueOnError)
	flags.SetOutput(stderr)
	kind := flags.String("kind", envOr("KODEMA_SERVICE_KIND", ""), "service adapter kind")
	service := flags.String("service", envOr("KODEMA_SERVICE_ID", ""), "service identity")
	tenant := flags.String("tenant", envOr("KODEMA_SERVICE_TENANT", ""), "tenant identity")
	project := flags.String("project", envOr("KODEMA_SERVICE_PROJECT", ""), "project identity")
	baseURL := flags.String("base-url", envOr("KODEMA_BASE_URL", ""), "public base URL")
	credentialFile := flags.String("credential-file", envOr("KODEMA_CREDENTIAL_FILE", ""), "mounted service credential file")
	caFile := flags.String("ca-file", envOr("KODEMA_CA_FILE", ""), "optional PEM CA bundle for HTTPS")
	schedule := flags.Duration("schedule", envDuration("KODEMA_SERVICE_SCHEDULE", 0), "timer schedule")
	rescan := flags.Duration("rescan", envDuration("KODEMA_SERVICE_RESCAN", 30*time.Second), "rescan interval")
	stateDir := flags.String("state-dir", envOr("KODEMA_SERVICE_STATE_DIR", ""), "durable local state directory")
	gitURL := flags.String("git-url", envOr("KODEMA_SERVICE_GIT_URL", ""), "Git base URL")
	opencodeURL := flags.String("opencode-url", envOr("KODEMA_OPENCODE_URL", ""), "opencode serve HTTP URL")
	opencodePassword := flags.String("opencode-password", "", "optional OpenCode server password for HTTP Basic Auth (or KODEMA_OPENCODE_PASSWORD / OPENCODE_PASSWORD; prefer --opencode-password-file)")
	opencodePasswordFile := flags.String("opencode-password-file", envOr("KODEMA_OPENCODE_PASSWORD_FILE", ""), "file containing the OpenCode server password for HTTP Basic Auth (recommended over --opencode-password)")
	opencodeMode := flags.String("mode", envOr("KODEMA_OPENCODE_MODE", string(opencode.OpenCodeExecutionModeWorktree)), "OpenCode execution mode (worktree|brain)")
	llmProxyListen := flags.String("llm-proxy-listen", envOr("KODEMA_OPENCODE_LLM_PROXY_LISTEN", ""), "optional loopback listen address for the attributed LLM reverse proxy")
	llmProxyAPIKey := flags.String("llm-proxy-api-key", "", "optional shared secret for inbound loopback LLM proxy requests (or KODEMA_OPENCODE_LLM_PROXY_API_KEY; auto-generated if empty)")
	defaultPerTurnProxyRouting, perTurnProxyEnvErr := envBoolStrict("KODEMA_OPENCODE_PER_TURN_PROXY_ROUTING", false)
	perTurnProxyRouting := flags.Bool("per-turn-proxy-routing", defaultPerTurnProxyRouting, "route LLM proxy calls via explicit /turn/{exec}/{action}/llm/* paths for concurrent turns")
	sandboxDefaultImage := flags.String("sandbox-default-image", envOr("KODEMA_OPENCODE_SANDBOX_DEFAULT_IMAGE", ""), "default stock toolchain image for Option 2 brain mode sandboxes")
	sandboxCachesRaw := flags.String("sandbox-caches", envOr("KODEMA_OPENCODE_SANDBOX_CACHES", ""), "comma-separated default Kodema cache names mounted into ephemeral sandboxes")
	sandboxPort := flags.Int("sandbox-port", opencode.DefaultOpenCodeSandboxPort, "container TCP port served by kodema sandbox serve in brain mode")
	sandboxMemory := flags.String("sandbox-memory", envOr("KODEMA_OPENCODE_SANDBOX_MEMORY", ""), "optional default memory limit for ephemeral sandboxes (e.g. 2Gi)")
	sandboxCPU := flags.String("sandbox-cpu", envOr("KODEMA_OPENCODE_SANDBOX_CPU", ""), "optional default CPU limit for ephemeral sandboxes (e.g. 2)")
	model := flags.String("model", envOr("KODEMA_OPENCODE_MODEL", ""), "optional default OpenCode model")
	agent := flags.String("agent", envOr("KODEMA_OPENCODE_AGENT", ""), "optional default OpenCode agent")
	maxConcurrency := flags.Int("max-concurrency", 0, "maximum concurrent OpenCode turns (defaults to 1 with static --llm-proxy-listen, 4 otherwise)")
	message := flags.String("message", envOr("KODEMA_EMAIL_MESSAGE", ""), "one RFC822 message file, or - for stdin")
	pipeline := flags.String("pipeline", envOr("KODEMA_EMAIL_PIPELINE", ""), "email intake pipeline identity")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		usage(stderr, fmt.Sprintf("unexpected argument %q", flags.Arg(0)))
		return 2
	}
	if strings.TrimSpace(*message) != "" {
		return runServicesEmailIntake(ctx, *kind, *tenant, *project, *pipeline, *message,
			*baseURL, *credentialFile, *caFile, stdout, stderr)
	}
	if strings.TrimSpace(*kind) == "" || strings.TrimSpace(*service) == "" ||
		strings.TrimSpace(*tenant) == "" || strings.TrimSpace(*project) == "" {
		usage(stderr, "services requires --kind, --service, --tenant and --project")
		return 2
	}
	if !servicekit.ServiceKind(*kind).Valid() {
		usage(stderr, fmt.Sprintf("unknown service kind %q", *kind))
		return 2
	}
	transport, err := newPublicTransportWithCA(*baseURL, *credentialFile, *caFile)
	if err != nil {
		usage(stderr, err.Error())
		return 2
	}
	if servicekit.ServiceKind(*kind) == servicekit.KindMergeCoordinator {
		if strings.TrimSpace(*stateDir) == "" || strings.TrimSpace(*gitURL) == "" {
			usage(stderr, "merge coordinator requires --state-dir and --git-url")
			return 2
		}
		config := mergeapp.MergeConfig{
			ServiceID:      id.PrincipalID(*service),
			TenantID:       id.TenantID(*tenant),
			ProjectID:      id.ProjectID(*project),
			StateDirectory: *stateDir,
			GitURL:         *gitURL,
			CredentialFile: *credentialFile,
			CAFile:         strings.TrimSpace(*caFile),
			RescanInterval: *rescan,
			Transport:      transport,
		}
		if err := runMergeCoordinator(ctx, config); err != nil {
			fmt.Fprintf(stderr, "kodema: %v\n", err)
			return 1
		}
		return 0
	}
	if servicekit.ServiceKind(*kind) == servicekit.KindOpenCode {
		setFlags := make(map[string]bool)
		flags.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
		if !setFlags["per-turn-proxy-routing"] && perTurnProxyEnvErr != nil {
			usage(stderr, perTurnProxyEnvErr.Error())
			return 2
		}
		explicitPassword := setFlags["opencode-password"]
		explicitPasswordFile := setFlags["opencode-password-file"]
		if explicitPassword && explicitPasswordFile {
			usage(stderr, "opencode service cannot use both --opencode-password and --opencode-password-file")
			return 2
		}
		effectiveOpenCodePassword := *opencodePassword
		effectiveOpenCodePasswordFile := strings.TrimSpace(*opencodePasswordFile)
		if explicitPasswordFile {
			if effectiveOpenCodePasswordFile == "" {
				usage(stderr, "opencode service --opencode-password-file requires a non-empty path")
				return 2
			}
			effectiveOpenCodePassword = ""
		} else if explicitPassword {
			effectiveOpenCodePasswordFile = ""
		} else if effectiveOpenCodePasswordFile != "" {
			if os.Getenv("KODEMA_OPENCODE_PASSWORD") != "" {
				usage(stderr, "opencode service cannot set both KODEMA_OPENCODE_PASSWORD and KODEMA_OPENCODE_PASSWORD_FILE")
				return 2
			}
			effectiveOpenCodePassword = ""
		} else {
			effectiveOpenCodePassword = os.Getenv("KODEMA_OPENCODE_PASSWORD")
			if effectiveOpenCodePassword == "" {
				effectiveOpenCodePassword = os.Getenv("OPENCODE_PASSWORD")
			}
		}
		effectiveProxyAPIKey := *llmProxyAPIKey
		if !setFlags["llm-proxy-api-key"] {
			effectiveProxyAPIKey = os.Getenv("KODEMA_OPENCODE_LLM_PROXY_API_KEY")
		}
		if effectiveOpenCodePasswordFile != "" {
			if err := opencode.ValidateOpenCodePasswordFile(effectiveOpenCodePasswordFile); err != nil {
				usage(stderr, err.Error())
				return 2
			}
		} else if effectiveOpenCodePassword != "" {
			if err := opencode.ValidateOpenCodePassword(effectiveOpenCodePassword); err != nil {
				usage(stderr, err.Error())
				return 2
			}
		}
		execMode := opencode.OpenCodeExecutionMode(strings.TrimSpace(*opencodeMode))
		if execMode == "" {
			execMode = opencode.OpenCodeExecutionModeWorktree
		}
		if !execMode.Valid() {
			usage(stderr, fmt.Sprintf("opencode service --mode %q is unsupported (want worktree or brain)", execMode))
			return 2
		}
		if execMode == opencode.OpenCodeExecutionModeWorktree {
			if strings.TrimSpace(*stateDir) == "" || strings.TrimSpace(*gitURL) == "" || strings.TrimSpace(*opencodeURL) == "" {
				usage(stderr, "opencode service requires --state-dir, --git-url and --opencode-url")
				return 2
			}
		} else {
			if strings.TrimSpace(*stateDir) == "" || strings.TrimSpace(*opencodeURL) == "" {
				usage(stderr, "opencode brain service requires --state-dir and --opencode-url")
				return 2
			}
		}
		usePerTurnRouting := *perTurnProxyRouting
		effectiveMaxConcurrency := *maxConcurrency
		if strings.TrimSpace(*llmProxyListen) != "" && !usePerTurnRouting {
			if effectiveMaxConcurrency > 1 {
				usage(stderr, "opencode service with --llm-proxy-listen requires --max-concurrency <= 1 unless --per-turn-proxy-routing is enabled")
				return 2
			}
			if effectiveMaxConcurrency <= 0 {
				effectiveMaxConcurrency = 1
			}
		}
		defaultCaches := splitComma(*sandboxCachesRaw)
		brainCfg := opencode.OpenCodeBrainConfig{
			DefaultSandboxImage:  strings.TrimSpace(*sandboxDefaultImage),
			DefaultSandboxCaches: defaultCaches,
			SandboxPort:          *sandboxPort,
			DefaultSandboxMemory: strings.TrimSpace(*sandboxMemory),
			DefaultSandboxCPU:    strings.TrimSpace(*sandboxCPU),
		}
		if execMode == opencode.OpenCodeExecutionModeBrain {
			normalized, err := opencode.NormalizeBrainConfig(brainCfg)
			if err != nil {
				usage(stderr, err.Error())
				return 2
			}
			brainCfg = normalized
		}
		config := opencodeapp.OpenCodeConfig{
			ServiceID:            id.PrincipalID(*service),
			TenantID:             id.TenantID(*tenant),
			ProjectID:            id.ProjectID(*project),
			ExecutionMode:        execMode,
			Brain:                brainCfg,
			StateDirectory:       *stateDir,
			BaseURL:              *baseURL,
			CAFile:               strings.TrimSpace(*caFile),
			GitURL:               *gitURL,
			CredentialFile:       *credentialFile,
			OpenCodeURL:          *opencodeURL,
			OpenCodePassword:     effectiveOpenCodePassword,
			OpenCodePasswordFile: effectiveOpenCodePasswordFile,
			ProxyListen:          *llmProxyListen,
			ProxyAPIKey:          effectiveProxyAPIKey,
			PerTurnProxyRouting:  usePerTurnRouting,
			Model:                *model,
			Agent:                *agent,
			RescanInterval:       *rescan,
			MaxConcurrentTurns:   effectiveMaxConcurrency,
			Transport:            transport,
		}
		if err := opencodeapp.RunOpenCode(ctx, config); err != nil {
			fmt.Fprintf(stderr, "kodema: %v\n", err)
			return 1
		}
		return 0
	}
	config := servicekit.Config{
		Kind:           servicekit.ServiceKind(*kind),
		ServiceID:      id.PrincipalID(*service),
		TenantID:       id.TenantID(*tenant),
		ProjectID:      id.ProjectID(*project),
		Schedule:       *schedule,
		RescanInterval: *rescan,
		Transport:      transport,
	}
	if err := servicekit.Run(ctx, config); err != nil {
		fmt.Fprintf(stderr, "kodema: %v\n", err)
		return 1
	}
	return 0
}

// runServicesEmailIntake runs the one-shot `kodema services --kind email`
// intake. The message source is read before the public transport is used, so an
// unreadable source is a local refusal, never an ambiguous mutation.
func runServicesEmailIntake(ctx context.Context, kind, tenant, project, pipeline, message, baseURL, credentialFile, caFile string, stdout, stderr io.Writer) int {
	if kind != string(servicekit.KindEmail) {
		usage(stderr, "email intake requires --kind email")
		return 2
	}
	if strings.TrimSpace(pipeline) == "" {
		usage(stderr, "email intake requires --pipeline")
		return 2
	}
	if strings.TrimSpace(tenant) == "" || strings.TrimSpace(project) == "" {
		usage(stderr, "email intake requires --tenant and --project")
		return 2
	}
	var source io.Reader = os.Stdin
	if message != "-" {
		file, err := os.Open(message)
		if err != nil {
			fmt.Fprintf(stderr, "kodema: email message: %v\n", err)
			return 1
		}
		defer file.Close()
		source = file
	}
	transport, err := newPublicTransportWithCA(baseURL, credentialFile, caFile)
	if err != nil {
		usage(stderr, err.Error())
		return 2
	}
	result, err := emailapp.RunEmailIntake(ctx, emailapp.EmailIntakeConfig{
		TenantID:   id.TenantID(tenant),
		ProjectID:  id.ProjectID(project),
		PipelineID: pipeline,
		Message:    source,
		Transport:  transport,
	})
	if err != nil {
		fmt.Fprintf(stderr, "kodema: %v\n", err)
		return 1
	}
	// Only the committed identities are reported, and only to the injected
	// stdout: never the message identity, subject, body, envelope, digest or
	// credential. A failed write is a fixed refusal with a nonzero exit.
	if _, err := fmt.Fprintf(stdout, "task_id=%s execution_id=%s\n", result.TaskID, result.ExecutionID); err != nil {
		fmt.Fprintln(stderr, "kodema: email intake: writing the result failed")
		return 1
	}
	return 0
}
