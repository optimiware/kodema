package cli

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/optimiware/kodema/internal/userland/sandbox"
)

// runSandbox dispatches `kodema sandbox <subcommand>`.
func runSandbox(ctx context.Context, args []string, ports Ports, deps CommandDeps) int {
	if len(args) == 0 {
		return usage(ports.Err, "sandbox requires a subcommand (serve)")
	}
	if args[0] == "-h" || args[0] == "--help" {
		return runHelp(ports.Out, ports.Err, []string{"sandbox"})
	}
	switch args[0] {
	case "serve":
		if hasHelpFlag(args[1:]) {
			return runHelp(ports.Out, ports.Err, []string{"sandbox", "serve"})
		}
		return runSandboxServe(ctx, args[1:], ports, deps)
	default:
		return usage(ports.Err, fmt.Sprintf("unknown sandbox subcommand %q (expected serve)", args[0]))
	}
}

func runSandboxServe(ctx context.Context, args []string, ports Ports, deps CommandDeps) int {
	envLookup := os.LookupEnv
	if deps.Env != nil || deps.Login.Environment != nil {
		getenv := resolveEnv(deps)
		envLookup = func(key string) (string, bool) {
			v := getenv(key)
			if v == "" {
				return "", false
			}
			return v, true
		}
	}
	envDefault := func(key, fallback string) string {
		if v, ok := envLookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return fallback
	}

	flags := flag.NewFlagSet("sandbox serve", flag.ContinueOnError)
	flags.SetOutput(ports.Err)
	listen := flags.String("listen", envDefault("KODEMA_SANDBOX_LISTEN", sandbox.DefaultListenAddr), "HTTP listen address (e.g. 127.0.0.1:8090 or 0.0.0.0:8090)")
	workspace := flags.String("workspace", envDefault("KODEMA_SANDBOX_WORKSPACE", sandbox.DefaultWorkspaceDir), "workspace root directory to jail")
	tokenFile := flags.String("token-file", envDefault(sandbox.DefaultTokenFileEnv, ""), "file containing the sandbox bearer token")
	tokenEnv := flags.String("token-env", sandbox.DefaultTokenEnv, "environment variable containing the sandbox bearer token")
	generateTokenFile := flags.String("generate-token-file", "", "generate a fresh 256-bit bearer token and write it with 0600 permissions to this path")
	unlinkTokenFile := flags.Bool("unlink-token-file", false, "unlink --token-file immediately after reading it into memory")
	maxCmdTimeout := flags.Duration("max-cmd-timeout", sandbox.DefaultMaxCmdTimeout, "maximum timeout for a single bash tool call")
	maxOutputBytes := flags.Int("max-output-bytes", sandbox.DefaultMaxOutputBytes, "maximum stdout/stderr bytes captured per bash tool call")
	maxFileBytes := flags.Int64("max-file-bytes", sandbox.DefaultMaxFileBytes, "maximum file size in bytes for read_file/write_file/edit_file/grep")

	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		return usage(ports.Err, fmt.Sprintf("unexpected argument %q", flags.Arg(0)))
	}

	token, err := sandbox.ResolveToken(sandbox.TokenConfig{
		TokenFile:         *tokenFile,
		TokenEnv:          *tokenEnv,
		GenerateTokenFile: *generateTokenFile,
		WorkspaceDir:      *workspace,
		UnlinkTokenFile:   *unlinkTokenFile,
		LookupEnv:         envLookup,
	})
	if err != nil {
		return usage(ports.Err, err.Error())
	}

	server, err := sandbox.NewServer(sandbox.Config{
		ListenAddr:     *listen,
		WorkspaceDir:   *workspace,
		Token:          token,
		TokenEnv:       *tokenEnv,
		MaxCmdTimeout:  *maxCmdTimeout,
		MaxOutputBytes: *maxOutputBytes,
		MaxFileBytes:   *maxFileBytes,
	})
	if err != nil {
		return usage(ports.Err, err.Error())
	}

	if err := server.Serve(ctx); err != nil {
		fmt.Fprintf(ports.Err, "kodema: sandbox serve: %v\n", err)
		return 1
	}
	return 0
}
