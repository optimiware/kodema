// Package sandbox implements the authenticated, /workspace-jailed MCP server
// invoked as `kodema sandbox serve` inside ephemeral toolchain containers.
//
// It uses only the Go standard library and opens no Kodema store or control
// plane database, so any stock upstream toolchain image (golang, node, rust,
// python, etc.) works out of the box when `/kodema/bin/kodema` is mounted.
package sandbox

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	// DefaultListenAddr is the default listen address for `kodema sandbox serve`.
	DefaultListenAddr = "127.0.0.1:8090"
	// DefaultWorkspaceDir is the default canonical workspace root inside a container.
	DefaultWorkspaceDir = "/workspace"
	// DefaultTokenEnv is the default environment variable holding the bearer token.
	DefaultTokenEnv = "KODEMA_SANDBOX_TOKEN"

	// DefaultMaxCmdTimeout bounds a single bash tool execution when not configured.
	DefaultMaxCmdTimeout = 5 * time.Minute
	// HardMaxCmdTimeout is the upper safety cap for --max-cmd-timeout.
	HardMaxCmdTimeout = 30 * time.Minute
	// DefaultCmdTimeout is used when a bash call omits timeout_seconds.
	DefaultCmdTimeout = 60 * time.Second

	// DefaultMaxOutputBytes bounds stdout/stderr captured per bash call (256 KiB).
	DefaultMaxOutputBytes = 256 << 10
	// HardMaxOutputBytes is the upper safety cap for --max-output-bytes (16 MiB).
	HardMaxOutputBytes = 16 << 20

	// DefaultMaxFileBytes bounds read_file/write_file/edit_file/grep payloads (4 MiB).
	DefaultMaxFileBytes int64 = 4 << 20
	// HardMaxFileBytes is the upper safety cap for --max-file-bytes (64 MiB).
	HardMaxFileBytes int64 = 64 << 20

	// DefaultMaxRequestBodyBytes bounds one HTTP JSON-RPC POST /mcp request body.
	DefaultMaxRequestBodyBytes int64 = 8 << 20
	// DefaultMaxGlobResults caps files returned by one glob call.
	DefaultMaxGlobResults = 1000
	// DefaultMaxGrepMatches caps matching lines returned by one grep call.
	DefaultMaxGrepMatches = 500
	// DefaultMaxGrepLineBytes caps the rendered length of one matched line.
	DefaultMaxGrepLineBytes = 4096
	// DefaultReadFileLineLimit caps lines returned by one read_file call.
	DefaultReadFileLineLimit = 2000

	maxTokenFileBytes = 64 << 10
	// tokenRandomBytes is 32 bytes = 256 bits of entropy.
	tokenRandomBytes = 32
)

// TokenConfig configures how `kodema sandbox serve` acquires its bearer token.
type TokenConfig struct {
	// Token supplies an explicit bearer token directly (primarily for tests/embedding).
	Token string
	// TokenFile reads the bearer token from an existing file (trimmed).
	TokenFile string
	// TokenEnv names the environment variable carrying the bearer token.
	// Defaults to DefaultTokenEnv ("KODEMA_SANDBOX_TOKEN").
	TokenEnv string
	// GenerateTokenFile generates a fresh 256-bit random bearer token and
	// writes it atomically with 0600 permissions to this path.
	GenerateTokenFile string
	// LookupEnv optionally overrides os.LookupEnv (for tests).
	LookupEnv func(string) (string, bool)
}

// Config configures one sandbox MCP server instance.
type Config struct {
	ListenAddr          string
	WorkspaceDir        string
	Token               string
	TokenEnv            string
	MaxCmdTimeout       time.Duration
	DefaultCmdTimeout   time.Duration
	MaxOutputBytes      int
	MaxFileBytes        int64
	MaxRequestBodyBytes int64
	MaxGlobResults      int
	MaxGrepMatches      int
	MaxGrepLineBytes    int
	ShellPath           string
	// ShutdownGrace bounds graceful HTTP drain after active child process groups are killed.
	ShutdownGrace time.Duration
}

// ResolveToken loads or generates the sandbox bearer token. It fails closed
// when no token source is configured.
func ResolveToken(cfg TokenConfig) (string, error) {
	tokenEnv := strings.TrimSpace(cfg.TokenEnv)
	if tokenEnv == "" {
		tokenEnv = DefaultTokenEnv
	}
	if err := validateEnvName(tokenEnv); err != nil {
		return "", err
	}

	if tok := strings.TrimSpace(cfg.Token); tok != "" {
		if err := validateTokenValue(tok); err != nil {
			return "", err
		}
		return tok, nil
	}

	tokenFile := strings.TrimSpace(cfg.TokenFile)
	genFile := strings.TrimSpace(cfg.GenerateTokenFile)
	if tokenFile != "" && genFile != "" {
		return "", errors.New("sandbox token-file and generate-token-file are mutually exclusive")
	}

	if tokenFile != "" {
		tok, err := readTokenFile(tokenFile)
		if err != nil {
			return "", err
		}
		return tok, nil
	}

	if genFile != "" {
		tok, err := GenerateBearerToken()
		if err != nil {
			return "", err
		}
		if err := writeTokenFileAtomic(genFile, tok); err != nil {
			return "", err
		}
		return tok, nil
	}

	lookup := cfg.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if raw, ok := lookup(tokenEnv); ok {
		tok := strings.TrimSpace(raw)
		if tok != "" {
			if err := validateTokenValue(tok); err != nil {
				return "", err
			}
			return tok, nil
		}
	}

	return "", fmt.Errorf("sandbox authentication token is required (set %s, --token-file, or --generate-token-file)", tokenEnv)
}

// GenerateBearerToken generates a fresh 256-bit cryptographically random bearer token.
func GenerateBearerToken() (string, error) {
	var raw [tokenRandomBytes]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate sandbox bearer token: %w", err)
	}
	return "kodsbox." + hex.EncodeToString(raw[:]), nil
}

func readTokenFile(path string) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", fmt.Errorf("open sandbox token file: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("stat sandbox token file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("sandbox token file must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxTokenFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("read sandbox token file: %w", err)
	}
	if len(data) > maxTokenFileBytes {
		return "", errors.New("sandbox token file exceeds size limit")
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", errors.New("sandbox token file is empty")
	}
	if err := validateTokenValue(tok); err != nil {
		return "", err
	}
	return tok, nil
}

func writeTokenFileAtomic(path, token string) error {
	cleaned := filepath.Clean(path)
	if strings.ContainsRune(cleaned, 0) {
		return errors.New("token file path contains NUL byte")
	}
	dir := filepath.Dir(cleaned)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create token directory: %w", err)
	}
	if info, err := os.Lstat(cleaned); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("refusing to write generated token to symlink path")
	}
	tmp, err := os.CreateTemp(dir, ".kodema-sandbox-token-*")
	if err != nil {
		return fmt.Errorf("create temp token file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp token file: %w", err)
	}
	if _, err := tmp.WriteString(token + "\n"); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp token file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync temp token file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp token file: %w", err)
	}
	if err := os.Rename(tmpName, cleaned); err != nil {
		return fmt.Errorf("install generated token file: %w", err)
	}
	cleanup = false
	return nil
}

func validateEnvName(name string) error {
	if name == "" || len(name) > 128 {
		return fmt.Errorf("invalid token environment variable name %q", name)
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		isLetter := (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || ch == '_'
		isDigit := ch >= '0' && ch <= '9'
		if i == 0 && !isLetter {
			return fmt.Errorf("invalid token environment variable name %q", name)
		}
		if !isLetter && !isDigit {
			return fmt.Errorf("invalid token environment variable name %q", name)
		}
	}
	return nil
}

func validateTokenValue(tok string) error {
	if len(tok) < 16 {
		return errors.New("sandbox authentication token must be at least 16 characters")
	}
	if len(tok) > 4096 {
		return errors.New("sandbox authentication token exceeds maximum length")
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] <= 0x20 || tok[i] == 0x7f {
			return errors.New("sandbox authentication token contains whitespace or control characters")
		}
	}
	return nil
}

// ValidateAndNormalize checks Config fields and applies safe defaults.
func (c Config) ValidateAndNormalize() (Config, error) {
	out := c
	if strings.TrimSpace(out.ListenAddr) == "" {
		out.ListenAddr = DefaultListenAddr
	}
	if err := validateListenAddr(out.ListenAddr); err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(out.WorkspaceDir) == "" {
		out.WorkspaceDir = DefaultWorkspaceDir
	}
	if strings.TrimSpace(out.Token) == "" {
		return Config{}, errors.New("sandbox server requires a non-empty bearer token")
	}
	if err := validateTokenValue(out.Token); err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(out.TokenEnv) == "" {
		out.TokenEnv = DefaultTokenEnv
	}
	if err := validateEnvName(out.TokenEnv); err != nil {
		return Config{}, err
	}
	if out.MaxCmdTimeout == 0 {
		out.MaxCmdTimeout = DefaultMaxCmdTimeout
	}
	if out.MaxCmdTimeout < 0 || out.MaxCmdTimeout > HardMaxCmdTimeout {
		return Config{}, fmt.Errorf("max-cmd-timeout must be between 1ms and %s", HardMaxCmdTimeout)
	}
	if out.DefaultCmdTimeout <= 0 || out.DefaultCmdTimeout > out.MaxCmdTimeout {
		out.DefaultCmdTimeout = min(DefaultCmdTimeout, out.MaxCmdTimeout)
	}
	if out.MaxOutputBytes == 0 {
		out.MaxOutputBytes = DefaultMaxOutputBytes
	}
	if out.MaxOutputBytes < 0 || out.MaxOutputBytes > HardMaxOutputBytes {
		return Config{}, fmt.Errorf("max-output-bytes must be between 1 and %d", HardMaxOutputBytes)
	}
	if out.MaxFileBytes == 0 {
		out.MaxFileBytes = DefaultMaxFileBytes
	}
	if out.MaxFileBytes < 0 || out.MaxFileBytes > HardMaxFileBytes {
		return Config{}, fmt.Errorf("max-file-bytes must be between 1 and %d", HardMaxFileBytes)
	}
	if out.MaxRequestBodyBytes <= 0 {
		// Ensure write_file up to MaxFileBytes comfortably fits in JSON-RPC body.
		minBody := out.MaxFileBytes*2 + (1 << 20)
		out.MaxRequestBodyBytes = max(DefaultMaxRequestBodyBytes, minBody)
	}
	if out.MaxGlobResults <= 0 {
		out.MaxGlobResults = DefaultMaxGlobResults
	}
	if out.MaxGrepMatches <= 0 {
		out.MaxGrepMatches = DefaultMaxGrepMatches
	}
	if out.MaxGrepLineBytes <= 0 {
		out.MaxGrepLineBytes = DefaultMaxGrepLineBytes
	}
	if out.ShutdownGrace <= 0 {
		out.ShutdownGrace = 5 * time.Second
	}
	return out, nil
}

func validateListenAddr(addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	if host != "" && host != "localhost" {
		if ip := net.ParseIP(host); ip == nil {
			return fmt.Errorf("invalid listen host %q: must be an IP address, localhost, or empty", host)
		}
	}
	port, err := net.LookupPort("tcp", portStr)
	if err != nil || port < 0 || port > 65535 {
		return fmt.Errorf("invalid listen port %q", portStr)
	}
	return nil
}
