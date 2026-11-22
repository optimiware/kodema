package app

import "github.com/optimiware/kodema/internal/apps/kit/servicekit"

// DefaultGitOutputBytes bounds one native Git stream when the configuration
// leaves the bound unset.
const DefaultGitOutputBytes = servicekit.DefaultGitOutputBytes

// OSGitExecutorConfig configures the native Git command executor.
type OSGitExecutorConfig = servicekit.OSGitExecutorConfig

// OSGitExecutor implements GitCommandExecutor over os/exec without a shell.
type OSGitExecutor = servicekit.OSGitExecutor

// NewOSGitExecutor validates the configuration and returns the executor.
var NewOSGitExecutor = servicekit.NewOSGitExecutor
