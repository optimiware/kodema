package runtime

import (
	"context"

	"github.com/optimiware/kodema/internal/id"
)

type executionProjectContextKey struct{}

// WithExecutionProject attaches the durable owning project identity of an
// execution to ctx for Shell runtime adapters that label workload pods with
// project metadata (for example Kubernetes NetworkPolicy pod selectors).
func WithExecutionProject(ctx context.Context, project id.ProjectID) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if project == "" {
		return ctx
	}
	return context.WithValue(ctx, executionProjectContextKey{}, project)
}

// ExecutionProject returns the owning project identity previously attached with
// WithExecutionProject, or empty when absent.
func ExecutionProject(ctx context.Context) id.ProjectID {
	if ctx == nil {
		return ""
	}
	project, _ := ctx.Value(executionProjectContextKey{}).(id.ProjectID)
	return project
}
