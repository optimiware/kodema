// Package adapterutil holds pure, runtime-adapter-internal helpers shared by
// the Kubernetes, Podman, and Process runtime adapters.
package adapterutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
	"github.com/optimiware/kodema/internal/kernel/engine"
	"github.com/optimiware/kodema/internal/shell/workspace"
)

// Redacted replaces configured credential and secret values in log output.
const Redacted = "[REDACTED]"

// ValidateIdentity refuses an unusable persisted identity before any effect.
func ValidateIdentity(identity engine.EffectIdentity) error {
	if _, err := id.ParseWorkloadID(string(identity.WorkloadID)); err != nil {
		return fault.Wrap(fault.InvalidInput, "workload id is invalid", err)
	}
	if _, err := id.ParseExecutionID(string(identity.ExecutionID)); err != nil {
		return fault.Wrap(fault.InvalidInput, "execution id is invalid", err)
	}
	if _, err := id.ParseActionID(string(identity.ActionID)); err != nil {
		return fault.Wrap(fault.InvalidInput, "action id is invalid", err)
	}
	if strings.TrimSpace(identity.IdempotencyKey) == "" {
		return fault.New(fault.InvalidInput, "idempotency key is empty")
	}
	return nil
}

// ValidateWorkspaceID refuses an unusable workspace identity before any effect.
func ValidateWorkspaceID(workspaceID id.WorkspaceID) error {
	if _, err := id.ParseWorkspaceID(string(workspaceID)); err != nil {
		return fault.Wrap(fault.InvalidInput, "workspace id is invalid", err)
	}
	return nil
}

// ValidatePrepareContext refuses an unusable action slug or task identity
// before any effect. An empty task id is valid and means the execution is
// task-free; an action slug is always required.
func ValidatePrepareContext(actionName id.ActionName, taskID id.TaskID) error {
	if _, err := id.ParseActionName(string(actionName)); err != nil {
		return fault.Wrap(fault.InvalidInput, "action name is invalid", err)
	}
	if taskID != "" {
		if _, err := id.ParseTaskID(string(taskID)); err != nil {
			return fault.Wrap(fault.InvalidInput, "task id is invalid", err)
		}
	}
	return nil
}

// StableDigest returns a short deterministic, DNS- and path-safe digest of a
// stable identity. Object names are derived only from persisted identities,
// never from randomness, so a retry finds the same object.
func StableDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:24]
}

// DescribeIdentity renders a stable, non-secret diagnostic identity.
func DescribeIdentity(identity engine.EffectIdentity) string {
	return fmt.Sprintf("workload %q (execution %q action %q)", identity.WorkloadID, identity.ExecutionID, identity.ActionID)
}

// RealClock is the production clock shared by runtime adapters. It never
// appears in unit tests.
type RealClock struct{}

// Now returns the current wall-clock time.
func (RealClock) Now() time.Time { return time.Now() }

// Sleep waits for the duration or until the context ends.
func (RealClock) Sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// BuildMaterialization validates and builds the complete all-or-nothing
// activation snapshot. Validation happens before any external call so a partial
// or unsafe snapshot never reaches the runtime boundary.
func BuildMaterialization(request engine.PrepareRequest, mode os.FileMode) (workspace.Materialization, error) {
	if _, err := id.ParseWorkspaceID(string(request.WorkspaceID)); err != nil {
		return workspace.Materialization{}, fault.Wrap(fault.InvalidInput, "preparation workspace id is invalid", err)
	}
	files := make([]workspace.File, 0, len(request.Inputs)+len(request.Outputs)+len(request.TaskFiles)+1)

	inputKeys := make([]string, 0, len(request.Inputs))
	for key := range request.Inputs {
		inputKeys = append(inputKeys, string(key))
	}
	sort.Strings(inputKeys)
	for _, raw := range inputKeys {
		path, err := workspace.InputPath(id.InputKey(raw))
		if err != nil {
			return workspace.Materialization{}, err
		}
		files = append(files, workspace.File{Path: path, Data: []byte(request.Inputs[id.InputKey(raw)]), Mode: mode})
	}

	actionNames := make([]string, 0, len(request.Outputs))
	for action := range request.Outputs {
		actionNames = append(actionNames, string(action))
	}
	sort.Strings(actionNames)
	for _, rawAction := range actionNames {
		action := id.ActionName(rawAction)
		outputs := request.Outputs[action]
		outputKeys := make([]string, 0, len(outputs))
		for key := range outputs {
			outputKeys = append(outputKeys, string(key))
		}
		sort.Strings(outputKeys)
		for _, rawKey := range outputKeys {
			path, err := workspace.OutputPath(action, id.OutputKey(rawKey))
			if err != nil {
				return workspace.Materialization{}, err
			}
			files = append(files, workspace.File{Path: path, Data: []byte(outputs[id.OutputKey(rawKey)]), Mode: mode})
		}
	}

	if request.Task != nil {
		files = append(files, workspace.File{Path: workspace.TaskPath, Data: append([]byte(nil), request.Task...), Mode: mode})
	}
	taskFiles, err := workspace.TaskMaterializationFiles(request.TaskFiles, mode)
	if err != nil {
		return workspace.Materialization{}, err
	}
	files = append(files, taskFiles...)

	secrets := make([]engine.SecretValue, len(request.Secrets))
	for index, secret := range request.Secrets {
		if _, err := workspace.SecretPath(secret.Name); err != nil {
			return workspace.Materialization{}, err
		}
		secrets[index] = engine.SecretValue{Name: secret.Name, Value: secret.Value}
	}

	materialization := workspace.Materialization{
		WorkspaceID: request.WorkspaceID,
		Files:       files,
		Secrets:     secrets,
		Credential:  request.Credential,
		Task:        request.Task,
	}
	if err := materialization.Validate(); err != nil {
		return workspace.Materialization{}, err
	}
	return materialization, nil
}
