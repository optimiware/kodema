package container

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// ValidateIdentity refuses an unusable persisted identity before any effect.
func ValidateIdentity(identity reconcile.EffectIdentity) error {
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

// StableDigest returns a short deterministic, DNS- and path-safe 24-hex-char
// SHA-256 prefix of value. Object names are derived only from persisted
// identities, never from randomness, so a retry finds the same object.
func StableDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:24]
}

// WorkspaceResourceName returns the deterministic runtime resource name (volume
// or PersistentVolumeClaim) for one retained workspace identity.
func WorkspaceResourceName(workspaceID id.WorkspaceID) (string, error) {
	if err := ValidateWorkspaceID(workspaceID); err != nil {
		return "", err
	}
	return "kodema-workspace-" + StableDigest(string(workspaceID)), nil
}

// DescribeIdentity renders a stable, non-secret diagnostic identity.
func DescribeIdentity(identity reconcile.EffectIdentity) string {
	return fmt.Sprintf("workload %q (execution %q action %q)", identity.WorkloadID, identity.ExecutionID, identity.ActionID)
}
