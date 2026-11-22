package container

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"os"
	"sort"
	"strings"

	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

const sandboxTokenSecretPrefix = "KODEMA_SBOX_TOK_"

// EnvVar is one literal container environment variable.
type EnvVar struct {
	Name  string
	Value string
}

// EnvOptions supplies the unshadowable runtime context appended after admitted
// literal environment entries.
type EnvOptions struct {
	KodemaURL   string
	ExecutionID id.ExecutionID
	ActionName  id.ActionName
	TaskID      id.TaskID
	CAFile      string
	GitURL      string
	Toolbox     bool
}

// ValidateContainerEnv checks that every admitted environment entry carries a
// valid non-reserved name.
func ValidateContainerEnv(env []definition.EnvVar) error {
	for index, entry := range env {
		if _, err := id.ParseEnvName(string(entry.Name)); err != nil {
			return fault.Wrap(fault.InvalidInput,
				fmt.Sprintf("container environment entry %d is invalid", index), err)
		}
	}
	return nil
}

// BuildContainerEnv merges admitted literal environment entries in declaration
// order (preserving KODEMA_LLM_*_BASE_URL entries intact) while stripping any
// variables controlled by the runtime, then appends reserved context, optional
// CA file, and scoped Git configuration in canonical order.
func BuildContainerEnv(admitted []definition.EnvVar, opts EnvOptions) ([]EnvVar, error) {
	gitEnv, err := workspace.GitEnvironment(opts.GitURL, opts.CAFile, opts.Toolbox)
	if err != nil {
		return nil, err
	}

	env := make([]EnvVar, 0, len(admitted)+len(gitEnv)+5)
	for _, entry := range admitted {
		name := string(entry.Name)
		if isReservedRuntimeEnv(name) {
			continue
		}
		if opts.CAFile != "" && name == workspace.EnvCAFile {
			continue
		}
		if len(gitEnv) > 0 && (name == workspace.EnvGitURL || workspace.IsGitConfigEnv(name)) {
			continue
		}
		env = append(env, EnvVar{Name: name, Value: entry.Value})
	}

	env = append(env,
		EnvVar{Name: id.EnvURL, Value: opts.KodemaURL},
		EnvVar{Name: id.EnvExecution, Value: string(opts.ExecutionID)},
		EnvVar{Name: id.EnvAction, Value: string(opts.ActionName)},
	)
	if opts.TaskID != "" {
		env = append(env, EnvVar{Name: id.EnvTask, Value: string(opts.TaskID)})
	}
	if opts.CAFile != "" {
		env = append(env, EnvVar{Name: workspace.EnvCAFile, Value: opts.CAFile})
	}
	for _, entry := range gitEnv {
		name, value, _ := strings.Cut(entry, "=")
		env = append(env, EnvVar{Name: name, Value: value})
	}
	return env, nil
}

func isReservedRuntimeEnv(name string) bool {
	return name == id.EnvURL ||
		name == id.EnvExecution ||
		name == id.EnvAction ||
		name == id.EnvTask
}

// EnvStrings formats literal environment entries as "NAME=VALUE" strings.
func EnvStrings(vars []EnvVar) []string {
	out := make([]string, len(vars))
	for index, entry := range vars {
		out[index] = entry.Name + "=" + entry.Value
	}
	return out
}

// BuildMaterialization validates and builds the complete all-or-nothing
// activation snapshot. A zero mode defaults to ReadOnlyFileMode. Validation
// completes before any external call so a partial or unsafe snapshot never
// reaches a runtime boundary.
func BuildMaterialization(request reconcile.PrepareRequest, mode os.FileMode) (workspace.Materialization, error) {
	if mode == 0 {
		mode = ReadOnlyFileMode
	}
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

	secrets := make([]reconcile.SecretValue, len(request.Secrets))
	for index, secret := range request.Secrets {
		if _, err := workspace.SecretPath(secret.Name); err != nil {
			return workspace.Materialization{}, err
		}
		secrets[index] = reconcile.SecretValue{Name: secret.Name, Value: secret.Value}
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

// PreparationDigest computes a deterministic 48-hex-character SHA-256 prefix
// (a valid Kubernetes label value) over the full activation declaration,
// excluding only the mutable execution credential so RefreshCredential never
// invalidates the digest.
func PreparationDigest(request reconcile.PrepareRequest, materialization workspace.Materialization, kodemaURL string) string {
	digest := sha256.New()
	writeDigestString(digest, string(request.Identity.ExecutionID))
	writeDigestString(digest, string(request.Identity.ActionID))
	writeDigestString(digest, string(request.Identity.WorkloadID))
	writeDigestString(digest, request.Identity.IdempotencyKey)
	writeDigestString(digest, string(request.WorkspaceID))
	writeDigestString(digest, string(request.ActionName))
	writeDigestString(digest, string(request.TaskID))
	writeDigestString(digest, request.Container.Image)
	writeDigestCount(digest, len(request.Container.Commands))
	for _, command := range request.Container.Commands {
		writeDigestString(digest, command)
	}
	writeDigestCount(digest, len(request.Container.Env))
	for _, entry := range request.Container.Env {
		writeDigestString(digest, string(entry.Name))
		writeDigestString(digest, entry.Value)
	}
	if len(request.Container.Outputs) > 0 {
		writeDigestString(digest, "outputs:present")
		writeDigestCount(digest, len(request.Container.Outputs))
		for _, output := range request.Container.Outputs {
			writeDigestString(digest, string(output))
		}
	}
	if request.Container.Resources != nil {
		writeDigestString(digest, "resources:present")
		writeDigestString(digest, request.Container.Resources.Memory)
		writeDigestString(digest, request.Container.Resources.CPU)
	}
	if len(request.Caches) > 0 {
		writeDigestString(digest, "caches:present")
		writeDigestCount(digest, len(request.Caches))
		for _, cache := range request.Caches {
			writeDigestString(digest, cache.Name)
			writeDigestString(digest, cache.Key)
		}
	}
	writeDigestString(digest, kodemaURL)
	writeDigestCount(digest, len(materialization.Files))
	for _, file := range materialization.Files {
		writeDigestString(digest, file.Path)
		writeDigestBytes(digest, file.Data)
	}
	secrets := append([]reconcile.SecretValue(nil), materialization.Secrets...)
	sort.Slice(secrets, func(left, right int) bool { return secrets[left].Name < secrets[right].Name })
	writeDigestCount(digest, len(secrets))
	for _, secret := range secrets {
		writeDigestString(digest, string(secret.Name))
		writeDigestString(digest, secret.Value)
	}
	if request.Task != nil {
		writeDigestString(digest, "task:present")
		writeDigestBytes(digest, request.Task)
	} else {
		writeDigestString(digest, "task:absent")
	}
	return hex.EncodeToString(digest.Sum(nil))[:48]
}

func writeDigestCount(digest hash.Hash, count int) {
	fmt.Fprintf(digest, "%d#", count)
}

func writeDigestString(digest hash.Hash, value string) {
	fmt.Fprintf(digest, "%d:", len(value))
	digest.Write([]byte(value))
}

func writeDigestBytes(digest hash.Hash, value []byte) {
	fmt.Fprintf(digest, "%d:", len(value))
	digest.Write(value)
}

// WorkloadRole returns the optional kodema.dev/role label value for a prepared
// workload. Ephemeral OpenCode sandbox actions (`name: sandbox` explicitly
// invoking `/kodema/bin/kodema sandbox serve` and declaring no non-sandbox
// secrets) carry RoleSandbox so tenant NetworkPolicies can select sandbox pods
// strictly by tenant, project, and role without exposing privileged workloads.
func WorkloadRole(request reconcile.PrepareRequest) string {
	if request.ActionName != RoleSandbox {
		return ""
	}
	for _, secretName := range request.Container.Secrets {
		if !strings.HasPrefix(string(secretName), sandboxTokenSecretPrefix) {
			return ""
		}
	}
	for _, secret := range request.Secrets {
		if !strings.HasPrefix(string(secret.Name), sandboxTokenSecretPrefix) {
			return ""
		}
	}
	for _, cmd := range request.Container.Commands {
		for rawLine := range strings.SplitSeq(cmd, "\n") {
			line := strings.TrimSpace(rawLine)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if line == "/kodema/bin/kodema sandbox serve" ||
				strings.HasPrefix(line, "/kodema/bin/kodema sandbox serve ") ||
				line == "exec /kodema/bin/kodema sandbox serve" ||
				strings.HasPrefix(line, "exec /kodema/bin/kodema sandbox serve ") {
				return RoleSandbox
			}
		}
	}
	return ""
}

// ValidatePinnedImage refuses an image reference that is not pinned to an
// immutable sha256 digest.
func ValidatePinnedImage(image string) error {
	if strings.TrimSpace(image) == "" {
		return fault.New(fault.InvalidInput, "container image is empty")
	}
	if _, err := pipeline.ParseImageReference(image); err != nil {
		if class, ok := fault.ClassOf(err); ok && class == fault.InvalidInput {
			return err
		}
		return fault.Wrap(fault.InvalidInput, "container image is not pinned to a digest", err)
	}
	return nil
}
