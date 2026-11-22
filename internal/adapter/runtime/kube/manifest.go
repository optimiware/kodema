// Manifest construction for the Kubernetes runtime adapter.
//
// Everything here is pure: it turns the persisted effect identity and the
// admitted container declaration into names, an activation snapshot and one Job
// manifest. No object is written and no cluster is contacted.
package kube

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"maps"
	"sort"
	"strings"

	"github.com/optimiware/kodema/internal/adapter/runtime/internal/adapterutil"
	"github.com/optimiware/kodema/internal/control/pipeline"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/engine/script"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// activationRoot is the read-only mount of the immutable activation snapshot.
const activationRoot = "/kodema"

// readOnlyMode is the explicit read-only mode for every projected file.
const readOnlyMode = 0o444

// workloadCAVolumeName is the pinned name of the installation CA volume.
const workloadCAVolumeName = "workload-ca"

const (
	toolboxPlanKey = "/kodema/toolbox-plan.json" // Control-plane data, never projected.
	workloadScript = "/kodema/script"
)

func validateToolbox(image, gitURL string) error {
	if image != "" {
		if _, err := pipeline.ParseImageReference(image); err != nil {
			return fault.Wrap(fault.InvalidInput, "toolbox image must be pinned by digest", err)
		}
	}
	if gitURL != "" {
		if _, err := workspace.GitOrigin(gitURL); err != nil {
			return err
		}
	}
	return nil
}

// activationConfigMapName returns the stable activation snapshot name.
func activationConfigMapName(workload id.WorkloadID) string {
	return "kodema-activation-" + adapterutil.StableDigest(string(workload))
}

// credentialSecretName returns the stable credential Secret name.
func credentialSecretName(workload id.WorkloadID) string {
	return "kodema-credential-" + adapterutil.StableDigest(string(workload))
}

// workloadJobName returns the stable Job name. A retry must find exactly this
// Job, never a generated replacement.
func workloadJobName(workload id.WorkloadID) string {
	return "kodema-job-" + adapterutil.StableDigest(string(workload))
}

// labels returns the pinned tenant/execution/action/workload/workspace labels
// shared by every owned object.
func (a *Adapter) labels(identity reconcile.EffectIdentity, workspaceID id.WorkspaceID) map[string]string {
	return map[string]string{
		LabelTenant:    string(a.tenant),
		LabelExecution: string(identity.ExecutionID),
		LabelAction:    string(identity.ActionID),
		LabelWorkload:  string(identity.WorkloadID),
		LabelWorkspace: string(workspaceID),
	}
}

// validKubeLabelValue reports whether value satisfies the Kubernetes label
// value grammar: 1..63 chars matching ^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$.
func validKubeLabelValue(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	isAlnum := func(c byte) bool {
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	}
	if !isAlnum(value[0]) || !isAlnum(value[len(value)-1]) {
		return false
	}
	for i := 1; i < len(value)-1; i++ {
		c := value[i]
		if !isAlnum(c) && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

const sandboxTokenSecretPrefix = "KODEMA_SBOX_TOK_"

// workloadRole returns the optional kodema.dev/role label value for a prepared
// workload. Ephemeral OpenCode sandbox actions (`name: sandbox` explicitly
// invoking `/kodema/bin/kodema sandbox serve` and declaring no non-sandbox
// secrets) carry RoleSandbox so tenant NetworkPolicies can select sandbox pods
// strictly by tenant, project, and role without exposing privileged workloads.
func workloadRole(request reconcile.PrepareRequest) string {
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
		for _, rawLine := range strings.Split(cmd, "\n") {
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

// activationConfigMap builds the immutable activation snapshot.
func (a *Adapter) activationConfigMap(identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, materialization workspace.Materialization, digest string) ConfigMap {
	data := make(map[string][]byte, len(materialization.Files))
	for _, file := range materialization.Files {
		data[file.Path] = append([]byte(nil), file.Data...)
	}
	labels := a.labels(identity, workspaceID)
	labels[LabelPreparation] = digest
	return ConfigMap{
		Name:      activationConfigMapName(identity.WorkloadID),
		Namespace: a.namespace,
		Labels:    labels,
		Data:      data,
		Immutable: true,
	}
}

// preparationDigest computes a deterministic 48-hex-character SHA-256 prefix
// (a valid Kubernetes label value) over the full activation declaration,
// excluding only the mutable execution credential.
func (a *Adapter) preparationDigest(request reconcile.PrepareRequest, materialization workspace.Materialization) string {
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
	writeDigestString(digest, a.kodemaURL)
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

// writeDigestCount encodes one slice length unambiguously into the digest.
func writeDigestCount(digest hash.Hash, count int) {
	fmt.Fprintf(digest, "%d#", count)
}

// writeDigestString length-prefixes one string value so concatenation is
// unambiguous.
func writeDigestString(digest hash.Hash, value string) {
	fmt.Fprintf(digest, "%d:", len(value))
	digest.Write([]byte(value))
}

// writeDigestBytes length-prefixes one byte slice so concatenation is
// unambiguous.
func writeDigestBytes(digest hash.Hash, value []byte) {
	fmt.Fprintf(digest, "%d:", len(value))
	digest.Write(value)
}

// credentialSecret builds the mutable credential and declared-secret mount.
func (a *Adapter) credentialSecret(identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, materialization workspace.Materialization) Secret {
	data := make(map[string][]byte, len(materialization.Secrets)+1)
	for _, secret := range materialization.Secrets {
		data[string(secret.Name)] = []byte(secret.Value)
	}
	data[id.SecretTokenName] = []byte(materialization.Credential)
	return Secret{
		Name:      credentialSecretName(identity.WorkloadID),
		Namespace: a.namespace,
		Labels:    a.labels(identity, workspaceID),
		Data:      data,
	}
}

// resolveResources validates any per-action resource hints against the
// installation's Kubernetes Policy maximums and returns the concrete requests
// and limits for the workload Job. When no per-action resources are declared,
// the installation Policy values are used verbatim.
func (a *Adapter) resolveResources(requested *definition.Resources) (Resources, error) {
	resolved := Resources{
		CPURequest:    a.policy.CPURequest,
		CPULimit:      a.policy.CPULimit,
		MemoryRequest: a.policy.MemoryRequest,
		MemoryLimit:   a.policy.MemoryLimit,
	}
	if requested == nil {
		return resolved, nil
	}
	if err := definition.ValidateResources(requested); err != nil {
		return Resources{}, err
	}
	if requested.Memory != "" {
		wantBytes, err := definition.ParseMemoryQuantity(requested.Memory)
		if err != nil {
			return Resources{}, err
		}
		maxRaw := strings.TrimSpace(a.policy.MaxMemoryLimit)
		if maxRaw == "" {
			maxRaw = strings.TrimSpace(a.policy.MemoryLimit)
		}
		maxBytes, err := definition.ParseMemoryQuantity(maxRaw)
		if err != nil {
			return Resources{}, fault.New(fault.InvalidInput,
				fmt.Sprintf("kubernetes installation memory maximum %q is not a bounded quantity", maxRaw))
		}
		if wantBytes > maxBytes {
			return Resources{}, fault.New(fault.InvalidInput, fmt.Sprintf(
				"container memory limit %q (%s) exceeds installation maximum %q (%s)",
				requested.Memory, definition.FormatMemoryBytes(wantBytes), maxRaw, definition.FormatMemoryBytes(maxBytes)))
		}
		resolved.MemoryLimit = requested.Memory
		if reqBytes, err := definition.ParseMemoryQuantity(strings.TrimSpace(resolved.MemoryRequest)); err == nil && reqBytes > wantBytes {
			resolved.MemoryRequest = requested.Memory
		}
	}
	if requested.CPU != "" {
		wantMillicores, err := definition.ParseCPUQuantity(requested.CPU)
		if err != nil {
			return Resources{}, err
		}
		maxRaw := strings.TrimSpace(a.policy.MaxCPULimit)
		if maxRaw == "" {
			maxRaw = strings.TrimSpace(a.policy.CPULimit)
		}
		maxMillicores, err := definition.ParseCPUQuantity(maxRaw)
		if err != nil {
			return Resources{}, fault.New(fault.InvalidInput,
				fmt.Sprintf("kubernetes installation CPU maximum %q is not a bounded quantity", maxRaw))
		}
		if wantMillicores > maxMillicores {
			return Resources{}, fault.New(fault.InvalidInput, fmt.Sprintf(
				"container CPU limit %q (%s) exceeds installation maximum %q (%s)",
				requested.CPU, definition.FormatCPUMillicores(wantMillicores), maxRaw, definition.FormatCPUMillicores(maxMillicores)))
		}
		resolved.CPULimit = requested.CPU
		if reqMillicores, err := definition.ParseCPUQuantity(strings.TrimSpace(resolved.CPURequest)); err == nil && reqMillicores > wantMillicores {
			resolved.CPURequest = requested.CPU
		}
	}
	return resolved, nil
}

// jobManifest builds the pinned, fail-fast, non-replaying Job manifest.
func (a *Adapter) jobManifest(prepared preparedWorkload, identity reconcile.EffectIdentity) Job {
	labels := a.labels(identity, prepared.workspace)
	if prepared.project != "" {
		labels[LabelProject] = string(prepared.project)
	}
	if prepared.role != "" {
		labels[LabelRole] = prepared.role
	}
	podLabels := make(map[string]string, len(labels))
	maps.Copy(podLabels, labels)

	env := make([]EnvVar, 0, len(prepared.container.env)+4)
	for _, entry := range prepared.container.env {
		if (prepared.caSecret != "" && entry.Name == workspace.EnvCAFile) ||
			(len(prepared.gitEnv) > 0 && (entry.Name == workspace.EnvGitURL || workspace.IsGitConfigEnv(entry.Name))) {
			continue
		}
		env = append(env, entry)
	}
	env = append(env,
		EnvVar{Name: id.EnvURL, Value: a.kodemaURL},
		EnvVar{Name: id.EnvExecution, Value: string(identity.ExecutionID)},
		EnvVar{Name: id.EnvAction, Value: string(prepared.actionName)},
	)
	if prepared.taskID != "" {
		env = append(env, EnvVar{Name: id.EnvTask, Value: string(prepared.taskID)})
	}
	for _, entry := range prepared.gitEnv {
		name, value, _ := strings.Cut(entry, "=")
		env = append(env, EnvVar{Name: name, Value: value})
	}

	activationItems := make([]VolumeItem, 0, len(prepared.activationPaths))
	var previousOutputs []VolumeItem
	for _, path := range prepared.activationPaths {
		item := VolumeItem{Key: path, Path: path, Mode: readOnlyMode}
		// ConfigMap projection exposes top-level entries through symlinks.
		// Don't project "outputs" as a symlink where kubelet needs a real
		// directory for the nested writable mount.
		if prepared.outputDir != "" && strings.HasPrefix(path, workspace.OutputRoot+"/") {
			previousOutputs = append(previousOutputs, item)
		} else {
			activationItems = append(activationItems, item)
		}
	}

	credentialItems := make([]VolumeItem, 0, len(prepared.secretNames)+1)
	for _, name := range prepared.secretNames {
		path, err := workspace.SecretPath(name)
		if err != nil {
			continue
		}
		credentialItems = append(credentialItems, VolumeItem{Key: string(name), Path: path, Mode: readOnlyMode})
	}
	credentialItems = append(credentialItems, VolumeItem{
		Key:  id.SecretTokenName,
		Path: workspace.ReservedCredentialPath,
		Mode: readOnlyMode,
	})

	ttl := a.policy.JobTTLSeconds
	volumes := []Volume{
		{
			Name:      "workspace",
			MountPath: workspace.WorkspaceRoot,
			ClaimName: prepared.claimName,
			ReadOnly:  false,
		},
		{
			Name:          "activation",
			MountPath:     activationRoot,
			ConfigMapName: prepared.configMap,
			ReadOnly:      true,
			Items:         activationItems,
		},
		{
			Name:       "credential",
			MountPath:  workspace.SecretRoot,
			SecretName: prepared.secret,
			ReadOnly:   true,
			Items:      credentialItems,
		},
	}
	// The installation CA is an optional fourth, read-only Secret volume. It
	// projects exactly the literal ca.crt key at the fixed trust path, outside
	// the workspace, activation and credential mounts.
	if prepared.caSecret != "" {
		env = append(env, EnvVar{Name: workspace.EnvCAFile, Value: WorkloadCAPath})
		volumes = append(volumes, Volume{
			Name:       workloadCAVolumeName,
			MountPath:  WorkloadCAMountPath,
			SecretName: prepared.caSecret,
			ReadOnly:   true,
			Items: []VolumeItem{{
				Key:  WorkloadCAKey,
				Path: WorkloadCAPath,
				Mode: readOnlyMode,
			}},
		})
	}
	workingDir := ""
	if prepared.toolboxImage != "" {
		workingDir = workspace.WorkspaceRoot
		volumes = append(volumes, Volume{Name: "toolbox", MountPath: workspace.ToolboxRoot, ReadOnly: true, EmptyDir: true})
	}
	if prepared.outputDir != "" {
		if len(previousOutputs) != 0 {
			volumes = append(volumes, Volume{Name: "previous-outputs", MountPath: workspace.OutputRoot, ConfigMapName: prepared.configMap, ReadOnly: true, Items: previousOutputs})
		}
		volumes = append(volumes, Volume{Name: "action-outputs", MountPath: prepared.outputDir, EmptyDir: true, MemoryBytes: workspace.OutputDirectoryBytes})
	}
	for _, cache := range prepared.caches {
		volName := "cache-" + cache.key[:16]
		mountPath := workspace.CacheRoot + "/" + cache.name
		if cache.claimName != "" {
			volumes = append(volumes, Volume{
				Name:      volName,
				MountPath: mountPath,
				ClaimName: cache.claimName,
				SubPath:   cache.key,
				ReadOnly:  false,
			})
		} else {
			volumes = append(volumes, Volume{
				Name:      volName,
				MountPath: mountPath,
				EmptyDir:  true,
				ReadOnly:  false,
			})
		}
	}
	command := []string{script.ShellPath, "-ec", prepared.container.script}
	if prepared.toolboxImage != "" {
		command = workspace.Entrypoint(workloadScript, workspace.Launch{Toolbox: true, Outputs: prepared.outputDir != ""})
	}
	resources := prepared.resources
	if resources.CPURequest == "" {
		resources.CPURequest = a.policy.CPURequest
	}
	if resources.CPULimit == "" {
		resources.CPULimit = a.policy.CPULimit
	}
	if resources.MemoryRequest == "" {
		resources.MemoryRequest = a.policy.MemoryRequest
	}
	if resources.MemoryLimit == "" {
		resources.MemoryLimit = a.policy.MemoryLimit
	}
	return Job{
		Name:      prepared.job,
		Namespace: a.namespace,
		Labels:    labels,
		Annotations: map[string]string{
			AnnotationIdempotencyKey: identity.IdempotencyKey,
		},
		Spec: JobSpec{
			Image:                        prepared.container.image,
			Command:                      command,
			ToolboxImage:                 prepared.toolboxImage,
			WorkingDir:                   workingDir,
			Env:                          env,
			Resources:                    resources,
			Volumes:                      volumes,
			RestartPolicy:                "Never",
			BackoffLimit:                 0,
			PodReplacementPolicy:         "Failed",
			TTLSecondsAfterFinished:      &ttl,
			AutomountServiceAccountToken: false,
			HostNetwork:                  false,
			PodLabels:                    podLabels,
			ImagePullSecrets:             append([]string(nil), a.imagePullSecrets...),
		},
	}
}
