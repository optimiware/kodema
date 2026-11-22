// Manifest construction for the Kubernetes runtime adapter.
//
// Everything here is pure: it turns the persisted effect identity and the
// admitted container declaration into names, an activation snapshot and one Job
// manifest. No object is written and no cluster is contacted.
package kube

import (
	"maps"
	"strings"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// workloadCAVolumeName is the pinned name of the installation CA volume.
const workloadCAVolumeName = "workload-ca"

// toolboxPlanKey stores control-plane toolbox metadata in the immutable
// activation ConfigMap; it is never projected into the workload container.
const toolboxPlanKey = "/kodema/toolbox-plan.json"

func validateToolbox(image, gitURL string) error {
	if image != "" {
		if err := container.ValidatePinnedImage(image); err != nil {
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
	return "kodema-activation-" + container.StableDigest(string(workload))
}

// credentialSecretName returns the stable credential Secret name.
func credentialSecretName(workload id.WorkloadID) string {
	return "kodema-credential-" + container.StableDigest(string(workload))
}

// workloadJobName returns the stable Job name. A retry must find exactly this
// Job, never a generated replacement.
func workloadJobName(workload id.WorkloadID) string {
	return "kodema-job-" + container.StableDigest(string(workload))
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

// activationConfigMap builds the immutable activation snapshot, always
// including the rendered fail-fast entrypoint script at /kodema/script.
func (a *Adapter) activationConfigMap(identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, materialization workspace.Materialization, renderedScript, digest string) ConfigMap {
	data := make(map[string][]byte, len(materialization.Files)+1)
	for _, file := range materialization.Files {
		data[file.Path] = append([]byte(nil), file.Data...)
	}
	data[container.WorkloadScriptPath] = []byte(renderedScript)
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
	res, err := a.resourcePolicy.Resolve(requested)
	if err != nil {
		return Resources{}, err
	}
	if requested.Memory != "" {
		resolved.MemoryLimit = res.MemoryQuantity
		resolved.MemoryRequest = container.ClampKubeRequest(a.policy.MemoryRequest, res.MemoryBytes, true)
	}
	if requested.CPU != "" {
		resolved.CPULimit = res.CPUQuantity
		resolved.CPURequest = container.ClampKubeRequest(a.policy.CPURequest, res.CPUMillicores, false)
	}
	return resolved, nil
}

// jobManifest builds the pinned, fail-fast, non-replaying Job manifest.
func (a *Adapter) jobManifest(prepared preparedWorkload, identity reconcile.EffectIdentity) (Job, error) {
	labels := a.labels(identity, prepared.workspace)
	if prepared.project != "" {
		labels[LabelProject] = string(prepared.project)
	}
	if prepared.role != "" {
		labels[LabelRole] = prepared.role
	}
	podLabels := make(map[string]string, len(labels))
	maps.Copy(podLabels, labels)

	caFile := ""
	if prepared.caSecret != "" {
		caFile = WorkloadCAPath
	}
	builtEnv, err := container.BuildContainerEnv(prepared.container.env, container.EnvOptions{
		KodemaURL:   a.kodemaURL,
		ExecutionID: identity.ExecutionID,
		ActionName:  prepared.actionName,
		TaskID:      prepared.taskID,
		CAFile:      caFile,
		GitURL:      prepared.gitURL,
		Toolbox:     prepared.toolboxImage != "",
	})
	if err != nil {
		return Job{}, err
	}
	env := make([]EnvVar, len(builtEnv))
	for index, entry := range builtEnv {
		env[index] = EnvVar{Name: entry.Name, Value: entry.Value}
	}

	activationItems := make([]VolumeItem, 0, len(prepared.activationPaths))
	var previousOutputs []VolumeItem
	for _, path := range prepared.activationPaths {
		item := VolumeItem{Key: path, Path: path, Mode: container.ReadOnlyFileMode}
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
		credentialItems = append(credentialItems, VolumeItem{Key: string(name), Path: path, Mode: container.ReadOnlyFileMode})
	}
	credentialItems = append(credentialItems, VolumeItem{
		Key:  id.SecretTokenName,
		Path: workspace.ReservedCredentialPath,
		Mode: container.ReadOnlyFileMode,
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
			MountPath:     container.ActivationRoot,
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
		volumes = append(volumes, Volume{
			Name:       workloadCAVolumeName,
			MountPath:  WorkloadCAMountPath,
			SecretName: prepared.caSecret,
			ReadOnly:   true,
			Items: []VolumeItem{{
				Key:  WorkloadCAKey,
				Path: WorkloadCAPath,
				Mode: container.ReadOnlyFileMode,
			}},
		})
	}
	if prepared.toolboxImage != "" {
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
	command := workspace.Entrypoint(container.WorkloadScriptPath, workspace.Launch{
		Toolbox: prepared.toolboxImage != "",
		Outputs: prepared.outputDir != "",
	})
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
			WorkingDir:                   workspace.WorkspaceRoot,
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
	}, nil
}
