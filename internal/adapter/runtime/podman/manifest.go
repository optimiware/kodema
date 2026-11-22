// Manifest construction and host-layout rules for the Podman runtime adapter.
//
// Everything here is pure: it turns the admitted activation declaration into
// confined host paths, an in-process prepared workload descriptor, and one
// pinned container spec. No file is written and no libpod call is made.
package podman

import (
	"os"
	"path/filepath"

	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/id"
)

// Host layout constants. Non-secret activation files under /kodema are written
// at filepath.Join(<root>/<workload>, P) and bind-mounted read-only at /kodema.
// Declared secrets and KODEMA_TOKEN are isolated under a 0700 host parent
// directory <root>/<workload>/secrets with a 0755 child
// <root>/<workload>/secrets/data bind-mounted read-only at /kodema/secrets, so
// host confinement holds while non-root container users can still traverse
// /kodema/secrets and read 0444 secret files. The shared installation CA bundle
// (when configured) lives at <root>/.ca/<digest>/ca.crt and is bind-mounted
// read-only at /etc/kodema/ca.
const (
	// activationRoot is the read-only bind destination of the activation data.
	activationRoot = container.ActivationRoot
	// entrypointScriptPath is the rendered fail-fast script inside the bind.
	entrypointScriptPath = container.WorkloadScriptPath
	// readOnlyMode is the explicit read-only mode of every prepared file.
	readOnlyMode = container.ReadOnlyFileMode
	// directoryMode is the explicit mode of non-secret prepared directories.
	directoryMode = container.DirectoryMode
	// secretDirectoryMode is the owner-only mode of the host secrets parent dir.
	secretDirectoryMode = container.SecretDirectoryMode
	// secretsDirectoryName is the per-workload 0700 host secrets parent dir.
	secretsDirectoryName = "secrets"
	// secretsMountDirectoryName is the 0755 child of secretsDirectoryName
	// bind-mounted read-only at /kodema/secrets.
	secretsMountDirectoryName = "data"
	// caDirectory holds shared installation CA bundles keyed by content digest.
	caDirectory = ".ca"
	// toolboxDirectory holds shared installation copies of the toolbox, keyed
	// by binary digest, bind-mounted read-only at /kodema/bin.
	toolboxDirectory = ".toolbox"
	// executableMode is the mode of the installed toolbox binary.
	executableMode = os.FileMode(0o555)
)

// preparedCache is one tenant/project-scoped non-authoritative cache mount
// recorded in memory for a prepared workload.
type preparedCache struct {
	name string
	key  string
}

// preparedWorkload holds the in-process prepared state for one workload
// identity, mirroring kube's preparedWorkload. Durable lifecycle truth lives in
// PostgreSQL and the container runtime, never in local JSON state files.
type preparedWorkload struct {
	identity        reconcile.EffectIdentity
	workspace       id.WorkspaceID
	project         id.ProjectID
	role            string
	digest          string
	actionName      id.ActionName
	taskID          id.TaskID
	image           string
	env             []definition.EnvVar
	caches          []preparedCache
	outputs         bool
	memoryBytes     int64
	cpuMillicores   int64
	hasToolbox      bool
	hasCA           bool
	caDir           string
	toolboxDir      string
	gitURL          string
	activationPaths []string
	secretNames     []id.SecretName
}

// workloadRoot returns the per-identity adapter root.
func workloadRoot(root string, workload id.WorkloadID) string {
	return filepath.Join(root, string(workload))
}

// activationDir returns the daemon-visible directory bind-mounted at /kodema.
func activationDir(root string, workload id.WorkloadID) string {
	return filepath.Join(workloadRoot(root, workload), activationRoot)
}

// secretsParentDir returns the 0700 host-confined parent directory that guards
// the workload's secrets on the host filesystem.
func secretsParentDir(root string, workload id.WorkloadID) string {
	return filepath.Join(workloadRoot(root, workload), secretsDirectoryName)
}

// secretsDir returns the 0755 child directory under the 0700 host parent that
// holds declared secrets and KODEMA_TOKEN and is bind-mounted read-only at
// /kodema/secrets so non-root container users can read 0444 secret files.
func secretsDir(root string, workload id.WorkloadID) string {
	return filepath.Join(secretsParentDir(root, workload), secretsMountDirectoryName)
}

// secretHostPath returns the isolated host file path for one declared secret.
func secretHostPath(root string, workload id.WorkloadID, name id.SecretName) string {
	return filepath.Join(secretsDir(root, workload), string(name))
}

// credentialHostPath returns the isolated host file path for KODEMA_TOKEN.
func credentialHostPath(root string, workload id.WorkloadID) string {
	return filepath.Join(secretsDir(root, workload), string(id.SecretTokenName))
}

// hostPath maps one confined logical path under /kodema to its host path inside
// the non-secret activation tree.
func hostPath(root string, workload id.WorkloadID, logical string) string {
	return filepath.Join(workloadRoot(root, workload), logical)
}

// labels returns the pinned identity, preparation, project, and role labels
// attached to the container.
func (a *Adapter) labels(prepared preparedWorkload) map[string]string {
	labels := map[string]string{
		LabelExecution:      string(prepared.identity.ExecutionID),
		LabelAction:         string(prepared.identity.ActionID),
		LabelWorkload:       string(prepared.identity.WorkloadID),
		LabelWorkspace:      string(prepared.workspace),
		LabelIdempotencyKey: prepared.identity.IdempotencyKey,
		LabelPreparation:    prepared.digest,
	}
	if prepared.project != "" {
		labels[LabelProject] = string(prepared.project)
	}
	if prepared.role != "" {
		labels[LabelRole] = prepared.role
	}
	return labels
}

// containerSpec builds the pinned, fail-fast, non-replaying container request.
// The admitted literal environment keeps its declaration order and the
// unshadowable reserved context is appended last.
func (a *Adapter) containerSpec(prepared preparedWorkload, identity reconcile.EffectIdentity) (ContainerSpec, error) {
	name, err := ContainerName(identity.WorkloadID)
	if err != nil {
		return ContainerSpec{}, err
	}
	volumeName, err := WorkspaceVolumeName(prepared.workspace)
	if err != nil {
		return ContainerSpec{}, err
	}
	caFile := ""
	if prepared.hasCA {
		caFile = WorkloadCAPath
	}
	envVars, err := container.BuildContainerEnv(prepared.env, container.EnvOptions{
		KodemaURL:   a.kodemaURL,
		ExecutionID: identity.ExecutionID,
		ActionName:  prepared.actionName,
		TaskID:      prepared.taskID,
		CAFile:      caFile,
		GitURL:      prepared.gitURL,
		Toolbox:     prepared.hasToolbox,
	})
	if err != nil {
		return ContainerSpec{}, err
	}
	env := container.EnvStrings(envVars)

	mounts := []Mount{
		{
			Type:        "bind",
			Source:      activationDir(a.root, identity.WorkloadID),
			Destination: activationRoot,
			ReadOnly:    true,
		},
		{
			Type:        "bind",
			Source:      secretsDir(a.root, identity.WorkloadID),
			Destination: workspace.SecretRoot,
			ReadOnly:    true,
		},
		{
			Type:        "volume",
			Source:      volumeName,
			Destination: workspace.WorkspaceRoot,
			ReadOnly:    false,
		},
	}
	if prepared.hasCA && prepared.caDir != "" {
		mounts = append(mounts, Mount{
			Type:        "bind",
			Source:      prepared.caDir,
			Destination: WorkloadCAMountPath,
			ReadOnly:    true,
		})
	}
	// Toolbox, outputs, and caches nest inside the read-only activation bind on
	// empty mount points created by Prepare.
	if prepared.hasToolbox && prepared.toolboxDir != "" {
		mounts = append(mounts, Mount{
			Type:        "bind",
			Source:      prepared.toolboxDir,
			Destination: workspace.ToolboxRoot,
			ReadOnly:    true,
		})
	}
	if prepared.outputs {
		outputs, err := workspace.OutputDirectory(prepared.actionName)
		if err != nil {
			return ContainerSpec{}, err
		}
		mounts = append(mounts, Mount{
			Type:        "tmpfs",
			Destination: outputs,
			SizeBytes:   workspace.OutputDirectoryBytes,
		})
	}
	for _, cache := range prepared.caches {
		mountPath, err := workspace.CachePath(cache.name)
		if err != nil {
			return ContainerSpec{}, err
		}
		cacheVol, err := CacheVolumeName(cache.key)
		if err != nil {
			return ContainerSpec{}, err
		}
		mounts = append(mounts, Mount{
			Type:        "volume",
			Source:      cacheVol,
			Destination: mountPath,
			ReadOnly:    false,
		})
	}
	memoryBytes := prepared.memoryBytes
	if memoryBytes <= 0 {
		memoryBytes = a.resources.DefaultMemoryBytes
	}
	cpuMillicores := prepared.cpuMillicores
	if cpuMillicores <= 0 {
		cpuMillicores = a.resources.DefaultCPUMillicores
	}
	return ContainerSpec{
		Name:  name,
		Image: prepared.image,
		Entrypoint: workspace.Entrypoint(entrypointScriptPath, workspace.Launch{
			Toolbox: prepared.hasToolbox,
			Outputs: prepared.outputs,
		}),
		Env:           env,
		Labels:        a.labels(prepared),
		WorkingDir:    workspace.WorkspaceRoot,
		Mounts:        mounts,
		RestartPolicy: "no",
		Resources: ContainerResources{
			MemoryBytes: memoryBytes,
			CPUQuota:    cpuMillicores * (DefaultCPUPeriod / 1000),
			CPUPeriod:   DefaultCPUPeriod,
		},
	}, nil
}

// mountPoints returns the empty directories Prepare creates inside the
// read-only activation directory so nested mounts (/kodema/secrets, toolbox,
// outputs tmpfs, caches) have explicit mount points.
func mountPoints(prepared preparedWorkload) ([]string, error) {
	points := []string{workspace.SecretRoot}
	if prepared.hasToolbox {
		points = append(points, workspace.ToolboxRoot)
	}
	if prepared.outputs {
		outputs, err := workspace.OutputDirectory(prepared.actionName)
		if err != nil {
			return nil, err
		}
		points = append(points, outputs)
	}
	for _, cache := range prepared.caches {
		mountPath, err := workspace.CachePath(cache.name)
		if err != nil {
			return nil, err
		}
		points = append(points, mountPath)
	}
	return points, nil
}
