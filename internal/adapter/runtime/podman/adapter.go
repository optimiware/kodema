// Podman runtime adapter operations.
//
// Every operation consumes the persisted reconcile.EffectIdentity. Start may
// only create the one stable container; a lost create or start response stays
// unknown and a retry reuses the same container. Stop acknowledges a stop
// request but never fabricates a stop barrier. Remove is idempotent,
// identity-scoped and bounded, and never touches the retained workspace volume.
package podman

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/optimiware/kodema/internal/adapter/runtime"
	"github.com/optimiware/kodema/internal/adapter/runtime/container"
	"github.com/optimiware/kodema/internal/control/workspace"
	"github.com/optimiware/kodema/internal/engine"
	"github.com/optimiware/kodema/internal/engine/definition"
	"github.com/optimiware/kodema/internal/engine/reconcile"
	"github.com/optimiware/kodema/internal/engine/script"
	"github.com/optimiware/kodema/internal/fault"
	"github.com/optimiware/kodema/internal/id"
)

// tempSequence makes each atomic replacement use a distinct temporary name.
var tempSequence atomic.Uint64

// checkPreparedWorkspace refuses a request that names a different workspace
// than the one prepared in this process.
func (a *Adapter) checkPreparedWorkspace(workload id.WorkloadID, workspaceID id.WorkspaceID) error {
	a.mu.Lock()
	prepared, ok := a.prepared[workload]
	a.mu.Unlock()
	if ok && prepared.workspace != workspaceID {
		return fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
	}
	return nil
}

// memoryLimitFor returns the configured memory limit of one prepared workload,
// falling back to the installation default after a process restart.
func (a *Adapter) memoryLimitFor(workload id.WorkloadID) int64 {
	a.mu.Lock()
	prepared, ok := a.prepared[workload]
	a.mu.Unlock()
	if ok && prepared.memoryBytes > 0 {
		return prepared.memoryBytes
	}
	return a.resources.DefaultMemoryBytes
}

// ensureDirectory creates one non-secret prepared directory, refusing a symlink
// or a non-directory at that path.
func (a *Adapter) ensureDirectory(ctx context.Context, path string) error {
	return a.ensureDirectoryWithMode(ctx, path, directoryMode)
}

// ensureDirectoryWithMode creates one directory with an explicit permission
// mode, refusing a symlink or non-directory at that path.
func (a *Adapter) ensureDirectoryWithMode(ctx context.Context, path string, mode os.FileMode) error {
	info, err := a.files.Lstat(ctx, path)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 {
			return fault.New(fault.Forbidden, "preparation path is a symlink")
		}
		if !info.IsDir() {
			return fault.New(fault.Forbidden, "preparation path is not a directory")
		}
		if mode == secretDirectoryMode && info.Mode().Perm()&0o077 != 0 {
			return fault.New(fault.Forbidden, "secrets directory permissions are too permissive")
		}
		return nil
	case errors.Is(err, os.ErrNotExist):
		if err := a.files.MkdirAll(ctx, path, mode); err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "preparation directory write outcome is unknown", err)
		}
		return nil
	default:
		return fault.Wrap(fault.DependencyUnavailable, "preparation path is unreachable", err)
	}
}

// ensureConfinedDirectories creates every non-secret activation directory and
// the isolated 0700 secrets directory one component at a time so a symlink
// anywhere below the adapter root is refused instead of followed.
func (a *Adapter) ensureConfinedDirectories(ctx context.Context, workload id.WorkloadID, files []workspace.File) error {
	base := workloadRoot(a.root, workload)
	if err := a.ensureDirectory(ctx, base); err != nil {
		return err
	}

	created := map[string]bool{base: true}
	for _, file := range files {
		dir := filepath.Dir(hostPath(a.root, workload, file.Path))
		relative, err := filepath.Rel(base, dir)
		if err != nil || strings.HasPrefix(relative, "..") {
			return fault.New(fault.InvalidInput, "prepared path escapes the workload root")
		}
		if relative == "." {
			continue
		}
		current := base
		for segment := range strings.SplitSeq(relative, string(filepath.Separator)) {
			current = filepath.Join(current, segment)
			if created[current] {
				continue
			}
			if err := a.ensureDirectory(ctx, current); err != nil {
				return err
			}
			created[current] = true
		}
	}
	if err := a.ensureDirectoryWithMode(ctx, secretsParentDir(a.root, workload), secretDirectoryMode); err != nil {
		return err
	}
	return a.ensureDirectory(ctx, secretsDir(a.root, workload))
}

// ensureMountPoint creates one empty directory inside the activation
// directory, one component at a time, for a mount that nests in the read-only
// activation bind.
func (a *Adapter) ensureMountPoint(ctx context.Context, workload id.WorkloadID, logical string) error {
	base := workloadRoot(a.root, workload)
	relative, err := filepath.Rel(base, hostPath(a.root, workload, logical))
	if err != nil || strings.HasPrefix(relative, "..") {
		return fault.New(fault.InvalidInput, "mount point escapes the workload root")
	}
	current := base
	for segment := range strings.SplitSeq(relative, string(filepath.Separator)) {
		current = filepath.Join(current, segment)
		if err := a.ensureDirectory(ctx, current); err != nil {
			return err
		}
	}
	return nil
}

// ensureWorkloadCA installs the shared filtered CA bundle once under
// <root>/.ca/<digest>/ca.crt so workloads bind-mount it read-only at
// /etc/kodema/ca without copying it into <root>/<workload>/kodema.
func (a *Adapter) ensureWorkloadCA(ctx context.Context) error {
	if a.caDir == "" || len(a.workloadCAPEM) == 0 {
		return nil
	}
	a.caMu.Lock()
	defer a.caMu.Unlock()
	target := filepath.Join(a.caDir, WorkloadCAKey)
	info, err := a.files.Lstat(ctx, target)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fault.New(fault.Forbidden, "installed workload CA bundle is not a regular file")
		}
		return nil
	case !errors.Is(err, os.ErrNotExist):
		return fault.Wrap(fault.DependencyUnavailable, "installed workload CA bundle is unreachable", err)
	}
	for _, directory := range []string{filepath.Dir(a.caDir), a.caDir} {
		if err := a.ensureDirectory(ctx, directory); err != nil {
			return err
		}
	}
	if err := a.files.WriteFile(ctx, target, a.workloadCAPEM, readOnlyMode); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "workload CA installation write outcome is unknown", err)
	}
	return nil
}

// ensureToolbox installs the installation copy of the toolbox once. The Files
// boundary replaces a file atomically, so a workload never binds a
// half-written binary. An existing regular file is the copy of this exact
// digest and is left alone. The source lies outside the adapter root, so it is
// read directly, like the CA bundle.
func (a *Adapter) ensureToolbox(ctx context.Context) error {
	if a.toolboxDir == "" {
		return nil
	}
	a.toolboxMu.Lock()
	defer a.toolboxMu.Unlock()
	target := filepath.Join(a.toolboxDir, workspace.ToolboxName)
	info, err := a.files.Lstat(ctx, target)
	switch {
	case err == nil:
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fault.New(fault.Forbidden, "installed toolbox is not a regular file")
		}
		return nil
	case !errors.Is(err, os.ErrNotExist):
		return fault.Wrap(fault.DependencyUnavailable, "installed toolbox is unreachable", err)
	}
	data, err := os.ReadFile(a.toolboxFile)
	if err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "toolbox file is unreadable", err)
	}
	// The configured executable can be replaced between New and the first
	// Prepare (for example, by an installer). Never publish different bytes
	// at a digest another version of the controller may already have pinned.
	sum := sha256.Sum256(data)
	if toolboxDigestName(sum[:]) != filepath.Base(a.toolboxDir) {
		return fault.New(fault.DependencyUnavailable, "toolbox file changed since startup; restart the controller")
	}
	for _, directory := range []string{filepath.Dir(a.toolboxDir), a.toolboxDir} {
		if err := a.ensureDirectory(ctx, directory); err != nil {
			return err
		}
	}
	if err := a.files.WriteFile(ctx, target, data, executableMode); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "toolbox installation write outcome is unknown", err)
	}
	return nil
}

// replaceFile atomically replaces one host file: write a temporary sibling, then
// rename it over the target. A failure leaves the previous bytes intact.
func (a *Adapter) replaceFile(ctx context.Context, target string, data []byte) error {
	temp := fmt.Sprintf("%s.tmp-%d", target, tempSequence.Add(1))
	if err := a.files.WriteFile(ctx, temp, data, readOnlyMode); err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "replacement write outcome is unknown", err)
	}
	if err := a.files.Rename(ctx, temp, target); err != nil {
		// The temporary file is ours and carries only the new value; removing it
		// keeps the directory clean without touching the previous token.
		_ = a.files.RemoveAll(ctx, temp)
		return fault.Wrap(fault.UncertainExternalEffect, "replacement outcome is unknown", err)
	}
	return nil
}

// writeCredential creates or atomically replaces only the reserved credential
// file inside the isolated secrets directory. The previous token survives a
// failed replacement.
func (a *Adapter) writeCredential(ctx context.Context, workload id.WorkloadID, credential string) error {
	target := credentialHostPath(a.root, workload)
	existing, err := a.files.ReadFile(ctx, target)
	if err == nil && string(existing) == credential {
		return nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fault.Wrap(fault.DependencyUnavailable, "credential file is unreachable", err)
	}
	return a.replaceFile(ctx, target, []byte(credential))
}

// verifyPreparedFiles confirms the activation tree, 0700 secrets parent,
// bind-mounted secrets child, declared secrets, and KODEMA_TOKEN exist as
// confined non-symlinked directories/files before Start creates a container.
func (a *Adapter) verifyPreparedFiles(ctx context.Context, workload id.WorkloadID, prepared preparedWorkload) error {
	parentInfo, err := a.files.Lstat(ctx, secretsParentDir(a.root, workload))
	if err != nil {
		return fault.Wrap(fault.DependencyUnavailable, "prepared directory is missing", err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return fault.New(fault.Forbidden, "prepared directory is not a directory")
	}
	if parentInfo.Mode().Perm()&0o077 != 0 {
		return fault.New(fault.Forbidden, "secrets directory permissions are too permissive")
	}
	for _, dir := range []string{activationDir(a.root, workload), secretsDir(a.root, workload)} {
		info, err := a.files.Lstat(ctx, dir)
		if err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "prepared directory is missing", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fault.New(fault.Forbidden, "prepared directory is not a directory")
		}
	}
	for _, logical := range prepared.activationPaths {
		host := hostPath(a.root, workload, logical)
		info, err := a.files.Lstat(ctx, host)
		if err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "prepared activation file is missing", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fault.New(fault.Forbidden, "prepared activation file is not a regular file")
		}
	}
	secretPaths := make([]string, 0, len(prepared.secretNames)+1)
	for _, name := range prepared.secretNames {
		secretPaths = append(secretPaths, secretHostPath(a.root, workload, name))
	}
	secretPaths = append(secretPaths, credentialHostPath(a.root, workload))
	for _, host := range secretPaths {
		info, err := a.files.Lstat(ctx, host)
		if err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "prepared secret file is missing", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fault.New(fault.Forbidden, "prepared secret file is not a regular file")
		}
	}
	return nil
}

// checkNoUnexpectedActivationFiles walks dir recursively and refuses any
// symlink (Forbidden) or regular file not in expectedFiles
// (ChangedIdempotencyContent), catching post-restart Prepare retries that drop
// previously written activation files.
func (a *Adapter) checkNoUnexpectedActivationFiles(ctx context.Context, dir string, expectedFiles map[string]bool) error {
	entries, err := a.files.ReadDir(ctx, dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fault.Wrap(fault.DependencyUnavailable, "activation directory is unreadable", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.Type()&os.ModeSymlink != 0 {
			return fault.New(fault.Forbidden, "activation snapshot path is a symlink")
		}
		if isTransientTempFile(name) {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := a.files.Lstat(ctx, path)
		if err != nil {
			return fault.Wrap(fault.DependencyUnavailable, "activation snapshot path is unreachable", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fault.New(fault.Forbidden, "activation snapshot path is a symlink")
		}
		if info.IsDir() {
			if err := a.checkNoUnexpectedActivationFiles(ctx, path, expectedFiles); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fault.New(fault.Forbidden, "activation snapshot path is not a regular file")
		}
		if !expectedFiles[path] {
			return fault.New(fault.ChangedIdempotencyContent,
				"preparation content changed for the same workload identity")
		}
	}
	return nil
}

// Prepare materializes the non-secret activation snapshot and isolated 0700
// secrets directory once and attaches the existing retained workspace volume.
// It never starts a container and never recreates a missing workspace.
func (a *Adapter) Prepare(ctx context.Context, request reconcile.PrepareRequest) (reconcile.Preparation, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidatePrepareContext(request.ActionName, request.TaskID); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := workspace.ValidateCacheMounts(request.Container.Caches, request.Caches); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidatePinnedImage(request.Container.Image); err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidateContainerEnv(request.Container.Env); err != nil {
		return reconcile.Preparation{}, err
	}
	resolved, err := a.resources.Resolve(request.Container.Resources)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	renderedScript, err := script.Render(request.Container.Commands)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	materialization, err := container.BuildMaterialization(request, readOnlyMode)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	materialization.Files = append(materialization.Files, workspace.File{
		Path: entrypointScriptPath,
		Data: []byte(renderedScript),
		Mode: readOnlyMode,
	})
	sort.Slice(materialization.Files, func(left, right int) bool {
		return materialization.Files[left].Path < materialization.Files[right].Path
	})
	digest := container.PreparationDigest(request, materialization, a.kodemaURL)

	if err := ctx.Err(); err != nil {
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "preparation cancelled before any effect", err)
	}

	var projectID id.ProjectID
	if candidate := runtime.ExecutionProject(ctx); candidate != "" {
		if _, err := id.ParseProjectID(string(candidate)); err == nil {
			projectID = candidate
		}
	}
	role := container.WorkloadRole(request)

	unlock := a.lockFor(request.Identity.WorkloadID)
	defer unlock()
	unlockWorkspace := a.lockForWorkspace(request.WorkspaceID)
	defer unlockWorkspace()

	a.mu.Lock()
	existingPrepared, alreadyPrepared := a.prepared[request.Identity.WorkloadID]
	a.mu.Unlock()
	if alreadyPrepared {
		if existingPrepared.workspace != request.WorkspaceID {
			return reconcile.Preparation{}, fault.New(fault.Forbidden, "workload identity is already prepared for a different workspace")
		}
		if existingPrepared.identity != request.Identity || existingPrepared.digest != digest {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"preparation content changed for the same workload identity")
		}
	}

	volumeName, err := WorkspaceVolumeName(request.WorkspaceID)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	volume, err := a.libpod.InspectVolume(ctx, volumeName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return reconcile.Preparation{}, fault.New(fault.NotFound,
				"retained workspace volume is missing; preparation never recreates it")
		}
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "retained workspace volume is unreachable", err)
	}
	if !ownsWorkspaceVolume(volume, volumeName, request.WorkspaceID) {
		return reconcile.Preparation{}, fault.New(fault.Forbidden, "workspace volume is not owned by this workspace")
	}

	if err := a.libpod.InspectImage(ctx, request.Container.Image); err != nil {
		if errors.Is(err, ErrNotFound) {
			return reconcile.Preparation{}, fault.New(fault.DependencyUnavailable,
				fmt.Sprintf("workload image %q is not present in the local container store; pull it before starting the execution", request.Container.Image))
		}
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "workload image state is unreachable", err)
	}

	containerName, err := ContainerName(request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	existingContainer, inspectErr := a.libpod.InspectContainer(ctx, containerName)
	switch {
	case inspectErr == nil:
		if err := checkContainerIdentity(existingContainer, request.Identity); err != nil {
			return reconcile.Preparation{}, err
		}
		if err := checkContainerWorkspace(existingContainer, request.WorkspaceID); err != nil {
			return reconcile.Preparation{}, err
		}
		if existingDigest := existingContainer.Labels[LabelPreparation]; existingDigest != "" && existingDigest != digest {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"preparation content changed for the same workload identity")
		}
	case errors.Is(inspectErr, ErrNotFound):
		// Normal first-time preparation.
	default:
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", inspectErr)
	}

	var prepared preparedWorkload
	if alreadyPrepared {
		prepared = existingPrepared
	} else {
		activationPaths := make([]string, len(materialization.Files))
		for index, file := range materialization.Files {
			activationPaths[index] = file.Path
		}
		secretNames := make([]id.SecretName, len(materialization.Secrets))
		for index, secret := range materialization.Secrets {
			secretNames[index] = secret.Name
		}
		slices.Sort(secretNames)
		var caches []preparedCache
		if len(request.Caches) > 0 {
			caches = make([]preparedCache, len(request.Caches))
			for index, cache := range request.Caches {
				caches[index] = preparedCache{name: cache.Name, key: cache.Key}
			}
		}
		prepared = preparedWorkload{
			identity:        request.Identity,
			workspace:       request.WorkspaceID,
			project:         projectID,
			role:            role,
			digest:          digest,
			actionName:      request.ActionName,
			taskID:          request.TaskID,
			image:           request.Container.Image,
			env:             append([]definition.EnvVar(nil), request.Container.Env...),
			caches:          caches,
			outputs:         a.toolboxDir != "" && len(request.Container.Outputs) > 0,
			memoryBytes:     resolved.MemoryBytes,
			cpuMillicores:   resolved.CPUMillicores,
			hasToolbox:      a.toolboxDir != "",
			hasCA:           a.caDir != "",
			caDir:           a.caDir,
			toolboxDir:      a.toolboxDir,
			gitURL:          a.gitURL,
			activationPaths: activationPaths,
			secretNames:     secretNames,
		}
	}

	if prepared.hasCA {
		if err := a.ensureWorkloadCA(ctx); err != nil {
			return reconcile.Preparation{}, err
		}
	}
	if prepared.hasToolbox {
		if err := a.ensureToolbox(ctx); err != nil {
			return reconcile.Preparation{}, err
		}
	}

	points, err := mountPoints(prepared)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if err := a.ensureConfinedDirectories(ctx, request.Identity.WorkloadID, materialization.Files); err != nil {
		return reconcile.Preparation{}, err
	}
	for _, point := range points {
		if err := a.ensureMountPoint(ctx, request.Identity.WorkloadID, point); err != nil {
			return reconcile.Preparation{}, err
		}
	}

	pendingFiles := make([]workspace.File, 0, len(materialization.Files))
	for _, file := range materialization.Files {
		host := hostPath(a.root, request.Identity.WorkloadID, file.Path)
		info, err := a.files.Lstat(ctx, host)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				pendingFiles = append(pendingFiles, file)
				continue
			}
			return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "activation snapshot is unreachable", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return reconcile.Preparation{}, fault.New(fault.Forbidden, "activation snapshot path is not a regular file")
		}
		existing, err := a.files.ReadFile(ctx, host)
		if err != nil {
			return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "activation snapshot is unreadable", err)
		}
		if !bytes.Equal(existing, file.Data) {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"preparation content changed for the same workload identity")
		}
	}

	// Verify no unexpected activation files remain on disk (for example after a
	// controller restart when a conflicting Prepare drops a previously written
	// input, output, or task file).
	expectedFiles := make(map[string]bool, len(materialization.Files))
	for _, file := range materialization.Files {
		expectedFiles[hostPath(a.root, request.Identity.WorkloadID, file.Path)] = true
	}
	if err := a.checkNoUnexpectedActivationFiles(ctx, activationDir(a.root, request.Identity.WorkloadID), expectedFiles); err != nil {
		return reconcile.Preparation{}, err
	}

	// Verify existing declared secret files match the request before writing.
	expectedSecrets := make(map[string]string, len(materialization.Secrets))
	for _, secret := range materialization.Secrets {
		expectedSecrets[string(secret.Name)] = secret.Value
	}
	secEntries, err := a.files.ReadDir(ctx, secretsDir(a.root, request.Identity.WorkloadID))
	if err != nil {
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "secrets directory is unreadable", err)
	}
	for _, entry := range secEntries {
		name := entry.Name()
		if entry.Type()&os.ModeSymlink != 0 {
			return reconcile.Preparation{}, fault.New(fault.Forbidden, "secret snapshot path is a symlink")
		}
		if name == string(id.SecretTokenName) || isTransientTempFile(name) {
			continue
		}
		if _, expected := expectedSecrets[name]; !expected {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"declared secret content changed for the same workload identity")
		}
	}

	pendingSecrets := make([]reconcile.SecretValue, 0, len(materialization.Secrets))
	for _, secret := range materialization.Secrets {
		host := secretHostPath(a.root, request.Identity.WorkloadID, secret.Name)
		info, err := a.files.Lstat(ctx, host)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				pendingSecrets = append(pendingSecrets, secret)
				continue
			}
			return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "secret snapshot is unreachable", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return reconcile.Preparation{}, fault.New(fault.Forbidden, "secret snapshot path is not a regular file")
		}
		existing, err := a.files.ReadFile(ctx, host)
		if err != nil {
			return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "secret snapshot is unreadable", err)
		}
		if string(existing) != secret.Value {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"declared secret content changed for the same workload identity")
		}
	}

	for _, file := range pendingFiles {
		host := hostPath(a.root, request.Identity.WorkloadID, file.Path)
		if err := a.files.WriteFile(ctx, host, file.Data, readOnlyMode); err != nil {
			return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable,
				"activation snapshot write outcome is unknown", err)
		}
	}
	for _, secret := range pendingSecrets {
		host := secretHostPath(a.root, request.Identity.WorkloadID, secret.Name)
		if err := a.files.WriteFile(ctx, host, []byte(secret.Value), readOnlyMode); err != nil {
			return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable,
				"secret snapshot write outcome is unknown", err)
		}
	}
	if err := a.writeCredential(ctx, request.Identity.WorkloadID, request.Credential); err != nil {
		return reconcile.Preparation{}, err
	}

	a.mu.Lock()
	a.prepared[request.Identity.WorkloadID] = prepared
	a.mu.Unlock()

	return reconcile.Preparation{
		WorkspaceID: request.WorkspaceID,
		Detail: fmt.Sprintf("materialized %d activation files for %s",
			len(materialization.Files), container.DescribeIdentity(request.Identity)),
	}, nil
}

// Start creates and starts the one stable container for a prepared identity. A
// lost create or start response stays unknown; a retry inspects and reuses the
// same container instead of replaying a fresh command.
func (a *Adapter) Start(ctx context.Context, request reconcile.StartRequest) (reconcile.StartResult, error) {
	unknown := func(detail string, err error) (reconcile.StartResult, error) {
		return reconcile.StartResult{
			Outcome:    reconcile.StartUnknown,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     detail,
		}, err
	}
	accepted := func(detail string) (reconcile.StartResult, error) {
		return reconcile.StartResult{
			Outcome:    reconcile.StartAccepted,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     detail,
		}, nil
	}

	if err := container.ValidateIdentity(request.Identity); err != nil {
		return unknown("start request identity is invalid", err)
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return unknown("start request workspace is invalid", err)
	}
	if err := ctx.Err(); err != nil {
		return unknown("start cancelled before any effect",
			fault.Wrap(fault.DependencyUnavailable, "start cancelled before any effect", err))
	}

	unlock := a.lockFor(request.Identity.WorkloadID)
	defer unlock()
	unlockWorkspace := a.lockForWorkspace(request.WorkspaceID)
	defer unlockWorkspace()

	containerName, err := ContainerName(request.Identity.WorkloadID)
	if err != nil {
		return unknown("container name is invalid", err)
	}

	a.mu.Lock()
	prepared, ok := a.prepared[request.Identity.WorkloadID]
	a.mu.Unlock()

	if !ok {
		// Recovery after a process restart: an existing stable container is
		// reused rather than replaced. Without a container there is no
		// completed in-process preparation, so Start refuses instead of
		// preparing implicitly.
		existing, inspectErr := a.libpod.InspectContainer(ctx, containerName)
		if inspectErr == nil {
			if err := checkContainerIdentity(existing, request.Identity); err != nil {
				return unknown("existing container does not match this identity", err)
			}
			if err := checkContainerWorkspace(existing, request.WorkspaceID); err != nil {
				return unknown("existing container is bound to a different workspace", err)
			}
			return startExisting(containerName, existing, unknown, accepted)
		}
		if errors.Is(inspectErr, ErrNotFound) {
			return unknown("start requires a completed preparation",
				fault.New(fault.InvalidInput, "start requires a completed preparation"))
		}
		return unknown("container state is unreachable",
			fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", inspectErr))
	}

	if prepared.workspace != request.WorkspaceID {
		return unknown("start workspace does not match the prepared workspace",
			fault.New(fault.Forbidden, "start workspace does not match the prepared workspace"))
	}
	if prepared.identity != request.Identity {
		return unknown("start identity does not match the prepared identity",
			fault.New(fault.ChangedIdempotencyContent, "start identity does not match the prepared identity"))
	}

	volumeName, err := WorkspaceVolumeName(request.WorkspaceID)
	if err != nil {
		return unknown("workspace volume name is invalid", err)
	}
	volume, err := a.libpod.InspectVolume(ctx, volumeName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return unknown("retained workspace volume is missing",
				fault.New(fault.NotFound, "retained workspace volume is missing; start never recreates it"))
		}
		return unknown("retained workspace volume is unreachable",
			fault.Wrap(fault.DependencyUnavailable, "retained workspace volume is unreachable", err))
	}
	if !ownsWorkspaceVolume(volume, volumeName, request.WorkspaceID) {
		return unknown("workspace volume is not owned by this workspace",
			fault.New(fault.Forbidden, "workspace volume is not owned by this workspace"))
	}
	if err := a.verifyPreparedFiles(ctx, request.Identity.WorkloadID, prepared); err != nil {
		return unknown("preparation is incomplete", err)
	}

	spec, err := a.containerSpec(prepared, request.Identity)
	if err != nil {
		return unknown("container specification is invalid", err)
	}

	state, err := a.libpod.InspectContainer(ctx, spec.Name)
	switch {
	case err == nil:
		if err := checkExistingContainer(state, request.Identity, request.WorkspaceID, prepared.image); err != nil {
			return unknown("existing container does not match this identity", err)
		}
		return startExisting(spec.Name, state, unknown, accepted)
	case errors.Is(err, ErrNotFound):
		// Create below.
	default:
		return unknown("container state is unreachable",
			fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", err))
	}

	created, err := a.libpod.CreateContainer(ctx, spec)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			if existing, inspectErr := a.libpod.InspectContainer(ctx, spec.Name); inspectErr == nil {
				if ownershipErr := checkExistingContainer(existing, request.Identity, request.WorkspaceID, prepared.image); ownershipErr != nil {
					return unknown("existing container does not match this identity", ownershipErr)
				}
				return startExisting(spec.Name, existing, unknown, accepted)
			}
		}
		return unknown("container creation outcome is unknown",
			fault.Wrap(fault.UncertainExternalEffect, "container creation outcome is unknown", err))
	}
	if err := ctx.Err(); err != nil {
		return unknown("start cancelled after container creation",
			fault.Wrap(fault.DependencyUnavailable, "start cancelled after container creation", err))
	}
	if err := a.libpod.StartContainer(ctx, created.Name); err != nil {
		return unknown("container start outcome is unknown",
			fault.Wrap(fault.UncertainExternalEffect, "container start outcome is unknown", err))
	}
	return accepted(fmt.Sprintf("created and started stable container %q", created.Name))
}

// startExisting reuses an existing running or exited container without
// replaying a fresh command. A container observed in "created" status stays
// StartUnknown conservatively because a libpod client cannot prove whether a
// prior start request is still in flight.
func startExisting(
	name string,
	state ContainerState,
	unknown func(string, error) (reconcile.StartResult, error),
	accepted func(string) (reconcile.StartResult, error),
) (reconcile.StartResult, error) {
	switch state.Status {
	case ContainerStatusRunning:
		return accepted(fmt.Sprintf("reused running stable container %q", name))
	case ContainerStatusExited:
		return accepted(fmt.Sprintf("reused exited stable container %q", name))
	case ContainerStatusCreated:
		return unknown("container exists in created state; start outcome is uncertain",
			fault.New(fault.UncertainExternalEffect, "container exists in created state; start outcome is uncertain"))
	default:
		return unknown("container status is unrecognized",
			fault.New(fault.UncertainExternalEffect, "container status is unrecognized"))
	}
}

// Observe reports the declared Kernel vocabulary for the stable identity. An
// absent container always reports AbsentObservation; an unreachable daemon or a
// container in "created" status stays unknown.
func (a *Adapter) Observe(ctx context.Context, request reconcile.ObserveRequest) (engine.Observation, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return engine.UnknownObservation(), err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return engine.UnknownObservation(), err
	}

	unlock := a.lockFor(request.Identity.WorkloadID)
	defer unlock()

	if err := a.checkPreparedWorkspace(request.Identity.WorkloadID, request.WorkspaceID); err != nil {
		return engine.UnknownObservation(), err
	}
	name, err := ContainerName(request.Identity.WorkloadID)
	if err != nil {
		return engine.UnknownObservation(), err
	}
	state, err := a.libpod.InspectContainer(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return engine.AbsentObservation(), nil
		}
		return engine.UnknownObservation(), fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", err)
	}
	if err := checkContainerIdentity(state, request.Identity); err != nil {
		return engine.UnknownObservation(), err
	}
	if err := checkContainerWorkspace(state, request.WorkspaceID); err != nil {
		return engine.UnknownObservation(), err
	}
	memoryLimitBytes := state.MemoryLimitBytes
	if state.Status == ContainerStatusExited && state.OOMKilled && memoryLimitBytes <= 0 {
		memoryLimitBytes = a.memoryLimitFor(request.Identity.WorkloadID)
	}
	return observationFor(state, memoryLimitBytes), nil
}

// Logs returns ordered, bounded and redacted chunks. It carries no lifecycle
// authority and fails closed when the redaction source is unavailable.
func (a *Adapter) Logs(ctx context.Context, request reconcile.LogsRequest) (reconcile.LogBatch, error) {
	if err := container.ValidateLogsRequest(request); err != nil {
		return reconcile.LogBatch{}, err
	}

	name, err := ContainerName(request.Identity.WorkloadID)
	if err != nil {
		return reconcile.LogBatch{}, err
	}
	state, err := a.libpod.InspectContainer(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return reconcile.LogBatch{}, fault.New(fault.NotFound, "log read requires the workload container")
		}
		return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", err)
	}
	if err := checkContainerIdentity(state, request.Identity); err != nil {
		return reconcile.LogBatch{}, err
	}
	values, err := a.redactionValues(ctx, request.Identity.WorkloadID)
	if err != nil {
		// Never emit output when the configured values cannot be redacted.
		// Drop ErrNotFound/os.ErrNotExist causes so callers checking
		// errors.Is(err, ErrNotFound) never mistake a missing redaction source
		// for a vanished container.
		cause := container.ErrRedactionSourceUnavailable
		if !errors.Is(err, ErrNotFound) && !errors.Is(err, os.ErrNotExist) {
			cause = errors.Join(cause, err)
		}
		return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "redaction source is unavailable; log read refused", cause)
	}

	stdoutOffset, stderrOffset := container.DecodeLogCursor(request.AfterOffset)
	positions := map[reconcile.LogStream]int64{
		reconcile.LogStdout: stdoutOffset,
		reconcile.LogStderr: stderrOffset,
	}

	batch := reconcile.LogBatch{}
	sequence := int64(0)
	for _, stream := range []reconcile.LogStream{reconcile.LogStdout, reconcile.LogStderr} {
		offset := positions[stream]
		page, err := a.libpod.ReadLogs(ctx, LogRequest{
			Name:   name,
			Stream: stream,
			Offset: offset,
			Limit:  request.Limit,
		})
		if err != nil {
			return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "container log read is unavailable", err)
		}
		chunk, hasChunk, nextPosition, err := container.BuildLogChunk(container.LogPageInput{
			Stream:        stream,
			Offset:        offset,
			Sequence:      sequence + 1,
			Raw:           page.Data,
			PageTruncated: page.Truncated,
			Values:        values,
			Limit:         request.Limit,
			MayGrow:       page.Truncated,
		})
		if err != nil {
			return reconcile.LogBatch{}, err
		}
		positions[stream] = nextPosition
		if !hasChunk {
			continue
		}
		sequence++
		batch.Chunks = append(batch.Chunks, chunk)
	}
	batch.NextOffset = container.EncodeLogCursor(positions[reconcile.LogStdout], positions[reconcile.LogStderr])
	return batch, nil
}

// Stop requests container termination but returns confirmation only for
// affirmatively observed whole-container termination. A stop acknowledgment or a
// lost response stays pending.
func (a *Adapter) Stop(ctx context.Context, request reconcile.StopRequest) (engine.StopEvidence, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return engine.PendingStop("stop request identity is invalid"), err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return engine.PendingStop("stop request workspace is invalid"), err
	}

	unlock := a.lockFor(request.Identity.WorkloadID)
	defer unlock()

	if err := a.checkPreparedWorkspace(request.Identity.WorkloadID, request.WorkspaceID); err != nil {
		return engine.PendingStop("stop request workspace does not match the prepared workspace"), err
	}
	name, err := ContainerName(request.Identity.WorkloadID)
	if err != nil {
		return engine.PendingStop("stop request container name is invalid"), err
	}

	state, err := a.libpod.InspectContainer(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q is absent", name)), nil
		}
		return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q state is unreachable", name)),
			fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", err)
	}
	if err := checkContainerIdentity(state, request.Identity); err != nil {
		return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q is not owned by this identity", name)), err
	}
	if err := checkContainerWorkspace(state, request.WorkspaceID); err != nil {
		return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q is bound to a different workspace", name)), err
	}

	switch state.Status {
	case ContainerStatusCreated:
		// Real libpod leaves a "created" container in "created" status after
		// StopContainer (pid 0, zero FinishedAt), which can never yield
		// whole-container exit evidence. Instead, issue a non-forced remove:
		// libpod refuses non-forced DELETE if the container started
		// concurrently, so a clean removal affirmatively proves no container
		// process is running and no delayed start can launch it.
		removeErr := a.libpod.RemoveContainer(ctx, name)
		switch {
		case removeErr == nil:
			return engine.ConfirmedStop(fmt.Sprintf(
				"container %q was removed without force from created state; no container process can run", name)), nil
		case errors.Is(removeErr, ErrNotFound):
			return engine.PendingStop(fmt.Sprintf(
				"stop unconfirmed: container %q disappeared without termination evidence", name)), nil
		default:
			// If a concurrent start moved the container into "running" before
			// our non-forced delete, re-inspect and fall through to
			// StopContainer; otherwise keep the outcome uncertain.
			nextState, inspectErr := a.libpod.InspectContainer(ctx, name)
			if inspectErr == nil && nextState.Status == ContainerStatusRunning {
				if err := checkContainerIdentity(nextState, request.Identity); err != nil {
					return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q is not owned by this identity", name)), err
				}
				if err := checkContainerWorkspace(nextState, request.WorkspaceID); err != nil {
					return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q is bound to a different workspace", name)), err
				}
				if stopErr := a.libpod.StopContainer(ctx, name); stopErr != nil && !errors.Is(stopErr, ErrNotFound) {
					return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q stop outcome is unknown", name)),
						fault.Wrap(fault.UncertainExternalEffect, "container stop outcome is unknown", stopErr)
				}
				break
			}
			return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q stop outcome is unknown", name)),
				fault.Wrap(fault.UncertainExternalEffect, "container stop outcome is unknown", removeErr)
		}
	case ContainerStatusRunning:
		// An already exited container is never sent a stop request: only its
		// observed termination evidence may confirm the barrier.
		if err := a.libpod.StopContainer(ctx, name); err != nil && !errors.Is(err, ErrNotFound) {
			return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q stop outcome is unknown", name)),
				fault.Wrap(fault.UncertainExternalEffect, "container stop outcome is unknown", err)
		}
	}

	state, err = a.libpod.InspectContainer(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q disappeared without termination evidence", name)), nil
		}
		return engine.PendingStop(fmt.Sprintf("stop unconfirmed: container %q state is unreachable", name)),
			fault.Wrap(fault.DependencyUnavailable, "container state is unreachable", err)
	}
	if state.Status == ContainerStatusExited && state.WholeStopped {
		return engine.ConfirmedStop(fmt.Sprintf(
			"container %q exited with code %d and every container process stopped", name, state.ExitCode)), nil
	}
	return engine.PendingStop(fmt.Sprintf(
		"stop unconfirmed: container %q has no whole-container termination evidence (%s)",
		name, describeContainerState(state))), nil
}

// Remove is idempotent, identity-scoped and bounded cleanup. It removes only the
// identity's container and materialization tree and never removes or writes the
// retained workspace volume. A live, created, or unconfirmed container retains
// every owned resource and names the container.
func (a *Adapter) Remove(ctx context.Context, request reconcile.RemoveRequest) (reconcile.Removal, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.Removal{Detail: "removal request identity is invalid"}, err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return reconcile.Removal{Detail: "removal request workspace is invalid"}, err
	}

	unlock := a.lockFor(request.Identity.WorkloadID)
	defer unlock()
	unlockWorkspace := a.lockForWorkspace(request.WorkspaceID)
	defer unlockWorkspace()

	if err := a.checkPreparedWorkspace(request.Identity.WorkloadID, request.WorkspaceID); err != nil {
		return reconcile.Removal{Detail: "removal request workspace does not match the prepared workspace"}, err
	}
	name, err := ContainerName(request.Identity.WorkloadID)
	if err != nil {
		return reconcile.Removal{Detail: "removal request container name is invalid"}, err
	}
	state, inspectErr := a.libpod.InspectContainer(ctx, name)
	containerPresent := false
	switch {
	case inspectErr == nil:
		if err := checkContainerIdentity(state, request.Identity); err != nil {
			return reconcile.Removal{Detail: retainedDetail(name, "container is not owned by this identity")}, nil
		}
		if err := checkContainerWorkspace(state, request.WorkspaceID); err != nil {
			return reconcile.Removal{Detail: retainedDetail(name, "container is bound to a different workspace")}, err
		}
		if !removable(state) {
			return reconcile.Removal{Detail: retainedDetail(name, describeContainerState(state))}, nil
		}
		containerPresent = true
	case errors.Is(inspectErr, ErrNotFound):
		containerPresent = false
	default:
		return reconcile.Removal{Detail: retainedDetail(name, "container state is unreachable")}, nil
	}

	if containerPresent {
		if err := container.RetryWithBackoff(ctx, a.clock, a.cleanup, func(ctx context.Context) error {
			err := a.libpod.RemoveContainer(ctx, name)
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}); err != nil {
			detail := retainedDetail(name, "container removal failed")
			if ctx.Err() != nil {
				return reconcile.Removal{Detail: detail},
					fault.Wrap(fault.DependencyUnavailable, "removal cancelled during cleanup", ctx.Err())
			}
			return reconcile.Removal{Detail: detail}, nil
		}
	}
	if err := container.RetryWithBackoff(ctx, a.clock, a.cleanup, func(ctx context.Context) error {
		err := a.files.RemoveAll(ctx, workloadRoot(a.root, request.Identity.WorkloadID))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}); err != nil {
		detail := retainedDetail(name, "materialization removal failed")
		if ctx.Err() != nil {
			return reconcile.Removal{Detail: detail},
				fault.Wrap(fault.DependencyUnavailable, "removal cancelled during cleanup", ctx.Err())
		}
		return reconcile.Removal{Detail: detail}, nil
	}
	a.mu.Lock()
	delete(a.prepared, request.Identity.WorkloadID)
	a.mu.Unlock()
	return reconcile.Removal{
		Removed: true,
		Detail: fmt.Sprintf("removed container %q and materialization for workload %q",
			name, request.Identity.WorkloadID),
	}, nil
}

// retainedDetail names the retained container so pending cleanup is
// attributable.
func retainedDetail(containerName, reason string) string {
	return fmt.Sprintf("cleanup pending: retained container %q: %s", containerName, reason)
}

// RefreshCredential atomically replaces only the reserved credential file in
// the isolated secrets directory. It never creates, starts, stops or removes
// the container, and a failed or lost replacement leaves the previous token
// intact for a retry.
func (a *Adapter) RefreshCredential(ctx context.Context, request reconcile.CredentialRequest) (reconcile.CredentialRefresh, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.CredentialRefresh{}, err
	}
	if strings.TrimSpace(request.Credential) == "" {
		return reconcile.CredentialRefresh{}, fault.New(fault.InvalidInput, "credential refresh requires a non-empty credential")
	}

	unlock := a.lockFor(request.Identity.WorkloadID)
	defer unlock()

	parentInfo, err := a.files.Lstat(ctx, secretsParentDir(a.root, request.Identity.WorkloadID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return reconcile.CredentialRefresh{Detail: "credential refresh requires a completed preparation"},
				fault.New(fault.NotFound, "credential refresh requires a completed preparation")
		}
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: secrets directory is unreachable"},
			fault.Wrap(fault.DependencyUnavailable, "secrets directory is unreachable", err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: secrets path is not a directory"},
			fault.New(fault.Forbidden, "secrets path is not a directory")
	}
	if parentInfo.Mode().Perm()&0o077 != 0 {
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: secrets directory permissions are too permissive"},
			fault.New(fault.Forbidden, "secrets directory permissions are too permissive")
	}

	dir := secretsDir(a.root, request.Identity.WorkloadID)
	dirInfo, err := a.files.Lstat(ctx, dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return reconcile.CredentialRefresh{Detail: "credential refresh requires a completed preparation"},
				fault.New(fault.NotFound, "credential refresh requires a completed preparation")
		}
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: secrets directory is unreachable"},
			fault.Wrap(fault.DependencyUnavailable, "secrets directory is unreachable", err)
	}
	if dirInfo.Mode()&os.ModeSymlink != 0 || !dirInfo.IsDir() {
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: secrets path is not a directory"},
			fault.New(fault.Forbidden, "secrets path is not a directory")
	}

	target := credentialHostPath(a.root, request.Identity.WorkloadID)
	info, err := a.files.Lstat(ctx, target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return reconcile.CredentialRefresh{Detail: "credential refresh requires a completed preparation"},
				fault.New(fault.NotFound, "credential refresh requires a completed preparation")
		}
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: credential file is unreachable"},
			fault.Wrap(fault.DependencyUnavailable, "credential file is unreachable", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return reconcile.CredentialRefresh{Detail: "credential refresh refused: credential file is not a regular file"},
			fault.New(fault.Forbidden, "credential file is not a regular file")
	}

	if err := a.replaceFile(ctx, target, []byte(request.Credential)); err != nil {
		return reconcile.CredentialRefresh{Detail: "credential replacement outcome is unknown"}, err
	}
	return reconcile.CredentialRefresh{
		Delivered: true,
		Detail:    fmt.Sprintf("replaced the execution credential for %s without restarting the container", container.DescribeIdentity(request.Identity)),
	}, nil
}
