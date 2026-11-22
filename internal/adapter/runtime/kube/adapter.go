// Kubernetes runtime adapter operations.
//
// Every operation consumes the persisted engine.EffectIdentity. Start may only
// create the one stable Job; a lost response stays unknown and a retry reuses
// the same Job. Stop suspends that same UID-bound Job and confirms only from
// affirmative evidence; it never deletes the Job and never fabricates a stop
// barrier. Remove is idempotent, attributed and bounded.
package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

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

// Log-source classification sentinels. The adapter wraps the failing inner
// operation in one of them so a caller can name the cause without parsing a
// message or reading a response body. They stay private causes; the public
// rendering remains a bounded, secret-free message.
var (
	// ErrPodListUnavailable reports that the workload pod list could not be read.
	ErrPodListUnavailable = errors.New("kubernetes: workload pod state is unavailable")
	// ErrPodLogNotFound reports that the pod log endpoint no longer has the pod.
	ErrPodLogNotFound = errors.New("kubernetes: workload pod log is not found")
	// ErrPodLogBadRequest reports a rejected pod-log request, including a
	// container that is still waiting to start.
	ErrPodLogBadRequest = errors.New("kubernetes: workload pod log request was rejected")
	// ErrPodLogServerError reports an API-server failure (HTTP 5.x) while
	// reading the pod log; the outcome is unknown but retryable. A transport
	// failure without a status stays the generic log-read failure.
	ErrPodLogServerError = errors.New("kubernetes: workload pod log read failed")
	// ErrRedactionSourceUnavailable reports that the configured values needed to
	// redact output could not be read, so no output may be emitted.
	ErrRedactionSourceUnavailable = container.ErrRedactionSourceUnavailable
)

// checkPreparedWorkspace refuses a request that names a different workspace
// than the one prepared in this process. It cannot prove anything after a
// restart, but it never silently rebinds a live preparation.
func (a *Adapter) checkPreparedWorkspace(identity reconcile.EffectIdentity, workspaceID id.WorkspaceID) error {
	a.mu.Lock()
	prepared, ok := a.prepared[identity.WorkloadID]
	a.mu.Unlock()
	if ok && prepared.workspace != workspaceID {
		return fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
	}
	return nil
}

// Prepare materializes the activation snapshot once and attaches the existing
// retained workspace claim. It never starts a workload and never recreates a
// missing workspace.
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
	resources, err := a.resolveResources(request.Container.Resources)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if err := container.ValidateContainerEnv(request.Container.Env); err != nil {
		return reconcile.Preparation{}, err
	}
	renderedScript, err := script.Render(request.Container.Commands)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	materialization, err := container.BuildMaterialization(request, container.ReadOnlyFileMode)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if err := ctx.Err(); err != nil {
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "preparation cancelled before any effect", err)
	}

	digest := container.PreparationDigest(request, materialization, a.kodemaURL)

	unlock := a.locks.Lock(request.WorkspaceID)
	defer unlock()

	a.mu.Lock()
	existingPrepared, alreadyPrepared := a.prepared[request.Identity.WorkloadID]
	a.mu.Unlock()
	if alreadyPrepared {
		if existingPrepared.workspace != request.WorkspaceID {
			return reconcile.Preparation{}, fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
		}
		if existingPrepared.identity != request.Identity || existingPrepared.digest != digest {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent,
				"activation snapshot content changed for the same workload identity")
		}
	}

	if err := a.checkTenantNamespace(ctx); err != nil {
		return reconcile.Preparation{}, err
	}

	claimName, err := WorkspaceClaimName(request.WorkspaceID)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	claim, err := a.cluster.GetClaim(ctx, a.namespace, claimName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return reconcile.Preparation{}, fault.New(fault.NotFound, "retained workspace claim is missing; preparation never recreates it")
		}
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "retained workspace claim is unreachable", err)
	}
	if claim.Labels[LabelWorkspace] != string(request.WorkspaceID) {
		return reconcile.Preparation{}, fault.New(fault.Forbidden, "workspace claim is not owned by this workspace")
	}

	configMap := a.activationConfigMap(request.Identity, request.WorkspaceID, materialization, renderedScript, digest)
	plan, err := a.prepareToolboxPlan(ctx, &configMap, request)
	if err != nil {
		return reconcile.Preparation{}, err
	}
	if err := a.checkDeclaredSecretContent(ctx, request.Identity, materialization); err != nil {
		return reconcile.Preparation{}, err
	}
	storedConfigMap, err := a.cluster.PutConfigMap(ctx, configMap)
	if err != nil {
		if errors.Is(err, ErrConflict) {
			return reconcile.Preparation{}, fault.New(fault.ChangedIdempotencyContent, "activation snapshot content changed for the same workload identity")
		}
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "activation snapshot write outcome is unknown", err)
	}
	if err := checkStoredActivationConfigMap(storedConfigMap, request.Identity, request.WorkspaceID, string(a.tenant), digest); err != nil {
		return reconcile.Preparation{}, err
	}
	caFile := ""
	if plan.CASecret != "" {
		caFile = WorkloadCAPath
	}
	if _, err := workspace.GitEnvironment(plan.GitURL, caFile, plan.Image != ""); err != nil {
		return reconcile.Preparation{}, err
	}
	outputDir := ""
	if plan.Image != "" && len(request.Container.Outputs) > 0 {
		outputDir, err = workspace.OutputDirectory(request.ActionName)
		if err != nil {
			return reconcile.Preparation{}, err
		}
	}

	secret := a.credentialSecret(request.Identity, request.WorkspaceID, materialization)
	if _, err := a.cluster.PutSecret(ctx, secret); err != nil {
		return reconcile.Preparation{}, fault.Wrap(fault.DependencyUnavailable, "credential secret write outcome is unknown", err)
	}

	activationPaths := make([]string, 0, len(materialization.Files)+1)
	for _, file := range materialization.Files {
		activationPaths = append(activationPaths, file.Path)
	}
	activationPaths = append(activationPaths, container.WorkloadScriptPath)
	sort.Strings(activationPaths)
	secretNames := make([]id.SecretName, 0, len(materialization.Secrets))
	for _, declared := range materialization.Secrets {
		secretNames = append(secretNames, declared.Name)
	}

	var caches []preparedCache
	if len(request.Caches) > 0 {
		caches = make([]preparedCache, len(request.Caches))
		if a.cacheClaimName != "" {
			// Opt-in shared tenant claim override: if the shared cache PVC is
			// missing, evicted, or terminating, degrade cleanly to emptyDir
			// rather than failing preparation or execution.
			resolvedCacheClaim := ""
			if cacheClaim, claimErr := a.cluster.GetClaim(ctx, a.namespace, a.cacheClaimName); claimErr == nil &&
				cacheClaim.Namespace == a.namespace && cacheClaim.DeletionTimestamp == nil {
				resolvedCacheClaim = a.cacheClaimName
			}
			for index, cache := range request.Caches {
				subPath := ""
				if resolvedCacheClaim != "" {
					subPath = cache.Key
				}
				caches[index] = preparedCache{
					name:      cache.Name,
					key:       cache.Key,
					claimName: resolvedCacheClaim,
					subPath:   subPath,
				}
			}
		} else {
			// Default per-slug persistent PVC mode: mount each owned
			// kodema-cache-<key> PVC at its root (no subPath). Missing,
			// unowned, workspace-labelled, or terminating claims degrade
			// cleanly to emptyDir.
			for index, cache := range request.Caches {
				entry := preparedCache{
					name: cache.Name,
					key:  cache.Key,
				}
				if cacheClaimName, nameErr := CacheClaimName(cache.Key); nameErr == nil {
					if claim, claimErr := a.cluster.GetClaim(ctx, a.namespace, cacheClaimName); claimErr == nil &&
						ownedCacheClaim(claim, a.namespace, a.tenant, cache.Key) {
						entry.claimName = cacheClaimName
					}
				}
				caches[index] = entry
			}
		}
	}

	var projectID id.ProjectID
	if candidate := runtime.ExecutionProject(ctx); candidate != "" {
		if _, err := id.ParseProjectID(string(candidate)); err == nil && validKubeLabelValue(string(candidate)) {
			projectID = candidate
		}
	}

	prepared := preparedWorkload{
		identity:   request.Identity,
		digest:     digest,
		workspace:  request.WorkspaceID,
		project:    projectID,
		role:       container.WorkloadRole(request),
		actionName: request.ActionName,
		taskID:     request.TaskID,
		container: containerDeclaration{
			image: request.Container.Image,
			env:   append([]definition.EnvVar(nil), request.Container.Env...),
		},
		claimName:       claimName,
		configMap:       configMap.Name,
		secret:          secret.Name,
		job:             workloadJobName(request.Identity.WorkloadID),
		activationPaths: activationPaths,
		toolboxImage:    plan.Image,
		caSecret:        plan.CASecret,
		gitURL:          plan.GitURL,
		outputDir:       outputDir,
		caches:          caches,
		secretNames:     secretNames,
		resources:       resources,
	}
	a.mu.Lock()
	a.prepared[request.Identity.WorkloadID] = prepared
	a.mu.Unlock()

	return reconcile.Preparation{
		WorkspaceID: request.WorkspaceID,
		Detail: fmt.Sprintf("materialized activation snapshot %q and credential secret %q for %s",
			configMap.Name, secret.Name, container.DescribeIdentity(request.Identity)),
	}, nil
}

// toolboxPlan is pinned in the immutable activation ConfigMap, alongside the
// rendered script. The Job projects only the script and declared files.
type toolboxPlan struct {
	Image    string
	GitURL   string
	CASecret string
	Outputs  []id.OutputKey
}

func (a *Adapter) prepareToolboxPlan(ctx context.Context, cm *ConfigMap, request reconcile.PrepareRequest) (toolboxPlan, error) {
	plan := toolboxPlan{Image: a.toolboxImage, GitURL: a.gitURL, CASecret: a.workloadCA.SecretName, Outputs: request.Container.Outputs}
	existing, err := a.cluster.GetConfigMap(ctx, a.namespace, cm.Name)
	switch {
	case err == nil:
		// Use the original installation settings on a retry, even across a
		// controller upgrade. An older activation had no toolbox.
		if !existing.Immutable {
			return toolboxPlan{}, fault.New(fault.Forbidden, "activation snapshot is not immutable")
		}
		if err := checkStoredActivationConfigMap(existing, request.Identity, request.WorkspaceID, string(a.tenant), cm.Labels[LabelPreparation]); err != nil {
			return toolboxPlan{}, err
		}
		raw, ok := existing.Data[toolboxPlanKey]
		if !ok {
			return toolboxPlan{CASecret: a.workloadCA.SecretName}, nil
		}
		if err := json.Unmarshal(raw, &plan); err != nil || plan.Image == "" {
			return toolboxPlan{}, fault.New(fault.InvalidInput, "invalid stored toolbox plan")
		}
		if err := validateToolbox(plan.Image, plan.GitURL, a.allowUnpinnedToolbox); err != nil {
			return toolboxPlan{}, err
		}
		if plan.CASecret != "" {
			if err := validateWorkloadCASecretName(plan.CASecret); err != nil {
				return toolboxPlan{}, err
			}
		}
		// Keep the request's declarations in the comparison PutConfigMap
		// performs on the whole immutable snapshot.
		plan.Outputs = request.Container.Outputs
	case !errors.Is(err, ErrNotFound):
		return toolboxPlan{}, fault.Wrap(fault.DependencyUnavailable, "activation snapshot is unavailable", err)
	}
	if plan.Image != "" {
		payload, err := json.Marshal(plan)
		if err != nil {
			return toolboxPlan{}, err
		}
		cm.Data[toolboxPlanKey] = payload
	}
	return plan, nil
}

// checkStoredActivationConfigMap verifies the durable ConfigMap labels match
// the requested identity, workspace and preparation digest before any Secret
// update or in-memory rebinding occurs.
func checkStoredActivationConfigMap(stored ConfigMap, identity reconcile.EffectIdentity, workspaceID id.WorkspaceID, tenant, digest string) error {
	if stored.Labels[LabelTenant] != tenant {
		return fault.New(fault.Forbidden, "activation snapshot is not owned by this tenant")
	}
	if stored.Labels[LabelWorkspace] != string(workspaceID) {
		return fault.New(fault.Forbidden, "workspace does not match the prepared workspace")
	}
	if stored.Labels[LabelWorkload] != string(identity.WorkloadID) ||
		stored.Labels[LabelExecution] != string(identity.ExecutionID) ||
		stored.Labels[LabelAction] != string(identity.ActionID) ||
		stored.Labels[LabelPreparation] != digest {
		return fault.New(fault.ChangedIdempotencyContent,
			"activation snapshot content changed for the same workload identity")
	}
	return nil
}

// checkDeclaredSecretContent refuses different declared secret values for an
// already-prepared identity. The execution credential is deliberately excluded:
// RefreshCredential may rotate it without a restart.
func (a *Adapter) checkDeclaredSecretContent(ctx context.Context, identity reconcile.EffectIdentity, materialization workspace.Materialization) error {
	existing, err := a.cluster.GetSecret(ctx, a.namespace, credentialSecretName(identity.WorkloadID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return fault.Wrap(fault.DependencyUnavailable, "credential secret is unreachable", err)
	}
	expected := make(map[string]string, len(materialization.Secrets))
	for _, declared := range materialization.Secrets {
		expected[string(declared.Name)] = declared.Value
	}
	for name, value := range existing.Data {
		if name == id.SecretTokenName {
			continue
		}
		want, ok := expected[name]
		if !ok || string(value) != want {
			return fault.New(fault.ChangedIdempotencyContent, "declared secret content changed for the same workload identity")
		}
	}
	for name, want := range expected {
		got, ok := existing.Data[name]
		if !ok || string(got) != want {
			return fault.New(fault.ChangedIdempotencyContent, "declared secret content changed for the same workload identity")
		}
	}
	return nil
}

// Start creates the one stable Job for a prepared identity. A lost create
// response stays unknown; a retry reuses the same Job and never replays a fresh
// command.
func (a *Adapter) Start(ctx context.Context, request reconcile.StartRequest) (reconcile.StartResult, error) {
	unknown := func(detail string, err error) (reconcile.StartResult, error) {
		return reconcile.StartResult{
			Outcome:    reconcile.StartUnknown,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     detail,
		}, err
	}
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return unknown("start request identity is invalid", err)
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return unknown("start request workspace is invalid", err)
	}
	if err := ctx.Err(); err != nil {
		return unknown("start cancelled before any effect", fault.Wrap(fault.DependencyUnavailable, "start cancelled before any effect", err))
	}

	unlock := a.locks.Lock(request.WorkspaceID)
	defer unlock()

	a.mu.Lock()
	prepared, ok := a.prepared[request.Identity.WorkloadID]
	a.mu.Unlock()

	if !ok {
		// Recovery after a process restart: an existing stable Job is accepted
		// rather than replaced. Without a Job there is no complete preparation,
		// so Start refuses instead of preparing implicitly.
		existing, err := a.cluster.GetJob(ctx, a.namespace, workloadJobName(request.Identity.WorkloadID))
		if err == nil {
			if existing.Annotations[AnnotationIdempotencyKey] != request.Identity.IdempotencyKey {
				return unknown("existing Job carries a different idempotency key", fault.New(fault.ChangedIdempotencyContent, "existing Job carries a different idempotency key"))
			}
			if existing.Labels[LabelWorkspace] != string(request.WorkspaceID) {
				return unknown("existing Job is bound to a different workspace", fault.New(fault.Forbidden, "existing Job is bound to a different workspace"))
			}
			return reconcile.StartResult{
				Outcome:    reconcile.StartAccepted,
				WorkloadID: request.Identity.WorkloadID,
				Detail:     fmt.Sprintf("reused existing stable Job %q", existing.Name),
			}, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return unknown("Job state is unreachable", fault.Wrap(fault.DependencyUnavailable, "Job state is unreachable", err))
		}
		return unknown("start requires a completed preparation", fault.New(fault.InvalidInput, "start requires a completed preparation"))
	}

	if prepared.workspace != request.WorkspaceID {
		return unknown("start workspace does not match the prepared workspace", fault.New(fault.Forbidden, "start workspace does not match the prepared workspace"))
	}
	if prepared.identity != request.Identity {
		return unknown("start identity does not match the prepared identity", fault.New(fault.ChangedIdempotencyContent, "start identity does not match the prepared identity"))
	}
	claim, err := a.cluster.GetClaim(ctx, a.namespace, prepared.claimName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return unknown("retained workspace claim is missing", fault.New(fault.NotFound, "retained workspace claim is missing; start never recreates it"))
		}
		return unknown("retained workspace claim is unreachable", fault.Wrap(fault.DependencyUnavailable, "retained workspace claim is unreachable", err))
	}
	if claim.Labels[LabelWorkspace] != string(request.WorkspaceID) {
		return unknown("workspace claim is not owned by this workspace", fault.New(fault.Forbidden, "workspace claim is not owned by this workspace"))
	}
	if _, err := a.cluster.GetConfigMap(ctx, a.namespace, prepared.configMap); err != nil {
		return unknown("activation snapshot is unavailable", fault.Wrap(fault.DependencyUnavailable, "activation snapshot is unavailable", err))
	}
	if _, err := a.cluster.GetSecret(ctx, a.namespace, prepared.secret); err != nil {
		return unknown("credential secret is unavailable", fault.Wrap(fault.DependencyUnavailable, "credential secret is unavailable", err))
	}

	existing, err := a.cluster.GetJob(ctx, a.namespace, prepared.job)
	switch {
	case err == nil:
		if existing.Annotations[AnnotationIdempotencyKey] != request.Identity.IdempotencyKey {
			return unknown("existing Job carries a different idempotency key", fault.New(fault.ChangedIdempotencyContent, "existing Job carries a different idempotency key"))
		}
		if existing.Labels[LabelWorkspace] != string(request.WorkspaceID) {
			return unknown("existing Job is bound to a different workspace", fault.New(fault.Forbidden, "existing Job is bound to a different workspace"))
		}
		return reconcile.StartResult{
			Outcome:    reconcile.StartAccepted,
			WorkloadID: request.Identity.WorkloadID,
			Detail:     fmt.Sprintf("reused existing stable Job %q", existing.Name),
		}, nil
	case errors.Is(err, ErrNotFound):
		// Create below.
	default:
		return unknown("Job state is unreachable", fault.Wrap(fault.DependencyUnavailable, "Job state is unreachable", err))
	}

	job, err := a.jobManifest(prepared, request.Identity)
	if err != nil {
		return unknown("Job manifest construction failed", err)
	}
	if _, err := a.cluster.CreateJob(ctx, job); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			if existing, getErr := a.cluster.GetJob(ctx, a.namespace, prepared.job); getErr == nil {
				if existing.Annotations[AnnotationIdempotencyKey] != request.Identity.IdempotencyKey {
					return unknown("existing Job carries a different idempotency key", fault.New(fault.ChangedIdempotencyContent, "existing Job carries a different idempotency key"))
				}
				if existing.Labels[LabelWorkspace] != string(request.WorkspaceID) {
					return unknown("existing Job is bound to a different workspace", fault.New(fault.Forbidden, "existing Job is bound to a different workspace"))
				}
				return reconcile.StartResult{
					Outcome:    reconcile.StartAccepted,
					WorkloadID: request.Identity.WorkloadID,
					Detail:     fmt.Sprintf("reused concurrently created stable Job %q", prepared.job),
				}, nil
			}
		}
		return unknown("Job creation outcome is unknown", fault.Wrap(fault.UncertainExternalEffect, "Job creation outcome is unknown", err))
	}
	return reconcile.StartResult{
		Outcome:    reconcile.StartAccepted,
		WorkloadID: request.Identity.WorkloadID,
		Detail:     fmt.Sprintf("created stable Job %q", job.Name),
	}, nil
}

// Observe reports the declared Kernel vocabulary for the stable identity.
// Absence and node loss stay uncertain.
func (a *Adapter) Observe(ctx context.Context, request reconcile.ObserveRequest) (engine.Observation, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return engine.UnknownObservation(), err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return engine.UnknownObservation(), err
	}
	if err := a.checkPreparedWorkspace(request.Identity, request.WorkspaceID); err != nil {
		return engine.UnknownObservation(), err
	}

	jobFound := false
	_, err := a.cluster.GetJob(ctx, a.namespace, workloadJobName(request.Identity.WorkloadID))
	switch {
	case err == nil:
		jobFound = true
	case errors.Is(err, ErrNotFound):
		jobFound = false
	default:
		return engine.UnknownObservation(), fault.Wrap(fault.DependencyUnavailable, "Job state is unreachable", err)
	}

	pods, err := a.cluster.ListPods(ctx, a.namespace, workloadSelector(request.Identity))
	if err != nil {
		return engine.UnknownObservation(), fault.Wrap(fault.DependencyUnavailable, "pod state is unreachable", err)
	}
	return observationFor(jobFound, pods, a.memoryLimitFor(request.Identity.WorkloadID)), nil
}

// memoryLimitFor returns the configured memory limit of one prepared workload,
// falling back to the installation policy limit after a process restart.
func (a *Adapter) memoryLimitFor(workload id.WorkloadID) string {
	a.mu.Lock()
	prepared, ok := a.prepared[workload]
	a.mu.Unlock()
	if ok && strings.TrimSpace(prepared.resources.MemoryLimit) != "" {
		return prepared.resources.MemoryLimit
	}
	return a.policy.MemoryLimit
}

// ownedCacheClaim reports whether claim is a live, tenant-owned persistent
// cache PVC matching cacheKey and never a retained workspace claim.
func ownedCacheClaim(claim Claim, namespace string, tenant id.TenantID, cacheKey string) bool {
	return claim.Namespace == namespace &&
		claim.Labels[LabelTenant] == string(tenant) &&
		claim.Labels[LabelCacheKey] == cacheKey &&
		claim.Labels[LabelWorkspace] == "" &&
		claim.DeletionTimestamp == nil
}

// Logs returns ordered, bounded and redacted chunks. It carries no lifecycle
// authority and fails closed when the redaction source is unavailable.
func (a *Adapter) Logs(ctx context.Context, request reconcile.LogsRequest) (reconcile.LogBatch, error) {
	if err := container.ValidateLogsRequest(request); err != nil {
		return reconcile.LogBatch{}, err
	}
	stdoutOffset, stderrOffset := container.DecodeLogCursor(request.AfterOffset)
	if stdoutOffset < 0 || stdoutOffset > container.MaxStdoutLogCursorPosition ||
		stderrOffset < 0 || stderrOffset > container.MaxStderrLogCursorPosition {
		return reconcile.LogBatch{}, fault.New(fault.InvalidInput,
			"log position exceeds the continuation-token range")
	}

	pods, err := a.cluster.ListPods(ctx, a.namespace, workloadSelector(request.Identity))
	if err != nil {
		return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "pod state is unreachable",
			errors.Join(ErrPodListUnavailable, err))
	}
	if len(pods) != 1 {
		return reconcile.LogBatch{}, fault.New(fault.NotFound, fmt.Sprintf("log read requires exactly one workload pod; observed %d", len(pods)))
	}
	pod := pods[0]

	secret, err := a.cluster.GetSecret(ctx, a.namespace, credentialSecretName(request.Identity.WorkloadID))
	if err != nil {
		// Never emit output when the configured values cannot be redacted. A
		// missing redaction source is not container absence: it stays a
		// retryable, fail-closed dependency fault. A NotFound cause is dropped so
		// no errors.Is(ErrNotFound) caller can mistake it for a vanished
		// container.
		cause := ErrRedactionSourceUnavailable
		if !errors.Is(err, ErrNotFound) {
			cause = errors.Join(cause, err)
		}
		return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "redaction source is unavailable; log read refused", cause)
	}
	values := redactionValues(secret)

	page, err := a.cluster.ReadPodLogs(ctx, a.namespace, pod.Name, PodLogRequest{
		Stream:       LogStreamSplit,
		Offset:       stdoutOffset,
		StderrOffset: stderrOffset,
		Limit:        request.Limit,
	})
	if err != nil {
		// A pod that vanished after the successful list keeps NotFound so the
		// collector records its explicit unavailable marker. Every other log
		// failure stays retryable: a terminated container's log can still be
		// readable, and waiting-to-start, rejection, server and transport
		// failures are not proof that output was never produced.
		if errors.Is(err, ErrNotFound) {
			return reconcile.LogBatch{}, fault.Wrap(fault.NotFound, "pod log is unavailable",
				errors.Join(ErrPodLogNotFound, err))
		}
		switch {
		case errors.Is(err, ErrPodLogBadRequest):
			return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "pod log read was rejected",
				errors.Join(ErrPodLogBadRequest, err))
		case errors.Is(err, ErrPodLogServerError):
			return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "pod log read is unavailable",
				errors.Join(ErrPodLogServerError, err))
		default:
			return reconcile.LogBatch{}, fault.Wrap(fault.DependencyUnavailable, "pod log read is unavailable", err)
		}
	}
	// A truncated page is a strict prefix of its stream: a value that starts
	// inside it may still complete after the page. Only the final page (no
	// truncation) is known to hold the whole remaining stream, so nothing there
	// can extend past its last byte.
	stdoutChunk, hasStdout, nextStdout, err := container.BuildLogChunk(container.LogPageInput{
		Stream:        reconcile.LogStdout,
		Offset:        stdoutOffset,
		Sequence:      1,
		Raw:           page.Data,
		PageTruncated: page.Truncated,
		Values:        values,
		Limit:         request.Limit,
		MayGrow:       page.Truncated,
	})
	if err != nil {
		return reconcile.LogBatch{}, err
	}
	stderrChunk, hasStderr, nextStderr, err := container.BuildLogChunk(container.LogPageInput{
		Stream:        reconcile.LogStderr,
		Offset:        stderrOffset,
		Sequence:      2,
		Raw:           page.StderrData,
		PageTruncated: page.StderrTruncated,
		Values:        values,
		Limit:         request.Limit,
		MayGrow:       page.StderrTruncated,
	})
	if err != nil {
		return reconcile.LogBatch{}, err
	}
	batch := reconcile.LogBatch{NextOffset: container.EncodeLogCursor(nextStdout, nextStderr)}
	if hasStdout {
		batch.Chunks = append(batch.Chunks, stdoutChunk)
	}
	if hasStderr {
		batch.Chunks = append(batch.Chunks, stderrChunk)
	}
	return batch, nil
}

// Stop requests suspension of the one stable Job and reports a stop barrier
// only from affirmative evidence. The PATCH is UID-guarded intent, not
// confirmation: the same Job must be observed again without a deletion
// timestamp, with spec.suspend true, a controller acknowledgment (or an already
// terminal Job), zero active pods and a present zero terminating count, and a
// fresh workload-pod list must have succeeded with no workload pod. A missing
// Job, a replaced Job, a resumed Job and a failed read all stay pending.
//
// The same UID-guarded PATCH clears spec.ttlSecondsAfterFinished, so a Job that
// finishes while suspended is not removed by the TTL controller before Remove
// deletes it. The independent affirmative terminated-container path is preserved
// unchanged, including for a Job that already carries a deletion timestamp: such
// a Job is never suspension evidence, but its workload pods are still listed and
// an affirmatively terminated container still confirms. Absence of pods is never
// stop evidence.
func (a *Adapter) Stop(ctx context.Context, request reconcile.StopRequest) (engine.StopEvidence, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return engine.PendingStop("stop request identity is invalid"), err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return engine.PendingStop("stop request workspace is invalid"), err
	}
	if err := a.checkPreparedWorkspace(request.Identity, request.WorkspaceID); err != nil {
		return engine.PendingStop("stop request workspace does not match the prepared workspace"), err
	}

	jobName := workloadJobName(request.Identity.WorkloadID)
	before, err := a.cluster.GetJob(ctx, a.namespace, jobName)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		// A missing Job is never suspension evidence, but the independent
		// terminated-container evidence path still applies. An absent Job with
		// no retained termination evidence can never confirm.
		pods, listErr := a.cluster.ListPods(ctx, a.namespace, workloadSelector(request.Identity))
		if listErr != nil {
			return engine.PendingStop("stop unconfirmed: pod state is unreachable"),
				fault.Wrap(fault.DependencyUnavailable, "pod state is unreachable", listErr)
		}
		return stopEvidenceFor(pods), nil
	default:
		return engine.PendingStop("stop unconfirmed: Job state is unreachable"),
			fault.Wrap(fault.DependencyUnavailable, "Job state is unreachable", err)
	}

	if problem := stopJobIdentityMismatch(before, request.Identity, request.WorkspaceID, a.namespace, string(a.tenant)); problem != "" {
		return engine.PendingStop("stop unconfirmed: " + problem),
			fault.New(fault.Forbidden, problem)
	}
	if before.DeletionTimestamp != nil {
		// A Job already being deleted (a foreground or TTL deletion that started
		// before this Stop) can never confirm a suspension, and the PATCH is
		// refused. The independent terminated-container path still applies: list
		// the workload pods and confirm only from affirmative termination.
		// Absence of pods is never stop evidence, and a running or terminating
		// pod without terminated-container evidence stays pending.
		pods, listErr := a.cluster.ListPods(ctx, a.namespace, workloadSelector(request.Identity))
		if listErr != nil {
			return engine.PendingStop("stop unconfirmed: pod state is unreachable"),
				fault.Wrap(fault.DependencyUnavailable, "pod state is unreachable", listErr)
		}
		return stopEvidenceFor(pods), nil
	}
	expectedUID := before.UID

	if _, suspendErr := a.cluster.SuspendJob(ctx, a.namespace, jobName, expectedUID); suspendErr != nil {
		switch {
		case errors.Is(suspendErr, ErrNotFound):
			return engine.PendingStop("stop unconfirmed: the Job disappeared before suspension"),
				fault.Wrap(fault.NotFound, "Job disappeared before suspension", suspendErr)
		case errors.Is(suspendErr, ErrConflict):
			return engine.PendingStop("stop unconfirmed: the Job identity changed before suspension"),
				fault.Wrap(fault.VersionConflict, "Job identity changed before suspension", suspendErr)
		default:
			// A definite 4xx refusal (for example the server's immutable-UID
			// validation) is not an unknown outcome: nothing was suspended. Keep
			// that class so cleanup does not spend its bound on a retry that can
			// never succeed. Transport loss and 5xx stay uncertain.
			if class, ok := fault.ClassOf(suspendErr); ok && class == fault.DependencyUnavailable {
				return engine.PendingStop("stop unconfirmed: Job suspension was refused"),
					fault.Wrap(fault.DependencyUnavailable, "Job suspension was refused", suspendErr)
			}
			return engine.PendingStop("stop unconfirmed: Job suspension outcome is unknown"),
				fault.Wrap(fault.UncertainExternalEffect, "Job suspension outcome is unknown", suspendErr)
		}
	}

	// Fresh observation bracketing the pod list: the intent PATCH is not
	// evidence, so the acknowledged state must be read back. A failed read
	// never confirms.
	mid, err := a.cluster.GetJob(ctx, a.namespace, jobName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return engine.PendingStop("stop unconfirmed: the Job disappeared during confirmation"), nil
		}
		return engine.PendingStop("stop unconfirmed: Job state is unreachable"),
			fault.Wrap(fault.DependencyUnavailable, "Job state is unreachable", err)
	}
	pods, err := a.cluster.ListPods(ctx, a.namespace, workloadSelector(request.Identity))
	if err != nil {
		return engine.PendingStop("stop unconfirmed: pod state is unreachable"),
			fault.Wrap(fault.DependencyUnavailable, "pod state is unreachable", err)
	}
	if len(pods) != 0 {
		// Preserve the existing independent evidence path: one affirmatively
		// terminated workload container confirms; anything else stays pending.
		return stopEvidenceFor(pods), nil
	}
	after, err := a.cluster.GetJob(ctx, a.namespace, jobName)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return engine.PendingStop("stop unconfirmed: the Job disappeared during confirmation"), nil
		}
		return engine.PendingStop("stop unconfirmed: Job state is unreachable"),
			fault.Wrap(fault.DependencyUnavailable, "Job state is unreachable", err)
	}
	if suspensionConfirmed(before, mid, after, request.Identity, request.WorkspaceID, a.namespace, string(a.tenant), expectedUID) {
		return engine.ConfirmedStop(fmt.Sprintf("Job %q is suspended with no workload pod and an acknowledged zero active/terminating count", jobName)), nil
	}
	return engine.PendingStop("stop unconfirmed: the suspended Job has not settled into an affirmed stop"), nil
}

// Remove is idempotent, attributed and bounded cleanup. It never releases or
// deletes the retained workspace claim; engine.Workspace.Release owns that
// decision. A live or unreachable workload retains the claim and names it in
// Detail.
func (a *Adapter) Remove(ctx context.Context, request reconcile.RemoveRequest) (reconcile.Removal, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.Removal{Detail: "removal request identity is invalid"}, err
	}
	if err := container.ValidateWorkspaceID(request.WorkspaceID); err != nil {
		return reconcile.Removal{Detail: "removal request workspace is invalid"}, err
	}
	if err := a.checkPreparedWorkspace(request.Identity, request.WorkspaceID); err != nil {
		return reconcile.Removal{Detail: "removal request workspace does not match the prepared workspace"}, err
	}

	unlock := a.locks.Lock(request.WorkspaceID)
	defer unlock()

	claimName, err := WorkspaceClaimName(request.WorkspaceID)
	if err != nil {
		return reconcile.Removal{Detail: "workspace claim name is invalid"}, err
	}

	pods, err := a.cluster.ListPods(ctx, a.namespace, workloadSelector(request.Identity))
	if err != nil {
		return reconcile.Removal{Detail: retainedDetail(claimName, "pod state is unreachable")}, nil
	}
	if !workspaceReleasable(pods) {
		// A live, terminating or unknown workload can still write to the
		// workspace. Retain every owned resource and name the claim.
		return reconcile.Removal{Detail: retainedDetail(claimName, "a workload pod is still live")}, nil
	}

	deleteResource := func(remove func(context.Context) error) error {
		return container.RetryWithBackoff(ctx, a.clock, a.cleanup, func(ctx context.Context) error {
			err := remove(ctx)
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		})
	}
	if err := deleteResource(func(ctx context.Context) error {
		return a.cluster.DeleteJob(ctx, a.namespace, workloadJobName(request.Identity.WorkloadID))
	}); err != nil {
		return reconcile.Removal{Detail: retainedDetail(claimName, "Job deletion failed: "+err.Error())}, nil
	}
	if err := deleteResource(func(ctx context.Context) error {
		return a.cluster.DeleteConfigMap(ctx, a.namespace, activationConfigMapName(request.Identity.WorkloadID))
	}); err != nil {
		return reconcile.Removal{Detail: retainedDetail(claimName, "activation snapshot deletion failed: "+err.Error())}, nil
	}
	if err := deleteResource(func(ctx context.Context) error {
		return a.cluster.DeleteSecret(ctx, a.namespace, credentialSecretName(request.Identity.WorkloadID))
	}); err != nil {
		return reconcile.Removal{Detail: retainedDetail(claimName, "credential secret deletion failed: "+err.Error())}, nil
	}
	a.mu.Lock()
	delete(a.prepared, request.Identity.WorkloadID)
	a.mu.Unlock()
	// Remove never releases the retained workspace claim. Only
	// engine.Workspace.Release may delete it, after every owned identity is
	// confirmed removed.
	return reconcile.Removal{
		Removed: true,
		Detail:  fmt.Sprintf("removed owned workload resources; retained workspace claim %q awaits Workspace.Release", claimName),
	}, nil
}

// retainedDetail names the retained workspace claim so pending cleanup is
// attributable.
func retainedDetail(claimName, reason string) string {
	return fmt.Sprintf("cleanup pending: retained workspace claim %q: %s", claimName, reason)
}

// RefreshCredential replaces only the mutable credential Secret. It never
// restarts or replaces the Job and is recoverable after a lost update response
// or a stale resource version.
func (a *Adapter) RefreshCredential(ctx context.Context, request reconcile.CredentialRequest) (reconcile.CredentialRefresh, error) {
	if err := container.ValidateIdentity(request.Identity); err != nil {
		return reconcile.CredentialRefresh{}, err
	}
	if strings.TrimSpace(request.Credential) == "" {
		return reconcile.CredentialRefresh{}, fault.New(fault.InvalidInput, "credential refresh requires a non-empty credential")
	}

	secret, err := a.cluster.GetSecret(ctx, a.namespace, credentialSecretName(request.Identity.WorkloadID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return reconcile.CredentialRefresh{}, fault.New(fault.NotFound, "credential secret is missing")
		}
		return reconcile.CredentialRefresh{}, fault.Wrap(fault.DependencyUnavailable, "credential secret is unreachable", err)
	}

	data := make(map[string][]byte, len(secret.Data))
	for name, value := range secret.Data {
		data[name] = append([]byte(nil), value...)
	}
	data[id.SecretTokenName] = []byte(request.Credential)
	secret.Data = data
	if _, err := a.cluster.PutSecret(ctx, secret); err != nil {
		if errors.Is(err, ErrConflict) {
			return reconcile.CredentialRefresh{Detail: "credential update used a stale resource version"},
				fault.Wrap(fault.VersionConflict, "credential update used a stale resource version", err)
		}
		return reconcile.CredentialRefresh{Detail: "credential update outcome is unknown"},
			fault.Wrap(fault.UncertainExternalEffect, "credential update outcome is unknown", err)
	}
	return reconcile.CredentialRefresh{
		Delivered: true,
		Detail:    fmt.Sprintf("replaced credential for %s without restarting the Job", container.DescribeIdentity(request.Identity)),
	}, nil
}
