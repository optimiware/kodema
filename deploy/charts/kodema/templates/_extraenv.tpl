{{- /*
config.extraEnv is a narrow pass-through for the optional server variables the
Go composition reads from the environment. It is not a general pod-spec escape
hatch and it never carries a secret:

  * only the allowlisted names below are accepted. The same expression lives in
    values.schema.json (propertyNames); a unit test requires the two to agree;
  * a value is a literal string, never an object, so a value file cannot smuggle
    in valueFrom as "extra environment";
  * a name the chart already renders itself is refused: a duplicate env entry is
    not an error in Kubernetes, the last duplicate wins, and the override would
    silently replace the production composition (profile, TLS dir, database URL,
    ...);
  * development tokens (KODEMA_DEV_*), cloud credentials (SCW_/AWS_/GOOGLE_) and
    credential-shaped names are refused. The only exception is a name ending in
    _FILE (for example KODEMA_OIDC_CA_FILE): that is a mounted path, not a
    credential value;
  * a value containing URL userinfo with a password (scheme://user:password@host)
    is refused, because it would render a literal password into the manifest.

The schema is the first line of defence; this guard repeats every rule so a
rendering path that skipped schema validation still cannot weaken the production
composition.
*/ -}}
{{- define "kodema.validateExtraEnv" -}}
{{- $allowlist := "^(KODEMA_OIDC_(ISSUER|AUDIENCE|JWKS_URL|TENANT_CLAIM|SUBJECT_CLAIM|PROJECT_CLAIM|CA_FILE)|KODEMA_DISCOVERY_(ISSUER|CLIENT_ID|AUDIENCE|AUTHORIZATION_ENDPOINT|TOKEN_ENDPOINT|DEVICE_AUTHORIZATION_ENDPOINT|REVOCATION_ENDPOINT|SCOPES|API_URL)|KODEMA_TENANT_NAMESPACE|KODEMA_WORKLOAD_((CPU|MEMORY)_(REQUEST|LIMIT)|MAX_(CPU|MEMORY)_LIMIT))$" }}
{{- /* Keep in sync with chartDirectEnvNames in internal/installation. */ -}}
{{- $fixed := list "KODEMA_PROFILE" "KODEMA_RUNTIME" "KODEMA_INSTALLATION_ID" "KODEMA_PUBLIC_URL" "KODEMA_MCP_PATH" "KODEMA_DATA_DIR" "KODEMA_DATABASE_URL" "KODEMA_CONTROLLER_DATABASE_URL" "KODEMA_PUBLISHER_DATABASE_URL" "KODEMA_AUTH_DATABASE_URL" "KODEMA_NATS_URL" "KODEMA_NATS_CA_FILE" "KODEMA_NATS_CERT_FILE" "KODEMA_NATS_KEY_FILE" "KODEMA_NATS_USER" "KODEMA_NATS_PASSWORD" "KODEMA_NATS_CALLOUT_USER" "KODEMA_NATS_CALLOUT_PASSWORD" "KODEMA_NATS_CALLOUT_ISSUER_SEED" "KODEMA_NATS_CALLOUT_ACCOUNT" "KODEMA_KEY_ADAPTER" "KODEMA_KEY_REF" "KODEMA_KEY_CURRENT_VERSION" "KODEMA_KEY_BLOBS_DIR" "KODEMA_KEYFILE" "KODEMA_TLS_DIR" "KODEMA_TENANT" "KODEMA_TENANTS" "KODEMA_GIT_STORAGE_ROOT" "KODEMA_WORKSPACE_PVC_STORAGE_CLASS" "KODEMA_WORKSPACE_PVC_SIZE" "KODEMA_WORKSPACE_PVC_ACCESS_MODE" "KODEMA_WORKLOAD_CA_SECRET" "KODEMA_TOOLBOX_IMAGE" "KODEMA_WORKLOAD_GIT_URL" }}
{{- if not (has .Values.environment (list "test" "production")) }}
{{- fail (printf "environment %q is unsupported in-cluster; only \"production\" and \"test\" boot inside Kubernetes (development uses in-memory state, --dev-token and plain HTTP)" .Values.environment) }}
{{- end }}
{{- /*
The tenant list is the installation's served set: one namespace, RBAC role and
network policy per entry, and KODEMA_TENANTS renders it verbatim. An id that is
not a lowercase DNS label would derive an invalid namespace, a duplicate would
merge two tenants' resources, and a singleton namespace override cannot name a
namespace per tenant. values.schema.json states the same rules.
*/ -}}
{{- if not .Values.tenants }}{{- fail "tenants must not be empty" }}{{- end }}
{{- $tenantNames := dict }}
{{- range $tenant := .Values.tenants }}
{{- $name := toString $tenant }}
{{- if not (regexMatch "^[a-z0-9]([a-z0-9-]{0,47}[a-z0-9])?$" $name) }}
{{- fail (printf "tenants entry %q is not a lowercase DNS label; the chart derives the namespace kodema-tenant-<tenant> from it" $name) }}
{{- end }}
{{- if hasKey $tenantNames $name }}
{{- fail (printf "tenants repeats %q; one tenant id would own two namespaces and RBAC roles" $name) }}
{{- end }}
{{- $_ := set $tenantNames $name true }}
{{- end }}
{{- if and (gt (len .Values.tenants) 1) .Values.config .Values.config.extraEnv (hasKey .Values.config.extraEnv "KODEMA_TENANT_NAMESPACE") }}
{{- fail "config.extraEnv.KODEMA_TENANT_NAMESPACE is a singleton override; a multi-tenant installation derives one namespace per tenant" }}
{{- end }}
{{- range $name, $value := .Values.config.extraEnv }}
{{- if not (kindIs "string" $value) }}
{{- fail (printf "config.extraEnv.%s must be a string literal value; objects and valueFrom are not supported" $name) }}
{{- end }}
{{- if eq (toString $value) "" }}
{{- fail (printf "config.extraEnv.%s must not be empty; an empty value would render an empty environment variable instead of a named configuration error" $name) }}
{{- end }}
{{- if has $name $fixed }}
{{- fail (printf "config.extraEnv.%s collides with a fixed variable the chart renders; a duplicate env entry would override the production composition" $name) }}
{{- end }}
{{- if hasPrefix "KODEMA_DEV_" $name }}
{{- fail (printf "config.extraEnv.%s must not set a development token; the production composition has no development credentials" $name) }}
{{- end }}
{{- if regexMatch "^(SCW|AWS|GOOGLE)_" $name }}
{{- fail (printf "config.extraEnv.%s must not set a cloud credential; credentials come only from the kodema-kms-credentials Secret (scw-kms) or the ServiceAccount's workload identity (aws-kms, gcp-kms)" $name) }}
{{- end }}
{{- if and (regexMatch "(?i)(token|secret|password|passwd|credential|private|key)" $name) (not (regexMatch "_FILE$" $name)) }}
{{- fail (printf "config.extraEnv.%s looks like a credential name; only a mounted *_FILE path may contain such a word, never a value" $name) }}
{{- end }}
{{- if not (regexMatch $allowlist $name) }}
{{- fail (printf "config.extraEnv.%s is not an allowlisted optional server variable; see deploy/charts/kodema/values.yaml" $name) }}
{{- end }}
{{- if regexMatch "://[^/@]*:[^@]*@" $value }}
{{- fail (printf "config.extraEnv.%s value looks like a URL with inline userinfo credentials; put the secret in a Secret, not in a value" $name) }}
{{- end }}
{{- end }}
{{- /*
Every in-cluster profile (production and test) must provide the OIDC
configuration cmd/kodema/production.go and cmd/kodema/test_profile.go refuse to
start without. Key-adapter credentials are checked by profile:

  * test uses config.keyAdapter=keyfile (mounted from secrets.keys) and needs
    no cloud KMS credentials Secret or workload-identity annotation;
  * production refuses keyfile and requires one production KMS adapter:
    - aws-kms (EKS IRSA) and gcp-kms (GKE Workload Identity) have no static
      credential, so kodema-kms-credentials is not required and the Deployment
      renders no reference to it; instead the chart-created ServiceAccount must
      carry eks.amazonaws.com/role-arn or iam.gke.io/gcp-service-account;
    - every other adapter (scw-kms, vault-transit) requires the
      kodema-kms-credentials Secret.

The schema states the same requirements (allOf/if/then) so a renderer that
skips schema validation is caught here as well.
*/ -}}
{{- $extraEnv := dict }}
{{- if .Values.config }}
{{- if .Values.config.extraEnv }}
{{- $extraEnv = .Values.config.extraEnv }}
{{- end }}
{{- end }}
{{- $required := list "KODEMA_OIDC_ISSUER" "KODEMA_OIDC_AUDIENCE" "KODEMA_OIDC_JWKS_URL" "KODEMA_OIDC_TENANT_CLAIM" }}
{{- $missing := list }}
{{- range $name := $required }}
{{- if not (hasKey $extraEnv $name) }}
{{- $missing = append $missing $name }}
{{- end }}
{{- end }}
{{- if $missing }}
{{- fail (printf "%s requires config.extraEnv.%s: the server refuses to start without the OIDC issuer, audience, JWKS URL and tenant claim; set them in the operator extra values (see deploy/README.md)" .Values.environment (join ", config.extraEnv." $missing)) }}
{{- end }}
{{- $adapter := "" }}
{{- if .Values.config }}
{{- $adapter = toString (.Values.config.keyAdapter | default "") }}
{{- end }}
{{- if eq .Values.environment "test" }}
{{- if ne $adapter "keyfile" }}
{{- fail (printf "environment \"test\" requires config.keyAdapter=\"keyfile\" (got %q): --profile test runs with the Kubernetes runtime and local keyfile" $adapter) }}
{{- end }}
{{- else if eq .Values.environment "production" }}
{{- if eq $adapter "keyfile" }}
{{- fail "production refuses config.keyAdapter=\"keyfile\": use scw-kms, aws-kms, gcp-kms or vault-transit" }}
{{- end }}
{{- if has $adapter (list "aws-kms" "gcp-kms") }}
{{- $annotation := ternary "eks.amazonaws.com/role-arn" "iam.gke.io/gcp-service-account" (eq $adapter "aws-kms") }}
{{- $identity := ternary "the EKS IRSA role ARN" "the Workload Identity Google service account" (eq $adapter "aws-kms") }}
{{- $create := true }}
{{- $annotations := dict }}
{{- if .Values.serviceAccount }}
{{- if hasKey .Values.serviceAccount "create" }}
{{- $create = .Values.serviceAccount.create }}
{{- end }}
{{- if .Values.serviceAccount.annotations }}
{{- $annotations = .Values.serviceAccount.annotations }}
{{- end }}
{{- end }}
{{- if and $create (not (get $annotations $annotation)) }}
{{- fail (printf "production requires serviceAccount.annotations[%q] (%s) for config.keyAdapter %s: the adapter has no static credential, so without the annotation the pod starts and every KMS decrypt is refused; set serviceAccount.create=false only when the ServiceAccount is managed outside this chart" $annotation $identity $adapter) }}
{{- end }}
{{- else }}
{{- $kmsCredentialsRequired := printf "production requires secrets.kmsCredentials.existingSecret (the kodema-kms-credentials Secret) for config.keyAdapter %q: scw-kms reads SCW_SECRET_KEY and vault-transit reads VAULT_ADDR/VAULT_TOKEN from it, and only aws-kms and gcp-kms run without a static credential" $adapter }}
{{- if not .Values.secrets }}
{{- fail $kmsCredentialsRequired }}
{{- end }}
{{- if not .Values.secrets.kmsCredentials }}
{{- fail $kmsCredentialsRequired }}
{{- end }}
{{- if not .Values.secrets.kmsCredentials.existingSecret }}
{{- fail $kmsCredentialsRequired }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
