#!/usr/bin/env bash
# install.sh: cloud-free bootstrap for Kodema on single-node k3s (or kind / any
# local Kubernetes cluster) using `kodema-stack` with `environment: test`,
# Kubernetes `batch/v1` Jobs, a local keyfile, mTLS NATS JetStream, OAuthSonas,
# and optional in-cluster TLS PostgreSQL (`sslmode=verify-full`).
#
# Re-runnable: existing CA, KEK keyfile, NATS NKey, and database passwords in
# KODEMA_K3S_STATE_DIR are preserved across runs. Secret values travel via 0600
# files (--from-file), never on argv or printed to stdout.
set -euo pipefail

LC_ALL=C
export LC_ALL

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

log() {
	printf '[kodema-k3s] %s\n' "$*" >&2
}

die() {
	printf '[kodema-k3s] ERROR: %s\n' "$*" >&2
	exit 1
}

require_cmd() {
	local cmd
	for cmd in "$@"; do
		command -v "$cmd" >/dev/null 2>&1 || die "required command not found in PATH: ${cmd}"
	done
}

usage() {
	cat >&2 <<'USAGE'
Usage:
  deploy/k3s/install.sh [options]

Options:
  --dry-run        Generate local PKI, keys, and rendered values YAML in STATE_DIR
                   and validate with `helm template` without contacting Kubernetes
  --skip-postgres  Do not deploy the optional in-cluster TLS PostgreSQL StatefulSet
                   (assumes kodema-database Secret is supplied externally)
  --skip-migrate   Skip the one-shot `kodema migrate` Job
  --skip-helm      Prepare Secrets, PostgreSQL, and migrations only
  --rotate-pki     Regenerate local CA and leaf TLS certificates
  -h, --help       Show this help message

Environment (all optional, safe defaults):
  KODEMA_K3S_NAMESPACE         Release namespace              (kodema-system)
  KODEMA_K3S_RELEASE           Helm release name              (kodema)
  KODEMA_K3S_TENANT            Tenant id (lowercase DNS label)(tenant-a)
  KODEMA_K3S_INSTALLATION_ID   Installation id                (kodema-k3s)
  KODEMA_K3S_STATE_DIR         Local state directory          (~/.local/state/kodema/k3s/<namespace>)
  KODEMA_K3S_KUBECONFIG        Kubeconfig file                ($KUBECONFIG or /etc/rancher/k3s/k3s.yaml)
  KODEMA_K3S_IMAGE_REPOSITORY  Kodema container image repo    (ghcr.io/optimiware/kodema/kodema)
  KODEMA_K3S_IMAGE_TAG         Kodema image tag               (contents of VERSION)
  KODEMA_K3S_IMAGE_DIGEST      Optional sha256:<64 hex> pin   ("")
  KODEMA_K3S_STORAGE_CLASS     PVC StorageClass override      ("" = cluster default, e.g. local-path)
  KODEMA_K3S_POSTGRES_IMAGE    Pinned PostgreSQL image        (postgres:18-alpine@sha256:...)
  KODEMA_K3S_POSTGRES_ENABLED  Deploy in-cluster TLS Postgres (1)
USAGE
}

DRY_RUN=0
SKIP_POSTGRES=0
SKIP_MIGRATE=0
SKIP_HELM=0
ROTATE_PKI=0

while [[ $# -gt 0 ]]; do
	case "$1" in
	--dry-run)
		DRY_RUN=1
		shift
		;;
	--skip-postgres)
		SKIP_POSTGRES=1
		shift
		;;
	--skip-migrate)
		SKIP_MIGRATE=1
		shift
		;;
	--skip-helm)
		SKIP_HELM=1
		shift
		;;
	--rotate-pki)
		ROTATE_PKI=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage
		die "unknown option: $1"
		;;
	esac
done

DEFAULT_VERSION="$(tr -d '[:space:]' <"${REPO_ROOT}/VERSION")"
NAMESPACE="${KODEMA_K3S_NAMESPACE:-kodema-system}"
RELEASE="${KODEMA_K3S_RELEASE:-kodema}"
TENANT="${KODEMA_K3S_TENANT:-tenant-a}"
INSTALLATION_ID="${KODEMA_K3S_INSTALLATION_ID:-kodema-k3s}"
STATE_ROOT="${XDG_STATE_HOME:-${HOME}/.local/state}/kodema/k3s"
STATE_DIR="${KODEMA_K3S_STATE_DIR:-${STATE_ROOT}/${NAMESPACE}}"
IMAGE_REPO="${KODEMA_K3S_IMAGE_REPOSITORY:-ghcr.io/optimiware/kodema/kodema}"
IMAGE_TAG="${KODEMA_K3S_IMAGE_TAG:-${DEFAULT_VERSION}}"
IMAGE_DIGEST="${KODEMA_K3S_IMAGE_DIGEST:-}"
STORAGE_CLASS="${KODEMA_K3S_STORAGE_CLASS:-}"
POSTGRES_IMAGE="${KODEMA_K3S_POSTGRES_IMAGE:-docker.io/library/postgres:18-alpine@sha256:300ebd02c6d4de9ee27ad7f6855d5d21b4d3a4a5117767c1197d284c635d9736}"
POSTGRES_ENABLED="${KODEMA_K3S_POSTGRES_ENABLED:-1}"
if [[ "$SKIP_POSTGRES" -eq 1 ]]; then
	POSTGRES_ENABLED=0
fi

# Validate DNS labels and digest before writing any manifest.
[[ "$NAMESPACE" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] ||
	die "invalid KODEMA_K3S_NAMESPACE '${NAMESPACE}': lowercase DNS label required"
[[ "$RELEASE" =~ ^[a-z0-9]([a-z0-9-]{0,51}[a-z0-9])?$ ]] ||
	die "invalid KODEMA_K3S_RELEASE '${RELEASE}': lowercase DNS label required"
[[ "$TENANT" =~ ^[a-z0-9]([a-z0-9-]{0,47}[a-z0-9])?$ ]] ||
	die "invalid KODEMA_K3S_TENANT '${TENANT}': lowercase DNS label (1-49 chars) required"
if [[ -n "$IMAGE_DIGEST" && ! "$IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then
	die "invalid KODEMA_K3S_IMAGE_DIGEST '${IMAGE_DIGEST}': want sha256:<64 lowercase hex>"
fi
if [[ ! "$POSTGRES_IMAGE" =~ ^[^[:space:]@]+(:[A-Za-z0-9._-]+)?@sha256:[a-f0-9]{64}$ ]]; then
	die "KODEMA_K3S_POSTGRES_IMAGE must be pinned by digest: repository[:tag]@sha256:<64 hex>"
fi

TENANT_NAMESPACE="kodema-tenant-${TENANT}"
KODEMA_HOST="kodema.${NAMESPACE}.svc.cluster.local"
NATS_HOST="nats.${NAMESPACE}.svc.cluster.local"
POSTGRES_HOST="kodema-postgres.${NAMESPACE}.svc.cluster.local"
OAUTH_HOST="oauthsonas.${NAMESPACE}.svc.cluster.local"

KODEMA_IMAGE_REF="${IMAGE_REPO}:${IMAGE_TAG}"
if [[ -n "$IMAGE_DIGEST" ]]; then
	KODEMA_IMAGE_REF="${IMAGE_REPO}@${IMAGE_DIGEST}"
fi

# Resolve kubeconfig when running against a live cluster.
KUBECONFIG_ARGS=()
if [[ -n "${KODEMA_K3S_KUBECONFIG:-}" ]]; then
	KUBECONFIG_ARGS=(--kubeconfig "${KODEMA_K3S_KUBECONFIG}")
elif [[ -z "${KUBECONFIG:-}" && -r /etc/rancher/k3s/k3s.yaml ]]; then
	KUBECONFIG_ARGS=(--kubeconfig /etc/rancher/k3s/k3s.yaml)
fi

kctl() {
	kubectl "${KUBECONFIG_ARGS[@]}" "$@"
}

helm_cmd() {
	helm "${KUBECONFIG_ARGS[@]}" "$@"
}

require_cmd openssl helm go
if [[ "$DRY_RUN" -eq 0 ]]; then
	require_cmd kubectl
fi

# ---- state directory -------------------------------------------------------

umask 077
mkdir -p "${STATE_DIR}/pki" "${STATE_DIR}/secrets" "${STATE_DIR}/nats-auth"
chmod 700 "${STATE_DIR}" "${STATE_DIR}/pki" "${STATE_DIR}/secrets" "${STATE_DIR}/nats-auth"
if [[ ! -s "${STATE_DIR}/.kodema-k3s" ]]; then
	printf 'namespace=%s\nrelease=%s\ntenant=%s\n' "$NAMESPACE" "$RELEASE" "$TENANT" >"${STATE_DIR}/.kodema-k3s"
	chmod 600 "${STATE_DIR}/.kodema-k3s"
fi

PKI_DIR="${STATE_DIR}/pki"
SECRETS_DIR="${STATE_DIR}/secrets"
NATS_AUTH_DIR="${STATE_DIR}/nats-auth"

# ---- local PKI -------------------------------------------------------------

if [[ "$ROTATE_PKI" -eq 1 || ! -s "${PKI_DIR}/ca.crt" || ! -s "${PKI_DIR}/ca.key" ]]; then
	log "creating local installation CA in ${PKI_DIR}"
	openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
		-keyout "${PKI_DIR}/ca.key" -out "${PKI_DIR}/ca.crt" -days 825 \
		-subj "/CN=Kodema k3s local CA" \
		-addext "basicConstraints=critical,CA:TRUE" \
		-addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1
	chmod 600 "${PKI_DIR}/ca.key"
	chmod 644 "${PKI_DIR}/ca.crt"
fi

issue_cert() {
	local name=$1 cn=$2 san=$3 eku=$4
	local key="${PKI_DIR}/${name}.key" crt="${PKI_DIR}/${name}.crt" csr="${PKI_DIR}/${name}.csr" ext="${PKI_DIR}/${name}.ext"
	if [[ "$ROTATE_PKI" -eq 0 && -s "$crt" && -s "$key" ]] && openssl verify -CAfile "${PKI_DIR}/ca.crt" "$crt" >/dev/null 2>&1; then
		return 0
	fi
	log "issuing ${name} certificate (CN=${cn})"
	cat >"$ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=${eku}
subjectAltName=${san}
EOF
	chmod 600 "$ext"
	openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
		-keyout "$key" -out "$csr" -subj "/CN=${cn}" >/dev/null 2>&1
	openssl x509 -req -in "$csr" -CA "${PKI_DIR}/ca.crt" -CAkey "${PKI_DIR}/ca.key" \
		-CAcreateserial -out "$crt" -days 825 -sha256 -extfile "$ext" >/dev/null 2>&1
	rm -f -- "$csr" "$ext"
	chmod 600 "$key"
	chmod 644 "$crt"
}

issue_cert kodema "$KODEMA_HOST" \
	"DNS:${KODEMA_HOST},DNS:kodema.${NAMESPACE}.svc,DNS:kodema,DNS:localhost,IP:127.0.0.1" \
	"serverAuth"
issue_cert nats-server "$NATS_HOST" \
	"DNS:${NATS_HOST},DNS:nats.${NAMESPACE}.svc,DNS:nats,DNS:nats-0.nats.${NAMESPACE}.svc.cluster.local" \
	"serverAuth"
issue_cert nats-client "kodema-control-plane" \
	"DNS:${KODEMA_HOST}" \
	"clientAuth"
issue_cert postgres "$POSTGRES_HOST" \
	"DNS:${POSTGRES_HOST},DNS:kodema-postgres.${NAMESPACE}.svc,DNS:kodema-postgres" \
	"serverAuth"
issue_cert oauthsonas "$OAUTH_HOST" \
	"DNS:${OAUTH_HOST},DNS:oauthsonas.${NAMESPACE}.svc,DNS:oauthsonas,DNS:localhost,IP:127.0.0.1" \
	"serverAuth"

# ---- secrets & key material ------------------------------------------------

ensure_hex_secret() {
	local file=$1 bytes=$2
	if [[ ! -s "$file" ]]; then
		openssl rand -hex "$bytes" | tr -d '\n' >"$file"
		chmod 600 "$file"
	fi
}

ensure_hex_secret "${SECRETS_DIR}/postgres-superuser-password" 24
ensure_hex_secret "${SECRETS_DIR}/kodema-admin-password" 24
ensure_hex_secret "${SECRETS_DIR}/runtime-password" 24
ensure_hex_secret "${SECRETS_DIR}/controller-password" 24
ensure_hex_secret "${SECRETS_DIR}/publisher-password" 24
ensure_hex_secret "${SECRETS_DIR}/auth-password" 24
ensure_hex_secret "${SECRETS_DIR}/oauthsonas-access-code" 16
ensure_hex_secret "${NATS_AUTH_DIR}/shell-password" 24
ensure_hex_secret "${NATS_AUTH_DIR}/callout-password" 24

if [[ ! -s "${NATS_AUTH_DIR}/shell-user" ]]; then
	printf 'kodema-shell' >"${NATS_AUTH_DIR}/shell-user"
	chmod 600 "${NATS_AUTH_DIR}/shell-user"
fi
if [[ ! -s "${NATS_AUTH_DIR}/callout-user" ]]; then
	printf 'kodema-callout' >"${NATS_AUTH_DIR}/callout-user"
	chmod 600 "${NATS_AUTH_DIR}/callout-user"
fi
if [[ ! -s "${NATS_AUTH_DIR}/issuer.seed" || ! -s "${NATS_AUTH_DIR}/issuer.pub" ]]; then
	log "generating Ed25519 NATS Account NKey pair via tools/nats-auth-key"
	(cd "$REPO_ROOT" && go run ./tools/nats-auth-key \
		-seed-file "${NATS_AUTH_DIR}/issuer.seed" \
		-public-file "${NATS_AUTH_DIR}/issuer.pub")
	chmod 600 "${NATS_AUTH_DIR}/issuer.seed" "${NATS_AUTH_DIR}/issuer.pub"
fi

if [[ ! -s "${SECRETS_DIR}/kek-v1.key" ]]; then
	log "generating 32-byte keyfile KEK (${SECRETS_DIR}/kek-v1.key)"
	openssl rand 32 >"${SECRETS_DIR}/kek-v1.key"
	chmod 600 "${SECRETS_DIR}/kek-v1.key"
fi

# Build PostgreSQL DSN files with sslmode=verify-full.
PG_TLS_QUERY="sslmode=verify-full&sslrootcert=/etc/kodema/pg-ca/ca.crt"
printf 'postgres://kodema_runtime:%s@%s:5432/kodema?%s' "$(cat "${SECRETS_DIR}/runtime-password")" "$POSTGRES_HOST" "$PG_TLS_QUERY" >"${SECRETS_DIR}/database_url"
printf 'postgres://kodema_controller:%s@%s:5432/kodema?%s' "$(cat "${SECRETS_DIR}/controller-password")" "$POSTGRES_HOST" "$PG_TLS_QUERY" >"${SECRETS_DIR}/controller_url"
printf 'postgres://kodema_publisher:%s@%s:5432/kodema?%s' "$(cat "${SECRETS_DIR}/publisher-password")" "$POSTGRES_HOST" "$PG_TLS_QUERY" >"${SECRETS_DIR}/publisher_url"
printf 'postgres://kodema_auth:%s@%s:5432/kodema?%s' "$(cat "${SECRETS_DIR}/auth-password")" "$POSTGRES_HOST" "$PG_TLS_QUERY" >"${SECRETS_DIR}/auth_url"
printf 'postgres://kodema_admin:%s@%s:5432/postgres?%s' "$(cat "${SECRETS_DIR}/kodema-admin-password")" "$POSTGRES_HOST" "$PG_TLS_QUERY" >"${SECRETS_DIR}/admin_url"
printf 'tls://%s:4222' "$NATS_HOST" >"${SECRETS_DIR}/nats_url"
chmod 600 "${SECRETS_DIR}/database_url" "${SECRETS_DIR}/controller_url" "${SECRETS_DIR}/publisher_url" "${SECRETS_DIR}/auth_url" "${SECRETS_DIR}/admin_url" "${SECRETS_DIR}/nats_url"

# ---- render Helm values ----------------------------------------------------

RENDERED_VALUES="${STATE_DIR}/values.yaml"
cat >"$RENDERED_VALUES" <<EOF
nats:
  enabled: true
  replicaCount: 1
  jetstream:
    storageClass: "${STORAGE_CLASS}"
    size: "2Gi"

kodema:
  environment: "test"
  replicaCount: 1
  image:
    repository: "${IMAGE_REPO}"
    tag: "${IMAGE_TAG}"
    digest: "${IMAGE_DIGEST}"
    pullPolicy: IfNotPresent
  config:
    installationID: "${INSTALLATION_ID}"
    publicURL: "https://${KODEMA_HOST}:8443"
    natsURL: "tls://${NATS_HOST}:4222"
    keyAdapter: "keyfile"
    keyReference: "keyfile:kek-v1"
    keyCurrentVersion: 1
    mcpPath: "/mcp"
    workspacePVC:
      storageClass: "${STORAGE_CLASS}"
      size: "2Gi"
      accessMode: "ReadWriteOnce"
    workloadCA:
      secretName: "kodema-workload-ca"
    extraEnv:
      KODEMA_OIDC_ISSUER: "https://${OAUTH_HOST}:8443"
      KODEMA_OIDC_AUDIENCE: "https://${KODEMA_HOST}:8443"
      KODEMA_OIDC_JWKS_URL: "https://${OAUTH_HOST}:8443/.well-known/jwks.json"
      KODEMA_OIDC_TENANT_CLAIM: "org_id"
      KODEMA_OIDC_CA_FILE: "/etc/kodema/tls/ca.crt"
  secrets:
    database:
      existingSecret: kodema-database
    broker:
      existingSecret: kodema-broker
    keys:
      existingSecret: kodema-keys
    tls:
      existingSecret: kodema-tls
    natsAuth:
      existingSecret: "kodema-nats-auth"
      account: "KODEMA"
  tenants:
    - "${TENANT}"
  networkPolicy:
    defaultDeny: true
    permitKodema: true
    permitGit: true
    egressFQDNs: []
  persistence:
    logs:
      enabled: true
      storageClass: "${STORAGE_CLASS}"
      size: "2Gi"
      accessMode: ReadWriteOnce
      mountPath: /var/lib/kodema

oauthsonas:
  enabled: true
  host: "${OAUTH_HOST}"
  port: 8443
  tlsSecret: oauthsonas-tls
  accessCodeSecret: oauthsonas-access
  accessCodeKey: access_code
  config: |
    issuer: https://${OAUTH_HOST}:8443
    api_audience: https://${KODEMA_HOST}:8443
    clients:
      - id: kodema-cli
        name: Kodema CLI
        public: true
        redirect_uris:
          - http://127.0.0.1/callback
    personas:
      - id: admin
        subject: admin
        email: admin@localhost.test
        name: Local Administrator
        organization_id: ${TENANT}
        roles: [admin]
      - id: engineer
        subject: engineer
        email: engineer@localhost.test
        name: Local Engineer
        organization_id: ${TENANT}
        roles: [engineer]
      - id: reviewer
        subject: reviewer
        email: reviewer@localhost.test
        name: Local Reviewer
        organization_id: ${TENANT}
        roles: [reviewer]
      - id: operator
        subject: operator
        email: operator@localhost.test
        name: Local Operator
        organization_id: ${TENANT}
        roles: [operator]
EOF
chmod 600 "$RENDERED_VALUES"

STAGED_CHARTS="${STATE_DIR}/charts"
rm -rf -- "$STAGED_CHARTS"
mkdir -p -- "$STAGED_CHARTS"
cp -R "${REPO_ROOT}/deploy/charts/kodema" "${STAGED_CHARTS}/kodema"
cp -R "${REPO_ROOT}/deploy/charts/kodema-stack" "${STAGED_CHARTS}/kodema-stack"
rm -rf -- "${STAGED_CHARTS}/kodema-stack/charts" "${STAGED_CHARTS}/kodema-stack/Chart.lock"
STACK_CHART="${STAGED_CHARTS}/kodema-stack"
log "building staged kodema-stack Helm chart dependencies"
helm dependency build "$STACK_CHART" >/dev/null

if [[ "$DRY_RUN" -eq 1 ]]; then
	log "dry-run: validating rendered values with helm template"
	helm template "$RELEASE" "$STACK_CHART" --namespace "$NAMESPACE" -f "$RENDERED_VALUES" >/dev/null
	log "dry-run succeeded; generated assets live in ${STATE_DIR}"
	exit 0
fi

# ---- namespaces and Secrets ------------------------------------------------

log "ensuring namespaces ${NAMESPACE} and ${TENANT_NAMESPACE}"
kctl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
---
apiVersion: v1
kind: Namespace
metadata:
  name: ${TENANT_NAMESPACE}
  labels:
    kodema.dev/tenant: ${TENANT}
    pod-security.kubernetes.io/enforce: baseline
    pod-security.kubernetes.io/warn: restricted
    pod-security.kubernetes.io/audit: restricted
EOF

apply_secret() {
	local ns=$1 name=$2
	shift 2
	kctl create secret generic "$name" -n "$ns" "$@" --dry-run=client -o yaml | kctl apply -f - >/dev/null
}

log "reconciling Kubernetes Secrets in ${NAMESPACE} and ${TENANT_NAMESPACE}"
apply_secret "$NAMESPACE" kodema-tls \
	--from-file="tls.crt=${PKI_DIR}/kodema.crt" \
	--from-file="tls.key=${PKI_DIR}/kodema.key" \
	--from-file="ca.crt=${PKI_DIR}/ca.crt"

apply_secret "$NAMESPACE" kodema-broker-tls \
	--from-file="tls.crt=${PKI_DIR}/nats-server.crt" \
	--from-file="tls.key=${PKI_DIR}/nats-server.key" \
	--from-file="ca.crt=${PKI_DIR}/ca.crt"

apply_secret "$NAMESPACE" kodema-broker \
	--from-file="nats_url=${SECRETS_DIR}/nats_url" \
	--from-file="ca.crt=${PKI_DIR}/ca.crt" \
	--from-file="tls.crt=${PKI_DIR}/nats-client.crt" \
	--from-file="tls.key=${PKI_DIR}/nats-client.key"

apply_secret "$NAMESPACE" kodema-nats-auth \
	--from-file="issuer.seed=${NATS_AUTH_DIR}/issuer.seed" \
	--from-file="issuer.pub=${NATS_AUTH_DIR}/issuer.pub" \
	--from-file="shell-user=${NATS_AUTH_DIR}/shell-user" \
	--from-file="shell-password=${NATS_AUTH_DIR}/shell-password" \
	--from-file="callout-user=${NATS_AUTH_DIR}/callout-user" \
	--from-file="callout-password=${NATS_AUTH_DIR}/callout-password"

apply_secret "$NAMESPACE" kodema-keys \
	--from-file="kek-v1.key=${SECRETS_DIR}/kek-v1.key"

apply_secret "$NAMESPACE" oauthsonas-tls \
	--from-file="tls.crt=${PKI_DIR}/oauthsonas.crt" \
	--from-file="tls.key=${PKI_DIR}/oauthsonas.key" \
	--from-file="ca.crt=${PKI_DIR}/ca.crt"

apply_secret "$NAMESPACE" oauthsonas-access \
	--from-file="access_code=${SECRETS_DIR}/oauthsonas-access-code"

apply_secret "$TENANT_NAMESPACE" kodema-workload-ca \
	--from-file="ca.crt=${PKI_DIR}/ca.crt"

if [[ "$POSTGRES_ENABLED" -eq 1 ]]; then
	apply_secret "$NAMESPACE" kodema-database \
		--from-file="database_url=${SECRETS_DIR}/database_url" \
		--from-file="controller_url=${SECRETS_DIR}/controller_url" \
		--from-file="publisher_url=${SECRETS_DIR}/publisher_url" \
		--from-file="auth_url=${SECRETS_DIR}/auth_url" \
		--from-file="admin_url=${SECRETS_DIR}/admin_url" \
		--from-file="ca.crt=${PKI_DIR}/ca.crt"

	apply_secret "$NAMESPACE" kodema-postgres-tls \
		--from-file="tls.crt=${PKI_DIR}/postgres.crt" \
		--from-file="tls.key=${PKI_DIR}/postgres.key" \
		--from-file="ca.crt=${PKI_DIR}/ca.crt"

	apply_secret "$NAMESPACE" kodema-postgres-superuser \
		--from-file="POSTGRES_PASSWORD=${SECRETS_DIR}/postgres-superuser-password"

	# ---- optional in-cluster TLS PostgreSQL --------------------------------
	log "deploying in-cluster TLS PostgreSQL in ${NAMESPACE}"
	STORAGE_CLASS_YAML=""
	if [[ -n "$STORAGE_CLASS" ]]; then
		STORAGE_CLASS_YAML="        storageClassName: ${STORAGE_CLASS}"
	fi
	kctl apply -n "$NAMESPACE" -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: kodema-postgres
  labels:
    app.kubernetes.io/name: kodema-postgres
    app.kubernetes.io/part-of: kodema-k3s
spec:
  clusterIP: None
  selector:
    app.kubernetes.io/name: kodema-postgres
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kodema-postgres
  labels:
    app.kubernetes.io/name: kodema-postgres
    app.kubernetes.io/part-of: kodema-k3s
spec:
  serviceName: kodema-postgres
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: kodema-postgres
  template:
    metadata:
      labels:
        app.kubernetes.io/name: kodema-postgres
        app.kubernetes.io/part-of: kodema-k3s
    spec:
      securityContext:
        fsGroup: 70
      initContainers:
        - name: prepare-tls
          image: ${POSTGRES_IMAGE}
          command:
            - /bin/sh
            - -c
            - |
              set -eu
              cp /tls-src/tls.crt /tls/server.crt
              cp /tls-src/tls.key /tls/server.key
              cp /tls-src/ca.crt /tls/ca.crt
              chmod 0644 /tls/server.crt /tls/ca.crt
              chmod 0600 /tls/server.key
          securityContext:
            runAsUser: 70
            runAsGroup: 70
          volumeMounts:
            - name: tls-src
              mountPath: /tls-src
              readOnly: true
            - name: tls
              mountPath: /tls
      containers:
        - name: postgres
          image: ${POSTGRES_IMAGE}
          args:
            - -c
            - ssl=on
            - -c
            - ssl_cert_file=/tls/server.crt
            - -c
            - ssl_key_file=/tls/server.key
            - -c
            - ssl_ca_file=/tls/ca.crt
          envFrom:
            - secretRef:
                name: kodema-postgres-superuser
          ports:
            - name: postgres
              containerPort: 5432
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres", "-d", "postgres"]
            initialDelaySeconds: 3
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql
            - name: tls
              mountPath: /tls
              readOnly: true
      volumes:
        - name: tls-src
          secret:
            secretName: kodema-postgres-tls
        - name: tls
          emptyDir:
            medium: Memory
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
${STORAGE_CLASS_YAML}
        resources:
          requests:
            storage: 5Gi
EOF

	log "waiting for kodema-postgres StatefulSet rollout"
	kctl rollout status statefulset/kodema-postgres -n "$NAMESPACE" --timeout=180s

	log "ensuring non-superuser kodema_admin role exists in PostgreSQL"
	admin_pw="$(cat "${SECRETS_DIR}/kodema-admin-password")"
	kctl exec -i -n "$NAMESPACE" statefulset/kodema-postgres -- \
		psql -v ON_ERROR_STOP=1 -U postgres -d postgres >/dev/null <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kodema_admin') THEN
    CREATE ROLE kodema_admin LOGIN CREATEROLE CREATEDB NOSUPERUSER NOBYPASSRLS;
  END IF;
END
\$\$;
ALTER ROLE kodema_admin WITH PASSWORD '${admin_pw}';
SQL
	unset admin_pw
fi

# ---- kodema migrate Job ----------------------------------------------------

if [[ "$SKIP_MIGRATE" -eq 0 ]]; then
	log "running one-shot kodema migrate Job in ${NAMESPACE}"
	kctl delete job kodema-migrate -n "$NAMESPACE" --ignore-not-found >/dev/null
	kctl apply -n "$NAMESPACE" -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: kodema-migrate
  labels:
    app.kubernetes.io/name: kodema-migrate
    app.kubernetes.io/part-of: kodema-k3s
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        fsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: migrate
          image: "${KODEMA_IMAGE_REF}"
          imagePullPolicy: IfNotPresent
          args: ["migrate"]
          env:
            - name: KODEMA_ADMIN_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kodema-database
                  key: admin_url
            - name: KODEMA_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kodema-database
                  key: database_url
            - name: KODEMA_CONTROLLER_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kodema-database
                  key: controller_url
            - name: KODEMA_PUBLISHER_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kodema-database
                  key: publisher_url
            - name: KODEMA_AUTH_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: kodema-database
                  key: auth_url
          securityContext:
            runAsNonRoot: true
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            seccompProfile:
              type: RuntimeDefault
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: database-ca
              mountPath: /etc/kodema/pg-ca
              readOnly: true
      volumes:
        - name: database-ca
          secret:
            secretName: kodema-database
            items:
              - key: ca.crt
                path: ca.crt
EOF
	if ! kctl wait --for=condition=complete job/kodema-migrate -n "$NAMESPACE" --timeout=180s; then
		kctl logs job/kodema-migrate -n "$NAMESPACE" >&2 || true
		die "kodema migrate Job failed"
	fi
fi

# ---- Helm install / upgrade ------------------------------------------------

if [[ "$SKIP_HELM" -eq 0 ]]; then
	log "installing Helm release ${RELEASE} in ${NAMESPACE}"
	helm_cmd upgrade --install "$RELEASE" "$STACK_CHART" \
		--namespace "$NAMESPACE" \
		-f "$RENDERED_VALUES" \
		--wait --timeout 5m

	log "waiting for kodema Deployment rollout"
	kctl rollout status deployment/kodema -n "$NAMESPACE" --timeout=180s
fi

log "k3s installation ready"
log "  namespace:        ${NAMESPACE}"
log "  tenant namespace: ${TENANT_NAMESPACE}"
log "  local CA bundle:  ${PKI_DIR}/ca.crt"
log "  rendered values:  ${RENDERED_VALUES}"
log "  next steps:       see deploy/k3s/README.md (bootstrap-admin + pipeline quickstart)"
