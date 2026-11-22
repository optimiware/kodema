#!/usr/bin/env bash
# down.sh: uninstall a local k3s Kodema installation created by deploy/k3s/install.sh.
#
# Without --purge: uninstalls the Helm release and removes the migrate Job and
# optional in-cluster PostgreSQL StatefulSet/Service while keeping PVCs, Secrets,
# and local PKI/keys in KODEMA_K3S_STATE_DIR.
#
# With --purge: also removes PVCs, Secrets, the tenant namespace, and the
# marker-verified local state directory.
set -euo pipefail

LC_ALL=C
export LC_ALL

log() {
	printf '[kodema-k3s] %s\n' "$*" >&2
}

die() {
	printf '[kodema-k3s] ERROR: %s\n' "$*" >&2
	exit 1
}

usage() {
	cat >&2 <<'USAGE'
Usage:
  deploy/k3s/down.sh [--purge]

Options:
  --purge     Also delete PVCs, Secrets, tenant namespace, and local STATE_DIR
  -h, --help  Show this help message

Environment:
  KODEMA_K3S_NAMESPACE   Release namespace   (kodema-system)
  KODEMA_K3S_RELEASE     Helm release name   (kodema)
  KODEMA_K3S_TENANT      Tenant id           (tenant-a)
  KODEMA_K3S_STATE_DIR   Local state dir     (~/.local/state/kodema/k3s/<namespace>)
  KODEMA_K3S_KUBECONFIG  Kubeconfig file     ($KUBECONFIG or /etc/rancher/k3s/k3s.yaml)
USAGE
}

PURGE=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--purge)
		PURGE=1
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

NAMESPACE="${KODEMA_K3S_NAMESPACE:-kodema-system}"
RELEASE="${KODEMA_K3S_RELEASE:-kodema}"
TENANT="${KODEMA_K3S_TENANT:-tenant-a}"
STATE_ROOT="${XDG_STATE_HOME:-${HOME}/.local/state}/kodema/k3s"
STATE_DIR="${KODEMA_K3S_STATE_DIR:-${STATE_ROOT}/${NAMESPACE}}"
TENANT_NAMESPACE="kodema-tenant-${TENANT}"

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

if command -v helm >/dev/null 2>&1; then
	if helm_cmd status "$RELEASE" -n "$NAMESPACE" >/dev/null 2>&1; then
		log "uninstalling Helm release ${RELEASE} from ${NAMESPACE}"
		helm_cmd uninstall "$RELEASE" -n "$NAMESPACE"
	fi
fi

if command -v kubectl >/dev/null 2>&1; then
	log "removing kodema-migrate Job and in-cluster PostgreSQL workload (if present)"
	kctl delete job kodema-migrate -n "$NAMESPACE" --ignore-not-found >/dev/null
	kctl delete statefulset kodema-postgres -n "$NAMESPACE" --ignore-not-found >/dev/null
	kctl delete service kodema-postgres -n "$NAMESPACE" --ignore-not-found >/dev/null

	if [[ "$PURGE" -eq 1 ]]; then
		log "purging Secrets, PVCs, and tenant namespace ${TENANT_NAMESPACE}"
		kctl delete secret -n "$NAMESPACE" --ignore-not-found \
			kodema-tls kodema-broker-tls kodema-broker kodema-nats-auth \
			kodema-keys kodema-database kodema-postgres-tls \
			kodema-postgres-superuser oauthsonas-tls oauthsonas-access >/dev/null
		kctl delete pvc -n "$NAMESPACE" --ignore-not-found \
			kodema-retained-logs data-kodema-postgres-0 data-nats-0 >/dev/null
		kctl delete namespace "$TENANT_NAMESPACE" --ignore-not-found >/dev/null
	fi
fi

if [[ "$PURGE" -eq 1 && -d "$STATE_DIR" ]]; then
	if [[ -f "${STATE_DIR}/.kodema-k3s" ]]; then
		log "removing local state directory ${STATE_DIR}"
		rm -rf -- "$STATE_DIR"
	else
		die "refusing to remove ${STATE_DIR}: missing .kodema-k3s ownership marker"
	fi
fi

log "done"
