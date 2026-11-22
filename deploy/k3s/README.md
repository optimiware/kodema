# Kodema on k3s / Local Kubernetes (Cloud-Free)

This directory provides a self-contained, cloud-free bootstrap flow for running the full `kodema-stack` Helm chart on a single-node **k3s** (or **kind**) cluster with **zero cloud provider credentials**:

- **`values.yaml`** — reference `kodema-stack` Helm values using `environment: "test"`, `KODEMA_RUNTIME=kubernetes`, a local 32-byte `keyfile` staged into memory (`kek-v1.key`), cluster-default StorageClass (`storageClass: ""`, e.g. `local-path` on k3s), mTLS NATS JetStream with Auth Callout, and in-cluster OAuthSonas.
- **`install.sh`** — idempotent bootstrap script that generates local PKI, Ed25519 NATS Account NKey, `kek-v1.key`, all required Kubernetes Secrets, an optional in-cluster TLS PostgreSQL 16/18 `StatefulSet` (`sslmode=verify-full`), a one-shot `kodema migrate` Job, and installs `deploy/charts/kodema-stack`.
- **`down.sh`** — clean teardown (`--purge` also removes PVCs, Secrets, the tenant namespace, and the local state directory).

## Networking prerequisites on k3s

1. **Default k3s (`flannel` + `kube-router` NetworkPolicy controller):**
   Ensure the host kernel enforces bridge netfilter rules so `kube-router` `NetworkPolicy` isolation works for tenant namespaces:
   ```bash
   sudo modprobe br_netfilter
   sudo sysctl -w net.bridge.bridge-nf-call-iptables=1
   ```
   Leave `kodema.networkPolicy.egressFQDNs: []` (the default). Setting `egressFQDNs` on a cluster without Cilium fails Helm rendering instead of silently leaving workloads blocked behind `kodema-tenant-default-deny`.

2. **When `networkPolicy.egressFQDNs` allowlists are needed on k3s:**
   Install k3s with Flannel and the built-in network-policy controller disabled, then install Cilium with its L7 DNS proxy enabled:
   ```bash
   curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="server --flannel-backend=none --disable-network-policy" sh -
   cilium install --set l7Proxy=true
   ```

## Quickstart

### 1. Bootstrap the cluster

```bash
# Optional: test local generation and Helm template rendering offline first
deploy/k3s/install.sh --dry-run

# Deploy PKI, Secrets, in-cluster TLS PostgreSQL, migrations, and kodema-stack
deploy/k3s/install.sh
```

In `environment: "test"`, leaving `kodema.image.digest` empty automatically falls back `KODEMA_TOOLBOX_IMAGE` to `repository:tag`, so the `/kodema/bin/kodema` workload toolbox initContainer (`kodema output push`, `kodema git credential`, `kodema harness exec`) works out of the box. To pin both the control plane and toolbox by immutable digest (required in `production`), pass `KODEMA_K3S_IMAGE_DIGEST`:

```bash
KODEMA_K3S_IMAGE_DIGEST=sha256:<64-hex-digest> deploy/k3s/install.sh
```

Wait until `/readyz` returns `200 OK`:

```bash
kubectl -n kodema-system port-forward svc/kodema 8443:8443 &
curl --cacert ~/.local/state/kodema/k3s/kodema-system/pki/ca.crt https://localhost:8443/readyz
```

### 2. Bootstrap the first tenant and administrator

Port-forward OAuthSonas (`svc/oauthsonas`), fetch an administrator token, and run `kodema bootstrap-admin --profile test` inside the `kodema` pod:

```bash
kubectl -n kodema-system port-forward svc/oauthsonas 8444:8443 &

OAUTHSONAS_URL=https://localhost:8444 \
OAUTHSONAS_CA=~/.local/state/kodema/k3s/kodema-system/pki/ca.crt \
OAUTHSONAS_ACCESS_CODE_FILE=~/.local/state/kodema/k3s/kodema-system/secrets/oauthsonas-access-code \
OAUTHSONAS_CLIENT_ID=kodema-cli \
OAUTHSONAS_REDIRECT=http://127.0.0.1/callback \
  scripts/oauthsonas-token admin > /tmp/admin.token

kubectl -n kodema-system exec -i deploy/kodema -- sh -c \
  'cat > /tmp/t && kodema bootstrap-admin --profile test --tenant tenant-a --tenant-name "Local k3s Tenant" --token-file /tmp/t && rm -f /tmp/t' \
  < /tmp/admin.token
```

## Self-hosted production on k3s with OpenBao / Vault Transit

For production installations on self-hosted Kubernetes without a cloud KMS, keep `kodema.environment: "production"` and select the `vault-transit` key adapter:

```yaml
kodema:
  environment: "production"
  config:
    keyAdapter: "vault-transit"
    keyReference: "vault-transit:transit/kodema-kek"
    keyCurrentVersion: 1
    workspacePVC:
      storageClass: ""
  secrets:
    keys:
      existingSecret: kodema-keys            # contains kek-v1.blob (Vault Transit ciphertext)
    kmsCredentials:
      existingSecret: kodema-kms-credentials # contains VAULT_ADDR, VAULT_TOKEN, optional VAULT_NAMESPACE and VAULT_CACERT
```

`kodema-keys` holds the Vault/OpenBao Transit ciphertext (e.g. `vault:v1:...`) under `kek-v1.blob`, and `kodema-kms-credentials` supplies `VAULT_ADDR` (`https://...`), `VAULT_TOKEN`, and optional `VAULT_NAMESPACE` and `VAULT_CACERT` (for an in-cluster CA).

## Teardown

```bash
# Uninstall Helm release and workloads (keep PVCs, Secrets, and local PKI)
deploy/k3s/down.sh

# Full cleanup including PVCs, Secrets, tenant namespace, and local state
deploy/k3s/down.sh --purge
```
