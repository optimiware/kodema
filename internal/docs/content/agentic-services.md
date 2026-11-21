# Agentic Services (Option 1 Worktree & Option 2 Brain/Hands)

Kodema lets you graduate from interactive user-harness orchestration or per-step container harnesses to always-on, project-scoped **agentic services** running as `kodsvc.` service accounts.

## Attributed `kodsvc.` LLM proxy

An enabled project service account can call Kodema's streaming LLM proxy (`/llm/{provider}/*`) without holding raw provider API keys:

- Authenticate with the service account's `kodsvc.` Bearer token (or `x-api-key`).
- Supply both attribution headers on every call:
  - `X-Kodema-Execution: <execution-id>`
  - `X-Kodema-Action: <action-id>`
- Kodema verifies that the named execution and `request: service/<id>` action are currently active and assigned to that exact service account, strips both headers and the `kodsvc.` token before dialing upstream, enforces active action/execution/task LLM budgets (`429` / `budget_exceeded`), records usage under the execution's `model_usage`, and aborts open streams if the execution is cancelled or hits its action deadline.

## Option 1: Per-project OpenCode service (`--mode worktree`)

`kodema services --kind opencode` defaults to `--mode worktree`: a long-lived per-project OpenCode service that answers explicit `request: service/<id>` actions in pipelines while keeping persistent per-task sessions and isolated local Git worktrees.

```sh
kodema services --kind opencode \
  --mode worktree \
  --service opencode-shop \
  --tenant tenant-local \
  --project project-local \
  --base-url "$KODEMA_URL" \
  --ca-file "$KODEMA_CA_FILE" \
  --credential-file /path/to/opencode-shop.token \
  --state-dir /var/lib/kodema/opencode-shop \
  --git-url "${KODEMA_GIT_URL:-${KODEMA_URL}/git}" \
  --opencode-url http://127.0.0.1:4096 \
  --llm-proxy-listen 127.0.0.1:18080 \
  --per-turn-proxy-routing \
  --max-concurrency 4
```

Key properties:

- **Minimal service-account privileges:** Option 1 worktree mode requires **no `engineer` role** (`roles: []` suffices to read project state/KV, call the attributed LLM proxy, and resolve explicit `service/<id>` requests; grant the service ID write access to `refs/heads/work/` in `admin/conf/git.conf`).
- **Durable task sessions:** Persists a 1:1 `task_id -> opencode_session_id` mapping and per-action idempotency checkpoints in `--state-dir` so subsequent turns (`plan` -> `implement` -> `revise`) reuse context and restarts never double-push.
- **Hardened per-task worktrees:** Prepares an isolated Git worktree per task on `refs/heads/work/<task-id>` from `base_ref` (default `refs/heads/main`), commits, and pushes the resulting candidate SHA.
- **Live cancellation:** Watches the project SSE event stream and calls OpenCode's `AbortSession` immediately when the execution is cancelled.
- **Concurrent turns:** When using `--llm-proxy-listen`, enable `--per-turn-proxy-routing` (routing calls via `/turn/{exec}/{action}/llm/*`) to run `--max-concurrency > 1`; without `--per-turn-proxy-routing`, static loopback proxy routing is limited to `--max-concurrency 1`.

### Request subject and outputs contract

Assign pipeline request actions to `service/<service-id>`:

- **Subject fields:**
  - `mode` (required): `plan`, `implement`, `fix`, `revise`, or `ai_review` (`implement`, `fix`, and `revise` mutate code and require `repository`).
  - `repository`: tenant-relative repository path (for example `shop/web`).
  - `base_ref`: base branch, ref, or commit SHA (defaults to `refs/heads/main` when `repository` is set).
  - `plan_key`, `feedback_key`: optional `kv:sha256:...` keys read from the project KV store and injected into the prompt.
  - Option 2 Brain-mode overrides: `sandbox_image`, `sandbox_caches` (comma-separated cache slugs), `sandbox_memory`, `sandbox_cpu`.
- **Declared response outputs** (declare the subset your pipeline needs): `branch`, `candidate`, `summary_key`, `plan_key`, `review_key`, `session_id`.

## Option 2: Brain/Hands split (`--mode brain` + `kodema sandbox serve`)

Option 2 separates long-lived LLM reasoning (**Brain**) from untrusted build/test execution (**Hands**):

1. **`kodema sandbox serve` (Hands):** Compiled directly into `/kodema/bin/kodema` using pure Go stdlib—no custom agent image required. Runs a Streamable HTTP MCP JSON-RPC 2.0 server (`POST /mcp`, `GET /healthz`) jailed strictly to `/workspace` via `os.Root`:
   - Exposes 6 tools: `read_file`, `write_file`, `edit_file`, `glob`, `grep`, `bash`.
   - Enforces constant-time Bearer token auth (`--token-file`, with optional `--unlink-token-file` for writable token files outside `/workspace`, `--token-env` defaulting to `KODEMA_SANDBOX_TOKEN`, or `--generate-token-file`), fails closed if no token is configured, strips token env vars (`KODEMA_SANDBOX_TOKEN`, `KODEMA_SANDBOX_TOKEN_FILE`, `--token-env`) from child `bash` processes, and enforces process-group timeout/cancel teardown (`--max-cmd-timeout`, `--max-output-bytes`, `--max-file-bytes`).
2. **OpenCode Brain (`--mode brain`):** Disables all built-in local shell and filesystem tools (`bash`, `write`, `edit`, `read`, `glob`, `grep`) in OpenCode config and dynamically spawns ephemeral per-task toolchain sandboxes via Kodema's public API (`KodemaExecutionSandboxManager`) using stock upstream images (for example `golang:1.24`, `node:22`, `python:3.12`):

```sh
kodema services --kind opencode \
  --mode brain \
  --service opencode-shop \
  --tenant tenant-local \
  --project project-local \
  --base-url "$KODEMA_URL" \
  --ca-file "$KODEMA_CA_FILE" \
  --credential-file /path/to/opencode-shop.token \
  --state-dir /var/lib/kodema/opencode-shop \
  --opencode-url http://127.0.0.1:4096 \
  --llm-proxy-listen 127.0.0.1:18080 \
  --per-turn-proxy-routing \
  --sandbox-default-image docker.io/library/golang:1.24 \
  --sandbox-caches go-mod,go-build \
  --sandbox-port 8090 \
  --sandbox-memory 2Gi \
  --sandbox-cpu 2
```

Important Option 2 operational details:

- **Service-account role requirement:** Brain mode's service account **requires the `engineer` role** so `KodemaExecutionSandboxManager` can call `pipeline.start` (inline sandbox execution), `execution.cancel` (teardown), and `secret.write`/`secret.delete` (ephemeral `KODEMA_SBOX_TOK_*` per-sandbox Bearer token delivery, never embedded in YAML or `/kodema/script`). Option 1 worktree mode does **not** require `engineer`.
- **Sandbox-free reasoning turns:** Non-coding turns (`plan`, `ai_review`) without `sandbox_image` run directly in the Brain pod without starting a sandbox container.
- **Polyglot concurrency & fault containment:** With `--per-turn-proxy-routing`, concurrent tasks can attach to distinct stock toolchain sandboxes (`golang:1.24`, `node:22`, etc.) simultaneously. If a sandbox crashes or is OOM-killed mid-turn, the Brain stays healthy, fails only that turn cleanly, and cleans up the sandbox execution and ephemeral token secret; cancelling the parent execution aborts the OpenCode session and tears down the sandbox.
- **Kubernetes NetworkPolicies (Helm):** Enable `opencodeBrain` in `deploy/charts/kodema/values.yaml` (`templates/workloads-opencode-brain.yaml`) to render paired tenant/project-scoped NetworkPolicies: the Brain pod denies all ingress and may egress only to DNS, the Kodema control plane, and matching `kodema.dev/role: sandbox` pods on `sandboxPort`, while sandbox pods accept MCP ingress only from that exact project's Brain pod and cannot reach each other or Brain session storage.
