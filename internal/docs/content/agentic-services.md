# Agentic Services (Option 1 Worktree & Option 2 Sandbox Mode)

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

Start `opencode serve` with `OPENCODE_PASSWORD` set from `/path/to/opencode.password` and isolated `XDG_DATA_HOME` / `XDG_CONFIG_HOME` directories so saved personal credentials in `~/.local/share/opencode/opencode.db` never collide with the local loopback proxy's `Authorization` / `X-Api-Key` header or override provider `baseURL` settings:

```sh
XDG_DATA_HOME=/var/lib/kodema/opencode-shop/xdg-data \
XDG_CONFIG_HOME=/var/lib/kodema/opencode-shop/xdg-config \
  opencode serve --hostname 127.0.0.1 --port 4096

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
  --opencode-password-file /path/to/opencode.password \
  --llm-proxy-listen 127.0.0.1:18080 \
  --max-concurrency 1
```

Configure `$XDG_CONFIG_HOME/opencode/opencode.json` using the OpenCode v2 schema (`providers`, `settings.baseURL`, and `headers`, plus `settings.apiKey` referencing `{env:KODEMA_OPENCODE_LLM_PROXY_API_KEY}` if required by the provider package):

```json
{
  "$schema": "https://opencode.ai/config.json",
  "providers": {
    "openai": {
      "settings": {
        "baseURL": "http://127.0.0.1:18080/llm/openai/v1"
      },
      "headers": {
        "Authorization": "Bearer {env:KODEMA_OPENCODE_LLM_PROXY_API_KEY}"
      }
    }
  }
}
```

Key properties:

- **OpenCode v2 Basic Auth:** `kodema services --kind opencode` speaks the OpenCode v2 HTTP API (`/api/info`, `/api/session`, `/api/session/{id}/prompt`, `/api/experimental/session/{id}/wait`, `/api/session/{id}/message`, `/api/session/{id}/interrupt`, and `/api/experimental/mcp/{server}`). Authenticate against `opencode serve` with `--opencode-password-file` (`KODEMA_OPENCODE_PASSWORD_FILE`, recommended) or `--opencode-password` (`KODEMA_OPENCODE_PASSWORD`, falling back to `OPENCODE_PASSWORD`), which sends HTTP Basic Auth with username `opencode`.
- **Model selection & `#variant`:** Pass `--model providerID/modelID` or `--model providerID/modelID#variant` (applied via OpenCode v2's `POST /api/session/{id}/model` API). Note that OpenCode v2 honors `#variant` on the session model API and `agents.<name>.model`, not on top-level `model` in `opencode.json`.
- **Minimal service-account privileges:** Option 1 worktree mode requires **no `engineer` role** (`roles: []` suffices to read project state/KV, call the attributed LLM proxy, and resolve explicit `service/<id>` requests; grant the service ID write access to `refs/heads/work/` in `admin/conf/git.conf`).
- **Durable task sessions:** Persists a 1:1 `task_id -> opencode_session_id` mapping and per-action idempotency checkpoints in `--state-dir` so subsequent turns (`plan` -> `implement` -> `revise`) reuse context and restarts never double-push.
- **Hardened per-task worktrees:** Prepares an isolated Git worktree per task on `refs/heads/work/<task-id>` from `base_ref` (default `refs/heads/main`), commits, and pushes the resulting candidate SHA.
- **Live cancellation:** Watches the project SSE event stream and calls OpenCode's `AbortSession` immediately when the execution is cancelled.
- **Concurrent turns (`--per-turn-proxy-routing`):** Against stock `opencode serve` with `--llm-proxy-listen`, keep `--max-concurrency 1` because stock OpenCode sends all LLM traffic to the provider's static `settings.baseURL` without per-turn headers (multiple active turns fail closed with HTTP `409 Conflict`). To run `--per-turn-proxy-routing --max-concurrency 4`, load a small OpenCode v2 server plugin (for example under `$XDG_CONFIG_HOME/opencode/plugins/kodema-session-header/index.ts`) that stamps each outbound request with `X-OpenCode-Session` (registering an `http.request` hook also keeps provider traffic on HTTP):

```ts
import { Plugin } from "@opencode/plugin"

export default Plugin.define({
  id: "kodema-session-header",
  async setup(ctx) {
    await ctx.session.hook("http.request", (e) => {
      e.request.headers.set("x-opencode-session", e.sessionID)
    })
  },
})
```

The loopback proxy binds each active `sessionID` to its `(executionID, actionID)` turn, strips `X-OpenCode-Session` before forwarding upstream, and fails closed with HTTP `403` for unknown or ended sessions (explicit `/turn/{exec}/{action}/llm/*` routing is also supported for custom clients).

### Request subject and outputs contract

Assign pipeline request actions to `service/<service-id>`:

- **Subject fields:**
  - `mode` (required): `plan`, `implement`, `fix`, `revise`, or `ai_review` (`implement`, `fix`, and `revise` mutate code and require `repository`).
  - `repository`: tenant-relative repository path (for example `shop/web`).
  - `base_ref`: base branch, ref, or commit SHA (defaults to `refs/heads/main` when `repository` is set).
  - `plan_key`, `feedback_key`: optional `kv:sha256:...` keys read from the project KV store and injected into the prompt.
  - Option 2 sandbox-mode overrides: `sandbox_image`, `sandbox_caches` (comma-separated cache slugs), `sandbox_memory`, `sandbox_cpu`.
- **Declared response outputs** (declare the subset your pipeline needs): `branch`, `candidate`, `summary_key`, `plan_key`, `review_key`, `session_id`.

## Option 2: Sandbox mode (`--mode sandbox` + `kodema sandbox serve`)

Option 2 separates long-lived LLM reasoning and session state (the OpenCode service) from untrusted build/test execution (ephemeral sandboxes):

1. **`kodema sandbox serve` (ephemeral sandbox):** Compiled directly into `/kodema/bin/kodema` using pure Go stdlib—no custom agent image required. Runs a Streamable HTTP MCP JSON-RPC 2.0 server (`POST /mcp`, `GET /healthz`) jailed strictly to `/workspace` via `os.Root`:
   - Exposes 6 tools: `read_file`, `write_file`, `edit_file`, `glob`, `grep`, `bash`.
   - Enforces constant-time Bearer token auth (`--token-file`, with optional `--unlink-token-file` for writable token files outside `/workspace`, `--token-env` defaulting to `KODEMA_SANDBOX_TOKEN`, or `--generate-token-file`), fails closed if no token is configured, strips token env vars (`KODEMA_SANDBOX_TOKEN`, `KODEMA_SANDBOX_TOKEN_FILE`, `--token-env`) from child `bash` processes, and enforces process-group timeout/cancel teardown (`--max-cmd-timeout`, `--max-output-bytes`, `--max-file-bytes`).
2. **OpenCode service (`--mode sandbox`):** Disables all built-in local shell and filesystem tools (`bash`, `write`, `edit`, `read`, `glob`, `grep`) in OpenCode config and dynamically spawns ephemeral per-task toolchain sandboxes via Kodema's public API (`KodemaExecutionSandboxManager`) using stock upstream images (for example `golang:1.24`, `node:22`, `python:3.12`):

```sh
kodema services --kind opencode \
  --mode sandbox \
  --service opencode-shop \
  --tenant tenant-local \
  --project project-local \
  --base-url "$KODEMA_URL" \
  --ca-file "$KODEMA_CA_FILE" \
  --credential-file /path/to/opencode-shop.token \
  --state-dir /var/lib/kodema/opencode-shop \
  --opencode-url http://127.0.0.1:4096 \
  --opencode-password-file /path/to/opencode.password \
  --llm-proxy-listen 127.0.0.1:18080 \
  --max-concurrency 1 \
  --sandbox-default-image docker.io/library/golang:1.24 \
  --sandbox-caches go-mod,go-build \
  --sandbox-port 8090 \
  --sandbox-memory 2Gi \
  --sandbox-cpu 2
```

Important Option 2 operational details:

- **Service-account role requirement:** Sandbox mode's service account **requires the `engineer` role** so `KodemaExecutionSandboxManager` can call `pipeline.start` (inline sandbox execution), `execution.cancel` (teardown), and `secret.write`/`secret.delete` (ephemeral `KODEMA_SBOX_TOK_*` per-sandbox Bearer token delivery, never embedded in YAML or `/kodema/script`). Option 1 worktree mode does **not** require `engineer`.
- **Sandbox-free reasoning turns:** Non-coding turns (`plan`, `ai_review`) without `sandbox_image` run directly in the OpenCode service pod without starting a sandbox container.
- **OpenCode v2 concurrency constraint & fault containment:** OpenCode v2 runtime MCP attachment (`/api/experimental/mcp/{server}`) is location-scoped rather than session-scoped, so sandbox mode against an OpenCode v2 server enforces `--max-concurrency 1` per OpenCode v2 server instance (defaulting to `1`) to prevent concurrent turns from clobbering each other's sandbox MCP registration; run separate OpenCode v2 server instances for parallel sandbox-mode turns. Each stage or task can still target a different stock toolchain image (`golang:1.24`, `node:22`, etc.). If a sandbox crashes or is OOM-killed mid-turn, the OpenCode service stays healthy, fails only that turn cleanly, and cleans up the sandbox execution and ephemeral token secret; cancelling the parent execution aborts the OpenCode session and tears down the sandbox.
- **Kubernetes NetworkPolicies (Helm):** Enable `opencodeSandbox` in `deploy/charts/kodema/values.yaml` (`templates/workloads-opencode-sandbox.yaml`) to render paired tenant/project-scoped NetworkPolicies: the OpenCode service pod denies all ingress and may egress only to DNS, the Kodema control plane, and matching `kodema.dev/role: sandbox` pods on `sandboxPort`, while sandbox pods accept MCP ingress only from that exact project's OpenCode service pod and cannot reach each other or session storage.
