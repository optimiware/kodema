# Native LLM proxy, coding harnesses, and agentic services

Kodema includes a streaming, credential-injecting LLM proxy (`/llm/{provider}/*`) with live token/USD budget enforcement so external coding harnesses (**OpenCode**, **Claude Code**, **Codex**, **Pi**) and long-lived project **agentic services** run with **zero raw provider API keys exposed to workloads or agent pods**.

## How it works

1. Start Kodema (`kodema up` enables the LLM proxy automatically with `openai`, `anthropic`, and `opencode`).
2. Store provider API keys as project secrets:

   ```sh
   printf '%s' "$OPENCODE_API_KEY"  | kodema secret set OPENCODE_API_KEY  --stdin
   printf '%s' "$ANTHROPIC_API_KEY" | kodema secret set ANTHROPIC_API_KEY --stdin
   printf '%s' "$OPENAI_API_KEY"    | kodema secret set OPENAI_API_KEY    --stdin
   # Or read from a file with: kodema secret set ANTHROPIC_API_KEY --value-file <path>
   ```

3. Container workloads **never** list provider keys in `secrets: [...]`. Kodema enforces strict secret isolation: any secret owned by a configured LLM proxy provider is refused during workload secret materialization, even when local `kodema up` allows other project secrets.
4. At container activation, Kodema injects an unshadowable `KODEMA_LLM_<PROVIDER>_BASE_URL` environment variable for every configured provider in the execution's scope:

   | Provider | Injected variable | Proxy URL | Upstream default | Project secret |
   |---|---|---|---|---|
   | `openai` | `KODEMA_LLM_OPENAI_BASE_URL` | `${KODEMA_URL}/llm/openai/v1` | `https://api.openai.com` | `OPENAI_API_KEY` |
   | `anthropic` | `KODEMA_LLM_ANTHROPIC_BASE_URL` | `${KODEMA_URL}/llm/anthropic` | `https://api.anthropic.com` | `ANTHROPIC_API_KEY` |
   | `opencode` | `KODEMA_LLM_OPENCODE_BASE_URL` | `${KODEMA_URL}/llm/opencode/v1` | `https://opencode.ai/zen/v1` | `OPENCODE_API_KEY` |
   | Custom (`<slug>`) | `KODEMA_LLM_<SLUG>_BASE_URL` | `${KODEMA_URL}/llm/<slug>/v1` (or `/llm/<slug>` for `x-api-key`) | Configured `base_url` | Configured `secret_name` |

5. Inside container actions, wrap your harness command with **`kodema harness exec -- <cmd>`** (recommended) so Kodema handles TLS CA trust and live `KODEMA_TOKEN` rotation automatically. Kodema verifies the active execution and action deadline, enforces active LLM budgets, strips the caller credential, injects the provider secret inside the Shell, streams SSE responses chunk-by-chunk, and records per-execution token usage in `model_usage`.

## Custom OpenAI-compatible providers

Add custom providers (OpenRouter, LiteLLM, vLLM, Ollama, corporate gateways) or override a built-in provider directly with `--llm-provider`:

```sh
kodema up \
  --llm-provider openrouter=https://openrouter.ai/api/v1 \
  --llm-provider local-vllm=http://host.containers.internal:8000/v1,secret=VLLM_API_KEY
```

Store the matching project secret (for example `printf '%s' "$OPENROUTER_API_KEY" | kodema secret set OPENROUTER_API_KEY --stdin`, `printf '%s' "$VLLM_API_KEY" | kodema secret set VLLM_API_KEY --stdin`, or `--value-file <path>`), and workloads automatically receive `KODEMA_LLM_OPENROUTER_BASE_URL` and `KODEMA_LLM_LOCAL_VLLM_BASE_URL`.

For `kodema serve`, pass `--models-file /path/to/models.json` (or `KODEMA_MODELS_FILE`) or repeatable `--llm-provider <name>=<base_url>[,secret=<NAME>][,auth=<Authorization|x-api-key>][,tenant=<TENANT>][,project=<PROJECT>]`.

### Enabling on Kubernetes (Helm)

In [`deploy/charts/kodema`](../../deploy/charts/kodema/values.yaml) (and `kodema.*` in `deploy/charts/kodema-stack`), configure `config.models` to mount `KODEMA_MODELS_FILE` without putting raw API keys in Helm values (`secretName` always references Kodema's encrypted database secrets):

```yaml
config:
  models:
    defaultProviders: true # enables openai, anthropic, and opencode for every declared tenant
    providers:
      - tenantID: tenant-a
        name: openrouter
        baseURL: https://openrouter.ai/api/v1
        secretName: OPENROUTER_API_KEY
    budgets:
      defaultExecution:
        maxTokens: 250000
      pricing:
        - model: "anthropic/*"
          inputUSDPerMillionTokens: 3.0
          outputUSDPerMillionTokens: 15.0
        - model: "*"
          inputUSDPerMillionTokens: 3.0
          outputUSDPerMillionTokens: 15.0
  secretRules:
    rules:
      - tenantID: tenant-a
        names: ["NPM_TOKEN"]
```

- `environment: "production"` keeps both `config.models` and `config.secretRules` fail-closed/off by default (with zero implicit `budgets.pricing`) and refuses `allowAll: true` secret rules and non-`https://` provider URLs.
- `environment: "test"` (including [`deploy/k3s/values.yaml`](../../deploy/k3s/values.yaml)) defaults `config.secretRules.allowAllInTest: true` for declared tenants, enables `config.models.defaultProviders: true`, and seeds conservative wildcard `budgets.pricing` estimate rates in the k3s reference values.
- To supply an operator-managed ConfigMap instead of inline values, set `config.models.existingConfigMap` or `config.secretRules.existingConfigMap` (mutually exclusive with inline fields).

## Recommended in-container pattern: `kodema harness exec -- <cmd>`

`kodema harness exec` is built into `/kodema/bin/kodema` (on `PATH` inside every container action). It starts an ephemeral loopback-only HTTP reverse proxy (`127.0.0.1:0`) for the lifetime of the child command:

- Trusts `$KODEMA_CA_FILE` (or `--ca-file`) on the upstream hop to `$KODEMA_URL`—eliminating `NODE_EXTRA_CA_CERTS`, `SSL_CERT_FILE`, and `CURL_CA_BUNDLE` boilerplate for LLM calls.
- Re-reads `/kodema/secrets/KODEMA_TOKEN` (or `--token-file`) **fresh on every upstream request**, so live controller token rotations during long coding runs never cause `401` errors.
- Generates a random per-invocation local dummy API key, validates it on loopback requests, and exports `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_API_KEY`, `OPENCODE_BASE_URL`, `OPENCODE_API_KEY`, `KODEMA_HARNESS_PROXY_URL`, and rewritten `KODEMA_LLM_<NAME>_BASE_URL` variables to the child process.
- When `OPENCODE_CONFIG_CONTENT` is unset or empty, automatically populates a default OpenCode v2 `OPENCODE_CONFIG_CONTENT` routing the built-in `opencode`, `openai`, and `anthropic` providers through the loopback proxy (using `{env:*_API_KEY}` references). Any caller-supplied non-empty `OPENCODE_CONFIG_CONTENT` is preserved verbatim so custom provider overrides continue to work.

Combine `kodema harness exec` with **named non-authoritative caches** (`caches: [...]` mounted at `/kodema/cache/<name>`) and **active LLM budgets** (`budget: {max_tokens, max_cost_usd}`):

### OpenCode

```yaml
budget:
  max_tokens: 250000
actions:
  - name: opencode
    image: registry.example.com/kodema/opencode-harness@sha256:0000000000000000000000000000000000000000000000000000000000000000
    timeout: 30m
    caches: [go-mod, go-build]
    budget:
      max_tokens: 200000
    env:
      GOMODCACHE: /kodema/cache/go-mod
      GOCACHE: /kodema/cache/go-build
    commands:
      - kodema harness exec -- opencode run "Implement /kodema/task/spec.json and verify tests pass"
```

### Claude Code

```yaml
actions:
  - name: claude
    image: registry.example.com/kodema/claude-harness@sha256:0000000000000000000000000000000000000000000000000000000000000000
    timeout: 30m
    budget:
      max_tokens: 200000
    commands:
      - kodema harness exec -- claude -p "Implement /kodema/task/spec.json and verify tests pass" --dangerously-skip-permissions
```

### Codex

```yaml
actions:
  - name: codex
    image: registry.example.com/kodema/codex-harness@sha256:0000000000000000000000000000000000000000000000000000000000000000
    timeout: 30m
    commands:
      - kodema harness exec -- codex exec "Implement /kodema/task/spec.json and verify tests pass"
```

### Pi

```yaml
actions:
  - name: pi
    image: registry.example.com/kodema/pi-harness@sha256:0000000000000000000000000000000000000000000000000000000000000000
    timeout: 30m
    commands:
      - kodema harness exec -- pi -p "Implement /kodema/task/spec.json and verify tests pass"
```

See also [`examples/pipelines/external-harness.yaml`](../../examples/pipelines/external-harness.yaml), the full [examples catalog](../../examples/README.md), and the tested multi-stage examples in [`examples/agentic-sdlc/README.md`](../../examples/agentic-sdlc/README.md).

## Active LLM token and cost budgets

Active budgets are enforced **live inside the LLM proxy** before dialing upstream across three scopes:

1. **Per-action:** `budget: {max_tokens, max_cost_usd}` on a container action.
2. **Per-execution:** top-level `budget: {max_tokens, max_cost_usd}` on a pipeline (combined with installation defaults in `models.json`; the stricter bound applies).
3. **Per-task:** installation default task budget accumulated across all sibling executions of the same task.

When any active limit is reached or exceeded, Kodema immediately rejects the call with HTTP `429` (`budget_exceeded`) and records a `model.budget.exceeded` event.

Cost budgets (`max_cost_usd`) look up per-million-token rates from `budgets.pricing` in `models.json` in specificity order: exact model name, `upstream_model`, `provider/*` wildcard, and `*` fallback:

```json
{
  "providers": [
    {
      "tenant_id": "tenant-a",
      "name": "opencode",
      "base_url": "https://opencode.ai/zen/v1",
      "secret_name": "OPENCODE_API_KEY"
    }
  ],
  "budgets": {
    "pricing": [
      {
        "model": "opencode/gemini-4-neon",
        "input_usd_per_million_tokens": 2.5,
        "output_usd_per_million_tokens": 10.0
      },
      {
        "model": "opencode/*",
        "input_usd_per_million_tokens": 3.0,
        "output_usd_per_million_tokens": 15.0
      },
      {
        "model": "*",
        "input_usd_per_million_tokens": 3.0,
        "output_usd_per_million_tokens": 15.0
      }
    ]
  }
}
```

- **Local development (`kodema up` and `deploy/k3s/values.yaml`):** seeds conservative wildcard estimate rates (`$3.00` input / `$15.00` output per 1M tokens for `openai/*`, `anthropic/*`, `opencode/*`, and `*`) when generating the default `models.json`, so shipped pipelines declaring `max_cost_usd` enforce a real spend guardrail out of the box. Passing an explicit `--models-file` to `kodema up` preserves that file verbatim without injecting default pricing.
- **Production (`kodema serve --profile production` and `deploy/charts/kodema/values.yaml`):** emits zero implicit pricing and fails closed with HTTP `501` (`unsupported_capability`) whenever `max_cost_usd` is active for a model with no matching `budgets.pricing` entry.

## Minimal coding-harness base image

With `kodema harness exec` injected automatically via `/kodema/bin/kodema`, a coding-harness container image needs **no custom proxy shell wrapper**:

```dockerfile
FROM docker.io/library/node:22-bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git jq \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g opencode-ai
```

Build and pin the digest before referencing it in a pipeline:

```sh
podman build -t localhost/kodema-opencode-harness:latest -f Containerfile .
```

## Attributed `kodsvc.` LLM proxy for agentic services

Long-lived userland agentic services authenticate with a project service account token (`kodsvc.`) instead of an execution token (`kodex.`). To invoke `/llm/{provider}/*`, the service must send both attribution headers:

- `X-Kodema-Execution: <execution-id>`
- `X-Kodema-Action: <action-id>`

Before dialing upstream, Kodema verifies that the service account is enabled in scope and that the named execution and `request: service/<id>` action are active and assigned to that exact service account. Kodema strips both headers and the `kodsvc.` token before forwarding, enforces active execution/task LLM budgets (`429` / `budget_exceeded`), records usage under the execution's `model_usage`, and terminates open streams if the execution is cancelled or passes its action deadline.

## Agentic Service Option 1: Per-project OpenCode service (`--mode worktree`)

Option 1 runs a persistent per-project OpenCode service (`kodema services --kind opencode --mode worktree`, the default mode) against an OpenCode v2 server (`opencode serve`, e.g. `v2.3.2+`, authenticated via HTTP Basic Auth `opencode:<password>` using `OPENCODE_PASSWORD`) that answers explicit `request: service/<id>` pipeline actions using durable per-task sessions and hardened local Git worktrees.

Create the project service account and write its `kodsvc.` credential atomically (`0600`, without echoing the plaintext token to stdout; service credentials allow up to `30d` by default, configurable via `--service-credential-max-ttl` / `KODEMA_SERVICE_CREDENTIAL_MAX_TTL`):

```sh
kodema service create opencode-shop --project project-local
kodema service credential issue opencode-shop \
  --project project-local \
  --ttl 30d \
  --rotate-if-present \
  --out /path/to/opencode-shop.token
```

Run `opencode serve` with isolated `XDG_DATA_HOME` and `XDG_CONFIG_HOME` directories so saved credentials in `~/.local/share/opencode/opencode.db` never override `settings.apiKey`, send a conflicting second auth header (`Authorization` vs `X-Api-Key`), or inherit built-in `opencode` catalog overrides:

```sh
XDG_DATA_HOME=/var/lib/kodema/opencode-shop/xdg-data \
XDG_CONFIG_HOME=/var/lib/kodema/opencode-shop/xdg-config \
OPENCODE_PASSWORD="$(cat /path/to/opencode-server.password)" \
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
  --opencode-password-file /path/to/opencode-server.password \
  --llm-proxy-listen 127.0.0.1:18080 \
  --max-concurrency 1
```

Configure `$XDG_CONFIG_HOME/opencode/opencode.json` with the OpenCode v2 schema (`providers`, `settings.baseURL`, `settings.apiKey`, and `headers`):

```json
{
  "$schema": "https://opencode.ai/config.json",
  "providers": {
    "openai": {
      "settings": {
        "baseURL": "http://127.0.0.1:18080/llm/openai/v1",
        "apiKey": "{env:KODEMA_OPENCODE_LLM_PROXY_API_KEY}"
      },
      "headers": {
        "Authorization": "Bearer {env:KODEMA_OPENCODE_LLM_PROXY_API_KEY}"
      }
    }
  }
}
```

> **OpenCode v2 authentication & model selection:** Pass `--opencode-password-file` (`KODEMA_OPENCODE_PASSWORD_FILE`, recommended) or `--opencode-password` (`KODEMA_OPENCODE_PASSWORD` / `OPENCODE_PASSWORD`) to match the password configured on `opencode serve`. Use `--model providerID/modelID` or `--model providerID/modelID#variant` to select a default model per turn via OpenCode v2's session model API (`#variant` is honored on `agents.<name>.model` and the session model API, not on root `model` in `opencode.json`).

How Option 1 works:

- **Minimal service-account role:** Option 1 worktree mode requires **no `engineer` role** (`roles: []` can read project state/KV, call the attributed LLM proxy, and settle `service/<id>` requests). Grant the service ID `RW+ refs/heads/work/` in `admin/conf/git.conf`.
- **Durable task-to-session mapping:** Persists `task_id -> opencode_session_id` and per-action checkpoints under `--state-dir` so `plan`, `implement`, `fix`, `revise`, and `ai_review` turns share session continuity across retries and stages without double-pushing.
- **Hardened per-task Git worktrees:** Prepares an isolated worktree per task checked out on `refs/heads/work/<task-id>` from `base_ref` (default `refs/heads/main`), commits changes, pushes to Git, stores long outputs in KV (`summary_key`, `plan_key`, `review_key`), and settles the request action with declared outputs (`branch`, `candidate`, `summary_key`, `plan_key`, `review_key`, `session_id`).
- **Live SSE cancellation:** Subscribes to the project event stream and invokes OpenCode `AbortSession` immediately when an execution is cancelled.
- **Concurrent turns (`--per-turn-proxy-routing`):** Against stock `opencode serve` with `--llm-proxy-listen`, keep `--max-concurrency 1` (without `--per-turn-proxy-routing`) because stock OpenCode sends all LLM calls to the provider's static `settings.baseURL` and the proxy fails closed with HTTP `409 Conflict` when multiple turns overlap without explicit attribution. To enable `--per-turn-proxy-routing --max-concurrency 4`, add a ~10-line OpenCode v2 plugin (for example `$XDG_CONFIG_HOME/opencode/plugins/kodema-session-header/index.ts`) that stamps every outbound provider request with `X-OpenCode-Session` (registering an `http.request` hook also keeps provider traffic on HTTP):

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

The loopback proxy binds each active `sessionID` to its `(executionID, actionID)` turn, strips `X-OpenCode-Session` before forwarding upstream, and fails closed with HTTP `403` on unknown or ended sessions (explicit `/turn/{exec}/{action}/llm/*` routing is also supported for custom clients).

## Agentic Service Option 2: Sandbox mode (`--mode sandbox` + `kodema sandbox serve`)

Option 2 separates LLM reasoning and persistent sessions (the OpenCode service) from build/test tool execution (ephemeral sandboxes) so the OpenCode service pod never executes local shell commands or mutates local files, and every task runs inside an ephemeral stock toolchain image (`golang:1.24`, `node:22`, `python:3.12`, etc.):

1. **`kodema sandbox serve` (ephemeral sandbox):** Compiled into `/kodema/bin/kodema` using pure Go standard library—zero custom agent images required. Serves Streamable HTTP MCP JSON-RPC 2.0 at `POST /mcp` (plus `GET /healthz`) jailed strictly to `/workspace` via `os.Root`:
   - 6 jailed tools: `read_file`, `write_file`, `edit_file`, `glob`, `grep`, `bash`.
   - Constant-time Bearer token auth (`--token-file`, with optional `--unlink-token-file` for writable token files outside `/workspace`, `--token-env` defaulting to `KODEMA_SANDBOX_TOKEN`, or `--generate-token-file`), failing closed when no token is configured.
   - Strips token env vars (`KODEMA_SANDBOX_TOKEN`, `KODEMA_SANDBOX_TOKEN_FILE`, `--token-env`) before launching child `bash` processes and kills entire process groups on timeout or cancellation (`--max-cmd-timeout`, `--max-output-bytes`, `--max-file-bytes`).
2. **OpenCode service (`--mode sandbox`):** Disables all built-in local shell/filesystem tools (`bash`, `write`, `edit`, `read`, `glob`, `grep`) in OpenCode config and attaches ephemeral per-task sandboxes created through Kodema's public API (`KodemaExecutionSandboxManager`):

```sh
kodema service create opencode-shop --project project-local --role engineer
kodema service credential issue opencode-shop \
  --project project-local \
  --ttl 30d \
  --rotate-if-present \
  --out /path/to/opencode-shop.token

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
  --opencode-password-file /path/to/opencode-server.password \
  --llm-proxy-listen 127.0.0.1:18080 \
  --max-concurrency 1 \
  --sandbox-default-image docker.io/library/golang:1.24 \
  --sandbox-caches go-mod,go-build \
  --sandbox-port 8090 \
  --sandbox-memory 2Gi \
  --sandbox-cpu 2
```

Key Option 2 characteristics:

- **Role requirement difference vs Option 1:** Sandbox mode's service account **requires the `engineer` role** so `KodemaExecutionSandboxManager` can invoke `pipeline.start` (spawning inline sandbox executions), `execution.cancel` (teardown), and `secret.write`/`secret.delete` (delivering ephemeral `KODEMA_SBOX_TOK_*` per-sandbox Bearer tokens out-of-band, never embedded in YAML or `/kodema/script`). Option 1 worktree mode requires **no `engineer` role**.
- **Sandbox-free reasoning turns:** Non-coding modes (`plan`, `ai_review`) without `sandbox_image` execute directly in the OpenCode service without launching a sandbox container.
- **Polyglot toolchains & OpenCode v2 concurrency constraint:** Each stage or task can specify its own stock upstream image and caches (for example `sandbox_image: docker.io/library/golang:1.24` with `sandbox_caches: go-mod,go-build` for a Go backend stage and `sandbox_image: docker.io/library/node:22` with `sandbox_caches: npm` for a Node frontend stage). Because OpenCode v2 runtime MCP attachment (`/api/experimental/mcp/{server}`) is location-scoped rather than session-scoped, sandbox mode against an OpenCode v2 server enforces `--max-concurrency 1` per OpenCode v2 server instance (defaulting to `1`); run separate OpenCode v2 server instances for parallel sandbox-mode turns.
- **Fault containment & cancellation teardown:** If a sandbox container crashes or is OOM-killed mid-turn, the OpenCode service pod stays healthy, aborts only that turn (`ErrOpenCodeSandboxTerminated`), cancels the sandbox execution, and deletes the ephemeral token secret. Cancelling the parent execution tears down both the OpenCode session and the sandbox.
- **Kubernetes NetworkPolicies (Helm):** Enable `opencodeSandbox` in `deploy/charts/kodema/values.yaml` (rendered by `deploy/charts/kodema/templates/workloads-opencode-sandbox.yaml`) to isolate OpenCode service and sandbox pods strictly by `(tenant, project)`: OpenCode service pods deny all ingress and egress only to DNS, the Kodema API, and matching `kodema.dev/role: sandbox` pods on `sandboxPort`; sandbox pods accept ingress only from that exact project's OpenCode service pod and cannot reach each other or session storage. See [`examples/agentic-sdlc/opencode-sandbox-values.yaml`](../../examples/agentic-sdlc/opencode-sandbox-values.yaml).

## Inspecting per-execution token usage

Every proxied LLM call (both `kodex.` workloads and attributed `kodsvc.` services) is attributed to the execution ID in `model_usage` and returned by `execution.inspect` (REST and MCP):

```sh
kodema inspect <execution-id>
kodema inspect <execution-id> --output json
```

When an execution has made model calls, `kodema inspect` prints a `model_usage` summary line (`requests`, `input_tokens`, `output_tokens`, `total_tokens`) and a per-provider/model breakdown table.
