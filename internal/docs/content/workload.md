# Container Workload Contract

Every container action receives the same filesystem layout, environment variables, optional Git authentication (when `KODEMA_GIT_URL` is configured), and `/kodema/bin/kodema` toolbox—regardless of whether it runs Claude Code, OpenCode, Codex, Pi, or a shell script.

## Filesystem layout

| Path | Mode | Contents |
|---|---|---|
| `/workspace` | Writable | Working directory shared sequentially by all container actions of one execution and preserved across planned `request` waits. Disposable when the execution ends or is cancelled by `task.ask`. |
| `/kodema/cache/<name>` | Writable | Present for each cache slug declared in `caches: [...]` (up to 8 per container). Persisted across executions strictly per `(tenant_id, project_id, cache_name)` (Podman named volume or Kubernetes shared cache PVC subPath, degrading cleanly to `emptyDir`). **Non-authoritative:** use only for dependency/build acceleration (`GOMODCACHE`, `GOCACHE`, `npm_config_cache`, etc.), never as security or verification truth. |
| `/kodema/inputs/<key>` | Read-only | Admitted execution input values. |
| `/kodema/outputs/<action>/<key>` | Read-only | Settled outputs of earlier succeeded actions in this execution. |
| `/kodema/outputs/<current-action>/` | Writable | Present when the current action declares `outputs`. Write one file per declared key; Kodema automatically runs `kodema output push` after the last command succeeds (trimming one trailing newline per file, max 8 keys, max 4 KiB each). |
| `/kodema/task.json` | Read-only | Task conversation snapshot at admission: a JSON array of messages (`author`, `at`, `body`, optional `references`). Absent for task-free executions. |
| `/kodema/task/title` | Read-only | Admitted task title (UTF-8 text, immutable for the execution). Absent for task-free executions. |
| `/kodema/task/description` | Read-only | Admitted task Markdown description (UTF-8 text, empty file when unset). Absent for task-free executions. |
| `/kodema/task/metadata.json` | Read-only | Admitted task metadata as a deterministic JSON object of string values (`{}` when unset). Absent for task-free executions. |
| `/kodema/task/spec.json` | Read-only | Combined admitted task specification JSON object (`{"id", "title", "description", "metadata"}`). Absent for task-free executions. |
| `/kodema/secrets/<NAME>` | Read-only | Declared project secrets resolved at container activation. |
| `/kodema/secrets/KODEMA_TOKEN` | Read-only | Live execution credential (`kodex.*`), automatically renewed by the controller during long actions and revoked during request waits or terminal transitions. |
| `/kodema/bin/kodema` | Read-only | Control-plane-matched `kodema` CLI toolbox, automatically added to `PATH`. |

## Standard environment variables

Always supplied (unshadowable):

- `KODEMA_URL` — HTTPS base URL of Kodema's API reachable from the workload.
- `KODEMA_EXECUTION` — current execution ID.
- `KODEMA_ACTION` — current action name.
- `KODEMA_TASK` — associated task ID (unset for task-free executions).

Supplied when configured by the installation:

- `KODEMA_CA_FILE` — path to the installation's custom CA PEM bundle (automatically trusted by `kodema`, `kodema harness exec`, and configured Git origins).
- `KODEMA_GIT_URL` — base URL of the installation's Git service (`$KODEMA_GIT_URL/<repository>`). Kodema injects `GIT_CONFIG_*` so Git authenticates to that origin alone via `/kodema/bin/kodema git credential` with no pipeline setup.
- `KODEMA_LLM_<PROVIDER>_BASE_URL` — unshadowable streaming LLM proxy endpoint for every provider enabled in the execution's scope (provider name normalized to uppercase `[A-Z0-9_]`). In `kodema up`, Kodema enables three built-in providers out of the box plus any custom `--llm-provider` entries:
  - `KODEMA_LLM_OPENAI_BASE_URL` -> `${KODEMA_URL}/llm/openai/v1` (backed by project secret `OPENAI_API_KEY`)
  - `KODEMA_LLM_ANTHROPIC_BASE_URL` -> `${KODEMA_URL}/llm/anthropic` (backed by project secret `ANTHROPIC_API_KEY`)
  - `KODEMA_LLM_OPENCODE_BASE_URL` -> `${KODEMA_URL}/llm/opencode/v1` (OpenCode Zen at `https://opencode.ai/zen/v1`, backed by project secret `OPENCODE_API_KEY`)
  - Custom providers (e.g. `--llm-provider openrouter=https://openrouter.ai/api/v1`) -> `KODEMA_LLM_OPENROUTER_BASE_URL=${KODEMA_URL}/llm/openrouter/v1`

## Recommended: `kodema harness exec -- <cmd>` (zero boilerplate, rotation-safe)

Store provider API keys once as project secrets (for example `printf '%s' "$ANTHROPIC_API_KEY" | kodema secret set ANTHROPIC_API_KEY --stdin` for `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or `OPENCODE_API_KEY`, or `--value-file <path>`). **Never declare provider keys in container `secrets: [...]`**—Kodema keeps raw provider keys inside the control plane and strictly refuses to mount LLM-proxy-managed secrets into workload containers.

Wrap any coding harness command with **`kodema harness exec -- <cmd...>`**:

```sh
kodema harness exec -- opencode run "Implement /kodema/task/spec.json and run tests"
kodema harness exec -- claude -p "Implement /kodema/task/spec.json and run tests" --dangerously-skip-permissions
kodema harness exec -- codex exec "Implement /kodema/task/spec.json and run tests"
kodema harness exec -- pi -p "Implement /kodema/task/spec.json and run tests"
```

What `kodema harness exec` handles automatically:

- Starts an ephemeral loopback-only HTTP reverse proxy (`127.0.0.1:0`) for the lifetime of `<cmd...>` forwarding `/llm/<provider>/*` to `$KODEMA_URL/llm/<provider>/*` over HTTPS while trusting `$KODEMA_CA_FILE` (zero `NODE_EXTRA_CA_CERTS`/`SSL_CERT_FILE`/`CURL_CA_BUNDLE` boilerplate needed for LLM traffic).
- Re-reads `/kodema/secrets/KODEMA_TOKEN` (override with `--token-file`) **fresh on every upstream request**, so live controller token rotations during long coding sessions never cause `401` errors.
- Exports `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `ANTHROPIC_BASE_URL`, `ANTHROPIC_API_KEY`, `OPENCODE_BASE_URL`, `OPENCODE_API_KEY`, `KODEMA_HARNESS_PROXY_URL`, and rewritten `KODEMA_LLM_<NAME>_BASE_URL` variables to the child process with a random per-invocation local dummy API key (verifying that key on loopback before forwarding).
- Automatically generates `OPENCODE_CONFIG_CONTENT` routing OpenCode's `opencode` (OpenCode Zen, `https://opencode.ai/zen/v1`), `openai`, and `anthropic` providers through the loopback proxy using `{env:*_API_KEY}` references so `kodema harness exec -- opencode run ...` works zero-config (preserving any non-empty `OPENCODE_CONFIG_CONTENT` explicitly set by the caller).
- Streams SSE responses chunk-by-chunk and attributes all token usage to the execution's `model_usage` (`kodema inspect <execution-id>`).

If a tool calls `$KODEMA_URL` directly without `kodema harness exec`, trust `$KODEMA_CA_FILE` (`NODE_EXTRA_CA_CERTS`, `SSL_CERT_FILE`, `CURL_CA_BUNDLE`, `GIT_SSL_CAINFO`) and read `/kodema/secrets/KODEMA_TOKEN` on each request. Read any other declared non-proxy secret file with `IFS= read -r ... < /kodema/secrets/<NAME>`.

## Active LLM budgets

Kodema enforces active token and USD spend budgets **live inside the LLM proxy** before dialing an upstream provider across three scopes:

- **Per-action:** `budget: {max_tokens: ..., max_cost_usd: ...}` on a container action.
- **Per-execution:** top-level `budget: {max_tokens: ..., max_cost_usd: ...}` on a pipeline (combined with installation defaults; the stricter limit wins).
- **Per-task:** installation default task budget accumulated across sibling executions of the same task.

When any active limit is reached or exceeded, the LLM proxy rejects the call with HTTP `429` (`budget_exceeded`) and records an attributable `model.budget.exceeded` event. Cost budgets fail closed if a model has no configured pricing rate.

## Toolbox commands inside containers

`kodema` automatically detects `/kodema/secrets/KODEMA_TOKEN`, `$KODEMA_CA_FILE`, and `$KODEMA_URL` inside a container action:

- **Run a coding harness behind the rotation-safe loopback LLM proxy:**
  ```sh
  kodema harness exec -- opencode run "..."
  ```
- **Store and read immutable KV artifacts** (project resolved automatically from `$KODEMA_EXECUTION`, max 16 MiB, canonical `kv:sha256:<64-hex>` key):
  ```sh
  key="$(kodema kv put plan.md --content-type text/markdown)"
  kodema kv get "$key" --out /workspace/plan.md
  ```
- **Read Kodema docs:** `kodema docs` / `kodema docs workload` / `kodema docs install --target agents`
- **Pull shared company/project context & skills:**
  ```sh
  kodema context pull --repo context --repo myproject/context --target agents --include-kodema-docs
  ```
- **Ask a human or service for unplanned input** (save durable commits or KV keys first—this cancels the current execution!):
  ```sh
  kodema task ask --question "Which target branch should this land on?" --to role/engineer --ref "commit=$(git rev-parse HEAD)"
  ```
- **Inspect or comment on the task:** `kodema task show "$KODEMA_TASK"`, `kodema call task.comment ...`
- **Connect an in-container harness over MCP:** `kodema mcp stdio` bridges stdin/stdout to `$KODEMA_URL/mcp` using `/kodema/secrets/KODEMA_TOKEN`.
- **Run an ephemeral jailed MCP tool sandbox (Option 2):** `kodema sandbox serve --listen 0.0.0.0:8090 --workspace /workspace --token-file /kodema/secrets/<NAME> --token-env <NAME>` (pure Go stdlib, `os.Root`-jailed `read_file`, `write_file`, `edit_file`, `glob`, `grep`, `bash`; reserve `--unlink-token-file` for writable token files outside `/workspace`, never read-only `/kodema/secrets/` mounts).
- **Submit outputs early (optional):** `kodema output push` (also run automatically after the final command).

## Execution permissions boundary

An execution credential (`kodex.*`) can read/edit its assigned task's content and metadata, comment on or ask questions on its task, create project tasks, read/write project KV objects (`kv.put`, `kv.get`), submit its own action outputs, publish custom events, and call configured models/tools. It **cannot** resolve requests, reply to waiting tasks, read plaintext secrets, or manage pipelines/memberships/bindings.
