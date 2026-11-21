# Agentic SDLC Examples

This directory contains a tested, end-to-end suite of Agentic SDLC pipelines
and Kubernetes NetworkPolicy values demonstrating all three Kodema agent
execution patterns—starting from an interactive user harness acting as **PM &
Orchestrator** on `kodema up` and graduating to 24/7 automation.

Every pipeline YAML here is decoded and validated against Kodema's real
admission parser and service subject contracts by
[`examples/agentic_sdlc_test.go`](../agentic_sdlc_test.go).

## Starting point: your user harness as PM & Orchestrator

Before running any 24/7 service, connect your everyday AI harness (**Claude
Code**, **Codex**, **OpenCode**, or **Cursor**) to a local Podman Kodema
instance with zero Kubernetes setup and zero local app builds:

```sh
kodema up
source ~/.kodema/instances/default/env.sh
kodema secret set ANTHROPIC_API_KEY      # or OPENAI_API_KEY, OPENCODE_API_KEY
kodema mcp install claude                # or codex, opencode, cursor
kodema docs install --target agents      # or claude, opencode, all
```

Collaborate with your harness over MCP to store specs in KV (`kv_put`), apply
these pipelines (`pipeline_apply`), create and drive multi-stage tasks
(`task_create`, `task_start`, `execution_inspect`, `execution_logs_read`),
iterate through 3-way review gates (`execution_request_resolve` with `pass`,
`revise`, or `fail`), and close tasks (`task_complete`).

## Pattern A: In-container harness SDLC

Best when you want each coding or verification stage to run as an isolated,
stateless container Job with zero long-lived agent pods:

1. **Stage 1 — Implement ([`pattern-a-implement.yaml`](pattern-a-implement.yaml)):**
   - Wraps the agent with `kodema harness exec -- opencode run ...` (rotation-safe
     loopback LLM proxy, zero CA boilerplate, zero raw provider keys in containers).
   - Mounts non-authoritative named caches (`caches: [go-mod, go-build]`) at
     `/kodema/cache/go-mod` and `/kodema/cache/go-build`.
   - Enforces active execution and action LLM budgets (`budget.max_tokens` and
     `budget.max_cost_usd`).
   - Reads the spec from KV (`kodema kv get`) and publishes a diff summary to KV
     (`kodema kv put`), setting `completes_task: false` so the task stays `open`.
2. **Stage 2 — Verify and 3-way review ([`pattern-a-verify-review.yaml`](pattern-a-verify-review.yaml)):**
   - Runs deterministic `go test -race ./...` in a separate toolchain image and
     uploads the report to KV (`report_key`).
   - Waits at `request: role/reviewer`:
     - `kodema approve <exec>` (`pass`) mints an immutable **`decision_id`**.
     - `kodema revise <exec> --reason "..."` (`revise`) appends feedback to the
       task conversation and keeps the task `open` without escalating to
       `role/operator`.
     - `kodema reject <exec> --reason "..."` (`fail`) fails the execution and
       alerts `role/operator`.
3. **Stage 3 — Serialized merge ([`pattern-a-merge.yaml`](pattern-a-merge.yaml)):**
   - Passes `decision_id` to `service/merge-coordinator`
     (`kodema services --kind merge_coordinator`), which pushes with
     `-o kodema.decision_id=<id>` (composing with
     `option approve-rebase.refs/heads/main = disjoint` in `admin/conf/git.conf`).
   - Closes the task explicitly with `kodema task complete <task-id>`.

```sh
kodema pipeline apply examples/agentic-sdlc/pattern-a-implement.yaml
kodema pipeline apply examples/agentic-sdlc/pattern-a-verify-review.yaml
kodema pipeline apply examples/agentic-sdlc/pattern-a-merge.yaml

SPEC_KEY="$(kodema kv put ./spec.md --content-type text/markdown)"
kodema task run --title "Add checkout validation" \
  --pipeline pattern-a-implement \
  --input repository=shop/web \
  --input base_ref=refs/heads/main \
  --input "spec_key=$SPEC_KEY"
```

## Pattern B: Option 1 per-project OpenCode service (`--mode worktree`)

[`pattern-b-worktree-sdlc.yaml`](pattern-b-worktree-sdlc.yaml) dispatches
`plan`, `implement`, and `ai_review` turns to a persistent per-project OpenCode
service (`service/opencode-shop`), followed by deterministic verification and a
human `role/reviewer` gate:

```sh
kodema services --kind opencode \
  --mode worktree \
  --service opencode-shop \
  --tenant tenant-local \
  --project project-local \
  --base-url "$KODEMA_URL" \
  --ca-file "$KODEMA_CA_FILE" \
  --credential-file /var/run/kodema/opencode-shop-cred \
  --state-dir /var/lib/kodema/opencode-shop \
  --git-url "$KODEMA_GIT_URL" \
  --opencode-url http://127.0.0.1:4096 \
  --llm-proxy-listen 127.0.0.1:18080 \
  --per-turn-proxy-routing \
  --max-concurrency 4
```

- Requires **no `engineer` role** on `opencode-shop` (only project membership /
  service enablement plus `RW+ refs/heads/work/` in `admin/conf/git.conf`).
- Reuses one durable OpenCode session per `task_id` across `plan`, `implement`,
  `revise`, and `ai_review`, while isolating each task in its own local Git
  worktree (`refs/heads/work/<task-id>`).
- Routes LLM calls through Kodema's attributed `kodsvc.` proxy with
  `X-Kodema-Execution` and `X-Kodema-Action` headers, enforcing pipeline/task
  budgets and aborting sessions immediately on execution cancellation.

## Pattern C: Option 2 Brain/Hands split (`--mode brain` + `kodema sandbox serve`)

[`pattern-c-brain-polyglot.yaml`](pattern-c-brain-polyglot.yaml) separates LLM
reasoning (**Brain**) from ephemeral toolchain execution (**Hands**):

- `plan` and `ai-review` omit `sandbox_image` and run directly in Brain with no
  sandbox container.
- `implement-backend` launches an ephemeral Go sandbox using stock
  `docker.io/library/golang:1.24` with `sandbox_caches: go-mod,go-build`.
- `implement-frontend` launches an ephemeral Node sandbox using stock
  `docker.io/library/node:22` with `sandbox_caches: npm`.
- Inside each ephemeral sandbox, `/kodema/bin/kodema sandbox serve` exposes
  `os.Root`-jailed `/workspace` MCP tools (`read_file`, `write_file`,
  `edit_file`, `glob`, `grep`, `bash`) behind constant-time Bearer auth.

```sh
kodema services --kind opencode \
  --mode brain \
  --service opencode-shop \
  --tenant tenant-local \
  --project project-local \
  --base-url "$KODEMA_URL" \
  --ca-file "$KODEMA_CA_FILE" \
  --credential-file /var/run/kodema/opencode-shop-cred \
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

- **Role requirement:** Unlike Option 1 worktree mode, Option 2 Brain mode's
  service account **requires the `engineer` role** so
  `KodemaExecutionSandboxManager` can start inline sandbox executions
  (`pipeline.start`), cancel them on teardown (`execution.cancel`), and manage
  ephemeral per-sandbox Bearer credentials (`secret.write` / `secret.delete`).
- **Kubernetes isolation:** Apply [`opencode-brain-values.yaml`](opencode-brain-values.yaml)
  with the Kodema Helm chart (`deploy/charts/kodema`) to render strict
  tenant/project-scoped Brain and sandbox NetworkPolicies.

## Further reading

- [Single-command local quickstart](../../docs/runbooks/quickstart.md)
- [Native LLM proxy, coding harnesses, and agentic services](../../docs/runbooks/llm-proxy-harnesses.md)
- [Product specification (`KODEMA.md`)](../../KODEMA.md)
