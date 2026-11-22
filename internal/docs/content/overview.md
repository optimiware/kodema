# Kodema Overview

Kodema is an API-first, multi-tenant software factory that runs coding agents, container pipelines, and agentic services on Kubernetes (or local Podman).

## Fastest path: your harness as PM & Orchestrator

You do not need Kubernetes or local app builds to deliver software with Kodema:

1. **Start Kodema locally with Podman:**
   ```sh
   kodema up
   source ~/.kodema/instances/default/env.sh
   ```
   This starts the local control plane, Git companion, and Podman container runtime with zero Kubernetes setup.
2. **Configure LLM provider key(s) in the control plane:**
   ```sh
   printf '%s' "$ANTHROPIC_API_KEY" | kodema secret set ANTHROPIC_API_KEY --stdin
   # or OPENAI_API_KEY / OPENCODE_API_KEY, or --value-file <path>
   ```
   Workload containers and agentic services call Kodema's LLM proxy (`/llm/<provider>/*`) and never hold raw provider API keys.
3. **Connect your user harness (Claude Code, Codex, OpenCode, Cursor) via MCP + embedded docs/skill:**
   ```sh
   kodema mcp install claude              # or codex, opencode, cursor (--print to preview)
   kodema docs install --target agents    # or claude, opencode, all
   ```
4. **Collaborate with your harness as PM & Orchestrator — zero local app builds:**
   - Do **not** write application code directly on the user's laptop or run local builds/tests to deliver features.
   - Instead, collaborate with the user to clarify requirements, store specs or plans in the project KV store (`kv_put` -> `kv:sha256:...`), create/manage tasks (`task_create`, `task_create_and_start`, `task_start`), apply pipelines (`pipeline_apply`), inspect executions and logs (`execution_inspect`, `execution_logs_read`), answer questions (`task_reply`), resolve review gates (`execution_request_resolve` with `pass | revise | fail`), and close tasks (`task_complete`).
   - All coding, testing, verification, review, and protected Git merges run inside isolated Kodema containers or project-scoped agentic services.
5. **Graduate smoothly to 24/7 automation:** The exact same tasks, KV artifacts, and pipelines move seamlessly from interactive user-harness orchestration to an automated `kodsvc.` service account dispatcher, per-project OpenCode service (`kodema services --kind opencode --mode worktree` or `--mode sandbox` with `kodema sandbox serve`), and `service/merge-coordinator` (`kodema services --kind merge_coordinator`).

## Core model

> A **pipeline** is an ordered list of **actions**; an action runs a container or requests an answer; an **execution** runs a pipeline once; a **task** is a conversation that starts executions.

Hierarchy:

- **Tenant** — isolated organization (row-level security, dedicated derivation key, isolated Kubernetes namespace/network policy, isolated Git workers).
- **Project** — unit of visibility and configuration inside a tenant (pipelines, tasks, executions, KV objects, resource bindings, secrets, service accounts).
- **Resource binding** — registered external resource (such as a Git repository) owned and enforced by a service.
- **Pipeline** — named YAML definition managed strictly through the API (`pipeline.apply`, `pipeline.update`, `pipeline.delete`). Kodema never auto-syncs pipelines from Git pushes. Supports optional top-level `completes_task: false` (keeping tasks open across multi-stage pipelines) and `budget` limits.
- **Execution** — one immutable run of a pipeline (stored or inline YAML). Admission snapshots exact YAML bytes, resolves images to immutable digests, and captures inputs, budgets, caches, and any task conversation.
- **Action** — one step in an execution: either a **container action** or a **request action** (`role/<role>` or `service/<id>`).
- **Task** — persistent conversation with title, Markdown description, optional `parent_id` hierarchy, messages, userland JSON `metadata`, selected pipeline/inputs, and execution history. States: `open`, `waiting`, `done`, `cancelled`. Multi-stage tasks using `completes_task: false` close via explicit `task.complete`.
- **KV store** — project-scoped, write-once, content-addressed immutable object store (`kv:sha256:<64-hex>`, up to 16 MiB via `kv.put` / `kv.get` or `kodema kv put` / `kodema kv get`) for specs, plans, review reports, and handoff artifacts.
- **Decision ID** — immutable approval identity (`decision_id`) minted when a `request: role/<role>` resolves with `pass`, verifiable across executions via `POST /api/v1/verify` and Git push option `-o kodema.decision_id=<id>`.
- **Service account** — project-scoped automation principal (`kodsvc.*`) with non-admin roles.

## Identity and roles

Five roles exist per tenant/project:

- `admin` — tenant/project configuration, memberships, resource bindings, service accounts. **Grants no review authority.**
- `engineer` — repositories, pipelines, tasks, starting/cancelling executions, and managing project secrets.
- `reviewer` — answering `role/reviewer` requests (`pass`, `revise`, or `fail`).
- `operator` — runtime operations, answering `role/operator` requests, retrying failed tasks.
- `viewer` — read-only project operational visibility.

Every project member and enabled project service account can read non-secret operational state (`execution.*`, `task.*`, `pipeline.*`, `kv.get`, `resource.binding.*`, `repository.list`, `secret.metadata.*`, `service.list`, `event.read`, `event.subscribe`). Secret values are never readable through management APIs.

## Projects, repositories, and pipelines

- **Projects:** `kodema project create <id> [--name <name>] [--copy-members-from <project>]` (or MCP `project_create`) creates a project in the caller's tenant (project IDs and display names must be unique within a tenant) and initializes creator or copied project memberships automatically; `kodema project list` (or MCP `project_list`) defaults `tenant_id` from the caller's authenticated tenant.
- **Repositories:** `kodema repo create <name> [--project <id>]` (or MCP `repository_create`) provisions a Git repository over HTTP and registers its project resource binding in one step; `kodema repo list [--project <id>]` (or MCP `repository_list`) lists registered Git repositories.
- **Pipelines:** `kodema pipeline apply <file-or-dir>` (deriving pipeline names from filenames without `.yaml`/`.yml`), `kodema pipeline list`, `kodema pipeline show <name|id> [--yaml]`, and `kodema pipeline delete <name|id>`.

## API and MCP conventions

- REST (`/api/v1/...`) and MCP (`/mcp`) dispatch through one shared router and authorizer with 1:1 operation parity.
- Live project events stream over HTTP Server-Sent Events at `GET /api/v1/projects/{project}/events/stream` (`text/event-stream`, supporting `since`/`after`, repeatable `type`, and `Last-Event-ID` reconnect) using the same Bearer auth, membership, scope boundary, and credential-expiry rules as `event.subscribe` and `event.read`.
- In MCP, tool names use underscores (for example `task_create_and_start`, `kv_put`, `kv_get`, `task_complete`) and accept either flat top-level arguments or the wrapped `{"input": {...}, "expected_version": ..., "idempotency_key": ...}` envelope.
- State-changing operations require caller-supplied `idempotency_key` (retries with identical input converge; changed input fails with `changed_idempotency_content`).
- Versioned mutations require `expected_version` (stale versions fail with `version_conflict`; re-read the entity and retry).
- Discover every operation offline with `kodema call --list` and inspect its input JSON Schema with `kodema call --schema <operation>`.

## Further reading

- `kodema://docs/pipelines` (`kodema docs pipelines`) — pipeline YAML schema, caches, budgets, `completes_task`, and execution rules
- `kodema://docs/workload` (`kodema docs workload`) — container contract, `/kodema/*` files, `kodema harness exec`, caches, budgets, KV, and `kodema sandbox serve`
- `kodema://docs/git-rules` (`kodema docs git-rules`) — Git service, `admin/conf/git.conf`, Decision IDs, and disjoint rebase approval
- `kodema://docs/workflows` (`kodema docs workflows`) — 3-way reviews (`pass|revise|fail`), multi-stage tasks, KV handoffs, and recovery
- `kodema://docs/context` (`kodema docs context`) — shared company/project Git context repos and skills
- `kodema://docs/agentic-services` (`kodema docs agentic-services`) — Option 1 OpenCode worktree mode, Option 2 sandbox mode, attributed `kodsvc.` LLM proxy, and Helm NetworkPolicies
