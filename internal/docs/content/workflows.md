# Kodema Workflows

## 1. Planned approval gates & 3-way review decisions (`pass | revise | fail`)

Use a `request` action in pipeline YAML when human or service sign-off is part of the planned flow:

1. An earlier container action builds, tests, pushes a candidate branch/commit, stores large diffs or review reports in KV (`kodema kv put`), and writes outputs (for example `/kodema/outputs/implement/candidate`).
2. The `request` action (`request: role/reviewer` or `request: role/operator` or `request: service/<id>`) resolves its immutable `subject`, revokes the execution token, keeps `/workspace`, and appears in the role inbox.
3. A responder inspects the inbox and settles the request with one of three decisions:
   ```sh
   kodema inbox
   # Approve (mints immutable decision_id):
   kodema approve <execution-id> [--candidate <sha>] [--comment "..."] [--output key=val]
   # Request domain revision (appends feedback to task messages; does NOT alert role/operator inbox):
   kodema revise <execution-id> --reason "Handle nil pointer on empty cart" [--output key=val]
   # Hard rejection (fails execution and surfaces failed task to role/operator):
   kodema reject <execution-id> --reason "Security policy violation" [--decision fail|revise]
   ```
   When a verified `git.ref.updated` event moves the target ref past the request's `expected_base` while the request is pending, `kodema inbox` flags the item as `STALE` with the superseding commit and reason. `kodema approve` refuses stale requests by default (pass `--allow-stale` to override explicitly when disjoint-rebase approval is enabled, or `--candidate <sha>` to confirm the exact candidate commit being approved).
4. **Outcomes:**
   - **`pass`** advances the **same execution**, reuses `/workspace` with a fresh execution token, and mints an immutable **`decision_id`** (returned by `execution.request.resolve` and visible on `execution.inspect`). That `decision_id` can authorize either an in-execution push or a separate merge stage (`-o kodema.decision_id=<id>` or `service/merge-coordinator`).
   - **`revise`** records a domain revision rejection (`needs_revision`), atomically appends the feedback note to the task's conversation messages, and keeps the task `open` **without** escalating it to the `role/operator` failure inbox. The next implement/revise execution reads the feedback directly from `/kodema/task.json`.
   - **`fail`** or timeout fails the execution and surfaces the task in the `role/operator` failed-task inbox.

## 2. Unplanned conversation handoffs (fresh execution)

Use `task.ask` / `task.reply` when an agent needs clarification or a human decision that wasn't planned as a pipeline gate:

1. **Persist context durably first** — push a commit to a work branch, store a report in KV (`kodema kv put`), or publish an artifact digest. Never reference disposable `/workspace` paths or process-local session IDs because `/workspace` is discarded on ask.
2. **Ask on the task:**
   ```sh
   kodema task ask --question "Should we migrate existing rows or backfill lazily?" \
     --to role/engineer \
     --ref "commit=$(git rev-parse HEAD)"
   ```
   This atomically appends the question, marks the task `waiting`, cancels the current execution, and revokes its credential.
3. **Human/service replies:**
   ```sh
   kodema inbox --kind waiting_task
   kodema task reply <task-id> --answer "Backfill lazily in a separate job."
   ```
   Replying atomically appends the answer, reopens the task, and queues a **fresh execution** starting at action 1 with the updated conversation in `/kodema/task.json`.
4. **Restore in the fresh execution:** the first action reads `/kodema/task.json`, checks out the referenced commit/KV key/digest, and continues. Missing referenced context should fail visibly.

## 3. Multi-stage SDLC tasks, decoupled completion, hierarchy, and KV handoffs

Keep an entire feature lifecycle tied to one durable task across multiple pipelines (for example `plan` -> `implement` -> `verify-and-review` -> `merge`):

- **Decoupled task completion (`completes_task: false` + `task.complete`):** Declare `completes_task: false` at the top of intermediate pipeline YAMLs (or pass `--completes-task=false` / `--no-complete` on `kodema task run`, `kodema task start`, or `kodema task restart`). Succeeding leaves the task `open` instead of prematurely marking it `done`. When all stages finish, close the task explicitly:
  ```sh
  kodema task complete <task-id> --reason "Verified, approved, and merged to main"
  ```
- **Task hierarchy (`parent_id`):** Link subtasks to an epic or parent feature task in the same project with `--parent <parent-task-id>` on `kodema task create` / `kodema task run`, and list children with `kodema task list --parent <parent-task-id>`.
- **Project KV handoff artifacts (`kv:sha256:...`):** Action outputs are capped at 8 keys x 4 KiB. Store larger specs, architecture plans, test logs, or review reports in Kodema's project-scoped, write-once immutable KV store (up to 16 MiB per object) and pass the short `kv:sha256:<64-hex>` key through task inputs, action outputs, or request subjects:
  ```sh
  # Put from file or stdin (CLI or MCP kv_put):
  SPEC_KEY="$(kodema kv put spec.md --content-type text/markdown)"
  # Retrieve exact bytes (CLI or MCP kv_get):
  kodema kv get "$SPEC_KEY" --out /workspace/spec.md
  ```
- **Attach string metadata at creation:** `kodema task run --title "..." --pipeline implement --meta ticket=ENG-42 --meta priority=high` (or `--metadata k=v`). Metadata is immutable per execution and materialized at `/kodema/task/metadata.json` and `/kodema/task/spec.json`.
- **Chain the next pipeline stage atomically:** switch pipelines and pass outputs forward on the same task:
  ```sh
  kodema task start <task-id> \
    --pipeline verify-and-review \
    --no-complete \
    --input-from-output branch=implement.branch \
    --input-from-output candidate=implement.candidate
  ```
- **Supersede a running or waiting execution cleanly:** when a branch is updated or a pending approval is stale, atomically cancel the active execution (marked `superseded`) and start a fresh attempt:
  ```sh
  kodema task restart <task-id> \
    [--pipeline <name|id>] \
    [--input k=v] \
    [--input-from-output k=action.output] \
    [--reason "rebased onto latest main"]
  ```

## 4. From interactive PM/Orchestrator to 24/7 automation

Start by orchestrating tasks and pipelines interactively from your user harness (Claude Code, Codex, OpenCode, Cursor connected via `kodema mcp install`):

- Your harness collaborates with you to write specs (`kv_put`), create tasks (`task_create`), run isolated implementation/verification pipelines (`task_create_and_start` / `task_start`), inspect logs (`execution_logs_read`), iterate on `revise` feedback, and close tasks (`task_complete`)—without touching your laptop's local build toolchain.
- When ready for 24/7 hands-off delivery, attach the exact same pipelines to:
  - A `kodsvc.` service account dispatcher watching the project event stream,
  - A per-project OpenCode service (`kodema services --kind opencode --mode worktree` or `--mode sandbox` with `kodema sandbox serve`; see `kodema://docs/agentic-services`), and
  - A serialized merge coordinator (`kodema services --kind merge_coordinator`) that serializes pushes to `target_ref`, forwards `-o kodema.decision_id=<id>`, automatically rebases onto a fast-forwarded `target_ref` with `-o kodema.approved_base=<sha> -o kodema.approved_candidate=<sha>` when `option approve-rebase.<ref> = disjoint` is configured in `kodema git serve`, wakes on live SSE events, and resolves confirmed pre-push or `pre-receive` rejections as `fail` without wedging subsequent queued merges.

## 5. Delivery, artifacts, and promotion

- Artifacts are digest-pinned output strings (`registry.example.com/org/app@sha256:...`), not Kodema database entities.
- Build publishes image bytes to the external registry, then submits the immutable digest as a declared output.
- Test consumes `{from: build.outputs.image}` (read from `/kodema/outputs/build/image`) without rebuilding.
- Promotion updates `<tenant>/<project>/deploy` on a work branch, requests `role/operator` approval for that exact `git.ref.update`, and fast-forwards the protected ref after approval.
- Rollback is a new promotion execution targeting an older digest.

## 6. Live monitoring, inbox waiting, and multi-action logs

- **Stream project events:** `kodema events [--project <id>] [--type <type>...] [--since <cursor>] [--limit <n>] [--follow] [--output table|json]` reads committed events or live-streams decoded project events over `GET /api/v1/projects/{project}/events/stream` (`text/event-stream`, `Last-Event-ID` cursor resume, zero gaps/duplicates, NDJSON in `--follow --output json`).
- **Project-wide watch:** `kodema watch --project <id> [--until request] [--output table|json]` prints an initial active snapshot (tasks, executions, pending inbox) and streams live transitions across the project. Pass `--until request` to exit 0 as soon as a pending assigned request or waiting task question needs human attention.
- **Block until inbox work arrives:** `kodema inbox --wait [--project <id>] [--kind <kind>] [--output table|json]` exits immediately if an actionable item is already present, or waits on the live project event stream until an assigned request, waiting question, or failed task appears.
- **Follow logs across actions:** `kodema logs --follow <execution-id>` (without `--action`) follows sequential container actions in order with `==> action <name> (<id>) <==` headers (or pure NDJSON with `--output json`), drains each completed action before advancing, skips non-container request actions, and pauses cleanly with a stderr status note if the execution waits on a `request` action. Pass `--action <name|id>` to follow one specific action.

## 7. Inspecting and recovering failed work

- Inspect status, `decision_id`, `model_usage`, and action diagnostics (including `OOMKilled` and cleanup status): `kodema inspect <execution-id>`
- Read retained logs: `kodema logs <execution-id> --action <name> --stream stderr`
- Follow a single execution to completion (with failed-action stderr tail): `kodema watch <execution-id>`
- Retry a failed open task (as Engineer or Operator): `kodema task start <task-id>` (starts a fresh execution while preserving failed history).
- Cancel work: `kodema cancel <execution-id>` or `kodema task cancel <task-id>`.
