# Kodema Pipelines

Pipelines are ordered sequences of actions executed one at a time inside a single execution.

## Execution rules

- Actions execute strictly in declaration order, with at most one active action per execution.
- There are **no** `when` conditions, loops, backward jumps, parallel branches, DAGs, joins, or YAML anchors/aliases. Put branching logic inside container scripts or choose a different pipeline.
- Every action has a finite timeout (Go duration from `1m` to `720h` / 30 days, defaulting to the installation default).
- Admission snapshots exact YAML bytes and resolves container images to immutable digests; later edits to the pipeline do not change an admitted execution.
- Only succeeded actions produce downstream outputs. Action failure stops the execution and cancels remaining actions with `upstream_failed`; explicit cancellation cancels remaining actions with `requested`.
- By default, a task's execution succeeding marks an open task `done`. Set top-level `completes_task: false` (or pass `--completes-task=false` / `--no-complete` on `task run`/`task start`/`task restart`) for intermediate SDLC stages so the task stays `open` until explicit `task.complete` (or a final stage with `completes_task: true`).

## Schema

```yaml
inputs: [repo_path, resource_id, target_ref, expected_base]
completes_task: false
budget:
  max_tokens: 250000
  max_cost_usd: 5.0
env:
  LOG_LEVEL: info

actions:
  - name: implement
    image: registry.example.com/coding-agent:1
    timeout: 30m
    caches: [go-mod, go-build]
    outputs: [candidate, summary_key]
    secrets: [BUILD_REGISTRY_AUTH]
    budget:
      max_tokens: 200000
    env:
      AGENT_MODE: batch
      GOMODCACHE: /kodema/cache/go-mod
      GOCACHE: /kodema/cache/go-build
    resources:
      memory: 4Gi
      cpu: "2"
    commands:
      - git clone "$KODEMA_GIT_URL/$(cat /kodema/inputs/repo_path)" /workspace/repo
      - cd /workspace/repo && kodema harness exec -- opencode run "Implement /kodema/task/spec.json and run tests"
      - git -C /workspace/repo rev-parse HEAD > /kodema/outputs/implement/candidate
      - git -C /workspace/repo diff --stat "$(cat /kodema/inputs/expected_base)"..HEAD | kodema kv put --content-type text/plain > /kodema/outputs/implement/summary_key

  - name: review
    request: role/reviewer
    timeout: 72h
    outputs: [notes]
    subject:
      operation: git.ref.update
      resource: {from: inputs.resource_id}
      ref: {from: inputs.target_ref}
      candidate: {from: implement.outputs.candidate}
      expected_base: {from: inputs.expected_base}
      summary_key: {from: implement.outputs.summary_key}

  - name: integrate
    image: registry.example.com/git-tools:1
    timeout: 5m
    commands:
      - |
        git -C /workspace/repo push origin \
          "$(cat /kodema/outputs/implement/candidate):$(cat /kodema/inputs/target_ref)"
```

## Top-level fields

- `actions` (required): ordered non-empty list of container or request actions.
- `inputs` (optional): declared admission input keys (string names or `{key, kind, required}` specs).
- `env` (optional): pipeline-level literal string environment variables inherited by container actions.
- `completes_task` (optional boolean, default `true`): when `false`, succeeding leaves an associated open task `open` so later stages can run on the same task before explicit `task.complete`.
- `budget` (optional): execution-level active LLM budget with at least one of `max_tokens` (positive integer, up to `1000000000`) or `max_cost_usd` (positive finite number, up to `1000000.0`). Combined with installation defaults (stricter limit wins) and enforced live by the LLM proxy before upstream calls (`429` / `budget_exceeded`).
- `repository`, `service`, `on.push` (optional): Git push trigger binding (see below).

## Container actions

- Required: `name`, `image`, `commands`.
- Optional:
  - `env`: string literals only; action values override pipeline-level `env`.
  - `secrets`: declared secret names resolved at activation.
  - `outputs`: declared UTF-8 keys (up to 8 keys, 4 KiB max each at submission; store larger payloads in the project KV store via `kodema kv put` and pass the `kv:sha256:...` key as an output).
  - `caches`: up to 8 lowercase cache slugs (`^[a-z0-9][a-z0-9._-]{0,62}$`) mounted writable at `/kodema/cache/<name>`, scoped strictly per `(tenant, project, cache_name)` and persisted across executions. Caches are explicitly **non-authoritative** (never a source of security or verification truth).
  - `budget`: per-action active LLM budget (`max_tokens` and/or `max_cost_usd`), enforced alongside execution and task budgets.
  - `timeout`: action duration (`1m` to `720h`).
  - `resources`: `memory` and/or `cpu`.
- Exit code `0` succeeds; nonzero fails. Commands run in `/workspace` with `/kodema/bin` appended to `PATH`.
- `KODEMA_URL`, `KODEMA_EXECUTION`, `KODEMA_ACTION`, `KODEMA_TASK` and secret name `KODEMA_TOKEN` are reserved and refused at apply. Runtime-supplied `KODEMA_CA_FILE`, `KODEMA_GIT_URL`, `KODEMA_LLM_<PROVIDER>_BASE_URL`, and `GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_*` / `GIT_CONFIG_VALUE_*` cannot be shadowed when configured by the installation. Secrets owned by configured LLM proxy providers cannot be mounted into containers via `secrets: [...]`; workloads authenticate to `/llm/<provider>/*` with `/kodema/secrets/KODEMA_TOKEN` (or `kodema harness exec`).

## Request actions

- Required: `name`, `request` (`role/<role>` or `service/<id>`), `subject`.
- Optional: `timeout` (defaults to the installation default), `outputs` (declared response keys provided by the responder). Request actions cannot declare `caches`, `budget`, `commands`, `env`, `secrets`, or `resources`.
- Runs **no container** and revokes the execution credential while waiting, while retaining `/workspace` for subsequent actions.
- `subject` values must be literals or direct lookups only: `{from: inputs.<key>}` or `{from: <earlier-action>.outputs.<key>}`. No string interpolation, self-references, or forward references.
- Resolution accepts three decisions: `pass`, `revise`, and `fail`:
  - `pass` succeeds the action, advances the execution, and mints an immutable `decision_id` (returned by `execution.request.resolve` and visible on `execution.inspect`) that can authorize cross-execution merges (`-o kodema.decision_id=<id>` or `service/merge-coordinator`).
  - `revise` fails the action with domain revision feedback (`needs_revision`), appends the reviewer note to the task conversation, and keeps the task open **without** escalating to the `role/operator` failure inbox.
  - `fail` rejects the action, fails the execution, and surfaces the failed task to `role/operator`.
- For approval-guarded effects (such as `git.ref.update`), include `operation`, `resource` (the registered resource ID, e.g. `git-shop-web`), and the exact effect parameters (`ref`, `candidate`, `expected_base`) in `subject`. Extra subject fields (such as `summary_key`) act as immutable display context for the reviewer and are ignored during verification matching.

## Push triggers (`on.push`)

- Declaring `on.push` requires `repository` (a registered resource ID) and `service` (a service account ID; binding requires Admin authority).
- Refs match **exact** full ref names (for example `refs/heads/main`) or **trailing-slash prefixes** (for example `refs/heads/work/`), with no globs or regexes.
- Push pipelines may only declare inputs from `repository` (the registered resource ID), `ref`, `commit`, and `previous_commit`; no other inputs (even optional) may be declared, and only declared keys are populated.
- Ref deletions and execution-originated pushes never trigger `on.push` (preventing push loops).

```yaml
repository: git-shop-web
service: svc-ci
on:
  push: [refs/heads/main, refs/heads/work/]
inputs: [repository, ref, commit, previous_commit]
actions:
  - name: verify
    image: registry.example.com/ci:1
    commands:
      - git clone "$KODEMA_GIT_URL/shop/web" /workspace/repo
      - git -C /workspace/repo checkout --detach "$(cat /kodema/inputs/commit)"
```

## Applying and managing pipelines

- Store/update named pipelines: `kodema pipeline apply <file-or-dir>` (calls `pipeline.apply`; each pipeline's name is derived from its filename without `.yaml`/`.yml`, for example `build.yaml` applies as `build`).
- List, inspect, and delete stored pipelines: `kodema pipeline list`, `kodema pipeline show <name|id> [--yaml]`, and `kodema pipeline delete <name|id>`.
- Run a local YAML file inline without storing it: `kodema run <file> [--input k=v] [--detach]` (calls `pipeline.start`).
- Create and start a task on a stored pipeline: `kodema task run --title "..." --pipeline <name|id> [--input k=v] [--completes-task=false|--no-complete]`.
