# Repository Conventions

These conventions define durable, project-wide rules for every agent and
engineer working in the example context repository.

## Layout

- `conventions.md` records durable, project-wide rules.
- `skills/` contains reusable Markdown skills addressed by lowercase slug
  (for example `build-and-test` and `promote-and-verify`).
- Pipelines live outside the context repository and are managed through the
  Kodema API (`kodema pipeline apply` / `pipeline.apply`), never auto-synced
  from Git.

## Durable references

Handoffs, task questions (`kodema task ask`), and review subjects must
reference durable values that survive container teardown:

- Git commits and exact refs (`refs/heads/work/<task-id>`),
- Immutable OCI image digests (`registry.example.com/<repo>@sha256:<64-hex>`),
- Immutable project KV keys (`kv:sha256:<64-hex>`) for specs, plans, diffs, and
  test reports.

Never reference `/workspace`, `/tmp`, or process-local paths in handoffs; those
disappear when an execution ends or is cancelled.

## Credentials

Declare secret names only (`secrets: [...]`). Values are resolved at container
activation by the runtime and never stored in knowledge, pipeline YAML, logs,
events, or receipts. LLM provider keys remain in Kodema's control plane and are
accessed through `kodema harness exec` or the attributed `kodsvc.` proxy.

## Verification

- Treat named dependency caches (`/kodema/cache/<name>`) as non-authoritative
  accelerators, never as verification truth.
- Record explicit readiness observations after rollout rather than treating a
  deploy command's exit code as proof of health.
