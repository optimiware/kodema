# Skill: promote and verify

Use this skill to promote an already-built, digest-pinned image through a
protected deploy target and verify post-rollout readiness.

## When to use

- Promoting a verified `<repo>@sha256:<64-hex>` image digest to a protected
  deployment branch.
- Rolling back a deployment by promoting a previously verified older digest.

## Inputs

- Immutable `image` reference (`registry.example.com/<org>/<app>@sha256:<64-hex>`).
- Target registered deploy resource (`deploy-repo-1`) and exact ref
  (`refs/heads/deploy`).

## Steps

1. Write the deploy-repository work-branch candidate referencing the exact
   forwarded `image` digest and record `candidate` and `expected_base` outputs.
2. Request human approval (`role/reviewer`) bound to the exact `operation`
   (`git.ref.update`), `resource`, `ref`, `expected_base`, and `candidate`.
3. Integrate the approved protected update without rebuilding image bytes.
4. Observe target readiness explicitly and emit a `ready` output.

## Rollback

Rollback is a new promotion of an older digest through the same protected
update. It is not an automatic data rollback and it never rebuilds the image.

## Guardrails

- Promotion and rollback share the registered deploy resource (`deploy-repo-1`)
  and exact ref (`refs/heads/deploy`).
- Approval binds the exact candidate; extra display context does not change it.
- Readiness is an observed result, not a command exit.

## Related pipelines

- [`examples/pipelines/promote.yaml`](../../pipelines/promote.yaml)
- [`examples/pipelines/rollback.yaml`](../../pipelines/rollback.yaml)
- [`examples/pipelines/readiness.yaml`](../../pipelines/readiness.yaml)
