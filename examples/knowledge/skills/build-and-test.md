# Skill: build and test

Use this skill when a change must become a digest-pinned OCI image and be
verified before promotion.

## When to use

- After code changes pass unit tests and are ready to package as an immutable
  container image.
- Whenever a downstream test stage needs to validate an already-published
  digest without rebuilding.

## Inputs

- Source repository path and exact ref or commit SHA.
- External registry destination (`registry.example.com/<org>/<app>`).
- Declared registry auth secret name (`REGISTRY_AUTH_CONFIG`) when pushing via
  unprivileged Kaniko.

## Steps

1. Build the image in an unprivileged container action (no Docker/Podman socket
   or `/dev/fuse`) and publish its bytes to the external registry.
2. Submit the immutable `<repo>@sha256:<64-hex>` digest as the declared `image`
   output (`/kodema/outputs/<action>/image`).
3. Run the test recipe against the forwarded `image` input; never rebuild or
   republish inside the test step.
4. Keep the exact commit and digest in the task handoff so retries and
   promotions reuse the exact same candidate.

## Guardrails

- A mutable tag is not a pinned reference.
- A lost output response is retried with the same digest, never a new build.
- Command exit success is not proof that a deployment is healthy.

## Related pipelines

- [`examples/pipelines/build.yaml`](../../pipelines/build.yaml)
- [`examples/pipelines/oci-build.yaml`](../../pipelines/oci-build.yaml)
- [`examples/pipelines/test.yaml`](../../pipelines/test.yaml)
