# Kodema documentation

| Document | Contents |
|---|---|
| [KODEMA.md](../KODEMA.md) | The product specification and acceptance criteria. It governs on conflict. |
| [specs/](specs/README.md) | Detailed specifications: package boundaries, state transitions, atomicity and failure behavior. |
| [runbooks/quickstart.md](runbooks/quickstart.md) | A local Kodema with `kodema up`, and a first pipeline with `kodema run`. |
| [runbooks/llm-proxy-harnesses.md](runbooks/llm-proxy-harnesses.md) | `kodema harness exec`, active LLM budgets, attributed `kodsvc.` proxy, coding-harness recipes, and Option 1 / Option 2 OpenCode services. |
| [Agentic SDLC examples](../examples/agentic-sdlc/README.md) | Tested end-to-end Agentic SDLC pipelines covering in-container harnesses, Option 1 worktrees, and Option 2 Brain/Hands (`kodema sandbox serve`). |
| [runbooks/unprivileged-oci-builds.md](runbooks/unprivileged-oci-builds.md) | Unprivileged OCI container builds (Kaniko, `ko`, `buildah` VFS) and workload memory/CPU limits. |
| [runbooks/podman-quickstart.md](runbooks/podman-quickstart.md) | The Podman bundle: the single-host test installation the release smoke runs on. |
| [runbooks/production-deployment.md](runbooks/production-deployment.md) | Deployment on Kubernetes (Scaleway, AWS, GCP). |
| [runbooks/release-smoke.md](runbooks/release-smoke.md) | The release smoke scenarios against an installation. |
| [runbooks/unified-test-suite.md](runbooks/unified-test-suite.md) | The suites that need real infrastructure. |
| [runbooks/release.md](runbooks/release.md) | How a version is built and published. |
