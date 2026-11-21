<p align="center">
  <img src="docs/assets/kodema-icon.svg" width="128" height="128" alt="The Kodema mascot: a small tree spirit shaped like a gopher">
</p>

<h1 align="center">Kodema</h1>

<p align="center">
  <strong>Run coding agents as a team, at enterprise scale, on your own Kubernetes</strong>
</p>

<p align="center">
  <sub><strong>The wood remembers</strong></sub>
</p>

<p align="center">
  <em>Unseen, the wood thinks<br>
  branches commit while you sleep<br>
  dawn finds the leaves changed</em>
</p>

<p align="center">
  <a href="https://github.com/optimiware/kodema/actions/workflows/ci.yml"><img src="https://github.com/optimiware/kodema/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/optimiware/kodema/releases"><img src="https://img.shields.io/github/v/release/optimiware/kodema" alt="Latest release"></a>
  <a href=".go-version"><img src="https://img.shields.io/github/go-mod/go-version/optimiware/kodema" alt="Go version"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/optimiware/kodema" alt="License: Apache-2.0"></a>
</p>

Kodema is an open-source software factory for engineering organizations. It
takes a task to a tested, merged change: a coding agent does the work in a
container, the people you choose approve it, and all of it runs on the
Kubernetes cluster you already operate.

A coding agent in a terminal serves one developer on one laptop. A company has
hundreds of repositories, people who start work and others who must review it,
and teams that must never see each other's code. Kodema is built for that
shape:

- **Any harness.** Claude Code, Codex, OpenCode, Pi or a shell script is a
  container command. Kodema gives each of them the same workspace, task,
  credential and model access, so changing harness changes an image, not the
  pipeline or its rules.
- **Enterprise scale.** Multi-tenant, role-based and audited. Each action is a
  plain Kubernetes Job, so capacity is your cluster's, and work that waits for
  a person holds a volume and no CPU.
- **People where you choose.** A pipeline can ask a role for a decision. The
  Git service refuses a push to a protected branch without a passed review of
  that exact commit.
- **Yours.** Apache-2.0, written in Go, self-hosted. Your code, pipelines and
  decisions stay in your cluster and your database.

> A pipeline is an ordered list of actions; an action runs a container or
> requests an answer; an execution runs a pipeline once; a task is a
> conversation that starts executions.

## One contract for every harness

Kodema has no plugin per agent. A harness is an image and a command, and every
container action is given the same things:

| A harness needs | Kodema provides |
|---|---|
| The code | `/workspace`, shared by the actions of one execution and kept across approval waits, plus optional non-authoritative named dependency caches (`caches: [...]`) mounted at `/kodema/cache/<name>` per project. |
| The task | `/kodema/task/` (`title`, `description`, `metadata.json`, `spec.json`), `/kodema/task.json` (the task's conversation), inputs/earlier outputs as files, and the project-scoped immutable KV store (`kodema kv put` / `kodema kv get`, `kv:sha256:...`, up to 16 MiB) for large specs, plans, and review reports. |
| An identity | `/kodema/secrets/KODEMA_TOKEN`, an execution credential that is renewed automatically during long runs and revoked while the execution waits for a decision or ends. |
| Tools | `/kodema/bin/kodema`, on `PATH` on both Kubernetes and Podman (`harness exec`, `kv`, `sandbox serve`, `docs`, `context`, `output push`, `git credential`). Git authenticates as the execution automatically when the workload Git endpoint is configured. |
| A model | The native credential-injecting LLM proxy (`/llm/<provider>/*`) with live token/USD budgets (`budget: {max_tokens, max_cost_usd}`). Wrap any harness with `kodema harness exec -- <cmd>` for rotation-safe loopback proxying and zero CA boilerplate; provider keys stay in the control plane. |
| A person | Planned `request` gates (`pass`, `revise`, or `fail`, minting immutable cross-execution Decision IDs on `pass`), or unplanned `task.ask` / `task.reply` conversation handoffs across fresh executions. |

```yaml
inputs: [repository, target_ref, expected_base]
actions:
  - name: implement
    image: registry.example.com/coding-agent@sha256:…   # the harness is this image
    outputs: [candidate]
    commands:
      - implement-and-push
  - name: test
    image: registry.example.com/test-tools@sha256:…
    commands:
      - test-candidate "$(cat /kodema/outputs/implement/candidate)"
  - name: review
    request: role/reviewer
    timeout: 72h
    subject:
      operation: git.ref.update
      resource: {from: inputs.repository}
      ref: {from: inputs.target_ref}
      candidate: {from: implement.outputs.candidate}
      expected_base: {from: inputs.expected_base}
```

Each action names its own image, so one pipeline can implement with one
harness and check the result with another. The review, the Git rules and the
audit trail do not depend on which harness ran.
[KODEMA.md](KODEMA.md#environment-and-materialized-files) has the contract in
full, and [external-harness.yaml](examples/pipelines/external-harness.yaml) is
the recipe for model access.

## Built for enterprise scale

| A company needs | What Kodema does |
|---|---|
| Teams that cannot see each other | A tenant is an isolated organization: a business unit, a subsidiary, a customer. The boundary is drawn again at every layer: forced row-level security in PostgreSQL, a namespace with a default-deny network policy per tenant on Kubernetes, a derivation key per tenant for credentials, and Git workers and volumes per tenant. |
| Its own sign-in and roles | People sign in through your OIDC provider. Five roles (Admin, Engineer, Reviewer, Operator, Viewer) are granted per tenant and per project, and a role change applies on the next call. The Admin role grants no review authority. |
| Control over what agents merge | Access rules live in a Git repository, in the spirit of gitolite. Per repository and per branch, a person decides, a program decides, or nobody does. |
| Secrets that stay secret | Values are envelope-encrypted under a key in your cloud KMS. The management API returns metadata, never a value. A container gets a secret as a file, and only if its action declares it. |
| To say what ran, and who allowed it | One function authorizes every operation. A decision keeps its actor and time. An execution keeps the exact pipeline bytes it was admitted with, their SHA-256 and the image digests. |
| Capacity for thousands of tasks | An execution is a row in PostgreSQL and its work is Kubernetes Jobs, with no CRDs and no operator. Your scheduler and autoscaler decide how many run at once. |
| To survive a failure | PostgreSQL records facts and NATS only distributes them, so a broker outage delays integrations and does not stop executions. After a crash the controller observes what it had started; it never reruns a command it cannot prove did not run. |
| To install it like everything else | One Helm chart for the control plane, and OpenTofu roots for Scaleway, AWS and Google Cloud that create the cluster, managed PostgreSQL and the KMS key. |

The [production deployment guide](docs/runbooks/production-deployment.md) has
the procedure for your own cluster.

## Status

The current version is in [`VERSION`](VERSION). The changes and known
limitations of each version are in [CHANGELOG.md](CHANGELOG.md). Kodema is
young, and the design is ahead of the evidence in these places:

- The control plane and the NATS broker each run as one replica. An upgrade of
  the control plane is a short outage.
- Scaleway is the only cloud on which a deployment has run. The AWS and Google
  Cloud deployments are implemented and have not been validated on a real
  cloud.
- The reference deployment is a starting point, not a sizing guarantee:
  load-test your own mix of work.
- Isolation is around ordinary containers, for teams that trust their own
  engineers. It is not a sandbox for untrusted code.

## Quick start

On Linux with rootless Podman 5 or newer, or on macOS with a Podman machine:

```sh
curl -fsSL https://raw.githubusercontent.com/optimiware/kodema/master/install.sh | sh
kodema up
source ~/.kodema/instances/default/env.sh
```

The first line installs `kodema` into `~/.local/bin`. It is one binary: the
client, the local installation and the server. [install.sh](install.sh)
downloads it from the latest release for your system and checks it against the
release's `SHA256SUMS`. `kodema up` then starts a complete local Kodema. Run a
pipeline file on it:

```sh
podman pull docker.io/library/alpine:3.24
cat > hello.yaml <<'EOF'
actions:
  - name: hello
    image: docker.io/library/alpine:3.24
    timeout: 2m
    commands:
      - echo "hello from a Kodema workload"
EOF
kodema run hello.yaml
```

```text
execution execution-a917c44ec73787377abaf074c6b6b61f started
hello from a Kodema workload
execution execution-a917c44ec73787377abaf074c6b6b61f succeeded
```

The [quickstart](docs/runbooks/quickstart.md) continues with approvals, tasks,
sign-in, Git and MCP.

### Connect your AI harness as PM & Orchestrator (zero local app builds)

The fastest way to build software with Kodema is to use your everyday AI harness
(Claude Code, Codex, OpenCode, or Cursor) as **PM & Orchestrator** while Kodema
runs all coding, verification, review, and protected Git merges inside isolated
containers or agentic services—with **zero Kubernetes setup and zero local app
builds**:

1. **Start Kodema locally with Podman:**
   ```sh
   kodema up
   source ~/.kodema/instances/default/env.sh
   ```
2. **Store LLM provider key(s) in the control plane:**
   ```sh
   printf '%s' "$ANTHROPIC_API_KEY" | kodema secret set ANTHROPIC_API_KEY --stdin
   # or OPENAI_API_KEY / OPENCODE_API_KEY, or --value-file <path>
   ```
   Provider keys stay inside Kodema's control plane; workload containers and
   agentic services call Kodema's LLM proxy without ever holding raw keys.
3. **Connect your harness over MCP and install Kodema's embedded docs skill:**
   ```sh
   kodema mcp install claude              # or codex, opencode, cursor (--print to preview)
   kodema docs install --target agents    # or claude, opencode, all
   ```
4. **Collaborate with your harness as PM & Orchestrator:**
   Shape specs, tasks, and pipelines with your harness instead of editing app
   code or running builds on your laptop. Your harness drives Kodema over MCP
   (`task_create`, `pipeline_apply`, `pipeline_start`, `kv_put`/`kv_get`,
   `execution_inspect`, `execution_logs_read`, `task_reply`,
   `execution_request_resolve` with `pass|revise|fail`, and `task_complete`).
5. **Graduate smoothly to 24/7 automation:**
   The exact same tasks, KV artifacts, and pipelines move unchanged from
   interactive harness orchestration to always-on `kodsvc.` services:
   - **Per-project OpenCode service (Option 1 — worktrees):**
     `kodema services --kind opencode --mode worktree`
   - **Brain/Hands split (Option 2 — ephemeral stock-image MCP sandboxes):**
     `kodema services --kind opencode --mode brain` + `kodema sandbox serve`
   - **Serialized merge coordinator with cross-execution Decision IDs:**
     `kodema services --kind merge_coordinator`

See the tested [Agentic SDLC examples](examples/agentic-sdlc/README.md) and the
[LLM proxy & harnesses runbook](docs/runbooks/llm-proxy-harnesses.md) for
complete pipelines and service configurations.

### Other ways to install

Without the script: download the `kodema` archive for your system and
`SHA256SUMS` from the
[latest release](https://github.com/optimiware/kodema/releases/latest), then:

```sh
sha256sum --check --ignore-missing SHA256SUMS    # macOS: shasum -a 256 --check --ignore-missing SHA256SUMS
tar -xzf kodema_*.tar.gz
install -m 0755 kodema_*/kodema ~/.local/bin/
```

From a checkout, with Go: `make build` builds `bin/kodema`; see the
[quickstart](docs/runbooks/quickstart.md#from-a-checkout).

Releases up to 0.2.3 shipped the client as a second binary, `kodactl`. Its
commands are now `kodema`'s, under the same names: `kodactl run` is
`kodema run`. Two were renamed: `kodactl status` is `kodema session`, and
`kodactl helper` is `kodema git credential`.

### The Podman bundle

The Podman bundle is a second single-host installation, with a separate
identity provider; the release smoke test runs on it. It installs the released
image (Podman 5, `git` 2.44, `jq`, `openssl`, `curl`, `socat`):

```sh
git clone --branch v0.7.0 https://github.com/optimiware/kodema && cd kodema
podman pull ghcr.io/optimiware/kodema/kodema:0.7.0
KODEMA_LOCAL_KODEMA_IMAGE=ghcr.io/optimiware/kodema/kodema:0.7.0 ./deploy/podman/install.sh
```

Then follow the [Podman bundle runbook](docs/runbooks/podman-quickstart.md) to
[run a first pipeline](docs/runbooks/podman-quickstart.md#run-a-first-pipeline).

## Documentation

| Document | Contents |
|---|---|
| [KODEMA.md](KODEMA.md) | The product specification and acceptance criteria. It governs on conflict. |
| [docs/README.md](docs/README.md) | Detailed specifications and runbooks. |
| [docs/runbooks/quickstart.md](docs/runbooks/quickstart.md) | A local Kodema with `kodema up`, connecting your AI harness over MCP, and a first pipeline with `kodema run`. |
| [docs/runbooks/llm-proxy-harnesses.md](docs/runbooks/llm-proxy-harnesses.md) | `kodema harness exec`, LLM budgets, attributed `kodsvc.` proxy, and Option 1 / Option 2 OpenCode services. |
| [examples/agentic-sdlc/README.md](examples/agentic-sdlc/README.md) | Tested end-to-end Agentic SDLC pipelines (in-container harness, Option 1 worktrees, Option 2 Brain/Hands). |
| [docs/runbooks/podman-quickstart.md](docs/runbooks/podman-quickstart.md) | The Podman bundle: the single-host test installation the release smoke runs on. |
| [docs/runbooks/production-deployment.md](docs/runbooks/production-deployment.md) | Deployment on Kubernetes (Scaleway, AWS, GCP). |
| [docs/runbooks/release.md](docs/runbooks/release.md) | How a version is built and published. |

## Repository layout

| Path | Contents |
|---|---|
| `cmd/kodema` | The one binary: the installation (`up`, `serve`, `git serve`, `auth`, `agent`, `services`, `migrate`, `bootstrap-admin`), the client (`run`, `login`, `inbox`, `approve`, `logs`, ...) and the toolbox of a container action (`output push`, `git credential`). |
| `api`, `client` | The public API declarations and the Go client. |
| `internal` | The Kernel, the Shell and Userland. |
| `deploy` | The image, the Helm charts, the Podman bundle and the OpenTofu roots per cloud. |
| `examples` | Example pipelines and knowledge files. |
| `test` | Contract, component, integration, e2e and qualification suites. |

## Build and test

The Go version is pinned in [`.go-version`](.go-version).

```sh
make build    # bin/kodema, stamped with the version and revision
make image    # the container image of this checkout, which `kodema up` starts
make check    # gofmt, vet, unit tests, race tests, architecture and docs checks
make dist     # dist/: the kodema archives, Helm chart packages, SHA256SUMS (needs helm)
```

The suites that need real infrastructure (Podman, kind, the stage) are
described in
[docs/runbooks/unified-test-suite.md](docs/runbooks/unified-test-suite.md).

## The name

Kodema takes its name from the kodama (木霊), a tree spirit of Japanese folklore:
it lives in a tree and is seldom seen. The haiku above says what this one does.
Pipelines and agents commit to branches while you are away, and when you return
you find the result, and whatever waits for your approval.

Ours is a gopher, because Kodema is written in Go. The mascot is inspired by
the [Go gopher](https://go.dev/blog/gopher), designed by Renee French and
licensed under CC BY 4.0. The icon and the social preview are in
[docs/assets](docs/assets).

## License

Apache-2.0. See [LICENSE](LICENSE).
