# Git Service and Access Rules

Kodema's optional Git companion (`kodema git serve`) is an independent Userland service that enforces access rules via Kodema's `/api/v1/verify` contract before accepting any read or push.

## Repository naming conventions

Tenant-relative repository paths:

```text
<tenant>/admin                  # access rules (conf/git.conf), Admin-managed
<tenant>/context                # company-wide Markdown docs, conventions, skills
<tenant>/<project>/context      # project-specific Markdown docs and skills
<tenant>/<project>/<component>  # application source repositories
<tenant>/<project>/deploy       # deployment / GitOps repositories
```

Inside a container action, repositories are addressed as `$KODEMA_GIT_URL/<repository>` (tenant-relative).

## Access rules (`admin/conf/git.conf`)

Tenant Admins manage access rules by editing `conf/git.conf` in the tenant's `admin` repository and pushing. Rules apply immediately on the next request.

```text
# <tenant>/admin -> conf/git.conf

repo admin
    RW                              = @admin

repo shop/web
    option resource                 = git-shop-web
    R                               = @all
    RW+  refs/heads/work/           = @engineer @execution
    RW   refs/heads/main            = @execution
    option approve.refs/heads/main  = reviewer

repo shop/deploy
    option resource                 = git-shop-deploy
    R                               = @operator @execution
    RW+  refs/heads/release/        = @execution
    RW   refs/heads/main            = @execution
    option approve.refs/heads/main  = operator
```

### Evaluation semantics

- Permissions:
  - `R` — read (clone/fetch)
  - `RW` — create or fast-forward ref
  - `RW+` — create, fast-forward, rewind, or delete ref
  - `-` — explicit deny
- **First-match wins** in declaration order: the first rule whose ref and principal match and whose permission covers the attempt allows; a matching `-` denies; no match refuses.
- Refs match **exact** names, or by **prefix** when ending in `/` (for example `refs/heads/work/`). No regular expressions. Omitting a ref covers all refs.
- Principals: `@admin`, `@engineer`, `@reviewer`, `@operator`, `@viewer`, `@execution` (running execution in scope), `@all` (every principal in scope), or explicit service-account IDs (for example `merge-coordinator` or `opencode-shop`). There is no implicit Admin bypass.
- `option resource = <resource-id>` binds the repository stanza to its registered Kodema resource ID. Always preserve it when editing `conf/git.conf`.

### Approval-guarded refs (`option approve.<ref> = <role>`)

When `option approve.<ref> = <role>` is set:

- By default, the `pre-receive` hook requires a **passed** `request: role/<role>` in the **same execution** that is pushing.
- That passed request's immutable subject must match `operation: git.ref.update`, `resource`, exact `ref`, `expected_base` (empty or all-zero OID for an unborn ref), and `candidate` commit OID.
- Humans carry no approval evidence and cannot push directly to approval-guarded refs. To land workstation changes, push to a `work/` branch and run a review/integration pipeline.

### Cross-execution approval Decision IDs (`-o kodema.decision_id=<id>`)

When a `request: role/<role>` resolves with `decision: pass`, Kodema mints an immutable, project-scoped **`decision_id`** (returned by `execution.request.resolve` and exposed on `execution.inspect`). This decouples review from integration across separate executions or a serialized merge coordinator without weakening security:

- `POST /api/v1/verify` accepts optional `decision_id` alongside `approval: role/<role>`. Kodema verifies that the recorded immutable decision belongs to the same tenant and project, is unrevoked and unexpired, was passed by an authorized `role/<role>` actor, matches exact `operation`, `resource`, and subject fields (`ref`, `expected_base`, `candidate`), and that the pushing actor is either an execution on the same execution/task or an enabled service holding an active assigned request on that execution/task.
- In `kodema git serve`, a pushing execution or authorized service account (such as `service/merge-coordinator` granted `RW refs/heads/main = merge-coordinator`) supplies the Git push option:
  ```sh
  git push -o kodema.decision_id="$DECISION_ID" origin "$CANDIDATE:refs/heads/main"
  ```
- When delegating merges to `service/merge-coordinator` (`kodema services --kind merge_coordinator`), pass `decision_id` in the merge request's `subject` alongside `repository`, `ref`, `base`, and `candidate`; the merge coordinator forwards `-o kodema.decision_id=<id>` when pushing.
- Cross-project, revoked, non-`pass`, or subject-mismatched Decision IDs are always refused.

### Opt-in disjoint rebase approval (`option approve-rebase.<ref> = disjoint`)

By default, every approval-guarded ref strictly requires exact `(expected_base, candidate)` matching—if the target ref moved after approval, the push is refused until a fresh review approves the new base and candidate.

A repository may opt in per ref to accept a clean linear rebase onto a moved target ref when the intervening commits touch only disjoint files:

```text
repo shop/web
    option resource                        = git-shop-web
    R                                      = @all
    RW+  refs/heads/work/                  = @engineer @execution
    RW   refs/heads/main                   = @execution merge-coordinator
    option approve.refs/heads/main         = reviewer
    option approve-rebase.refs/heads/main  = disjoint
```

Rules and security invariants:

- `option approve-rebase.<ref> = disjoint` requires a covering `option approve.<ref> = <role>` rule in the same repository stanza; unknown modes, duplicate declarations, or dangling `approve-rebase` rules are rejected.
- Without `option approve-rebase.<ref> = disjoint`, strict exact `(expected_base, candidate)` matching remains enforced even if push options are supplied.
- `kodema services --kind merge_coordinator` serializes pushes to `target_ref`, forwards `-o kodema.decision_id=<id>`, and by default rebases onto a moved `target_ref` while supplying both Git push options `-o kodema.approved_base=<40-hex-sha> -o kodema.approved_candidate=<40-hex-sha>` so `kodema git serve` verifies the disjoint-rebase policy in `pre-receive`. Pushing executions or custom rebase-aware merge services may also supply the same push options directly.
- For non-creation, non-deletion updates when push options differ from `(old_oid, new_oid)`, `kodema git serve` enforces all of the following in `pre-receive`:
  1. **Verified approval**: either the pushing execution holds a passed `request: role/<role>` or the supplied `kodema.decision_id` verifies that approval for `(operation: git.ref.update, resource, ref, expected_base: approved_base, candidate: approved_candidate)`.
  2. **Commit validity & fast-forward ancestry**: `approved_base`, `approved_candidate`, `current_ref`, and `pushed` are existing commit objects; `approved_base` is a strict ancestor of `approved_candidate` and an ancestor of (or equal to) `current_ref`; and `current_ref` is a strict ancestor of `pushed` (fast-forward only).
  3. **Linear, merge-free commit series**: `approved_base..approved_candidate` and `current_ref..pushed` contain no merge commits and have identical commit counts.
  4. **Disjoint touched paths**: the union of all paths touched in `approved_base..approved_candidate` (and `current_ref..pushed`) is 100% disjoint from all paths modified in `approved_base..current_ref`, checked with `--no-renames` (covering both old and new paths on renames/deletes) and rejecting file/directory prefix collisions.
  5. **Exact per-commit patch & mode equivalence**: every commit in `current_ref..pushed` matches its corresponding commit in `approved_base..approved_candidate` byte-for-byte in `git diff-tree -r --no-commit-id --no-renames --raw --no-abbrev -z`, ensuring identical file content, whitespace, binary blobs, symlinks, empty files, and file modes.
- Any failure refuses the push with a diagnostic explaining why a fresh review is required. Accepted pushes still report `git.ref.updated` with the actual pushed commit OID and its verification ID.

## Onboarding a repository

Create a bare repository, commit its default `conf/git.conf` stanza (including `option resource = <resource-id>`), and register its project resource binding in one command:

```sh
kodema repo create <name> [--project <id>] [--default-branch main]
kodema repo list [--project <id>]
```

The same operations are exposed over REST and MCP as `repository.create` (`repository_create`) and `repository.list` (`repository_list`).

## Workstation Git setup

Configure a workstation to authenticate with your `kodema login` session (never storing tokens in a keychain):

```sh
kodema git configure --origin https://<kodema-git-host> --global
```

Inside container actions, Kodema configures the credential helper and CA automatically whenever `KODEMA_GIT_URL` is set.
