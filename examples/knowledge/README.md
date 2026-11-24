# Context Repository Knowledge

A context repository holds project knowledge as plain Markdown. Knowledge lives
in Git next to the code it describes and is referenced from task handoffs or
pulled into agent workspaces, so every fresh agent session reads the same
durable instructions instead of disposable workspace state.

## Layout

```text
examples/knowledge/
├── README.md                    # Context repository guide (this file)
├── conventions.md               # Project-wide durable rules
└── skills/
    ├── build-and-test.md        # Reusable skill: build & test pinned images
    └── promote-and-verify.md    # Reusable skill: promote, roll back & verify
```

## Skills & conventions

A skill is a Markdown document under `skills/` whose filename is a lowercase
slug (`^[a-z][a-z0-9-]{0,63}$`). Skills are reusable playbooks that coding
agents can load by name:

| File | Type | Covers |
|---|---|---|
| [`conventions.md`](conventions.md) | Doc | Repository layout, durable handoff references, and credential hygiene |
| [`skills/build-and-test.md`](skills/build-and-test.md) | Skill (`build-and-test`) | Building digest-pinned images and testing forwarded digests |
| [`skills/promote-and-verify.md`](skills/promote-and-verify.md) | Skill (`promote-and-verify`) | Protected promotion, rollback, and explicit readiness observation |

Knowledge is Markdown only: no binary blobs, no symlinks, no embedded
credentials, and no secret values. Declare secret names, never their values.

## Using context repositories with `kodema context`

Inspect and validate a local context directory:

```sh
kodema context list --dir examples/knowledge
kodema context show build-and-test --dir examples/knowledge
```

Pull shared tenant and project context repositories (plus Kodema's embedded
product docs) inside a container action or workstation:

```sh
kodema context pull \
  --repo context \
  --repo myproject/context \
  --target agents \
  --include-kodema-docs
```

## Further reading

- [Examples catalog](../README.md)
- [Delivery examples](../delivery/README.md)
- [Quickstart: embedded docs and shared company context](../../docs/runbooks/quickstart.md#embedded-docs-and-shared-company-context)
