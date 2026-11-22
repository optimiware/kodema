// Package container holds the shared container-runtime core used by the
// Process, Podman, and Kubernetes runtime adapters (S01 §2, S07 §3).
//
// Owned invariants:
//   - Canonical workload paths, file modes, resource bounds, and label keys so
//     sibling container runtimes agree on every mount path and metadata key.
//   - Deterministic identity validation, digest-derived resource naming, and
//     activation snapshot materialization/digests excluding mutable credentials.
//   - Canonical container environment construction that preserves admitted
//     literal variables (including KODEMA_LLM_*_BASE_URL) and appends
//     unshadowable reserved context, CA trust path, and scoped Git config.
//   - Bounded resource policy normalization, parsing, and per-action resolution.
//   - Reference-counted per-key locking, injected-clock retry with backoff, and
//     explicit runtime capability checks.
//   - Page-level log redaction without splitting configured secrets across page
//     boundaries, lossless dual-stream continuation cursors, and request bounds.
//
// Everything in this package is pure or clock-injected: it never opens a file,
// socket, container daemon, Kubernetes client, or subprocess.
package container
