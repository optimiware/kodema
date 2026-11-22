// Package podman implements the Podman (libpod) container runtime adapter.
//
// The adapter talks to the container engine only through the small Libpod seam
// in api.go and to the host filesystem only through the Files seam. It is
// deliberately independent of the containers/podman Go client: unit tests drive
// deterministic in-memory libpod, file and clock fakes, and no socket, process,
// container or daemon call happens here.
//
// The adapter implements the declared runtime.Client and reconcile.Workspace
// contracts with no local disk state machines (no prepared.json, no
// start-attempted.json, no .removed/ tombstones, and no .workspaces/ retention
// markers): PostgreSQL and the container runtime are the sole authorities for
// durable lifecycle state. Every operation consumes the persisted
// reconcile.EffectIdentity so a retry converges on one stable container.
//
// Preparation materializes the non-secret activation snapshot under
// <root>/<workload>/kodema (bind-mounted read-only at /kodema) and isolates
// declared secrets and the mutable execution credential in a separate 0700
// directory <root>/<workload>/secrets (bind-mounted read-only at
// /kodema/secrets). When configured, the installation CA bundle is written once
// to <root>/.ca/<digest>/ca.crt and bind-mounted read-only at /etc/kodema/ca
// (/etc/kodema/ca/ca.crt). The execution's pre-existing retained workspace
// volume is attached read-write at /workspace. The credential file is the sole
// mutable mounted file and is replaced atomically by RefreshCredential without
// restarting the container.
//
// Observation and stop evidence stay in the declared Kernel vocabulary:
//
//   - an absent container always reports AbsentObservation, never proof that a
//     command did not run;
//   - a container in "created" status is treated conservatively as unknown in
//     Observe, non-removable in Remove, and uncertain on duplicate Start;
//   - only affirmatively observed whole-container termination confirms the stop
//     barrier; a stop acknowledgment never does;
//   - Remove removes only the identity's container and materialization tree and
//     never touches the retained workspace volume.
package podman
