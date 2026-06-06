# INTENT — message

`message` is a schema-derived triad component on the emitted daemon runtime. It
owns two binaries: the `message` CLI (thin client) and `message-daemon` (the
stamp-and-forward ingress). Neither carries a durable message ledger — message
is a stateless boundary surface. Routing policy, delivery state, and channel
authority remain in router.

## The three planes

Message's runtime is the three schema-driven planes (`schema/signal.schema`,
`schema/nexus.schema`, `schema/sema.schema`), generated into `src/schema/`:

- **Signal** — the daemon's wire surface on `message.sock`. Operations:
  `Submit(MessageSubmission)`, `SubmitStamped(StampedMessageSubmission)`,
  `QueryInbox(InboxQuery)`. Replies: `SubmissionAccepted` / `SubmissionRejected`
  / `InboxListing` / `Unimplemented` / `Error`.
- **Nexus** — the internal-feature catalog (z6qu). Message's one internal
  feature is the forward-to-router decision plus the `ForwardToRouter` effect.
  The Nexus `decide` stamps and forwards `Submit`/`QueryInbox`, and replies
  `Unimplemented` to an already-stamped submission (the daemon mints provenance;
  it never accepts it from a peer).
- **SEMA** — honestly empty. Message owns no durable state, so its SEMA engine
  is a no-op returning `Stateless`. The plane exists only to satisfy the uniform
  three-plane shape.

## The emitted daemon

The daemon skeleton is emitted into `src/schema/daemon.rs` from the
`NexusDaemonShape` in `build.rs` (process `message-daemon`, single working
listener, no meta tier). The only daemon code message hand-writes is `impl
ComponentDaemon for MessageDaemon` in `src/daemon.rs`: `Configuration` /
`Engine` / `Error` / `PROCESS_NAME` + `build_runtime` + `handle_working_input`.
The daemon bin is the one-liner `MessageDaemon::run_to_exit_code()`. The daemon
reads a binary rkyv `Configuration` from its single argv argument (socket path,
router socket path, database path, owner name, owner uid) — no environment
variables on the production path, no flags.

## Wire translation to router

The daemon's inbound socket (`message.sock`) speaks the schema-derived
signal-frame format the emitted spine decodes. The router still speaks the
hand-written `signal-message` `MessageChannel` wire, so `RouterForwarder`
(`src/router.rs`) is the translation seam: schema `ForwardRequest` → wire
`MessageRequest` → router call → wire `MessageReply` → schema `Output`.
Provenance (origin + ingress timestamp) is minted in the forwarder from the
accepted connection's kernel-vouched peer credentials (`ConnectionContext` /
`SO_PEERCRED`) plus daemon configuration. A peer uid matching the configured
owner uid stamps the configured `owner_name` as this daemon's local harness
component instance; any other peer uid is `NonOwnerUser(uid)`. Provenance is
never accepted from the caller payload.

## Residuals (carried, not yet resolved)

- **CLI wire format.** The `message` CLI (`src/command.rs`, `src/surface.rs`)
  still encodes the old `signal-message` `MessageChannel` frames, not the
  schema-derived signal frames the migrated daemon now decodes. The CLI must be
  migrated to the new wire before the CLI↔daemon path works end to end.
