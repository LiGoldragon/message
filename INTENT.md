# INTENT — message

`message` is a schema-derived triad component on the emitted daemon runtime. It
owns three binaries: the `message` CLI (ordinary thin client), `meta-message`
(owner meta-policy client), and `message-daemon` (the stamp-and-forward
ingress). Neither CLI nor daemon carries a durable message ledger — message is a
stateless boundary surface. Routing policy, delivery state, and channel
authority remain in router.

## The three planes

Message's runtime is the three schema-driven planes (`schema/signal.schema`,
`schema/nexus.schema`, `schema/sema.schema`), generated into `src/schema/`:

- **Signal** — the daemon's wire surface on `message.sock`. Operations:
  `Submit(MessageSubmission)`, `SubmitStamped(StampedMessageSubmission)`,
  `QueryInbox(InboxQuery)`. Replies: `SubmissionAccepted` / `SubmissionRejected`
  / `InboxListing` / `Unimplemented` / `Error`.
- **Nexus** — the internal-feature catalog (z6qu). Message's one internal
  feature is the forward-to-router decision plus the `ForwardToRouter` effect
  vocabulary. The generated `NexusEngine::execute` surface performs one typed
  decision step; Message explicitly sequences its one router effect and feeds
  the typed effect result back through the generated decision surface.
  `Submit`/`QueryInbox` stamp and forward, and `SubmitStamped` replies
  `Unimplemented` (the daemon mints provenance; it never accepts it from a
  peer).
- **SEMA** — honestly empty. Message owns no durable state, so its SEMA engine
  is a no-op returning `Stateless`. The plane exists only to satisfy the uniform
  three-plane shape.

## The emitted daemon

The async task-backed daemon skeleton is emitted into `src/schema/daemon.rs` from the
`NexusDaemonShape` in `build.rs` (process `message-daemon`, working listener
plus owner-only meta listener). The only daemon code message hand-writes is `impl
ComponentDaemon for MessageDaemon` in `src/daemon.rs`: `Configuration` /
`Engine` / `Error` / `PROCESS_NAME` + `build_runtime` + `handle_working_input`.
The daemon bin is the one-liner `MessageDaemon::run_to_exit_code()`. The daemon
reads a binary rkyv `Configuration` from its single argv argument (socket path,
meta socket path, router socket path, database path, owner name, owner uid) —
no environment variables on the production path, no flags. The meta listener
accepts `meta-signal-message` frames; `Configure` currently replies typed
`RequestUnimplemented(NotBuiltYet)` until runtime reconfiguration is wired to
the daemon's local configuration type.

## Wire translation to router

The CLI and daemon inbound socket (`message.sock`) speak the schema-derived
signal-frame format the emitted spine decodes. Router ingress speaks the
published schema-derived `signal-message` contract, so `RouterForwarder`
(`src/router.rs`) is the translation seam: daemon-local schema
`ForwardRequest` → `signal_message::Input` → router call →
`signal_message::Output` → daemon-local schema `Output`.
Provenance (origin + ingress timestamp) is minted in the forwarder from the
accepted connection's kernel-vouched peer credentials (`ConnectionContext` /
`SO_PEERCRED`) plus daemon configuration. A peer uid matching the configured
owner uid stamps the configured `owner_name` as this daemon's local harness
component instance; any other peer uid is `NonOwnerUser(uid)`. Provenance is
never accepted from the caller payload.
