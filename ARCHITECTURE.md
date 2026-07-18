# message — architecture

*The engine's stateful local messenger. Owns the `message` and
`meta-message` CLIs plus the supervised `message` daemon (binary:
`message-daemon`) and the durable local message state in `messenger.sema`.*

`message` owns three runtime binaries plus one bootstrap helper:

- The `message` CLI — one NOTA in, one NOTA out. Validates a
  user-typed NOTA record through Rust types, projects to a
  schema-derived signal frame, sends it to `message`
  on the engine's user-writable socket (`message.sock`, mode
  0660), reads one reply frame, prints the NOTA reply.
- The `meta-message` CLI — one owner-side `meta-signal-message`
  NOTA request in, one NOTA reply out. It connects to the owner
  meta socket from `MESSAGE_META_SOCKET` and currently receives
  typed skeleton-honest `RequestUnimplemented(NotBuiltYet)` replies
  for `Configure`.
- The `message` daemon (binary file: `message-daemon`) — a
  schema-derived triad daemon on the emitted `triad-runtime`
  runtime. The daemon skeleton is EMITTED into
  `src/schema/daemon.rs` from the `NexusDaemonShape` in
  `build.rs`; message hand-writes only `impl ComponentDaemon for
  MessageDaemon`. It reads a binary rkyv `Configuration` from its
  single argv argument (socket path, meta socket path, router socket path, database
  path, owner name, owner uid), binds `message.sock`, decodes schema-derived
  signal frames, binds the owner-only meta socket, runs the generated Nexus
  decision step, and forwards the translated request to `router`'s internal
  socket (`router.sock`) over the `signal-message` wire as a
  `StampedMessageSubmission`. The owner meta socket accepts
  `meta-signal-message` frames and currently returns typed unimplemented
  replies.
- `message-write-configuration` — a text-edge bootstrap helper, not a daemon
  surface. It accepts one NOTA `ConfigurationWriteRequest` and writes the binary
  rkyv startup file consumed by `message-daemon`.

There is no `MessageProxy` component here. The supervised
first-stack component is named `message`; the long-lived
binary is `message-daemon`.

## 0 · TL;DR

This repo owns the engine's stateful local messenger: a supervised daemon
plus ordinary and owner-side CLI clients, and the durable local message
state in `messenger.sema`. Since packet 2.1 that store holds the **agent
registry** — the durable consumer view of agent identity plus the local
delivery registry (orchestrator-allocated agent identifier, endpoint, resume
identity, death mark, optional pid + start-time pin; the ORCHESTRATOR is the
mint, psyche-ruled 2026-07-17). Since packet 3.1 it also holds the **message
ledger** (a bounded window with minted provenance on every row), the
**per-recipient inbox**, and the **thread index** (plain sender-chosen
names; participants auto-join; an optional relation ties a thread to a
repository + feature branch). A submission persists and answers locally —
the router is out of the local loop entirely; the delivery leg moves in with
packet 3.2a, and the router shrinks to the deferred host-to-host plane in
3.2b.

```mermaid
flowchart LR
    "human or harness" -->|"one NOTA Send / Inbox / Thread(s) / Subscribe"| "message CLI"
    "message CLI" -->|"length-prefixed schema signal frame"| "message"
    "message" -->|"ledger + inbox + thread commit"| "messenger.sema"
    "messenger.sema" -->|"typed reply projection"| "message"
    "message" -->|"length-prefixed reply frame"| "message CLI"
    "message CLI" -->|"one NOTA reply"| "human or harness"
    "meta-message CLI" -->|"meta-signal-message Configure"| "message meta socket"
    "message meta socket" -->|"RequestUnimplemented(NotBuiltYet)"| "meta-message CLI"
```

## 1 · Component Surface

`message` exposes:

- a `message` binary;
- a `meta-message` binary;
- a `message-daemon` binary;
- NOTA `Send`, `Inbox`, `Thread`, `Threads`, and `Subscribe` input records;
- one length-prefixed schema signal request frame per CLI invocation;
- one daemon-bound `message.sock` for working ingress;
- one owner-only meta socket for `meta-signal-message`;
- one NOTA reply projection per invocation;
- no caller-provided identity or provenance (all minted at ingress).

## 1.5 · Runtime triad — Signal / Nexus / SEMA

Message's runtime is the three schema-driven planes
(`skills/component-triad.md` §"Runtime triad"), generated into `src/schema/`:

```mermaid
flowchart LR
    "message.sock" --> "Signal (Input/Output)"
    "Signal (Input/Output)" --> "Nexus decide"
    "Nexus decide" -->|"ReplyToSignal"| "Signal (Input/Output)"
    "Nexus decide" -->|"ApplyRegistry / ReadRegistry effect"| "SEMA (messenger.sema)"
    "Nexus decide" -->|"ApplyMessageStore / ReadMessageStore effect"| "SEMA (messenger.sema)"
    "SEMA (messenger.sema)" -->|"typed reply projection"| "Nexus decide"
```

- **Signal** — the wire surface the emitted daemon decodes on `message.sock`.
- **Nexus** (`MessageEngine`) — the internal-feature catalog: registry
  apply/read plus message-store apply/read effects. The generated
  `NexusEngine::execute` performs one typed decision step; Message explicitly
  sequences its one effect and feeds the typed result back through the
  generated decision surface. A `Submit` is provenance-stamped in the effect
  runner (origin from `SO_PEERCRED`, sender resolved through the registry's
  process pins via `/proc` ancestry, daemon-minted ingress timestamp) and
  committed; `SubmitStamped` replies `Unimplemented` (the daemon mints
  provenance, never accepts it from a peer).
- **SEMA** — owns `messenger.sema` and commits both the agent-registry
  transitions (`AssignAgentIdentity` seat/reseat, `BindAgentEndpoint`,
  `QueryAgentRegistry`) and the message-state transitions (ledger append with
  bounded-window reaping, inbox reference, thread append with participant
  auto-join, explicit thread subscription). Store failures project to typed
  rejection replies, never a reply-less connection close.

The emitted daemon (`src/schema/daemon.rs`) owns the argv-config load, the
working-socket and owner-meta-socket binds, and the decode →
`handle_working_input` → encode spine. `MessageDaemon` (`src/daemon.rs`)
supplies only the `ComponentDaemon` escape hatches. The owner meta hook decodes
`meta-signal-message` frames and returns typed `RequestUnimplemented(NotBuiltYet)`
for `Configure` until live reconfiguration is wired. The daemon's durable state is `messenger.sema` at the configured database
path: agent registry, message ledger (bounded window `LEDGER_RETENTION_LIMIT`
with oldest-first reaping of rows AND their inbox/thread references), inbox,
and thread index.

Provenance (`src/provenance.rs`) is minted at ingress, never accepted from a
caller payload: `OriginPolicy` classifies the accepted connection from its
kernel-vouched `SO_PEERCRED` credentials, and `SenderResolver` walks the
peer's `/proc` ancestry against the registry's pid + start-time pins to name
the sending agent by its orchestrator-minted identifier (best-effort
enrichment — no match falls back to the owner-name or uid label; resolution
never gates a submission).

The published `signal-message` contract and the daemon's own emitted signal
module are index-aligned end to end — every shared operation's frame encodes
with either vocabulary and decodes with the other, enforced by
`tests/contract_convergence.rs`. `SubmitStamped`'s origin payload diverges by
design (leaner local origin vs the contract's cross-host origin) and is
typed-unimplemented in both directions.

## 2 · State and Ownership

The message component owns all durable local message state in
`messenger.sema` (`src/tables.rs`): the agent registry (seating
orchestrator-allocated identities — the orchestrator mints with spirit's
short-hash discipline and pushes; a reseat refreshes the pin and clears the
stale endpoint — the live delivery endpoint per agent, and the killed/dead
mark liveness feeds), the message ledger (a bounded window; every row
carries minted origin, resolved sender, and ingress stamp), the
per-recipient inbox, and the thread index (plain sender-chosen names,
auto-joined participants, optional repository + feature-branch relation).
The messenger participates in **no** version-handover snapshot (the Mirror
mechanism is orchestrate's own); store continuity across daemon versions is
carried by the store file and per-family migrations alone. The CLI requires
`MESSAGE_SOCKET` or `PERSONA_SOCKET_PATH` and exits if the message
daemon socket is absent. The daemon requires a typed binary rkyv
`Configuration` on argv (socket path, meta socket path, router socket path,
database path, owner name) whose `router_socket_path` names the router's
internal socket and whose `meta_socket_path` names the owner-only socket; the
emitted spine exits at decode time if the configuration is missing or malformed.

Caller identity is not accepted from the model or CLI payload.
`MessageSubmission` and `InboxQuery` stay sender-free, and the component sends
no in-band proof material. The daemon first gates trust with the configured
owner uid against the accepted stream's kernel-vouched peer uid. When that
check passes, the stored origin classifies `Owner` and the resolved sender
falls back to the configured `owner_name` when no registry pin matches the
peer's `/proc` ancestry; other peer uids stamp `NonOwnerUser` with a uid
label fallback. The
persona manager builds the configuration record from the engine's spawn
envelope and writes it to a binary rkyv file on spawn; the daemon never reads
environment variables for control-plane settings.

Typed-configuration-via-argv is the destination shape: every control-plane
setting (socket paths, owner identity, router socket) arrives as a typed
`Configuration` field decoded from the single argv argument by
`Configuration::from_binary_path`. The `SignalMessageSocket::from_environment`
constructor on the CLI side reads `MESSAGE_SOCKET` / `PERSONA_SOCKET_PATH` as
the ordinary CLI's socket discovery. `meta-message` reads `MESSAGE_META_SOCKET`
for the owner meta socket. The daemon path reads no environment variables.

Pending delivery, retry, and delivery results remain router concerns until
packet 3.2a moves the delivery leg in; the message ledger, inbox, threads,
and the delivery-target registry are message state now.

## 3 · Boundaries

This repo owns:

- NOTA parsing for the `message` command;
- NOTA parsing for the `meta-message` command;
- projection from NOTA `Send` / `Inbox` / `Thread` / `Threads` / `Subscribe`
  to the daemon signal wire;
- projection from NOTA `meta-signal-message` requests to owner meta frames;
- projection from `signal-message` replies back to NOTA;
- projection from `meta-signal-message` replies back to NOTA;
- length-prefixed Signal frame transport from CLI to `message.sock`;
- length-prefixed meta-signal frame transport from `meta-message` to the owner
  meta socket;
- frame-level exchange echoing for the current one-operation request/reply;
- provenance minting (origin classification, sender resolution, ingress
  stamping) at the accepted connection;
- the durable message ledger, inbox, and thread index.

This repo does not own:

- the published contract definition (`signal-message` is its own crate;
  convergence is test-enforced);
- the local delivery attempt to a terminal or harness endpoint (packet 3.2a);
- host-to-host routing, attestation, and trust (`router`);
- terminal endpoint vocabulary;
- terminal byte transport.

### 3.1 · Existence vs delivery, and the message-sent hook

Per archived intent `alom`, the conceptual ownership line between `message`
and `router` is existence versus delivery, and it is durable: `message` and
`router` stay separate because the SO_PEERCRED trust boundary cannot move
into `router`. `message` owns the **EXISTENCE** fact — an authenticated
message authored and witnessed at the SO_PEERCRED ingress — as the event it
emits at that boundary. `router` is authoritative for delivery on the routed
gated-or-remote path (durable on the harness-channel ack). A direct-delivery
fast path lets a message addressed to a publicly-reachable local agent by uid
deliver peer-to-peer without `router`, establishing delivery on the target's
direct ack. (Since packet 3.1 the existence fact is durable: the ledger row committed at
the SO_PEERCRED ingress IS the witnessed existence event. Only cross-host
delivery — where SO_PEERCRED cannot vouch and attestation is required —
remains router territory.)

Per archived intent `q73w`, the message lifecycle exposes hookable events:
the mail dispatch system emits and commits a typed `MessageSent` action at
the message-sent boundary, firing as soon as a message is sent so hooks, UI,
observers, routers, and subscribers can react immediately.

## 4 · Invariants

- The CLI accepts exactly one NOTA input record.
- The CLI prints exactly one NOTA reply record.
- The `meta-message` CLI accepts exactly one `meta-signal-message` NOTA request.
- The `meta-message` CLI prints exactly one `meta-signal-message` NOTA reply.
- Supported CLI input variants are `Send`, `Inbox`, `Thread`, `Threads`,
  and `Subscribe`.
- The message daemon socket is mandatory for the CLI.
- The owner meta socket is mandatory for the `meta-message` CLI.
- The configured router socket path is dormant (reserved for the deferred
  external-host escalation); the daemon never connects to it.
- The ledger is a bounded window: past `LEDGER_RETENTION_LIMIT` the oldest
  messages reap together with their inbox and thread references.
- The daemon is multi-listener: the emitted spine binds one working
  `message.sock` and one owner-only meta socket from the `NexusDaemonShape` in
  `build.rs`.
- The owner meta socket accepts `meta-signal-message` frames and replies typed
  `RequestUnimplemented(NotBuiltYet)` for `Configure` until live
  reconfiguration is built.
- The daemon uses the emitted `triad-runtime` `AsyncMultiListenerDaemon` spine
  (the `ComponentDaemon` / `DaemonBinder` default methods in
  `src/schema/daemon.rs`) for ingress instead of owning a hand-written accept
  loop.
- The daemon reads its typed binary rkyv `Configuration` from argv before
  accepting message ingress, and the stamped origin is derived from the
  accepted connection's kernel-vouched peer uid and the configured owner uid.
- CLI and daemon outbound traffic are length-prefixed rkyv Signal frames.
- Request/reply matching is frame-level: every request frame carries an
  `ExchangeIdentifier`, and every reply frame echoes the same identifier.
- The current message ingress path is deliberately one operation per request.
  Multi-operation request execution belongs in the shared Signal runtime slice,
  not in this component's ad hoc codec.
- Multi-payload router requests are rejected as typed `UnexpectedDaemonInput`
  until shared Signal batching exists; there is no outer Signal verb on the
  new frame kernel.
- Sender identity is absent from the CLI payload and absent from frame auth.
- Provenance is minted at ingress (`src/provenance.rs`) from
  `ConnectionContext` (`SO_PEERCRED`) and daemon configuration: origin
  classification, `/proc`-ancestry sender resolution against registry pins,
  ingress stamp. Provenance is never accepted from the CLI payload.
- The generated `NexusEngine::execute` surface owns the typed decision entry.
  Message's component code supplies the decision implementation and
  explicitly sequences its single store effect. There is no hand-written
  recursive Nexus loop and no local continuation budget in the component.
- Every domain failure is a typed rejection reply; the engine never returns
  `Err` for a store failure (a reply-less close must mean daemon death).
- The daemon hand-writes only `impl ComponentDaemon for MessageDaemon`
  (`src/daemon.rs`): `Configuration` / `Engine` / `Error` / `PROCESS_NAME` +
  `build_runtime` + `handle_working_input`. The daemon spine, request gate,
  accept loop, and lifecycle are emitted by schema-rust into the shared
  async task-backed triad-runtime shell.
- A graceful stop exits the daemon, releases the `message.sock` binding, and
  rejects later CLI ingress through the emitted spine's shutdown path.
- The production daemon reads no environment variables for control-plane
  configuration; it decodes a binary rkyv `Configuration` from its single argv
  argument. Witness: a source scan forbids env-var reads in the daemon binary
  (`src/bin/message_daemon.rs`) and the daemon hook source (`src/daemon.rs`).

## Code Map

```text
schema/signal.schema           daemon-local signal runtime (Input/Output)
schema/nexus.schema            internal-feature catalog (registry + store effects)
schema/sema.schema             durable state plane — registry + message-store apply/read
build.rs                       GenerationPlan + NexusDaemonShape (emits src/schema/*.rs)
src/schema/signal.rs           generated Signal plane
src/schema/nexus.rs            generated Nexus plane (NexusEngine)
src/schema/sema.rs             generated SEMA plane (SemaEngine)
src/schema/daemon.rs           EMITTED daemon skeleton (ComponentDaemon, the spine)
src/tables.rs                  messenger.sema — registry, ledger, inbox, thread families
src/main.rs                    message CLI entry
src/bin/meta_message.rs        owner meta CLI entry
src/bin/message_daemon.rs      daemon entry (one-liner: MessageDaemon::run_to_exit_code())
src/bin/message_validate_output.rs test/debug validator for message CLI NOTA replies
src/bin/message_write_configuration.rs NOTA -> binary daemon startup helper
src/config.rs                  binary rkyv daemon Configuration (impl BindingSurface)
src/daemon.rs                  impl ComponentDaemon for MessageDaemon (the only daemon code)
src/engine.rs                  MessageEngine + request-scoped generated Nexus runner hooks
src/meta.rs                    meta-message client, codec, command, and skeleton replies
src/frame_bytes.rs             preserved length-prefixed frame bytes for signal_channel contracts
src/command.rs                 CLI NOTA input/output projection
src/output_validator.rs        structured validator for sandbox message artifacts
src/provenance.rs              origin policy + /proc-ancestry sender resolution
src/surface.rs                 message-local NOTA surface records
src/error.rs                   crate error enum
tests/process_boundary.rs      emitted daemon over a real socket (send/inbox/thread)
tests/message_store.rs         ledger/inbox/thread engine witnesses
tests/agent_registry.rs        registry engine witnesses
tests/contract_convergence.rs  contract <-> daemon frame compatibility
```

## Constraint Tests

| Constraint | Test |
|---|---|
| The emitted daemon spine serves over a real socket and replies `Unimplemented` straight from the Nexus decision for an already-stamped submission. | `nix build .#checks.x86_64-linux.message-emitted-daemon-replies-unimplemented-for-already-stamped-submission` |
| The owner meta CLI reaches the meta socket and receives a typed `RequestUnimplemented(NotBuiltYet)` reply for `Configure`. | `nix build .#checks.x86_64-linux.message-meta-cli-reaches-owner-policy-socket` |
| The daemon stamps an owner-peer submission as the configured harness component instance from SO_PEERCRED + config and forwards it to the router, translating the acceptance back. | `nix build .#checks.x86_64-linux.message-daemon-stamps-owner-submission-to-router` |
| The daemon stamps a non-owner peer as `NonOwnerUser(uid)`, proving peer credentials survive the generated-runner hook path. | `nix build .#checks.x86_64-linux.message-daemon-stamps-non-owner-submission-to-router` |
| A router-unreachable forward yields a typed `Error` output. | `nix build .#checks.x86_64-linux.message-router-unreachable-yields-typed-error` |
| Message's generated Nexus plane owns the decision entry, and component code cannot reintroduce a local recursive Nexus loop or retired completion vocabulary. | `nix build .#checks.x86_64-linux.message-nexus-loop-is-generated` |
| The production daemon reads no environment variables for control-plane configuration. | `nix build .#checks.x86_64-linux.message-daemon-reads-no-control-plane-environment-variables` |
| Local ledger, actor index, in-band proof, and endpoint surfaces cannot return. | `nix build .#checks.x86_64-linux.message-component-cannot-own-local-ledger` |
| Retired terminal-brand vocabulary cannot return. | `nix build .#checks.x86_64-linux.message-runtime-cannot-reference-retired-terminal-brand` |
| The whole surviving test suite passes. | `nix build .#checks.x86_64-linux.default` |

## See Also

- `../signal-message/ARCHITECTURE.md`
- `../router/ARCHITECTURE.md`
