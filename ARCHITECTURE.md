# message — architecture

*Engine message ingress / text boundary. Owns the
`message` and `meta-message` CLIs plus the supervised `message`
daemon (binary: `message-daemon`).*

`message` owns three binaries:

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

There is no `MessageProxy` component here. The supervised
first-stack component is named `message`; the long-lived
binary is `message-daemon`.

## 0 · TL;DR

This repo owns the engine's message-ingress boundary: a
small supervised daemon plus ordinary and owner-side CLI clients. None carries
a durable message ledger; all are stateless boundary surfaces. Routing policy,
delivery state, and channel authority remain in `router`.

```mermaid
flowchart LR
    "human or harness" -->|"one NOTA Send or Inbox"| "message CLI"
    "message CLI" -->|"length-prefixed schema signal frame"| "message"
    "message" -->|"StampedMessageSubmission"| "router"
    "router" -->|"length-prefixed reply frame"| "message"
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
- NOTA `Send` and `Inbox` input records;
- one length-prefixed schema signal request frame per CLI invocation;
- one daemon-bound `message.sock` for working ingress;
- one owner-only meta socket for `meta-signal-message`;
- one router client path to internal `router.sock`;
- one NOTA reply projection per invocation;
- no caller-provided identity and no local actor index.

## 1.5 · Runtime triad — Signal / Nexus / SEMA

Message's runtime is the three schema-driven planes
(`skills/component-triad.md` §"Runtime triad"), generated into `src/schema/`:

```mermaid
flowchart LR
    "message.sock" --> "Signal (Input/Output)"
    "Signal (Input/Output)" --> "Nexus decide"
    "Nexus decide" -->|"ForwardToRouter effect"| "RouterForwarder"
    "RouterForwarder" -->|"signal-message wire"| "router"
    "Nexus decide" -->|"ReplyToSignal"| "Signal (Input/Output)"
    "SEMA (Stateless)" -.->|"no durable state"| "Nexus decide"
```

- **Signal** — the wire surface the emitted daemon decodes on `message.sock`.
- **Nexus** (`MessageEngine`) — the internal-feature catalog: the forward
  decision plus the `ForwardToRouter` effect vocabulary. The generated
  `NexusEngine::execute` performs one typed decision step; Message explicitly
  sequences its one router effect and feeds the typed effect result through the
  generated decision surface. `Submit`/`QueryInbox` stamp and forward;
  `SubmitStamped` replies `Unimplemented` (the daemon mints provenance, never
  accepts it from a peer).
- **SEMA** — honestly empty (`Stateless`). Message owns no durable state; the
  plane exists only to satisfy the uniform three-plane shape.

The emitted daemon (`src/schema/daemon.rs`) owns the argv-config load, the
working-socket and owner-meta-socket binds, and the decode →
`handle_working_input` → encode spine. `MessageDaemon` (`src/daemon.rs`)
supplies only the `ComponentDaemon` escape hatches. The owner meta hook decodes
`meta-signal-message` frames and returns typed `RequestUnimplemented(NotBuiltYet)`
for `Configure` until live reconfiguration is wired. The daemon is stateless
across requests — no redb, no durable message ledger.

`RouterForwarder` (`src/router.rs`) is the translation seam between the
daemon-local inbound wire and the published schema-derived `signal-message`
wire: it stamps provenance (peer-credential-derived origin + daemon-minted
ingress timestamp) onto the submission, sends `signal_message::Input` to
router, and translates `signal_message::Output` back to the daemon-local schema
`Output`. Provenance is never encoded as strings and never accepted from the
caller payload.

## 2 · State and Ownership

The message component owns no durable message state. The CLI requires
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
check passes, it stamps the configured `owner_name` as this daemon's local
harness component instance and forwards typed provenance in
`StampedMessageSubmission`; other peer uids stamp `NonOwnerUser(uid)`. The
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

Actor registration, actor listing, pending delivery, retry, delivery results,
and message ledger state are router or engine-manager concerns, not message
state.

## 3 · Boundaries

This repo owns:

- NOTA parsing for the `message` command;
- NOTA parsing for the `meta-message` command;
- projection from NOTA `Send` / `Inbox` to `signal-message`;
- projection from NOTA `meta-signal-message` requests to owner meta frames;
- projection from `signal-message` replies back to NOTA;
- projection from `meta-signal-message` replies back to NOTA;
- length-prefixed Signal frame transport from CLI to `message.sock`;
- length-prefixed meta-signal frame transport from `meta-message` to the owner
  meta socket;
- frame-level exchange echoing for the current one-operation request/reply
- daemon stamping from `MessageSubmission` to `StampedMessageSubmission`
  using the accepted connection's peer credentials;
- daemon forwarding from `message.sock` to the configured router socket.

This repo does not own:

- message or router contract definitions;
- final routing policy;
- durable database tables;
- actor registration writes;
- local message ledgers;
- terminal endpoint vocabulary;
- terminal byte transport;
- durable daemon state.

## 4 · Invariants

- The CLI accepts exactly one NOTA input record.
- The CLI prints exactly one NOTA reply record.
- The `meta-message` CLI accepts exactly one `meta-signal-message` NOTA request.
- The `meta-message` CLI prints exactly one `meta-signal-message` NOTA reply.
- Supported input variants are `Send` and `Inbox`.
- The message daemon socket is mandatory for the CLI.
- The owner meta socket is mandatory for the `meta-message` CLI.
- The router socket is mandatory for the daemon.
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
- Provenance is typed in `StampedMessageSubmission`; the daemon mints it in
  `RouterForwarder::stamp` from `ConnectionContext` (`SO_PEERCRED`) and daemon
  configuration. A matching peer uid stamps the configured `owner_name` as a
  local harness component instance; any other peer uid stamps
  `NonOwnerUser(uid)`. Provenance is never accepted from the CLI payload.
- The generated `NexusEngine::execute` surface owns the typed decision entry.
  Message's component code supplies the decision implementation and explicitly
  sequences its single `ForwardToRouter` effect. There is no hand-written
  recursive Nexus loop, no retired `ForwardCompleted` vocabulary, and no local
  continuation budget in the component.
- The component does not write local message or pending logs.
- The daemon hand-writes only `impl ComponentDaemon for MessageDaemon`
  (`src/daemon.rs`): `Configuration` / `Engine` / `Error` / `PROCESS_NAME` +
  `build_runtime` + `handle_working_input`. The daemon spine, request gate,
  accept loop, and lifecycle are emitted by schema-rust-next into the shared
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
schema/nexus.schema            internal-feature catalog (forward decision + effect)
schema/sema.schema             durable state plane — honestly empty (Stateless)
build.rs                       GenerationPlan + NexusDaemonShape (emits src/schema/*.rs)
src/schema/signal.rs           generated Signal plane
src/schema/nexus.rs            generated Nexus plane (NexusEngine)
src/schema/sema.rs             generated SEMA plane (SemaEngine)
src/schema/daemon.rs           EMITTED daemon skeleton (ComponentDaemon, the spine)
src/main.rs                    message CLI entry
src/bin/meta_message.rs        owner meta CLI entry
src/bin/message_daemon.rs      daemon entry (one-liner: MessageDaemon::run_to_exit_code())
src/bin/message_validate_output.rs test/debug validator for message CLI NOTA replies
src/config.rs                  binary rkyv daemon Configuration (impl BindingSurface)
src/daemon.rs                  impl ComponentDaemon for MessageDaemon (the only daemon code)
src/engine.rs                  MessageEngine + request-scoped generated Nexus runner hooks
src/meta.rs                    meta-message client, codec, command, and skeleton replies
src/frame_bytes.rs             preserved length-prefixed frame bytes for signal_channel contracts
src/command.rs                 CLI NOTA input/output projection
src/output_validator.rs        structured validator for sandbox message artifacts
src/router.rs                  RouterForwarder + signal-message contract client/codec
src/surface.rs                 message-local NOTA surface records
src/error.rs                   crate error enum
tests/process_boundary.rs      emitted daemon over a real socket
tests/forward_to_router.rs     Nexus forward effect against a stub router
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
