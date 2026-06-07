# message — architecture

*Engine message ingress / text boundary. Owns the
`message` CLI and the `message` daemon (binary:
`message-daemon`), the supervised first-stack
component.*

`message` owns two binaries:

- The `message` CLI — one NOTA in, one NOTA out. Validates a
  user-typed NOTA record through Rust types, projects to a
  schema-derived signal frame, sends it to `message`
  on the engine's user-writable socket (`message.sock`, mode
  0660), reads one reply frame, prints the NOTA reply.
- The `message` daemon (binary file: `message-daemon`) — a
  schema-derived triad daemon on the emitted `triad-runtime`
  runtime. The daemon skeleton is EMITTED into
  `src/schema/daemon.rs` from the `NexusDaemonShape` in
  `build.rs`; message hand-writes only `impl ComponentDaemon for
  MessageDaemon`. It reads a binary rkyv `Configuration` from its
  single argv argument (socket path, router socket path, database
  path, owner name, owner uid), binds `message.sock`, decodes schema-derived
  signal frames, runs the Nexus forward decision, and forwards the
  translated request to `router`'s internal socket
  (`router.sock`) over the `signal-message` wire as a
  `StampedMessageSubmission`.

There is no `MessageProxy` component here. The supervised
first-stack component is named `message`; the long-lived
binary is `message-daemon`.

> **Scope.** Any "sema" reference in this doc means today's `sema`
> library (rename pending → `sema-db`). The eventual `Sema` is broader; today's
> message is a realization step. See `~/primary/ESSENCE.md` §"Today and
> eventually".

## 0 · TL;DR

This repo owns the engine's message-ingress boundary: a
small supervised daemon plus a CLI client. Neither carries
a durable message ledger; both are stateless boundary
surfaces. Routing policy, delivery state, and channel
authority remain in `router`.

```mermaid
flowchart LR
    "human or harness" -->|"one NOTA Send or Inbox"| "message CLI"
    "message CLI" -->|"length-prefixed schema signal frame"| "message"
    "message" -->|"StampedMessageSubmission"| "router"
    "router" -->|"length-prefixed reply frame"| "message"
    "message" -->|"length-prefixed reply frame"| "message CLI"
    "message CLI" -->|"one NOTA reply"| "human or harness"
```

## 1 · Component Surface

`message` exposes:

- a `message` binary;
- a `message-daemon` binary;
- NOTA `Send` and `Inbox` input records;
- one length-prefixed schema signal request frame per CLI invocation;
- one daemon-bound `message.sock` for working ingress;
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
  decision plus the `ForwardToRouter` effect. `Submit`/`QueryInbox` stamp and
  forward; `SubmitStamped` replies `Unimplemented` (the daemon mints provenance,
  never accepts it from a peer).
- **SEMA** — honestly empty (`Stateless`). Message owns no durable state; the
  plane exists only to satisfy the uniform three-plane shape.

The emitted daemon (`src/schema/daemon.rs`) owns the argv-config load, the
single working-socket bind, and the decode → `handle_working_input` → encode
spine. `MessageDaemon` (`src/daemon.rs`) supplies only the `ComponentDaemon`
escape hatches. The daemon is single-listener (no meta tier) and stateless
across requests — no redb, no durable message ledger.

`RouterForwarder` (`src/router.rs`) is the translation seam between the
schema-derived inbound wire and the router's `signal-message` wire: it stamps
provenance (peer-credential-derived origin + daemon-minted ingress timestamp)
onto the submission and translates the router's reply back to the schema
`Output`. Provenance is never encoded as strings and never accepted from the
caller payload.

## 2 · State and Ownership

The message component owns no durable message state. The CLI requires
`MESSAGE_SOCKET` or `PERSONA_SOCKET_PATH` and exits if the message
daemon socket is absent. The daemon requires a typed binary rkyv
`Configuration` on argv (socket path, router socket path, database path,
owner name) whose `router_socket_path` names the router's internal socket;
the emitted spine exits at decode time if the configuration is missing or
malformed.

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
the CLI's socket discovery; the daemon path reads no environment variables.

Actor registration, actor listing, pending delivery, retry, delivery results,
and message ledger state are router or engine-manager concerns, not message
state.

## 3 · Boundaries

This repo owns:

- NOTA parsing for the `message` command;
- projection from NOTA `Send` / `Inbox` to `signal-message`;
- projection from `signal-message` replies back to NOTA;
- length-prefixed Signal frame transport from CLI to `message.sock`;
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
- Supported input variants are `Send` and `Inbox`.
- The message daemon socket is mandatory for the CLI.
- The router socket is mandatory for the daemon.
- The daemon is single-listener: the emitted spine binds one working
  `message.sock` from the `NexusDaemonShape` in `build.rs`. Message has no
  meta tier.
- The daemon uses the emitted `triad-runtime` `ActorSingleListenerDaemon` spine
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
- The generated `NexusEngine::execute` runner owns the recursive Nexus loop and
  continuation budget. Message's component code supplies only one decision step
  plus the `ForwardToRouter` effect hook through a request-scoped wrapper that
  carries `ConnectionContext`.
- The component does not write local message or pending logs.
- The daemon hand-writes only `impl ComponentDaemon for MessageDaemon`
  (`src/daemon.rs`): `Configuration` / `Engine` / `Error` / `PROCESS_NAME` +
  `build_runtime` + `handle_working_input`. The daemon spine, request gate,
  accept loop, and lifecycle are emitted by schema-rust-next into the shared
  actor-native triad-runtime shell.
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
src/bin/message_daemon.rs      daemon entry (one-liner: MessageDaemon::run_to_exit_code())
src/bin/message_validate_output.rs test/debug validator for message CLI NOTA replies
src/config.rs                  binary rkyv daemon Configuration (impl DaemonConfiguration)
src/daemon.rs                  impl ComponentDaemon for MessageDaemon (the only daemon code)
src/engine.rs                  MessageEngine + request-scoped generated Nexus runner hooks
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
| The daemon stamps an owner-peer submission as the configured harness component instance from SO_PEERCRED + config and forwards it to the router, translating the acceptance back. | `nix build .#checks.x86_64-linux.message-daemon-stamps-owner-submission-to-router` |
| The daemon stamps a non-owner peer as `NonOwnerUser(uid)`, proving peer credentials survive the generated-runner hook path. | `nix build .#checks.x86_64-linux.message-daemon-stamps-non-owner-submission-to-router` |
| A router-unreachable forward yields a typed `Error` output. | `nix build .#checks.x86_64-linux.message-router-unreachable-yields-typed-error` |
| Message's generated Nexus plane owns the runner adapter, and component code cannot reintroduce a local recursive Nexus loop. | `nix build .#checks.x86_64-linux.message-nexus-loop-is-generated` |
| The production daemon reads no environment variables for control-plane configuration. | `nix build .#checks.x86_64-linux.message-daemon-reads-no-control-plane-environment-variables` |
| Local ledger, actor index, in-band proof, and endpoint surfaces cannot return. | `nix build .#checks.x86_64-linux.message-component-cannot-own-local-ledger` |
| Retired terminal-brand vocabulary cannot return. | `nix build .#checks.x86_64-linux.message-runtime-cannot-reference-retired-terminal-brand` |
| The whole surviving test suite passes. | `nix build .#checks.x86_64-linux.default` |

## See Also

- `../signal-message/ARCHITECTURE.md`
- `../router/ARCHITECTURE.md`
- `../signal-persona-origin/ARCHITECTURE.md`
