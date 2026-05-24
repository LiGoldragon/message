# message — architecture

*Engine message ingress / text boundary. Owns the
`message` CLI and the `message` daemon (binary:
`message-daemon`), the supervised first-stack
component.*

`message` owns two binaries:

- The `message` CLI — one NOTA in, one NOTA out. Validates a
  user-typed NOTA record through Rust types, projects to a
  `signal-message` frame, sends it to `message`
  on the engine's user-writable socket (`message.sock`, mode
  0660), reads one reply frame, prints the NOTA reply.
- The `message` daemon (binary file:
  `message-daemon`) — a small Kameo daemon supervised
  by `persona-daemon` as the engine's message ingress
  component. Reads a typed `MessageDaemonConfiguration` record
  passed by argv via `nota-config`; binds `message.sock` at the
  configured mode (0660 with the engine-owner group in
  production) and stamps `MessageSubmission` frames with the
  configured owner identity, SO_PEERCRED-derived origin, and
  ingress time; forwards `StampedMessageSubmission` frames to
  `persona-router`'s internal socket (`router.sock`, 0600).

There is no `MessageProxy` component here. The supervised
first-stack component is named `message`; the long-lived
binary is `message-daemon`.

> **Scope.** Any "sema" reference in this doc means today's `sema`
> library (rename pending → `sema-db`). The eventual `Sema` is broader; today's
> message is a realization step. See `~/primary/ESSENCE.md` §"Today and
> eventually".

---

## 0 · TL;DR

This repo owns the engine's message-ingress boundary: a
small supervised daemon plus a CLI client. Neither carries
a durable message ledger; both are stateless boundary
surfaces. Routing policy, delivery state, and channel
authority remain in `persona-router`.

```mermaid
flowchart LR
    "human or harness" -->|"one NOTA Send or Inbox"| "message CLI"
    "message CLI" -->|"length-prefixed signal-message frame"| "message"
    "message" -->|"StampedMessageSubmission"| "persona-router"
    "persona-router" -->|"length-prefixed reply frame"| "message"
    "message" -->|"length-prefixed reply frame"| "message CLI"
    "message CLI" -->|"one NOTA reply"| "human or harness"
```

## 1 · Component Surface

`message` exposes:

- a `message` binary;
- a `message-daemon` binary;
- NOTA `Send` and `Inbox` input records;
- one length-prefixed `signal-message` request frame per CLI invocation;
- one daemon-bound `message.sock` for owner ingress;
- one router client path to internal `router.sock`;
- one NOTA reply projection per invocation;
- no caller-provided identity and no local actor index.

## 1.5 · Daemon actor topology

```mermaid
flowchart TB
    "MessageDaemonRoot" --> "MessageDaemonConnection"
    "MessageDaemonConnection" --> "SignalRouterClient"
    "SignalRouterClient" --> "persona-router"
```

The current slice has one data-bearing Kameo root actor:
`MessageDaemonRoot { router, stamper, forwarded_count }`. Connection handling,
SO_PEERCRED extraction, origin stamping, and the router client are ordinary
data-bearing types for now. The next actor split is supervision, listener,
origin stamping, and router-client actors. The daemon is stateless across CLI
requests — no redb, no durable message ledger.

The CLI surface (`message` binary) connects to `message.sock` like any other
client. The daemon converts client-side `MessageSubmission` into
router-side `StampedMessageSubmission` by attaching typed provenance from the
typed `MessageDaemonConfiguration`'s `owner_identity`, kernel peer
credentials, and a daemon-minted ingress timestamp. It does not encode
provenance as strings and it does not infer engine ownership from its own
effective uid; the owner identity is supplied by the persona manager via the
typed configuration record. The `geteuid()`-based stamper constructor
(`MessageOriginStamper::for_current_user`) is a test-only and standalone-launch
affordance, never reached on the supervised production path.

## 2 · State and Ownership

The message component owns no durable message state. The CLI requires
`MESSAGE_SOCKET` or `PERSONA_SOCKET_PATH` and exits if the message
daemon socket is absent. The daemon requires a typed `MessageDaemonConfiguration`
on argv whose `router_socket_path` names the router's internal socket; it
exits at decode time if the configuration is missing or malformed.

Caller identity is not accepted from the model or CLI payload.
`MessageSubmission` and `InboxQuery` stay sender-free, and the component sends
no in-band proof material. The daemon stamps message submissions from the
configured `OwnerIdentity` plus SO_PEERCRED and forwards typed provenance in
`StampedMessageSubmission`. The persona manager builds the configuration
record from the engine's spawn envelope and writes it to a NOTA file on
spawn; the daemon never reads environment variables for control-plane
settings.

Typed-configuration-via-argv is the destination shape: every control-plane
setting (socket paths, socket modes, owner identity, supervision socket,
router socket) arrives as a typed `MessageDaemonConfiguration` field decoded
by `nota-config::ConfigurationSource::from_argv`. The residual
`from_environment` constructors on `SignalRouterSocket`, `SupervisionListener`,
and the peer-socket enumeration helpers are transitional dead code, unreached
on the production path and retiring on the next refactor.

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
  path;
- daemon stamping from `MessageSubmission` to `StampedMessageSubmission`;
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
- The daemon applies the configured socket mode from
  `MessageDaemonConfiguration` to `message.sock` before accepting client
  traffic.
- The daemon reads its typed `MessageDaemonConfiguration` from argv before
  accepting message ingress, and `External(Owner)` is derived from the
  configured `owner_identity` rather than `message-daemon`'s own
  uid.
- CLI and daemon outbound traffic are length-prefixed rkyv Signal frames.
- Request/reply matching is frame-level: every request frame carries an
  `ExchangeIdentifier`, and every reply frame echoes the same identifier.
- The current message ingress path is deliberately one operation per request.
  Multi-operation request execution belongs in the shared Signal runtime slice,
  not in this component's ad hoc codec.
- A mismatched outer Signal verb and request payload is rejected as typed
  `RequestRejectionReason`, not by string parsing.
- Sender identity is absent from the CLI payload and absent from frame auth.
- Provenance is typed in `StampedMessageSubmission`; the daemon mints it from
  SO_PEERCRED and never accepts it from the CLI payload.
- The component does not write local message or pending logs.
- The daemon root is a data-bearing Kameo actor.
- The component depends on the stable Persona Kameo lifecycle reference, not
  crates.io Kameo and not a raw revision pin.
- A graceful supervision stop exits the daemon, stops `MessageDaemonRoot` with
  a clean terminal outcome, releases the `message.sock` binding, and rejects
  later CLI ingress.
- The production daemon reads no environment variables for control-plane
  configuration. Test fixtures may opt in via an explicit named env var, per
  the `nota-config` test-shim discipline. Witness: a source scan forbids
  env-var reads in the daemon binary and daemon runtime sources.

## Code Map

```text
src/main.rs                    message CLI entry
src/bin/message_daemon.rs daemon entry
src/bin/message_validate_output.rs test/debug validator for message CLI NOTA replies
src/command.rs                 NOTA input/output projection
src/daemon.rs                  daemon listener and data-bearing Kameo root
src/output_validator.rs        structured validator for sandbox message artifacts
src/router.rs                  Signal frame clients and codec
src/surface.rs                 message-local NOTA surface records
src/error.rs                   crate error enum
tests/                         ingress and architectural-truth tests
```

## Constraint Tests

| Constraint | Test |
|---|---|
| The router Signal path cannot create a local message ledger. | `nix build .#checks.x86_64-linux.message-cli-sends-router-signal-without-local-ledger` |
| Inbox reads come from the router, not a local ledger. | `nix build .#checks.x86_64-linux.message-cli-inbox-uses-router-signal-not-local-ledger` |
| The message daemon socket is mandatory for the CLI. | `nix build .#checks.x86_64-linux.message-cli-requires-message-socket` |
| The daemon applies the configured socket mode. | `nix build .#checks.x86_64-linux.message-daemon-applies-configured-socket-mode` |
| The daemon stamps and forwards CLI Signal frames to the router socket. | `nix build .#checks.x86_64-linux.message-daemon-forwards-cli-signal-frame-to-router-socket` |
| The daemon root stamps owner identity from the typed configuration, not the CLI payload. | `nix build .#checks.x86_64-linux.message-daemon-root-stamps-owner-identity-from-configuration` |
| The component uses the stable Persona Kameo lifecycle reference. | `nix build .#checks.x86_64-linux.message-component-uses-stable-kameo-lifecycle-reference` |
| The daemon root shutdown returns a terminal outcome. | `nix build .#checks.x86_64-linux.message-daemon-root-shutdown-returns-terminal-outcome` |
| Graceful daemon stop releases `message.sock` and rejects later ingress. | `nix build .#checks.x86_64-linux.message-daemon-graceful-stop-releases-message-socket-and-rejects-ingress` |
| The production daemon reads no environment variables for control-plane configuration. | `nix build .#checks.x86_64-linux.message-daemon-reads-no-control-plane-environment-variables` |
| Mismatched Signal verb/payload pairs are rejected by typed Signal reason. | `nix build .#checks.x86_64-linux.message-frame-codec-rejects-mismatched-signal-verb` |
| The component does not construct in-band proof material. | `nix build .#checks.x86_64-linux.message-component-cannot-own-local-ledger` |
| Retired terminal-brand vocabulary cannot return. | `nix build .#checks.x86_64-linux.message-runtime-cannot-reference-retired-terminal-brand` |
| Local ledger and endpoint surfaces cannot return. | `nix build .#checks.x86_64-linux.message-component-cannot-own-local-ledger` |

## See Also

- `../signal-message/ARCHITECTURE.md`
- `../persona-router/ARCHITECTURE.md`
- `../signal-persona/ARCHITECTURE.md`
