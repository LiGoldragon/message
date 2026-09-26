# UPGRADES

## 0.15.0 -> 0.16.0 -- the letter names the message it may be acknowledged by

Repins on meta-signal-flow 10.0.0 (cbea31e), signal-message 7.0.0 (63e11b4)
and meta-signal-message 0.7.1 (18bf4af). 0.15.0 was never deployed, so this
supersedes it rather than following it in the field.

- `Letter` gained `MessageId` as its first position, so the pane text Flow
  types is now `Soft.{ m-7f3a2c Flow.e167d8 Text.«...» }`. Message fills it
  from the record's own id -- the one `Acknowledge` takes -- so a recipient
  reading its pane can answer Read from what it read and nothing else. Before
  this the letter named its sender and its content and no message, and no
  recipient could Acknowledge at all.
- `MessageId` is now declared in meta-signal-flow, where Flow renders it, and
  imported here; `signal_message::MessageId` still names it.
- Nothing else on Message's wire, ledger or store changed.

## 0.14.0 -> 0.15.0 -- Message through Flow (e167d8 stage S2)

A rewrite onto signal-message 6.0.0, meta-signal-message 0.7.0, signal-flow
7.0.0 and meta-signal-flow 9.0.0. Message no longer writes panes: every
delivery is Flow's `Deliver`, over Flow's meta socket.

### What breaks

- Executables: `message-daemon`, `meta-message`, `relay`, `message-cluster`,
  `message-validate-output` and `message-write-configuration` are gone. The
  Nexus is `message-nexus` with **no arguments**; the meta CLI is
  `message-meta`.
- Store: `~/.local/state/message/message.sema`, new record kinds. The old
  `messenger.sema` is never opened; move it aside, nothing is migrated.
- Wire: the whole ordinary and meta vocabulary (see the contracts' UPGRADES).
  Inbox, threads, the agent registry, the cluster relay and the flow-delivery
  park are retired.
- Flow must admit Message on its meta socket: run Message outside any flow's
  pane (it then resolves as the owner), and configure Flow's
  `MessageNexusPath` to the `message-nexus` executable.


## 0.13.0 -> 0.14.0 -- Flow resolution repinned on signal-flow 6.2.0

`message` reached Flow Nexus over `signal-flow` **1.1.0** (`968ae3b0`) while
the deployed Flow Nexus spoke 5.1.0, five breaking wire versions apart: the
rkyv archive `FlowResolver` wrote could not be read by the Nexus it was
sending it to, so Flow-resolved delivery could not have been working. This
release pins the current ordinary Flow contract, signal-flow 6.2.0
(`e9e243c75cce0ff3534149b6ff43af06d97c60db`), which is what Flow 0.14.0
serves.

### What changes

- `signal-flow` moves from `968ae3b0` (1.1.0) to `a8cdd5cc` (6.1.0). Nothing
  in this repository's own wire, store or command-line text changes.
- The eight `signal_flow` names `nexus_delivery` uses -- `EndpointSelection`,
  `FlowNode`, `HarnessKind`, `HerdrRoute`, `HerdrRouteSelection`, `Query`,
  `Response`, `RouteReadiness` -- are the same in 6.1.0, so no source change
  was needed. The repin compiles and the whole suite passes unchanged.
- `FlowLifecycle` gained `Retired` in 6.0.0 and `Exited` in 6.2.0. `message`
  only ever constructs `Active` in its fixtures and never matches the
  lifecycle exhaustively, so the new variants reach it only as values Flow
  itself already refuses to resolve as a recipient.

### Operationally

`message-daemon` must be deployed together with Flow 0.14.0. A `message` on
this pin cannot talk to a Flow Nexus older than 0.14.0, and the old pin could
not talk to any Flow Nexus in service.

## 0.11.1 → 0.12.0 — Datom stack, new wire, new store schema

This release changes the wire, the command-line text, and the durable store
format. `message-daemon` is a deployed CriomOS-home user unit, so all three
matter operationally.

### What breaks

1. **The socket wire.** Both listeners now carry a 4-byte big-endian length
   prefix over the *bare* rkyv archive of the producer contract root
   (`signal_message::Query` / `Response`, `meta_signal_message::Query` /
   `Response`). The `signal-frame` envelope is gone. Old peers cannot talk to
   a new daemon, and new peers cannot talk to an old one.
2. **The command-line text.** Every CLI takes one inline **Datom** value
   instead of a Dotos value. A body containing spaces is written in
   guillemets: `«like this»`.
3. **The durable store.** `MESSENGER_SCHEMA_VERSION` moves 3 → 4 and the
   additive-migration list is **empty**. Every durable record embeds
   producer-owned contract types whose archived layout changed, so a store
   written by 0.11.1 cannot be re-stamped and read — it **fails closed**.

### Deploying

Deployment is a CriomOS-home flake pin advance and is **not** performed by
this repository. Whoever advances that pin must, in order:

1. Stop the `message-daemon` user unit. A running 0.11.1 daemon holds the v3
   store open.
2. Move the existing store aside. Its path is the `database_path` in the
   daemon's binary configuration — by default `messenger.sema` in the
   component's state directory. The daemon will not read it; a v3 file makes
   startup fail with a schema-version mismatch rather than silently
   misinterpreting rows. There is no in-place migration and none is intended:
   the record layout changed underneath, so re-stamping would corrupt.
   Message history in that file is not carried forward. Keep the file if it
   matters; nothing will read it.
3. Rewrite the binary configuration with the new
   `message-write-configuration`, whose one inline Datom argument is now
   shaped
   `{ { <message-socket> <mode> <supervision-socket> <mode> <router-socket> [ <ingresses> ] <owner-identity> } <database-path> <owner-label> <output-path> }`.
   The previous parenthesised form is refused outright.
4. Advance the pin and start the unit. Confirm both sockets appear.
5. Update every peer that speaks either socket in the same step. A peer on the
   old envelope will not interoperate; there is deliberately no compatibility
   path.

### Not changed

The terminal-cell programmatic-input leg (`'P'` + u64 big-endian length +
text) and the harness delivery leg's 4-byte prefix keep their framing. The
terminal leg's payload text is now Datom rather than Dotos.

### Still missing, and relevant to operators

`message` does not forward submissions to the router socket. The
configuration carries `router_socket_path` and the contract carries
`SubmitStamped` and `StampedMessageSubmission`, but no revision of this
repository has ever contained forwarding code; `Query::Submit` writes the
local ledger and answers `SubmissionAccepted`, and `Query::SubmitStamped`
answers `MessageRequestUnimplemented(NotInPrototypeScope)`. Do not expect a
deployed daemon to feed a router.
