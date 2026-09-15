# UPGRADES

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
   written by 0.11.1 cannot be re-stamped and read — the daemon **fails
   closed**. An offline preservation tool is available for a copied v3 file;
   it does not make those archived rows active under the new contract.

### Deploying

Deployment is a CriomOS-home flake pin advance and is **not** performed by
this repository. Whoever advances that pin must, in order:

1. Stop the `message-daemon` user unit. A running 0.11.1 daemon holds the v3
   store open.
2. Copy the stopped v3 store, then run
   `message-inspect-v3-store --source <copied-v3-store>` to confirm its
   schema and row counts without printing payloads. Run
   `message-migrate-v3-store --source <copied-v3-store> --destination <new-v5-store>`.
   The migration refuses any schema other than v3, an unknown durable table,
   a pre-existing destination, or a pre-existing backup. It leaves the source
   untouched, writes an exact `<new-v5-store>.v3-backup`, and writes raw v3
   records to `legacy_v3_archive` in the fresh v5 store. Those records remain
   historical evidence: the daemon does not deserialize them and the legacy
   `delivery_outbox` is never treated as FlowDeliver work.
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
