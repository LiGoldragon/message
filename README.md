# message

The Message Nexus: durable messages and receipts, delivered through Flow.

A message is just a message: `Send.{ [ recipients ] Priority Content }`.
Message owns the record, the Priority, the sender (named from the peer through
Flow's `ResolvePeer`, never from the payload), the ledger of receipts with every
grade kept separate, parking until Flow shows a recipient at rest, and the
recipient's own Read. Flow is the only pane writer: Message reaches a pane only
through Flow's typed `Deliver`, and never runs Herdr.

## Executables

- `message-nexus` — the Nexus. Starts with no arguments. Store
  `~/.local/state/message/message.sema`; sockets
  `$XDG_RUNTIME_DIR/message/message.sock` (ordinary) and `message-owner.sock`
  (meta), both `0600`; it reaches Flow at `$XDG_RUNTIME_DIR/flow/flow.sock` and
  `flow-meta.sock` until meta `Configure` says otherwise.
- `message` — one inline `signal-message` datom (socket `MESSAGE_SOCKET`).
- `message-meta` — one inline `meta-signal-message` datom (socket
  `MESSAGE_META_SOCKET`).

```sh
message 'Send.{ [ 7d41e0 ] Soft Text.«Stage 1 is deployed; run the tier tests.» }'
# Submitted.{ m-18a8… [ { 7d41e0 NotRequested Parked } ] }
message 'Observe.m-18a8…'          # Receipts on open, then ReceiptObserved per change
message 'Acknowledge.m-18a8…'      # from the recipient's own pane: Read
message-meta 'Send.{ [ 7d41e0 ] HardAbrupt Text.«Stop the build.» }'   # stamped Owner
message-meta 'Redeliver.{ m-18a8… 7d41e0 }'                            # out of Uncertain
```

## Delivery

Send vets every recipient with Flow's `Vet` (the first refusal fails the whole
Send), records the message and a `Submitted` receipt per recipient, then calls
Flow's `Deliver` per recipient with DeliveryId `<MessageId>:<FlowId>:<attempt>`.
`RecipientWorking` (Soft) and `ComposerOccupied` (any Priority) park the
recipient: Message holds Flow's `Observe.Agent` for it and delivers again, under
the same DeliveryId, on each frame showing Idle, Done or Gone. Flow keeps a
typed delivery by its DeliveryId, so a repeat after a crash types nothing. Only
`Redeliver`, out of `Uncertain`, opens a new attempt. A restarted Nexus resumes
every recipient still `Submitted` or `Parked`.

Run `nix flake check -L` for the complete proof matrix.
