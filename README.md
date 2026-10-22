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
  `~/.local/state/message/message.memory`; sockets
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


## Codex MCP sender

The workspace installs `message-mcp-send` beside the ordinary Message tools.
It is an MCP stdio process, launched directly by the selected Codex app-server.
Its only tool, `send`, accepts a `query` containing an ordinary `Send` datom.
The native thread comes exclusively from the call's `params._meta.threadId`,
never from tool arguments, environment variables, a caller PID, or a model's
claimed sender. The adapter converts this to the producer-owned `SendThread`
operation and connects directly to Message; it does not launch a CLI child.
Message reads the socket peer from the kernel, then asks Flow IdentifyThread.
Flow must have the actual app-server/MCP executable pair configured and the
thread recorded as the unique live Awake Codex flow. This does not adopt an
unregistered legacy thread or provide adversarial same-user isolation.

The deployment owner registers the package's `bin/message-mcp-send` as a
stdio `mcp_servers` command using a derivation reference and configures the
same executable identity in Flow. A wrapper or intermediary process that
changes the direct parent must be qualified, not accepted by weakening Flow.
No credential copy or additional server is required. Missing thread metadata
refuses before connection. An uncertain response is reported without retry.
The fresh native process-tree, tool metadata and sender-attribution witnesses
remain deployment gates; source tests cannot establish those properties.
