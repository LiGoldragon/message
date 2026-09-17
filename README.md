# Message

Message is the behavioral consumer of the ordinary and owner Message
interfaces. `signal-message` owns every public ordinary Type and
`meta-signal-message` owns every public owner Type; this repository imports
those Types by identity.

It provides:

- `message`, a one-value Datom client for the ordinary interface (argv or stdin);
- `message-meta`, the privileged one-value Datom client;
- `message-nexus`, the two-listener runtime;
- `message-write-configuration`, a Datom-to-binary startup helper;
- `messenger.sema`, the bounded durable ledger, inbox, thread index, agent
  registry, delivery outbox, and event-scoped Nexus receipt store.

`Deliver` carries `ClusterMessage.Peer` or `ClusterMessage.Relay` through the
ordinary socket. The Nexus resolves each target through Flow Nexus, persists
the source-event identity and target attempt before crossing a harness
boundary, and returns `DeliveryRecorded` with typed recipient receipts. Claude
delivery uses the daemon attach protocol; Codex delivery uses app-server
`turn/start`. Both receive the canonical ClusterMessage Datom directly.

The compatibility binary names `meta-message` and `message-daemon` remain
available during deployment migration. There is no cluster delivery wrapper
or adapter CLI. The Nexus receives one binary
configuration path as its only argument. The
ordinary CLI connects through `MESSAGE_SOCKET`; the owner CLI connects through
`MESSAGE_META_SOCKET`. Both CLIs accept exactly one inline Datom value and
print the producer-owned reply in Datom.

The wire is a 4-byte big-endian length prefix over the bare rkyv archive of
the producer-owned contract root — no envelope, because one connection carries
one request and one reply. The portable frame itself comes from `signal`, so
every component speaks one frame type.

There is no component-local structural language, generated Rust, build script,
frame model, or compatibility vocabulary. The producer contracts are the
surface seen by humans, agents, harnesses, and GUIs; Message supplies the
behavior behind them.
