# Message

Message is the behavioral consumer of the ordinary and owner Message
interfaces. `signal-message` owns every public ordinary Type and
`meta-signal-message` owns every public owner Type; this repository imports
those Types by identity.

It provides:

- `message`, a one-value Dotos client for the ordinary interface;
- `meta-message`, a one-value Dotos client for the owner interface;
- `message-daemon`, the two-listener runtime;
- `message-write-configuration`, a Dotos-to-binary startup helper;
- `messenger.sema`, the bounded durable ledger, inbox, thread index, agent
  registry, and delivery outbox.

The daemon receives one binary configuration path as its only argument. The
ordinary CLI connects through `MESSAGE_SOCKET`; the owner CLI connects through
`MESSAGE_META_SOCKET`. Both CLIs accept exactly one inline Dotos value and
print the producer-owned reply in Dotos.

There is no component-local structural language, generated Rust, build script,
frame model, or compatibility vocabulary. The producer contracts are the
surface seen by humans, agents, harnesses, and GUIs; Message supplies the
behavior behind them.
