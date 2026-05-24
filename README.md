# Message

`message` is the engine's message-ingress component. It owns the
`message` CLI and the `message` daemon (binary:
`message-daemon`), the supervised first-stack component.

The `message` binary accepts exactly one NOTA input record, validates it
through Rust types, sends one length-prefixed `signal-message` frame
to `message` on the engine's user-writable socket
(`message.sock`, mode 0660), reads one typed reply frame, and prints one
NOTA reply.

The `message` daemon (binary file `message-daemon`) is the
engine's user-writable ingress boundary: it binds `message.sock` (mode 0660,
engine-owner group), stamps `MessageSubmission` frames with SO_PEERCRED-
derived origin and ingress time, then forwards `StampedMessageSubmission`
frames to `persona-router` over the internal `router.sock`. No durable
state; no local message ledger.

The supported input records are:

```sh
MESSAGE_SOCKET=/run/persona/engine-main/message.sock \
  message "(Send designer [Need a layout pass.])"

MESSAGE_SOCKET=/run/persona/engine-main/message.sock \
  message "(Inbox designer)"
```

The message component does not construct in-band proof material, read a local
actor index, or write message ledgers, pending logs, terminal endpoints, or
actor-registration files. Origin stamping is typed contract data at the
daemon/router ingress boundary, not a caller-provided proof.

Durable message acceptance, pending delivery, retry, owner approval, and
terminal delivery state belong to `persona-router` and its downstream
`persona-harness` / `persona-terminal` path. This crate owns text projection at
the edge: NOTA in, Signal out, Signal in, NOTA out.
