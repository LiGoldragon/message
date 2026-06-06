# message skill

Work here when the change concerns the `message` CLI, `message-daemon`,
NOTA message projection, message ingress, or real harness message tests.

Rules for work here:

- Keep the repo at the human/harness text boundary. Message binary records
  belong in `signal-message`.
- `message` sends length-prefixed rkyv Signal frames to
  `message-daemon` through `MESSAGE_SOCKET` and prints one NOTA
  reply projection.
- `message-daemon` binds the supervised `message.sock`, stamps
  `MessageSubmission` frames into `StampedMessageSubmission`, forwards typed
  frames to `persona-router`, and owns no durable message state.
- The component must not write local message ledgers, pending logs, or
  actor-registration files. Router-owned Sema tables are the durable message
  owner.
- Do not trust sender fields written by a model. The component does not include
  a sender field, read a local actor index, resolve process ancestry, or
  construct in-band proof material. Origin stamping is typed data minted from
  SO_PEERCRED plus daemon configuration, not a string field from the caller.
  A trusted owner peer stamps the configured local harness instance; other
  peers stamp `NonOwnerUser(uid)`.
- Supported input variants are `Send` and `Inbox`. Registry, listing, retry,
  tail, and delivery operations belong to router, mind, harness, or terminal
  surfaces as their contracts land.
- Do not add a local ledger fallback, terminal endpoint vocabulary, or router
  line-protocol fallback here.
- Rebuild stateful harness workflows through `persona-router`,
  `persona-harness`, `persona-terminal`, and typed Signal contracts.

Use component-to-component rkyv frames through relation-specific Signal
contracts when the CLI or daemon crosses into router/store territory. Use NOTA
only at CLI, harness, and audit projection boundaries.
