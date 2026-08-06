# Agent Instructions — Message

## Center

Message executes the public Types owned by `signal-message` and
`meta-signal-message`. Those producers are the structural authority. This
component owns behavior only: durable messenger state, provenance, delivery,
the ordinary and owner listeners, and Dotos command surfaces.

Protos is the universal programming medium. Ethos and Dotos are how humans,
agents, harnesses, and interfaces perceive code and data. Complete
self-hosting and substrate replacement are expected outcomes, so component
logic must not assume Rust, LLVM, or the current operating system is permanent.
Beauty and elegant, extensible logic win every trade-off.

## Invariants

- Consume the exact producer heads directly. Do not copy, rename, wrap, or
  re-export individual producer Types as a component vocabulary.
- Do not add component-local structural inputs, generated Rust, build-time
  generation, daemon-shape policy, or a second Signal/Nexus/SEMA contract.
- The bootstrap generator is provisional: strict producer Types are generated;
  current behavior and roles are handwritten until Logos can express them.
- Public text is Dotos. Do not restore retired readers, aliases, feature gates,
  or file formats.
- Old surfaces die outright. Do not add legacy readers, names, aliases, or
  feature-gated resurrection paths.
- `message-daemon` reads one binary configuration path from argv. It does not
  read control-plane environment variables.
- `message` uses `MESSAGE_SOCKET`; `meta-message` uses
  `MESSAGE_META_SOCKET`. Each accepts exactly one inline Dotos value.
- `messenger.sema` is the component's durable store. Preserve its fail-closed
  version discipline and bounded ledger.

## Work and history

Use fresh recorded-main Jujutsu workspaces. Treat dirty unpublished worktrees
as neither reference nor design authority. Use exact-path coordination claims,
release them immediately after proof/publication, and use `jj` for history.
