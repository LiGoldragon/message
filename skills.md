# Message work

Work here for messenger behavior: ordinary or owner Dotos clients, the
two-listener daemon, provenance, durable message state, delivery, and their
proofs.

The public Types belong to the exact `signal-message` and
`meta-signal-message` producer heads. Use those Types directly. Do not create a
friendlier component vocabulary, structural generator, local frame model,
compatibility reader, or retired text surface.

The component owns `messenger.sema`: a bounded ledger plus inbox, thread,
agent-registry, and delivery-outbox arrangements. Preserve typed fail-closed
store versioning. Public text is Dotos; component transport is the producer's
bound Signal frame inside Triad's length prefix.
