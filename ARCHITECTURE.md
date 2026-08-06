# Message architecture

Message has one structural boundary and one behavioral center.

The structural boundary is producer-owned:

- `signal-message` owns ordinary `Input`, `Output`, their payload Types, and
  contract 1 / revision 2 bound frames.
- `meta-signal-message` owns owner requests, replies, and contract 2 / revision
  2 bound frames.
- Triad Runtime supplies only the length-prefixed transport envelope.

Message never translates those contracts into a second local vocabulary.
Clients encode producer requests directly; listeners decode producer frames
directly; the engine matches producer inputs and returns producer outputs.

The behavioral center is component-owned:

- `MessageEngine` decides ordinary requests.
- `MessengerTables` owns the bounded `messenger.sema` ledger, inbox, thread,
  agent-registry, and delivery-outbox arrangements.
- `OriginPolicy` derives sender and ingress facts from the connection.
- `DeliveryRunner` delivers producer-owned inbox entries through harness Signal
  or terminal Dotos.
- `MessageDaemon` serves ordinary and owner sockets. Owner Configure currently
  returns the producer's typed `OperationUnimplemented(NotBuiltYet)` reply.

Startup configuration embeds the exact producer-owned daemon configuration and
adds only two private runtime values: the durable database path and fallback
owner label. It is archived as binary state; the writer helper accepts one
inline Dotos request.

Text and binary remain deliberately separate:

- Humans, agents, harnesses, and GUIs see Dotos.
- Component connections carry the producer's bound archived frames.
- Durable private arrangements are rkyv records inside `messenger.sema`.

The component contains no structural source directory, build-time generator,
generated daemon spine, local frame wrapper, compatibility reader, or retired
text feature.

## Proof surface

- direct producer-bound frame round trip;
- zero component structural ownership inputs;
- registry seat/bind and unknown-agent rejection;
- durable inbox/thread write and read;
- durable delivery parking and Dotos terminal injection;
- current-store reopen without repair or identity loss;
- live ordinary and owner daemon listeners;
- default and binary-only Cargo matrices plus Nix flake checks.
