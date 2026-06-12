# Agent Instructions - Message

You MUST read lore's `AGENTS.md` and the primary workspace
orchestration protocol before editing this repository.

## Repo Role

Message is the engine message-ingress component. It owns the `message`
CLI, owner-side `meta-message` CLI, and supervised `message-daemon`; together
they carry ordinary NOTA message requests and owner meta-policy requests into
typed Signal frames.

## Current Phase

This repo is in supervised ingress phase. Keep the implementation narrow:

- A `message` binary that decodes one NOTA input record.
- A `meta-message` binary that decodes one `meta-signal-message` NOTA request
  and sends it to the owner meta socket.
- A `message-daemon` binary that binds `message.sock`, accepts
  length-prefixed schema-derived Signal frames, stamps them, and forwards
  `signal-message` frames to `router` over the internal router socket. It also
  binds the owner-only meta socket and answers `meta-signal-message` Configure
  with typed `RequestUnimplemented(NotBuiltYet)` until reconfiguration is built.
- The CLI uses `MESSAGE_SOCKET` / `PERSONA_SOCKET_PATH`; the daemon reads a
  binary rkyv `Configuration` from its single argv argument and uses the
  configured meta and router socket paths. `meta-message` uses
  `MESSAGE_META_SOCKET`.
- The component must not append to a local ledger or write actor registration
  state.
- The component must not construct in-band proof material or read a local actor
  index. Origin stamping is typed `StampedMessageSubmission` data minted from
  SO_PEERCRED, never caller-provided text.
- Do not add a router line-protocol fallback.

BEADS is transitional workspace coordination. Do not add a BEADS bridge here;
Persona's typed fabric is intended to absorb that role later.

## Version Control

This is a Git-backed colocated Jujutsu repository. Use `jj` for local history
work and keep Git as the remote/storage compatibility layer.

## Rust

Follow lore's Rust discipline: domain values are typed, behavior lives on the
types, errors use one crate enum, and public surfaces speak NOTA unless the
boundary is explicitly binary.
