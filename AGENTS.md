# Agent Instructions - Message

You MUST read lore's `AGENTS.md` and the primary workspace
orchestration protocol before editing this repository.

## Repo Role

Message is the engine message-ingress component. It owns the `message`
CLI and the supervised `message-daemon`; together they carry NOTA
message requests from humans and harnesses into typed `signal-message`
frames for the router.

## Current Phase

This repo is in supervised ingress phase. Keep the implementation narrow:

- A `message` binary that decodes one NOTA input record.
- A `message-daemon` binary that binds `message.sock`, accepts
  length-prefixed schema-derived Signal frames, stamps them, and forwards
  `signal-message` frames to `router` over the internal router socket.
- The CLI uses `MESSAGE_SOCKET` / `PERSONA_SOCKET_PATH`; the daemon reads a
  binary rkyv `Configuration` from its single argv argument and uses the
  configured router socket path.
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
