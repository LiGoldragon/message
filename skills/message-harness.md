# Message harness surface

Harnesses perceive Message through the same producer-owned Types as every
other reader.

- Ordinary requests and replies are `signal-message` Dotos values at the CLI
  and bound contract 1 / revision 2 frames on the socket.
- Owner requests and replies are `meta-signal-message` Dotos values at the CLI
  and bound contract 2 / revision 2 frames on the owner socket.
- `message` reads one inline Dotos value and uses `MESSAGE_SOCKET`.
- `meta-message` reads one inline Dotos value and uses
  `MESSAGE_META_SOCKET`.
- Terminal delivery renders the producer-owned inbox entry directly in Dotos.

Do not teach a harness a component-local request language or legacy reader.
The producer contract is the thinking and display surface.
