# Message harness surface

Harnesses perceive Message through the producer-owned Types.

- `message` reads one inline `signal-message` datom and uses `MESSAGE_SOCKET`.
- `message-meta` reads one inline `meta-signal-message` datom and uses
  `MESSAGE_META_SOCKET`.
- What lands in a recipient's pane is Flow's rendering of the typed Message:
  `Soft.{ Flow.<sender> Text.«…» }`, the Priority first.
- A recipient acknowledges with `message 'Acknowledge.<MessageId>'` from its
  own pane; that alone makes a receipt Read.
