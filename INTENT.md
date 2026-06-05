# INTENT — message

`message` owns two binaries: the `message` CLI (thin client) and `message-daemon`
(supervised first-stack component). Neither carries a durable message ledger. Both are
stateless boundary surfaces. Routing policy, delivery state, and channel authority
remain in router.

The daemon owns one Kameo root actor and binds `message.sock` (mode 0660, engine-owner
group). It reads typed `MessageDaemonConfiguration` from argv via `nota-config`—socket
paths, socket modes, owner identity, supervision socket. The daemon stamps
`MessageSubmission` frames with configured owner identity, SO_PEERCRED-derived origin,
and ingress timestamp; then forwards `StampedMessageSubmission` frames to router's
internal socket (`router.sock`, 0600). Provenance is typed, minted by the daemon, never
inferred from uid or accepted from payload.

The CLI accepts exactly one NOTA `Send` or `Inbox` record, projects to a length-prefixed
Signal frame, sends to `message.sock`, reads one reply frame, prints the NOTA reply.
Request/reply matching is frame-level: every request carries an ExchangeIdentifier, and
every reply echoes it.

Key constraints: caller identity is not accepted from model or CLI payload. The daemon
requires a typed configuration on argv; it exits if missing or malformed. The daemon
applies configured socket mode before accepting client traffic. CLI and daemon outbound
traffic are length-prefixed rkyv Signal frames. The component depends on stable Persona
Kameo lifecycle reference. Graceful supervision stop releases the socket and rejects later
ingress. Production daemon reads no environment variables for control-plane configuration.
Mismatched Signal verb/payload pairs are rejected as typed RequestRejectionReason.
