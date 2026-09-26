# Message architecture

One Nexus (`crates/message-nexus`), two thin CLIs (`crates/message`,
`crates/message-meta`). The Nexus compiles its contracts without datom; the
CLIs enable it.

- `configuration` — the no-argument defaults (store, own sockets, Flow's).
- `store` — the Sema ledger: `MessageRecord`, append-only `ReceiptRecord`,
  `ParkRecord` (present while a recipient is Submitted or Parked, so a restart
  resumes it), `ConfigurationRecord`.
- `ledger` — appends grades and tells Observe subscribers.
- `flow_edge` — the Message→Flow edge: meta `ResolvePeer Vet Deliver`,
  ordinary `Observe.Agent`.
- `delivery` — one Deliver as a grade, parking, the Observe.Agent watch of
  each parked recipient, resumption.
- `service` — Send, Withdraw, Acknowledge, QueryReceipts, Redeliver,
  Configure.
- `listener` — the two sockets and the meta gate.
- `peer` — `SO_PEERCRED` plus the process start time, the identity Flow
  resolves.

Grades are never upgraded into one another: Submitted, Parked, Transported,
Presented, Uncertain, Read (the recipient's Acknowledge only), Withdrawn,
Refused.<Flow's DeliveryRejection>.

The meta socket answers the owner (a process in no flow's pane) and flows whose
aspect is in `MetaAspects` (default Psyche). Same-UID sockets make this an
accident-grade gate, not a security boundary. While Flow is unreachable only
Configure is answered, so the owner can repair the Flow socket paths.
