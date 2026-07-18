//! The messenger's local delivery leg (packet 3.2a) — moved in from the
//! router, which is no longer in the local loop.
//!
//! Resolution reads the messenger's OWN durable registry: a recipient name
//! resolves first as an agent identifier (the orchestrator-minted short
//! hash), else as a thread name (fan-out to every participant except the
//! sender). A bound, not-killed endpoint gets an immediate delivery attempt;
//! everything else parks in the durable delivery outbox and drains when the
//! agent's endpoint appears (`BindAgentEndpoint`). A killed agent is never
//! attempted (the phase-4 bounce owns that seam).
//!
//! Two harness legs, ported byte-for-byte from the router's proven wire:
//! - `PtySocket` — terminal-cell `data.sock`: `b"P"` + u64 big-endian length
//!   + the rendered message text, acknowledged by one `b'A'`.
//! - `HarnessSocket` — the harness daemon: a `signal-harness`
//!   `HarnessRequest::MessageDelivery` frame, acknowledged by
//!   `HarnessEvent::DeliveryCompleted`.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use nota::NotaEncode;
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
};
use signal_harness::{
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest,
    MessageBody as HarnessMessageBody, MessageDelivery, MessageSender as HarnessMessageSender,
    MessageSlot as HarnessMessageSlot, Request as HarnessSignalRequest,
};

use crate::schema::signal::{
    AgentDeathMark, AgentEndpoint, AgentEndpointKind, AgentRegistryEntry, EndpointSelection,
    InboxEntry, LedgerRecord, Sender,
};
use crate::tables::MessengerTables;

/// Why a message parked instead of delivering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParkReason {
    /// The recipient names no registered agent and no thread; the message
    /// waits in the inbox for a future reader.
    UnknownRecipient,
    /// The agent is registered but has no bound endpoint yet.
    NoEndpoint,
    /// The agent is marked killed; delivery is withheld (the phase-4 bounce
    /// owns this seam).
    Killed,
    /// The bound endpoint refused or was unreachable; the message stays
    /// parked for the next endpoint appearance.
    EndpointUnavailable,
}

/// Whether a failed attempt parks the slot durably in the outbox (a fresh
/// submission) or leaves outbox state untouched (a drain re-attempt, whose
/// slot is already parked).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkPolicy {
    ParkDurably,
    AttemptOnly,
}

/// The disposition of one delivery decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryDisposition {
    Delivered,
    Parked(ParkReason),
    /// A thread fan-out: one disposition per participant recipient.
    FannedOut(Vec<(String, DeliveryDisposition)>),
}

/// Runs local delivery decisions against the durable registry and outbox.
#[derive(Debug)]
pub struct DeliveryRunner<'runtime> {
    tables: &'runtime MessengerTables,
}

impl<'runtime> DeliveryRunner<'runtime> {
    pub fn new(tables: &'runtime MessengerTables) -> Self {
        Self { tables }
    }

    /// Decide and attempt delivery for one freshly committed ledger record.
    /// Failures park (durably, in the outbox) — they never fail the
    /// submission, whose acceptance is the existence fact.
    pub fn deliver_committed(&self, record: &LedgerRecord) -> DeliveryDisposition {
        let recipient = record.message_submission.recipient.payload().clone();
        self.deliver_to_name(&recipient, record, ParkPolicy::ParkDurably)
    }

    /// Drain the delivery outbox for one agent (called after its endpoint
    /// binds): every parked slot is re-attempted; successes leave the
    /// outbox.
    pub fn drain_outbox(&self, agent_identifier: &str) {
        let Ok(slots) = self.tables.outbox_slots(agent_identifier) else {
            return;
        };
        for slot in slots {
            let Ok(Some(record)) = self.tables.ledger_record_public(slot) else {
                let _ = self.tables.remove_outbox_slot(agent_identifier, slot);
                continue;
            };
            if let DeliveryDisposition::Delivered =
                self.deliver_to_name(agent_identifier, &record, ParkPolicy::AttemptOnly)
            {
                let _ = self.tables.remove_outbox_slot(agent_identifier, slot);
            }
        }
    }

    fn deliver_to_name(
        &self,
        recipient: &str,
        record: &LedgerRecord,
        park_policy: ParkPolicy,
    ) -> DeliveryDisposition {
        if let Ok(Some(entry)) = self.tables.registry_entry(recipient) {
            return self.deliver_to_agent(&entry, record, park_policy);
        }
        if let Ok(Some(participants)) = self.tables.thread_participants(recipient) {
            let sender = record.sender_name.payload().as_str();
            let mut outcomes = Vec::new();
            for participant in participants {
                if participant == sender {
                    continue;
                }
                let disposition = match self.tables.registry_entry(&participant) {
                    Ok(Some(entry)) => self.deliver_to_agent(&entry, record, park_policy),
                    _ => DeliveryDisposition::Parked(ParkReason::UnknownRecipient),
                };
                outcomes.push((participant, disposition));
            }
            return DeliveryDisposition::FannedOut(outcomes);
        }
        DeliveryDisposition::Parked(ParkReason::UnknownRecipient)
    }

    fn deliver_to_agent(
        &self,
        entry: &AgentRegistryEntry,
        record: &LedgerRecord,
        park_policy: ParkPolicy,
    ) -> DeliveryDisposition {
        let agent = entry.agent_identifier.payload().as_str();
        if entry.agent_death_mark == AgentDeathMark::Killed {
            return DeliveryDisposition::Parked(ParkReason::Killed);
        }
        let EndpointSelection::Bound(endpoint) = &entry.endpoint_selection else {
            if park_policy == ParkPolicy::ParkDurably {
                let _ = self
                    .tables
                    .append_outbox_slot(agent, *record.message_slot.payload());
            }
            return DeliveryDisposition::Parked(ParkReason::NoEndpoint);
        };
        match EndpointLeg::new(endpoint).deliver(agent, record) {
            Ok(true) => DeliveryDisposition::Delivered,
            Ok(false) | Err(_) => {
                if park_policy == ParkPolicy::ParkDurably {
                    let _ = self
                        .tables
                        .append_outbox_slot(agent, *record.message_slot.payload());
                }
                DeliveryDisposition::Parked(ParkReason::EndpointUnavailable)
            }
        }
    }
}

/// One bound endpoint's delivery leg.
#[derive(Debug)]
struct EndpointLeg<'endpoint> {
    endpoint: &'endpoint AgentEndpoint,
}

impl<'endpoint> EndpointLeg<'endpoint> {
    fn new(endpoint: &'endpoint AgentEndpoint) -> Self {
        Self { endpoint }
    }

    fn deliver(&self, agent: &str, record: &LedgerRecord) -> std::io::Result<bool> {
        let path = self.endpoint.endpoint_path.payload().as_str();
        match self.endpoint.agent_endpoint_kind {
            AgentEndpointKind::PtySocket => Self::deliver_to_terminal(path, record),
            AgentEndpointKind::HarnessSocket => Self::deliver_to_harness(path, agent, record),
        }
    }

    /// Terminal-cell programmatic input: `'P'` + u64 BE length + text, one
    /// `'A'` acceptance byte back. The rendered text is the typed
    /// `InboxEntry` NOTA projection — the same record an inbox read returns.
    fn deliver_to_terminal(path: &str, record: &LedgerRecord) -> std::io::Result<bool> {
        let text = Self::rendered(record);
        let mut stream = UnixStream::connect(Path::new(path))?;
        stream.write_all(b"P")?;
        stream.write_all(&(text.len() as u64).to_be_bytes())?;
        stream.write_all(text.as_bytes())?;
        stream.flush()?;
        let mut acceptance = [0_u8; 1];
        stream.read_exact(&mut acceptance)?;
        Ok(acceptance[0] == b'A')
    }

    fn rendered(record: &LedgerRecord) -> String {
        InboxEntry {
            message_slot: record.message_slot.clone(),
            sender: Sender::new(record.sender_name.payload().clone()),
            body: record.message_submission.body.clone(),
            thread_selection: record.message_submission.thread_selection.clone(),
            stamped_at: record.stamped_at.clone(),
        }
        .to_nota()
    }

    fn deliver_to_harness(path: &str, agent: &str, record: &LedgerRecord) -> std::io::Result<bool> {
        let request = HarnessSignalRequest::from_payload(HarnessRequest::MessageDelivery(
            MessageDelivery {
                harness: HarnessName::new(agent),
                sender: HarnessMessageSender::new(record.sender_name.payload().as_str()),
                body: HarnessMessageBody::new(record.message_submission.body.payload().as_str()),
                message_slot: HarnessMessageSlot::new(*record.message_slot.payload()),
            },
        ));
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let frame = HarnessFrame::new(HarnessFrameBody::Request { exchange, request });
        let mut stream = UnixStream::connect(Path::new(path))?;
        stream.write_all(
            frame
                .encode_length_prefixed()
                .map_err(std::io::Error::other)?
                .as_slice(),
        )?;
        stream.flush()?;
        match Self::read_harness_event(&mut stream)? {
            HarnessEvent::DeliveryCompleted(event) => Ok(event.harness.as_str() == agent),
            _ => Ok(false),
        }
    }

    fn read_harness_event(stream: &mut impl Read) -> std::io::Result<HarnessEvent> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream.read_exact(&mut bytes[4..])?;
        let body = HarnessFrame::decode_length_prefixed(bytes.as_slice())
            .map_err(std::io::Error::other)?
            .into_body();
        match body {
            HarnessFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(std::io::Error::other(format!(
                        "unexpected harness sub-reply: {other:?}"
                    ))),
                },
                Reply::Rejected { reason } => Err(std::io::Error::other(format!(
                    "harness rejected delivery: {reason:?}"
                ))),
            },
            other => Err(std::io::Error::other(format!(
                "expected harness reply frame, got {other:?}"
            ))),
        }
    }
}
