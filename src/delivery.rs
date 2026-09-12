//! The messenger's local delivery leg.
//!
//! Resolution reads the messenger's OWN durable registry: a recipient name
//! resolves first as an agent identifier (the orchestrator-minted short
//! hash), else as a thread name (fan-out to every participant except the
//! sender). A bound, not-killed endpoint gets an immediate delivery attempt;
//! everything else parks in the durable delivery outbox and drains when the
//! agent's endpoint appears (`BindAgentEndpoint`). A killed agent is never
//! attempted (the phase-4 bounce owns that seam).
//!
//! Two delivery legs:
//! - `PtySocket` — terminal-cell `data.sock`: `b"P"` + u64 big-endian length
//!   + the rendered message text, acknowledged by one `b'A'`.
//! - `HarnessSocket` — the harness daemon: a `signal-harness`
//!   `HarnessRequest::MessageDelivery` frame, acknowledged by
//!   `HarnessEvent::DeliveryCompleted`.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use signal::{ByteViewable, Restorable, Signal, Signalizable};
use signal_harness::{MessageDelivery, Query as HarnessQuery, Response as HarnessResponse};

use crate::{runtime_model::LedgerRecord, tables::MessengerTables};
use signal_message::InboxEntry;
use signal_message::{
    AgentDeathMark, AgentEndpoint, AgentEndpointKind, AgentRegistryEntry, EndpointSelection,
};

/// Why a message parked instead of delivering.
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
        let recipient = record.message_submission.message_recipient.clone();
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
        let agent = entry.agent_identifier.as_str();
        if entry.agent_death_mark == AgentDeathMark::Killed {
            return DeliveryDisposition::Parked(ParkReason::Killed);
        }
        let EndpointSelection::Bound(endpoint) = &entry.endpoint_selection else {
            if park_policy == ParkPolicy::ParkDurably {
                let _ = self.tables.append_outbox_slot(agent, record.message_slot);
            }
            return DeliveryDisposition::Parked(ParkReason::NoEndpoint);
        };
        match EndpointLeg::new(endpoint).deliver(agent, record) {
            Ok(true) => DeliveryDisposition::Delivered,
            Ok(false) | Err(_) => {
                if park_policy == ParkPolicy::ParkDurably {
                    let _ = self.tables.append_outbox_slot(agent, record.message_slot);
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
        let path = self.endpoint.endpoint_path.as_str();
        match self.endpoint.agent_endpoint_kind {
            AgentEndpointKind::PtySocket => Self::deliver_to_terminal(path, record),
            AgentEndpointKind::HarnessSocket => Self::deliver_to_harness(path, agent, record),
        }
    }

    /// Terminal-cell programmatic input: `'P'` + u64 BE length + text, one
    /// `'A'` acceptance byte back. The rendered text is the typed
    /// producer-owned `InboxEntry` Datom projection — the same record an
    /// inbox read returns.
    ///
    /// Current terminal-cell serves programmatic input on the session's
    /// CONTROL socket (its data socket answers input frames with an attach
    /// rejection), while reachability discovery stores the session's
    /// `data.sock` as the endpoint. Both sockets live in the same session
    /// directory — the layout discovery already depends on — so a
    /// `data.sock` endpoint delivers to its sibling `control.sock`; any
    /// other bound path is used exactly as bound. Proven end-to-end by
    /// `tests/pty_end_to_end.rs` against a live terminal-cell PTY.
    fn deliver_to_terminal(path: &str, record: &LedgerRecord) -> std::io::Result<bool> {
        let text = Self::rendered(record)?;
        let mut stream = UnixStream::connect(Self::programmatic_input_path(path))?;
        stream.write_all(b"P")?;
        stream.write_all(&(text.len() as u64).to_be_bytes())?;
        stream.write_all(text.as_bytes())?;
        stream.flush()?;
        let mut acceptance = [0_u8; 1];
        stream.read_exact(&mut acceptance)?;
        Ok(acceptance[0] == b'A')
    }

    fn programmatic_input_path(path: &str) -> std::path::PathBuf {
        let bound = Path::new(path);
        if bound.file_name() == Some(std::ffi::OsStr::new("data.sock"))
            && let Some(parent) = bound.parent()
        {
            return parent.join("control.sock");
        }
        bound.to_path_buf()
    }

    fn rendered(record: &LedgerRecord) -> std::io::Result<String> {
        Ok(crate::text::write(&InboxEntry {
            message_slot: record.message_slot,
            message_sender: record.sender_name.payload().clone(),
            message_body: record.message_submission.message_body.clone(),
            thread_selection: record.message_submission.thread_selection.clone(),
            stamped_at: record.stamped_at,
        }))
    }

    fn deliver_to_harness(path: &str, agent: &str, record: &LedgerRecord) -> std::io::Result<bool> {
        let request = HarnessQuery::MessageDelivery(MessageDelivery {
            harness_name: agent.to_owned(),
            message_sender: record.sender_name.payload().clone(),
            message_body: record.message_submission.message_body.clone(),
            message_slot: record.message_slot,
        });
        let bytes = request
            .signalize()
            .map_err(std::io::Error::other)?
            .bytes()
            .to_vec();
        let mut stream = UnixStream::connect(Path::new(path))?;
        stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
        stream.write_all(&bytes)?;
        stream.flush()?;
        match Self::read_harness_response(&mut stream)? {
            HarnessResponse::DeliveryCompleted(event) => Ok(event.harness_name == agent),
            _ => Ok(false),
        }
    }

    fn read_harness_response(stream: &mut impl Read) -> std::io::Result<HarnessResponse> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = vec![0_u8; length];
        stream.read_exact(&mut bytes)?;
        Signal::<HarnessResponse>::from(bytes)
            .restore()
            .map_err(std::io::Error::other)
    }
}
