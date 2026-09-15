//! The flow-delivery park and its land-on-idle drain.
//!
//! A `FlowDeliver` query hands the messenger a `TypedPromptEnvelope` and the
//! name of the flow it is for. The messenger never types the text at a
//! harness: it PARKS the envelope durably, keyed by target flow and source
//! event identifier, and answers `DeliveryQueued`. When the target flow is
//! announced idle, the park drains: the envelope leaves the store and a
//! `CompactReceipt` — source identifier, landing stamp, byte count, never the
//! text — is produced.
//!
//! Storage: the park reuses the durable prompt-relay family, which already
//! carries a `TypedPromptEnvelope` under a destination + source-event key.
//! The older `delivery_outbox` family holds `InboxRecord` slot references into
//! the message ledger and cannot carry an envelope; giving flow delivery its
//! own family would mean a store-schema bump, which this prototype does not
//! own. Flow rows are namespaced by their destination (`flow:<name>`), so the
//! two uses of the family never see each other's rows.

use signal_message::{
    CompactReceipt, DeliveryQueueState, DeliveryQueuedAcknowledgment, FlowDeliveryRequest,
    MessageOrigin, SourceEventIdentifier, TargetFlowName, TimestampNanos, TypedPromptEnvelope,
};

use crate::{Result, relay::DeliveryState, runtime_model::RelayRecord, tables::MessengerTables};

/// The durable address of one parked flow delivery: target flow plus the
/// source event that caused it. A repeat of the same source event for the same
/// flow addresses the same row, which is what makes the park idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParkedDeliveryKey {
    pub target_flow_name: TargetFlowName,
    pub source_event_identifier: SourceEventIdentifier,
}

impl ParkedDeliveryKey {
    pub fn new(
        target_flow_name: TargetFlowName,
        source_event_identifier: SourceEventIdentifier,
    ) -> Self {
        Self {
            target_flow_name,
            source_event_identifier,
        }
    }

    /// The destination stored on the row: the flow name under the flow
    /// namespace of the shared relay family.
    pub fn destination(&self) -> String {
        Self::destination_for(&self.target_flow_name)
    }

    pub fn destination_for(target_flow_name: &str) -> String {
        format!("flow:{target_flow_name}")
    }

    /// The record key: length-prefixed on both parts, so no pair of names can
    /// spell another pair's key.
    pub fn record_key(&self) -> String {
        let destination = self.destination();
        format!(
            "{}:{destination}{}:{}",
            destination.len(),
            self.source_event_identifier.len(),
            self.source_event_identifier
        )
    }
}

/// Whether a park created the row or found the same source event already
/// parked for that flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParkOutcome {
    Parked,
    AlreadyParked,
}

/// The flow-delivery park over the messenger's durable store.
#[derive(Debug)]
pub struct FlowDeliveryOutbox<'runtime> {
    tables: &'runtime MessengerTables,
}

impl<'runtime> FlowDeliveryOutbox<'runtime> {
    pub fn new(tables: &'runtime MessengerTables) -> Self {
        Self { tables }
    }

    /// Park one delivery. A second arrival of the same source event for the
    /// same flow is the same fact, not a second row.
    pub fn park(
        &self,
        request: &FlowDeliveryRequest,
        origin: MessageOrigin,
    ) -> Result<(DeliveryQueuedAcknowledgment, ParkOutcome)> {
        let key = ParkedDeliveryKey::new(
            request.target_flow_name.clone(),
            request
                .typed_prompt_envelope
                .source_event_identifier
                .clone(),
        );
        let record_key = key.record_key();
        let outcome = match self.tables.relay_record(&record_key)? {
            Some(_) => ParkOutcome::AlreadyParked,
            None => {
                self.tables.admit_relay_record(
                    &record_key,
                    RelayRecord {
                        destination: key.destination(),
                        origin,
                        envelope: request.typed_prompt_envelope.clone(),
                        state: DeliveryState::Pending,
                    },
                )?;
                ParkOutcome::Parked
            }
        };
        Ok((
            DeliveryQueuedAcknowledgment {
                source_event_identifier: key.source_event_identifier,
                target_flow_name: key.target_flow_name,
                delivery_queue_state: DeliveryQueueState::Parked,
            },
            outcome,
        ))
    }

    /// Every envelope currently parked for one flow, oldest arrival first.
    pub fn parked(&self, target_flow_name: &TargetFlowName) -> Result<Vec<TypedPromptEnvelope>> {
        let destination = ParkedDeliveryKey::destination_for(target_flow_name);
        Ok(self
            .tables
            .relay_records()?
            .into_iter()
            .filter(|record| record.destination == destination)
            .filter(|record| record.state == DeliveryState::Pending)
            .map(|record| record.envelope)
            .collect())
    }

    /// Land every delivery parked for one flow: the stored envelope leaves the
    /// park and its compact receipt is produced. The byte count is measured on
    /// the STORED bytes — the text is never re-encoded on its way out.
    pub fn drain(&self, target_flow_name: &TargetFlowName) -> Result<Vec<CompactReceipt>> {
        let mut receipts = Vec::new();
        for envelope in self.parked(target_flow_name)? {
            let key = ParkedDeliveryKey::new(
                target_flow_name.clone(),
                envelope.source_event_identifier.clone(),
            );
            self.tables.retract_relay_record(&key.record_key())?;
            receipts.push(CompactReceipt {
                source_event_identifier: envelope.source_event_identifier,
                landed_at: Self::landed_at(),
                // `String::len` IS the UTF-8 byte length: the count is of the
                // stored bytes, never of characters.
                byte_count: envelope.raw_prompt_text.len() as i64,
            });
        }
        Ok(receipts)
    }

    fn landed_at() -> TimestampNanos {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as TimestampNanos)
            .unwrap_or(0)
    }
}
