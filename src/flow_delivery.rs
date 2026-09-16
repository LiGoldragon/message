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
//! own. Flow rows are namespaced by their destination (`flow:<ordinal>:<name>`),
//! so the two uses of the family never see each other's rows.
//!
//! Arrival order is carried by that destination and by nothing else. The store
//! iterates a family in record-key order and discards insertion order, and
//! `RelayRecord` has no timestamp field — adding one would change the archived
//! layout of a family this prototype does not own the schema of. So the
//! ordinal is minted into the destination spelling, where it costs no layout
//! change and where the key that quotes the destination sorts by it. The
//! living's words come back out in the order the living typed them.

use signal_message::{
    CompactReceipt, DeliveryQueueState, DeliveryQueuedAcknowledgment, FlowDeliveryRequest,
    MessageOrigin, SourceEventIdentifier, TargetFlowName, TimestampNanos, TypedPromptEnvelope,
};

use crate::{Result, relay::DeliveryState, runtime_model::RelayRecord, tables::MessengerTables};

/// Where a parked delivery sits in the arrival order of its flow. Minted on
/// park as one past the highest ordinal currently parked for that flow, so it
/// is monotonic over the rows that exist and restarts once the park empties —
/// which is all an order among parked rows can mean.
pub type ArrivalOrdinal = u64;

/// The width the ordinal is spelled at. `u64::MAX` is twenty digits, so a
/// zero-padded twenty-digit field makes lexicographic order and numeric order
/// the same order for every value the type can hold.
const ORDINAL_WIDTH: usize = 20;

/// The flow namespace inside the shared relay family. A destination that does
/// not open with this is a prompt-relay row and none of the park's business.
const FLOW_NAMESPACE: &str = "flow:";

/// The destination stored on a parked row: which flow the delivery is for,
/// and where it stands in that flow's arrival order.
///
/// This is the park's addressing, not wire vocabulary — no peer ever sees a
/// destination spelling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowDestination {
    pub target_flow_name: TargetFlowName,
    pub arrival_ordinal: ArrivalOrdinal,
}

impl FlowDestination {
    pub fn new(target_flow_name: TargetFlowName, arrival_ordinal: ArrivalOrdinal) -> Self {
        Self {
            target_flow_name,
            arrival_ordinal,
        }
    }

    /// The stored spelling. The ordinal precedes the name so that rows of one
    /// flow sort by arrival; the name is last so it may hold any character,
    /// the separator included, without confusing the reader.
    pub fn spelled(&self) -> String {
        let Self {
            target_flow_name,
            arrival_ordinal,
        } = self;
        format!(
            "{FLOW_NAMESPACE}{arrival_ordinal:0width$}:{target_flow_name}",
            width = ORDINAL_WIDTH
        )
    }

    /// Read a stored spelling back. `None` for any destination that is not a
    /// flow park row — a prompt-relay destination, or a `flow:`-prefixed one
    /// that does not carry an ordinal.
    pub fn read(spelling: &str) -> Option<Self> {
        let body = spelling.strip_prefix(FLOW_NAMESPACE)?;
        let (ordinal, target_flow_name) = body.split_at_checked(ORDINAL_WIDTH)?;
        let target_flow_name = target_flow_name.strip_prefix(':')?;
        Some(Self::new(
            target_flow_name.to_owned(),
            ordinal.parse().ok()?,
        ))
    }
}

/// The durable address of one parked flow delivery: the flow destination plus
/// the source event that caused it. A repeat of the same source event for the
/// same flow addresses the row already parked, which is what makes the park
/// idempotent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParkedDeliveryKey {
    pub flow_destination: FlowDestination,
    pub source_event_identifier: SourceEventIdentifier,
}

impl ParkedDeliveryKey {
    pub fn new(
        target_flow_name: TargetFlowName,
        arrival_ordinal: ArrivalOrdinal,
        source_event_identifier: SourceEventIdentifier,
    ) -> Self {
        Self {
            flow_destination: FlowDestination::new(target_flow_name, arrival_ordinal),
            source_event_identifier,
        }
    }

    pub fn target_flow_name(&self) -> &TargetFlowName {
        &self.flow_destination.target_flow_name
    }

    pub fn arrival_ordinal(&self) -> ArrivalOrdinal {
        self.flow_destination.arrival_ordinal
    }

    /// The record key: length-prefixed on both parts, so no pair of names can
    /// spell another pair's key. Within one flow the destination length is
    /// constant and the ordinal is fixed-width, so store-key order over these
    /// keys IS arrival order.
    pub fn record_key(&self) -> String {
        RelayRecord::key_for(
            &self.flow_destination.spelled(),
            &self.source_event_identifier,
        )
    }
}

/// One row of the park as it was read: its address and the envelope it holds.
#[derive(Clone, Debug, PartialEq)]
pub struct ParkedDelivery {
    pub parked_delivery_key: ParkedDeliveryKey,
    pub typed_prompt_envelope: TypedPromptEnvelope,
}

/// One delivery about to land: the row to retract and the receipt already
/// built from it. The receipt exists BEFORE anything is retracted, so no
/// store failure can destroy the living's words and lose the only record that
/// they arrived.
#[derive(Clone, Debug, PartialEq)]
pub struct LandedDelivery {
    pub parked_delivery_key: ParkedDeliveryKey,
    pub compact_receipt: CompactReceipt,
}

/// One flow's whole landing, read in one pass and committed in one act.
///
/// A landing is a read: it retracts nothing. `FlowDeliveryOutbox::land`
/// turns it into the single all-or-nothing retraction that makes it true.
#[derive(Clone, Debug, PartialEq)]
pub struct Landing {
    pub target_flow_name: TargetFlowName,
    pub landed_deliveries: Vec<LandedDelivery>,
}

impl Landing {
    pub fn is_empty(&self) -> bool {
        self.landed_deliveries.is_empty()
    }
}

/// What one park attempt was: a new row, the same fact already parked, or a
/// source event identifier re-used for different words.
#[derive(Clone, Debug, PartialEq)]
pub enum ParkAttempt {
    Parked(DeliveryQueuedAcknowledgment),
    AlreadyParked(DeliveryQueuedAcknowledgment),
    /// The same source event identifier is parked for this flow carrying a
    /// DIFFERENT envelope. Dropping the second text and answering "queued"
    /// would lose the living's words behind a success; this is the refusal
    /// the prompt relay next door has always made (`RelayError::Conflict`).
    Conflicting,
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
    /// same flow is the same fact, not a second row — but only when it
    /// carries the same envelope.
    ///
    /// The comparison is on the envelope alone and not on the origin: the
    /// same words re-submitted over a different connection are still the same
    /// words, and refusing them as "conflicting" would name the wrong thing.
    pub fn park(
        &self,
        request: &FlowDeliveryRequest,
        origin: MessageOrigin,
    ) -> Result<ParkAttempt> {
        let envelope = &request.typed_prompt_envelope;
        let parked = self.parked_deliveries(&request.target_flow_name)?;
        if let Some(existing) = parked.iter().find(|delivery| {
            delivery.parked_delivery_key.source_event_identifier == envelope.source_event_identifier
        }) {
            if existing.typed_prompt_envelope != *envelope {
                return Ok(ParkAttempt::Conflicting);
            }
            return Ok(ParkAttempt::AlreadyParked(Self::acknowledgment(
                &existing.parked_delivery_key,
            )));
        }
        let key = ParkedDeliveryKey::new(
            request.target_flow_name.clone(),
            Self::next_arrival_ordinal(&parked),
            envelope.source_event_identifier.clone(),
        );
        self.tables.admit_relay_record(
            &key.record_key(),
            RelayRecord {
                destination: key.flow_destination.spelled(),
                origin,
                envelope: envelope.clone(),
                state: DeliveryState::Pending,
            },
        )?;
        Ok(ParkAttempt::Parked(Self::acknowledgment(&key)))
    }

    /// Every row currently parked for one flow, oldest arrival first.
    pub fn parked_deliveries(
        &self,
        target_flow_name: &TargetFlowName,
    ) -> Result<Vec<ParkedDelivery>> {
        Ok(self
            .tables
            .relay_records()?
            .into_iter()
            .filter(|record| record.state == DeliveryState::Pending)
            .filter_map(|record| {
                let flow_destination = FlowDestination::read(&record.destination)?;
                (flow_destination.target_flow_name == *target_flow_name).then(|| ParkedDelivery {
                    parked_delivery_key: ParkedDeliveryKey {
                        source_event_identifier: record.envelope.source_event_identifier.clone(),
                        flow_destination,
                    },
                    typed_prompt_envelope: record.envelope,
                })
            })
            .collect())
    }

    /// Every envelope currently parked for one flow, oldest arrival first.
    pub fn parked(&self, target_flow_name: &TargetFlowName) -> Result<Vec<TypedPromptEnvelope>> {
        Ok(self
            .parked_deliveries(target_flow_name)?
            .into_iter()
            .map(|delivery| delivery.typed_prompt_envelope)
            .collect())
    }

    /// Read what would land for one flow, receipts and all, retracting
    /// nothing. Every receipt is built here, before any row is touched.
    ///
    /// The byte count is measured on the STORED bytes — the text is never
    /// re-encoded on its way out.
    pub fn landing(&self, target_flow_name: &TargetFlowName) -> Result<Landing> {
        Ok(Landing {
            target_flow_name: target_flow_name.clone(),
            landed_deliveries: self
                .parked_deliveries(target_flow_name)?
                .into_iter()
                .map(|delivery| LandedDelivery {
                    compact_receipt: CompactReceipt {
                        source_event_identifier: delivery
                            .typed_prompt_envelope
                            .source_event_identifier
                            .clone(),
                        landed_at: Self::landed_at(),
                        // `String::len` IS the UTF-8 byte length: the count is
                        // of the stored bytes, never of characters.
                        byte_count: delivery.typed_prompt_envelope.raw_prompt_text.len() as i64,
                    },
                    parked_delivery_key: delivery.parked_delivery_key,
                })
                .collect(),
        })
    }

    /// Make a landing true: retract every one of its rows in ONE commit, then
    /// hand back the receipts.
    ///
    /// All-or-nothing. If any row of the landing is already gone — a second
    /// drain of the same flow racing this one — the commit refuses before it
    /// writes anything, this call returns the refusal, and not one parked row
    /// is lost and not one receipt is invented. The loser of that race
    /// delivers nothing; the winner delivers everything exactly once.
    pub fn land(&self, landing: Landing) -> Result<Vec<CompactReceipt>> {
        if landing.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<String> = landing
            .landed_deliveries
            .iter()
            .map(|delivery| delivery.parked_delivery_key.record_key())
            .collect();
        self.tables.retract_relay_records(&keys)?;
        Ok(landing
            .landed_deliveries
            .into_iter()
            .map(|delivery| delivery.compact_receipt)
            .collect())
    }

    /// Land every delivery parked for one flow: read the whole landing, then
    /// commit it as one act.
    pub fn drain(&self, target_flow_name: &TargetFlowName) -> Result<Vec<CompactReceipt>> {
        self.land(self.landing(target_flow_name)?)
    }

    fn next_arrival_ordinal(parked: &[ParkedDelivery]) -> ArrivalOrdinal {
        parked
            .iter()
            .map(|delivery| delivery.parked_delivery_key.arrival_ordinal())
            .max()
            .map_or(0, |highest| highest.saturating_add(1))
    }

    fn acknowledgment(key: &ParkedDeliveryKey) -> DeliveryQueuedAcknowledgment {
        DeliveryQueuedAcknowledgment {
            source_event_identifier: key.source_event_identifier.clone(),
            target_flow_name: key.target_flow_name().clone(),
            delivery_queue_state: DeliveryQueueState::Parked,
        }
    }

    fn landed_at() -> TimestampNanos {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos().min(i64::MAX as u128) as TimestampNanos)
            .unwrap_or(0)
    }
}
