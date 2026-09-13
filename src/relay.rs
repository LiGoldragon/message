//! Durable prompt relay, deliberately disconnected from the live listeners.
//!
//! The caller supplies `MessageOrigin`; a prompt variant describes content and
//! never proves that origin.  A record is written before a delivery attempt,
//! so an interrupted write is retained as `InFlight`/`Unknown` rather than
//! retried automatically after reopening the store.

use rkyv::{Archive, Deserialize, Serialize};
use redb::{ReadableDatabase, ReadableTable};
use signal_message::{MessageOrigin, PromptVariant, TypedPromptEnvelope};

const RELAY_RECORDS: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("relay_records");

#[derive(Debug, thiserror::Error)]
pub enum RelayError {
    #[error("relay storage: {0}")]
    Storage(String),
    #[error("source event and destination were already admitted with different content or origin")]
    Conflict,
}

pub type Result<T> = std::result::Result<T, RelayError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetReadiness { Ready, Busy, Dirty }

pub trait DeliveryPort {
    fn readiness(&self, destination: &str) -> TargetReadiness;
    fn deliver(&self, destination: &str, bytes: &[u8]) -> std::io::Result<()>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct RelayInput {
    pub destination: String,
    pub origin: MessageOrigin,
    pub envelope: TypedPromptEnvelope,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RelayDisposition {
    Pending(TargetReadiness),
    RecordedOnly,
    DuplicatePending(TargetReadiness),
    InFlight,
    ByteAccepted,
    RecipientObserved,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
enum DeliveryState { Pending, InFlight, ByteAccepted, RecipientObserved, Unknown }

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct RelayRecord {
    destination: String,
    origin: MessageOrigin,
    envelope: TypedPromptEnvelope,
    state: DeliveryState,
}

pub struct Relay { database: redb::Database }

impl Relay {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let database = redb::Database::create(path.as_ref()).map_err(storage)?;
        { let write = database.begin_write().map_err(storage)?; write.open_table(RELAY_RECORDS).map_err(storage)?; write.commit().map_err(storage)?; }
        Ok(Self { database })
    }

    pub fn submit(&self, input: RelayInput, port: &impl DeliveryPort) -> Result<RelayDisposition> {
        let key = key(&input);
        let readiness = port.readiness(&input.destination);
        let record = RelayRecord { destination: input.destination, origin: input.origin, envelope: input.envelope, state: DeliveryState::Pending };
        let admitted = self.admit(&key, &record)?;
        if !admitted {
            return match self.record(&key)?.state {
                DeliveryState::Pending => Ok(RelayDisposition::DuplicatePending(readiness)),
                DeliveryState::InFlight | DeliveryState::Unknown => Ok(RelayDisposition::InFlight),
                DeliveryState::ByteAccepted => Ok(RelayDisposition::ByteAccepted),
                DeliveryState::RecipientObserved => Ok(RelayDisposition::RecipientObserved),
            };
        }
        if !matches!(record.envelope.prompt_variant, PromptVariant::HumanPrompt) {
            return Ok(RelayDisposition::RecordedOnly);
        }
        if readiness != TargetReadiness::Ready { return Ok(RelayDisposition::Pending(readiness)); }
        self.replace_state(&key, DeliveryState::InFlight)?;
        let bytes = self.record(&key)?.envelope.raw_prompt_text.as_bytes().to_vec();
        if port.deliver(&record.destination, &bytes).is_err() {
            self.replace_state(&key, DeliveryState::Unknown)?;
            return Ok(RelayDisposition::InFlight);
        }
        self.replace_state(&key, DeliveryState::ByteAccepted)?;
        Ok(RelayDisposition::ByteAccepted)
    }

    pub fn recipient_observed(&self, destination: &str, source_event_identifier: &str) -> Result<()> {
        self.replace_state(&format!("{destination}\0{source_event_identifier}"), DeliveryState::RecipientObserved)
    }

    pub fn pending_count(&self) -> Result<usize> {
        let read = self.database.begin_read().map_err(storage)?;
        let table = read.open_table(RELAY_RECORDS).map_err(storage)?;
        let mut count = 0;
        for entry in table.iter().map_err(storage)? { let (_, value) = entry.map_err(storage)?; if matches!(decode(value.value())?.state, DeliveryState::Pending) { count += 1; } }
        Ok(count)
    }

    fn admit(&self, key: &str, record: &RelayRecord) -> Result<bool> {
        let write = self.database.begin_write().map_err(storage)?;
        let mut table = write.open_table(RELAY_RECORDS).map_err(storage)?;
        let existing = table.get(key).map_err(storage)?.map(|value| decode(value.value())).transpose()?;
        if let Some(existing) = existing {
            if existing.destination != record.destination || existing.origin != record.origin || existing.envelope != record.envelope { return Err(RelayError::Conflict); }
            drop(table); write.commit().map_err(storage)?; return Ok(false);
        }
        let encoded = encode(record)?;
        table.insert(key, encoded.as_slice()).map_err(storage)?;
        drop(table); write.commit().map_err(storage)?;
        Ok(true)
    }

    fn record(&self, key: &str) -> Result<RelayRecord> {
        let read = self.database.begin_read().map_err(storage)?;
        let table = read.open_table(RELAY_RECORDS).map_err(storage)?;
        let value = table.get(key).map_err(storage)?.ok_or_else(|| RelayError::Storage("missing admitted relay record".into()))?;
        decode(value.value())
    }

    fn replace_state(&self, key: &str, state: DeliveryState) -> Result<()> {
        let write = self.database.begin_write().map_err(storage)?;
        let mut table = write.open_table(RELAY_RECORDS).map_err(storage)?;
        let mut record = table.get(key).map_err(storage)?.map(|value| decode(value.value())).transpose()?.ok_or_else(|| RelayError::Storage("missing admitted relay record".into()))?;
        record.state = state;
        let encoded = encode(&record)?;
        table.insert(key, encoded.as_slice()).map_err(storage)?;
        drop(table); write.commit().map_err(storage)
    }
}

fn key(input: &RelayInput) -> String { format!("{}\0{}", input.destination, input.envelope.source_event_identifier) }
fn encode(record: &RelayRecord) -> Result<Vec<u8>> { rkyv::to_bytes::<rkyv::rancor::Error>(record).map(|value| value.to_vec()).map_err(storage) }
fn decode(bytes: &[u8]) -> Result<RelayRecord> { rkyv::from_bytes::<RelayRecord, rkyv::rancor::Error>(bytes).map_err(storage) }
fn storage(error: impl std::fmt::Display) -> RelayError { RelayError::Storage(error.to_string()) }
