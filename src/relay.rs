//! Durable prompt relay, deliberately disconnected from the live listeners.
//!
//! The caller supplies `MessageOrigin`; a prompt variant describes content and
//! never proves that origin.  A record is written before a delivery attempt,
//! so an interrupted write is retained as `InFlight`/`Unknown` rather than
//! retried automatically after reopening the store.

use std::sync::Arc;

use rkyv::{Archive, Deserialize, Serialize};
use signal_message::{MessageOrigin, PromptVariant, TypedPromptEnvelope};
use crate::{MessengerTables, runtime_model::RelayRecord};

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
pub(crate) enum DeliveryState { Pending, InFlight, ByteAccepted, RecipientObserved, Unknown }

pub struct Relay { tables: Arc<MessengerTables> }

impl Relay {
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self { tables: Arc::new(MessengerTables::open(path.as_ref()).map_err(storage)?) })
    }

    pub(crate) fn from_tables(tables: Arc<MessengerTables>) -> Self { Self { tables } }

    pub fn submit(&self, input: RelayInput, port: &impl DeliveryPort) -> Result<RelayDisposition> {
        let key = key(&input);
        let record = RelayRecord { destination: input.destination, origin: input.origin, envelope: input.envelope, state: DeliveryState::Pending };
        let admitted = self.admit(&key, &record)?;
        let readiness = port.readiness(&record.destination);
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
        let key = key_parts(destination, source_event_identifier);
        match self.record(&key)?.state {
            DeliveryState::ByteAccepted => self.replace_state(&key, DeliveryState::RecipientObserved),
            DeliveryState::RecipientObserved => Ok(()),
            _ => Err(RelayError::Storage("recipient observation requires prior byte acceptance".into())),
        }
    }

    pub fn pending_count(&self) -> Result<usize> {
        Ok(self.tables.relay_records().map_err(storage)?.into_iter().filter(|record| matches!(record.state, DeliveryState::Pending)).count())
    }

    fn admit(&self, key: &str, record: &RelayRecord) -> Result<bool> {
        let existing = self.tables.relay_record(key).map_err(storage)?;
        if let Some(existing) = existing {
            if existing.destination != record.destination || existing.origin != record.origin || existing.envelope != record.envelope { return Err(RelayError::Conflict); }
            return Ok(false);
        }
        self.tables.admit_relay_record(key, record.clone()).map_err(storage)?;
        Ok(true)
    }

    fn record(&self, key: &str) -> Result<RelayRecord> {
        self.tables.relay_record(key).map_err(storage)?.ok_or_else(|| RelayError::Storage("missing admitted relay record".into()))
    }

    fn replace_state(&self, key: &str, state: DeliveryState) -> Result<()> {
        let mut record = self.record(key)?;
        record.state = state;
        self.tables.replace_relay_record(key, record).map_err(storage)
    }
}

fn key(input: &RelayInput) -> String { key_parts(&input.destination, &input.envelope.source_event_identifier) }
fn key_parts(destination: &str, source_event_identifier: &str) -> String { format!("{}:{destination}{}:{source_event_identifier}", destination.len(), source_event_identifier.len()) }
fn storage(error: impl std::fmt::Display) -> RelayError { RelayError::Storage(error.to_string()) }
