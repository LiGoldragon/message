//! The ledger, in Message's own Sema store.
//!
//! - `MessageRecord`: one per message, as sent.
//! - `ReceiptRecord`: append-only, one per grade a recipient reaches, keyed
//!   `<MessageId>/<sequence>` so a message's receipts read in order.
//! - `ParkRecord`: a recipient still owed a delivery; present exactly while
//!   its latest grade is Submitted or Parked, so a restart resumes it.
//! - `ConfigurationRecord`: the configuration, seeded from the defaults on
//!   a new store and replaced by meta Configure.

use meta_signal_flow::{Content, DeliveryId, InterruptWitness, Sender};
use meta_signal_message::MessageConfiguration;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, KeyRange, KeyedMutation, QueryPlan,
    RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_flow::FlowId;
use signal_message::{Grade, MessageId, Priority};
use std::path::Path;

const MESSAGE_TABLE: TableName = TableName::new("message_nexus_messages");
const RECEIPT_TABLE: TableName = TableName::new("message_nexus_receipts");
const PARK_TABLE: TableName = TableName::new("message_nexus_parks");
const CONFIGURATION_TABLE: TableName = TableName::new("message_nexus_configuration");
const CONFIGURATION_KEY: &str = "configuration";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the store holds no single configuration")]
    StateInvariant,
}

/// A message as it was sent. The sender is the one Message named.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct MessageRecord {
    pub message_id: MessageId,
    pub sender: Sender,
    pub flow_id_vector: Vec<FlowId>,
    pub priority: Priority,
    pub content: Content,
    pub stamped_at: i64,
}

impl EngineRecord for MessageRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.message_id.clone())
    }
}

/// One grade a recipient reached, under the delivery that reached it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ReceiptRecord {
    pub message_id: MessageId,
    pub sequence: i64,
    pub flow_id: FlowId,
    pub delivery_id: DeliveryId,
    pub interrupt_witness: InterruptWitness,
    pub grade: Grade,
    pub stamped_at: i64,
}

impl ReceiptRecord {
    fn key_of(message_id: &str, sequence: i64) -> RecordKey {
        RecordKey::new(format!("{message_id}/{sequence:012}"))
    }
}

impl EngineRecord for ReceiptRecord {
    fn record_key(&self) -> RecordKey {
        Self::key_of(&self.message_id, self.sequence)
    }
}

/// A recipient still owed a delivery: submitted and not yet tried, or
/// parked until it rests.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ParkRecord {
    pub message_id: MessageId,
    pub flow_id: FlowId,
}

impl EngineRecord for ParkRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{}/{}", self.message_id, self.flow_id))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationRecord {
    pub message_configuration: MessageConfiguration,
}

impl EngineRecord for ConfigurationRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

pub struct MessageStore {
    engine: Engine,
    messages: TableReference<MessageRecord>,
    receipts: TableReference<ReceiptRecord>,
    parks: TableReference<ParkRecord>,
    configuration: TableReference<ConfigurationRecord>,
}

/// Reads and writes the ledger.
pub trait KeepsLedger {
    fn message(&self, message_id: &str) -> Result<Option<MessageRecord>, StoreError>;
    /// Records a message, each recipient's Submitted receipt and its park
    /// row, in one commit.
    fn record_message(
        &self,
        message: MessageRecord,
        receipts: Vec<ReceiptRecord>,
    ) -> Result<(), StoreError>;
    /// Every receipt of a message, oldest first.
    fn receipts(&self, message_id: &str) -> Result<Vec<ReceiptRecord>, StoreError>;
    /// Appends a receipt, and makes the park row agree with its grade, in
    /// one commit.
    fn append_receipt(&self, receipt: ReceiptRecord) -> Result<(), StoreError>;
    fn parks(&self) -> Result<Vec<ParkRecord>, StoreError>;
    fn configuration(&self) -> Result<MessageConfiguration, StoreError>;
    fn configure(&self, configuration: MessageConfiguration) -> Result<(), StoreError>;
}

impl MessageStore {
    /// Opens the store; a new one is seeded with the given configuration.
    pub fn open(path: &Path, defaults: MessageConfiguration) -> Result<Self, StoreError> {
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))?;
        let messages = engine.register_table(TableDescriptor::new(
            MESSAGE_TABLE,
            FamilyName::new("message-nexus-message"),
            SchemaHash::for_label("message-nexus-message-v1"),
        ))?;
        let receipts = engine.register_table(TableDescriptor::new(
            RECEIPT_TABLE,
            FamilyName::new("message-nexus-receipt"),
            SchemaHash::for_label("message-nexus-receipt-v1"),
        ))?;
        let parks = engine.register_table(TableDescriptor::new(
            PARK_TABLE,
            FamilyName::new("message-nexus-park"),
            SchemaHash::for_label("message-nexus-park-v1"),
        ))?;
        let configuration = engine.register_table(TableDescriptor::new(
            CONFIGURATION_TABLE,
            FamilyName::new("message-nexus-configuration"),
            SchemaHash::for_label("message-nexus-configuration-v1"),
        ))?;
        let store = Self {
            engine,
            messages,
            receipts,
            parks,
            configuration,
        };
        if store.configuration_records()?.is_empty() {
            store.engine.assert(Assertion::new(
                store.configuration,
                ConfigurationRecord {
                    message_configuration: defaults,
                },
            ))?;
        }
        Ok(store)
    }

    fn configuration_records(&self) -> Result<Vec<ConfigurationRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.configuration,
                RecordKey::new(CONFIGURATION_KEY),
            ))?
            .records()
            .to_vec())
    }

    fn park_present(&self, park: &ParkRecord) -> Result<bool, StoreError> {
        Ok(!self
            .engine
            .match_records(QueryPlan::key(self.parks, park.record_key()))?
            .records()
            .is_empty())
    }
}

impl KeepsLedger for MessageStore {
    fn message(&self, message_id: &str) -> Result<Option<MessageRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.messages, RecordKey::new(message_id)))?
            .records()
            .first()
            .cloned())
    }

    fn record_message(
        &self,
        message: MessageRecord,
        receipts: Vec<ReceiptRecord>,
    ) -> Result<(), StoreError> {
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.messages, message);
        for receipt in receipts {
            commit = commit.assert(
                self.parks,
                ParkRecord {
                    message_id: receipt.message_id.clone(),
                    flow_id: receipt.flow_id.clone(),
                },
            );
            commit = commit.assert(self.receipts, receipt);
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn receipts(&self, message_id: &str) -> Result<Vec<ReceiptRecord>, StoreError> {
        // '0' follows '/', so the range holds exactly this message's keys.
        let range = KeyRange::new(
            Some(RecordKey::new(format!("{message_id}/"))),
            Some(RecordKey::new(format!("{message_id}0"))),
        );
        let mut receipts = self
            .engine
            .match_records(QueryPlan::key_range(self.receipts, range))?
            .records()
            .to_vec();
        receipts.retain(|receipt| receipt.message_id == message_id);
        receipts.sort_by_key(|receipt| receipt.sequence);
        Ok(receipts)
    }

    fn append_receipt(&self, receipt: ReceiptRecord) -> Result<(), StoreError> {
        let park = ParkRecord {
            message_id: receipt.message_id.clone(),
            flow_id: receipt.flow_id.clone(),
        };
        let parked = matches!(receipt.grade, Grade::Submitted | Grade::Parked);
        let present = self.park_present(&park)?;
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.receipts, receipt);
        if parked && !present {
            commit = commit.assert(self.parks, park);
        } else if !parked && present {
            commit = commit.retract(self.parks, park.record_key());
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn parks(&self) -> Result<Vec<ParkRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.parks))?
            .records()
            .to_vec())
    }

    fn configuration(&self) -> Result<MessageConfiguration, StoreError> {
        match self.configuration_records()?.as_slice() {
            [record] => Ok(record.message_configuration.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn configure(&self, configuration: MessageConfiguration) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.configuration,
            RecordKey::new(CONFIGURATION_KEY),
            ConfigurationRecord {
                message_configuration: configuration,
            },
        ))?;
        Ok(())
    }
}
