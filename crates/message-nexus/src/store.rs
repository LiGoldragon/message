//! Message's configuration and first-configuration state.

use crate::{generated::memory::Standard, message_library::Configuration};
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan,
    RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use std::path::Path;

const STANDARD_TABLE: TableName = TableName::new("message_nexus_standard");
const STANDARD_KEY: &str = "standard";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("the store does not hold exactly one Standard record")]
    StateInvariant,
}

impl EngineRecord for Standard {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(STANDARD_KEY)
    }
}

pub struct MessageStore {
    engine: Engine,
    standard: TableReference<Standard>,
}

pub trait KeepsStandard {
    fn standard(&self) -> Result<Standard, StoreError>;
    fn replace_standard(&self, standard: Standard) -> Result<(), StoreError>;
}

pub trait OpensStore: Sized {
    fn open(path: &Path, seed: Standard) -> Result<Self, StoreError>;
}

trait ReadsRows {
    fn standards(&self) -> Result<Vec<Standard>, StoreError>;
}

impl OpensStore for MessageStore {
    fn open(path: &Path, seed: Standard) -> Result<Self, StoreError> {
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(2)))?;
        let standard = engine.register_table(TableDescriptor::new(
            STANDARD_TABLE,
            FamilyName::new("message-nexus-standard"),
            SchemaHash::for_label("message-nexus-standard-v2"),
        ))?;
        let store = Self { engine, standard };
        if store.standards()?.is_empty() {
            store.engine.assert_keyed(KeyedAssertion::new(
                store.standard,
                RecordKey::new(STANDARD_KEY),
                seed,
            ))?;
        }
        Ok(store)
    }
}

impl ReadsRows for MessageStore {
    fn standards(&self) -> Result<Vec<Standard>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.standard, RecordKey::new(STANDARD_KEY)))?
            .records()
            .to_vec())
    }
}

impl KeepsStandard for MessageStore {
    fn standard(&self) -> Result<Standard, StoreError> {
        match self.standards()?.as_slice() {
            [standard] => Ok(standard.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn replace_standard(&self, standard: Standard) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.standard,
            RecordKey::new(STANDARD_KEY),
            standard,
        ))?;
        Ok(())
    }
}

pub trait ChangesConfiguration {
    fn changed_configuration(&self, configuration: &Configuration, meta: bool) -> Standard;
}

impl ChangesConfiguration for Standard {
    fn changed_configuration(&self, configuration: &Configuration, meta: bool) -> Standard {
        Standard {
            configuration: configuration.clone(),
            meta_configured: self.meta_configured || meta,
        }
    }
}
