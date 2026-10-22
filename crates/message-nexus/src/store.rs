//! Message's configuration and first-configuration state.

use crate::{generated::memory::Standard, message_library::Configuration};
use memory_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan,
    RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use std::path::Path;

const STANDARD_TABLE: TableName = TableName::new("message_nexus_standard");
const STANDARD_KEY: &str = "standard";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("memory-engine: {0}")]
    Engine(#[from] memory_engine::Error),
    #[error("store {path} is not current Message Standard2: {reason}")]
    Unrecognized { path: String, reason: String },
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
        Self::recognize(path)?;
        let descriptor = TableDescriptor::<Standard>::new(
            STANDARD_TABLE,
            FamilyName::new("message-nexus-standard"),
            SchemaHash::for_label("message-nexus-standard-v2"),
        );
        let mut engine =
            Engine::open(EngineOpen::new(path, SchemaVersion::new(2)).with_family(&descriptor))?;
        let standard = engine.register_table(descriptor)?;
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

/// Existing occupants are inspected without opening an Engine writer. Legacy
/// relay, schema3 and schema5 stores remain evidence; ordinary startup never
/// converts them or stamps a new Standard into them.
trait RecognizesStore {
    fn recognize(path: &Path) -> Result<(), StoreError>;
}
impl RecognizesStore for MessageStore {
    fn recognize(path: &Path) -> Result<(), StoreError> {
        use redb::{ReadableDatabase, ReadableTable};
        if !path.exists() {
            return Ok(());
        }
        let reject = |reason: String| StoreError::Unrecognized {
            path: path.display().to_string(),
            reason,
        };
        // A killed writer leaves redb's allocator recovery pending. Repair only
        // an in-memory snapshot for recognition, never an unknown original.
        // Keep the source shared-locked until all identity checks have passed.
        let mut _snapshot_guard = None;
        let ordinary_database;
        let recovered_database;
        let read = match redb::ReadOnlyDatabase::open(path) {
            Ok(database) => {
                ordinary_database = database;
                ordinary_database
                    .begin_read()
                    .map_err(|error| reject(error.to_string()))?
            }
            Err(redb::DatabaseError::RepairAborted) => {
                use redb::StorageBackend;
                use std::io::Read;
                let mut source =
                    std::fs::File::open(path).map_err(|error| reject(error.to_string()))?;
                source
                    .try_lock_shared()
                    .map_err(|error| reject(error.to_string()))?;
                let mut bytes = Vec::new();
                source
                    .read_to_end(&mut bytes)
                    .map_err(|error| reject(error.to_string()))?;
                let backend = redb::backends::InMemoryBackend::new();
                backend
                    .set_len(bytes.len() as u64)
                    .map_err(|error| reject(error.to_string()))?;
                StorageBackend::write(&backend, 0, &bytes)
                    .map_err(|error| reject(error.to_string()))?;
                _snapshot_guard = Some(source);
                recovered_database = redb::Database::builder()
                    .create_with_backend(backend)
                    .map_err(|error| reject(error.to_string()))?;
                recovered_database
                    .begin_read()
                    .map_err(|error| reject(error.to_string()))?
            }
            Err(error) => return Err(reject(error.to_string())),
        };
        let metadata = read
            .open_table(redb::TableDefinition::<&str, u64>::new("__memory_meta"))
            .map_err(|error| reject(error.to_string()))?;
        let version = metadata
            .get("schema_version")
            .map_err(|error| reject(error.to_string()))?
            .map(|value| value.value());
        if version != Some(2) {
            return Err(reject(format!("schema version {version:?}, expected 2")));
        }
        let counters = read
            .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                "__memory_engine_counters",
            ))
            .map_err(|error| reject(error.to_string()))?;
        let layout = counters
            .get("engine_storage_layout")
            .map_err(|error| reject(error.to_string()))?
            .map(|bytes| rkyv::from_bytes::<u64, rkyv::rancor::Error>(bytes.value()))
            .transpose()
            .map_err(|error| reject(error.to_string()))?;
        if layout != Some(7) {
            return Err(reject(format!("engine layout {layout:?}, expected 7")));
        }
        let catalog = read
            .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                "__memory_engine_catalog",
            ))
            .map_err(|error| reject(error.to_string()))?;
        let mut count = 0;
        for entry in catalog.iter().map_err(|error| reject(error.to_string()))? {
            let (key, value) = entry.map_err(|error| reject(error.to_string()))?;
            let registration = rkyv::from_bytes::<
                memory_engine::TableRegistration,
                rkyv::rancor::Error,
            >(value.value())
            .map_err(|error| reject(error.to_string()))?;
            let identity = registration.identity();
            if key.value() != "message_nexus_standard"
                || identity.table_name() != "message_nexus_standard"
                || identity.family().as_str() != "message-nexus-standard"
                || identity.schema_hash() != SchemaHash::for_label("message-nexus-standard-v2")
            {
                return Err(reject(format!(
                    "foreign or historical family {:?}",
                    key.value()
                )));
            }
            count += 1;
        }
        if count != 1 {
            return Err(reject("missing Standard2 family".into()));
        }
        let standards = read
            .open_table(redb::TableDefinition::<String, &[u8]>::new(
                "message_nexus_standard",
            ))
            .map_err(|error| reject(error.to_string()))?;
        let mut count = 0;
        for entry in standards
            .iter()
            .map_err(|error| reject(error.to_string()))?
        {
            let (key, value) = entry.map_err(|error| reject(error.to_string()))?;
            if key.value() != STANDARD_KEY {
                return Err(reject("noncanonical Standard key".into()));
            }
            rkyv::from_bytes::<Standard, rkyv::rancor::Error>(value.value())
                .map_err(|error| reject(error.to_string()))?;
            count += 1;
        }
        if count != 1 {
            return Err(reject("missing or duplicate Standard row".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod recognition_tests {
    use super::*;
    fn seed() -> Standard {
        Standard {
            configuration: Configuration {
                ordinary: "/run/message.sock".into(),
                meta: "/run/message-meta.sock".into(),
                flow: "/run/flow.sock".into(),
            },
            meta_configured: false,
        }
    }
    #[test]
    fn current_standard_reopens_without_reseeding_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.memory");
        let store = MessageStore::open(&path, seed()).unwrap();
        let mut configured = store.standard().unwrap();
        configured.meta_configured = true;
        store.replace_standard(configured.clone()).unwrap();
        drop(store);
        let reopened = MessageStore::open(&path, seed()).unwrap();
        assert_eq!(reopened.standard().unwrap(), configured);
    }
    #[test]
    fn foreign_and_legacy_occupants_are_refused_without_changing_bytes() {
        for version in [2, 3, 5] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("message.memory");
            let engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(version))).unwrap();
            drop(engine);
            let before = std::fs::read(&path).unwrap();
            assert!(matches!(
                MessageStore::open(&path, seed()),
                Err(StoreError::Unrecognized { .. })
            ));
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }
    // Child-only crash fixture: exit without dropping the Engine, exactly as an
    // externally terminated Nexus does. The parent owns all temporary paths.
    #[test]
    fn crash_writer() {
        let Some(path) = std::env::var_os("MESSAGE_TEST_CRASH_STORE") else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        if std::env::var_os("MESSAGE_TEST_FOREIGN_STORE").is_some() {
            let _engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(5))).unwrap();
            std::process::exit(0);
        }
        let store = MessageStore::open(&path, seed()).unwrap();
        let mut configured = store.standard().unwrap();
        configured.meta_configured = true;
        store.replace_standard(configured).unwrap();
        std::process::exit(0);
    }

    #[test]
    fn crash_recognition_preserves_original_and_rejects_foreign_store() {
        for foreign in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("message.memory");
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "store::recognition_tests::crash_writer"])
                .env("MESSAGE_TEST_CRASH_STORE", &path)
                .env_remove("MESSAGE_TEST_FOREIGN_STORE");
            if foreign {
                command.env("MESSAGE_TEST_FOREIGN_STORE", "1");
            }
            assert!(command.status().unwrap().success());
            assert!(matches!(
                redb::ReadOnlyDatabase::open(&path),
                Err(redb::DatabaseError::RepairAborted)
            ));
            let before = std::fs::read(&path).unwrap();
            if foreign {
                assert!(matches!(
                    MessageStore::open(&path, seed()),
                    Err(StoreError::Unrecognized { .. })
                ));
            } else {
                MessageStore::recognize(&path).unwrap();
            }
            assert_eq!(std::fs::read(&path).unwrap(), before);
            if !foreign {
                let reopened = MessageStore::open(&path, seed()).unwrap();
                assert!(reopened.standard().unwrap().meta_configured);
            }
        }
    }
}
