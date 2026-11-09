//! Message's configuration and first-configuration state.

use crate::{
    generated::memory::{MetaConfigured, Standard},
    message_library::Configuration,
    request_tracking::{MakesRequestTracking, RequestTracking},
};
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan,
    RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_flow::Namespace;
use std::path::Path;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

const STANDARD_TABLE: TableName = TableName::new("message_nexus_standard");
const STANDARD_KEY: &str = "standard";
const REQUEST_TRACKING_TABLE: TableName = TableName::new("message_nexus_request_tracking");
const REQUEST_TRACKING_KEY: &str = "request-tracking";

pub type DurableRequestTracking = RequestTracking<signal_flow::Deliver_Data>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("store {path} is not current Message 0.22 Memory: {reason}")]
    Unrecognized { path: String, reason: String },
    #[error("the store does not hold exactly one Standard record")]
    StateInvariant,
    #[error("cannot mint the Message Memory namespace: {0}")]
    Namespace(String),
    #[cfg(test)]
    #[error("injected request-tracking replacement failure")]
    InjectedTrackingReplacement,
}

impl EngineRecord for Standard {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(STANDARD_KEY)
    }
}

impl EngineRecord for DurableRequestTracking {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(REQUEST_TRACKING_KEY)
    }
}

pub struct MessageStore {
    engine: Engine,
    standard: TableReference<Standard>,
    request_tracking: TableReference<DurableRequestTracking>,
    #[cfg(test)]
    fail_tracking_replacement: AtomicBool,
}

pub trait KeepsStandard {
    fn standard(&self) -> Result<Standard, StoreError>;
    fn replace_standard(&self, standard: Standard) -> Result<(), StoreError>;
}

pub trait KeepsRequestTracking {
    fn request_tracking(&self) -> Result<DurableRequestTracking, StoreError>;
    fn replace_request_tracking(&self, tracking: DurableRequestTracking) -> Result<(), StoreError>;
}

pub trait OpensStore: Sized {
    fn open(path: &Path, seed: Standard) -> Result<Self, StoreError>;
}

#[cfg(test)]
pub(crate) trait InjectsTrackingFailure {
    fn fail_next_tracking_replacement(&self);
}

trait ReadsRows {
    fn standards(&self) -> Result<Vec<Standard>, StoreError>;
    fn request_trackings(&self) -> Result<Vec<DurableRequestTracking>, StoreError>;
}

impl OpensStore for MessageStore {
    fn open(path: &Path, seed: Standard) -> Result<Self, StoreError> {
        Self::recognize(path)?;
        let standard_descriptor = TableDescriptor::<Standard>::new(
            STANDARD_TABLE,
            FamilyName::new("message-nexus-standard"),
            SchemaHash::for_label("message-nexus-standard-v2"),
        );
        let tracking_descriptor = TableDescriptor::<DurableRequestTracking>::new(
            REQUEST_TRACKING_TABLE,
            FamilyName::new("message-nexus-request-tracking"),
            SchemaHash::for_label("message-nexus-request-tracking-v1"),
        );
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(3))
                .with_family(&standard_descriptor)
                .with_family(&tracking_descriptor),
        )?;
        let standard = engine.register_table(standard_descriptor)?;
        let request_tracking = engine.register_table(tracking_descriptor)?;
        let store = Self {
            engine,
            standard,
            request_tracking,
            #[cfg(test)]
            fail_tracking_replacement: AtomicBool::new(false),
        };
        if store.standards()?.is_empty() {
            store.engine.assert_keyed(KeyedAssertion::new(
                store.standard,
                RecordKey::new(STANDARD_KEY),
                seed,
            ))?;
        }
        if store.request_trackings()?.is_empty() {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes)
                .map_err(|error| StoreError::Namespace(error.to_string()))?;
            let namespace = bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            store.engine.assert_keyed(KeyedAssertion::new(
                store.request_tracking,
                RecordKey::new(REQUEST_TRACKING_KEY),
                DurableRequestTracking::new(Namespace(namespace)),
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

    fn request_trackings(&self) -> Result<Vec<DurableRequestTracking>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.request_tracking,
                RecordKey::new(REQUEST_TRACKING_KEY),
            ))?
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

impl KeepsRequestTracking for MessageStore {
    fn request_tracking(&self) -> Result<DurableRequestTracking, StoreError> {
        match self.request_trackings()?.as_slice() {
            [tracking] => Ok(tracking.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn replace_request_tracking(&self, tracking: DurableRequestTracking) -> Result<(), StoreError> {
        #[cfg(test)]
        if self.fail_tracking_replacement.swap(false, Ordering::AcqRel) {
            return Err(StoreError::InjectedTrackingReplacement);
        }
        self.engine.mutate_keyed(KeyedMutation::new(
            self.request_tracking,
            RecordKey::new(REQUEST_TRACKING_KEY),
            tracking,
        ))?;
        Ok(())
    }
}

#[cfg(test)]
impl InjectsTrackingFailure for MessageStore {
    fn fail_next_tracking_replacement(&self) {
        self.fail_tracking_replacement
            .store(true, Ordering::Release);
    }
}

pub trait ChangesConfiguration {
    fn changed_configuration(&self, configuration: &Configuration, meta: bool) -> Standard;
}

impl ChangesConfiguration for Standard {
    fn changed_configuration(&self, configuration: &Configuration, meta: bool) -> Standard {
        Standard {
            configuration: configuration.clone(),
            meta_configured: MetaConfigured(self.meta_configured.0 || meta),
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
            .open_table(redb::TableDefinition::<&str, u64>::new("__sema_meta"))
            .map_err(|error| reject(error.to_string()))?;
        let version = metadata
            .get("schema_version")
            .map_err(|error| reject(error.to_string()))?
            .map(|value| value.value());
        if version != Some(3) {
            return Err(reject(format!("schema version {version:?}, expected 3")));
        }
        let counters = read
            .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                "__sema_engine_counters",
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
                "__sema_engine_catalog",
            ))
            .map_err(|error| reject(error.to_string()))?;
        let mut count = 0;
        for entry in catalog.iter().map_err(|error| reject(error.to_string()))? {
            let (key, value) = entry.map_err(|error| reject(error.to_string()))?;
            let registration = rkyv::from_bytes::<
                sema_engine::TableRegistration,
                rkyv::rancor::Error,
            >(value.value())
            .map_err(|error| reject(error.to_string()))?;
            let identity = registration.identity();
            let recognized = match key.value() {
                "message_nexus_standard" => {
                    identity.table_name() == "message_nexus_standard"
                        && identity.family().as_str() == "message-nexus-standard"
                        && identity.schema_hash()
                            == SchemaHash::for_label("message-nexus-standard-v2")
                }
                "message_nexus_request_tracking" => {
                    identity.table_name() == "message_nexus_request_tracking"
                        && identity.family().as_str() == "message-nexus-request-tracking"
                        && identity.schema_hash()
                            == SchemaHash::for_label("message-nexus-request-tracking-v1")
                }
                _ => false,
            };
            if !recognized {
                return Err(reject(format!(
                    "foreign or historical family {:?}",
                    key.value()
                )));
            }
            count += 1;
        }
        if count != 2 {
            return Err(reject("missing Message 0.22 families".into()));
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
        let tracking = read
            .open_table(redb::TableDefinition::<String, &[u8]>::new(
                "message_nexus_request_tracking",
            ))
            .map_err(|error| reject(error.to_string()))?;
        let mut count = 0;
        for entry in tracking.iter().map_err(|error| reject(error.to_string()))? {
            let (key, value) = entry.map_err(|error| reject(error.to_string()))?;
            if key.value() != REQUEST_TRACKING_KEY {
                return Err(reject("noncanonical RequestTracking key".into()));
            }
            rkyv::from_bytes::<DurableRequestTracking, rkyv::rancor::Error>(value.value())
                .map_err(|error| reject(error.to_string()))?;
            count += 1;
        }
        if count != 1 {
            return Err(reject("missing or duplicate RequestTracking row".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod recognition_tests {
    use super::*;
    use crate::request_tracking::{ClassifiesOrdered, OrderedClassifier, TracksRequests};
    fn seed() -> Standard {
        Standard {
            configuration: Configuration {
                ordinary: crate::message_library::Ordinary("/run/message.sock".into()),
                meta: crate::message_library::Meta("/run/message-meta.sock".into()),
                flow: crate::message_library::Flow("/run/flow.sock".into()),
            },
            meta_configured: crate::generated::memory::MetaConfigured(false),
        }
    }
    #[test]
    fn current_standard_reopens_without_reseeding_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.sema");
        let store = MessageStore::open(&path, seed()).unwrap();
        let namespace = store.request_tracking().unwrap().namespace;
        let mut configured = store.standard().unwrap();
        configured.meta_configured = crate::generated::memory::MetaConfigured(true);
        store.replace_standard(configured.clone()).unwrap();
        drop(store);
        let reopened = MessageStore::open(&path, seed()).unwrap();
        assert_eq!(reopened.standard().unwrap(), configured);
        assert_eq!(reopened.request_tracking().unwrap().namespace, namespace);
    }

    fn store_namespace(path: &Path) -> String {
        MessageStore::open(path, seed())
            .unwrap()
            .request_tracking()
            .unwrap()
            .namespace
            .0
    }

    #[test]
    fn fresh_message_memories_have_distinct_persisted_namespaces() {
        let one = tempfile::tempdir().unwrap();
        let two = tempfile::tempdir().unwrap();
        let one_path = one.path().join("message.sema");
        let two_path = two.path().join("message.sema");
        let first = store_namespace(&one_path);
        assert_eq!(store_namespace(&one_path), first);
        assert_ne!(store_namespace(&two_path), first);
    }

    #[test]
    fn failed_effect_replacement_replays_before_the_cursor_can_advance() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.sema");
        let store = MessageStore::open(&path, seed()).unwrap();
        let mut tracking = store.request_tracking().unwrap();
        tracking.observe_store("flow-store".into());
        store.replace_request_tracking(tracking).unwrap();
        let address = signal_flow::Address {
            aspect: signal_flow::Aspect::Mind,
            topic: signal_flow::Topic("flow".into()),
            model: signal_flow::Model::Sol,
        };
        let snapshot = signal_flow::VisibleDeliverySnapshot {
            request_id: signal_flow::RequestId("outside".into()),
            delivery_cursor: signal_flow::DeliveryCursor(1),
            visible_sender: signal_flow::VisibleSender::Sender(address.clone()),
            address,
            visible_delivery_state: signal_flow::VisibleDeliveryState::Delivered,
        };
        let mut classifier = OrderedClassifier::default();
        classifier.push(snapshot.clone());
        let write = classifier
            .next_ready(&store.request_tracking().unwrap(), 1)
            .unwrap()
            .unwrap();
        store.fail_next_tracking_replacement();
        assert!(matches!(
            store.replace_request_tracking(write),
            Err(StoreError::InjectedTrackingReplacement)
        ));
        assert_eq!(classifier.held_len(), 1);
        let unchanged = store.request_tracking().unwrap();
        assert!(unchanged.classified_events.is_empty());
        assert_eq!(unchanged.observation_cursor, None);
        drop(store);

        let reopened = MessageStore::open(&path, seed()).unwrap();
        let mut replay = OrderedClassifier::default();
        replay.push(snapshot);
        let write = replay
            .next_ready(&reopened.request_tracking().unwrap(), 1)
            .unwrap()
            .unwrap();
        reopened.replace_request_tracking(write).unwrap();
        replay.commit_front();
        let mut committed = reopened.request_tracking().unwrap();
        committed.advance_cursor(1).unwrap();
        reopened.replace_request_tracking(committed).unwrap();
        drop(reopened);

        let verified = MessageStore::open(&path, seed())
            .unwrap()
            .request_tracking()
            .unwrap();
        assert_eq!(verified.observation_cursor, Some(1));
        assert_eq!(verified.classified_events.len(), 1);
        assert_eq!(verified.classified_events[0].delivery_cursor, 1);
    }
    #[test]
    fn foreign_and_legacy_occupants_are_refused_without_changing_bytes() {
        for version in [2, 3, 5] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("message.sema");
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
        configured.meta_configured = crate::generated::memory::MetaConfigured(true);
        store.replace_standard(configured).unwrap();
        std::process::exit(0);
    }

    #[test]
    fn crash_recognition_preserves_original_and_rejects_foreign_store() {
        for foreign in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("message.sema");
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
                assert!(reopened.standard().unwrap().meta_configured.0);
            }
        }
    }
}
