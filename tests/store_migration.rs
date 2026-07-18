//! Witnesses for the additive v2 -> v3 store migration.
//!
//! Production's `messenger.sema` was born at store version 2 (2026-07-18,
//! the messenger's first deployment); the messenger promotion bumped the
//! store to v3 with the agent registry's layout unchanged. These pin the
//! migration contract: a v2 store is preserved aside, re-stamped, and
//! re-opened with its registry rows carried and the new families empty; a
//! second open needs no repair; an unknown prior still fails closed with no
//! preserve taken.
//!
//! The production-snapshot witness runs only when the local fixture
//! directory (`MESSAGE_MIGRATION_FIXTURE_DIRECTORY`, no default) holds the
//! captured store — real store content never ships in the repository.

use std::path::{Path, PathBuf};

use message::schema::signal::{
    AgentDeathMark, AgentIdentifier, AgentRegistryEntry, AgentRegistryQuery, EndpointSelection,
    ProcessPinSelection, ResumeSelection,
};
use message::MessengerTables;
use sema_engine::{
    Engine, EngineOpen, FamilyName, KeyedAssertion, RecordKey, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, VersionedStoreName, VersioningPolicy,
};
use tempfile::TempDir;

const PRIOR_STORE_VERSION: SchemaVersion = SchemaVersion::new(2);
const UNKNOWN_PRIOR_VERSION: SchemaVersion = SchemaVersion::new(1);

fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("messenger.sema")
}

/// A store exactly as the deployed v0.8.0 daemon left it: stamped v2,
/// carrying the agent-registry family under its v2 catalog identity,
/// optionally with one seated row.
fn bear_prior_version_store(path: &Path, seated: Option<&str>) {
    let mut engine = Engine::open(
        EngineOpen::new(path, PRIOR_STORE_VERSION)
            .with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger"))),
    )
    .expect("open v2 store");
    let registry = engine
        .register_table::<AgentRegistryEntry>(TableDescriptor::new(
            TableName::new("agent_registry"),
            FamilyName::new("agent-registry"),
            SchemaHash::for_label("messenger-agent-registry-v2"),
        ))
        .expect("register v2 agent registry");
    if let Some(identifier) = seated {
        engine
            .assert_keyed(KeyedAssertion::new(
                registry,
                RecordKey::new(identifier),
                AgentRegistryEntry {
                    agent_identifier: AgentIdentifier::new(identifier.to_owned()),
                    endpoint_selection: EndpointSelection::None,
                    resume_selection: ResumeSelection::None,
                    agent_death_mark: AgentDeathMark::NotDead,
                    process_pin_selection: ProcessPinSelection::None,
                },
            ))
            .expect("seat v2 registry row");
    }
}

fn premigration_preserves(directory: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(directory)
        .expect("read store directory")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("premigration"))
        })
        .collect()
}

#[test]
fn v2_store_opens_carries_rows_and_leaves_one_preserve() {
    let root = TempDir::new().expect("tempdir");
    let store = store_path(&root);
    bear_prior_version_store(&store, Some("x7f2"));

    let tables = MessengerTables::open(&store).expect("v2 store opens through the migration");

    let carried = tables
        .registry_entry("x7f2")
        .expect("query carried row")
        .expect("the v2 row survives the re-stamp");
    assert_eq!(carried.agent_identifier, AgentIdentifier::new("x7f2".to_owned()));
    let preserves = premigration_preserves(root.path());
    assert_eq!(
        preserves.len(),
        1,
        "exactly one pre-migration preserve beside the store: {preserves:?}"
    );
}

#[test]
fn migrated_store_reopens_without_a_second_preserve() {
    let root = TempDir::new().expect("tempdir");
    let store = store_path(&root);
    bear_prior_version_store(&store, None);

    drop(MessengerTables::open(&store).expect("first open migrates"));
    drop(MessengerTables::open(&store).expect("second open needs no repair"));

    assert_eq!(
        premigration_preserves(root.path()).len(),
        1,
        "the second open takes no new preserve"
    );
}

#[test]
fn unknown_prior_version_fails_closed_without_a_preserve() {
    let root = TempDir::new().expect("tempdir");
    let store = store_path(&root);
    drop(
        Engine::open(
            EngineOpen::new(&store, UNKNOWN_PRIOR_VERSION)
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger"))),
        )
        .expect("bear a v1-stamped store"),
    );

    MessengerTables::open(&store).expect_err("a v1 store is not additive and fails closed");

    assert!(
        premigration_preserves(root.path()).is_empty(),
        "no preserve is taken for an unrepairable store"
    );
}

#[test]
fn captured_production_snapshot_migrates_when_fixture_present() {
    let Ok(directory) = std::env::var("MESSAGE_MIGRATION_FIXTURE_DIRECTORY") else {
        eprintln!("skipped: MESSAGE_MIGRATION_FIXTURE_DIRECTORY unset");
        return;
    };
    let fixture = Path::new(&directory).join("messenger.sema.v2-preredeploy-0101-20260718T184652Z");
    if !fixture.exists() {
        eprintln!("skipped: fixture absent at {}", fixture.display());
        return;
    }
    let root = TempDir::new().expect("tempdir");
    let store = store_path(&root);
    std::fs::copy(&fixture, &store).expect("copy fixture aside");

    let tables =
        MessengerTables::open(&store).expect("the captured production store opens and migrates");

    let entries = tables
        .query_entries(&AgentRegistryQuery::All)
        .expect("registry readable after migration");
    assert!(
        entries.is_empty(),
        "the captured store's registry was empty at capture"
    );
    assert_eq!(premigration_preserves(root.path()).len(), 1);
    drop(tables);
    drop(MessengerTables::open(&store).expect("captured store reopens without repair"));
}
