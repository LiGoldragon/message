use message::MessengerTables;
use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use signal_message::{
    AgentIdentityAssignment, AgentRegistryQuery, ProcessPinSelection, ResumeSelection,
};
use std::process::Command;

#[test]
fn v3_copy_moves_to_a_fresh_store_with_an_opaque_archive_and_exact_backup() {
    const SEMA_META: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
    const ARCHIVE: TableDefinition<&str, &[u8]> = TableDefinition::new("legacy_v3_archive");
    const TABLES: [&str; 6] = [
        "agent_registry",
        "delivery_outbox",
        "ledger_head",
        "message_ledger",
        "recipient_inbox",
        "thread_index",
    ];
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("legacy.sema");
    let destination = directory.path().join("migrated.sema");
    {
        let database = Database::create(&source).unwrap();
        let transaction = database.begin_write().unwrap();
        transaction
            .open_table(SEMA_META)
            .unwrap()
            .insert("schema_version", 3_u64)
            .unwrap();
        for table_name in TABLES {
            let mut table = transaction
                .open_table(TableDefinition::<String, &[u8]>::new(table_name))
                .unwrap();
            table
                .insert(format!("{table_name}-id"), [1_u8, 2, 3].as_slice())
                .unwrap();
        }
        transaction.commit().unwrap();
    }
    let source_bytes = std::fs::read(&source).unwrap();
    let outcome = Command::new(env!("CARGO_BIN_EXE_message-migrate-v3-store"))
        .args([
            "--source",
            source.to_str().unwrap(),
            "--destination",
            destination.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        outcome.status.success(),
        "{}",
        String::from_utf8_lossy(&outcome.stderr)
    );
    assert_eq!(std::fs::read(&source).unwrap(), source_bytes);
    assert_eq!(
        std::fs::read(format!("{}.v3-backup", destination.display())).unwrap(),
        source_bytes
    );
    MessengerTables::open(&destination).unwrap();
    let database = Database::open(&destination).unwrap();
    let transaction = database.begin_read().unwrap();
    let archive = transaction.open_table(ARCHIVE).unwrap();
    assert_eq!(archive.len().unwrap(), 6);
    assert_eq!(
        archive
            .get("delivery_outbox:delivery_outbox-id")
            .unwrap()
            .unwrap()
            .value(),
        [1_u8, 2, 3]
    );
}

#[test]
fn current_store_reopens_without_repair_or_identity_loss() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("messenger.sema");
    {
        let tables = MessengerTables::open(&path).unwrap();
        tables
            .seat_identity(&AgentIdentityAssignment {
                agent_identifier: "persistent".to_owned(),
                process_pin_selection: ProcessPinSelection::None,
                resume_selection: ResumeSelection::None,
            })
            .unwrap();
    }
    let reopened = MessengerTables::open(&path).unwrap();
    let entries = reopened.query_entries(&AgentRegistryQuery::All).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].agent_identifier, "persistent");
}

/// The Datom-stack move changed the archived layout of every durable record,
/// so a store written by the previous schema must be refused rather than
/// re-stamped forward and read as if its bytes matched. Silent
/// misinterpretation of a production store is the failure this guards.
#[test]
fn a_store_from_the_previous_schema_is_refused_rather_than_re_stamped() {
    const SEMA_META: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("__sema_meta");

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("messenger.sema");
    {
        let tables = MessengerTables::open(&path).unwrap();
        tables
            .seat_identity(&AgentIdentityAssignment {
                agent_identifier: "before".to_owned(),
                process_pin_selection: ProcessPinSelection::None,
                resume_selection: ResumeSelection::None,
            })
            .unwrap();
    }

    // Stamp the file back to the pre-Datom schema version.
    {
        let database = redb::Database::create(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(SEMA_META).unwrap();
            table.insert("schema_version", 3_u64).unwrap();
        }
        transaction.commit().unwrap();
    }

    let outcome = MessengerTables::open(&path);
    assert!(
        outcome.is_err(),
        "a store stamped at the previous schema must fail closed, not open"
    );
}
