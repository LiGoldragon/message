use message::MessengerTables;
use signal_message::{
    AgentIdentityAssignment, AgentRegistryQuery, ProcessPinSelection, ResumeSelection,
};

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

#[test]
fn v4_catalog_and_rows_survive_the_additive_v5_relay_family() {
    const SEMA_META: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("__sema_meta");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("messenger.sema");
    {
        let tables = MessengerTables::open(&path).unwrap();
        tables.seat_identity(&AgentIdentityAssignment { agent_identifier: "v4-row".into(), process_pin_selection: ProcessPinSelection::None, resume_selection: ResumeSelection::None }).unwrap();
    }
    {
        let database = redb::Database::create(&path).unwrap(); let transaction = database.begin_write().unwrap();
        transaction.open_table(SEMA_META).unwrap().insert("schema_version", 4_u64).unwrap(); transaction.commit().unwrap();
    }
    let reopened = MessengerTables::open(&path).unwrap();
    assert_eq!(reopened.query_entries(&AgentRegistryQuery::All).unwrap()[0].agent_identifier, "v4-row");
}
