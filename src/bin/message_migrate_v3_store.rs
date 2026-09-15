//! One-time offline migration of a 0.11.1 (schema v3) messenger store.
//!
//! The 0.12 contract changed archived producer layouts, so v3 records cannot
//! be re-stamped. This tool never changes its source: it makes an exact
//! sibling backup, copies each durable v3 row into an opaque archive in a new
//! v5 store, and leaves that archive out of the daemon's active tables.

use std::path::{Path, PathBuf};

use message::MessengerTables;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};

const SEMA_META: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const ARCHIVE: TableDefinition<&str, &[u8]> = TableDefinition::new("legacy_v3_archive");
const V3_DATA_TABLES: [&str; 6] = [
    "agent_registry",
    "delivery_outbox",
    "ledger_head",
    "message_ledger",
    "recipient_inbox",
    "thread_index",
];

fn usage() -> ! {
    eprintln!("usage: message-migrate-v3-store --source V3_COPY --destination V5_STORE");
    std::process::exit(2);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    if arguments.next().as_deref() != Some("--source") {
        usage();
    }
    let source = PathBuf::from(arguments.next().unwrap_or_else(|| usage()));
    if arguments.next().as_deref() != Some("--destination") {
        usage();
    }
    let destination = PathBuf::from(arguments.next().unwrap_or_else(|| usage()));
    if arguments.next().is_some() || source == destination || destination.exists() {
        usage();
    }
    let backup = PathBuf::from(format!("{}.v3-backup", destination.display()));
    if backup.exists() {
        return Err(format!("backup already exists: {}", backup.display()).into());
    }
    std::fs::copy(&source, &backup)?;
    let inspection = PathBuf::from(format!("{}.v3-inspection", destination.display()));
    if inspection.exists() {
        return Err(format!("inspection copy already exists: {}", inspection.display()).into());
    }
    std::fs::copy(&backup, &inspection)?;
    let archive = collect_v3_rows(&inspection)?;
    MessengerTables::open(&destination)?;
    let database = Database::open(&destination)?;
    let transaction = database.begin_write()?;
    {
        let mut table = transaction.open_table(ARCHIVE)?;
        for (key, value) in &archive {
            table.insert(key.as_str(), value.as_slice())?;
        }
    }
    transaction.commit()?;
    println!(
        "migrated_schema=5 archived_rows={} backup={}",
        archive.len(),
        backup.display()
    );
    Ok(())
}

fn collect_v3_rows(source: &Path) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
    let database = Database::open(source)?;
    let transaction = database.begin_read()?;
    let schema = transaction
        .open_table(SEMA_META)?
        .get("schema_version")?
        .map(|value| value.value());
    if schema != Some(3) {
        return Err("source is not a schema-v3 messenger store".into());
    }
    let names = transaction
        .list_tables()?
        .map(|handle| handle.name().to_owned())
        .collect::<Vec<_>>();
    for name in names.iter().filter(|name| !name.starts_with("__sema_")) {
        if !V3_DATA_TABLES.contains(&name.as_str()) {
            return Err(format!("unsupported v3 table: {name}").into());
        }
    }
    let mut archive = Vec::new();
    for name in V3_DATA_TABLES {
        if !names.contains(&name.to_owned()) {
            continue;
        }
        let table = transaction.open_table(TableDefinition::<String, &[u8]>::new(name))?;
        for entry in table.iter()? {
            let (key, value) = entry?;
            archive.push((format!("{name}:{}", key.value()), value.value().to_vec()));
        }
    }
    Ok(archive)
}
