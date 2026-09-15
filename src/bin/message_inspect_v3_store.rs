//! Offline, payload-free inspection for a 0.11.x messenger store.
//!
//! The tool opens a supplied copy of `messenger.sema` and reports only its
//! schema stamp and table row counts. It never reads or prints a row value, so
//! it can decide whether an empty-store transition is safe without exposing
//! message content. Redb may update metadata while opening a file; never pass
//! the active store or an archival original.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition, TableHandle};

const SEMA_META: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");

fn usage() -> ! {
    eprintln!("usage: message-inspect-v3-store --source COPIED_STORE");
    std::process::exit(2);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    if arguments.next().as_deref() != Some("--source") {
        usage();
    }
    let source = arguments.next().unwrap_or_else(|| usage());
    if arguments.next().is_some() {
        usage();
    }
    let source = Path::new(&source);
    let database = Database::open(source)?;
    let transaction = database.begin_read()?;
    let schema_version = transaction
        .open_table(SEMA_META)?
        .get("schema_version")?
        .map(|value| value.value())
        .ok_or("store has no schema version")?;
    let mut tables = transaction
        .list_tables()?
        .map(|handle| handle.name().to_owned())
        .collect::<Vec<_>>();
    tables.sort();
    println!("schema_version={schema_version}");
    for name in tables {
        if name.starts_with("__sema_") {
            println!("table={name} rows=internal");
            continue;
        }
        let table = transaction.open_table(TableDefinition::<String, &[u8]>::new(name.as_str()))?;
        println!("table={name} rows={}", table.len()?);
    }
    Ok(())
}
