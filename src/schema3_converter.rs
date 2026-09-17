//! Private, copy-first schema-3 to schema-5 store conversion.
//!
//! This module intentionally has no CLI. The retired store is opened only as
//! a private copy; current data is published by one rename after every family
//! has decoded, been range-checked, and been written.
use crate::runtime_model::{
    InboxRecord, LedgerHead, LedgerRecord, NextMessageSlot, OldestMessageSlot, SenderName, Slots,
    ThreadRecord,
};
use legacy_message::runtime_model::{
    InboxRecord as LegacyInboxRecord, LedgerHead as LegacyLedgerHead,
    LedgerRecord as LegacyLedgerRecord, ThreadRecord as LegacyThreadRecord,
};
use legacy_sema_engine::{
    Engine, EngineOpen, FamilyName, QueryPlan, RecordKey, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, VersionedStoreName, VersioningPolicy,
};
use legacy_signal_message::schema::lib::{z2Vc72 as LegacyAgent, WireShape, WireValue};
use sha2::{Digest, Sha256};
use signal_message::{
    AgentDeathMark, AgentEndpoint, AgentEndpointKind, AgentRegistryEntry, ComponentName,
    ConnectionClass, EndpointSelection, HarnessProcessPin, InternalComponentInstanceOrigin,
    MessageKind, MessageOrigin, OtherPersonaEngine, ProcessPinSelection, ResumeSelection,
    ThreadRelation, ThreadRelationSelection, ThreadSelection,
};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::{collections::BTreeSet, path::Path};

#[derive(Debug, thiserror::Error)]
pub enum Schema3ConversionError {
    #[error("schema3 source is not a nonempty regular file")]
    SourceNotRegularFile,
    #[error("schema3 destination already exists")]
    DestinationExists,
    #[error("schema3 source changed during conversion")]
    SourceChanged,
    #[error("schema3 private copy failed: {0}")]
    PrivateCopy(String),
    #[error("schema3 decode or invariant failed: {0}")]
    Decode(String),
    #[error("schema3 value cannot fit current integer: {0}")]
    Range(String),
    #[error("schema5 destination write failed: {0}")]
    DestinationWrite(String),
}

pub(crate) struct Schema3Snapshot {
    pub(crate) agents: Vec<AgentRegistryEntry>,
    pub(crate) ledger: Vec<LedgerRecord>,
    pub(crate) head: Option<LedgerHead>,
    pub(crate) inbox: Vec<InboxRecord>,
    pub(crate) threads: Vec<ThreadRecord>,
    pub(crate) outbox: Vec<InboxRecord>,
}

/// Convert a schema-3 file into a *new* schema-5 file. Neither an existing
/// destination nor the source is opened by the current writer.
pub fn convert(source: &Path, destination: &Path) -> Result<(), Schema3ConversionError> {
    if !source.is_file()
        || std::fs::metadata(source)
            .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?
            .len()
            == 0
    {
        return Err(Schema3ConversionError::SourceNotRegularFile);
    }
    if destination.exists() {
        return Err(Schema3ConversionError::DestinationExists);
    }
    let original = std::fs::read(source)
        .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?;
    let digest = Sha256::digest(&original);
    let nonce = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?
            .as_nanos()
    );
    let private = std::env::temp_dir().join(format!("message-schema3-convert-{nonce}"));
    let private_source = private.join("messenger.sema");
    let snapshot = (|| {
        #[cfg(unix)]
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&private)
            .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?;
        #[cfg(not(unix))]
        std::fs::create_dir(&private)
            .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?;
        std::fs::write(&private_source, &original)
            .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?;
        #[cfg(unix)]
        std::fs::set_permissions(&private_source, PermissionsExt::from_mode(0o600))
            .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| decode(&private_source)))
            .unwrap_or_else(|_| {
                Err(Schema3ConversionError::Decode(
                    "legacy decoder panic".into(),
                ))
            })
    })();
    let _ = std::fs::remove_dir_all(&private);
    let snapshot = snapshot?;
    source_matches(source, &digest)?;

    let temporary = destination.with_file_name(format!(
        ".{}-schema5-{nonce}",
        destination
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
    ));
    if temporary.exists() {
        return Err(Schema3ConversionError::DestinationExists);
    }
    let write = (|| -> Result<(), Schema3ConversionError> {
        let tables = crate::tables::MessengerTables::open(&temporary)
            .map_err(|error| Schema3ConversionError::DestinationWrite(error.to_string()))?;
        tables
            .import_schema3_snapshot(snapshot)
            .map_err(|error| Schema3ConversionError::DestinationWrite(error.to_string()))?;
        drop(tables);
        source_matches(source, &digest)?;
        // hard_link has create-only semantics: unlike rename it cannot replace a
        // destination another process created after our initial exists check.
        std::fs::hard_link(&temporary, destination)
            .map_err(|error| Schema3ConversionError::DestinationWrite(error.to_string()))?;
        // Publication is committed once the create-only hard link succeeds.
        // Staging cleanup cannot turn that committed outcome into an error. The caller supplies a stopped immutable source; a later cross-file
        // observation cannot safely roll back a destination another writer may
        // have replaced, so the final integrity check is immediately before it.
        Ok(())
    })();
    if write.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write
}

fn source_matches(source: &Path, digest: &[u8]) -> Result<(), Schema3ConversionError> {
    let current = std::fs::read(source)
        .map_err(|error| Schema3ConversionError::PrivateCopy(error.to_string()))?;
    if Sha256::digest(current).as_slice() == digest {
        Ok(())
    } else {
        Err(Schema3ConversionError::SourceChanged)
    }
}

fn decode(path: &Path) -> Result<Schema3Snapshot, Schema3ConversionError> {
    let mut engine = Engine::open(
        EngineOpen::new(path, SchemaVersion::new(3))
            .with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger"))),
    )
    .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?;
    macro_rules! table {
        ($name:literal, $family:literal, $version:expr, $type:ty) => {
            engine
                .register_table::<$type>(TableDescriptor::new(
                    TableName::new($name),
                    FamilyName::new($family),
                    SchemaHash::for_label(format!("messenger-{}-v{}", $family, $version)),
                ))
                .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        };
    }
    let agents = table!("agent_registry", "agent-registry", 2, LegacyAgent);
    let ledger = table!("message_ledger", "message-ledger", 3, LegacyLedgerRecord);
    let head = table!("ledger_head", "message-ledger-head", 3, LegacyLedgerHead);
    let inbox = table!("recipient_inbox", "recipient-inbox", 3, LegacyInboxRecord);
    let thread = table!("thread_index", "thread-index", 3, LegacyThreadRecord);
    let outbox = table!("delivery_outbox", "delivery-outbox", 3, LegacyInboxRecord);
    // `QueryPlan::all` does not expose regular-table keys.  We validate each
    // scanned value against the deployed writer's canonical point lookup; a
    // duplicate embedded identity or a noncanonical/swapped key then refuses.
    macro_rules! validate_keyed {
        ($table:expr, $key:expr, $record:expr, $label:literal) => {{
            let found = engine
                .match_records(QueryPlan::key($table, RecordKey::new($key)))
                .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?;
            if found.records().len() != 1 || found.records().first() != Some(&$record) {
                return Err(Schema3ConversionError::Decode(format!(
                    "{} canonical key",
                    $label
                )));
            }
        }};
    }

    let old_ledger = engine
        .match_records(QueryPlan::all(ledger))
        .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        .records()
        .to_vec();
    let slots: BTreeSet<u64> = old_ledger
        .iter()
        .map(|record| integer_u64(record.message_slot.to_wire(), "message slot"))
        .collect::<Result<_, _>>()?;
    if slots.len() != old_ledger.len() {
        return Err(Schema3ConversionError::Decode(
            "duplicate ledger slot".into(),
        ));
    }
    for record in &old_ledger {
        validate_keyed!(
            ledger,
            format!(
                "{:020}",
                integer_u64(record.message_slot.to_wire(), "message slot")?
            ),
            record.clone(),
            "ledger"
        );
    }
    let old_heads = engine
        .match_records(QueryPlan::all(head))
        .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        .records()
        .to_vec();
    if old_heads.len() > 1 {
        return Err(Schema3ConversionError::Decode(
            "multiple ledger heads".into(),
        ));
    }
    if let Some(record) = old_heads.first() {
        validate_keyed!(head, "head", record.clone(), "ledger head");
    }
    if let Some(head) = old_heads.first() {
        let oldest = integer_u64(head.oldest_message_slot.payload().to_wire(), "oldest slot")?;
        let next = integer_u64(head.next_message_slot.payload().to_wire(), "next slot")?;
        if oldest > next
            || next.saturating_sub(oldest) > 1024
            || slots.iter().any(|slot| *slot < oldest || *slot >= next)
        {
            return Err(Schema3ConversionError::Decode(
                "ledger head invariant".into(),
            ));
        }
    }
    let old_inbox = engine
        .match_records(QueryPlan::all(inbox))
        .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        .records()
        .to_vec();
    let old_threads = engine
        .match_records(QueryPlan::all(thread))
        .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        .records()
        .to_vec();
    let old_outbox = engine
        .match_records(QueryPlan::all(outbox))
        .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        .records()
        .to_vec();
    let agents_scanned = engine
        .match_records(QueryPlan::all(agents))
        .map_err(|error| Schema3ConversionError::Decode(error.to_string()))?
        .records()
        .to_vec();
    let mut agent_keys = BTreeSet::new();
    for record in &agents_scanned {
        let key = record.field_0.payload().clone();
        if !agent_keys.insert(key.clone()) {
            return Err(Schema3ConversionError::Decode(
                "duplicate agent identifier".into(),
            ));
        }
        validate_keyed!(agents, key, record.clone(), "agent registry");
    }
    let mut inbox_keys = BTreeSet::new();
    for record in &old_inbox {
        let key = record.recipient.payload().clone();
        if !inbox_keys.insert(key.clone()) {
            return Err(Schema3ConversionError::Decode(
                "duplicate inbox recipient".into(),
            ));
        }
        validate_keyed!(inbox, key, record.clone(), "inbox");
    }
    let mut outbox_keys = BTreeSet::new();
    for record in &old_outbox {
        let key = record.recipient.payload().clone();
        if !outbox_keys.insert(key.clone()) {
            return Err(Schema3ConversionError::Decode(
                "duplicate outbox recipient".into(),
            ));
        }
        validate_keyed!(outbox, key, record.clone(), "outbox");
    }
    let mut thread_keys = BTreeSet::new();
    for record in &old_threads {
        let key = record.thread_name.payload().clone();
        if !thread_keys.insert(key.clone()) {
            return Err(Schema3ConversionError::Decode(
                "duplicate thread name".into(),
            ));
        }
        validate_keyed!(thread, key, record.clone(), "thread");
    }
    for row in old_inbox.iter().chain(old_outbox.iter()) {
        if legacy_slots(&row.slots)?
            .iter()
            .any(|slot| !slots.contains(slot))
        {
            return Err(Schema3ConversionError::Decode(
                "inbox/outbox reference".into(),
            ));
        }
    }
    for row in &old_threads {
        if legacy_slots(&row.slots)?
            .iter()
            .any(|slot| !slots.contains(slot))
        {
            return Err(Schema3ConversionError::Decode("thread reference".into()));
        }
    }

    Ok(Schema3Snapshot {
        agents: agents_scanned
            .into_iter()
            .map(map_agent)
            .collect::<Result<_, _>>()?,
        ledger: old_ledger
            .into_iter()
            .map(map_ledger)
            .collect::<Result<_, _>>()?,
        head: old_heads.into_iter().next().map(map_head).transpose()?,
        inbox: old_inbox
            .into_iter()
            .map(map_inbox)
            .collect::<Result<_, _>>()?,
        threads: old_threads
            .into_iter()
            .map(map_thread)
            .collect::<Result<_, _>>()?,
        outbox: old_outbox
            .into_iter()
            .map(map_inbox)
            .collect::<Result<_, _>>()?,
    })
}

fn product(value: WireValue, label: &str) -> Result<Vec<WireValue>, Schema3ConversionError> {
    match value {
        WireValue::Product(fields) => Ok(fields),
        _ => Err(Schema3ConversionError::Decode(format!("{label} product"))),
    }
}
fn fields(
    value: WireValue,
    expected: usize,
    label: &str,
) -> Result<Vec<WireValue>, Schema3ConversionError> {
    let fields = product(value, label)?;
    if fields.len() == expected {
        Ok(fields)
    } else {
        Err(Schema3ConversionError::Decode(format!("{label} arity")))
    }
}
fn text(value: WireValue, label: &str) -> Result<String, Schema3ConversionError> {
    match value {
        WireValue::Text(value) => Ok(value),
        _ => Err(Schema3ConversionError::Decode(format!("{label} text"))),
    }
}
fn integer_u64(value: WireValue, label: &str) -> Result<u64, Schema3ConversionError> {
    match value {
        WireValue::Integer(value) => Ok(value),
        _ => Err(Schema3ConversionError::Decode(format!("{label} integer"))),
    }
}
fn variant(value: WireValue, label: &str) -> Result<(u16, Vec<WireValue>), Schema3ConversionError> {
    match value {
        WireValue::Variant { ordinal, fields } => Ok((ordinal, fields)),
        _ => Err(Schema3ConversionError::Decode(format!("{label} variant"))),
    }
}
fn checked(value: u64, label: &str) -> Result<i64, Schema3ConversionError> {
    i64::try_from(value).map_err(|_| Schema3ConversionError::Range(label.into()))
}
fn legacy_slots(
    slots: &legacy_message::runtime_model::Slots,
) -> Result<Vec<u64>, Schema3ConversionError> {
    slots
        .payload()
        .iter()
        .map(|slot| integer_u64(slot.to_wire(), "slot"))
        .collect()
}

fn map_ledger(old: LegacyLedgerRecord) -> Result<LedgerRecord, Schema3ConversionError> {
    Ok(LedgerRecord {
        message_slot: checked(
            integer_u64(old.message_slot.to_wire(), "message slot")?,
            "message slot",
        )?,
        message_submission: map_submission(old.message_submission.to_wire())?,
        message_origin: map_origin(old.message_origin.to_wire())?,
        sender_name: SenderName::new(old.sender_name.into_payload()),
        stamped_at: checked(
            integer_u64(old.stamped_at.to_wire(), "timestamp")?,
            "timestamp",
        )?,
    })
}
fn map_head(old: LegacyLedgerHead) -> Result<LedgerHead, Schema3ConversionError> {
    Ok(LedgerHead {
        next_message_slot: NextMessageSlot::new(checked(
            integer_u64(old.next_message_slot.payload().to_wire(), "next slot")?,
            "next slot",
        )?),
        oldest_message_slot: OldestMessageSlot::new(checked(
            integer_u64(old.oldest_message_slot.payload().to_wire(), "oldest slot")?,
            "oldest slot",
        )?),
    })
}
fn map_inbox(old: LegacyInboxRecord) -> Result<InboxRecord, Schema3ConversionError> {
    Ok(InboxRecord {
        recipient: text(old.recipient.to_wire(), "recipient")?,
        slots: Slots::new(
            legacy_slots(&old.slots)?
                .into_iter()
                .map(|slot| checked(slot, "slot"))
                .collect::<Result<_, _>>()?,
        ),
    })
}
fn map_thread(old: LegacyThreadRecord) -> Result<ThreadRecord, Schema3ConversionError> {
    Ok(ThreadRecord {
        thread_name: text(old.thread_name.to_wire(), "thread name")?,
        thread_relation_selection: map_relation(old.thread_relation_selection.to_wire())?,
        participants: match old.participants.to_wire() {
            WireValue::Sequence(values) => values
                .into_iter()
                .map(|value| text(value, "participant"))
                .collect::<Result<_, _>>()?,
            _ => {
                return Err(Schema3ConversionError::Decode(
                    "participants sequence".into(),
                ))
            }
        },
        slots: Slots::new(
            legacy_slots(&old.slots)?
                .into_iter()
                .map(|slot| checked(slot, "slot"))
                .collect::<Result<_, _>>()?,
        ),
    })
}
fn map_submission(
    value: WireValue,
) -> Result<signal_message::MessageSubmission, Schema3ConversionError> {
    let mut fields = fields(value, 4, "submission")?;
    let thread = fields.pop().unwrap();
    let body = fields.pop().unwrap();
    let kind = fields.pop().unwrap();
    let recipient = fields.pop().unwrap();
    let (ordinal, values) = variant(kind, "message kind")?;
    if !values.is_empty() {
        return Err(Schema3ConversionError::Decode("message kind fields".into()));
    }
    Ok(signal_message::MessageSubmission {
        message_recipient: text(recipient, "recipient")?,
        message_kind: match ordinal {
            0 => MessageKind::Send,
            1 => MessageKind::Inbox,
            _ => {
                return Err(Schema3ConversionError::Decode(
                    "message kind ordinal".into(),
                ))
            }
        },
        message_body: text(body, "body")?,
        thread_selection: match variant(thread, "thread")? {
            (1, values) if values.is_empty() => ThreadSelection::None,
            (0, values) if values.len() == 1 => {
                ThreadSelection::Named(text(values.into_iter().next().unwrap(), "thread name")?)
            }
            _ => return Err(Schema3ConversionError::Decode("thread shape".into())),
        },
    })
}
fn map_relation(value: WireValue) -> Result<ThreadRelationSelection, Schema3ConversionError> {
    match variant(value, "thread relation")? {
        (0, values) if values.is_empty() => Ok(ThreadRelationSelection::None),
        (1, values) if values.len() == 1 => {
            let mut relation = fields(values.into_iter().next().unwrap(), 2, "relation")?;
            let branch = text(relation.pop().unwrap(), "branch")?;
            let repository = text(relation.pop().unwrap(), "repository")?;
            Ok(ThreadRelationSelection::Related(ThreadRelation {
                repository_name: repository,
                feature_branch_name: branch,
            }))
        }
        _ => Err(Schema3ConversionError::Decode(
            "thread relation shape".into(),
        )),
    }
}
fn map_agent(old: LegacyAgent) -> Result<AgentRegistryEntry, Schema3ConversionError> {
    let mut agent_fields = fields(old.to_wire(), 5, "agent")?;
    let pin = agent_fields.pop().unwrap();
    let death = agent_fields.pop().unwrap();
    let resume = agent_fields.pop().unwrap();
    let endpoint = agent_fields.pop().unwrap();
    let identifier = agent_fields.pop().unwrap();
    let (death_ordinal, death_fields) = variant(death, "death")?;
    let agent_death_mark = match (death_ordinal, death_fields.is_empty()) {
        (0, true) => AgentDeathMark::NotDead,
        (1, true) => AgentDeathMark::Killed,
        _ => return Err(Schema3ConversionError::Decode("death shape".into())),
    };
    Ok(AgentRegistryEntry {
        agent_identifier: text(identifier, "agent identifier")?,
        endpoint_selection: match variant(endpoint, "endpoint")? {
            (1, values) if values.is_empty() => EndpointSelection::None,
            (0, values) if values.len() == 1 => {
                let mut endpoint =
                    fields(values.into_iter().next().unwrap(), 2, "endpoint binding")?;
                let path = text(endpoint.pop().unwrap(), "endpoint path")?;
                let (kind, kind_fields) = variant(endpoint.pop().unwrap(), "endpoint kind")?;
                if !kind_fields.is_empty() {
                    return Err(Schema3ConversionError::Decode(
                        "endpoint kind fields".into(),
                    ));
                }
                EndpointSelection::Bound(AgentEndpoint {
                    agent_endpoint_kind: match kind {
                        0 => AgentEndpointKind::HarnessSocket,
                        1 => AgentEndpointKind::PtySocket,
                        _ => {
                            return Err(Schema3ConversionError::Decode(
                                "endpoint kind ordinal".into(),
                            ))
                        }
                    },
                    endpoint_path: path,
                })
            }
            _ => return Err(Schema3ConversionError::Decode("endpoint shape".into())),
        },
        resume_selection: match variant(resume, "resume")? {
            (1, values) if values.is_empty() => ResumeSelection::None,
            (0, values) if values.len() == 1 => {
                ResumeSelection::Resumed(text(values.into_iter().next().unwrap(), "resume")?)
            }
            _ => return Err(Schema3ConversionError::Decode("resume shape".into())),
        },
        agent_death_mark,
        process_pin_selection: match variant(pin, "pin")? {
            (1, values) if values.is_empty() => ProcessPinSelection::None,
            (0, values) if values.len() == 1 => {
                let mut pin = fields(values.into_iter().next().unwrap(), 2, "pin")?;
                let start = checked(
                    integer_u64(pin.pop().unwrap(), "harness start")?,
                    "harness start",
                )?;
                let pid = checked(
                    integer_u64(pin.pop().unwrap(), "harness pid")?,
                    "harness pid",
                )?;
                ProcessPinSelection::Pinned(HarnessProcessPin {
                    harness_pid: pid,
                    harness_start_time: start,
                })
            }
            _ => return Err(Schema3ConversionError::Decode("pin shape".into())),
        },
    })
}
fn map_origin(value: WireValue) -> Result<MessageOrigin, Schema3ConversionError> {
    match variant(value, "origin")? {
        (0, values) if values.len() == 1 => Ok(MessageOrigin::External(map_connection(
            values.into_iter().next().unwrap(),
        )?)),
        (1, values) if values.len() == 1 => {
            let mut instance = fields(values.into_iter().next().unwrap(), 2, "component instance")?;
            let instance_name = text(instance.pop().unwrap(), "component instance name")?;
            let component_name = map_component(instance.pop().unwrap())?;
            Ok(MessageOrigin::InternalComponentInstance(
                InternalComponentInstanceOrigin {
                    component_name,
                    component_instance_name: instance_name,
                },
            ))
        }
        (2, values) if values.len() == 1 => Ok(MessageOrigin::Internal(map_component(
            values.into_iter().next().unwrap(),
        )?)),
        _ => Err(Schema3ConversionError::Decode("origin shape".into())),
    }
}
fn map_connection(value: WireValue) -> Result<ConnectionClass, Schema3ConversionError> {
    match variant(value, "connection")? {
        (0, values) if values.is_empty() => Ok(ConnectionClass::Owner),
        (1, values) if values.len() == 1 => Ok(ConnectionClass::Network(text(
            values.into_iter().next().unwrap(),
            "network",
        )?)),
        (2, values) if values.len() == 1 => {
            let mut engine = fields(values.into_iter().next().unwrap(), 2, "other persona")?;
            let host = text(engine.pop().unwrap(), "other persona host")?;
            let id = text(engine.pop().unwrap(), "other persona id")?;
            Ok(ConnectionClass::OtherPersona(OtherPersonaEngine {
                engine_identifier: id,
                host,
            }))
        }
        (3, values) if values.len() == 1 => Ok(ConnectionClass::System(text(
            values.into_iter().next().unwrap(),
            "system",
        )?)),
        (4, values) if values.len() == 1 => Ok(ConnectionClass::NonOwnerUser(checked(
            integer_u64(values.into_iter().next().unwrap(), "unix user")?,
            "unix user",
        )?)),
        _ => Err(Schema3ConversionError::Decode("connection shape".into())),
    }
}
fn map_component(value: WireValue) -> Result<ComponentName, Schema3ConversionError> {
    let (ordinal, values) = variant(value, "component")?;
    if !values.is_empty() {
        return Err(Schema3ConversionError::Decode("component fields".into()));
    }
    Ok(match ordinal {
        0 => ComponentName::Introspect,
        1 => ComponentName::Terminal,
        2 => ComponentName::System,
        3 => ComponentName::Mind,
        4 => ComponentName::Spirit,
        5 => ComponentName::Message,
        6 => ComponentName::Harness,
        7 => ComponentName::Router,
        8 => ComponentName::Orchestrate,
        _ => return Err(Schema3ConversionError::Decode("component ordinal".into())),
    })
}
