//! Offline-only reader for the deployed schema-3 archive graph.
//!
//! This never writes the supplied store.  It is deliberately a separate
//! legacy boundary: normal daemon opening remains fail-closed on schema 3.
use legacy_sema_engine::{Engine, EngineOpen, FamilyName, QueryPlan, SchemaHash, SchemaVersion, TableDescriptor, TableName, VersionedStoreName, VersioningPolicy};
use legacy_signal_message::schema::lib::{z2VLZR, z2VMa5, z2VTJ1, z2VUSt, z2VVDs, z2VY18, z2VY2v, z2Vari, z2Vc72};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
#[cfg(unix)] use std::os::unix::fs::PermissionsExt;
use sha2::{Digest, Sha256};

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct SenderName(String);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct LedgerRecord { message_slot: z2VLZR, message_submission: z2VY2v, message_origin: z2VTJ1, sender_name: SenderName, stamped_at: z2VY18 }
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct NextMessageSlot(z2VLZR);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct OldestMessageSlot(z2VLZR);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct LedgerHead { next_message_slot: NextMessageSlot, oldest_message_slot: OldestMessageSlot }
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct Slots(Vec<z2VLZR>);
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct InboxRecord { recipient: z2Vari, slots: Slots }
#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)] struct ThreadRecord { thread_name: z2VUSt, thread_relation_selection: z2VVDs, participants: z2VMa5, slots: Slots }

#[derive(Debug, Clone, PartialEq, Eq)] pub struct Schema3Probe { pub agent_registry: usize, pub message_ledger: usize, pub ledger_head: usize, pub recipient_inbox: usize, pub thread_index: usize, pub delivery_outbox: usize }
#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum Schema3ProbeRefusal { InputNotRegularFile, InputUnreadable, PrivateCopyUnavailable, SourceChanged, LegacyDecodeOrInvariant, PrivateCleanup }
#[derive(Debug, Clone, PartialEq, Eq)] pub enum Schema3ProbeOutcome { Observed(Schema3Probe), Refused(Schema3ProbeRefusal) }

pub fn probe(path: &Path) -> Schema3ProbeOutcome {
 fn refused(r:Schema3ProbeRefusal)->Schema3ProbeOutcome{Schema3ProbeOutcome::Refused(r)}
 if !path.is_file() { return refused(Schema3ProbeRefusal::InputNotRegularFile) }
 let Ok(before)=std::fs::read(path) else{return refused(Schema3ProbeRefusal::InputUnreadable)};
 let digest=Sha256::digest(&before);
 let Ok(nanos)=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else{return refused(Schema3ProbeRefusal::PrivateCopyUnavailable)};
 let temporary=std::env::temp_dir().join(format!("message-schema3-probe-{}-{}",std::process::id(),nanos.as_nanos()));
 if std::fs::create_dir(&temporary).is_err(){return refused(Schema3ProbeRefusal::PrivateCopyUnavailable)};
 #[cfg(unix)] if std::fs::set_permissions(&temporary,std::fs::Permissions::from_mode(0o700)).is_err(){let _=std::fs::remove_dir_all(&temporary);return refused(Schema3ProbeRefusal::PrivateCopyUnavailable)};
 let copy=temporary.join("messenger.sema");
 if std::fs::write(&copy,&before).is_err(){let _=std::fs::remove_dir_all(&temporary);return refused(Schema3ProbeRefusal::PrivateCopyUnavailable)};
 #[cfg(unix)] if std::fs::set_permissions(&copy,std::fs::Permissions::from_mode(0o600)).is_err(){let _=std::fs::remove_dir_all(&temporary);return refused(Schema3ProbeRefusal::PrivateCopyUnavailable)};
 let result=probe_copy(&copy);
 if std::fs::remove_dir_all(&temporary).is_err(){return refused(Schema3ProbeRefusal::PrivateCleanup)};
 let Ok(after)=std::fs::read(path) else{return refused(Schema3ProbeRefusal::SourceChanged)};
 if Sha256::digest(&after)!=digest { return refused(Schema3ProbeRefusal::SourceChanged) }
 match result { Ok(value)=>Schema3ProbeOutcome::Observed(value), Err(_)=>refused(Schema3ProbeRefusal::LegacyDecodeOrInvariant) }
}

fn probe_copy(path: &Path) -> Result<Schema3Probe, String> {
 let mut e=Engine::open(EngineOpen::new(path,SchemaVersion::new(3)).with_versioning(VersioningPolicy::new(VersionedStoreName::new("messenger")))).map_err(|x|x.to_string())?;
 macro_rules! table {($n:literal,$f:literal,$v:expr,$t:ty)=>{ e.register_table::<$t>(TableDescriptor::new(TableName::new($n),FamilyName::new($f),SchemaHash::for_label(format!("messenger-{}-v{}",$f,$v)))).map_err(|x|x.to_string())? };}
 let agents=table!("agent_registry","agent-registry",2,z2Vc72); let ledger=table!("message_ledger","message-ledger",3,LedgerRecord); let head=table!("ledger_head","message-ledger-head",3,LedgerHead); let inbox=table!("recipient_inbox","recipient-inbox",3,InboxRecord); let thread=table!("thread_index","thread-index",3,ThreadRecord); let outbox=table!("delivery_outbox","delivery-outbox",3,InboxRecord);
 let l=e.match_records(QueryPlan::all(ledger)).map_err(|x|x.to_string())?.records().to_vec(); let slots:BTreeSet<u64>=l.iter().map(|r|*r.message_slot.payload()).collect();
 if slots.len()!=l.len(){return Err("schema3 duplicate ledger slot".into())}
 let heads=e.match_records(QueryPlan::all(head)).map_err(|x|x.to_string())?.records().to_vec();
 if heads.len()>1{return Err("schema3 multiple ledger heads".into())}
 if let Some(h)=heads.first(){let oldest=*h.oldest_message_slot.0.payload();let next=*h.next_message_slot.0.payload();if oldest>next || next.saturating_sub(oldest)>1024 || slots.iter().any(|slot|*slot<oldest||*slot>=next){return Err("schema3 ledger head invariant failed".into())}}
 let check=|rows:Vec<InboxRecord>| -> Result<usize,String> { for row in &rows { for slot in &row.slots.0 { if !slots.contains(slot.payload()) { return Err("schema3 reference invariant failed".into()) } } } Ok(rows.len()) };
 let inbox_rows=e.match_records(QueryPlan::all(inbox)).map_err(|x|x.to_string())?.records().to_vec(); let thread_rows=e.match_records(QueryPlan::all(thread)).map_err(|x|x.to_string())?.records().to_vec(); let out_rows=e.match_records(QueryPlan::all(outbox)).map_err(|x|x.to_string())?.records().to_vec();
 for row in &thread_rows { for slot in &row.slots.0 { if !slots.contains(slot.payload()) { return Err("schema3 reference invariant failed".into()) } } }
 Ok(Schema3Probe { agent_registry:e.match_records(QueryPlan::all(agents)).map_err(|x|x.to_string())?.records().len(), message_ledger:l.len(), ledger_head:heads.len(), recipient_inbox:check(inbox_rows)?, thread_index:thread_rows.len(), delivery_outbox:check(out_rows)? })
}
