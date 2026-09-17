//! Behavioral proof for private schema-3 to schema-5 conversion.
//!
//! The source fixture is created exclusively with the historical 0.11.1
//! public API. Conversion only ever opens a private copy of that source.

use legacy_message::{
    runtime_model::{LedgerDraft, SenderName},
    MessengerTables as LegacyMessengerTables,
};
use legacy_signal_message::schema::lib::{
    z2VNPW, z2VNcG, z2VPn2, z2VTJ1, z2VTiK, z2VXMQ, z2VY18, z2VY2v, z2VY3v, z2Vari, z2Vcfd, z2VdsV,
    z2VevD, z2Vf2p,
};
use message::{
    schema3_converter::{convert, Schema3ConversionError},
    MessengerTables,
};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn historical_draft(recipient: &str, body: &str, threaded: bool) -> LedgerDraft {
    LedgerDraft {
        message_submission: z2VY2v {
            field_0: z2Vari::new(recipient.to_owned()),
            field_1: z2VdsV::z2VXeo,
            field_2: z2VNcG::new(body.to_owned()),
            field_3: if threaded {
                z2VTiK::z2VPTM(legacy_signal_message::schema::lib::z2VUSt::new(
                    "migration-proof".to_owned(),
                ))
            } else {
                z2VTiK::z2VR2m
            },
        },
        message_origin: z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(1000))),
        sender_name: SenderName::new("historical-sender".to_owned()),
        stamped_at: z2VY18::new(z2Vf2p::new(1)),
    }
}

/// All six schema-3 durable families, including one pending outbox reference.
fn historical_store(directory: &Path) -> PathBuf {
    let path = directory.join("messenger.sema");
    let tables = LegacyMessengerTables::open(&path).expect("historical schema-3 store opens");
    tables
        .seat_identity(&z2VevD {
            field_0: z2VNPW::new("recipient".to_owned()),
            field_1: z2Vcfd::z2VRLv,
            field_2: z2VXMQ::z2VNZi,
        })
        .expect("historical registry record");
    let first = tables
        .store_submission(&historical_draft(
            "recipient",
            "first historical ledger row",
            true,
        ))
        .expect("historical threaded ledger record");
    tables
        .store_submission(&historical_draft(
            "recipient",
            "second historical ledger row",
            false,
        ))
        .expect("historical inbox ledger record");
    tables
        .append_outbox_slot("recipient", *first.payload().payload())
        .expect("historical pending outbox record");
    drop(tables);
    path
}

fn historical_range_store(directory: &Path) -> PathBuf {
    let path = directory.join("range.sema");
    let tables = LegacyMessengerTables::open(&path).expect("historical range store opens");
    let mut draft = historical_draft("recipient", "range fixture", false);
    draft.message_origin = z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(u64::MAX)));
    tables
        .store_submission(&draft)
        .expect("historical range ledger record");
    drop(tables);
    path
}

fn hash(path: &Path) -> Vec<u8> {
    Sha256::digest(fs::read(path).expect("fixture bytes")).to_vec()
}

#[test]
fn converts_all_six_families_and_preserves_pending_outbox_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let source = historical_store(directory.path());
    let destination = directory.path().join("schema5.sema");
    let source_before = hash(&source);

    convert(&source, &destination).expect("valid schema-3 archive converts");
    assert_eq!(hash(&source), source_before, "source is never restamped");

    let tables = MessengerTables::open(&destination).expect("current store opens");
    let identity = tables
        .registry_entry("recipient")
        .unwrap()
        .expect("registry preserved");
    assert_eq!(identity.agent_identifier, "recipient");
    assert_eq!(tables.outbox_slots("recipient").unwrap(), vec![0]);
    let inbox = tables.inbox_entries(&"recipient".to_owned()).unwrap();
    assert_eq!(inbox.len(), 2);
    assert_eq!(inbox[0].message_sender, "historical-sender");
    assert_eq!(inbox[0].message_body, "first historical ledger row");
    assert_eq!(inbox[1].message_body, "second historical ledger row");
    let thread = tables
        .thread_contents(&"migration-proof".to_owned())
        .unwrap()
        .expect("thread preserved");
    assert_eq!(thread.participants, vec!["historical-sender", "recipient"]);
    assert_eq!(thread.thread_entries.len(), 1);
    assert_eq!(
        thread.thread_entries[0].message_body,
        "first historical ledger row"
    );
    drop(tables);

    let reopened = MessengerTables::open(&destination).expect("current store reopens");
    assert_eq!(reopened.outbox_slots("recipient").unwrap(), vec![0]);
    assert_eq!(
        reopened
            .ledger_record_public(0)
            .unwrap()
            .unwrap()
            .message_submission
            .message_body,
        "first historical ledger row"
    );
    assert_eq!(
        reopened
            .inbox_entries(&"recipient".to_owned())
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn invalid_pending_reference_refuses_without_destination_or_source_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let source = historical_store(directory.path());
    LegacyMessengerTables::open(&source)
        .unwrap()
        .append_outbox_slot("recipient", 999)
        .unwrap();
    let source_before = hash(&source);
    let destination = directory.path().join("must-not-publish.sema");

    assert!(matches!(
        convert(&source, &destination),
        Err(Schema3ConversionError::Decode(_))
    ));
    assert_eq!(hash(&source), source_before);
    assert!(!destination.exists());
}

#[test]
fn out_of_range_legacy_integer_refuses_without_destination_or_source_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let source = historical_range_store(directory.path());
    let source_before = hash(&source);
    let destination = directory.path().join("must-not-publish.sema");

    assert!(matches!(
        convert(&source, &destination),
        Err(Schema3ConversionError::Range(_))
    ));
    assert_eq!(hash(&source), source_before);
    assert!(!destination.exists());
}

#[test]
fn corrupt_source_refuses_without_destination_or_source_mutation() {
    let directory = tempfile::tempdir().unwrap();
    let source = historical_store(directory.path());
    let bytes = fs::read(&source).unwrap();
    fs::write(&source, &bytes[..bytes.len() / 2]).unwrap();
    let source_before = hash(&source);
    let destination = directory.path().join("must-not-publish.sema");

    assert!(matches!(
        convert(&source, &destination),
        Err(Schema3ConversionError::Decode(_))
    ));
    assert_eq!(hash(&source), source_before);
    assert!(!destination.exists());
}

#[test]
fn existing_or_same_destination_is_never_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let source = historical_store(directory.path());
    let source_before = hash(&source);
    let destination = directory.path().join("existing.sema");
    fs::write(&destination, b"preexisting destination").unwrap();
    let destination_before = fs::read(&destination).unwrap();

    assert!(matches!(
        convert(&source, &destination),
        Err(Schema3ConversionError::DestinationExists)
    ));
    assert_eq!(hash(&source), source_before);
    assert_eq!(fs::read(&destination).unwrap(), destination_before);

    assert!(matches!(
        convert(&source, &source),
        Err(Schema3ConversionError::DestinationExists)
    ));
    assert_eq!(hash(&source), source_before);
}
