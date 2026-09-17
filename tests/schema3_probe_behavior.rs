//! Behavioral proof for the schema-3 inspection boundary.
//!
//! The fixture is authored by the exact historical Message 0.11.1 API.  It
//! is deliberately not made by the probe or by hand-written archive bytes.

use legacy_message::{MessengerTables as LegacyMessengerTables, runtime_model::{LedgerDraft, SenderName}};
use legacy_signal_message::schema::lib::{
    z2VNPW, z2VNcG, z2VPn2, z2VTJ1, z2VTiK, z2VXMQ, z2VY2v, z2VY3v, z2VY18, z2Vari, z2Vcfd,
    z2VdsV, z2VevD, z2Vf2p,
};
use message::schema3_probe::{probe, Schema3ProbeOutcome, Schema3ProbeRefusal};
use sha2::{Digest, Sha256};
use std::{fs, path::{Path, PathBuf}, process::Command};

fn historical_draft(recipient: &str, body: &str, threaded: bool) -> LedgerDraft {
    LedgerDraft {
        message_submission: z2VY2v {
            field_0: z2Vari::new(recipient.to_owned()),
            field_1: z2VdsV::z2VXeo,
            field_2: z2VNcG::new(body.to_owned()),
            field_3: if threaded {
                z2VTiK::z2VPTM(legacy_signal_message::schema::lib::z2VUSt::new("migration-proof".to_owned()))
            } else { z2VTiK::z2VR2m },
        },
        message_origin: z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(1000))),
        sender_name: SenderName::new("historical-sender".to_owned()),
        stamped_at: z2VY18::new(z2Vf2p::new(1)),
    }
}

/// Make a real deployed-shape store by the historical producer's public API.
/// It includes every family: registry, ledger, head, inbox, thread index, and
/// pending outbox.
fn historical_store(directory: &Path) -> PathBuf {
    let path = directory.join("messenger.sema");
    let tables = LegacyMessengerTables::open(&path).expect("historical schema-3 store opens");
    tables.seat_identity(&z2VevD {
        field_0: z2VNPW::new("recipient".to_owned()),
        field_1: z2Vcfd::z2VRLv,
        field_2: z2VXMQ::z2VNZi,
    }).expect("historical registry record");
    let first = tables.store_submission(&historical_draft("recipient", "first historical ledger row", true))
        .expect("historical threaded ledger record");
    tables.store_submission(&historical_draft("recipient", "second historical ledger row", false))
        .expect("historical inbox ledger record");
    tables.append_outbox_slot("recipient", *first.payload().payload())
        .expect("historical pending outbox record");
    drop(tables);
    path
}

fn hash(path: &Path) -> Vec<u8> { Sha256::digest(fs::read(path).expect("fixture bytes")).to_vec() }

fn observed(path: &Path) -> message::schema3_probe::Schema3Probe {
    match probe(path) {
        Schema3ProbeOutcome::Observed(value) => value,
        refusal => panic!("fixture must be observed, got {refusal:?}"),
    }
}

#[test]
fn historical_fixture_observes_all_six_families_without_changing_source_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let store = historical_store(directory.path());
    let before = hash(&store);

    let value = observed(&store);

    assert_eq!(value.agent_registry, 1);
    assert_eq!(value.message_ledger, 2);
    assert_eq!(value.ledger_head, 1);
    assert_eq!(value.recipient_inbox, 1);
    assert_eq!(value.thread_index, 1);
    assert_eq!(value.delivery_outbox, 1);
    assert_eq!(hash(&store), before, "probe is metadata-only for the source archive");
}

#[test]
fn historical_pending_reference_that_has_no_ledger_row_is_refused_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let store = historical_store(directory.path());
    LegacyMessengerTables::open(&store).unwrap()
        .append_outbox_slot("recipient", 999)
        .expect("historical API can create an invalid pending reference");
    let before = hash(&store);

    assert_eq!(probe(&store), Schema3ProbeOutcome::Refused(Schema3ProbeRefusal::LegacyDecodeOrInvariant));
    assert_eq!(hash(&store), before);
}

#[test]
fn corrupt_historical_row_is_refused_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let store = historical_store(directory.path());
    let mut bytes = fs::read(&store).unwrap();
    let offset = bytes.len() / 2;
    bytes[offset] ^= 0xff;
    fs::write(&store, bytes).unwrap();
    let before = hash(&store);

    assert_eq!(probe(&store), Schema3ProbeOutcome::Refused(Schema3ProbeRefusal::LegacyDecodeOrInvariant));
    assert_eq!(hash(&store), before);
}

#[test]
fn absent_path_is_refused_without_creating_a_store_and_repeat_is_consistent() {
    let directory = tempfile::tempdir().unwrap();
    let absent = directory.path().join("does-not-exist.sema");
    let expected = Schema3ProbeOutcome::Refused(Schema3ProbeRefusal::InputNotRegularFile);
    assert_eq!(probe(&absent), expected);
    assert!(!absent.exists());
    assert_eq!(probe(&absent), expected);
    assert!(!absent.exists());
}

#[test]
fn cli_emits_typed_observation_and_refusal_outcomes() {
    let directory = tempfile::tempdir().unwrap();
    let store = historical_store(directory.path());
    let binary = env!("CARGO_BIN_EXE_message-schema3-probe");

    let accepted = Command::new(binary).arg(&store).output().unwrap();
    assert!(accepted.status.success());
    assert_eq!(String::from_utf8(accepted.stdout).unwrap(), "Observed.Schema3.{ 1 2 1 1 1 1 }\n");

    let absent = directory.path().join("absent.sema");
    let refused = Command::new(binary).arg(&absent).output().unwrap();
    assert_eq!(refused.status.code(), Some(1));
    assert_eq!(String::from_utf8(refused.stdout).unwrap(), "Refused.Schema3.InputNotRegularFile\n");
}

#[test]
fn direct_historical_open_mutates_but_copy_first_probe_does_not() {
    let directory = tempfile::tempdir().unwrap();
    let direct = historical_store(directory.path());
    let direct_before = hash(&direct);
    drop(LegacyMessengerTables::open(&direct).expect("a343-equivalent direct legacy open"));
    assert_ne!(hash(&direct), direct_before, "direct legacy engine opening is not source-safe");

    let protected_directory = tempfile::tempdir().unwrap();
    let protected = historical_store(protected_directory.path());
    let protected_before = hash(&protected);
    assert!(matches!(probe(&protected), Schema3ProbeOutcome::Observed(_)));
    assert_eq!(hash(&protected), protected_before, "copy-first probe preserves the historical archive");
}
