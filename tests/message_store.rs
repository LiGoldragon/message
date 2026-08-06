use message::{
    MessengerTables,
    runtime_model::{LedgerDraft, SenderName},
};
use signal_message::schema::lib::{
    z2VNcG, z2VPn2, z2VSVi, z2VTJ1, z2VTiK, z2VUSt, z2VVrY, z2VWzi, z2VY2v, z2VY3v, z2VY18, z2Vari,
    z2VdsV, z2Vf2p,
};

fn draft(recipient: &str, body: &str, thread: z2VTiK) -> LedgerDraft {
    LedgerDraft {
        message_submission: z2VY2v {
            field_0: z2Vari::new(recipient.to_owned()),
            field_1: z2VdsV::z2VXeo,
            field_2: z2VNcG::new(body.to_owned()),
            field_3: thread,
        },
        message_origin: z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(1000))),
        sender_name: SenderName::new("sender".to_owned()),
        stamped_at: z2VY18::new(z2Vf2p::new(41)),
    }
}

#[test]
fn one_durable_write_feeds_inbox_and_thread_reads() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let thread = z2VUSt::new("design".to_owned());
    let accepted = tables
        .store_submission(&draft(
            "designer",
            "beauty rules",
            z2VTiK::z2VPTM(thread.clone()),
        ))
        .unwrap();
    assert_eq!(*accepted.payload().payload(), 0);

    let inbox = tables
        .inbox_entries(&z2VSVi::new(z2Vari::new("designer".to_owned())))
        .unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].field_1.payload(), "sender");
    assert_eq!(inbox[0].field_2.payload(), "beauty rules");

    let contents = tables
        .thread_contents(&z2VVrY::new(thread).into_payload())
        .unwrap()
        .unwrap();
    let entries: &z2VWzi = &contents.field_3;
    assert_eq!(entries.payload().len(), 1);
}

#[test]
fn empty_inbox_is_an_empty_producer_collection() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let entries = tables
        .inbox_entries(&z2VSVi::new(z2Vari::new("absent".to_owned())))
        .unwrap();
    assert!(entries.is_empty());
}
