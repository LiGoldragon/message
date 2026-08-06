use message::{
    DeliveryDisposition, DeliveryRunner, MessengerTables, ParkReason,
    runtime_model::{LedgerDraft, SenderName},
};
use signal_message::schema::lib::{
    z2VNPW, z2VNcG, z2VPn2, z2VTJ1, z2VTiK, z2VXMQ, z2VY2v, z2VY3v, z2VY18, z2Vari, z2Vcfd, z2VdsV,
    z2VevD, z2Vf2p,
};

fn draft(recipient: &str) -> LedgerDraft {
    LedgerDraft {
        message_submission: z2VY2v {
            field_0: z2Vari::new(recipient.to_owned()),
            field_1: z2VdsV::z2VXeo,
            field_2: z2VNcG::new("waiting".to_owned()),
            field_3: z2VTiK::z2VR2m,
        },
        message_origin: z2VTJ1::z2VWSr(z2VY3v::z2VN6o(z2VPn2::new(1000))),
        sender_name: SenderName::new("sender".to_owned()),
        stamped_at: z2VY18::new(z2Vf2p::new(1)),
    }
}

#[test]
fn registered_agent_without_endpoint_is_parked_durably() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    tables
        .seat_identity(&z2VevD {
            field_0: z2VNPW::new("recipient".to_owned()),
            field_1: z2Vcfd::z2VRLv,
            field_2: z2VXMQ::z2VNZi,
        })
        .unwrap();
    let accepted = tables.store_submission(&draft("recipient")).unwrap();
    let record = tables
        .ledger_record_public(*accepted.payload().payload())
        .unwrap()
        .unwrap();
    let disposition = DeliveryRunner::new(&tables).deliver_committed(&record);
    assert_eq!(
        disposition,
        DeliveryDisposition::Parked(ParkReason::NoEndpoint)
    );
    assert_eq!(tables.outbox_slots("recipient").unwrap(), vec![0]);
}

#[test]
fn unknown_recipient_remains_an_inbox_fact_without_false_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let accepted = tables.store_submission(&draft("future-agent")).unwrap();
    let record = tables
        .ledger_record_public(*accepted.payload().payload())
        .unwrap()
        .unwrap();
    assert_eq!(
        DeliveryRunner::new(&tables).deliver_committed(&record),
        DeliveryDisposition::Parked(ParkReason::UnknownRecipient)
    );
    assert!(tables.outbox_slots("future-agent").unwrap().is_empty());
}
