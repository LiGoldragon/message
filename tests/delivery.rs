use message::{
    DeliveryDisposition, DeliveryRunner, MessengerTables, ParkReason,
    runtime_model::{LedgerDraft, SenderName},
};
use signal_message::{
    AgentIdentityAssignment, ConnectionClass, MessageKind, MessageOrigin, MessageSubmission,
    ProcessPinSelection, ResumeSelection, ThreadSelection,
};

fn draft(recipient: &str) -> LedgerDraft {
    LedgerDraft {
        message_submission: MessageSubmission {
            message_recipient: recipient.to_owned(),
            message_kind: MessageKind::Send,
            message_body: "waiting".to_owned(),
            thread_selection: ThreadSelection::None,
        },
        message_origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
        sender_name: SenderName::new("sender".to_owned()),
        stamped_at: 1,
    }
}

#[test]
fn registered_agent_without_endpoint_is_parked_durably() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    tables
        .seat_identity(&AgentIdentityAssignment {
            agent_identifier: "recipient".to_owned(),
            process_pin_selection: ProcessPinSelection::None,
            resume_selection: ResumeSelection::None,
        })
        .unwrap();
    let accepted = tables.store_submission(&draft("recipient")).unwrap();
    let record = tables.ledger_record_public(accepted).unwrap().unwrap();
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
    let record = tables.ledger_record_public(accepted).unwrap().unwrap();
    assert_eq!(
        DeliveryRunner::new(&tables).deliver_committed(&record),
        DeliveryDisposition::Parked(ParkReason::UnknownRecipient)
    );
    assert!(tables.outbox_slots("future-agent").unwrap().is_empty());
}
