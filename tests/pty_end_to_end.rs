use std::io::{Read, Write};
use std::os::unix::net::UnixListener;

use message::{
    DeliveryDisposition, DeliveryRunner, MessengerTables,
    runtime_model::{LedgerDraft, SenderName},
};
use signal_message::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentityAssignment,
    ConnectionClass, InboxEntry, MessageKind, MessageOrigin, MessageSubmission,
    ProcessPinSelection, ResumeSelection, ThreadSelection,
};

#[test]
fn pty_leg_sends_the_producer_inbox_entry_as_datom() {
    let directory = tempfile::tempdir().unwrap();
    let session = directory.path().join("terminal-session");
    std::fs::create_dir(&session).unwrap();
    let control = session.join("control.sock");
    let data = session.join("data.sock");
    let listener = UnixListener::bind(&control).unwrap();
    let receiver = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut kind = [0_u8; 1];
        stream.read_exact(&mut kind).unwrap();
        assert_eq!(&kind, b"P");
        let mut length = [0_u8; 8];
        stream.read_exact(&mut length).unwrap();
        let mut body = vec![0; u64::from_be_bytes(length) as usize];
        stream.read_exact(&mut body).unwrap();
        stream.write_all(b"A").unwrap();
        String::from_utf8(body).unwrap()
    });

    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    tables
        .seat_identity(&AgentIdentityAssignment {
            agent_identifier: "recipient".to_owned(),
            process_pin_selection: ProcessPinSelection::None,
            resume_selection: ResumeSelection::None,
        })
        .unwrap();
    tables
        .bind_endpoint(&AgentEndpointBinding {
            agent_identifier: "recipient".to_owned(),
            agent_endpoint: AgentEndpoint {
                agent_endpoint_kind: AgentEndpointKind::PtySocket,
                endpoint_path: data.to_string_lossy().into_owned(),
            },
            harness_pid: 1,
            harness_start_time: 1,
        })
        .unwrap();
    let accepted = tables
        .store_submission(&LedgerDraft {
            message_submission: MessageSubmission {
                message_recipient: "recipient".to_owned(),
                message_kind: MessageKind::Send,
                message_body: "visible Datom".to_owned(),
                thread_selection: ThreadSelection::None,
            },
            message_origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
            sender_name: SenderName::new("sender".to_owned()),
            stamped_at: 9,
        })
        .unwrap();
    let record = tables.ledger_record_public(accepted).unwrap().unwrap();
    assert_eq!(
        DeliveryRunner::new(&tables).deliver_committed(&record),
        DeliveryDisposition::Delivered
    );

    let text = receiver.join().unwrap();
    let entry = message::text::read::<InboxEntry>(text.trim()).unwrap();
    assert_eq!(entry.message_sender, "sender");
    assert_eq!(entry.message_body, "visible Datom");
}
