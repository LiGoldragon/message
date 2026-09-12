use message::{
    MessengerTables,
    runtime_model::{LedgerDraft, SenderName},
};
use signal_message::{
    ConnectionClass, MessageKind, MessageOrigin, MessageSubmission, ThreadEntries, ThreadSelection,
};

fn draft(recipient: &str, body: &str, thread: ThreadSelection) -> LedgerDraft {
    LedgerDraft {
        message_submission: MessageSubmission {
            message_recipient: recipient.to_owned(),
            message_kind: MessageKind::Send,
            message_body: body.to_owned(),
            thread_selection: thread,
        },
        message_origin: MessageOrigin::External(ConnectionClass::NonOwnerUser(1000)),
        sender_name: SenderName::new("sender".to_owned()),
        stamped_at: 41,
    }
}

#[test]
fn one_durable_write_feeds_inbox_and_thread_reads() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let thread = "design".to_owned();
    let accepted = tables
        .store_submission(&draft(
            "designer",
            "beauty rules",
            ThreadSelection::Named(thread.clone()),
        ))
        .unwrap();
    assert_eq!(accepted, 0);

    let inbox = tables.inbox_entries(&"designer".to_owned()).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].message_sender, "sender");
    assert_eq!(inbox[0].message_body, "beauty rules");

    let contents = tables.thread_contents(&thread).unwrap().unwrap();
    let entries: &ThreadEntries = &contents.thread_entries;
    assert_eq!(entries.len(), 1);
}

#[test]
fn empty_inbox_is_an_empty_producer_collection() {
    let directory = tempfile::tempdir().unwrap();
    let tables = MessengerTables::open(&directory.path().join("messenger.sema")).unwrap();
    let entries = tables.inbox_entries(&"absent".to_owned()).unwrap();
    assert!(entries.is_empty());
}
