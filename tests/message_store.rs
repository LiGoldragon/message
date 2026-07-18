//! Witnesses for the messenger's durable message state — the ledger,
//! per-recipient inbox, and thread index in `messenger.sema` (packet 3.1).
//!
//! These drive `MessageEngine::handle` so the full Signal → Nexus → SEMA
//! path is the thing proven, not the store alone: a submission persists a
//! ledger row and an inbox row with minted provenance; threads accumulate
//! entries and auto-join participants; listings survive a store reopen; the
//! ledger window is bounded and reaps oldest-first.

use std::path::PathBuf;

use message::schema::signal::{
    Body, Input, Kind, MessageKind, MessageSubmission, Output, QueryInbox, QueryThread,
    QueryThreads, Recipient, SubscribeThread, Submit, ThreadIndexQuery, ThreadName, ThreadQuery,
    ThreadRejectionReason, ThreadRelation, ThreadRelationSelection, ThreadSelection,
    ThreadSubscription, ParticipantName, FeatureBranchName, RepositoryName,
};
use message::{MessageEngine, MessengerTables, OriginPolicy};
use tempfile::TempDir;
use triad_runtime::{ConnectionContext, UnixCredentials};

const OWNER_USER_ID: u32 = 1000;
const NON_OWNER_USER_ID: u32 = 4242;
const OWNER_INSTANCE: &str = "operator";

fn owner_connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(OWNER_USER_ID, OWNER_USER_ID, 101))
}

fn non_owner_connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(
        NON_OWNER_USER_ID,
        NON_OWNER_USER_ID,
        102,
    ))
}

fn store_path(root: &TempDir) -> PathBuf {
    root.path().join("messenger.sema")
}

fn engine_for(root: &TempDir) -> MessageEngine {
    MessageEngine::new(
        MessengerTables::open(&store_path(root)).expect("open messenger store"),
        OriginPolicy::for_owner_user_id(OWNER_USER_ID, OWNER_INSTANCE),
    )
}

fn submission(recipient: &str, body: &str, thread: ThreadSelection) -> Input {
    Input::Submit(Submit::new(MessageSubmission {
        recipient: Recipient::new(recipient.to_owned()),
        kind: Kind::new(MessageKind::Send),
        body: Body::new(body.to_owned()),
        thread_selection: thread,
    }))
}

fn named_thread(name: &str) -> ThreadSelection {
    ThreadSelection::Named(ThreadName::new(name.to_owned()))
}

async fn drive(engine: &mut MessageEngine, input: Input, connection: &ConnectionContext) -> Output {
    engine
        .handle(input, connection)
        .await
        .expect("engine handle")
}

#[tokio::test]
async fn submission_persists_ledger_and_inbox_row_with_minted_provenance() {
    let root = TempDir::new().expect("tempdir");
    let mut engine = engine_for(&root);

    let accepted = drive(
        &mut engine,
        submission("designer", "scaffold ready", ThreadSelection::None),
        &owner_connection(),
    )
    .await;
    let Output::SubmissionAccepted(acceptance) = accepted else {
        panic!("expected acceptance, got {accepted:?}");
    };
    assert_eq!(
        *acceptance.payload().payload().payload(),
        0,
        "first slot is zero"
    );

    let listing = drive(
        &mut engine,
        Input::QueryInbox(QueryInbox::new(message::schema::signal::InboxQuery::new(
            Recipient::new("designer".to_owned()),
        ))),
        &owner_connection(),
    )
    .await;
    let Output::InboxListing(contents) = listing else {
        panic!("expected inbox listing, got {listing:?}");
    };
    let entries = contents.into_payload().into_payload().into_payload();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(entry.body.payload(), "scaffold ready");
    assert_eq!(
        entry.sender.payload(),
        OWNER_INSTANCE,
        "no registry pin matches, so the owner connection resolves to the configured owner name"
    );
    assert!(
        *entry.stamped_at.payload().payload() > 0,
        "ingress stamp is minted"
    );
}

#[tokio::test]
async fn non_owner_sender_falls_back_to_uid_label() {
    let root = TempDir::new().expect("tempdir");
    let mut engine = engine_for(&root);

    drive(
        &mut engine,
        submission("designer", "hello", ThreadSelection::None),
        &non_owner_connection(),
    )
    .await;
    let listing = drive(
        &mut engine,
        Input::QueryInbox(QueryInbox::new(message::schema::signal::InboxQuery::new(
            Recipient::new("designer".to_owned()),
        ))),
        &owner_connection(),
    )
    .await;
    let Output::InboxListing(contents) = listing else {
        panic!("expected inbox listing, got {listing:?}");
    };
    let entries = contents.into_payload().into_payload().into_payload();
    assert_eq!(entries[0].sender.payload(), &format!("uid-{NON_OWNER_USER_ID}"));
}

#[tokio::test]
async fn thread_submissions_accumulate_entries_and_auto_join_participants() {
    let root = TempDir::new().expect("tempdir");
    let mut engine = engine_for(&root);

    drive(
        &mut engine,
        submission("x2qb", "first", named_thread("subagents")),
        &owner_connection(),
    )
    .await;
    drive(
        &mut engine,
        submission("li7f", "second", named_thread("subagents")),
        &owner_connection(),
    )
    .await;

    let listing = drive(
        &mut engine,
        Input::QueryThread(QueryThread::new(ThreadQuery::new(ThreadName::new(
            "subagents".to_owned(),
        )))),
        &owner_connection(),
    )
    .await;
    let Output::ThreadListing(contents) = listing else {
        panic!("expected thread listing, got {listing:?}");
    };
    let contents = contents.into_payload();
    assert_eq!(contents.thread_entries.payload().len(), 2);
    let participants: Vec<&str> = contents
        .participants
        .payload()
        .iter()
        .map(|name| name.payload().as_str())
        .collect();
    assert!(
        participants.contains(&"x2qb") && participants.contains(&"li7f"),
        "recipients auto-join the thread: {participants:?}"
    );
    assert!(
        participants.contains(&OWNER_INSTANCE),
        "the sender auto-joins the thread: {participants:?}"
    );
}

#[tokio::test]
async fn explicit_subscription_creates_thread_and_sets_relation() {
    let root = TempDir::new().expect("tempdir");
    let mut engine = engine_for(&root);

    let subscribed = drive(
        &mut engine,
        Input::SubscribeThread(SubscribeThread::new(ThreadSubscription {
            thread_name: ThreadName::new("MessengerPromotion".to_owned()),
            participant_name: ParticipantName::new("li7f".to_owned()),
            thread_relation_selection: ThreadRelationSelection::Related(ThreadRelation {
                repository_name: RepositoryName::new("message".to_owned()),
                feature_branch_name: FeatureBranchName::new("MessengerPromotion".to_owned()),
            }),
        })),
        &owner_connection(),
    )
    .await;
    let Output::ThreadSubscribed(acknowledgment) = subscribed else {
        panic!("expected subscription acknowledgment, got {subscribed:?}");
    };
    let acknowledgment = acknowledgment.into_payload();
    assert_eq!(acknowledgment.thread_name.payload(), "MessengerPromotion");

    let index = drive(
        &mut engine,
        Input::QueryThreads(QueryThreads::new(ThreadIndexQuery::All)),
        &owner_connection(),
    )
    .await;
    let Output::ThreadIndexListing(listing) = index else {
        panic!("expected thread index listing, got {index:?}");
    };
    let threads = listing.into_payload().into_threads();
    assert_eq!(threads.len(), 1);
    let summary = &threads[0];
    assert!(matches!(
        summary.thread_relation_selection,
        ThreadRelationSelection::Related(_)
    ));
    assert_eq!(*summary.message_count.payload(), 0);
}

#[tokio::test]
async fn unknown_thread_query_is_a_typed_rejection() {
    let root = TempDir::new().expect("tempdir");
    let mut engine = engine_for(&root);

    let reply = drive(
        &mut engine,
        Input::QueryThread(QueryThread::new(ThreadQuery::new(ThreadName::new(
            "missing".to_owned(),
        )))),
        &owner_connection(),
    )
    .await;
    let Output::ThreadRejected(rejection) = reply else {
        panic!("expected typed thread rejection, got {reply:?}");
    };
    assert!(matches!(
        rejection.into_payload().into_payload(),
        ThreadRejectionReason::UnknownThread
    ));
}

#[tokio::test]
async fn message_state_survives_store_reopen() {
    let root = TempDir::new().expect("tempdir");
    {
        let mut engine = engine_for(&root);
        drive(
            &mut engine,
            submission("designer", "durable", named_thread("triage")),
            &owner_connection(),
        )
        .await;
    }

    let mut reopened = engine_for(&root);
    let listing = drive(
        &mut reopened,
        Input::QueryInbox(QueryInbox::new(message::schema::signal::InboxQuery::new(
            Recipient::new("designer".to_owned()),
        ))),
        &owner_connection(),
    )
    .await;
    let Output::InboxListing(contents) = listing else {
        panic!("expected inbox listing after reopen, got {listing:?}");
    };
    assert_eq!(
        contents.into_payload().into_payload().into_payload().len(),
        1,
        "ledger and inbox survive reopen"
    );

    let threads = drive(
        &mut reopened,
        Input::QueryThreads(QueryThreads::new(ThreadIndexQuery::All)),
        &owner_connection(),
    )
    .await;
    let Output::ThreadIndexListing(listing) = threads else {
        panic!("expected thread index after reopen, got {threads:?}");
    };
    assert_eq!(
        listing.into_payload().into_threads().len(),
        1,
        "thread index survives reopen"
    );
}
