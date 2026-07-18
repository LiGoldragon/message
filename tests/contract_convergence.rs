//! Structural convergence witnesses: the published `signal-message` contract
//! and the daemon's own emitted signal module stay frame-compatible.
//!
//! Orchestrate pushes registry operations to the messenger socket speaking
//! the PUBLISHED contract, while the daemon ingress decodes its own emitted
//! mirror. The mint-relocation worker verified header identity by hand
//! (constant-for-constant); these tests make that verification structural —
//! every implemented shared operation is encoded with the contract types and
//! decoded with the daemon types (and replies the reverse), so any drift in
//! variant order, header packing, or payload layout fails the suite instead
//! of silently poisoning the wire.
//!
//! `SubmitStamped` is deliberately absent: its origin payload diverges by
//! design (the daemon's stored origin is leaner than the contract's
//! cross-host origin), and the operation is typed-unimplemented in both
//! directions. It must never be extended on either side without converging
//! the origin models first.

use message::schema::signal as daemon;
use signal_message as contract;

fn daemon_decodes(bytes: &[u8]) -> daemon::Input {
    let (_route, input) = daemon::Input::decode_signal_frame(bytes).expect("daemon decodes frame");
    input
}

fn contract_decodes(bytes: &[u8]) -> contract::Output {
    let (_route, output) =
        contract::Output::decode_signal_frame(bytes).expect("contract decodes frame");
    output
}

#[test]
fn contract_submit_decodes_as_daemon_submit() {
    let wire = contract::Input::Submit(contract::MessageSubmission {
        message_recipient: contract::MessageRecipient::new("designer".to_owned()),
        message_kind: contract::MessageKind::Send,
        message_body: contract::MessageBody::new("cross-vocabulary".to_owned()),
        thread_selection: contract::ThreadSelection::Named(contract::ThreadName::new(
            "launch-plan".to_owned(),
        )),
    });
    let bytes = wire.encode_signal_frame().expect("encode contract frame");

    let daemon::Input::Submit(submit) = daemon_decodes(&bytes) else {
        panic!("contract Submit did not decode as daemon Submit");
    };
    let submission = submit.into_payload();
    assert_eq!(submission.recipient.payload(), "designer");
    assert_eq!(submission.body.payload(), "cross-vocabulary");
    assert_eq!(
        submission.thread_selection,
        daemon::ThreadSelection::Named(daemon::ThreadName::new("launch-plan".to_owned()))
    );
}

#[test]
fn contract_registry_operations_decode_as_daemon_registry_operations() {
    let assign = contract::Input::AssignAgentIdentity(contract::AgentIdentityAssignment {
        agent_identifier: contract::AgentIdentifier::new("li7f".to_owned()),
        process_pin_selection: contract::ProcessPinSelection::Pinned(contract::HarnessProcessPin {
            harness_pid: contract::HarnessPid::new(4242),
            harness_start_time: contract::HarnessStartTime::new(777888),
        }),
        resume_selection: contract::ResumeSelection::Resumed(contract::ResumeIdentity::new(
            "session-019f".to_owned(),
        )),
    });
    let bytes = assign.encode_signal_frame().expect("encode contract frame");
    let daemon::Input::AssignAgentIdentity(assignment) = daemon_decodes(&bytes) else {
        panic!("contract AssignAgentIdentity did not decode as the daemon operation");
    };
    let assignment = assignment.into_payload();
    assert_eq!(assignment.agent_identifier.payload(), "li7f");
    assert!(matches!(
        assignment.process_pin_selection,
        daemon::ProcessPinSelection::Pinned(_)
    ));

    let query = contract::Input::QueryAgentRegistry(contract::AgentRegistryQuery::ByAgent(
        contract::AgentIdentifier::new("li7f".to_owned()),
    ));
    let bytes = query.encode_signal_frame().expect("encode contract frame");
    let daemon::Input::QueryAgentRegistry(decoded) = daemon_decodes(&bytes) else {
        panic!("contract QueryAgentRegistry did not decode as the daemon operation");
    };
    assert!(matches!(
        decoded.into_payload(),
        daemon::AgentRegistryQuery::ByAgent(identifier) if identifier.payload() == "li7f"
    ));
}

#[test]
fn contract_thread_operations_decode_as_daemon_thread_operations() {
    let query = contract::Input::QueryThread(contract::ThreadQuery::new(
        contract::ThreadName::new("subagents".to_owned()),
    ));
    let bytes = query.encode_signal_frame().expect("encode contract frame");
    let daemon::Input::QueryThread(decoded) = daemon_decodes(&bytes) else {
        panic!("contract QueryThread did not decode as the daemon operation");
    };
    assert_eq!(decoded.into_payload().into_payload().payload(), "subagents");

    let subscribe = contract::Input::SubscribeThread(contract::ThreadSubscription {
        thread_name: contract::ThreadName::new("subagents".to_owned()),
        participant_name: contract::ParticipantName::new("li7f".to_owned()),
        thread_relation_selection: contract::ThreadRelationSelection::Related(
            contract::ThreadRelation {
                repository_name: contract::RepositoryName::new("orchestrate".to_owned()),
                feature_branch_name: contract::FeatureBranchName::new(
                    "MessengerPromotion".to_owned(),
                ),
            },
        ),
    });
    let bytes = subscribe
        .encode_signal_frame()
        .expect("encode contract frame");
    let daemon::Input::SubscribeThread(decoded) = daemon_decodes(&bytes) else {
        panic!("contract SubscribeThread did not decode as the daemon operation");
    };
    let subscription = decoded.into_payload();
    assert_eq!(subscription.participant_name.payload(), "li7f");
    assert!(matches!(
        subscription.thread_relation_selection,
        daemon::ThreadRelationSelection::Related(_)
    ));

    let index = contract::Input::QueryThreads(contract::ThreadIndexQuery::All);
    let bytes = index.encode_signal_frame().expect("encode contract frame");
    assert!(matches!(daemon_decodes(&bytes), daemon::Input::QueryThreads(_)));
}

#[test]
fn daemon_replies_decode_as_contract_replies() {
    let accepted = daemon::Output::SubmissionAccepted(daemon::SubmissionAccepted::new(
        daemon::SubmissionAcceptance::new(daemon::MessageSlot::new(7)),
    ));
    let bytes = accepted.encode_signal_frame().expect("encode daemon frame");
    assert!(matches!(
        contract_decodes(&bytes),
        contract::Output::SubmissionAccepted(acceptance)
            if acceptance.payload().payload() == &7
    ));

    let inbox = daemon::Output::InboxListing(daemon::InboxListing::new(
        daemon::InboxContents::new(daemon::InboxEntries::new(vec![daemon::InboxEntry {
            message_slot: daemon::MessageSlot::new(7),
            sender: daemon::Sender::new("li7f".to_owned()),
            body: daemon::Body::new("hello".to_owned()),
            thread_selection: daemon::ThreadSelection::None,
            stamped_at: daemon::StampedAt::new(daemon::TimestampNanos::new(11)),
        }])),
    ));
    let bytes = inbox.encode_signal_frame().expect("encode daemon frame");
    let contract::Output::InboxListing(listing) = contract_decodes(&bytes) else {
        panic!("daemon InboxListing did not decode as the contract reply");
    };
    let entries = listing.into_entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].message_sender.as_str(), "li7f");
    assert_eq!(
        entries[0].thread_selection,
        contract::ThreadSelection::None
    );

    let rejected = daemon::Output::ThreadRejected(daemon::ThreadRejected::new(
        daemon::ThreadRejection::new(daemon::ThreadRejectionReason::UnknownThread),
    ));
    let bytes = rejected.encode_signal_frame().expect("encode daemon frame");
    assert!(matches!(
        contract_decodes(&bytes),
        contract::Output::ThreadRejected(rejection)
            if matches!(rejection.payload(), contract::ThreadRejectionReason::UnknownThread)
    ));

    let error = daemon::Output::Error(daemon::Error::new(daemon::ErrorReport::new(
        daemon::ErrorMessage::new("store rejected".to_owned()),
    )));
    let bytes = error.encode_signal_frame().expect("encode daemon frame");
    assert!(matches!(
        contract_decodes(&bytes),
        contract::Output::Error(report)
            if report.payload().payload().as_str() == "store rejected"
    ));
}

#[test]
fn shared_submit_frames_are_byte_identical_across_vocabularies() {
    let wire = contract::Input::Submit(contract::MessageSubmission {
        message_recipient: contract::MessageRecipient::new("designer".to_owned()),
        message_kind: contract::MessageKind::Send,
        message_body: contract::MessageBody::new("cross-vocabulary".to_owned()),
        thread_selection: contract::ThreadSelection::Named(contract::ThreadName::new(
            "launch-plan".to_owned(),
        )),
    });
    let local = daemon::Input::Submit(daemon::Submit::new(daemon::MessageSubmission {
        recipient: daemon::Recipient::new("designer".to_owned()),
        kind: daemon::Kind::new(daemon::MessageKind::Send),
        body: daemon::Body::new("cross-vocabulary".to_owned()),
        thread_selection: daemon::ThreadSelection::Named(daemon::ThreadName::new(
            "launch-plan".to_owned(),
        )),
    }));
    assert_eq!(
        wire.encode_signal_frame().expect("encode contract"),
        local.encode_signal_frame().expect("encode daemon"),
        "one logical Submit must encode to identical bytes in both vocabularies; \
         a size or order drift in EITHER enum breaks every shared operation"
    );
}
