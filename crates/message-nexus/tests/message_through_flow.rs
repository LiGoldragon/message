//! The Message Nexus, as a process, against a scripted Flow. What Flow
//! recorded as typed is the oracle; nothing here reads Message's source.

mod support;

use message_nexus::frame::FramedStream;
use meta_signal_flow::{
    BodyRefusal, Content, DeliveryRejection, InterruptWitness, Message, MetaRefusal, Sender,
};
use meta_signal_message::{
    Activation, MessageConfiguration, Query as MetaQuery, RedeliverRequest,
    Response as MetaResponse,
};
use signal_flow::FlowAspect;
use signal_message::{
    BodyRefused_Data, Grade, MessageRejection, Priority, Query, Receipt, Response, SendRejection,
    SendRequest, Submission,
};
use support::{
    NexusProcess,
    fake_flow::{FakeFlow, Pane},
};

const SENDER: &str = "5e11de";
const RECIPIENT: &str = "7d41e0";

fn started() -> (FakeFlow, NexusProcess) {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    flow.set_pane(RECIPIENT, Pane::Idle);
    flow.set_peer(Some(FakeFlow::caller(SENDER, FlowAspect::Field)));
    (flow, NexusProcess::start(home, runtime))
}

fn send(priority: Priority, text: &str) -> Query {
    Query::Send(SendRequest {
        flow_id_vector: vec![RECIPIENT.into()],
        priority,
        content: Content::Text(text.into()),
    })
}

fn submitted(response: Response) -> Submission {
    match response {
        Response::Submitted(submission) => submission,
        other => panic!("expected Submitted, got {other:?}"),
    }
}

fn receipt(grade: Grade) -> Receipt {
    Receipt {
        flow_id: RECIPIENT.into(),
        interrupt_witness: InterruptWitness::NotRequested,
        grade,
    }
}

/// Reads Observe frames until one carries the grade.
fn observe_until(stream: &mut std::os::unix::net::UnixStream, grade: Grade) {
    loop {
        match stream.read_frame::<Response>().unwrap() {
            Response::ReceiptObserved(receipt) if receipt.grade == grade => return,
            Response::Receipts(submission)
                if submission.receipt_vector.iter().any(|r| r.grade == grade) =>
            {
                return;
            }
            _ => {}
        }
    }
}

#[test]
fn a_letter_reaches_flow_with_its_priority_head_and_the_peers_name() {
    let (flow, nexus) = started();
    let submission = submitted(nexus.ask(&send(Priority::MiddleAbrupt, "run the tier tests")));
    assert_eq!(submission.receipt_vector, [receipt(Grade::Presented)]);
    let typed = flow.typed();
    assert_eq!(typed.len(), 1);
    assert_eq!(typed[0].flow_id, RECIPIENT);
    assert_eq!(
        typed[0].delivery_id,
        format!("{}:{RECIPIENT}:0", submission.message_id)
    );
    let Message::MiddleAbrupt(letter) = &typed[0].message else {
        panic!("the Priority is the head: {:?}", typed[0].message);
    };
    assert_eq!(letter.sender, Sender::Flow(SENDER.into()));
    // The id Flow types after the head is the one Acknowledge takes, so a
    // recipient can answer from what it reads in its own pane and nothing
    // else. Anything narrower (the DeliveryId, which carries the attempt)
    // would not be acknowledgeable.
    assert_eq!(letter.message_id, submission.message_id);
    flow.set_peer(Some(FakeFlow::caller(RECIPIENT, FlowAspect::Mind)));
    assert_eq!(
        nexus.ask(&Query::Acknowledge(letter.message_id.clone())),
        Response::Acknowledged(submission.message_id.clone())
    );
}

#[test]
fn a_peer_in_no_flow_cannot_send_on_the_ordinary_socket() {
    let (flow, nexus) = started();
    flow.set_peer(None);
    assert_eq!(
        nexus.ask(&send(Priority::Soft, "hello")),
        Response::SendRejected(SendRejection::SenderUnknown)
    );
    assert!(flow.typed().is_empty());
}

#[test]
fn one_refused_recipient_fails_the_whole_send_and_nothing_is_typed() {
    let (flow, nexus) = started();
    flow.set_pane("88475f", Pane::Idle);
    let request = Query::Send(SendRequest {
        flow_id_vector: vec![RECIPIENT.into(), "88475f".into(), "ffffff".into()],
        priority: Priority::Soft,
        content: Content::Text("hello".into()),
    });
    assert_eq!(
        nexus.ask(&request),
        Response::SendRejected(SendRejection::UnknownRecipient("ffffff".into()))
    );
    assert_eq!(
        nexus.ask(&send(Priority::Soft, "/compact")),
        Response::SendRejected(SendRejection::BodyRefused(BodyRefused_Data {
            flow_id: RECIPIENT.into(),
            body_refusal: BodyRefusal::HarnessCommand("/compact".into()),
        }))
    );
    assert!(flow.typed().is_empty());
}

#[test]
fn a_soft_letter_to_a_working_recipient_parks_and_lands_at_rest() {
    let (flow, nexus) = started();
    flow.set_pane(RECIPIENT, Pane::Working);
    let submission = submitted(nexus.ask(&send(Priority::Soft, "when you rest")));
    assert_eq!(submission.receipt_vector, [receipt(Grade::Parked)]);
    let mut observed = nexus.observe(&submission.message_id);
    flow.wait_observed(RECIPIENT);
    assert!(flow.typed().is_empty());
    flow.set_pane(RECIPIENT, Pane::Idle);
    let typed = flow.wait_typed(1);
    assert_eq!(
        typed[0].delivery_id,
        format!("{}:{RECIPIENT}:0", submission.message_id)
    );
    observe_until(&mut observed, Grade::Presented);
}

#[test]
fn an_occupied_composer_parks_every_priority_until_the_next_rest() {
    let (flow, nexus) = started();
    flow.set_pane(RECIPIENT, Pane::ComposerOccupied);
    let submission = submitted(nexus.ask(&send(Priority::HardAbrupt, "urgent")));
    assert_eq!(submission.receipt_vector, [receipt(Grade::Parked)]);
    flow.wait_observed(RECIPIENT);
    flow.set_pane(RECIPIENT, Pane::Working);
    flow.set_pane(RECIPIENT, Pane::Idle);
    assert!(matches!(
        flow.wait_typed(1)[0].message,
        Message::HardAbrupt(_)
    ));
    let mut observed = nexus.observe(&submission.message_id);
    observe_until(&mut observed, Grade::Presented);
}

#[test]
fn the_sender_withdraws_a_parked_letter_and_it_is_never_typed() {
    let (flow, nexus) = started();
    flow.set_pane(RECIPIENT, Pane::Working);
    let submission = submitted(nexus.ask(&send(Priority::Soft, "never mind")));
    flow.wait_observed(RECIPIENT);
    flow.set_peer(Some(FakeFlow::caller(RECIPIENT, FlowAspect::Mind)));
    assert_eq!(
        nexus.ask(&Query::Withdraw(submission.message_id.clone())),
        Response::MessageRejected(MessageRejection::NotSender)
    );
    flow.set_peer(Some(FakeFlow::caller(SENDER, FlowAspect::Field)));
    assert_eq!(
        nexus.ask(&Query::Withdraw(submission.message_id.clone())),
        Response::Withdrawn(submission.message_id.clone())
    );
    flow.wait_observers_closed(1);
    flow.set_pane(RECIPIENT, Pane::Idle);
    assert_eq!(
        nexus.ask(&Query::QueryReceipts(submission.message_id.clone())),
        Response::Receipts(Submission {
            message_id: submission.message_id.clone(),
            receipt_vector: vec![receipt(Grade::Withdrawn)],
        })
    );
    assert_eq!(
        nexus.ask(&Query::Withdraw(submission.message_id)),
        Response::MessageRejected(MessageRejection::NotParked)
    );
    assert!(flow.typed().is_empty());
}

#[test]
fn only_a_recipient_acknowledges_and_that_alone_is_read() {
    let (flow, nexus) = started();
    let submission = submitted(nexus.ask(&send(Priority::Soft, "please read")));
    let message_id = submission.message_id;
    assert_eq!(
        nexus.ask(&Query::Acknowledge(message_id.clone())),
        Response::MessageRejected(MessageRejection::NotRecipient)
    );
    flow.set_peer(Some(FakeFlow::caller(RECIPIENT, FlowAspect::Mind)));
    assert_eq!(
        nexus.ask(&Query::Acknowledge(message_id.clone())),
        Response::Acknowledged(message_id.clone())
    );
    assert_eq!(
        nexus.ask(&Query::QueryReceipts(message_id.clone())),
        Response::Receipts(Submission {
            message_id,
            receipt_vector: vec![receipt(Grade::Read)],
        })
    );
}

#[test]
fn redeliver_is_the_only_way_out_of_uncertain() {
    let (flow, nexus) = started();
    flow.set_pane(RECIPIENT, Pane::Uncertain);
    flow.set_peer(None);
    let MetaResponse::Submitted(submission) = nexus.ask_meta(&MetaQuery::Send(SendRequest {
        flow_id_vector: vec![RECIPIENT.into()],
        priority: Priority::MiddleAbrupt,
        content: Content::Text("from the owner".into()),
    })) else {
        panic!("the owner's Send is answered on the meta socket");
    };
    assert_eq!(submission.receipt_vector, [receipt(Grade::Uncertain)]);
    let Message::MiddleAbrupt(letter) = &flow.typed()[0].message else {
        panic!("head");
    };
    assert_eq!(letter.sender, Sender::Owner);
    flow.set_pane(RECIPIENT, Pane::Idle);
    let redeliver = MetaQuery::Redeliver(RedeliverRequest {
        message_id: submission.message_id.clone(),
        flow_id: RECIPIENT.into(),
    });
    assert_eq!(
        nexus.ask_meta(&redeliver),
        MetaResponse::Redelivered(receipt(Grade::Presented))
    );
    let typed = flow.typed();
    assert_eq!(typed.len(), 2);
    assert_eq!(
        typed[1].delivery_id,
        format!("{}:{RECIPIENT}:1", submission.message_id)
    );
    assert_eq!(
        nexus.ask_meta(&redeliver),
        MetaResponse::RedeliverRejected(MessageRejection::NotUncertain)
    );
}

#[test]
fn the_meta_socket_refuses_a_flow_outside_meta_aspects() {
    let (flow, nexus) = started();
    let field = FakeFlow::caller(SENDER, FlowAspect::Field);
    flow.set_peer(Some(field.clone()));
    let redeliver = MetaQuery::Redeliver(RedeliverRequest {
        message_id: "m-0".into(),
        flow_id: RECIPIENT.into(),
    });
    assert_eq!(
        nexus.ask_meta(&redeliver),
        MetaResponse::MetaRefused(MetaRefusal::PeerNotAuthorized(field))
    );
    flow.set_peer(Some(FakeFlow::caller("9a9a9a", FlowAspect::Psyche)));
    assert_eq!(
        nexus.ask_meta(&redeliver),
        MetaResponse::RedeliverRejected(MessageRejection::UnknownMessage)
    );
}

#[test]
fn configure_is_stored_and_applied_to_the_next_call() {
    let (flow, nexus) = started();
    flow.set_peer(None);
    let elsewhere = tempfile::tempdir().unwrap();
    let second = FakeFlow::start(elsewhere.path());
    second.set_pane(RECIPIENT, Pane::Idle);
    second.set_peer(Some(FakeFlow::caller(SENDER, FlowAspect::Field)));
    let runtime = flow.runtime_directory.to_string_lossy().into_owned();
    let configuration = MessageConfiguration {
        ordinary_socket_path: format!("{runtime}/message/message.sock"),
        meta_socket_path: format!("{runtime}/message/message-owner.sock"),
        flow_socket_path: elsewhere
            .path()
            .join("flow/flow.sock")
            .to_string_lossy()
            .into(),
        flow_meta_socket_path: elsewhere
            .path()
            .join("flow/flow-meta.sock")
            .to_string_lossy()
            .into(),
        meta_aspects: vec![FlowAspect::Psyche],
    };
    let MetaResponse::Configured(configured) =
        nexus.ask_meta(&MetaQuery::Configure(configuration.clone()))
    else {
        panic!("the owner configures");
    };
    assert_eq!(configured.activation, Activation::Applied);
    submitted(nexus.ask(&send(Priority::Soft, "to the second Flow")));
    assert_eq!(second.typed().len(), 1);
    assert!(flow.typed().is_empty());
    // The meta gate now asks the second Flow, which names this peer a Field
    // flow: refused, until the peer is the owner again.
    let mut moved = configuration;
    moved.ordinary_socket_path = format!("{runtime}/message/elsewhere.sock");
    assert!(matches!(
        nexus.ask_meta(&MetaQuery::Configure(moved.clone())),
        MetaResponse::MetaRefused(MetaRefusal::PeerNotAuthorized(_))
    ));
    second.set_peer(None);
    let MetaResponse::Configured(configured) = nexus.ask_meta(&MetaQuery::Configure(moved)) else {
        panic!("the owner configures");
    };
    assert_eq!(configured.activation, Activation::NexusRestartRequired);
}

#[test]
fn a_restarted_nexus_resumes_its_parked_letters() {
    let (flow, nexus) = started();
    flow.set_pane(RECIPIENT, Pane::Working);
    let submission = submitted(nexus.ask(&send(Priority::Soft, "after the restart")));
    flow.wait_observed(RECIPIENT);
    let (home, runtime) = nexus.stop();
    flow.wait_observers_closed(1);
    let nexus = NexusProcess::start(home, runtime);
    flow.wait_observed(RECIPIENT);
    flow.set_pane(RECIPIENT, Pane::Idle);
    flow.wait_typed(1);
    let mut observed = nexus.observe(&submission.message_id);
    observe_until(&mut observed, Grade::Presented);
    assert_eq!(flow.typed().len(), 1);
}

#[test]
fn a_flow_refusal_at_delivery_is_kept_as_its_own_grade() {
    let (flow, nexus) = started();
    flow.set_pane(RECIPIENT, Pane::Working);
    let submission = submitted(nexus.ask(&send(Priority::MiddleAbrupt, "mid-turn")));
    assert_eq!(submission.receipt_vector, [receipt(Grade::Transported)]);
    flow.set_pane(RECIPIENT, Pane::Blocked);
    let submission = submitted(nexus.ask(&send(Priority::HardAbrupt, "into a dialog")));
    assert_eq!(
        submission.receipt_vector,
        [receipt(Grade::Refused(DeliveryRejection::RecipientBlocked))]
    );
    assert_eq!(flow.typed().len(), 1);
}
