//! Send asks Flow to identify, lock and deliver one typed request.

mod support;

use signal_flow::{
    Address, Aspect, Model, SendRequest, SendableMessage, SendableRequest, VisibleDeliveryState,
    VisibleSendAcceptance,
};
use signal_message::{Query, Response, SendThread_Data};
use support::{NexusProcess, fake_flow::FakeFlow};

use message_nexus::{
    DefaultConfiguration, LaysOutDefaults,
    configuration::SeedsConfiguration,
    generated::memory::Standard,
    request_tracking::Classification,
    store::{KeepsRequestTracking, MessageStore, OpensStore},
};

fn quiet_send_stream(nexus: &NexusProcess) -> std::os::unix::net::UnixStream {
    use message_nexus::frame::FramedStream;
    let mut stream = std::os::unix::net::UnixStream::connect(nexus.ordinary_socket()).unwrap();
    stream
        .write_frame(&Query::Send(SendRequest {
            recipient: sender(),
            sendable_message: SendableMessage::Soft(SendableRequest::Question(
                "quiet observer".into(),
            )),
        }))
        .unwrap();
    assert!(matches!(
        stream.read_frame::<Response>().unwrap(),
        Response::SendAccepted(_)
    ));
    assert!(matches!(
        stream.read_frame::<Response>().unwrap(),
        Response::SendsObserved(_)
    ));
    stream
}

fn sender() -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        model: Model::Sol,
    }
}

#[test]
fn send_reaches_flow_with_the_lock_and_original_request() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let recipient = sender();
    let request = SendRequest {
        recipient: recipient.clone(),
        sendable_message: SendableMessage::MiddleAbrupt(SendableRequest::Question(
            "What is current?".into(),
        )),
    };

    assert_eq!(
        nexus.ask(&Query::Send(request.clone())),
        Response::SendAccepted(VisibleSendAcceptance {
            request_id: "fake-request".into(),
            delivery_cursor: 1,
            visible_delivery_state: VisibleDeliveryState::Queued,
        })
    );

    let delivered = flow.typed();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].lock.address, recipient);
    assert_eq!(delivered[0].sendable_message, request.sendable_message);
    assert_eq!(delivered[0].lock.sender.topic, "flow");
    assert_eq!(flow.acknowledged(), vec![delivered[0].deliver_key.clone()]);
    assert_eq!(flow.bound().len(), 1);
}

#[test]
fn message_consumes_its_live_delivery_stream_and_reopens_the_durable_cursor() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    assert_eq!(flow.wait_for_delivery_observations(1), vec![None]);
    flow.interrupt_delivery_observer();
    assert_eq!(
        flow.wait_for_delivery_observations(2),
        vec![None, Some(0)],
        "the replacement stream resumes at Message's durable cursor"
    );

    assert!(matches!(
        nexus.ask(&Query::Send(SendRequest {
            recipient: sender(),
            sendable_message: SendableMessage::Soft(SendableRequest::Question(
                "classify the live delivery".into(),
            )),
        })),
        Response::SendAccepted(_)
    ));
    assert_eq!(flow.acknowledged().len(), 1);

    let (home, runtime) = nexus.stop();
    let defaults = DefaultConfiguration {
        home: home.path().into(),
        runtime_directory: runtime.path().into(),
    };
    let store = MessageStore::open(
        &defaults.store_path(),
        Standard {
            configuration: defaults.message_configuration(),
            meta_configured: false,
        },
    )
    .unwrap();
    let tracking = store.request_tracking().unwrap();
    assert_eq!(tracking.observation_cursor, Some(2));
    assert_eq!(tracking.classified_events.len(), 1);
    assert_eq!(
        tracking.classified_events[0].classification,
        Classification::MessageRelay
    );
    assert_eq!(tracking.classified_events[0].delivery_cursor, 2);
    drop(store);

    let reopened = NexusProcess::start(home, runtime);
    assert_eq!(
        flow.wait_for_delivery_observations(3),
        vec![None, Some(0), Some(2)],
        "a process reopen resumes the same Flow store at the committed cursor"
    );
    reopened.stop_gracefully();
}

#[test]
fn message_forwards_flow_statuses_after_admission_and_preserves_resume_cursor() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let _flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let responses = nexus.ask_stream(&Query::Send(SendRequest {
        recipient: sender(),
        sendable_message: SendableMessage::Soft(SendableRequest::Question("status stream".into())),
    }));

    assert!(matches!(responses.first(), Some(Response::SendAccepted(_))));
    assert!(matches!(responses.get(1), Some(Response::SendsObserved(_))));
    let Some(Response::ObservationInterrupted(interrupted)) = responses.get(2) else {
        panic!("Flow stream loss must be explicit after admission");
    };
    assert_eq!(interrupted.delivery_cursor_option, Some(1));
}

#[test]
fn client_disconnect_interrupts_quiet_flow_observation_without_touching_queue() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start_quiet(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let stream = quiet_send_stream(&nexus);
    assert_eq!(flow.typed().len(), 1);
    drop(stream);
    flow.wait_for_observer_close();
    assert_eq!(
        flow.typed().len(),
        1,
        "disconnect does not cancel or replay the durable send"
    );
}

#[test]
fn nexus_shutdown_interrupts_quiet_flow_observation() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start_quiet(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let _stream = quiet_send_stream(&nexus);
    nexus.stop_gracefully();
    flow.wait_for_observer_close();
    assert_eq!(
        flow.typed().len(),
        1,
        "Nexus shutdown does not cancel or replay the durable send"
    );
}

#[test]
fn thread_send_forwards_kernel_peer_and_exact_thread_before_delivery() {
    use message_nexus::peer::ReadsProcess;
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let request = Query::SendThread(SendThread_Data {
        send_request: SendRequest {
            recipient: sender(),
            sendable_message: SendableMessage::MiddleAbrupt(SendableRequest::Question(
                "whole thread body".into(),
            )),
        },
        native_session_id: "actual-thread".into(),
    });
    assert!(matches!(nexus.ask(&request), Response::SendAccepted(_)));
    let identified = flow.identified_threads();
    assert_eq!(identified.len(), 1);
    assert_eq!(
        identified[0].process,
        signal_flow::Process::this_process().unwrap()
    );
    assert_eq!(identified[0].native_session_id, "actual-thread");
    assert_eq!(flow.typed().len(), 1);
    assert_eq!(
        flow.typed()[0].sendable_message,
        SendableMessage::MiddleAbrupt(SendableRequest::Question("whole thread body".into()))
    );
}

#[test]
fn failed_thread_identification_keeps_precise_refusal_and_never_delivers() {
    use message_nexus::peer::ReadsProcess;
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let process = signal_flow::Process::this_process().unwrap();
    for (thread, message_refusal) in [
        ("not-configured", signal_message::SendRefusal::NotConfigured),
        (
            "not-send-server",
            signal_message::SendRefusal::NotSendServer(process.clone()),
        ),
        (
            "unknown",
            signal_message::SendRefusal::Unidentified(process.clone()),
        ),
    ] {
        let query = Query::SendThread(SendThread_Data {
            send_request: SendRequest {
                recipient: sender(),
                sendable_message: SendableMessage::MiddleAbrupt(SendableRequest::Question(
                    "never deliver".into(),
                )),
            },
            native_session_id: thread.into(),
        });
        assert_eq!(nexus.ask(&query), Response::Refused(message_refusal));
    }
    assert!(flow.typed().is_empty());
    assert_eq!(flow.identified_threads().len(), 3);
}
