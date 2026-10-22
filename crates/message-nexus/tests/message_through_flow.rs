//! Send asks Flow to identify, lock and deliver one typed request.

mod support;

use signal_flow::{Address, Aspect, Delivery, Layer, Message, Recipient, Request, SendRequest};
use signal_message::{Query, Response, SendThread_Data};
use support::{NexusProcess, fake_flow::FakeFlow};

fn quiet_send_stream(nexus: &NexusProcess) -> std::os::unix::net::UnixStream {
    use message_nexus::frame::FramedStream;
    let mut stream = std::os::unix::net::UnixStream::connect(nexus.ordinary_socket()).unwrap();
    stream
        .write_frame(&Query::Send(SendRequest {
            recipient: Recipient::Up,
            delivery: Delivery::Wake,
            message: Message::Soft(Request::Question("quiet observer".into())),
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

#[test]
fn send_reaches_flow_with_the_lock_and_original_request() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let recipient = Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        layer: Layer::Secondary,
    };
    let request = SendRequest {
        recipient: Recipient::Address(recipient.clone()),
        delivery: Delivery::Wake,
        message: Message::MiddleAbrupt(Request::Question("What is current?".into())),
    };

    assert_eq!(
        nexus.ask(&Query::Send(request.clone())),
        Response::Sent(signal_flow::SendReceipt {
            logical_delivery_id: "fake-logical-delivery".into(),
            native_receipt: signal_flow::NativeReceipt {
                harness_kind: signal_flow::HarnessKind::Codex,
                native_session_id: "fake-session".into(),
                native_prompt_id: "fake-prompt".into(),
            },
        })
    );

    let delivered = flow.typed();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].lock.address, recipient);
    assert_eq!(delivered[0].message, request.message);
    assert_eq!(delivered[0].delivery, request.delivery);
    assert_eq!(delivered[0].lock.sender.topic, "flow");
    assert_eq!(flow.bound().len(), 1);
}

#[test]
fn message_forwards_flow_statuses_after_admission_and_preserves_resume_cursor() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let _flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let responses = nexus.ask_stream(&Query::Send(SendRequest {
        recipient: Recipient::Up,
        delivery: Delivery::Wake,
        message: Message::Soft(Request::Question("status stream".into())),
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
            recipient: Recipient::Up,
            delivery: Delivery::Wake,
            message: Message::MiddleAbrupt(Request::Question("whole thread body".into())),
        },
        native_session_id: "actual-thread".into(),
    });
    assert!(matches!(nexus.ask(&request), Response::Sent(_)));
    let identified = flow.identified_threads();
    assert_eq!(identified.len(), 1);
    assert_eq!(
        identified[0].process,
        signal_flow::Process::this_process().unwrap()
    );
    assert_eq!(identified[0].native_session_id, "actual-thread");
    assert_eq!(flow.typed().len(), 1);
    assert_eq!(
        flow.typed()[0].message,
        Message::MiddleAbrupt(Request::Question("whole thread body".into()))
    );
}

#[test]
fn failed_thread_identification_keeps_precise_refusal_and_never_delivers() {
    use message_nexus::peer::ReadsProcess;
    use signal_flow::Refusal;
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let process = signal_flow::Process::this_process().unwrap();
    for (thread, refusal) in [
        ("not-configured", Refusal::NotConfigured),
        ("not-send-server", Refusal::NotSendServer(process.clone())),
        ("unknown", Refusal::Unidentified(process)),
    ] {
        let query = Query::SendThread(SendThread_Data {
            send_request: SendRequest {
                recipient: Recipient::Up,
                delivery: Delivery::Wake,
                message: Message::MiddleAbrupt(Request::Question("never deliver".into())),
            },
            native_session_id: thread.into(),
        });
        assert_eq!(
            nexus.ask(&query),
            Response::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::Refused(refusal),
            })
        );
    }
    assert!(flow.typed().is_empty());
    assert_eq!(flow.identified_threads().len(), 3);
}
