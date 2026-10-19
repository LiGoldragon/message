//! Send asks Flow to identify, lock and deliver one typed request.

mod support;

use signal_flow::{Address, Aspect, Layer, Recipient, Request};
use signal_message::{Query, Response, Send_Data};
use support::{NexusProcess, fake_flow::FakeFlow};

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
    let request = Request::Question("What is current?".into());

    assert_eq!(
        nexus.ask(&Query::Send(Send_Data {
            recipient: Recipient::Address(recipient.clone()),
            request: request.clone(),
        })),
        Response::Delivered
    );

    let delivered = flow.typed();
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].lock.address, recipient);
    assert_eq!(delivered[0].request, request);
    assert_eq!(delivered[0].lock.sender.topic, "flow");
    assert_eq!(flow.bound().len(), 1);
}

#[test]
fn thread_send_forwards_kernel_peer_and_exact_thread_before_delivery() {
    use message_nexus::peer::ReadsProcess;
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let request = Query::SendThread(signal_message::SendThread_Data {
        recipient: Recipient::Up,
        request: Request::Question("whole thread body".into()),
        native_session_id: "actual-thread".into(),
    });
    assert_eq!(nexus.ask(&request), Response::Delivered);
    let identified = flow.identified_threads();
    assert_eq!(identified.len(), 1);
    assert_eq!(
        identified[0].process,
        signal_flow::Process::this_process().unwrap()
    );
    assert_eq!(identified[0].native_session_id, "actual-thread");
    assert_eq!(flow.typed().len(), 1);
    assert_eq!(
        flow.typed()[0].request,
        Request::Question("whole thread body".into())
    );
}

#[test]
fn failed_thread_identification_keeps_precise_refusal_and_never_delivers() {
    use message_nexus::peer::ReadsProcess;
    use signal_message::SendRefusal;
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    let nexus = NexusProcess::start(home, runtime);
    let process = signal_flow::Process::this_process().unwrap();
    for (thread, refusal) in [
        ("not-configured", SendRefusal::NotConfigured),
        (
            "not-send-server",
            SendRefusal::NotSendServer(process.clone()),
        ),
        ("unknown", SendRefusal::Unidentified(process)),
    ] {
        let query = Query::SendThread(signal_message::SendThread_Data {
            recipient: Recipient::Up,
            request: Request::Question("never deliver".into()),
            native_session_id: thread.into(),
        });
        assert_eq!(nexus.ask(&query), Response::Refused(refusal));
    }
    assert!(flow.typed().is_empty());
    assert_eq!(flow.identified_threads().len(), 3);
}
