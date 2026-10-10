//! Send asks Flow to identify, lock and deliver one typed request.

mod support;

use message_nexus::frame::FramedStream;
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
