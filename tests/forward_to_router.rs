//! In-process witnesses for message's Nexus forward-to-router effect.
//!
//! These drive `MessageEngine::handle` directly against a stub router that
//! speaks the `signal-message` wire, exercising the generated Nexus runner:
//! `SignalArrived` -> `decide` -> `ForwardToRouter` effect -> the router call ->
//! `EffectCompleted` -> `decide` -> `ReplyToSignal`. They cover the two paths
//! the router-independent process-boundary test does not: a successful submit
//! forward and the router-unreachable fallback.

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    os::unix::net::UnixListener,
    path::PathBuf,
    thread,
};

use message::router::{SignalRouterFrameCodec, SignalRouterSocket};
use message::{
    MessageEngine, MessengerTables, RouterForwarder,
    router::OriginPolicy,
    schema::signal::{
        Body, Input, MessageKind, MessageSubmission, Output, Recipient, Submit, ThreadSelection,
    },
};
use signal_message::{
    ComponentInstanceName, ComponentName, ConnectionClass, Input as SignalMessageInput,
    InternalComponentInstanceOrigin, MessageOrigin, MessageSlot, NetworkPeer,
    Output as SignalMessageOutput, SubmissionAcceptance, UnixUserIdentifier,
};
use tempfile::TempDir;
use triad_runtime::{ConnectionContext, UnixCredentials};

/// The owner uid the in-process engine is configured to recognize, and the
/// matching/non-matching peer connections the tests stamp with.
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
        202,
    ))
}

fn tcp_connection() -> ConnectionContext {
    ConnectionContext::from(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4242)))
}

/// A one-shot router stub: accepts one connection, decodes one `signal-message`
/// request, and replies with a fixed `SubmissionAccepted` slot 7. Returns the
/// request it received so the test can assert the daemon stamped + translated it.
struct StubRouter {
    listener: UnixListener,
}

impl StubRouter {
    fn bind(socket_path: &std::path::Path) -> Self {
        Self {
            listener: UnixListener::bind(socket_path).expect("bind stub router socket"),
        }
    }

    fn serve_one_acceptance(self) -> thread::JoinHandle<SignalMessageInput> {
        thread::spawn(move || {
            let codec = SignalRouterFrameCodec::default();
            let (mut stream, _address) = self.listener.accept().expect("accept router connection");
            let frame = codec
                .read_frame(&mut stream)
                .expect("read router request frame");
            let received = codec
                .request_from_frame(frame)
                .expect("decode router request");
            let reply = codec.reply_frame(
                received.exchange,
                SignalMessageOutput::SubmissionAccepted(SubmissionAcceptance::new(
                    MessageSlot::new(7),
                )),
            );
            codec
                .write_frame(&mut stream, &reply)
                .expect("write router reply");
            received.request
        })
    }
}

fn engine_for(router_socket_path: PathBuf, store_root: &TempDir) -> MessageEngine {
    MessageEngine::new(
        RouterForwarder::new(
            SignalRouterSocket::from_path(router_socket_path),
            OriginPolicy::for_owner_user_id(OWNER_USER_ID, OWNER_INSTANCE),
        ),
        MessengerTables::open(&store_root.path().join("messenger.sema"))
            .expect("open messenger store"),
    )
}

fn send_input(recipient: &str, body: &str) -> Input {
    Input::Submit(Submit::new(MessageSubmission {
        recipient: Recipient::new(recipient.to_owned()),
        kind: MessageKind::Send.into(),
        body: Body::new(body.to_owned()),
        thread_selection: ThreadSelection::None,
    }))
}

#[test]
fn submit_is_stamped_with_configured_harness_instance_when_the_peer_uid_matches_the_owner() {
    let temp = TempDir::new().expect("tempdir");
    let router_socket_path = temp.path().join("router.sock");
    let router = StubRouter::bind(&router_socket_path);
    let router_thread = router.serve_one_acceptance();

    let store_root = TempDir::new().expect("store dir");
    let mut engine = engine_for(router_socket_path, &store_root);
    let output = tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(engine.handle(send_input("designer", "hello"), &owner_connection()))
        .expect("handle submit");

    match output {
        Output::SubmissionAccepted(acceptance) => {
            assert_eq!(acceptance.into_payload().into_payload().into_payload(), 7);
        }
        other => panic!("expected SubmissionAccepted translated from router, got {other:?}"),
    }

    let forwarded = router_thread.join().expect("router thread");
    match forwarded {
        SignalMessageInput::SubmitStamped(stamped) => {
            assert_eq!(
                stamped.message_submission.message_recipient.as_str(),
                "designer"
            );
            assert_eq!(stamped.message_submission.message_body.as_str(), "hello");
            // The origin is minted from peer credentials plus daemon
            // configuration, NOT from the payload: a trusted owner uid becomes
            // this daemon's configured local harness instance.
            assert_eq!(
                stamped.message_origin,
                MessageOrigin::InternalComponentInstance(InternalComponentInstanceOrigin {
                    component_name: ComponentName::Harness,
                    component_instance_name: ComponentInstanceName::new(OWNER_INSTANCE.to_owned()),
                })
            );
        }
        other => panic!("expected daemon to stamp the submission before forwarding, got {other:?}"),
    }
}

#[test]
fn submit_from_a_non_owner_peer_is_stamped_with_a_non_owner_origin() {
    let temp = TempDir::new().expect("tempdir");
    let router_socket_path = temp.path().join("router.sock");
    let router = StubRouter::bind(&router_socket_path);
    let router_thread = router.serve_one_acceptance();

    let store_root = TempDir::new().expect("store dir");
    let mut engine = engine_for(router_socket_path, &store_root);
    let output = tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(engine.handle(
            send_input("designer", "from a stranger"),
            &non_owner_connection(),
        ))
        .expect("handle submit");

    assert!(matches!(output, Output::SubmissionAccepted(_)));

    let forwarded = router_thread.join().expect("router thread");
    match forwarded {
        SignalMessageInput::SubmitStamped(stamped) => {
            // A peer uid that does not match the owner mints NonOwnerUser(uid) —
            // the peer-credential origin classification the migration regressed.
            assert_eq!(
                stamped.message_origin,
                MessageOrigin::External(ConnectionClass::NonOwnerUser(UnixUserIdentifier::new(
                    u64::from(NON_OWNER_USER_ID)
                )))
            );
        }
        other => panic!("expected daemon to stamp the submission before forwarding, got {other:?}"),
    }
}

#[test]
fn submit_from_a_tcp_peer_is_stamped_with_a_network_origin() {
    let temp = TempDir::new().expect("tempdir");
    let router_socket_path = temp.path().join("router.sock");
    let router = StubRouter::bind(&router_socket_path);
    let router_thread = router.serve_one_acceptance();

    let store_root = TempDir::new().expect("store dir");
    let mut engine = engine_for(router_socket_path, &store_root);
    let output = tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(engine.handle(send_input("designer", "from tcp"), &tcp_connection()))
        .expect("handle submit");

    assert!(matches!(output, Output::SubmissionAccepted(_)));

    let forwarded = router_thread.join().expect("router thread");
    match forwarded {
        SignalMessageInput::SubmitStamped(stamped) => {
            assert_eq!(
                stamped.message_origin,
                MessageOrigin::External(ConnectionClass::Network(NetworkPeer::new(
                    "127.0.0.1:4242"
                )))
            );
        }
        other => panic!("expected daemon to stamp the submission before forwarding, got {other:?}"),
    }
}

#[test]
fn router_unreachable_yields_typed_error_output() {
    let temp = TempDir::new().expect("tempdir");
    // No stub router bound at this path — the forward connect fails.
    let router_socket_path = temp.path().join("absent-router.sock");

    let store_root = TempDir::new().expect("store dir");
    let mut engine = engine_for(router_socket_path, &store_root);
    let output = tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(engine.handle(send_input("designer", "no router"), &owner_connection()))
        .expect("handle submit without router");

    assert!(
        matches!(output, Output::Error(_)),
        "expected typed Error when the router socket is unreachable, got {output:?}"
    );
}
