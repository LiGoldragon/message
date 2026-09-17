use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use message::{Configuration, client::MessageSocket};
use signal_message::{
    ClusterMessage, DeliveryRequest, FlowDeliveryRequest, FlowIdleAnnouncement,
    MessageDaemonConfiguration, OwnerIdentity, PeerEnvelope, PeerSender,
    PromptInterpretationSelection, PromptVariant, Query, ReceiptKind, Response,
    TypedPromptEnvelope,
};

fn contract(directory: &std::path::Path) -> MessageDaemonConfiguration {
    MessageDaemonConfiguration {
        message_socket_path: directory
            .join("message.sock")
            .to_string_lossy()
            .into_owned(),
        message_socket_mode: 0o600,
        supervision_socket_path: directory
            .join("meta-message.sock")
            .to_string_lossy()
            .into_owned(),
        supervision_socket_mode: 0o600,
        router_socket_path: directory.join("router.sock").to_string_lossy().into_owned(),
        component_ingresses: Vec::new(),
        owner_identity: OwnerIdentity::UnixUser(i64::from(rustix::process::getuid().as_raw())),
    }
}

#[test]
fn ordinary_signal_records_a_typed_parked_delivery_idempotently() {
    let directory = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let configuration = Configuration::new(
        contract(directory.path()),
        directory.path().join("delivery.sema"),
        "owner",
    )
    .unwrap();
    let configuration_path = directory.path().join("message.configuration");
    configuration
        .write_binary_file(&configuration_path)
        .unwrap();
    let _daemon = launch_daemon(&configuration_path, home.path());
    wait_for(configuration.socket_path());
    let client = MessageSocket::from_path(configuration.socket_path()).client();
    let request = DeliveryRequest {
        source_event_identifier: "fac697-process-boundary".into(),
        cluster_message: ClusterMessage::Peer(PeerEnvelope {
            peer_sender: PeerSender {
                flow_identifier: "fac697".into(),
                session_identifier: "codex-primary".into(),
            },
            source_event_identifier: "fac697-process-boundary".into(),
            peer_source_path: "flows/fac697/reports/message.md".into(),
            peer_body_sha256: "52b797a276d825aaa28f449f1d35682bd4d271f6455be84e3869cdd7aed2ca03"
                .into(),
            peer_body: "NEXUS".into(),
        }),
        target_flows: vec!["unresolved-fixture".into()],
    };
    let mut invalid = request.clone();
    let ClusterMessage::Peer(peer) = &mut invalid.cluster_message else {
        unreachable!()
    };
    peer.peer_body_sha256 = "0".repeat(64);
    assert!(matches!(
        client.submit(Query::Deliver(invalid)).unwrap(),
        Response::Error(_)
    ));
    let query = Query::Deliver(request);
    for _ in 0..2 {
        let Response::DeliveryRecorded(report) = client.submit(query.clone()).unwrap() else {
            panic!("ordinary Deliver did not return DeliveryRecorded")
        };
        assert_eq!(report.source_event_identifier, "fac697-process-boundary");
        assert_eq!(report.recipient_receipts.len(), 1);
        assert_eq!(
            report.recipient_receipts[0].receipt_kind,
            ReceiptKind::Parked
        );
    }
    let Query::Deliver(mut conflicting) = query else {
        unreachable!()
    };
    conflicting.target_flows = vec!["another-target".into()];
    let ClusterMessage::Peer(peer) = &mut conflicting.cluster_message else {
        unreachable!()
    };
    peer.peer_body = "OTHER".into();
    peer.peer_body_sha256 =
        "1c55d9b826e8dfa994370e306ae8dc2e849f3e003381dc848a0b95f782c0c0e3".into();
    assert!(matches!(
        client.submit(Query::Deliver(conflicting)).unwrap(),
        Response::Error(_)
    ));
}

/// Wait on the tested event — the listener binding its socket — with a
/// generous upper bound so a stuck daemon fails the run instead of hanging it.
/// The bound is a timeout, not a timing assumption: an unoptimised daemon
/// opening its durable store takes seconds on a loaded builder.
fn wait_for(path: &std::path::Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("socket did not appear within the bound: {}", path.display());
}

/// Daemons discover provisional flow markers relative to HOME. Process tests
/// therefore receive a disposable HOME and never create markers in the
/// calling user's flow lanes.
struct DaemonChild(Child);

impl Drop for DaemonChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn launch_daemon(configuration_path: &std::path::Path, home: &std::path::Path) -> DaemonChild {
    DaemonChild(
        Command::new(env!("CARGO_BIN_EXE_message-daemon"))
            .arg(configuration_path)
            .env("HOME", home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

#[test]
fn daemon_executes_both_producer_owned_contracts() {
    let directory = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let contract = contract(directory.path());
    let configuration = Configuration::new(
        contract.clone(),
        directory.path().join("messenger.sema"),
        "owner",
    )
    .unwrap();
    let configuration_path = directory.path().join("message.configuration");
    configuration
        .write_binary_file(&configuration_path)
        .unwrap();

    let _daemon = launch_daemon(&configuration_path, home.path());
    wait_for(configuration.socket_path());
    wait_for(configuration.meta_socket_path());

    let output = MessageSocket::from_path(configuration.socket_path())
        .client()
        .submit(Query::QueryInbox("empty".to_owned()))
        .unwrap();
    match output {
        Response::InboxListing(listing) => assert!(listing.messages.is_empty()),
        other => panic!("unexpected ordinary reply: {other:?}"),
    }
}

#[test]
fn isolated_nexus_socket_parks_then_drains_on_a_typed_flow_idle_witness() {
    let directory = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let flow = format!("socket-fixture-{}", std::process::id());
    let lanes = home.path().join("primary/flows");
    std::fs::create_dir_all(&lanes).unwrap();
    let marker = lanes.join(format!(".{flow}.flow-id"));
    std::fs::write(&marker, "fixture flow marker\n").unwrap();

    let contract = contract(directory.path());
    let configuration = Configuration::new(
        contract,
        directory.path().join("fresh-messenger.sema"),
        "owner",
    )
    .unwrap();
    let configuration_path = directory.path().join("message.configuration");
    configuration
        .write_binary_file(&configuration_path)
        .unwrap();
    let _daemon = launch_daemon(&configuration_path, home.path());
    wait_for(configuration.socket_path());
    let client = MessageSocket::from_path(configuration.socket_path()).client();
    let queued = client
        .submit(Query::FlowDeliver(FlowDeliveryRequest {
            typed_prompt_envelope: TypedPromptEnvelope {
                prompt_variant: PromptVariant::HumanPrompt,
                source_event_identifier: "cf7879:source:5350d56d".to_owned(),
                raw_prompt_text: "fixture exact bytes: señal ✓".to_owned(),
                prompt_interpretation_selection: PromptInterpretationSelection::None,
            },
            target_flow_name: flow.clone(),
        }))
        .unwrap();
    assert!(matches!(queued, Response::DeliveryQueued(_)));

    let landed = client
        .submit(Query::FlowAnnounceIdle(FlowIdleAnnouncement {
            target_flow_name: flow,
        }))
        .unwrap();
    std::fs::remove_file(marker).unwrap();
    match landed {
        Response::FlowIdleAcknowledged(acknowledgment) => {
            assert_eq!(acknowledgment.landed_receipts.len(), 1);
            assert_eq!(acknowledgment.landed_receipts[0].byte_count, 31);
            assert!(acknowledgment.landed_receipts[0].landed_at > 0);
        }
        other => panic!("unexpected idle reply: {other:?}"),
    }
}
