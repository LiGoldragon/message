use nota_codec::{Encoder, Error, NotaEncode};
use persona_message::command::{CommandLine, Input};
use persona_message::daemon::{
    ForwardDecision, MessageDaemon, MessageDaemonInput, MessageDaemonRoot, MessageDaemonRootInput,
    MessageIngressContext, PeerCredentials, SocketMode,
};
use persona_message::output_validator::OutputValidatorCommandLine;
use persona_message::router::SignalRouterFrameCodec;
use persona_message::router::{SignalMessageSocket, SignalRouterSocket};
use persona_message::supervision::{
    SupervisionFrameCodec, SupervisionListener, SupervisionProfile, SupervisionSocketMode,
};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Operation, Request, RequestPayload,
    RequestRejectionReason, SessionEpoch, SignalVerb,
};
use signal_frame::{
    ExchangeIdentifier as FrameExchangeIdentifier, ExchangeLane as FrameExchangeLane,
    LaneSequence as FrameLaneSequence, Request as FrameRequest, SessionEpoch as FrameSessionEpoch,
};
use signal_persona::engine_management::{
    Frame as SupervisionFrame, FrameBody as SupervisionFrameBody, Operation as SupervisionRequest,
    Query as SupervisionQuery, Reply as SupervisionReply,
};
use signal_persona::{
    ComponentHealth, ComponentKind, ComponentName, EngineManagementProtocolVersion, Presence,
};
use signal_persona_auth::{
    ComponentInstanceName, ComponentName as ProvenanceComponentName, ConnectionClass,
    InternalComponentInstanceOrigin, MessageOrigin, OwnerIdentity, UnixUserId,
};
use signal_persona_message::{
    ComponentMessageIngress, Frame, FrameBody as MessageFrameBody, InboxEntry, InboxListing,
    MessageBody, MessageKind, MessageRecipient, MessageReply, MessageRequest, MessageSender,
    MessageSlot, SubmissionAcceptance,
};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

use kameo::actor::{ActorStateAbsence, ActorTerminalReason};

struct MessageFixture {
    directory: tempfile::TempDir,
}

impl MessageFixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        Self { directory }
    }

    fn router_socket_path(&self) -> PathBuf {
        self.directory.path().join("router.signal.sock")
    }

    fn message_socket_path(&self) -> PathBuf {
        self.directory.path().join("message.signal.sock")
    }

    fn component_message_socket_path(&self, component_instance_name: &str) -> PathBuf {
        self.directory
            .path()
            .join("message-ingress")
            .join(format!("{component_instance_name}.sock"))
    }

    fn supervision_socket_path(&self) -> PathBuf {
        self.directory.path().join("message.supervision.sock")
    }

    fn start_path(&self) -> PathBuf {
        self.directory.path().join("start")
    }

    fn local_ledger_path(&self) -> PathBuf {
        self.directory
            .path()
            .join(".persona-message")
            .join(["messages", ".nota.log"].concat())
    }

    fn configuration_path(&self) -> PathBuf {
        self.directory.path().join("message-daemon.nota")
    }

    /// Write a typed `MessageDaemonConfiguration` NOTA file for the
    /// daemon to read via `nota_config::ConfigurationSource`.
    fn write_message_daemon_configuration(&self, owner_identity: OwnerIdentity) -> PathBuf {
        self.write_message_daemon_configuration_with_ingresses(owner_identity, Vec::new())
    }

    fn write_message_daemon_configuration_with_ingresses(
        &self,
        owner_identity: OwnerIdentity,
        component_ingresses: Vec<ComponentMessageIngress>,
    ) -> PathBuf {
        let path = self.configuration_path();
        let configuration = signal_persona_message::MessageDaemonConfiguration {
            message_socket_path: signal_persona::WirePath::new(
                self.message_socket_path().display().to_string(),
            ),
            message_socket_mode: signal_persona::SocketMode::new(0o660),
            supervision_socket_path: signal_persona::WirePath::new(
                self.supervision_socket_path().display().to_string(),
            ),
            supervision_socket_mode: signal_persona::SocketMode::new(0o600),
            router_socket_path: signal_persona::WirePath::new(
                self.router_socket_path().display().to_string(),
            ),
            component_ingresses,
            owner_identity,
        };
        let mut encoder = Encoder::new();
        configuration
            .encode(&mut encoder)
            .expect("configuration encodes");
        std::fs::write(&path, encoder.into_string()).expect("configuration writes");
        path
    }

    fn spawn_message_after_start(
        &self,
        start_path: &Path,
        message_socket_path: Option<&Path>,
        input: &str,
    ) -> std::process::Child {
        let mut command = Command::new("sh");
        command.arg("-c").arg(format!(
            "while [ ! -f '{}' ]; do sleep 0.05; done; '{}' '{}'",
            start_path.display(),
            env!("CARGO_BIN_EXE_message"),
            input
        ));
        command.current_dir(self.directory.path());
        if let Some(message_socket_path) = message_socket_path {
            command.env("PERSONA_MESSAGE_SOCKET", message_socket_path);
        }
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        command.spawn().expect("message shell starts")
    }

    fn spawn_daemon_after_router_start_with_configuration(
        &self,
        start_path: &Path,
        configuration_path: &Path,
    ) -> std::process::Child {
        let mut command = Command::new("sh");
        command.arg("-c").arg(format!(
            "while [ ! -f '{}' ]; do sleep 0.05; done; '{}' '{}'",
            start_path.display(),
            env!("CARGO_BIN_EXE_persona-message-daemon"),
            configuration_path.display(),
        ));
        command.current_dir(self.directory.path());
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        command.spawn().expect("message daemon shell starts")
    }
}

struct RouterReply {
    reply: MessageReply,
}

impl RouterReply {
    fn accepted(slot: u64) -> Self {
        Self {
            reply: MessageReply::SubmissionAccepted(SubmissionAcceptance {
                message_slot: MessageSlot::new(slot),
            }),
        }
    }

    fn inbox(sender: &str, body: &str) -> Self {
        Self {
            reply: MessageReply::InboxListing(InboxListing {
                messages: vec![InboxEntry {
                    message_slot: MessageSlot::new(3),
                    sender: MessageSender::new(sender),
                    body: MessageBody::new(body),
                }],
            }),
        }
    }
}

struct RecordedFrame {
    request: MessageRequest,
}

struct FakeRouter {
    listener: UnixListener,
    start_path: PathBuf,
}

impl FakeRouter {
    fn bind(socket_path: &Path, start_path: PathBuf) -> Self {
        Self {
            listener: UnixListener::bind(socket_path).expect("router socket binds"),
            start_path,
        }
    }

    fn serve(self, reply: RouterReply) -> std::thread::JoinHandle<RecordedFrame> {
        std::thread::spawn(move || {
            std::fs::write(&self.start_path, "").expect("start marker writes");
            let (mut stream, _) = self.listener.accept().expect("router accepts");
            let codec = SignalRouterFrameCodec::default();
            let frame = codec.read_frame(&mut stream).expect("router input reads");
            let MessageFrameBody::Request { exchange, request } = frame.into_body() else {
                panic!("expected signal request frame");
            };
            let checked = request.into_checked().expect("router input checks");
            let (operation, tail) = checked.operations.into_head_and_tail();
            assert!(tail.is_empty());
            let verb = operation.verb;
            let payload = operation.payload;
            assert_eq!(verb, payload.signal_verb());
            let frame = codec.reply_frame(exchange, verb, reply.reply);
            codec
                .write_frame(&mut stream, &frame)
                .expect("router reply writes");
            RecordedFrame { request: payload }
        })
    }
}

#[test]
fn message_daemon_applies_configured_socket_mode() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.message_socket_path();
    let router_socket_path = fixture.router_socket_path();
    let supervision_socket_path = fixture.supervision_socket_path();
    let daemon = MessageDaemon::from_input(MessageDaemonInput {
        message_socket: SignalMessageSocket::from_path(message_socket_path.clone()),
        message_socket_mode: SocketMode::from_octal(0o660),
        router_socket: SignalRouterSocket::from_path(router_socket_path),
        supervision_socket_path,
        supervision_socket_mode: SupervisionSocketMode::from_octal(0o600),
        component_ingresses: Vec::new(),
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(unsafe { libc::geteuid() })),
    });

    let _listener = daemon
        .bind_listener()
        .expect("message daemon binds listener with managed mode");
    let mode = std::fs::metadata(message_socket_path)
        .expect("message socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o660);
}

#[test]
fn message_frame_codec_rejects_mismatched_signal_verb() {
    let request = Request::from_operations(NonEmpty::single(Operation::new(
        SignalVerb::Assert,
        MessageRequest::InboxQuery(signal_persona_message::InboxQuery {
            recipient: signal_persona_message::MessageRecipient::new("operator"),
        }),
    )));
    let frame = Frame::new(MessageFrameBody::Request {
        exchange: test_exchange(),
        request,
    });
    let error = SignalRouterFrameCodec::default()
        .request_from_frame(frame)
        .expect_err("mismatched verb is rejected");

    match error {
        persona_message::Error::InvalidSignalRequest { reason } => {
            assert_eq!(
                reason,
                RequestRejectionReason::VerbPayloadMismatch { index: 0 }
            );
        }
        other => panic!("expected typed signal request rejection, got {other:?}"),
    }
}

#[test]
fn message_daemon_root_stamps_owner_identity_from_configuration() {
    let root = MessageDaemonRoot::new(MessageDaemonRootInput {
        router_socket: SignalRouterSocket::from_path(PathBuf::from("/tmp/unused-router.sock")),
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(7000)),
    });
    let request = MessageRequest::MessageSubmission(signal_persona_message::MessageSubmission {
        recipient: MessageRecipient::new("router"),
        kind: MessageKind::Send,
        body: MessageBody::new("origin-check"),
    });

    let decision = root
        .stamp_request(
            request,
            MessageIngressContext::external_peer(PeerCredentials::from_user_id(UnixUserId::new(
                7001,
            ))),
        )
        .expect("message request stamps");

    let ForwardDecision::Forward(MessageRequest::StampedMessageSubmission(stamped)) = decision
    else {
        panic!("expected stamped forward decision");
    };
    assert_eq!(
        stamped.origin,
        MessageOrigin::External(ConnectionClass::NonOwnerUser(UnixUserId::new(7001)))
    );
}

#[test]
fn message_daemon_root_stamps_component_instance_origin_from_ingress() {
    let root = MessageDaemonRoot::new(MessageDaemonRootInput {
        router_socket: SignalRouterSocket::from_path(PathBuf::from("/tmp/unused-router.sock")),
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(7000)),
    });
    let origin = InternalComponentInstanceOrigin::new(
        ProvenanceComponentName::Harness,
        ComponentInstanceName::new("initiator"),
    );
    let request = MessageRequest::MessageSubmission(signal_persona_message::MessageSubmission {
        recipient: MessageRecipient::new("responder"),
        kind: MessageKind::Send,
        body: MessageBody::new("component-origin-check"),
    });

    let decision = root
        .stamp_request(
            request,
            MessageIngressContext::internal_component_instance(
                origin.clone(),
                PeerCredentials::from_user_id(UnixUserId::new(7000)),
            ),
        )
        .expect("message request stamps");

    let ForwardDecision::Forward(MessageRequest::StampedMessageSubmission(stamped)) = decision
    else {
        panic!("expected stamped forward decision");
    };
    assert_eq!(
        stamped.origin,
        MessageOrigin::InternalComponentInstance(origin)
    );
}

#[test]
fn message_daemon_root_shutdown_returns_terminal_outcome() {
    let runtime = tokio::runtime::Runtime::new().expect("test runtime starts");
    let root = runtime.block_on(MessageDaemonRoot::start_root(MessageDaemonRootInput {
        router_socket: SignalRouterSocket::from_path(PathBuf::from("/tmp/unused-router.sock")),
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(7000)),
    }));

    let outcome = runtime
        .block_on(MessageDaemonRoot::stop_root(root))
        .expect("message daemon root stops");

    assert_eq!(outcome.state, ActorStateAbsence::Dropped);
    assert_eq!(outcome.reason, ActorTerminalReason::Stopped);
    MessageDaemonRoot::assert_stopped_outcome(outcome).expect("terminal outcome is clean");
}

#[test]
fn message_daemon_answers_component_supervision_relation() {
    let fixture = MessageFixture::new();
    let supervision_socket = fixture.supervision_socket_path();
    let _supervision = SupervisionListener::new(
        SupervisionProfile::message(),
        supervision_socket.clone(),
        SupervisionSocketMode::from_octal(0o600),
    )
    .spawn()
    .expect("message supervision listener starts");

    let mode = std::fs::metadata(&supervision_socket)
        .expect("supervision socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = UnixStream::connect(&supervision_socket).expect("client connects");
    let codec = SupervisionFrameCodec::new(1024 * 1024);

    send_supervision_request(
        &mut stream,
        SupervisionRequest::Announce(Presence {
            expected_component: ComponentName::new("persona-message"),
            expected_kind: ComponentKind::Message,
            engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
        }),
    );
    let identity = codec.read_reply(&mut stream).expect("identity reply");
    assert!(matches!(
        identity,
        SupervisionReply::Identified(identity)
            if identity.name.as_str() == "persona-message"
                && identity.kind == ComponentKind::Message
    ));

    send_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(ComponentName::new(
            "persona-message",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::Ready(_)
    ));

    send_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::HealthStatus(ComponentName::new(
            "persona-message",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("health reply"),
        SupervisionReply::HealthReport(report)
            if report.health == ComponentHealth::Running
    ));
}

#[test]
fn persona_message_daemon_graceful_stop_releases_message_socket_and_rejects_ingress() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.message_socket_path();
    let router_socket_path = fixture.router_socket_path();
    let supervision_socket_path = fixture.supervision_socket_path();
    let start_path = fixture.start_path();
    let _router_listener = UnixListener::bind(&router_socket_path).expect("router socket binds");
    let configuration_path = fixture.write_message_daemon_configuration(OwnerIdentity::UnixUser(
        UnixUserId::new(unsafe { libc::geteuid() }),
    ));
    let daemon = fixture
        .spawn_daemon_after_router_start_with_configuration(&start_path, &configuration_path);
    std::fs::write(&start_path, "").expect("start marker writes");
    wait_for_path(&message_socket_path, "message socket");
    wait_for_path(&supervision_socket_path, "supervision socket");

    {
        let mut supervision =
            UnixStream::connect(&supervision_socket_path).expect("supervision client connects");
        let codec = SupervisionFrameCodec::new(1024 * 1024);
        send_supervision_request(
            &mut supervision,
            SupervisionRequest::Stop(ComponentName::new("persona-message")),
        );
        assert!(matches!(
            codec
                .read_reply(&mut supervision)
                .expect("graceful stop acknowledgement reads"),
            SupervisionReply::StopAcknowledged(_)
        ));
    }

    let daemon_output = wait_for_child_output(daemon, "message daemon exits after graceful stop");
    assert!(
        daemon_output.status.success(),
        "message daemon should exit cleanly after graceful stop: stderr={}",
        String::from_utf8_lossy(&daemon_output.stderr)
    );
    assert!(
        !message_socket_path.exists(),
        "message socket path is removed after daemon shutdown"
    );

    let shell = fixture.spawn_message_after_start(
        &start_path,
        Some(&message_socket_path),
        "(Send designer after-stop)",
    );
    let output = shell.wait_with_output().expect("message shell exits");
    assert!(
        !output.status.success(),
        "message CLI ingress after daemon stop should fail"
    );
}

#[test]
fn command_line_send_routes_signal_frame_without_writing_local_ledger() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.message_socket_path();
    let start_path = fixture.start_path();
    let fake_router =
        FakeRouter::bind(&message_socket_path, start_path.clone()).serve(RouterReply::accepted(7));
    let shell = fixture.spawn_message_after_start(
        &start_path,
        Some(&message_socket_path),
        "(Send designer signal-hello)",
    );

    let output = shell.wait_with_output().expect("message shell exits");
    let recorded = fake_router.join().expect("router thread joins");
    let text = String::from_utf8(output.stdout).expect("output is utf8");

    assert!(output.status.success());
    let MessageRequest::MessageSubmission(submission) = recorded.request else {
        panic!("expected message submission");
    };
    assert_eq!(submission.recipient.as_str(), "designer");
    assert_eq!(submission.kind, MessageKind::Send);
    assert_eq!(submission.body, MessageBody::new("signal-hello"));
    assert!(text.contains("(SubmissionAccepted 7)"));
    assert!(!fixture.local_ledger_path().exists());
}

#[test]
fn command_line_send_preserves_bare_identifier_body_in_signal_payload() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.message_socket_path();
    let start_path = fixture.start_path();
    let fake_router =
        FakeRouter::bind(&message_socket_path, start_path.clone()).serve(RouterReply::accepted(8));
    let shell = fixture.spawn_message_after_start(
        &start_path,
        Some(&message_socket_path),
        "(Send designer ready-token)",
    );

    let output = shell.wait_with_output().expect("message shell exits");
    let recorded = fake_router.join().expect("router thread joins");

    assert!(output.status.success());
    let MessageRequest::MessageSubmission(submission) = recorded.request else {
        panic!("expected message submission");
    };
    assert_eq!(submission.kind, MessageKind::Send);
    assert_eq!(submission.body.as_str(), "ready-token");
}

#[test]
fn command_line_inbox_routes_signal_frame_without_reading_local_ledger() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.message_socket_path();
    let start_path = fixture.start_path();
    let local_ledger_path = fixture.local_ledger_path();
    std::fs::create_dir_all(local_ledger_path.parent().expect("ledger parent"))
        .expect("ledger directory writes");
    std::fs::write(
        &local_ledger_path,
        "(Message m-old direct-operator-designer operator designer stale-local [])\n",
    )
    .expect("stale local ledger writes");
    let fake_router = FakeRouter::bind(&message_socket_path, start_path.clone())
        .serve(RouterReply::inbox("operator", "router-only"));
    let shell = fixture.spawn_message_after_start(
        &start_path,
        Some(&message_socket_path),
        "(Inbox designer)",
    );

    let output = shell.wait_with_output().expect("message shell exits");
    let recorded = fake_router.join().expect("router thread joins");
    let text = String::from_utf8(output.stdout).expect("output is utf8");

    assert!(output.status.success());
    let MessageRequest::InboxQuery(query) = recorded.request else {
        panic!("expected inbox query");
    };
    assert_eq!(query.recipient.as_str(), "designer");
    assert!(text.contains("RouterInboxListing"));
    assert!(text.contains("router-only"));
    assert!(!text.contains("stale-local"));
}

#[test]
fn message_output_validator_decodes_submission_accepted_nota() {
    let fixture = MessageFixture::new();
    let output_path = fixture.directory.path().join("accepted.nota");
    let output = persona_message::command::Output::from_router_reply(
        MessageReply::SubmissionAccepted(SubmissionAcceptance {
            message_slot: MessageSlot::new(19),
        }),
    )
    .expect("reply projects to command output")
    .to_nota()
    .expect("command output encodes");
    std::fs::write(&output_path, output).expect("output fixture writes");

    OutputValidatorCommandLine::from_arguments([
        "--file",
        output_path.to_str().expect("path is utf8"),
        "expect-submission-accepted",
    ])
    .run()
    .expect("validator accepts typed submission output");
}

#[test]
fn message_output_validator_decodes_router_inbox_listing_nota() {
    let fixture = MessageFixture::new();
    let output_path = fixture.directory.path().join("inbox.nota");
    let output = persona_message::command::Output::from_router_reply(MessageReply::InboxListing(
        InboxListing {
            messages: vec![InboxEntry {
                message_slot: MessageSlot::new(21),
                sender: MessageSender::new("reviewer"),
                body: MessageBody::new("reviewer completed task"),
            }],
        },
    ))
    .expect("reply projects to command output")
    .to_nota()
    .expect("command output encodes");
    std::fs::write(&output_path, output).expect("output fixture writes");

    OutputValidatorCommandLine::from_arguments([
        "--file",
        output_path.to_str().expect("path is utf8"),
        "expect-inbox-entry",
        "--sender",
        "reviewer",
        "--body",
        "reviewer completed task",
    ])
    .run()
    .expect("validator accepts typed inbox output");

    OutputValidatorCommandLine::from_arguments([
        "--file",
        output_path.to_str().expect("path is utf8"),
        "expect-inbox-body-absent",
        "--body",
        "message not present",
    ])
    .run()
    .expect("validator accepts absent body expectation");
}

#[test]
fn command_line_send_requires_message_socket() {
    let fixture = MessageFixture::new();
    let start_path = fixture.start_path();
    let shell = fixture.spawn_message_after_start(&start_path, None, "(Send designer hello)");
    std::fs::write(&start_path, "").expect("start marker writes");

    let output = shell.wait_with_output().expect("message shell exits");
    let stderr = String::from_utf8(output.stderr).expect("stderr is utf8");

    assert!(!output.status.success());
    assert!(stderr.contains("SignalMessageSocketMissing"));
    assert!(!fixture.local_ledger_path().exists());
}

#[test]
fn persona_message_daemon_forwards_cli_signal_frame_to_router_socket() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.message_socket_path();
    let router_socket_path = fixture.router_socket_path();
    let start_path = fixture.start_path();
    let fake_router =
        FakeRouter::bind(&router_socket_path, start_path.clone()).serve(RouterReply::accepted(11));
    let configuration_path = fixture.write_message_daemon_configuration(OwnerIdentity::UnixUser(
        UnixUserId::new(unsafe { libc::geteuid() }),
    ));
    let _ = (&message_socket_path, &router_socket_path);
    let mut daemon = fixture
        .spawn_daemon_after_router_start_with_configuration(&start_path, &configuration_path);

    for _ in 0..100 {
        if message_socket_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(message_socket_path.exists(), "daemon bound message socket");
    let shell = fixture.spawn_message_after_start(
        &start_path,
        Some(&message_socket_path),
        "(Send designer daemon-forward)",
    );

    let output = shell.wait_with_output().expect("message shell exits");
    let recorded = fake_router.join().expect("router thread joins");
    let text = String::from_utf8(output.stdout).expect("output is utf8");
    let _ = daemon.kill();
    let _ = daemon.wait();

    assert!(output.status.success());
    let MessageRequest::StampedMessageSubmission(stamped) = recorded.request else {
        panic!("expected daemon-forwarded stamped message submission");
    };
    let submission = stamped.submission;
    assert_eq!(submission.recipient.as_str(), "designer");
    assert_eq!(submission.kind, MessageKind::Send);
    assert_eq!(submission.body.as_str(), "daemon-forward");
    assert_eq!(
        stamped.origin,
        MessageOrigin::External(ConnectionClass::Owner)
    );
    assert!(stamped.stamped_at.into_u64() > 0);
    assert!(text.contains("(SubmissionAccepted 11)"));
    assert!(!fixture.local_ledger_path().exists());
}

#[test]
fn persona_message_daemon_forwards_component_ingress_as_internal_component_instance() {
    let fixture = MessageFixture::new();
    let message_socket_path = fixture.component_message_socket_path("initiator");
    let router_socket_path = fixture.router_socket_path();
    let start_path = fixture.start_path();
    let fake_router =
        FakeRouter::bind(&router_socket_path, start_path.clone()).serve(RouterReply::accepted(12));
    let origin = InternalComponentInstanceOrigin::new(
        ProvenanceComponentName::Harness,
        ComponentInstanceName::new("initiator"),
    );
    let configuration_path = fixture.write_message_daemon_configuration_with_ingresses(
        OwnerIdentity::UnixUser(UnixUserId::new(unsafe { libc::geteuid() })),
        vec![ComponentMessageIngress {
            origin: origin.clone(),
            socket_path: signal_persona::WirePath::new(message_socket_path.display().to_string()),
            socket_mode: signal_persona::SocketMode::new(0o600),
        }],
    );
    let mut daemon = fixture
        .spawn_daemon_after_router_start_with_configuration(&start_path, &configuration_path);

    for _ in 0..100 {
        if message_socket_path.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        message_socket_path.exists(),
        "daemon bound component ingress socket"
    );
    let shell = fixture.spawn_message_after_start(
        &start_path,
        Some(&message_socket_path),
        "(Send responder from-initiator)",
    );

    let output = shell.wait_with_output().expect("message shell exits");
    let recorded = fake_router.join().expect("router thread joins");
    let _ = daemon.kill();
    let _ = daemon.wait();

    assert!(output.status.success());
    let MessageRequest::StampedMessageSubmission(stamped) = recorded.request else {
        panic!("expected daemon-forwarded stamped message submission");
    };
    assert_eq!(stamped.submission.recipient.as_str(), "responder");
    assert_eq!(stamped.submission.body.as_str(), "from-initiator");
    assert_eq!(
        stamped.origin,
        MessageOrigin::InternalComponentInstance(origin)
    );
}

#[test]
fn command_line_takes_exactly_one_argument() {
    let command = CommandLine::from_arguments(["(Inbox", "designer)"]);
    let mut output = Vec::new();

    let error = command
        .run(&mut output)
        .expect_err("split nota is rejected");

    assert!(
        error
            .to_string()
            .contains("unexpected command-line argument")
    );
}

#[test]
fn input_rejects_unknown_record_heads() {
    let error = Input::from_nota("(Bead)").expect_err("unknown input is rejected");

    match error {
        persona_message::Error::Nota(Error::UnknownVariant { enum_name, got }) => {
            assert_eq!(enum_name, "Input");
            assert_eq!(got, "Bead");
        }
        other => panic!("expected unknown input kind, got {other:?}"),
    }
}

fn send_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(SupervisionFrameBody::Request {
        exchange: test_frame_exchange(),
        request: FrameRequest::from_payload(request),
    });
    std::io::Write::write_all(
        stream,
        frame
            .encode_length_prefixed()
            .expect("supervision request encodes")
            .as_slice(),
    )
    .expect("supervision request writes");
}

fn test_frame_exchange() -> FrameExchangeIdentifier {
    FrameExchangeIdentifier::new(
        FrameSessionEpoch::new(0),
        FrameExchangeLane::Connector,
        FrameLaneSequence::first(),
    )
}

fn wait_for_path(path: &Path, label: &str) {
    for _ in 0..100 {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("{label} did not appear at {}", path.display());
}

fn wait_for_child_output(mut child: Child, label: &str) -> Output {
    for _ in 0..100 {
        if child.try_wait().expect("child status checks").is_some() {
            return child.wait_with_output().expect(label);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let output = child.wait_with_output().expect(label);
    panic!(
        "{label} timed out: status={:?} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn test_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}
