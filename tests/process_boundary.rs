//! Process-boundary witness for the stateful messenger daemon.
//!
//! Spawns the real `message-daemon` binary with a binary rkyv configuration,
//! connects to its `message.sock`, and exchanges schema-derived signal frames
//! over the wire. This proves the emitted daemon spine end to end: argv config
//! load -> single working-socket bind -> length-prefixed signal-frame decode ->
//! Nexus `decide` -> SEMA commit in `messenger.sema` -> signal-frame encode ->
//! wire reply. No router is involved anywhere: the messenger is the durable
//! owner of local message state (packet 3.1).

use std::{
    io::Write,
    os::unix::{ffi::OsStrExt, net::UnixStream},
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use message::{
    Configuration,
    command::Output as CommandOutput,
    schema::signal::{
        Body, ConnectionClass, Input, MessageKind, MessageOrigin, MessageSubmission,
        Output as SignalOutput, Recipient, StampedMessageSubmission, SubmitStamped,
        ThreadSelection, TimestampNanos,
    },
};
use meta_signal_message::Operation as MetaMessageOperation;
use nota::NotaEncode;
use signal_frame::RequestPayload;
use signal_message::{
    MessageDaemonConfiguration as MetaConfiguration, MessageDaemonConfigurationParts,
    OwnerIdentity, SocketMode, UnixUserIdentifier, WirePath,
};
use tempfile::TempDir;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

struct DaemonProcess {
    child: Child,
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl DaemonProcess {
    fn spawn(
        socket_path: &Path,
        meta_socket_path: &Path,
        router_socket_path: &Path,
        database_path: &Path,
    ) -> Self {
        let configuration_path = socket_path.with_extension("config.rkyv");
        Configuration::new(
            socket_path,
            meta_socket_path,
            router_socket_path,
            database_path,
            "owner",
            CurrentProcessUser::owner_user_id(),
        )
        .write_binary_file(&configuration_path)
        .expect("write binary daemon configuration");
        let child = Command::new(env!("CARGO_BIN_EXE_message-daemon"))
            .arg(configuration_path)
            .spawn()
            .expect("spawn message daemon");
        let process = Self { child };
        wait_for_socket(socket_path);
        wait_for_socket(meta_socket_path);
        process
    }
}

struct CurrentProcessUser;

impl CurrentProcessUser {
    fn owner_user_id() -> u32 {
        rustix::process::getuid().as_raw()
    }
}

/// One request/reply round trip over the daemon's working socket, framed the
/// way the emitted spine frames it: a length-prefixed envelope around the
/// schema signal frame.
fn exchange(socket_path: &Path, input: &Input) -> SignalOutput {
    let mut stream = UnixStream::connect(socket_path).expect("connect to message socket");
    let codec = LengthPrefixedCodec::default();
    let body = FrameBody::new(
        input
            .encode_signal_frame()
            .expect("encode input signal frame"),
    );
    codec
        .write_body(&mut stream, &body)
        .expect("write request frame");
    stream.flush().expect("flush request");
    let reply = codec.read_body(&mut stream).expect("read reply frame");
    let (_route, output) =
        SignalOutput::decode_signal_frame(&reply.into_bytes()).expect("decode output signal frame");
    output
}

#[test]
fn daemon_replies_unimplemented_for_already_stamped_submission_over_real_socket() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("message.sock");
    let meta_socket_path = temp.path().join("meta-message.sock");
    let router_socket_path = temp.path().join("router.sock");
    let database_path = temp.path().join("messenger.sema");

    let _daemon = DaemonProcess::spawn(
        &socket_path,
        &meta_socket_path,
        &router_socket_path,
        &database_path,
    );

    // An already-stamped submission: the daemon mints provenance, it never
    // accepts it from a peer, so this replies Unimplemented straight from the
    // Nexus decision.
    let stamped = Input::SubmitStamped(SubmitStamped::new(StampedMessageSubmission {
        submission: MessageSubmission {
            recipient: Recipient::new("designer".to_owned()),
            kind: MessageKind::Send.into(),
            body: Body::new("already stamped".to_owned()),
            thread_selection: ThreadSelection::None,
        }
        .into(),
        origin: MessageOrigin::External(ConnectionClass::Owner).into(),
        stamped_at: TimestampNanos::new(1).into(),
    }));

    match exchange(&socket_path, &stamped) {
        SignalOutput::Unimplemented(unimplemented) => {
            let unimplemented = unimplemented.into_payload();
            assert_eq!(
                unimplemented.unimplemented_operation_kind.into_payload(),
                message::schema::signal::OperationKind::SubmitStamped
            );
        }
        other => panic!("expected Unimplemented for already-stamped submission, got {other:?}"),
    }
}

#[test]
fn cli_send_persists_locally_and_inbox_reads_it_back_across_the_real_socket() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("message.sock");
    let meta_socket_path = temp.path().join("meta-message.sock");
    let router_socket_path = temp.path().join("router.sock");
    let database_path = temp.path().join("messenger.sema");

    let _daemon = DaemonProcess::spawn(
        &socket_path,
        &meta_socket_path,
        &router_socket_path,
        &database_path,
    );

    let send_output = Command::new(env!("CARGO_BIN_EXE_message"))
        .env("MESSAGE_SOCKET", &socket_path)
        .arg("(Send designer [hello from cli] (Named launch-plan))")
        .output()
        .expect("run message CLI send");
    assert!(
        send_output.status.success(),
        "message CLI send failed: {}",
        String::from_utf8_lossy(&send_output.stderr)
    );
    let send_stdout = String::from_utf8(send_output.stdout).expect("CLI stdout is utf8");
    match CommandOutput::from_nota(send_stdout.trim()).expect("decode CLI NOTA output") {
        CommandOutput::SubmissionAccepted(message_slot) => {
            assert_eq!(message_slot, 0, "first slot in a fresh store");
        }
        other => panic!("expected CLI SubmissionAccepted output, got {other:?}"),
    }

    let inbox_output = Command::new(env!("CARGO_BIN_EXE_message"))
        .env("MESSAGE_SOCKET", &socket_path)
        .arg("(Inbox designer)")
        .output()
        .expect("run message CLI inbox");
    assert!(
        inbox_output.status.success(),
        "message CLI inbox failed: {}",
        String::from_utf8_lossy(&inbox_output.stderr)
    );
    let inbox_stdout = String::from_utf8(inbox_output.stdout).expect("CLI stdout is utf8");
    match CommandOutput::from_nota(inbox_stdout.trim()).expect("decode CLI inbox output") {
        CommandOutput::InboxListing(entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].body, "hello from cli");
            assert_eq!(entries[0].sender.as_str(), "owner");
            assert!(entries[0].stamped_at > 0, "ingress stamp minted");
        }
        other => panic!("expected CLI InboxListing output, got {other:?}"),
    }

    let thread_output = Command::new(env!("CARGO_BIN_EXE_message"))
        .env("MESSAGE_SOCKET", &socket_path)
        .arg("(Thread launch-plan)")
        .output()
        .expect("run message CLI thread");
    assert!(
        thread_output.status.success(),
        "message CLI thread failed: {}",
        String::from_utf8_lossy(&thread_output.stderr)
    );
    let thread_stdout = String::from_utf8(thread_output.stdout).expect("CLI stdout is utf8");
    match CommandOutput::from_nota(thread_stdout.trim()).expect("decode CLI thread output") {
        CommandOutput::ThreadListing(contents) => {
            assert_eq!(contents.thread_entries.payload().len(), 1);
        }
        other => panic!("expected CLI ThreadListing output, got {other:?}"),
    }
}

#[test]
fn meta_cli_reaches_owner_policy_socket_and_gets_typed_unimplemented_reply() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("message.sock");
    let meta_socket_path = temp.path().join("meta-message.sock");
    let router_socket_path = temp.path().join("router.sock");
    let database_path = temp.path().join("messenger.sema");

    let _daemon = DaemonProcess::spawn(
        &socket_path,
        &meta_socket_path,
        &router_socket_path,
        &database_path,
    );

    let request =
        MetaMessageOperation::Configure(MetaConfiguration::from(MessageDaemonConfigurationParts {
            message_socket_path: WirePath::new(socket_path.to_string_lossy().into_owned()),
            message_socket_mode: SocketMode::new(0o660),
            supervision_socket_path: WirePath::new(
                temp.path()
                    .join("message-supervision.sock")
                    .to_string_lossy()
                    .into_owned(),
            ),
            supervision_socket_mode: SocketMode::new(0o600),
            router_socket_path: WirePath::new(router_socket_path.to_string_lossy().into_owned()),
            component_ingresses: Vec::new(),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(u64::from(
                CurrentProcessUser::owner_user_id(),
            ))),
        }))
        .into_request()
        .to_nota();

    let cli_output = Command::new(env!("CARGO_BIN_EXE_meta-message"))
        .env("MESSAGE_META_SOCKET", &meta_socket_path)
        .arg(request)
        .output()
        .expect("run meta-message CLI");

    assert!(
        cli_output.status.success(),
        "meta-message CLI failed: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
    let stdout = String::from_utf8(cli_output.stdout).expect("meta CLI stdout is utf8");
    assert!(stdout.contains("RequestUnimplemented"));
    assert!(stdout.contains("Configure"));
    assert!(stdout.contains("NotBuiltYet"));
}

fn wait_for_socket(path: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "socket did not appear at {}",
        String::from_utf8_lossy(path.as_os_str().as_bytes())
    );
}
