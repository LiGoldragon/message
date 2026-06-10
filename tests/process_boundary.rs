//! Process-boundary witness for the migrated message daemon.
//!
//! Spawns the real `message-daemon` binary with a binary rkyv configuration,
//! connects to its `message.sock`, and exchanges schema-derived signal frames
//! over the wire. This proves the emitted daemon spine end to end: argv config
//! load -> single working-socket bind -> length-prefixed signal-frame decode ->
//! Nexus `decide` -> signal-frame encode -> wire reply.
//!
//! The `SubmitStamped` path is the router-independent witness: the daemon
//! replies `Unimplemented` directly from the Nexus decision without forwarding,
//! so the test needs no router mock to exercise the full emitted pipeline. The
//! router-forward paths (`Submit` / `QueryInbox`) are covered by the in-process
//! engine test below, which drives `MessageEngine::handle` against a stub
//! router listener.

use std::{
    io::Write,
    os::unix::{
        ffi::OsStrExt,
        net::{UnixListener, UnixStream},
    },
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use message::{
    Configuration,
    command::Output as CommandOutput,
    router::SignalRouterFrameCodec,
    schema::signal::{
        Body, Input, MessageKind, MessageOrigin, MessageSubmission, Output as SignalOutput,
        OwnerName, Recipient, StampedMessageSubmission, SubmitStamped, TimestampNanos,
    },
};
use signal_message::{
    ComponentInstanceName as RouterComponentInstanceName, ComponentName as RouterComponentName,
    Input as SignalMessageInput, InternalComponentInstanceOrigin as RouterInstanceOrigin,
    MessageOrigin as RouterMessageOrigin, MessageSlot, Output as SignalMessageOutput,
    SubmissionAcceptance,
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
    fn spawn(socket_path: &Path, router_socket_path: &Path, database_path: &Path) -> Self {
        let configuration_path = socket_path.with_extension("config.rkyv");
        Configuration::new(
            socket_path,
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

struct StubRouter {
    listener: UnixListener,
}

impl StubRouter {
    fn bind(socket_path: &Path) -> Self {
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

#[test]
fn daemon_replies_unimplemented_for_already_stamped_submission_over_real_socket() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("message.sock");
    let router_socket_path = temp.path().join("router.sock");
    let database_path = temp.path().join("message.unused");

    let _daemon = DaemonProcess::spawn(&socket_path, &router_socket_path, &database_path);

    // An already-stamped submission: the daemon mints provenance, it never
    // accepts it from a peer, so this replies Unimplemented straight from the
    // Nexus decision — no router contact required.
    let stamped = Input::SubmitStamped(SubmitStamped::new(StampedMessageSubmission {
        message_submission: MessageSubmission {
            recipient: Recipient::new("designer".to_owned()),
            message_kind: MessageKind::Send,
            body: Body::new("already stamped".to_owned()),
        },
        message_origin: MessageOrigin {
            connection_class: message::schema::signal::ConnectionClass::Owner,
            owner_name: OwnerName::new("peer".to_owned()),
        },
        timestamp_nanos: TimestampNanos::new(1),
    }));

    match exchange(&socket_path, &stamped) {
        SignalOutput::Unimplemented(unimplemented) => {
            let unimplemented = unimplemented.into_payload();
            assert_eq!(
                unimplemented.operation_kind,
                message::schema::signal::OperationKind::SubmitStamped
            );
        }
        other => panic!("expected Unimplemented for already-stamped submission, got {other:?}"),
    }
}

#[test]
fn cli_send_crosses_generated_daemon_socket_and_forwards_to_router() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("message.sock");
    let router_socket_path = temp.path().join("router.sock");
    let database_path = temp.path().join("message.unused");
    let router = StubRouter::bind(&router_socket_path);
    let router_thread = router.serve_one_acceptance();

    let _daemon = DaemonProcess::spawn(&socket_path, &router_socket_path, &database_path);

    let cli_output = Command::new(env!("CARGO_BIN_EXE_message"))
        .env("MESSAGE_SOCKET", &socket_path)
        .arg("(Send designer [hello from cli])")
        .output()
        .expect("run message CLI");

    assert!(
        cli_output.status.success(),
        "message CLI failed: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
    let stdout = String::from_utf8(cli_output.stdout).expect("CLI stdout is utf8");
    match CommandOutput::from_nota(stdout.trim()).expect("decode CLI NOTA output") {
        CommandOutput::SubmissionAccepted(message_slot) => {
            assert_eq!(message_slot, 7);
        }
        other => panic!("expected CLI SubmissionAccepted output, got {other:?}"),
    }

    let forwarded = router_thread.join().expect("router thread");
    match forwarded {
        SignalMessageInput::SubmitStamped(stamped) => {
            assert_eq!(stamped.submission.recipient.as_str(), "designer");
            assert_eq!(stamped.submission.body.as_str(), "hello from cli");
            assert_eq!(
                stamped.origin,
                RouterMessageOrigin::InternalComponentInstance(RouterInstanceOrigin {
                    component: RouterComponentName::Harness,
                    instance: RouterComponentInstanceName::new("owner".to_owned()),
                })
            );
        }
        other => panic!("expected daemon to stamp CLI submission before forwarding, got {other:?}"),
    }
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
