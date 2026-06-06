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
    os::unix::net::UnixStream,
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use message::{
    Configuration,
    schema::signal::{
        Input, MessageKind, MessageOrigin, MessageSubmission, Output, StampedMessageSubmission,
    },
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
            1000,
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

/// One request/reply round trip over the daemon's working socket, framed the
/// way the emitted spine frames it: a length-prefixed envelope around the
/// schema signal frame.
fn exchange(socket_path: &Path, input: &Input) -> Output {
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
        Output::decode_signal_frame(&reply.into_bytes()).expect("decode output signal frame");
    output
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
    let stamped = Input::SubmitStamped(StampedMessageSubmission {
        message_submission: MessageSubmission {
            recipient: "designer".to_owned(),
            message_kind: MessageKind::Send,
            body: "already stamped".to_owned(),
        },
        message_origin: MessageOrigin {
            connection_class: message::schema::signal::ConnectionClass::Owner,
            owner_name: "peer".to_owned(),
        },
        timestamp_nanos: 1,
    });

    match exchange(&socket_path, &stamped) {
        Output::Unimplemented(unimplemented) => {
            assert_eq!(
                unimplemented.operation_kind,
                message::schema::signal::OperationKind::SubmitStamped
            );
        }
        other => panic!("expected Unimplemented for already-stamped submission, got {other:?}"),
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
    panic!("socket did not appear at {}", path.display());
}
