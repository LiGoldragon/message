//! Packet 3.5: the PtySocket end-to-end proof — the ground-truth report's
//! named gap. A REAL terminal-cell daemon owns a REAL PTY running `cat`; the
//! REAL messenger engine seats an agent, binds the session's actual
//! `data.sock` as its PtySocket endpoint (exactly the endpoint orchestrate's
//! reachability discovery pushes), and a Submit delivers through the 3.2a
//! leg. The witness is terminal-cell's own transcript wait: the delivered
//! note rendered in the live PTY.
//!
//! Environment-gated (the testing skill's stateful-fixture pattern): set
//! `TERMINAL_CELL_BINARY`, `TERMINAL_CELL_WAIT_BINARY`, and
//! `TERMINAL_CELL_DAEMON_BIN` to the built terminal-cell binaries; the test
//! skips cleanly when they are absent.

use std::path::PathBuf;
use std::process::Command;

use message::schema::signal::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentifier,
    AgentIdentityAssignment, AssignAgentIdentity, BindAgentEndpoint, Body, EndpointPath,
    HarnessPid, HarnessStartTime, Input, Kind, MessageKind, MessageSubmission, Output,
    ProcessPinSelection, Recipient, ResumeSelection, Submit, ThreadSelection,
};
use message::{MessageEngine, MessengerTables, OriginPolicy};
use tempfile::TempDir;
use triad_runtime::{ConnectionContext, UnixCredentials};

const OWNER_USER_ID: u32 = 1000;

fn binary(name: &str) -> Option<PathBuf> {
    std::env::var_os(name).map(PathBuf::from).filter(|path| path.exists())
}

async fn drive(engine: &mut MessageEngine, input: Input) -> Output {
    engine
        .handle(
            input,
            &ConnectionContext::from(UnixCredentials::new(OWNER_USER_ID, OWNER_USER_ID, 77)),
        )
        .await
        .expect("engine handle")
}

#[tokio::test]
async fn a_send_lands_in_a_live_terminal_cell_pty_via_the_messenger_path() {
    let (Some(cell_binary), Some(wait_binary)) = (
        binary("TERMINAL_CELL_BINARY"),
        binary("TERMINAL_CELL_WAIT_BINARY"),
    ) else {
        eprintln!(
            "skipping PTY end-to-end: TERMINAL_CELL_BINARY / TERMINAL_CELL_WAIT_BINARY not set"
        );
        return;
    };

    // 1. A real terminal-cell session running `cat`: programmatic input
    //    echoes straight back into the PTY transcript. The runtime root must
    //    stay short — the session's Unix socket paths live under it and are
    //    bounded by SUN_LEN.
    let runtime_root = PathBuf::from(format!("/tmp/mp35-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&runtime_root);
    std::fs::create_dir_all(&runtime_root).expect("short runtime root");
    let mut launcher = Command::new(&cell_binary)
        .env("TERMINAL_CELL_RUNTIME_DIR", &runtime_root)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("terminal-cell spawns");
    {
        use std::io::Write;
        launcher
            .stdin
            .take()
            .expect("launcher stdin")
            .write_all(b"(LaunchCell (None None cat [] []))")
            .expect("launch request written");
    }
    let launch = launcher
        .wait_with_output()
        .expect("terminal-cell launches");
    let reply = String::from_utf8_lossy(&launch.stdout).into_owned();
    assert!(
        launch.status.success(),
        "cell launch succeeds: {reply} {}",
        String::from_utf8_lossy(&launch.stderr)
    );
    let socket_for = |name: &str| -> String {
        reply
            .split_whitespace()
            .map(|token| token.trim_matches(['(', ')', '[', ']']))
            .find(|token| token.ends_with(name))
            .unwrap_or_else(|| panic!("launch reply names {name}: {reply}"))
            .to_string()
    };
    let data_socket = socket_for("data.sock");
    let control_socket = socket_for("control.sock");

    // 2. The real messenger engine: seat the agent and bind the session's
    //    actual data socket as its PtySocket endpoint — the same binding
    //    orchestrate's reachability discovery pushes after registration.
    let store_root = TempDir::new().expect("store root");
    let mut engine = MessageEngine::new(
        MessengerTables::open(&store_root.path().join("messenger.sema"))
            .expect("open messenger store"),
        OriginPolicy::for_owner_user_id(OWNER_USER_ID, "operator"),
    );
    let seated = drive(
        &mut engine,
        Input::AssignAgentIdentity(AssignAgentIdentity::new(AgentIdentityAssignment {
            agent_identifier: AgentIdentifier::new("li7f".to_owned()),
            process_pin_selection: ProcessPinSelection::None,
            resume_selection: ResumeSelection::None,
        })),
    )
    .await;
    assert!(matches!(seated, Output::AgentIdentityAssigned(_)));
    let bound = drive(
        &mut engine,
        Input::BindAgentEndpoint(BindAgentEndpoint::new(AgentEndpointBinding {
            agent_identifier: AgentIdentifier::new("li7f".to_owned()),
            agent_endpoint: AgentEndpoint {
                agent_endpoint_kind: AgentEndpointKind::PtySocket,
                endpoint_path: EndpointPath::new(data_socket.clone()),
            },
            harness_pid: HarnessPid::new(4242),
            harness_start_time: HarnessStartTime::new(777888),
        })),
    )
    .await;
    assert!(matches!(bound, Output::AgentEndpointBound(_)));

    // 3. One real Send through the full local path.
    let reply = drive(
        &mut engine,
        Input::Submit(Submit::new(MessageSubmission {
            recipient: Recipient::new("li7f".to_owned()),
            kind: Kind::new(MessageKind::Send),
            body: Body::new("pty-end-to-end-proof".to_owned()),
            thread_selection: ThreadSelection::None,
        })),
    )
    .await;
    assert!(
        matches!(reply, Output::SubmissionAccepted(_)),
        "submission accepted, got {reply:?}"
    );

    // 4. The witness: terminal-cell's own transcript wait sees the delivered
    //    note text in the live PTY (cat echoed the injected input).
    let waited = Command::new(&wait_binary)
        .arg("--control-socket")
        .arg(&control_socket)
        .arg("--text")
        .arg("pty-end-to-end-proof")
        .output()
        .expect("terminal-cell-wait runs");
    assert!(
        waited.status.success(),
        "delivered message rendered in the live PTY transcript: {} {}",
        String::from_utf8_lossy(&waited.stdout),
        String::from_utf8_lossy(&waited.stderr)
    );
    let _ = std::fs::remove_dir_all(&runtime_root);
}
