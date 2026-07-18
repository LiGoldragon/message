//! Witnesses for the messenger's own delivery leg (packet 3.2a): the router
//! is NOT in the loop. A fake terminal-cell endpoint (a Unix listener
//! speaking the `'P'`-frame protocol) proves the PtySocket leg; the durable
//! outbox proves parking; `BindAgentEndpoint` proves drain-on-appearance;
//! thread fan-out proves the group leg.

use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use message::schema::signal::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentifier,
    AgentIdentityAssignment, Body, EndpointPath, HarnessPid, HarnessStartTime, Input, Kind,
    MessageKind, MessageSubmission, Output, ProcessPinSelection, Recipient, ResumeSelection,
    Submit, ThreadName, ThreadSelection,
};
use message::{MessageEngine, MessengerTables, OriginPolicy};
use tempfile::TempDir;
use triad_runtime::{ConnectionContext, UnixCredentials};

const OWNER_USER_ID: u32 = 1000;
const OWNER_INSTANCE: &str = "operator";

fn owner_connection() -> ConnectionContext {
    ConnectionContext::from(UnixCredentials::new(OWNER_USER_ID, OWNER_USER_ID, 101))
}

fn engine_for(root: &TempDir) -> MessageEngine {
    MessageEngine::new(
        MessengerTables::open(&root.path().join("messenger.sema")).expect("open messenger store"),
        OriginPolicy::for_owner_user_id(OWNER_USER_ID, OWNER_INSTANCE),
    )
}

/// A fake terminal-cell data socket: accepts `'P'` frames, replies `'A'`,
/// and reports each delivered text on a channel.
struct FakeTerminalCell {
    path: PathBuf,
    received: mpsc::Receiver<String>,
}

impl FakeTerminalCell {
    fn bind(path: &Path) -> Self {
        let listener = UnixListener::bind(path).expect("bind fake terminal cell");
        let (sender, received) = mpsc::channel();
        thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let mut head = [0_u8; 1];
                if stream.read_exact(&mut head).is_err() || head[0] != b'P' {
                    continue;
                }
                let mut length = [0_u8; 8];
                stream.read_exact(&mut length).expect("read length");
                let mut text = vec![0_u8; u64::from_be_bytes(length) as usize];
                stream.read_exact(&mut text).expect("read text");
                stream.write_all(b"A").expect("acknowledge");
                let _ = sender.send(String::from_utf8(text).expect("utf8 delivery"));
            }
        });
        Self {
            path: path.to_owned(),
            received,
        }
    }

    fn delivered(&self) -> Option<String> {
        self.received
            .recv_timeout(std::time::Duration::from_secs(2))
            .ok()
    }
}

async fn drive(engine: &mut MessageEngine, input: Input) -> Output {
    engine
        .handle(input, &owner_connection())
        .await
        .expect("engine handle")
}

async fn seat_and_bind(engine: &mut MessageEngine, agent: &str, endpoint_path: &Path) {
    seat(engine, agent).await;
    bind(engine, agent, endpoint_path).await;
}

async fn seat(engine: &mut MessageEngine, agent: &str) {
    let seated = drive(
        engine,
        Input::AssignAgentIdentity(message::schema::signal::AssignAgentIdentity::new(
            AgentIdentityAssignment {
                agent_identifier: AgentIdentifier::new(agent.to_owned()),
                process_pin_selection: ProcessPinSelection::None,
                resume_selection: ResumeSelection::None,
            },
        )),
    )
    .await;
    assert!(matches!(seated, Output::AgentIdentityAssigned(_)));
}

async fn bind(engine: &mut MessageEngine, agent: &str, endpoint_path: &Path) {
    let bound = drive(
        engine,
        Input::BindAgentEndpoint(message::schema::signal::BindAgentEndpoint::new(
            AgentEndpointBinding {
                agent_identifier: AgentIdentifier::new(agent.to_owned()),
                agent_endpoint: AgentEndpoint {
                    agent_endpoint_kind: AgentEndpointKind::PtySocket,
                    endpoint_path: EndpointPath::new(
                        endpoint_path.to_string_lossy().into_owned(),
                    ),
                },
                harness_pid: HarnessPid::new(4242),
                harness_start_time: HarnessStartTime::new(777888),
            },
        )),
    )
    .await;
    assert!(matches!(bound, Output::AgentEndpointBound(_)));
}

fn submission(recipient: &str, body: &str, thread: ThreadSelection) -> Input {
    Input::Submit(Submit::new(MessageSubmission {
        recipient: Recipient::new(recipient.to_owned()),
        kind: Kind::new(MessageKind::Send),
        body: Body::new(body.to_owned()),
        thread_selection: thread,
    }))
}

#[tokio::test]
async fn send_to_a_bound_agent_lands_in_the_terminal_cell_without_the_router() {
    let root = TempDir::new().expect("tempdir");
    let cell = FakeTerminalCell::bind(&root.path().join("data.sock"));
    let mut engine = engine_for(&root);
    seat_and_bind(&mut engine, "li7f", &cell.path).await;

    let reply = drive(
        &mut engine,
        submission("li7f", "delivered live", ThreadSelection::None),
    )
    .await;
    assert!(matches!(reply, Output::SubmissionAccepted(_)));

    let text = cell.delivered().expect("terminal cell received the message");
    assert!(
        text.contains("delivered live") && text.contains(OWNER_INSTANCE),
        "delivered text carries body and sender: {text}"
    );
}

#[tokio::test]
async fn send_to_an_unbound_agent_parks_and_drains_when_the_endpoint_appears() {
    let root = TempDir::new().expect("tempdir");
    let mut engine = engine_for(&root);
    seat(&mut engine, "x2qb").await;

    let reply = drive(
        &mut engine,
        submission("x2qb", "waiting for you", ThreadSelection::None),
    )
    .await;
    assert!(matches!(reply, Output::SubmissionAccepted(_)));

    let cell = FakeTerminalCell::bind(&root.path().join("data.sock"));
    bind(&mut engine, "x2qb", &cell.path).await;

    let text = cell.delivered().expect("outbox drained on endpoint appearance");
    assert!(text.contains("waiting for you"), "drained text: {text}");
}

#[tokio::test]
async fn thread_send_fans_out_to_every_participant_except_the_sender() {
    let root = TempDir::new().expect("tempdir");
    let cell_one = FakeTerminalCell::bind(&root.path().join("one.sock"));
    let cell_two = FakeTerminalCell::bind(&root.path().join("two.sock"));
    let mut engine = engine_for(&root);
    seat_and_bind(&mut engine, "li7f", &cell_one.path).await;
    seat_and_bind(&mut engine, "x2qb", &cell_two.path).await;

    // Both agents join the thread by sending into it once.
    drive(
        &mut engine,
        submission(
            "x2qb",
            "hello two",
            ThreadSelection::Named(ThreadName::new("subagents".to_owned())),
        ),
    )
    .await;
    cell_two.delivered().expect("direct delivery to two");
    drive(
        &mut engine,
        submission(
            "li7f",
            "hello one",
            ThreadSelection::Named(ThreadName::new("subagents".to_owned())),
        ),
    )
    .await;
    cell_one.delivered().expect("direct delivery to one");

    // A send addressed TO the thread reaches all participants except the
    // sender (the owner instance sent every message above, so both agents
    // receive).
    let reply = drive(
        &mut engine,
        submission("subagents", "all hands", ThreadSelection::None),
    )
    .await;
    assert!(matches!(reply, Output::SubmissionAccepted(_)));
    assert!(
        cell_one.delivered().is_some_and(|text| text.contains("all hands")),
        "participant one received the thread send"
    );
    assert!(
        cell_two.delivered().is_some_and(|text| text.contains("all hands")),
        "participant two received the thread send"
    );
}
