//! Where the Message Nexus's configuration comes from: the executable's
//! defaults on a new store, the store itself on every later start, and meta
//! Configure as the only way to change it. No argument and no environment
//! variable beyond the two anchors (HOME, XDG_RUNTIME_DIR) reaches it.

mod support;

use meta_signal_flow::Content;
use meta_signal_message::{
    Activation, MessageConfiguration, Query as MetaQuery, Response as MetaResponse,
};
use signal_flow::FlowAspect;
use signal_message::{Priority, Query, Response, SendRequest};
use std::{path::Path, process::Command};
use support::{
    NexusProcess,
    fake_flow::{FakeFlow, Pane},
};

const SENDER: &str = "5e11de";
const RECIPIENT: &str = "7d41e0";

fn send(text: &str) -> Query {
    Query::Send(SendRequest {
        flow_id_vector: vec![RECIPIENT.into()],
        priority: Priority::Soft,
        content: Content::Text(text.into()),
    })
}

fn path_text(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// The configuration the defaults would give a runtime directory, with
/// Flow's sockets under another directory.
fn configuration(runtime: &Path, flow_runtime: &Path) -> MessageConfiguration {
    MessageConfiguration {
        ordinary_socket_path: path_text(&runtime.join("message/message.sock")),
        meta_socket_path: path_text(&runtime.join("message/message-owner.sock")),
        flow_socket_path: path_text(&flow_runtime.join("flow/flow.sock")),
        flow_meta_socket_path: path_text(&flow_runtime.join("flow/flow-meta.sock")),
        meta_aspects: vec![FlowAspect::Psyche],
    }
}

fn configured(response: MetaResponse) -> Activation {
    match response {
        MetaResponse::Configured(configured) => configured.activation,
        other => panic!("expected Configured, got {other:?}"),
    }
}

#[test]
fn a_nexus_given_a_configuration_file_argument_does_not_start() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_message-nexus"))
        .arg(home.path().join(".local/state/message/message-daemon.signal"))
        .env_clear()
        .env("HOME", home.path())
        .env("XDG_RUNTIME_DIR", runtime.path())
        .status()
        .expect("message-nexus runs");
    assert!(!status.success());
    assert!(!runtime.path().join("message/message.sock").exists());
    assert!(!home.path().join(".local/state/message/message.sema").exists());
}

#[test]
fn a_configured_flow_edge_is_resumed_after_a_restart() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let first_flow = FakeFlow::start(runtime.path());
    let elsewhere = tempfile::tempdir().unwrap();
    let second_flow = FakeFlow::start(elsewhere.path());
    for flow in [&first_flow, &second_flow] {
        flow.set_pane(RECIPIENT, Pane::Idle);
    }
    let configuration = configuration(runtime.path(), elsewhere.path());
    let nexus = NexusProcess::start(home, runtime);
    first_flow.set_peer(None);
    assert_eq!(
        configured(nexus.ask_meta(&MetaQuery::Configure(configuration))),
        Activation::Applied
    );
    let (home, runtime) = nexus.stop();
    let nexus = NexusProcess::start(home, runtime);
    second_flow.set_peer(Some(FakeFlow::caller(SENDER, FlowAspect::Field)));
    assert!(matches!(
        nexus.ask(&send("after the restart")),
        Response::Submitted(_)
    ));
    assert_eq!(second_flow.typed().len(), 1);
    assert!(first_flow.typed().is_empty());
}

#[test]
fn a_restarted_nexus_listens_where_configure_moved_its_sockets() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    flow.set_peer(None);
    let mut moved = configuration(runtime.path(), runtime.path());
    let moved_ordinary = runtime.path().join("moved/message.sock");
    let moved_meta = runtime.path().join("moved/message-owner.sock");
    moved.ordinary_socket_path = path_text(&moved_ordinary);
    moved.meta_socket_path = path_text(&moved_meta);
    let nexus = NexusProcess::start(home, runtime);
    assert_eq!(
        configured(nexus.ask_meta(&MetaQuery::Configure(moved))),
        Activation::NexusRestartRequired
    );
    let (home, runtime) = nexus.stop();
    let default_ordinary = runtime.path().join("message/message.sock");
    let nexus =
        NexusProcess::start_listening_at(home, runtime, moved_ordinary, moved_meta, &[]);
    assert!(!default_ordinary.exists());
    assert!(matches!(
        nexus.ask_meta(&MetaQuery::Configure(configuration(
            Path::new("/unused"),
            Path::new("/unused")
        ))),
        MetaResponse::Configured(_)
    ));
}

#[test]
fn environment_variables_beyond_the_anchors_do_not_configure_the_nexus() {
    let home = tempfile::tempdir().unwrap();
    let runtime = tempfile::tempdir().unwrap();
    let flow = FakeFlow::start(runtime.path());
    flow.set_pane(RECIPIENT, Pane::Idle);
    flow.set_peer(Some(FakeFlow::caller(SENDER, FlowAspect::Field)));
    let elsewhere = tempfile::tempdir().unwrap();
    let decoy = FakeFlow::start(elsewhere.path());
    decoy.set_pane(RECIPIENT, Pane::Idle);
    decoy.set_peer(Some(FakeFlow::caller(SENDER, FlowAspect::Field)));
    let decoy_socket = path_text(&elsewhere.path().join("flow/flow.sock"));
    let decoy_meta = path_text(&elsewhere.path().join("flow/flow-meta.sock"));
    let ordinary = runtime.path().join("message/message.sock");
    let meta = runtime.path().join("message/message-owner.sock");
    let nexus = NexusProcess::start_listening_at(
        home,
        runtime,
        ordinary,
        meta,
        &[
            ("MESSAGE_SOCKET", "/nonexistent/message.sock"),
            ("MESSAGE_META_SOCKET", "/nonexistent/message-owner.sock"),
            ("FLOW_SOCKET", &decoy_socket),
            ("FLOW_META_SOCKET", &decoy_meta),
        ],
    );
    assert!(matches!(nexus.ask(&send("by default")), Response::Submitted(_)));
    assert_eq!(flow.typed().len(), 1);
    assert!(decoy.typed().is_empty());
}
