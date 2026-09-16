//! Isolated process fixture for source selection before any delivery adapter.

use message::{Configuration, client::MessageSocket};
use signal_message::{
    FlowIdleAnnouncement, MessageDaemonConfiguration, OwnerIdentity, Query, Response,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::{Digest, Sha1};
use std::{
    fs,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

const BODY: &str = "one two three four five six seven eight nine ten eleven twelve";

fn transcript(path: &Path, copies: usize) {
    let record = serde_json::json!({
        "type": "user",
        "uuid": "claude-turn-1",
        "sessionId": "cf7879-session",
        "timestamp": "2026-09-16T00:00:00Z",
        "message": { "content": BODY },
    })
    .to_string();
    fs::write(
        path,
        std::iter::repeat_n(format!("{record}\n"), copies).collect::<String>(),
    )
    .unwrap();
}

fn relay(path: &Path) -> Command {
    relay_selection(
        path,
        "one two three four five six",
        "seven eight nine ten eleven twelve",
    )
}

fn relay_selection(path: &Path, head: &str, tail: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_relay"));
    command
        .env_clear()
        .env("FLOW_ID", "cf7879")
        .env("RELAY_SESSION_ID", "cf7879-session")
        .env("RELAY_CLUSTER_MEMBERS", "cf7879@cf7879-session")
        .env("RELAY_TRANSCRIPT", path)
        .env("HOME", path.parent().unwrap())
        .arg(head)
        .arg(tail);
    command
}

fn headers(stream: &mut UnixStream) -> String {
    let mut received = Vec::new();
    loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        received.push(byte[0]);
        if received.ends_with(b"\r\n\r\n") {
            return String::from_utf8(received).unwrap();
        }
        assert!(received.len() <= 16_384, "HTTP upgrade was too large");
    }
}

fn client_frame(stream: &mut UnixStream) -> serde_json::Value {
    let mut first = [0; 2];
    stream.read_exact(&mut first).unwrap();
    assert_eq!(first[0], 0x81);
    assert_ne!(first[1] & 0x80, 0, "client frames must be masked");
    let mut length = usize::from(first[1] & 0x7f);
    if length == 126 {
        let mut extended = [0; 2];
        stream.read_exact(&mut extended).unwrap();
        length = usize::from(u16::from_be_bytes(extended));
    }
    let mut mask = [0; 4];
    stream.read_exact(&mut mask).unwrap();
    let mut body = vec![0; length];
    stream.read_exact(&mut body).unwrap();
    for (index, byte) in body.iter_mut().enumerate() {
        *byte ^= mask[index % mask.len()];
    }
    serde_json::from_slice(&body).unwrap()
}

fn reply(stream: &mut UnixStream, id: u64, result: serde_json::Value) {
    let body =
        serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":id,"result":result})).unwrap();
    assert!(body.len() < 126);
    stream.write_all(&[0x81, body.len() as u8]).unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
}

fn fake_codex_server(listener: UnixListener) -> std::thread::JoinHandle<serde_json::Value> {
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept fake Codex socket: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let request = headers(&mut stream);
        let key = request
            .lines()
            .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap();
        let accept = STANDARD.encode(Sha1::digest(
            format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
        ));
        write!(stream, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        stream.flush().unwrap();
        let initialize = client_frame(&mut stream);
        assert_eq!(initialize["method"], "initialize");
        reply(&mut stream, 1, serde_json::json!({}));
        let initialized = client_frame(&mut stream);
        assert_eq!(initialized["method"], "initialized");
        let resume = client_frame(&mut stream);
        assert_eq!(resume["method"], "thread/resume");
        reply(&mut stream, 2, serde_json::json!({}));
        let turn = client_frame(&mut stream);
        assert_eq!(turn["method"], "turn/start");
        reply(
            &mut stream,
            3,
            serde_json::json!({"turn":{"id":"fixture-turn","status":"inProgress"}}),
        );
        turn
    })
}

#[test]
fn ordinary_claude_turn_reaches_the_typed_relay_header_without_context_or_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("claude.jsonl");
    transcript(&path, 1);

    let output = relay(&path).output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("Relay.{"), "{stdout}");
    assert!(stdout.ends_with(BODY));
    assert!(stdout.contains("unreviewed: Context receipt unavailable"));
}

#[test]
fn codex_rollout_uses_native_session_meta_and_payload_identifier() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("codex.jsonl");
    fs::write(&path, include_str!("fixtures/codex-session-meta-response.jsonl")).unwrap();
    let output = relay(&path).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(rendered.starts_with("Relay.{"));
    assert!(rendered.ends_with(BODY));
}

#[test]
fn prompt_relay_provenance_record_is_refused_without_socket_write_and_neighbor_is_selectable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("claude.jsonl");
    let socket_path = directory.path().join("must-not-connect.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let relayed = serde_json::json!({
        "type": "user",
        "uuid": "611f76ba-f42f-45ba-aefa-4f27369071fc",
        "sessionId": "cf7879-session",
        "timestamp": "2026-09-16T08:14:34.780Z",
        "message": { "content": [
            { "type": "text", "text": "{\"provenance\":{\"source_path\":\"/sanitized/source.jsonl\",\"source_format\":\"codex\",\"source_message_id\":\"msg_01a0a722-4c6b-7da2-ac40-c694a71d565a\",\"source_timestamp\":null,\"sha256_utf8\":\"29ac8517808b35a12a66c760ef7d5eeeaf9aaad6e93b9ae167ea48af9f35b5b6\"}}" },
            { "type": "text", "text": "relayed source words must never become a new relay" }
        ] }
    });
    let ordinary = serde_json::json!({
        "type": "user",
        "uuid": "claude-turn-2",
        "sessionId": "cf7879-session",
        "timestamp": "2026-09-16T08:15:34.780Z",
        "message": { "content": BODY }
    });
    fs::write(&path, format!("{relayed}\n{ordinary}\n")).unwrap();

    let refused = relay_selection(
        &path,
        "relayed source words must never become",
        "must never become a new relay",
    )
    .env("RELAY_CODEX_THREAD_ID", "cf7879-session")
    .env("RELAY_CODEX_SOCKET", &socket_path)
    .output()
    .unwrap();
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr)
            .contains("no user record has the supplied first and last six words"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    assert!(
        matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );

    let selected = relay(&path).output().unwrap();
    assert!(selected.status.success());
    assert!(String::from_utf8(selected.stdout).unwrap().ends_with(BODY));
}

#[test]
fn ambiguous_or_mismatched_context_source_is_refused_before_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("claude.jsonl");
    transcript(&path, 2);
    let ambiguous = relay(&path).output().unwrap();
    assert!(!ambiguous.status.success());
    assert!(
        String::from_utf8(ambiguous.stderr)
            .unwrap()
            .contains("selectors are ambiguous")
    );

    transcript(&path, 1);
    let receipt = directory.path().join("wrong-context.json");
    fs::write(&receipt, "{\"kind\":\"clusterrelay-derived-context\",\"machine_authored\":true,\"verbatim_source_text\":\"wrong\",\"source\":{},\"derived\":{}}").unwrap();
    let mismatch = relay(&path)
        .env("RELAY_CONTEXT_RECEIPT", receipt)
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    assert!(
        String::from_utf8(mismatch.stderr)
            .unwrap()
            .contains("Context receipt")
    );
}

#[test]
fn fake_codex_socket_receives_the_exact_header_and_ordinary_claude_body() {
    let directory = tempfile::tempdir().unwrap();
    let transcript_path = directory.path().join("claude.jsonl");
    let socket_path = directory.path().join("codex.sock");
    transcript(&transcript_path, 1);
    let listener = UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept fake Codex socket: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let request = headers(&mut stream);
        let key = request
            .lines()
            .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
            .unwrap();
        let accept = STANDARD.encode(Sha1::digest(
            format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes(),
        ));
        write!(stream, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").unwrap();
        stream.flush().unwrap();
        let initialize = client_frame(&mut stream);
        assert_eq!(initialize["method"], "initialize");
        reply(&mut stream, 1, serde_json::json!({}));
        let initialized = client_frame(&mut stream);
        assert_eq!(initialized["method"], "initialized");
        let resume = client_frame(&mut stream);
        assert_eq!(resume["method"], "thread/resume");
        reply(&mut stream, 2, serde_json::json!({}));
        let turn = client_frame(&mut stream);
        assert_eq!(turn["method"], "turn/start");
        reply(
            &mut stream,
            3,
            serde_json::json!({"turn":{"id":"fixture-turn","status":"inProgress"}}),
        );
        turn
    });
    let output = relay(&transcript_path)
        .env("RELAY_CODEX_THREAD_ID", "cf7879-session")
        .env("RELAY_CODEX_SOCKET", &socket_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let turn = server.join().unwrap();
    assert_eq!(turn["params"]["threadId"], "cf7879-session");
    let header = turn["params"]["input"][0]["text"].as_str().unwrap();
    assert!(
        header.contains("unreviewed: Context receipt unavailable"),
        "{header}"
    );
    assert_eq!(turn["params"]["input"][1]["text"], BODY);
    let receipt = String::from_utf8(output.stdout).unwrap();
    assert!(receipt.contains("codex-turn-start-acknowledged"));
    assert!(receipt.contains("inProgress"));
}

#[test]
fn flow_route_fixture_fans_out_to_each_codex_target_excludes_source_and_keeps_unavailable_outcome()
{
    let directory = tempfile::tempdir().unwrap();
    let transcript_path = directory.path().join("claude.jsonl");
    let first_socket = directory.path().join("codex-first.sock");
    let second_socket = directory.path().join("codex-second.sock");
    transcript(&transcript_path, 1);
    let first = fake_codex_server(UnixListener::bind(&first_socket).unwrap());
    let second = fake_codex_server(UnixListener::bind(&second_socket).unwrap());
    let routes = directory.path().join("flow-routes.json");
    fs::write(
        &routes,
        serde_json::json!({"routes":[
            {"flow_identifier":"source","session_identifier":"cf7879-session","harness":"codex","readiness":"idle","endpoint":directory.path().join("must-not-connect.sock")},
            {"flow_identifier":"codex-first","session_identifier":"codex-first-session","harness":"codex","readiness":"idle","endpoint":first_socket},
            {"flow_identifier":"codex-second","session_identifier":"codex-second-session","harness":"codex","readiness":"idle","endpoint":second_socket},
            {"flow_identifier":"claude-unavailable","session_identifier":"claude-session","harness":"claude","readiness":"idle","endpoint":directory.path().join("missing-prompt-relay")}
        ]}).to_string(),
    ).unwrap();
    let output = relay(&transcript_path)
        .env("FLOW_ID", "source")
        .env("RELAY_CLUSTER_MEMBERS", "source@cf7879-session,codex-first@codex-first-session,codex-second@codex-second-session,claude-unavailable@claude-session")
        .env("RELAY_FLOW_ROUTES", routes)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["kind"], "cluster-relay-fanout");
    let outcomes = receipt["outcomes"].as_array().unwrap();
    assert_eq!(outcomes.len(), 3, "source route must be excluded");
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome["flow_identifier"] != "source")
    );
    assert_eq!(outcomes[0]["outcome"]["kind"], "accepted");
    assert_eq!(outcomes[1]["outcome"]["kind"], "accepted");
    assert_eq!(outcomes[2]["outcome"]["kind"], "unavailable");
    assert!(
        outcomes[2]["outcome"]["reason"]
            .as_str()
            .unwrap()
            .contains("start configured Claude prompt-relay")
    );
    let turns = [first.join().unwrap(), second.join().unwrap()];
    for (turn, thread) in turns
        .iter()
        .zip(["codex-first-session", "codex-second-session"])
    {
        assert_eq!(turn["params"]["threadId"], thread);
        assert_eq!(turn["params"]["input"][1]["text"], BODY);
        assert!(
            turn["params"]["input"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("Relay.{"),
            "{}",
            turn["params"]["input"][0]["text"]
        );
    }
}

#[test]
fn unknown_route_readiness_refuses_without_connecting_an_endpoint() {
    let directory = tempfile::tempdir().unwrap();
    let transcript_path = directory.path().join("claude.jsonl");
    transcript(&transcript_path, 1);
    let routes = directory.path().join("flow-routes.json");
    fs::write(
        &routes,
        serde_json::json!({"routes":[
            {"flow_identifier":"source","session_identifier":"cf7879-session","harness":"codex","readiness":"unknown","endpoint":directory.path().join("source.sock")},
            {"flow_identifier":"peer","session_identifier":"peer-session","harness":"codex","readiness":"unknown","endpoint":directory.path().join("must-not-connect.sock")}
        ]})
        .to_string(),
    )
    .unwrap();
    let output = relay(&transcript_path)
        .env("RELAY_CLUSTER_MEMBERS", "cf7879@cf7879-session,peer@peer-session")
        .env("RELAY_FLOW_ROUTES", routes)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(receipt["outcomes"][0]["outcome"]["kind"], "unavailable");
    assert!(receipt["outcomes"][0]["outcome"]["reason"]
        .as_str()
        .unwrap()
        .contains("no fresh readiness witness"));
}

#[test]
fn configured_claude_peer_file_is_bounded_and_requires_matching_pty_receipt() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let transcript_path = directory.path().join("claude.jsonl");
    transcript(&transcript_path, 1);
    let relay_cli = directory.path().join("fake-prompt-relay");
    fs::write(
        &relay_cli,
        r#"#!/bin/sh
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--source" ] && [ -r "$2" ]; then source_ok=1; fi
  if [ "$1" = "--session-short" ]; then session="$2"; fi
  shift
done
[ "$source_ok" = 1 ] || exit 9
echo '{"kind":"claude-bytes-written-to-pty","session_id":"claude-live"}' 
"#,
    )
    .unwrap();
    fs::set_permissions(&relay_cli, fs::Permissions::from_mode(0o700)).unwrap();
    let routes = directory.path().join("flow-routes.json");
    fs::write(&routes, serde_json::json!({"routes":[
        {"flow_identifier":"claude","session_identifier":"claude-live","harness":"claude","readiness":"idle","endpoint":relay_cli}
    ]}).to_string()).unwrap();
    let output = relay(&transcript_path)
        .env("FLOW_ID", "source")
        .env(
            "RELAY_CLUSTER_MEMBERS",
            "source@cf7879-session,claude@claude-live",
        )
        .env("RELAY_FLOW_ROUTES", routes)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        receipt["outcomes"][0]["outcome"]["receipt"]["kind"], "claude-pty-write-acknowledged",
        "{receipt:?}"
    );
    // The fake CLI checked that the private peer file was readable; production
    // writes the typed Relay header and original body into that file.
}

#[test]
fn configured_busy_nexus_route_parks_the_typed_cluster_relay_before_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let transcript_path = directory.path().join("claude.jsonl");
    transcript(&transcript_path, 1);
    let home = directory.path().join("home");
    let target_flow = "busy-nexus";
    let marker_dir = home.join("primary/flows");
    fs::create_dir_all(&marker_dir).unwrap();
    fs::write(
        marker_dir.join(format!(".{target_flow}.flow-id")),
        "fixture target\n",
    )
    .unwrap();
    let contract = MessageDaemonConfiguration {
        message_socket_path: directory
            .path()
            .join("message.sock")
            .to_string_lossy()
            .into_owned(),
        message_socket_mode: 0o600,
        supervision_socket_path: directory
            .path()
            .join("meta.sock")
            .to_string_lossy()
            .into_owned(),
        supervision_socket_mode: 0o600,
        router_socket_path: directory
            .path()
            .join("router.sock")
            .to_string_lossy()
            .into_owned(),
        component_ingresses: Vec::new(),
        owner_identity: OwnerIdentity::UnixUser(i64::from(rustix::process::getuid().as_raw())),
    };
    let configuration =
        Configuration::new(contract, directory.path().join("messenger.sema"), "fixture").unwrap();
    let configuration_path = directory.path().join("message.configuration");
    configuration
        .write_binary_file(&configuration_path)
        .unwrap();
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_message-daemon"))
        .env_clear()
        .env("HOME", &home)
        .arg(&configuration_path)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !configuration.socket_path().exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        configuration.socket_path().exists(),
        "temporary Message Nexus did not bind"
    );
    let routes = directory.path().join("flow-routes.json");
    fs::write(&routes, serde_json::json!({"routes":[
        {"flow_identifier":target_flow,"session_identifier":"busy-session","harness":"nexus","readiness":"busy","endpoint":configuration.socket_path()}
    ]}).to_string()).unwrap();
    let output = relay(&transcript_path)
        .env("FLOW_ID", "source")
        .env(
            "RELAY_CLUSTER_MEMBERS",
            "source@cf7879-session,busy-nexus@busy-session",
        )
        .env("RELAY_FLOW_ROUTES", routes)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        receipt["outcomes"][0]["outcome"]["receipt"]["kind"], "nexus-flow-delivery-parked",
        "{receipt:?}"
    );
    assert!(
        receipt["outcomes"][0]["outcome"]["receipt"]["source_event_identifier"]
            .as_str()
            .unwrap()
            .starts_with("source:cf7879-session:claude-turn-1")
    );
    let landed = MessageSocket::from_path(configuration.socket_path())
        .client()
        .submit(Query::FlowAnnounceIdle(FlowIdleAnnouncement {
            target_flow_name: target_flow.to_owned(),
        }))
        .unwrap();
    match landed {
        Response::FlowIdleAcknowledged(acknowledgment) => {
            assert_eq!(acknowledgment.landed_receipts.len(), 1);
            // The stored raw packet is header + blank line + original body, not body alone.
            assert!(
                acknowledgment.landed_receipts[0].byte_count > i64::try_from(BODY.len()).unwrap()
            );
        }
        other => panic!("unexpected Flow idle reply: {other:?}"),
    }
    let _ = daemon.kill();
    let _ = daemon.wait();
}
