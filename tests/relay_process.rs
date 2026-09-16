//! Isolated process fixture for source selection before any delivery adapter.

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
    fs::write(path, std::iter::repeat_n(format!("{record}\n"), copies).collect::<String>()).unwrap();
}

fn relay(path: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_relay"));
    command
        .env_clear()
        .env("FLOW_ID", "cf7879")
        .env("RELAY_SESSION_ID", "cf7879-session")
        .env("RELAY_CLUSTER_MEMBERS", "cf7879@cf7879-session")
        .env("RELAY_TRANSCRIPT", path)
        .env("HOME", path.parent().unwrap())
        .arg("one two three four five six")
        .arg("seven eight nine ten eleven twelve");
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
    let body = serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":id,"result":result})).unwrap();
    assert!(body.len() < 126);
    stream.write_all(&[0x81, body.len() as u8]).unwrap();
    stream.write_all(&body).unwrap();
    stream.flush().unwrap();
}

#[test]
fn ordinary_claude_turn_reaches_the_typed_relay_header_without_context_or_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("claude.jsonl");
    transcript(&path, 1);

    let output = relay(&path).output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ClusterRelay"), "{stdout}");
    assert!(stdout.ends_with(BODY));
    assert!(stdout.contains("unreviewed: Context receipt unavailable"));
}

#[test]
fn ambiguous_or_mismatched_context_source_is_refused_before_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("claude.jsonl");
    transcript(&path, 2);
    let ambiguous = relay(&path).output().unwrap();
    assert!(!ambiguous.status.success());
    assert!(String::from_utf8(ambiguous.stderr).unwrap().contains("selectors are ambiguous"));

    transcript(&path, 1);
    let receipt = directory.path().join("wrong-context.json");
    fs::write(&receipt, "{\"kind\":\"clusterrelay-derived-context\",\"machine_authored\":true,\"verbatim_source_text\":\"wrong\",\"source\":{},\"derived\":{}}").unwrap();
    let mismatch = relay(&path)
        .env("RELAY_CONTEXT_RECEIPT", receipt)
        .output()
        .unwrap();
    assert!(!mismatch.status.success());
    assert!(String::from_utf8(mismatch.stderr).unwrap().contains("Context receipt"));
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
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("accept fake Codex socket: {error}"),
            }
        };
        stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let request = headers(&mut stream);
        let key = request.lines().find_map(|line| line.strip_prefix("Sec-WebSocket-Key: ")).unwrap();
        let accept = STANDARD.encode(Sha1::digest(format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes()));
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
        reply(&mut stream, 3, serde_json::json!({"turn":{"id":"fixture-turn","status":"inProgress"}}));
        turn
    });
    let output = relay(&transcript_path)
        .env("RELAY_CODEX_THREAD_ID", "cf7879-session")
        .env("RELAY_CODEX_SOCKET", &socket_path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let turn = server.join().unwrap();
    assert_eq!(turn["params"]["threadId"], "cf7879-session");
    let header = turn["params"]["input"][0]["text"].as_str().unwrap();
    assert!(header.contains("unreviewed: Context receipt unavailable"), "{header}");
    assert_eq!(turn["params"]["input"][1]["text"], BODY);
    let receipt = String::from_utf8(output.stdout).unwrap();
    assert!(receipt.contains("codex-turn-start-acknowledged"));
    assert!(receipt.contains("inProgress"));
}
