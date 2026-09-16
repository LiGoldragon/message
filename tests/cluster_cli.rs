//! The adapter boundary only admits a producer-rendered Datom whose hash
//! matches the exact prompt body.  It must not turn transport metadata into a
//! recipient-visible JSON prelude.

use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
};

const BODY: &str = "one two three four five six\n\n“a quoted context paragraph”\n\nseven eight nine ten eleven twelve";

fn relay(path: &std::path::Path) -> Command {
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

#[test]
fn verifies_a_producer_cluster_relay_and_preserves_verbatim_words() {
    let directory = tempfile::tempdir().unwrap();
    let transcript = directory.path().join("source.jsonl");
    fs::write(
        &transcript,
        serde_json::json!({
            "type": "user", "uuid": "source-1", "sessionId": "cf7879-session",
            "timestamp": "2026-09-16T00:00:00Z", "message": { "content": BODY },
        })
        .to_string(),
    )
    .unwrap();
    let rendered = String::from_utf8(relay(&transcript).output().unwrap().stdout).unwrap();
    // A Context string can itself contain blank lines, so the frame boundary
    // is supplied by the trusted source body rather than guessed by splitting
    // inside Datom text.
    let header = rendered
        .strip_suffix(BODY)
        .unwrap()
        .strip_suffix("\n\n")
        .unwrap();
    let body = BODY;
    let header_path = directory.path().join("relay.datom");
    fs::write(&header_path, header).unwrap();

    let mut verify = Command::new(env!("CARGO_BIN_EXE_message-cluster"))
        .args(["verify", "--datom-file", header_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    verify
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_bytes())
        .unwrap();
    let output = verify.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), rendered);

    let ordinary = Command::new(env!("CARGO_BIN_EXE_message"))
        .args(["cluster", header])
        .output()
        .unwrap();
    assert!(ordinary.status.success());
    assert_eq!(
        String::from_utf8(ordinary.stdout).unwrap(),
        format!("{header}\n")
    );

    let refusal = Command::new(env!("CARGO_BIN_EXE_message-cluster"))
        .args(["verify", "--datom-file", header_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .output()
        .unwrap();
    assert!(!refusal.status.success());
    assert!(String::from_utf8_lossy(&refusal.stderr).contains("prompt_sha256"));
}

#[test]
fn produces_and_verifies_a_typed_peer_with_multiline_raw_body() {
    let fixture_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let body = include_str!("fixtures/cf7879-peer-cluster-body.md");
    let header = include_str!("fixtures/cf7879-peer-cluster-header.datom");
    let expected = format!("{header}\n\n{body}");

    let produced = Command::new(env!("CARGO_BIN_EXE_message-cluster"))
        .current_dir(fixture_root)
        .args([
            "peer",
            "--source-file",
            "tests/fixtures/cf7879-peer-cluster-body.md",
            "--sender-flow",
            "efa157",
            "--sender-session",
            "efa15708-dc5d-42ce-af62-8ffb84c9815e",
            "--source-event",
            "msg_01a0aa9c-b778-76d1-8b1f-2bb0d6430fb2",
        ])
        .output()
        .unwrap();
    assert!(produced.status.success());
    assert_eq!(String::from_utf8(produced.stdout).unwrap(), expected);
    let directory = tempfile::tempdir().unwrap();
    let header_path = directory.path().join("peer.datom");
    fs::write(&header_path, header).unwrap();

    let mut verify = Command::new(env!("CARGO_BIN_EXE_message-cluster"))
        .args(["verify", "--datom-file", header_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    verify
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_bytes())
        .unwrap();
    let verified = verify.wait_with_output().unwrap();
    assert!(verified.status.success());
    assert_eq!(String::from_utf8(verified.stdout).unwrap(), expected);

    let ordinary = Command::new(env!("CARGO_BIN_EXE_message"))
        .args(["cluster", header])
        .output()
        .unwrap();
    assert!(ordinary.status.success());
    assert_eq!(
        String::from_utf8(ordinary.stdout).unwrap(),
        format!("{header}\n")
    );

    let mut rejected = Command::new(env!("CARGO_BIN_EXE_message-cluster"))
        .args(["verify", "--datom-file", header_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    rejected
        .stdin
        .take()
        .unwrap()
        .write_all(b"changed peer body")
        .unwrap();
    let rejected = rejected.wait_with_output().unwrap();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("peer_body_sha256"));
}
