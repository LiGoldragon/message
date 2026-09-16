//! Isolated process fixture for source selection before any delivery adapter.

use std::{fs, path::Path, process::Command};

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
        .env("RELAY_SESSION_ID", "root")
        .env("RELAY_CLUSTER_MEMBERS", "cf7879@root")
        .env("RELAY_TRANSCRIPT", path)
        .env("HOME", path.parent().unwrap())
        .arg("one two three four five six")
        .arg("seven eight nine ten eleven twelve");
    command
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
