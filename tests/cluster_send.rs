use std::{fs, os::unix::fs::PermissionsExt, process::Command};

const BODY: &str = "one two three four five six seven eight nine ten eleven twelve";

fn relay(path: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_relay"));
    command.env_clear().env("FLOW_ID", "cf7879").env("RELAY_SESSION_ID", "cf7879-session")
        .env("RELAY_CLUSTER_MEMBERS", "cf7879@cf7879-session").env("RELAY_TRANSCRIPT", path)
        .env("HOME", path.parent().unwrap()).arg("one two three four five six").arg("seven eight nine ten eleven twelve");
    command
}

#[test]
fn cluster_send_uses_one_flow_configured_prompt_relay_and_reports_its_acknowledgment() {
    let directory = tempfile::tempdir().unwrap();
    let transcript = directory.path().join("source.jsonl");
    fs::write(&transcript, serde_json::json!({ "type":"user", "uuid":"source", "sessionId":"cf7879-session", "timestamp":"2026-09-16T00:00:00Z", "message":{"content":BODY} }).to_string()).unwrap();
    let rendered = String::from_utf8(relay(&transcript).output().unwrap().stdout).unwrap();
    let header = rendered.strip_suffix(BODY).unwrap().strip_suffix("\n\n").unwrap();
    let body = directory.path().join("body.txt"); fs::write(&body, BODY).unwrap();
    let capture = directory.path().join("captured-header.datom");
    let adapter = directory.path().join("prompt-relay");
    fs::write(&adapter, format!("#!/bin/sh\ncat \"$7\" > {}\nprintf '%s\\n' '{{\"kind\":\"claude-bytes-written-to-pty\",\"session_id\":\"target-session\"}}'\n", capture.display())).unwrap();
    fs::set_permissions(&adapter, fs::Permissions::from_mode(0o700)).unwrap();
    let routes = directory.path().join("routes.json");
    fs::write(&routes, serde_json::json!({"routes":[{"flow_identifier":"target","session_identifier":"target-session","harness":"claude","readiness":"idle","endpoint":adapter}]}).to_string()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_message")).args(["cluster", header, "--body-file", body.to_str().unwrap(), "--route-config", routes.to_str().unwrap(), "--to", "target@target-session"]).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()["kind"], "claude-bytes-written-to-pty");
    assert_eq!(fs::read_to_string(capture).unwrap(), header);
}
