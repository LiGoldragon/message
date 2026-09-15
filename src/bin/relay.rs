//! Source-verifying cluster relay preparation.
//!
//! `relay` has only two public arguments: the first and final six prompt
//! words.  Identity, membership, and the selected cluster come from the
//! calling flow's declared environment; the model never constructs a prompt
//! body or a Message registry.  The command prints a producer-owned
//! `ClusterMessage` Datom header followed by the byte-exact source body.  A
//! transport adapter consumes those two values after it has selected an
//! actually supported delivery leg.

use std::{
    env, fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use datom_codec::Datomizable;
use protos::{Protosizable, Textualizable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use signal_message::{ClusterMember, ClusterMessage, ClusterRelay, ClusterTarget, Context};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn main() {
    match run(env::args().skip(1).collect()) {
        Ok(rendered) => print!("{rendered}"),
        Err(error) => {
            eprintln!("relay: {error}");
            std::process::exit(1);
        }
    }
}

fn run(arguments: Vec<String>) -> Result<String, String> {
    let [head, tail] = arguments.as_slice() else {
        return Err("expected exactly two arguments: first-six-words last-six-words".into());
    };
    let executor_flow_identifier = required("FLOW_ID")?;
    let executor_session_identifier = required("RELAY_SESSION_ID")?;
    let members = members(&required("RELAY_CLUSTER_MEMBERS")?)?;
    if !members.iter().any(|member| {
        member.flow_identifier == executor_flow_identifier
            && member.session_identifier == executor_session_identifier
    }) {
        return Err("FLOW_ID and RELAY_SESSION_ID are not one declared cluster member".into());
    }
    let codex_target = env::var("RELAY_CODEX_THREAD_ID").ok();
    if let Some(target) = &codex_target
        && !members
            .iter()
            .any(|member| member.session_identifier == *target)
    {
        return Err("RELAY_CODEX_THREAD_ID is not a declared cluster member".into());
    }
    let declared_target = env::var("RELAY_CLUSTER_TARGET").ok();
    let cluster_target = cluster_target(declared_target.as_deref())?;
    let source = locate(head, tail)?;
    let context = context_for(&source, &executor_flow_identifier)?;
    let header = ClusterMessage::Relay(ClusterRelay {
        flow_identifier: source.flow_identifier.clone(),
        session_identifier: source.session_identifier.clone(),
        transcript_path: source.path.display().to_string(),
        prompt_first_six_words: head.clone(),
        prompt_last_six_words: tail.clone(),
        prompt_sha256: source.sha256,
        context,
        timestamp_nanos: source.timestamp_nanos,
        cluster_target,
        cluster_members: members,
    });
    let prepared = PreparedRelay {
        header: header.datomize(vec![]).protosize().textualize(),
        body: source.body,
        executor_flow_identifier,
        executor_session_identifier,
    };
    if let Some(thread_id) = codex_target {
        let receipt = CodexAppServer::from_environment()?.deliver(&thread_id, &prepared)?;
        return Ok(serde_json::to_string(&receipt).map_err(|error| error.to_string())? + "\n");
    }
    Ok(prepared.render())
}

struct PreparedRelay {
    header: String,
    body: String,
    executor_flow_identifier: String,
    executor_session_identifier: String,
}

impl PreparedRelay {
    fn render(&self) -> String {
        format!("{}\n\n{}", self.header, self.body)
    }
}

/// Native Codex app-server adapter. A returned `inProgress` turn is only an
/// app-server acknowledgement; callers must inspect the recipient transcript
/// for a UserMessage before they call it delivery.
struct CodexAppServer {
    socket_path: PathBuf,
}

impl CodexAppServer {
    fn from_environment() -> Result<Self, String> {
        let socket_path = match env::var_os("RELAY_CODEX_SOCKET") {
            Some(path) => PathBuf::from(path),
            None => env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".codex/app-server-control/app-server-control.sock"))
                .ok_or_else(|| "cannot determine HOME for Codex app-server socket".to_owned())?,
        };
        Ok(Self { socket_path })
    }

    fn deliver(&self, thread_id: &str, relay: &PreparedRelay) -> Result<DeliveryReceipt, String> {
        let mut socket = UnixStream::connect(&self.socket_path).map_err(|error| {
            format!(
                "connect Codex app server {}: {error}",
                self.socket_path.display()
            )
        })?;
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .map_err(|error| error.to_string())?;
        let websocket_key = websocket_key()?;
        socket
            .write_all(format!("GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {websocket_key}\r\nSec-WebSocket-Version: 13\r\n\r\n").as_bytes())
            .map_err(|error| error.to_string())?;
        let response = read_http_headers(&mut socket)?;
        validate_websocket_upgrade(&response, &websocket_key)?;
        let mut rpc = WebSocketRpc { socket, next_id: 0 };
        rpc.call(
            "initialize",
            serde_json::json!({"clientInfo":{"name":"relay","version":"1"}}),
        )?;
        rpc.notify("initialized", serde_json::json!({}))?;
        rpc.call(
            "thread/resume",
            serde_json::json!({"threadId":thread_id,"excludeTurns":true}),
        )?;
        let result = rpc.call(
            "turn/start",
            serde_json::json!({
                "threadId":thread_id,
                "input":[{"type":"text","text":relay.header},{"type":"text","text":relay.body}],
            }),
        )?;
        let turn = result
            .get("turn")
            .or_else(|| result.get("result").and_then(|value| value.get("turn")));
        Ok(DeliveryReceipt {
            kind: "codex-turn-start-acknowledged",
            thread_id: thread_id.to_owned(),
            turn_id: turn
                .and_then(|value| value.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            status: turn
                .and_then(|value| value.get("status"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            executor_flow_identifier: relay.executor_flow_identifier.clone(),
            executor_session_identifier: relay.executor_session_identifier.clone(),
        })
    }
}

#[derive(serde::Serialize)]
struct DeliveryReceipt {
    kind: &'static str,
    thread_id: String,
    turn_id: Option<String>,
    status: Option<String>,
    executor_flow_identifier: String,
    executor_session_identifier: String,
}

struct WebSocketRpc {
    socket: UnixStream,
    next_id: u64,
}

impl WebSocketRpc {
    fn notify(&mut self, method: &str, params: serde_json::Value) -> Result<(), String> {
        self.write(serde_json::json!({"jsonrpc":"2.0","method":method,"params":params}))
    }

    fn call(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        self.next_id += 1;
        let id = self.next_id;
        self.write(serde_json::json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))?;
        loop {
            let reply = self.read()?;
            if reply.get("id").and_then(serde_json::Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = reply.get("error") {
                return Err(format!("Codex RPC {method} failed: {error}"));
            }
            return reply
                .get("result")
                .cloned()
                .ok_or_else(|| format!("Codex RPC {method} had no result"));
        }
    }

    fn write(&mut self, message: serde_json::Value) -> Result<(), String> {
        let payload = serde_json::to_vec(&message).map_err(|error| error.to_string())?;
        let mask = [0x31, 0x41, 0x59, 0x26];
        let mut header = vec![0x81];
        match payload.len() {
            length if length < 126 => header.push(0x80 | length as u8),
            length if length < 65_536 => {
                header.push(0xfe);
                header.extend_from_slice(&(length as u16).to_be_bytes());
            }
            length => {
                header.push(0xff);
                header.extend_from_slice(&(length as u64).to_be_bytes());
            }
        }
        header.extend_from_slice(&mask);
        self.socket
            .write_all(&header)
            .map_err(|error| error.to_string())?;
        for (index, byte) in payload.into_iter().enumerate() {
            self.socket
                .write_all(&[byte ^ mask[index % mask.len()]])
                .map_err(|error| error.to_string())?;
        }
        self.socket.flush().map_err(|error| error.to_string())
    }

    fn read(&mut self) -> Result<serde_json::Value, String> {
        let mut initial = [0; 2];
        self.socket
            .read_exact(&mut initial)
            .map_err(|error| error.to_string())?;
        let opcode = initial[0] & 0x0f;
        if initial[0] & 0x80 == 0 {
            return Err("fragmented Codex websocket frame".into());
        }
        let mut length = usize::from(initial[1] & 0x7f);
        if length == 126 {
            let mut bytes = [0; 2];
            self.socket
                .read_exact(&mut bytes)
                .map_err(|error| error.to_string())?;
            length = usize::from(u16::from_be_bytes(bytes));
        }
        if length == 127 {
            let mut bytes = [0; 8];
            self.socket
                .read_exact(&mut bytes)
                .map_err(|error| error.to_string())?;
            length = usize::try_from(u64::from_be_bytes(bytes))
                .map_err(|_| "Codex websocket frame too large".to_owned())?;
        }
        let mut body = vec![0; length];
        self.socket
            .read_exact(&mut body)
            .map_err(|error| error.to_string())?;
        if opcode == 0x9 {
            self.socket
                .write_all(&[0x8a, length as u8])
                .map_err(|error| error.to_string())?;
            self.socket
                .write_all(&body)
                .map_err(|error| error.to_string())?;
            return self.read();
        }
        if opcode != 0x1 {
            return Err("unsupported Codex websocket frame".into());
        }
        serde_json::from_slice(&body)
            .map_err(|error| format!("invalid Codex websocket JSON: {error}"))
    }
}

fn read_http_headers(socket: &mut UnixStream) -> Result<String, String> {
    let mut response = Vec::new();
    loop {
        let mut byte = [0];
        socket
            .read_exact(&mut byte)
            .map_err(|error| error.to_string())?;
        response.push(byte[0]);
        if response.ends_with(b"\r\n\r\n") {
            return String::from_utf8(response).map_err(|error| error.to_string());
        }
        if response.len() > 16_384 {
            return Err("Codex app-server headers too large".into());
        }
    }
}

fn websocket_key() -> Result<String, String> {
    let mut nonce = [0_u8; 16];
    fs::File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut nonce))
        .map_err(|error| format!("read websocket nonce: {error}"))?;
    Ok(STANDARD.encode(nonce))
}

/// Bind the upgrade response to this connection's random RFC 6455 nonce.
fn validate_websocket_upgrade(response: &str, websocket_key: &str) -> Result<(), String> {
    if !response.starts_with("HTTP/1.1 101") {
        return Err("Codex app-server websocket upgrade refused".into());
    }
    let mut digest = Sha1::new();
    digest.update(websocket_key.as_bytes());
    digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let expected = STANDARD.encode(digest.finalize());
    let accepted = response.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("sec-websocket-accept") && value.trim() == expected
        })
    });
    accepted
        .then_some(())
        .ok_or_else(|| "Codex app-server websocket upgrade has invalid Sec-WebSocket-Accept".into())
}

fn required(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing {name}; flow launch must declare it"))
}

fn cluster_target(value: Option<&str>) -> Result<ClusterTarget, String> {
    match value.unwrap_or("Primary") {
        "Primary" => Ok(ClusterTarget::Primary),
        "Secondary" => Ok(ClusterTarget::Secondary),
        "Core" => Ok(ClusterTarget::Core),
        other => Err(format!("unknown RELAY_CLUSTER_TARGET {other:?}")),
    }
}

/// Setup declaration, outside the public call: comma-separated `flow@session`
/// members.  A future Flow query replaces this one adapter without changing
/// the producer-owned `ClusterMessage` shape.
fn members(declaration: &str) -> Result<Vec<ClusterMember>, String> {
    let members = declaration
        .split(',')
        .filter(|member| !member.is_empty())
        .map(|member| match member.split_once('@') {
            Some((flow_identifier, session_identifier))
                if !flow_identifier.is_empty() && !session_identifier.is_empty() =>
            {
                Ok(ClusterMember {
                    flow_identifier: flow_identifier.to_owned(),
                    session_identifier: session_identifier.to_owned(),
                })
            }
            _ => Err(format!(
                "invalid cluster member {member:?}; expected flow@session"
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if members.is_empty() {
        return Err("RELAY_CLUSTER_MEMBERS must declare at least one member".into());
    }
    let mut seen = std::collections::BTreeSet::new();
    if members.iter().any(|member| {
        !seen.insert((
            member.flow_identifier.as_str(),
            member.session_identifier.as_str(),
        ))
    }) {
        return Err("RELAY_CLUSTER_MEMBERS contains a duplicate flow@session member".into());
    }
    Ok(members)
}

#[derive(Debug)]
struct Source {
    path: PathBuf,
    body: String,
    sha256: String,
    timestamp_nanos: i64,
    flow_identifier: String,
    session_identifier: String,
    source_turn_identifier: String,
    source_event_identifier: String,
    source_line: usize,
}

/// The Context runner is the single author of semantic context.  Relay only
/// validates the runner receipt against the byte-exact source it selected and
/// places that producer-owned value beside the unchanged body.
fn context_for(source: &Source, executor_flow_identifier: &str) -> Result<Context, String> {
    let receipt_path = env::var_os("RELAY_CONTEXT_RECEIPT")
        .map(PathBuf::from)
        .ok_or_else(|| {
            "missing RELAY_CONTEXT_RECEIPT; run clusterrelay-context first".to_owned()
        })?;
    context_from_receipt(source, executor_flow_identifier, &receipt_path)
}

fn context_from_receipt(
    source: &Source,
    executor_flow_identifier: &str,
    receipt_path: &Path,
) -> Result<Context, String> {
    let input = fs::read_to_string(&receipt_path)
        .map_err(|error| format!("read Context receipt {}: {error}", receipt_path.display()))?;
    let receipt: ContextReceipt = serde_json::from_str(&input)
        .map_err(|error| format!("parse Context receipt {}: {error}", receipt_path.display()))?;
    if receipt.kind != "clusterrelay-derived-context" || !receipt.machine_authored {
        return Err("Context receipt is not a machine-authored clusterrelay receipt".to_owned());
    }
    if receipt.source.source_path != source.path.display().to_string()
        || receipt.source.flow_identifier != executor_flow_identifier
        || receipt.source.source_session_identifier != source.session_identifier
        || receipt.source.source_turn_identifier != source.source_turn_identifier
        || receipt.source.source_event_identifier != source.source_event_identifier
        || receipt.source.source_line != source.source_line
        || receipt.source.prompt_sha256 != source.sha256
    {
        return Err("Context receipt provenance does not match the selected source".to_owned());
    }
    if receipt.verbatim_source_text != source.body {
        return Err("Context receipt does not preserve the selected source words".to_owned());
    }
    Ok(Context {
        flow_identifier: receipt.source.flow_identifier,
        source_turn_identifier: receipt.source.source_turn_identifier,
        transcript_path: receipt.source.source_path,
        prompt_sha256: receipt.source.prompt_sha256,
        what_living_said: receipt.derived.what_living_said,
        context_about: receipt.derived.context_about,
        context_answered: receipt.derived.context_answered,
        context_corrected: receipt.derived.context_corrected,
        context_uncertainties: receipt.derived.context_uncertainties,
    })
}

#[derive(Deserialize)]
struct ContextReceipt {
    kind: String,
    machine_authored: bool,
    source: ContextReceiptSource,
    verbatim_source_text: String,
    derived: ContextReceiptDerived,
}

#[derive(Deserialize)]
struct ContextReceiptSource {
    source_path: String,
    flow_identifier: String,
    source_turn_identifier: String,
    source_event_identifier: String,
    source_session_identifier: String,
    source_line: usize,
    prompt_sha256: String,
}

#[derive(Deserialize, Serialize)]
struct ContextReceiptDerived {
    what_living_said: String,
    context_about: String,
    context_answered: String,
    context_corrected: String,
    context_uncertainties: Vec<String>,
}

fn locate(head: &str, tail: &str) -> Result<Source, String> {
    let paths = match env::var_os("RELAY_TRANSCRIPT") {
        Some(path) => vec![PathBuf::from(path)],
        None => return Err(
            "missing RELAY_TRANSCRIPT; process-to-transcript discovery adapter is not installed"
                .into(),
        ),
    };
    let mut matches = Vec::new();
    for path in paths {
        matches.extend(records(&path, head, tail)?);
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err("no user record has the supplied first and last six words".into()),
        count => Err(format!(
            "{count} user records match; selectors are ambiguous"
        )),
    }
}

fn records(path: &Path, head: &str, tail: &str) -> Result<Vec<Source>, String> {
    let input =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let mut found = Vec::new();
    for (line_index, line) in input.lines().enumerate() {
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let Some(body) = user_body(&value) else {
            continue;
        };
        if first_six(&body) != head || last_six(&body) != tail {
            continue;
        }
        let timestamp = value
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("matched record in {} has no timestamp", path.display()))?;
        let timestamp_nanos = OffsetDateTime::parse(timestamp, &Rfc3339)
            .map_err(|error| format!("parse source timestamp: {error}"))?
            .unix_timestamp_nanos()
            .try_into()
            .map_err(|_| "source timestamp exceeds TimestampNanos".to_owned())?;
        let session_identifier = source_session_identifier(&value)?;
        let flow_identifier = session_identifier
            .chars()
            .take_while(|character| *character != '-')
            .take(6)
            .collect::<String>();
        if flow_identifier.len() != 6 {
            return Err(format!(
                "matched source session {session_identifier:?} has no flow identifier"
            ));
        }
        let source_turn_identifier = source_turn_identifier(
            &value,
            &session_identifier,
            timestamp,
            line_index + 1,
            &body,
        )?;
        let source_event_identifier = source_event_identifier(&value, timestamp)?;
        found.push(Source {
            path: path.to_path_buf(),
            sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
            body,
            timestamp_nanos,
            flow_identifier,
            session_identifier,
            source_turn_identifier,
            source_event_identifier,
            source_line: line_index + 1,
        });
    }
    Ok(found)
}

fn source_turn_identifier(
    value: &Value,
    session_identifier: &str,
    timestamp: &str,
    line: usize,
    body: &str,
) -> Result<String, String> {
    if value.get("type").and_then(Value::as_str) == Some("queue-operation") {
        return Ok(format!(
            "queue:{session_identifier}:{timestamp}:{line}:{:x}",
            Sha256::digest(body.as_bytes())
        ));
    }
    value
        .get("uuid")
        .or_else(|| value.get("promptId"))
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            "matched ordinary user record has no native source turn identifier".to_owned()
        })
}

fn source_event_identifier(value: &Value, timestamp: &str) -> Result<String, String> {
    if value.get("type").and_then(Value::as_str) == Some("queue-operation") {
        return Ok(format!("queue-enqueue:{timestamp}"));
    }
    value
        .get("uuid")
        .or_else(|| value.get("promptId"))
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            "matched ordinary user record has no native source event identifier".to_owned()
        })
}

fn source_session_identifier(value: &Value) -> Result<String, String> {
    value
        .get("sessionId")
        .or_else(|| value.get("session_id"))
        .and_then(Value::as_str)
        .filter(|identifier| !identifier.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "matched user record has no source session identifier".to_owned())
}

fn user_body(value: &Value) -> Option<String> {
    if value.get("type")?.as_str()? == "queue-operation"
        && value.get("operation")?.as_str()? == "enqueue"
    {
        let body = value.get("content")?.as_str()?;
        return (!is_relay_or_peer_text(body)).then(|| body.to_owned());
    }
    let payload = value.get("payload")?;
    if value.get("type")?.as_str()? != "response_item"
        || payload.get("type")?.as_str()? != "message"
        || payload.get("role")?.as_str()? != "user"
        || value.get("promptSource").and_then(Value::as_str) == Some("system")
    {
        return None;
    }
    payload.get("content")?.as_array()?.iter().find_map(|part| {
        matches!(part.get("type")?.as_str()?, "input_text" | "text").then(|| {
            part.get("text")?
                .as_str()
                .filter(|text| !is_relay_or_peer_text(text))
                .map(str::to_owned)
        })?
    })
}

fn is_relay_or_peer_text(text: &str) -> bool {
    text.contains("<cross-session-message")
        || text.starts_with("Another Claude session sent a message:")
        || text.starts_with("[PEER ")
}

fn first_six(body: &str) -> String {
    body.split_whitespace()
        .take(6)
        .collect::<Vec<_>>()
        .join(" ")
}
fn last_six(body: &str) -> String {
    let words = body.split_whitespace().collect::<Vec<_>>();
    words[words.len().saturating_sub(6)..].join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_record_requires_exact_word_boundaries_and_preserves_body_hash() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.jsonl");
        let body = "one two three four five six seven eight nine ten eleven twelve";
        fs::write(&path, format!("{{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"sessionId\":\"840e42bb-b2cd-42eb-a9ec-7659a5b13ded\",\"timestamp\":\"2026-09-15T22:28:09.894Z\",\"content\":\"{body}\"}}\n")).unwrap();
        let found = records(
            &path,
            "one two three four five six",
            "seven eight nine ten eleven twelve",
        )
        .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].body, body);
        assert_eq!(found[0].flow_identifier, "840e42");
        assert_eq!(
            found[0].sha256,
            format!("{:x}", Sha256::digest(body.as_bytes()))
        );
    }

    #[test]
    fn members_are_setup_data_not_public_prompt_arguments() {
        assert_eq!(members("cf7879@root,57a7aa@secondary").unwrap().len(), 2);
        assert!(members("not-a-member").is_err());
        assert!(members("").is_err());
        assert!(members("cf7879@root,cf7879@root").is_err());
    }

    #[test]
    fn websocket_upgrade_requires_the_expected_accept_value() {
        assert!(validate_websocket_upgrade(
            "HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
            "dGhlIHNhbXBsZSBub25jZQ==",
        )
        .is_ok());
        assert!(
            validate_websocket_upgrade(
                "HTTP/1.1 101 Switching Protocols\r\n\r\n",
                "dGhlIHNhbXBsZSBub25jZQ==",
            )
            .is_err()
        );
    }

    #[test]
    fn context_receipt_is_copied_only_when_its_source_provenance_matches() {
        let directory = tempfile::tempdir().unwrap();
        let transcript = directory.path().join("source.jsonl");
        let body = "the exact living words";
        let source = Source {
            path: transcript.clone(),
            body: body.to_owned(),
            sha256: format!("{:x}", Sha256::digest(body.as_bytes())),
            timestamp_nanos: 1,
            flow_identifier: "cf7879".to_owned(),
            session_identifier: "cf7879-session".to_owned(),
            source_turn_identifier: "msg-1".to_owned(),
            source_event_identifier: "msg-1".to_owned(),
            source_line: 1,
        };
        let receipt = directory.path().join("context.json");
        fs::write(
            &receipt,
            serde_json::json!({
                "kind": "clusterrelay-derived-context",
                "machine_authored": true,
                "verbatim_source_text": body,
                "source": {
                    "source_path": transcript.display().to_string(),
                    "flow_identifier": "cf7879",
                    "source_turn_identifier": "msg-1",
                    "source_event_identifier": "msg-1",
                    "source_session_identifier": "cf7879-session",
                    "source_line": 1,
                    "prompt_sha256": source.sha256,
                },
                "derived": {
                    "what_living_said": body,
                    "context_about": "relay",
                    "context_answered": "",
                    "context_corrected": "",
                    "context_uncertainties": ["none"],
                },
            })
            .to_string(),
        )
        .unwrap();
        let context = context_from_receipt(&source, "cf7879", &receipt).unwrap();
        assert_eq!(context.what_living_said, body);
        assert_eq!(context.source_turn_identifier, "msg-1");
    }
}
