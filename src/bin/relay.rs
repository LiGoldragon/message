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
    os::unix::{fs::OpenOptionsExt, net::UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use datom_codec::Datomizable;
use message::client::MessageSocket;
use protos::{Protosizable, Textualizable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use signal_message::{
    ClusterMember, ClusterMessage, ClusterRelay, ClusterTarget, Context, FlowDeliveryRequest,
    PromptInterpretationSelection, PromptVariant, Query, Response, TypedPromptEnvelope,
};
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
    let source = locate(head, tail, &members)?;
    let context = context_for(
        &source,
        &executor_flow_identifier,
        &executor_session_identifier,
    )?;
    let source_flow_identifier = source.flow_identifier.clone();
    let source_session_identifier = source.session_identifier.clone();
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
        cluster_members: members.clone(),
    });
    let prepared = PreparedRelay {
        header: header.datomize(vec![]).protosize().textualize(),
        body: source.body,
        source_flow_identifier,
        source_session_identifier,
        source_event_identifier: source.source_event_identifier,
        executor_flow_identifier,
        executor_session_identifier,
    };
    if let Some(route_file) = env::var_os("RELAY_FLOW_ROUTES") {
        let routes = FlowRouteConfiguration::read(Path::new(&route_file))?;
        let receipt = fanout(
            &members,
            &prepared.source_flow_identifier,
            &prepared.source_session_identifier,
            &prepared,
            &routes,
        );
        return Ok(serde_json::to_string(&receipt).map_err(|error| error.to_string())? + "\n");
    }
    if let Some(thread_id) = codex_target {
        let receipt = CodexAppServer::from_environment()?.deliver(&thread_id, &prepared)?;
        return Ok(serde_json::to_string(&receipt).map_err(|error| error.to_string())? + "\n");
    }
    Ok(prepared.render())
}

struct PreparedRelay {
    header: String,
    body: String,
    source_flow_identifier: String,
    source_session_identifier: String,
    source_event_identifier: String,
    executor_flow_identifier: String,
    executor_session_identifier: String,
}

impl PreparedRelay {
    #[cfg(test)]
    fn clone_for_test(&self) -> Self {
        Self {
            header: self.header.clone(),
            body: self.body.clone(),
            source_flow_identifier: self.source_flow_identifier.clone(),
            source_session_identifier: self.source_session_identifier.clone(),
            source_event_identifier: self.source_event_identifier.clone(),
            executor_flow_identifier: self.executor_flow_identifier.clone(),
            executor_session_identifier: self.executor_session_identifier.clone(),
        }
    }

    fn render(&self) -> String {
        format!("{}\n\n{}", self.header, self.body)
    }
}

/// A trusted Flow-owned route response. This binary validates and consumes the
/// supplied routes but never discovers sessions or stores a second registry.
#[derive(Debug, Deserialize)]
struct FlowRouteConfiguration {
    routes: Vec<FlowRoute>,
}

impl FlowRouteConfiguration {
    fn read(path: &Path) -> Result<Self, String> {
        let input = fs::read_to_string(path)
            .map_err(|error| format!("read configured Flow routes {}: {error}", path.display()))?;
        serde_json::from_str(&input)
            .map_err(|error| format!("parse configured Flow routes {}: {error}", path.display()))
    }
}

#[derive(Debug, Deserialize)]
struct FlowRoute {
    flow_identifier: String,
    session_identifier: String,
    harness: RouteHarness,
    readiness: RouteReadiness,
    endpoint: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum RouteHarness {
    Codex,
    Claude,
    Nexus,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum RouteReadiness {
    Idle,
    Busy,
    Unknown,
}

#[derive(Serialize)]
struct FanoutReceipt {
    kind: &'static str,
    outcomes: Vec<FanoutOutcome>,
}

#[derive(Serialize)]
struct FanoutOutcome {
    flow_identifier: String,
    session_identifier: String,
    outcome: FanoutDisposition,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum FanoutDisposition {
    Accepted { receipt: RouteReceipt },
    Unavailable { reason: String },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum RouteReceipt {
    CodexTurnStartAcknowledged {
        thread_id: String,
        turn_id: Option<String>,
        status: Option<String>,
        executor_flow_identifier: String,
        executor_session_identifier: String,
    },
    ClaudePtyWriteAcknowledged {
        session_identifier: String,
        executor_flow_identifier: String,
        executor_session_identifier: String,
    },
    NexusFlowDeliveryParked {
        target_flow_name: String,
        source_event_identifier: String,
        executor_flow_identifier: String,
        executor_session_identifier: String,
    },
}

/// Delivers only to trusted Flow-routed cluster members other than the exact
/// selected source. A busy Claude target is represented by an explicit Nexus
/// route; a failed PTY invocation is never reclassified as busy.
fn fanout(
    members: &[ClusterMember],
    source_flow_identifier: &str,
    source_session_identifier: &str,
    relay: &PreparedRelay,
    routes: &FlowRouteConfiguration,
) -> FanoutReceipt {
    let outcomes = members
        .iter()
        .filter(|member| {
            member.flow_identifier != source_flow_identifier
                || member.session_identifier != source_session_identifier
        })
        .map(|member| FanoutOutcome {
            flow_identifier: member.flow_identifier.clone(),
            session_identifier: member.session_identifier.clone(),
            outcome: fanout_member(member, relay, routes),
        })
        .collect();
    FanoutReceipt {
        kind: "cluster-relay-fanout",
        outcomes,
    }
}

fn fanout_member(
    member: &ClusterMember,
    relay: &PreparedRelay,
    configured: &FlowRouteConfiguration,
) -> FanoutDisposition {
    let routes = configured
        .routes
        .iter()
        .filter(|route| {
            route.flow_identifier == member.flow_identifier
                && route.session_identifier == member.session_identifier
        })
        .collect::<Vec<_>>();
    let [route] = routes.as_slice() else {
        return FanoutDisposition::Unavailable {
            reason:
                "configured Flow route lookup did not return one route for this declared member"
                    .to_owned(),
        };
    };
    let outcome = match route.harness {
        RouteHarness::Codex if route.readiness == RouteReadiness::Idle => CodexAppServer {
            socket_path: route.endpoint.clone(),
        }
        .deliver(&member.session_identifier, relay)
        .map(|receipt| RouteReceipt::CodexTurnStartAcknowledged {
            thread_id: receipt.thread_id,
            turn_id: receipt.turn_id,
            status: receipt.status,
            executor_flow_identifier: receipt.executor_flow_identifier,
            executor_session_identifier: receipt.executor_session_identifier,
        }),
        RouteHarness::Claude if route.readiness == RouteReadiness::Idle => ClaudePromptRelay {
            executable: route.endpoint.clone(),
        }
        .deliver(&member.session_identifier, relay),
        RouteHarness::Nexus if route.readiness == RouteReadiness::Busy => NexusFlowDeliver {
            socket_path: route.endpoint.clone(),
        }
        .park(&member.flow_identifier, relay),
        RouteHarness::Codex | RouteHarness::Claude | RouteHarness::Nexus => {
            Err("configured Flow route has no fresh readiness witness".to_owned())
        }
    };
    match outcome {
        Ok(receipt) => FanoutDisposition::Accepted { receipt },
        Err(reason) => FanoutDisposition::Unavailable { reason },
    }
}

struct ClaudePromptRelay {
    executable: PathBuf,
}

const ADAPTER_TIMEOUT: Duration = Duration::from_secs(10);
const ADAPTER_OUTPUT_LIMIT: u64 = 64 * 1024;

impl ClaudePromptRelay {
    fn deliver(
        &self,
        session_identifier: &str,
        relay: &PreparedRelay,
    ) -> Result<RouteReceipt, String> {
        // `prompt-relay` owns the following peer-file provenance envelope. The
        // input itself retains the typed ClusterRelay header and verbatim body.
        let peer_file = PeerFile::write("peer", relay.render().as_bytes())?;
        let stdout = PeerFile::empty("stdout")?;
        let stderr = PeerFile::empty("stderr")?;
        let stdout_handle = fs::OpenOptions::new()
            .write(true)
            .open(stdout.path())
            .map_err(|error| format!("open bounded Claude receipt: {error}"))?;
        let stderr_handle = fs::OpenOptions::new()
            .write(true)
            .open(stderr.path())
            .map_err(|error| format!("open bounded Claude error receipt: {error}"))?;
        let mut child = Command::new(&self.executable)
            .arg("claude")
            .arg("--source")
            .arg(peer_file.path())
            .arg("--source-format")
            .arg("peer-file")
            .arg("--session-short")
            .arg(session_identifier)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout_handle))
            .stderr(Stdio::from(stderr_handle))
            .spawn()
            .map_err(|error| {
                format!(
                    "start configured Claude prompt-relay {}: {error}",
                    self.executable.display()
                )
            })?;
        let status = wait_for_child(&mut child, ADAPTER_TIMEOUT, &[&stdout, &stderr])?;
        let captured_stderr = bounded_file(&stderr, "Claude prompt-relay stderr")?;
        if !status.success() {
            return Err(format!(
                "configured Claude prompt-relay refused: {}",
                String::from_utf8_lossy(&captured_stderr)
            ));
        }
        let captured_stdout = bounded_file(&stdout, "Claude prompt-relay receipt")?;
        let receipt: Value = serde_json::from_slice(&captured_stdout)
            .map_err(|error| format!("parse Claude prompt-relay receipt: {error}"))?;
        if receipt.get("kind").and_then(Value::as_str) != Some("claude-bytes-written-to-pty")
            || receipt.get("session_id").and_then(Value::as_str) != Some(session_identifier)
        {
            return Err(
                "configured Claude prompt-relay did not provide the matching PTY-write receipt"
                    .to_owned(),
            );
        }
        Ok(RouteReceipt::ClaudePtyWriteAcknowledged {
            session_identifier: session_identifier.to_owned(),
            executor_flow_identifier: relay.executor_flow_identifier.clone(),
            executor_session_identifier: relay.executor_session_identifier.clone(),
        })
    }
}

fn wait_for_child(
    child: &mut Child,
    timeout: Duration,
    output_files: &[&PeerFile],
) -> Result<ExitStatus, String> {
    let deadline = Instant::now() + timeout;
    loop {
        for file in output_files {
            let size = fs::metadata(file.path())
                .map_err(|error| format!("stat configured Claude output: {error}"))?
                .len();
            if size > ADAPTER_OUTPUT_LIMIT {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "configured Claude prompt-relay output exceeds {} bytes",
                    ADAPTER_OUTPUT_LIMIT
                ));
            }
        }
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("poll configured Claude prompt-relay: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("configured Claude prompt-relay exceeded its 10-second bound".to_owned());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn bounded_file(file: &PeerFile, label: &str) -> Result<Vec<u8>, String> {
    let size = fs::metadata(file.path())
        .map_err(|error| format!("stat {label}: {error}"))?
        .len();
    if size > ADAPTER_OUTPUT_LIMIT {
        return Err(format!("{label} exceeds {} bytes", ADAPTER_OUTPUT_LIMIT));
    }
    fs::read(file.path()).map_err(|error| format!("read {label}: {error}"))
}

struct PeerFile {
    path: PathBuf,
}
impl PeerFile {
    fn write(kind: &str, bytes: &[u8]) -> Result<Self, String> {
        let file = Self::empty(kind)?;
        let mut handle = fs::OpenOptions::new()
            .write(true)
            .open(file.path())
            .map_err(|error| format!("open peer relay file: {error}"))?;
        handle
            .write_all(bytes)
            .map_err(|error| format!("write peer relay file: {error}"))?;
        Ok(file)
    }
    fn empty(kind: &str) -> Result<Self, String> {
        for nonce in 0..128_u32 {
            let path = env::temp_dir().join(format!(
                "message-relay-{kind}-{}-{nonce}",
                std::process::id()
            ));
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
            {
                Ok(_) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(format!("create bounded relay {kind} file: {error}")),
            }
        }
        Err(format!("create bounded relay {kind} file"))
    }
    fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for PeerFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn parked_source_event_identifier(relay: &PreparedRelay) -> String {
    format!(
        "{}:{}:{}",
        relay.source_flow_identifier,
        relay.source_session_identifier,
        relay.source_event_identifier
    )
}

struct NexusFlowDeliver {
    socket_path: PathBuf,
}
impl NexusFlowDeliver {
    fn park(&self, target_flow_name: &str, relay: &PreparedRelay) -> Result<RouteReceipt, String> {
        let source_event_identifier = parked_source_event_identifier(relay);
        let response = MessageSocket::from_path(&self.socket_path)
            .client()
            .submit_with_timeout(
                Query::FlowDeliver(FlowDeliveryRequest {
                    typed_prompt_envelope: TypedPromptEnvelope {
                        prompt_variant: PromptVariant::HumanPrompt,
                        source_event_identifier: source_event_identifier.clone(),
                        // The typed ClusterRelay header (including Context) remains beside
                        // the untouched source body for the delayed Message leg.
                        raw_prompt_text: relay.render(),
                        prompt_interpretation_selection: PromptInterpretationSelection::None,
                    },
                    target_flow_name: target_flow_name.to_owned(),
                }),
                ADAPTER_TIMEOUT,
            )
            .map_err(|error| format!("submit configured FlowDeliver: {error}"))?;
        if !matches!(response, Response::DeliveryQueued(_)) {
            return Err("configured FlowDeliver did not park the relay".to_owned());
        }
        Ok(RouteReceipt::NexusFlowDeliveryParked {
            target_flow_name: target_flow_name.to_owned(),
            source_event_identifier,
            executor_flow_identifier: relay.executor_flow_identifier.clone(),
            executor_session_identifier: relay.executor_session_identifier.clone(),
        })
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
    let mut sessions = std::collections::BTreeSet::new();
    if members
        .iter()
        .any(|member| !sessions.insert(member.session_identifier.as_str()))
    {
        return Err("RELAY_CLUSTER_MEMBERS binds one session to more than one member".into());
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
fn context_for(
    source: &Source,
    executor_flow_identifier: &str,
    executor_session_identifier: &str,
) -> Result<Context, String> {
    match env::var_os("RELAY_CONTEXT_RECEIPT") {
        Some(receipt_path) => context_from_receipt(
            source,
            executor_flow_identifier,
            executor_session_identifier,
            Path::new(&receipt_path),
        ),
        None => Ok(Context {
            flow_identifier: source.flow_identifier.clone(),
            source_turn_identifier: source.source_turn_identifier.clone(),
            transcript_path: source.path.display().to_string(),
            prompt_sha256: source.sha256.clone(),
            what_living_said: source.body.clone(),
            context_about: "unreviewed: Context receipt unavailable".to_owned(),
            context_answered: "unavailable".to_owned(),
            context_corrected: "unavailable".to_owned(),
            context_uncertainties: vec!["unreviewed: Context receipt unavailable".to_owned()],
        }),
    }
}

fn context_from_receipt(
    source: &Source,
    executor_flow_identifier: &str,
    executor_session_identifier: &str,
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
        || receipt.source.source_flow_identifier != source.flow_identifier
        || receipt.source.source_session_identifier != source.session_identifier
        || receipt.source.source_turn_identifier != source.source_turn_identifier
        || receipt.source.source_event_identifier != source.source_event_identifier
        || receipt.source.source_line != source.source_line
        || receipt.source.prompt_sha256 != source.sha256
        || receipt.source.executor_flow_identifier != executor_flow_identifier
        || receipt.source.executor_session_identifier != executor_session_identifier
    {
        return Err("Context receipt provenance does not match the selected source".to_owned());
    }
    if receipt.verbatim_source_text != source.body {
        return Err("Context receipt does not preserve the selected source words".to_owned());
    }
    Ok(Context {
        flow_identifier: receipt.source.source_flow_identifier,
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
    source_flow_identifier: String,
    source_turn_identifier: String,
    source_event_identifier: String,
    source_session_identifier: String,
    source_line: usize,
    executor_flow_identifier: String,
    executor_session_identifier: String,
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

fn locate(head: &str, tail: &str, members: &[ClusterMember]) -> Result<Source, String> {
    let paths = match env::var_os("RELAY_TRANSCRIPT") {
        Some(path) => vec![PathBuf::from(path)],
        None => return Err(
            "missing RELAY_TRANSCRIPT; process-to-transcript discovery adapter is not installed"
                .into(),
        ),
    };
    let requested_event = env::var("RELAY_SOURCE_EVENT_ID").ok();
    let mut matches = Vec::new();
    for path in paths {
        matches.extend(records(&path, head, tail, members)?);
    }
    select_source(matches, requested_event.as_deref())
}

fn select_source(
    mut matches: Vec<Source>,
    requested_event: Option<&str>,
) -> Result<Source, String> {
    if let Some(requested_event) = requested_event {
        matches.retain(|source| source.source_event_identifier == requested_event);
        return match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => Err(format!(
                "no user record matches RELAY_SOURCE_EVENT_ID {requested_event:?}"
            )),
            count => Err(format!(
                "{count} user records match RELAY_SOURCE_EVENT_ID {requested_event:?}; source events are not unique"
            )),
        };
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err("no user record has the supplied first and last six words".into()),
        count => Err(format!(
            "{count} user records match; selectors are ambiguous"
        )),
    }
}

fn records(
    path: &Path,
    head: &str,
    tail: &str,
    members: &[ClusterMember],
) -> Result<Vec<Source>, String> {
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
        let flow_identifier = members
            .iter()
            .find(|member| member.session_identifier == session_identifier)
            .map(|member| member.flow_identifier.clone())
            .ok_or_else(|| {
                format!("matched source session {session_identifier:?} is not declared")
            })?;
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
        .or_else(|| value.get("payload").and_then(|payload| payload.get("id")))
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
        .or_else(|| value.get("payload").and_then(|payload| payload.get("id")))
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
        .or_else(|| value.get("session_meta").and_then(|meta| meta.get("id")))
        .and_then(Value::as_str)
        .filter(|identifier| !identifier.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "matched user record has no source session identifier".to_owned())
}

fn user_body(value: &Value) -> Option<String> {
    if is_cluster_relay_record(value) {
        return None;
    }
    if value.get("type")?.as_str()? == "queue-operation"
        && value.get("operation")?.as_str()? == "enqueue"
    {
        let body = value.get("content")?.as_str()?;
        return (!is_relay_or_peer_text(body)).then(|| body.to_owned());
    }
    if value.get("type")?.as_str()? == "user" {
        return text_content(value.get("message")?.get("content")?);
    }
    let payload = value.get("payload")?;
    (value.get("type")?.as_str()? == "response_item"
        && payload.get("type")?.as_str()? == "message"
        && payload.get("role")?.as_str()? == "user"
        && value.get("promptSource").and_then(Value::as_str) != Some("system"))
    .then(|| text_content(payload.get("content")?))?
}

fn text_content(content: &Value) -> Option<String> {
    let parts = match content {
        Value::String(text) => vec![text.as_str()],
        Value::Array(parts) => parts
            .iter()
            .filter(|part| {
                matches!(
                    part.get("type").and_then(Value::as_str),
                    Some("input_text" | "text")
                )
            })
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect(),
        _ => return None,
    };
    if parts.iter().any(|part| is_relay_or_peer_text(part)) {
        return None;
    }
    let body = parts.join("");
    (!body.is_empty()).then_some(body)
}

/// Relay emits a Datom header followed by the verbatim body. Codex rollout
/// serialization can preserve those as two `input_text` parts or consolidate
/// them into one part separated by a blank line. Reject the whole record so
/// the body cannot be selected and relayed again. The header test matches the
/// emitted Datom form, not ordinary human discussion of the word “Relay”.
fn is_cluster_relay_record(value: &Value) -> bool {
    let content = match value.get("type").and_then(Value::as_str) {
        Some("user") => value
            .get("message")
            .and_then(|message| message.get("content")),
        Some("response_item")
            if value
                .get("payload")
                .and_then(|payload| payload.get("type"))
                .and_then(Value::as_str)
                == Some("message") =>
        {
            value
                .get("payload")
                .and_then(|payload| payload.get("content"))
        }
        _ => None,
    };
    let Some(text_parts) = content_text_parts(content) else {
        return false;
    };
    if text_parts.iter().any(|part| is_relay_or_peer_text(part))
        || is_prompt_relay_provenance_parts(&text_parts)
    {
        return true;
    }
    match text_parts.as_slice() {
        [header, body, ..] => !body.is_empty() && is_emitted_cluster_header(header),
        [combined] => combined
            .split_once("\n\n")
            .is_some_and(|(header, body)| !body.is_empty() && is_emitted_cluster_header(header)),
        _ => false,
    }
}

fn content_text_parts(content: Option<&Value>) -> Option<Vec<&str>> {
    match content? {
        Value::String(text) => Some(vec![text]),
        Value::Array(parts) => Some(
            parts
                .iter()
                .filter(|part| {
                    matches!(
                        part.get("type").and_then(Value::as_str),
                        Some("input_text" | "text")
                    )
                })
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect(),
        ),
        _ => None,
    }
}

fn is_emitted_cluster_header(header: &str) -> bool {
    // Decode exact producer text when possible. Keep the established Relay
    // shape check for already-recorded rollout frames whose historical header
    // is intentionally less complete than the current producer contract.
    message::text::read::<ClusterMessage>(header).is_ok()
        || (header.starts_with("Relay.{")
            && header.ends_with('}')
            && [" Primary [", " Secondary [", " Core ["]
                .iter()
                .any(|target| header.contains(target)))
}

fn is_relay_or_peer_text(text: &str) -> bool {
    is_closed_cross_session_envelope(text)
        || ["[RELAY ", "[PEER ", "[WAKE ", "[SYSTEM "]
            .iter()
            .any(|prefix| text.starts_with(prefix))
        || is_prompt_relay_provenance_envelope(text)
}

fn is_closed_cross_session_envelope(text: &str) -> bool {
    let envelope = text
        .strip_prefix("Another Claude session sent a message:\n\n")
        .unwrap_or(text);
    envelope.starts_with("<cross-session-message") && envelope.contains("</cross-session-message>")
}

/// `tools/prompt-relay` prepends this JSON envelope and a blank line before it
/// writes the source words into a recipient transcript. Require the complete
/// leading envelope rather than treating ordinary discussion of provenance as
/// a relay marker.
fn is_prompt_relay_provenance_envelope(text: &str) -> bool {
    let Some((header, body)) = text.split_once("\n\n") else {
        return false;
    };
    !body.is_empty() && is_prompt_relay_provenance_header(header)
}

fn is_prompt_relay_provenance_parts(parts: &[&str]) -> bool {
    matches!(parts, [header, body, ..] if !body.is_empty() && is_prompt_relay_provenance_header(header))
}

fn is_prompt_relay_provenance_header(header: &str) -> bool {
    let Ok(header) = serde_json::from_str::<Value>(header) else {
        return false;
    };
    let Some(provenance) = header.get("provenance").and_then(Value::as_object) else {
        return false;
    };
    ["source_path", "source_format", "source_message_id"]
        .iter()
        .all(|key| {
            provenance
                .get(*key)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })
        && provenance.get("source_timestamp").is_some_and(|timestamp| {
            timestamp.is_null() || timestamp.as_str().is_some_and(|value| !value.is_empty())
        })
        && provenance
            .get("sha256_utf8")
            .and_then(Value::as_str)
            .is_some_and(|hash| {
                hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
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
            &[ClusterMember {
                flow_identifier: "840e42".to_owned(),
                session_identifier: "840e42bb-b2cd-42eb-a9ec-7659a5b13ded".to_owned(),
            }],
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
    fn explicit_source_event_selects_one_identical_body_and_rejects_unknown_event() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.jsonl");
        let body = "one two three four five six seven eight nine ten eleven twelve";
        fs::write(&path, format!(
            "{{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"sessionId\":\"efa15708-dc5d-42ce-af62-8ffb84c9815e\",\"timestamp\":\"2026-09-16T00:00:00Z\",\"content\":\"{body}\"}}\n{{\"type\":\"user\",\"uuid\":\"eb26a23f-ce6b-4117-bed1-6790bb373b74\",\"sessionId\":\"efa15708-dc5d-42ce-af62-8ffb84c9815e\",\"timestamp\":\"2026-09-16T00:00:01Z\",\"message\":{{\"content\":\"{body}\"}}}}\n"
        )).unwrap();
        let matches = records(
            &path,
            "one two three four five six",
            "seven eight nine ten eleven twelve",
            &[ClusterMember {
                flow_identifier: "efa157".to_owned(),
                session_identifier: "efa15708-dc5d-42ce-af62-8ffb84c9815e".to_owned(),
            }],
        )
        .unwrap();
        assert_eq!(matches.len(), 2);
        let selected =
            select_source(matches, Some("eb26a23f-ce6b-4117-bed1-6790bb373b74")).unwrap();
        assert_eq!(
            selected.source_event_identifier,
            "eb26a23f-ce6b-4117-bed1-6790bb373b74"
        );
        let unknown = records(
            &path,
            "one two three four five six",
            "seven eight nine ten eleven twelve",
            &[ClusterMember {
                flow_identifier: "efa157".to_owned(),
                session_identifier: "efa15708-dc5d-42ce-af62-8ffb84c9815e".to_owned(),
            }],
        )
        .unwrap();
        let error = select_source(unknown, Some("missing-event")).unwrap_err();
        assert!(error.contains("RELAY_SOURCE_EVENT_ID"));
    }

    #[test]
    fn ordinary_claude_user_record_is_selected_without_a_queue_operation() {
        let value = serde_json::json!({
            "type": "user",
            "message": { "content": "one two three four five six seven" }
        });
        assert_eq!(
            user_body(&value),
            Some("one two three four five six seven".to_owned())
        );
    }

    #[test]
    fn all_codex_text_parts_are_preserved_in_their_original_order() {
        let value = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "first " },
                    { "type": "image", "url": "ignored" },
                    { "type": "text", "text": "second" }
                ]
            }
        });
        assert_eq!(user_body(&value), Some("first second".to_owned()));
    }

    #[test]
    fn incoming_two_part_relay_record_is_excluded_as_one_record() {
        let relay = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": "Relay.{ source session path 1 Primary [ { source session } ] }" },
                    { "type": "input_text", "text": "one two three four five six seven" }
                ]
            }
        });
        assert_eq!(user_body(&relay), None);
    }

    #[test]
    fn consolidated_codex_rollout_relay_record_is_excluded_as_one_record() {
        let relay = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": "Relay.{ source session transcript 1 Primary [ { source session } ] }\n\none two three four five six seven"
                }]
            }
        });
        assert_eq!(user_body(&relay), None);
    }

    #[test]
    fn codec_produced_peer_record_is_excluded_as_one_record() {
        let header = include_str!("../../tests/fixtures/cf7879-peer-cluster-header.datom");
        let body = include_str!("../../tests/fixtures/cf7879-peer-cluster-body.md");
        let peer = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": header },
                    { "type": "input_text", "text": body }
                ]
            }
        });
        assert_eq!(user_body(&peer), None);
    }

    #[test]
    fn ordinary_human_discussion_of_relay_is_not_excluded() {
        let user = serde_json::json!({
            "type": "user",
            "message": { "content": "Relay is the topic of this ordinary human message" }
        });
        assert_eq!(
            user_body(&user),
            Some("Relay is the topic of this ordinary human message".to_owned())
        );
    }

    #[test]
    fn prompt_relay_json_provenance_envelope_is_excluded_for_combined_and_split_parts() {
        let header = "{\"provenance\":{\"source_path\":\"/tmp/source.jsonl\",\"source_format\":\"codex\",\"source_message_id\":\"msg-1\",\"source_timestamp\":null,\"sha256_utf8\":\"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"}}";
        let envelope = format!("{header}\n\nliving words");
        let string_record = serde_json::json!({
            "type": "user",
            "message": { "content": envelope }
        });
        let text_part_record = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    { "type": "input_text", "text": header },
                    { "type": "input_text", "text": "living words" }
                ]
            }
        });
        assert_eq!(user_body(&string_record), None);
        assert_eq!(user_body(&text_part_record), None);
    }

    #[test]
    fn ordinary_json_discussion_without_a_complete_provenance_envelope_is_not_excluded() {
        let user = serde_json::json!({
            "type": "user",
            "message": { "content": "{\"provenance\":{\"source_message_id\":\"msg-1\"}}\n\nThis is ordinary discussion." }
        });
        assert_eq!(
            user_body(&user),
            Some("{\"provenance\":{\"source_message_id\":\"msg-1\"}}\n\nThis is ordinary discussion.".to_owned())
        );
    }

    #[test]
    fn cross_session_markup_requires_a_closed_envelope_at_the_start() {
        assert!(is_relay_or_peer_text(
            "<cross-session-message source=\"peer\">machine text</cross-session-message>"
        ));
        assert!(!is_relay_or_peer_text(
            "A human quoted <cross-session-message> while discussing the protocol"
        ));
        assert!(!is_relay_or_peer_text(
            "<cross-session-message incomplete human quote"
        ));
        assert!(is_relay_or_peer_text(
            "Another Claude session sent a message:\n\n<cross-session-message source=\"peer\">machine text</cross-session-message>"
        ));
        assert!(!is_relay_or_peer_text(
            "Another Claude session sent a message: ordinary human discussion"
        ));
        assert!(!is_relay_or_peer_text(
            "Another Claude session sent a message:\n\n<cross-session-message incomplete human discussion"
        ));
    }

    #[test]
    fn members_are_setup_data_not_public_prompt_arguments() {
        assert_eq!(members("cf7879@root,57a7aa@secondary").unwrap().len(), 2);
        assert!(members("not-a-member").is_err());
        assert!(members("").is_err());
        assert!(members("cf7879@root,cf7879@root").is_err());
        assert!(members("cf7879@root,other@root").is_err());
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
                    "source_flow_identifier": "cf7879",
                    "source_turn_identifier": "msg-1",
                    "source_event_identifier": "msg-1",
                    "source_session_identifier": "cf7879-session",
                    "source_line": 1,
                    "executor_flow_identifier": "cf7879",
                    "executor_session_identifier": "executor-1",
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
        let context = context_from_receipt(&source, "cf7879", "executor-1", &receipt).unwrap();
        assert_eq!(context.what_living_said, body);
        assert_eq!(context.source_turn_identifier, "msg-1");
    }
    #[test]
    fn parked_event_identity_uses_selected_source_not_relay_executor() {
        let first = PreparedRelay {
            header: "Relay.{}".to_owned(),
            body: "same body".to_owned(),
            source_flow_identifier: "source".to_owned(),
            source_session_identifier: "source-session".to_owned(),
            source_event_identifier: "turn-a".to_owned(),
            executor_flow_identifier: "executor-a".to_owned(),
            executor_session_identifier: "executor-a-session".to_owned(),
        };
        let reexecuted = PreparedRelay {
            executor_flow_identifier: "executor-b".to_owned(),
            executor_session_identifier: "executor-b-session".to_owned(),
            ..first.clone_for_test()
        };
        let other_turn = PreparedRelay {
            source_event_identifier: "turn-b".to_owned(),
            ..first.clone_for_test()
        };
        assert_eq!(
            parked_source_event_identifier(&first),
            parked_source_event_identifier(&reexecuted)
        );
        assert_ne!(
            parked_source_event_identifier(&first),
            parked_source_event_identifier(&other_turn)
        );
    }
}
