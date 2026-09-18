//! Direct Message Nexus delivery through Flow resolution and harness protocols.
//!
//! This is deliberately inside the daemon. The ordinary CLI only sends one
//! typed Message Signal; it never selects or executes a harness bridge.

use std::{
    env,
    ffi::OsString,
    fmt::Debug,
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::{Digest as _, Sha1};
use signal_flow::{
    EndpointSelection, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection, Query as FlowQuery,
    Response as FlowResponse, RouteReadiness,
};
use signal_message::{ClusterMessage, ReceiptKind};

const FLOW_SOCKET: &str = "/run/user/1001/flow/flow.sock";

#[derive(Clone, Debug)]
pub struct FlowResolver {
    socket: PathBuf,
}

impl FlowResolver {
    pub fn conventional() -> Self {
        Self {
            socket: env::var_os("FLOW_SOCKET")
                .map(PathBuf::from)
                .unwrap_or_else(|| FLOW_SOCKET.into()),
        }
    }

    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self {
            socket: path.into(),
        }
    }

    pub fn resolve(&self, flow: &str) -> Result<Option<FlowNode>, String> {
        let mut peer = UnixStream::connect(&self.socket)
            .map_err(|error| format!("connect Flow Nexus {}: {error}", self.socket.display()))?;
        peer.set_read_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        peer.set_write_timeout(Some(Duration::from_secs(5)))
            .map_err(|e| e.to_string())?;
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&FlowQuery::ResolveRecipient(flow.to_owned()))
                .map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let mut reply = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
        match rkyv::from_bytes::<FlowResponse, rkyv::rancor::Error>(&reply)
            .map_err(|e| e.to_string())?
        {
            FlowResponse::RecipientResolved(node) => Ok(Some(node)),
            FlowResponse::RecipientResolutionRejected(_) => Ok(None),
            other => Err(format!(
                "Flow Nexus returned non-resolution response: {other:?}"
            )),
        }
    }
}

/// The complete Message-owned delivery boundary. Tests replace this contract
/// with the same adapter pointed at disposable Flow and Herdr processes.
///
/// `Accepted` records transport submission only. It does not claim that the
/// target harness consumed, interpreted, or completed the submitted message.
pub trait NexusDelivery: Debug + Send + Sync {
    fn resolve(&self, flow: &str) -> Result<Option<FlowNode>, String>;
    fn deliver(&self, node: &FlowNode, message: &ClusterMessage) -> Result<ReceiptKind, String>;
}

#[derive(Clone, Debug)]
pub struct LiveNexusDelivery {
    resolver: FlowResolver,
    herdr_program: OsString,
}

impl LiveNexusDelivery {
    pub fn conventional() -> Self {
        Self {
            resolver: FlowResolver::conventional(),
            herdr_program: env::var_os("MESSAGE_HERDR_PROGRAM").unwrap_or_else(|| "herdr".into()),
        }
    }

    pub fn with_paths(flow_socket: impl Into<PathBuf>, herdr_program: impl Into<OsString>) -> Self {
        Self {
            resolver: FlowResolver::from_path(flow_socket),
            herdr_program: herdr_program.into(),
        }
    }

    fn deliver_resolved(
        &self,
        node: &FlowNode,
        message: &ClusterMessage,
    ) -> Result<ReceiptKind, String> {
        if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection {
            let datom = crate::text::write(message);
            self.deliver_herdr(node, route, &datom)?;
            return Ok(ReceiptKind::Accepted);
        }
        let EndpointSelection::Available(endpoint) = &node.endpoint_selection else {
            return Ok(ReceiptKind::Parked);
        };
        if endpoint.route_readiness == RouteReadiness::Parked {
            return Ok(ReceiptKind::Parked);
        }
        let datom = crate::text::write(message);
        match node.harness_kind {
            HarnessKind::Claude => deliver_claude(
                Path::new(&endpoint.endpoint_path),
                &node.flow_id,
                &node.session_id,
                &datom,
            ),
            HarnessKind::Codex => {
                deliver_codex(Path::new(&endpoint.endpoint_path), &node.session_id, &datom)
            }
        }?;
        Ok(ReceiptKind::Accepted)
    }

    fn deliver_herdr(
        &self,
        node: &FlowNode,
        route: &HerdrRoute,
        datom: &str,
    ) -> Result<(), String> {
        let Some(current) = self.resolver.resolve(&node.flow_id)? else {
            return Err("Flow no longer resolves the Herdr recipient".into());
        };
        if current.flow_id != node.flow_id
            || current.session_id != node.session_id
            || current.harness_kind != node.harness_kind
            || current.herdr_route_selection != HerdrRouteSelection::Available(route.clone())
        {
            return Err("Herdr recipient changed or is no longer ready".into());
        }
        // Herdr exposes identity, visible-screen, and prompt operations, but no
        // revision or compare-and-swap token spanning them. The identity and
        // blank-composer reads therefore reject known-stale routes; they cannot
        // make the screen snapshot atomic with prompt submission. A successful
        // prompt is recorded as transport acceptance, and the endpoint-only
        // check below detects replacement observed immediately afterward.
        validate_herdr_composer(&self.herdr_program, route, &node.harness_kind)?;
        let status = Command::new(&self.herdr_program)
            .args([
                "--session",
                &route.herdr_session_name,
                "agent",
                "prompt",
                &route.herdr_pane_id,
                datom,
            ])
            .status()
            .map_err(|error| format!("start Herdr prompt: {error}"))?;
        if status.success() {
            validate_herdr_identity(
                &self.herdr_program,
                route,
                &node.harness_kind,
                IdentityCheck::EndpointOnly,
            )
        } else {
            Err("Herdr prompt refused or outcome is uncertain".into())
        }
    }
}

impl NexusDelivery for LiveNexusDelivery {
    fn resolve(&self, flow: &str) -> Result<Option<FlowNode>, String> {
        self.resolver.resolve(flow)
    }

    fn deliver(&self, node: &FlowNode, message: &ClusterMessage) -> Result<ReceiptKind, String> {
        self.deliver_resolved(node, message)
    }
}

fn validate_herdr_composer(
    program: &std::ffi::OsStr,
    route: &HerdrRoute,
    harness: &HarnessKind,
) -> Result<(), String> {
    validate_herdr_identity(program, route, harness, IdentityCheck::ReadyComposer)?;
    let visible = Command::new(program)
        .args([
            "--session",
            &route.herdr_session_name,
            "agent",
            "read",
            &route.herdr_pane_id,
            "--source",
            "visible",
            "--lines",
            "80",
        ])
        .output()
        .map_err(|e| e.to_string())?;
    if !visible.status.success() {
        return Err("Herdr composer snapshot refused".into());
    }
    let screen =
        String::from_utf8(visible.stdout).map_err(|_| "Herdr composer snapshot was not text")?;
    let lines = screen.lines().collect::<Vec<_>>();
    let blank_prompt = match harness {
        HarnessKind::Claude => blank_claude_composer(&lines),
        HarnessKind::Codex => blank_codex_composer(&lines),
    };
    if !blank_prompt {
        return Err("Herdr composer is not a supported blank prompt".into());
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum IdentityCheck {
    ReadyComposer,
    EndpointOnly,
}

fn validate_herdr_identity(
    program: &std::ffi::OsStr,
    route: &HerdrRoute,
    harness: &HarnessKind,
    check: IdentityCheck,
) -> Result<(), String> {
    let get = Command::new(program)
        .args([
            "--session",
            &route.herdr_session_name,
            "agent",
            "get",
            &route.herdr_pane_id,
        ])
        .output()
        .map_err(|e| e.to_string())?;
    if !get.status.success() {
        return Err("Herdr agent identity lookup refused".into());
    }
    let agent: serde_json::Value =
        serde_json::from_slice(&get.stdout).map_err(|_| "Herdr agent identity was not JSON")?;
    let agent = agent
        .pointer("/result/agent")
        .ok_or("Herdr agent result missing")?;
    if agent.get("name").and_then(serde_json::Value::as_str) != Some(&route.herdr_agent_name)
        || agent.get("pane_id").and_then(serde_json::Value::as_str) != Some(&route.herdr_pane_id)
        || agent.get("terminal_id").and_then(serde_json::Value::as_str)
            != Some(&route.herdr_terminal_id)
        || agent.get("agent").and_then(serde_json::Value::as_str)
            != Some(match harness {
                HarnessKind::Codex => "codex",
                HarnessKind::Claude => "claude",
            })
    {
        return Err("Herdr recipient is not the registered agent".into());
    }
    if matches!(check, IdentityCheck::ReadyComposer) {
        let status = agent
            .get("agent_status")
            .and_then(serde_json::Value::as_str);
        if !matches!(status, Some("idle" | "working"))
            || agent
                .get("interactive_ready")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err("Herdr recipient is not ready for prompt submission".into());
        }
    }
    Ok(())
}

fn is_composer_rule(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed.chars().count() >= 8
        && trimmed.starts_with('─')
        && trimmed.ends_with('─')
        && trimmed
            .chars()
            .filter(|character| *character == '─')
            .count()
            >= 8
}

fn blank_claude_composer(lines: &[&str]) -> bool {
    let Some(prompt) = lines.iter().rposition(|line| line.trim().starts_with('❯')) else {
        return false;
    };
    prompt > 0
        && lines[prompt].trim() == "❯"
        && prompt + 2 < lines.len()
        && lines.len() - prompt <= 6
        && is_composer_rule(lines[prompt - 1])
        && is_composer_rule(lines[prompt + 1])
        && lines[prompt + 2..]
            .iter()
            .any(|line| !line.trim().is_empty())
}

fn blank_codex_composer(lines: &[&str]) -> bool {
    let Some(prompt) = lines
        .iter()
        .rposition(|line| line.trim_start().starts_with('›'))
    else {
        return false;
    };
    lines[prompt].trim() == "› Ask Codex to do anything"
        && prompt + 1 < lines.len()
        && lines.len() - prompt <= 10
        && lines[prompt + 1..]
            .iter()
            .any(|line| !line.trim().is_empty())
}

fn deliver_claude(control: &Path, flow: &str, session: &str, datom: &str) -> Result<(), String> {
    let key_path = env::var_os("MESSAGE_CLAUDE_CONTROL_KEY")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".claude/daemon/control.key"))
        })
        .ok_or_else(|| "cannot locate Claude daemon control key".to_owned())?;
    let auth =
        fs::read_to_string(&key_path).map_err(|e| format!("read {}: {e}", key_path.display()))?;
    deliver_claude_with_key(control, session, datom, auth.trim(), || {
        let Some(current) = FlowResolver::conventional().resolve(flow)? else {
            return Err("Flow no longer resolves the Claude recipient".into());
        };
        let EndpointSelection::Available(endpoint) = current.endpoint_selection else {
            return Err("Flow no longer exposes a Claude endpoint".into());
        };
        if current.session_id != session
            || current.harness_kind != HarnessKind::Claude
            || endpoint.route_readiness != RouteReadiness::Ready
            || Path::new(&endpoint.endpoint_path) != control
        {
            return Err("Claude recipient changed or is no longer ready".into());
        }
        Ok(())
    })
}

fn deliver_claude_with_key<BeforePaste>(
    control: &Path,
    session: &str,
    datom: &str,
    auth: &str,
    before_paste: BeforePaste,
) -> Result<(), String>
where
    BeforePaste: FnOnce() -> Result<(), String>,
{
    let mut peer = UnixStream::connect(control)
        .map_err(|e| format!("connect Claude control {}: {e}", control.display()))?;
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| e.to_string())?;
    let short = session.split('-').next().unwrap_or(session);
    let attach = serde_json::json!({
        "proto":1,"op":"attach","short":short,"auth":auth,"cols":120,"rows":40,
        "attachId":format!("message-{}", std::process::id()),
        "caps":{"imark":false,"terminal":"xterm","mux":null,"ssh":false}
    });
    writeln!(peer, "{attach}").map_err(|e| e.to_string())?;
    let mut reply = String::new();
    BufReader::new(peer.try_clone().map_err(|e| e.to_string())?)
        .read_line(&mut reply)
        .map_err(|e| e.to_string())?;
    let accepted = serde_json::from_str::<serde_json::Value>(&reply)
        .ok()
        .and_then(|v| v.get("ok").and_then(|v| v.as_bool()))
        .unwrap_or(false);
    if !accepted {
        return Err("Claude daemon attach refused".into());
    }
    before_paste()?;
    peer.write_all(b"\x1b[200~").map_err(|e| e.to_string())?;
    peer.write_all(datom.as_bytes())
        .map_err(|e| e.to_string())?;
    peer.write_all(b"\x1b[201~\r").map_err(|e| e.to_string())?;
    peer.flush().map_err(|e| e.to_string())
}

fn deliver_codex(socket_path: &Path, thread_id: &str, datom: &str) -> Result<(), String> {
    let mut socket = UnixStream::connect(socket_path)
        .map_err(|e| format!("connect Codex app server {}: {e}", socket_path.display()))?;
    socket
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    let key = websocket_key()?;
    write!(socket, "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").map_err(|e| e.to_string())?;
    let response = read_http_headers(&mut socket)?;
    validate_websocket_upgrade(&response, &key)?;
    let mut rpc = WebSocketRpc { socket, next_id: 0 };
    rpc.call(
        "initialize",
        serde_json::json!({"clientInfo":{"name":"message-nexus","version":"1"}}),
    )?;
    rpc.notify("initialized", serde_json::json!({}))?;
    rpc.call(
        "thread/resume",
        serde_json::json!({"threadId":thread_id,"excludeTurns":true}),
    )?;
    rpc.call(
        "turn/start",
        serde_json::json!({"threadId":thread_id,"input":[{"type":"text","text":datom}]}),
    )?;
    Ok(())
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
            if reply.get("id").and_then(|v| v.as_u64()) != Some(id) {
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
        let payload = serde_json::to_vec(&message).map_err(|e| e.to_string())?;
        let mask = [0x31, 0x41, 0x59, 0x26];
        let mut header = vec![0x81];
        if payload.len() < 126 {
            header.push(0x80 | payload.len() as u8);
        } else if payload.len() < 65536 {
            header.push(0xfe);
            header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            header.push(0xff);
            header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        header.extend_from_slice(&mask);
        self.socket.write_all(&header).map_err(|e| e.to_string())?;
        let masked = payload
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ mask[i % 4])
            .collect::<Vec<_>>();
        self.socket.write_all(&masked).map_err(|e| e.to_string())?;
        self.socket.flush().map_err(|e| e.to_string())
    }
    fn read(&mut self) -> Result<serde_json::Value, String> {
        let mut initial = [0; 2];
        self.socket
            .read_exact(&mut initial)
            .map_err(|e| e.to_string())?;
        let opcode = initial[0] & 15;
        if initial[0] & 128 == 0 {
            return Err("fragmented Codex websocket frame".into());
        }
        let mut length = usize::from(initial[1] & 127);
        if length == 126 {
            let mut b = [0; 2];
            self.socket.read_exact(&mut b).map_err(|e| e.to_string())?;
            length = usize::from(u16::from_be_bytes(b));
        } else if length == 127 {
            let mut b = [0; 8];
            self.socket.read_exact(&mut b).map_err(|e| e.to_string())?;
            length = usize::try_from(u64::from_be_bytes(b))
                .map_err(|_| "Codex websocket frame too large")?;
        }
        let mut body = vec![0; length];
        self.socket
            .read_exact(&mut body)
            .map_err(|e| e.to_string())?;
        if opcode == 9 {
            self.socket
                .write_all(&[0x8a, length as u8])
                .map_err(|e| e.to_string())?;
            self.socket.write_all(&body).map_err(|e| e.to_string())?;
            return self.read();
        }
        if opcode != 1 {
            return Err("unsupported Codex websocket frame".into());
        }
        serde_json::from_slice(&body).map_err(|e| format!("invalid Codex websocket JSON: {e}"))
    }
}

fn read_http_headers(socket: &mut UnixStream) -> Result<String, String> {
    let mut out = Vec::new();
    loop {
        let mut b = [0];
        socket.read_exact(&mut b).map_err(|e| e.to_string())?;
        out.push(b[0]);
        if out.ends_with(b"\r\n\r\n") {
            return String::from_utf8(out).map_err(|e| e.to_string());
        }
        if out.len() > 16384 {
            return Err("Codex app-server headers too large".into());
        }
    }
}
fn websocket_key() -> Result<String, String> {
    let mut nonce = [0; 16];
    fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut nonce))
        .map_err(|e| e.to_string())?;
    Ok(STANDARD.encode(nonce))
}
fn validate_websocket_upgrade(response: &str, key: &str) -> Result<(), String> {
    if !response.starts_with("HTTP/1.1 101") {
        return Err("Codex app-server websocket upgrade refused".into());
    }
    let mut digest = Sha1::new();
    digest.update(key.as_bytes());
    digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    let expected = STANDARD.encode(digest.finalize());
    response
        .lines()
        .any(|line| {
            line.split_once(':').is_some_and(|(n, v)| {
                n.eq_ignore_ascii_case("sec-websocket-accept") && v.trim() == expected
            })
        })
        .then_some(())
        .ok_or_else(|| "Codex websocket accept mismatch".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_flow::{
        Available_Data, EndpointSelection, FlowLifecycle, HarnessKind, OriginClue,
        Response as FlowResponse,
    };
    use std::{
        os::unix::{fs::PermissionsExt, net::UnixListener},
        sync::mpsc,
        thread,
    };
    use tempfile::tempdir;

    fn herdr_route() -> HerdrRoute {
        HerdrRoute {
            herdr_session_name: "s".into(),
            herdr_agent_name: "agent".into(),
            herdr_pane_id: "w1:p1".into(),
            herdr_terminal_id: "term".into(),
        }
    }

    fn flow_node(harness_kind: HarnessKind, route: HerdrRouteSelection) -> FlowNode {
        FlowNode {
            flow_id: "flow-a".into(),
            session_id: "session-a".into(),
            harness_kind,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: route,
            origin_clue: OriginClue {
                flow_id: "origin".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
            },
            flow_lifecycle: FlowLifecycle::Active,
        }
    }

    fn serve_resolutions(socket: &Path, nodes: Vec<FlowNode>) -> thread::JoinHandle<()> {
        let listener = UnixListener::bind(socket).unwrap();
        thread::spawn(move || {
            for node in nodes {
                let (mut peer, _) = listener.accept().unwrap();
                let mut length = [0; 4];
                peer.read_exact(&mut length).unwrap();
                let mut body = vec![0; u32::from_be_bytes(length) as usize];
                peer.read_exact(&mut body).unwrap();
                assert_eq!(
                    rkyv::from_bytes::<FlowQuery, rkyv::rancor::Error>(&body).unwrap(),
                    FlowQuery::ResolveRecipient("flow-a".into())
                );
                let response = FlowResponse::RecipientResolved(node);
                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response).unwrap();
                peer.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
                peer.write_all(&bytes).unwrap();
            }
        })
    }

    fn serve_resolution(socket: &Path, node: FlowNode) -> thread::JoinHandle<()> {
        serve_resolutions(socket, vec![node])
    }

    struct FakeHerdr<'fixture> {
        directory: &'fixture Path,
        harness: &'fixture str,
        status: Option<&'fixture str>,
        interactive_ready: bool,
        terminal: &'fixture str,
        post_terminal: Option<&'fixture str>,
        screen: &'fixture str,
        prompt_exit: i32,
    }

    impl FakeHerdr<'_> {
        fn write(&self) -> PathBuf {
            let program = self.directory.join("herdr");
            let log = self.directory.join("commands");
            let prompt = self.directory.join("prompt");
            let get_once = self.directory.join("get-once");
            let status = self
                .status
                .map(|value| format!(r#", \"agent_status\":\"{value}\""#))
                .unwrap_or_default();
            let script = r#"#!/bin/sh
printf '%s\n' "$4" >> '__LOG__'
case "$4" in
get)
  if [ -e '__GET_ONCE__' ]; then terminal='__POST_TERMINAL__'; else touch '__GET_ONCE__'; terminal='__TERMINAL__'; fi
  printf '%s\n' "{\"id\":\"cli:agent:get\",\"result\":{\"agent\":{\"agent\":\"__HARNESS__\",\"name\":\"agent\",\"pane_id\":\"w1:p1\",\"terminal_id\":\"$terminal\",\"interactive_ready\":__READY____STATUS__}}}" ;;
read) cat <<'SCREEN'
__SCREEN__
SCREEN
;;
prompt) printf '%s' "$6" >> '__PROMPT__'; exit __PROMPT_EXIT__ ;;
*) exit 9 ;;
esac
"#
            .replace("__LOG__", &log.display().to_string())
            .replace("__HARNESS__", self.harness)
            .replace("__GET_ONCE__", &get_once.display().to_string())
            .replace(
                "__POST_TERMINAL__",
                self.post_terminal.unwrap_or(self.terminal),
            )
            .replace("__TERMINAL__", self.terminal)
            .replace("__READY__", &self.interactive_ready.to_string())
            .replace("__STATUS__", &status)
            .replace("__SCREEN__", self.screen)
            .replace("__PROMPT__", &prompt.display().to_string())
            .replace("__PROMPT_EXIT__", &self.prompt_exit.to_string());
            std::fs::write(&program, script).unwrap();
            std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
            program
        }
    }

    fn peer_message() -> ClusterMessage {
        use signal_message::{PeerEnvelope, PeerSender};
        ClusterMessage::Peer(PeerEnvelope {
            peer_sender: PeerSender {
                flow_identifier: "source-flow".into(),
                session_identifier: "source-session".into(),
            },
            source_event_identifier: "event-1".into(),
            peer_source_path: "flows/source/reports/exact.md".into(),
            peer_body_sha256: "a1e4e331d40278d0c2c1fdf2cdabd1690682bd13c1fd49dadd40c9df3dc6d6ad"
                .into(),
            peer_body: "exact body".into(),
        })
    }

    #[test]
    fn herdr_guard_accepts_live_claude_and_codex_blank_composers() {
        let directory = tempdir().unwrap();
        let program = FakeHerdr {
            directory: directory.path(),
            harness: "claude",
            status: Some("idle"),
            interactive_ready: true,
            terminal: "term",
            post_terminal: None,
            screen: "✻ Brewed for 31s · done 8:03 PM\n\n──────── primary Psyche opus ────────\n❯\n────────────────────────────────────\n  primary main  Opus 5·medium\n  -- INSERT -- auto mode on",
            prompt_exit: 0,
        }
        .write();
        assert!(
            validate_herdr_composer(program.as_os_str(), &herdr_route(), &HarnessKind::Claude)
                .is_ok()
        );

        let program = FakeHerdr {
            directory: directory.path(),
            harness: "codex",
            status: Some("working"),
            interactive_ready: true,
            terminal: "term",
            post_terminal: None,
            screen: "prior output\n\n› Ask Codex to do anything\n\n\n  ? for shortcuts                       100% context left",
            prompt_exit: 0,
        }
        .write();
        assert!(
            validate_herdr_composer(program.as_os_str(), &herdr_route(), &HarnessKind::Codex)
                .is_ok()
        );
    }

    #[test]
    fn herdr_guard_refuses_busy_nonblank_wrong_terminal_unready_and_missing_status() {
        let cases = [
            (
                Some("waiting"),
                true,
                "term",
                "──────── title ────────\n❯\n────────────────\nfooter",
            ),
            (
                Some("idle"),
                true,
                "term",
                "──────── title ────────\n❯ living draft\n────────────────\nfooter",
            ),
            (
                Some("idle"),
                true,
                "other",
                "──────── title ────────\n❯\n────────────────\nfooter",
            ),
            (
                Some("idle"),
                false,
                "term",
                "──────── title ────────\n❯\n────────────────\nfooter",
            ),
            (
                None,
                true,
                "term",
                "──────── title ────────\n❯\n────────────────\nfooter",
            ),
        ];
        for (status, interactive_ready, terminal, screen) in cases {
            let directory = tempdir().unwrap();
            let program = FakeHerdr {
                directory: directory.path(),
                harness: "claude",
                status,
                interactive_ready,
                terminal,
                post_terminal: None,
                screen,
                prompt_exit: 0,
            }
            .write();
            assert!(
                validate_herdr_composer(program.as_os_str(), &herdr_route(), &HarnessKind::Claude)
                    .is_err()
            );
            let commands = std::fs::read_to_string(directory.path().join("commands")).unwrap();
            assert!(!commands.lines().any(|command| command == "prompt"));
        }
    }

    #[test]
    fn herdr_delivery_submits_the_canonical_datom_once() {
        let directory = tempdir().unwrap();
        let program = FakeHerdr {
            directory: directory.path(),
            harness: "claude",
            status: Some("idle"),
            interactive_ready: true,
            terminal: "term",
            post_terminal: None,
            screen: "──────── primary Psyche opus ────────\n❯\n────────────────────────────────────\n  primary main  Opus 5·medium\n  -- INSERT -- auto mode on",
            prompt_exit: 0,
        }
        .write();
        let route = herdr_route();
        let node = flow_node(
            HarnessKind::Claude,
            HerdrRouteSelection::Available(route.clone()),
        );
        let socket = directory.path().join("flow.sock");
        let flow = serve_resolution(&socket, node.clone());
        let message = peer_message();
        let adapter = LiveNexusDelivery::with_paths(&socket, &program);
        assert_eq!(
            adapter.deliver(&node, &message).unwrap(),
            ReceiptKind::Accepted
        );
        flow.join().unwrap();
        assert_eq!(
            std::fs::read_to_string(directory.path().join("commands")).unwrap(),
            "get\nread\nprompt\nget\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("prompt")).unwrap(),
            crate::text::write(&message)
        );
    }

    #[test]
    fn changed_or_unavailable_flow_revalidation_never_invokes_herdr() {
        for changed in [
            flow_node(HarnessKind::Claude, HerdrRouteSelection::Unavailable),
            flow_node(
                HarnessKind::Claude,
                HerdrRouteSelection::Available(HerdrRoute {
                    herdr_terminal_id: "replacement".into(),
                    ..herdr_route()
                }),
            ),
        ] {
            let directory = tempdir().unwrap();
            let program = FakeHerdr {
                directory: directory.path(),
                harness: "claude",
                status: Some("idle"),
                interactive_ready: true,
                terminal: "term",
                post_terminal: None,
                screen: "──────── title ────────\n❯\n────────────────\nfooter",
                prompt_exit: 0,
            }
            .write();
            let node = flow_node(
                HarnessKind::Claude,
                HerdrRouteSelection::Available(herdr_route()),
            );
            let socket = directory.path().join("flow.sock");
            let flow = serve_resolution(&socket, changed);
            let adapter = LiveNexusDelivery::with_paths(&socket, &program);
            assert!(adapter.deliver(&node, &peer_message()).is_err());
            flow.join().unwrap();
            assert!(!directory.path().join("commands").exists());
        }
    }

    #[test]
    fn stale_herdr_route_with_parked_native_endpoint_never_falls_back() {
        let directory = tempdir().unwrap();
        let node = FlowNode {
            endpoint_selection: EndpointSelection::Available(Available_Data {
                endpoint_path: directory
                    .path()
                    .join("absent-native.sock")
                    .display()
                    .to_string(),
                route_readiness: RouteReadiness::Parked,
            }),
            ..flow_node(HarnessKind::Codex, HerdrRouteSelection::Unavailable)
        };
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&node).unwrap();
        let resolved = rkyv::from_bytes::<FlowNode, rkyv::rancor::Error>(&archive).unwrap();
        let adapter = LiveNexusDelivery::with_paths(
            directory.path().join("unused-flow.sock"),
            directory.path().join("unused-herdr"),
        );

        assert_eq!(
            adapter.deliver(&resolved, &peer_message()).unwrap(),
            ReceiptKind::Parked
        );
        assert!(!directory.path().join("absent-native.sock").exists());
    }

    #[test]
    fn accepted_and_ambiguous_parked_v6_rows_reopen_without_retry() {
        use crate::{MessageEngine, MessengerTables, OriginPolicy};
        use signal_message::{DeliveryRequest, Query, Response};
        use triad_runtime::{ConnectionContext, UnixCredentials};

        for (prompt_exit, expected, expected_commands) in [
            (0, ReceiptKind::Accepted, "get\nread\nprompt\nget\n"),
            (23, ReceiptKind::Parked, "get\nread\nprompt\n"),
        ] {
            let directory = tempdir().unwrap();
            let program = FakeHerdr {
                directory: directory.path(),
                harness: "claude",
                status: Some("working"),
                interactive_ready: true,
                terminal: "term",
                post_terminal: None,
                screen: "──────── primary Psyche opus ────────\n❯\n────────────────────────────────────\n  primary main  Opus 5·medium\n  -- INSERT -- auto mode on",
                prompt_exit,
            }
            .write();
            let node = flow_node(
                HarnessKind::Claude,
                HerdrRouteSelection::Available(herdr_route()),
            );
            let socket = directory.path().join("flow.sock");
            let flow = serve_resolutions(&socket, vec![node.clone(), node]);
            let store = directory.path().join("messenger.sema");
            let source_event_identifier = format!("durable-{prompt_exit}");
            let mut cluster_message = peer_message();
            let ClusterMessage::Peer(peer) = &mut cluster_message else {
                unreachable!()
            };
            peer.source_event_identifier = source_event_identifier.clone();
            let request = DeliveryRequest {
                source_event_identifier,
                cluster_message,
                target_flows: vec!["flow-a".into()],
            };
            let connection = ConnectionContext::from(UnixCredentials::new(
                1000,
                1000,
                std::process::id() as i32,
            ));
            let runtime = tokio::runtime::Runtime::new().unwrap();
            {
                let mut engine = MessageEngine::new(
                    MessengerTables::open(&store).unwrap(),
                    OriginPolicy::for_owner_user_id(1000, "owner"),
                )
                .with_nexus_delivery(LiveNexusDelivery::with_paths(&socket, &program));
                let response = runtime
                    .block_on(engine.handle(Query::Deliver(request.clone()), &connection))
                    .unwrap();
                let Response::DeliveryRecorded(report) = response else {
                    panic!("delivery was not durably recorded: {response:?}")
                };
                assert_eq!(report.recipient_receipts[0].receipt_kind, expected);
            }
            flow.join().unwrap();
            assert_eq!(
                std::fs::read_to_string(directory.path().join("commands")).unwrap(),
                expected_commands
            );

            std::fs::remove_file(&socket).ok();
            let mut reopened = MessageEngine::new(
                MessengerTables::open(&store).unwrap(),
                OriginPolicy::for_owner_user_id(1000, "owner"),
            )
            .with_nexus_delivery(LiveNexusDelivery::with_paths(&socket, &program));
            let Response::DeliveryRecorded(report) = runtime
                .block_on(reopened.handle(Query::Deliver(request), &connection))
                .unwrap()
            else {
                panic!("reopened delivery was not found")
            };
            assert_eq!(report.recipient_receipts[0].receipt_kind, expected);
            assert_eq!(
                std::fs::read_to_string(directory.path().join("commands")).unwrap(),
                expected_commands,
                "reopening and repeating a source event must not prompt again"
            );
        }
    }

    #[test]
    fn identity_replacement_after_prompt_is_an_uncertain_failure() {
        let directory = tempdir().unwrap();
        let program = FakeHerdr {
            directory: directory.path(),
            harness: "claude",
            status: Some("working"),
            interactive_ready: true,
            terminal: "term",
            post_terminal: Some("replacement"),
            screen: "──────── primary Psyche opus ────────\n❯\n────────────────────────────────────\n  primary main  Opus 5·medium",
            prompt_exit: 0,
        }
        .write();
        let node = flow_node(
            HarnessKind::Claude,
            HerdrRouteSelection::Available(herdr_route()),
        );
        let socket = directory.path().join("flow.sock");
        let flow = serve_resolution(&socket, node.clone());
        let adapter = LiveNexusDelivery::with_paths(&socket, &program);
        assert!(adapter.deliver(&node, &peer_message()).is_err());
        flow.join().unwrap();
        assert_eq!(
            std::fs::read_to_string(directory.path().join("commands")).unwrap(),
            "get\nread\nprompt\nget\n"
        );
    }

    #[test]
    fn flow_resolution_and_claude_delivery_use_direct_protocols() {
        let directory = tempdir().unwrap();
        let flow_socket = directory.path().join("flow.sock");
        let flow_listener = UnixListener::bind(&flow_socket).unwrap();
        let flow = thread::spawn(move || {
            let (mut peer, _) = flow_listener.accept().unwrap();
            let mut length = [0; 4];
            peer.read_exact(&mut length).unwrap();
            let mut body = vec![0; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut body).unwrap();
            assert_eq!(
                rkyv::from_bytes::<FlowQuery, rkyv::rancor::Error>(&body).unwrap(),
                FlowQuery::ResolveRecipient("da1e3f".into())
            );
            let response = FlowResponse::RecipientResolved(FlowNode {
                flow_id: "da1e3f".into(),
                session_id: "da1e3f9d-full".into(),
                harness_kind: HarnessKind::Claude,
                endpoint_selection: EndpointSelection::Available(Available_Data {
                    endpoint_path: "/tmp/control.sock".into(),
                    route_readiness: RouteReadiness::Ready,
                }),
                herdr_route_selection: HerdrRouteSelection::Unavailable,
                origin_clue: OriginClue {
                    flow_id: "origin".into(),
                    session_id: "session".into(),
                    turn_id: "turn".into(),
                },
                flow_lifecycle: FlowLifecycle::Active,
            });
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response).unwrap();
            peer.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
            peer.write_all(&bytes).unwrap();
        });
        let node = FlowResolver::from_path(flow_socket)
            .resolve("da1e3f")
            .unwrap()
            .unwrap();
        flow.join().unwrap();
        assert_eq!(node.harness_kind, HarnessKind::Claude);

        let control_socket = directory.path().join("control.sock");
        let control_listener = UnixListener::bind(&control_socket).unwrap();
        let (sent, received) = mpsc::channel();
        let control = thread::spawn(move || {
            let (mut peer, _) = control_listener.accept().unwrap();
            let mut attach = String::new();
            BufReader::new(peer.try_clone().unwrap())
                .read_line(&mut attach)
                .unwrap();
            let attach: serde_json::Value = serde_json::from_str(&attach).unwrap();
            assert_eq!(attach["short"], "da1e3f9d");
            assert_eq!(attach["auth"], "test-secret");
            peer.write_all(b"{\"ok\":true}\n").unwrap();
            let mut paste = Vec::new();
            peer.read_to_end(&mut paste).unwrap();
            sent.send(paste).unwrap();
        });
        deliver_claude_with_key(
            &control_socket,
            "da1e3f9d-full",
            "Peer.{ exact datom }",
            "test-secret",
            || Ok(()),
        )
        .unwrap();
        control.join().unwrap();
        assert_eq!(
            received.recv().unwrap(),
            b"\x1b[200~Peer.{ exact datom }\x1b[201~\r"
        );
    }
}
