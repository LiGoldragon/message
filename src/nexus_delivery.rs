//! Direct Message Nexus delivery through Flow resolution and harness protocols.
//!
//! This is deliberately inside the daemon. The ordinary CLI only sends one
//! typed Message Signal; it never selects or executes a harness bridge.

use std::{
    env, fs,
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

pub fn deliver(node: &FlowNode, message: &ClusterMessage) -> Result<ReceiptKind, String> {
    if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection {
        let datom = crate::text::write(message);
        deliver_herdr(node, route, &datom)?;
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

/// Submits only a Flow-resolved, ready Herdr route.  Flow owns the live
/// identity and blank-composer witness; Message repeats resolution immediately
/// before the single prompt call so replacement or stale routes receive no input.
fn deliver_herdr(node: &FlowNode, route: &HerdrRoute, datom: &str) -> Result<(), String> {
    let Some(current) = FlowResolver::conventional().resolve(&node.flow_id)? else {
        return Err("Flow no longer resolves the Herdr recipient".into());
    };
    if current.flow_id != node.flow_id
        || current.session_id != node.session_id
        || current.harness_kind != node.harness_kind
        || current.herdr_route_selection != HerdrRouteSelection::Available(route.clone())
    {
        return Err("Herdr recipient changed or is no longer ready".into());
    }
    let program = env::var_os("MESSAGE_HERDR_PROGRAM").unwrap_or_else(|| "herdr".into());
    validate_herdr_composer(&program, route, &node.harness_kind)?;
    let status = Command::new(&program)
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
        Ok(())
    } else {
        Err("Herdr prompt refused or outcome is uncertain".into())
    }
}

fn validate_herdr_composer(
    program: &std::ffi::OsStr,
    route: &HerdrRoute,
    harness: &HarnessKind,
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
        || agent
            .get("agent_status")
            .and_then(serde_json::Value::as_str)
            != Some("idle")
    {
        return Err("Herdr recipient is not the registered idle agent".into());
    }
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
    let blank_prompt = screen.lines().last().is_some_and(|line| line.trim() == "❯");
    if !blank_prompt {
        return Err("Herdr composer is not a supported blank prompt".into());
    }
    Ok(())
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
    use std::{os::unix::net::UnixListener, sync::mpsc, thread};
    use tempfile::tempdir;

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
