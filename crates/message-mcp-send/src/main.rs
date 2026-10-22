//! Stdio MCP adapter. Transport-stamped thread ids and SO_PEERCRED identify
//! the sender; progress notifications carry the durable Flow status stream.
use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use protos::{Compactable, Protosizable, ReaderBudget};
use serde_json::{Value, json};
use signal_flow::DeliveryState;
use signal_message::{Query, Response, SendThread_Data};
use std::{
    collections::{HashMap, HashSet},
    env,
    io::{self, BufRead, Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

const MAX_LINE: u64 = 1024 * 1024;
const MAX_ACTIVE_CALLS: usize = 16;
const MAX_SEEN_CALLS: usize = 65_536;

#[derive(Clone)]
struct Sender {
    socket: String,
}

trait ConnectsToMessage {
    fn connect(&self) -> Result<UnixStream, String>;
    fn send(&self, peer: &mut UnixStream, query: &Query) -> Result<(), String>;
}

impl ConnectsToMessage for Sender {
    fn connect(&self) -> Result<UnixStream, String> {
        let peer = UnixStream::connect(&self.socket).map_err(|e| e.to_string())?;
        peer.set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| e.to_string())?;
        Ok(peer)
    }

    fn send(&self, peer: &mut UnixStream, query: &Query) -> Result<(), String> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        let length = u32::try_from(bytes.len()).map_err(|_| "oversized Message frame")?;
        peer.write_all(&length.to_be_bytes())
            .and_then(|()| peer.write_all(&bytes))
            .map_err(|e| format!("Uncertain: Send outcome may be unknown; do not retry: {e}"))?;
        Ok(())
    }
}

struct ActiveCall {
    observer: Option<UnixStream>,
    cancelled: Arc<AtomicBool>,
}

struct CallRequest {
    id: Value,
    key: String,
    progress_token: Value,
    query: Query,
}

struct Adapter {
    initialized: bool,
    ready: bool,
    seen_ids: HashSet<String>,
}

trait ServesMcpRequests {
    fn new() -> Self;
    fn call_request(&mut self, input: &Value) -> Result<CallRequest, Value>;
    fn thread_query(params: &Value) -> Result<Query, String>;
    fn synchronous(&mut self, input: &Value) -> Option<Value>;
}

impl ServesMcpRequests for Adapter {
    fn new() -> Self {
        Self {
            initialized: false,
            ready: false,
            seen_ids: HashSet::new(),
        }
    }

    fn call_request(&mut self, input: &Value) -> Result<CallRequest, Value> {
        let id = input
            .get("id")
            .filter(|id| id.is_string() || id.is_number())
            .cloned()
            .ok_or_else(|| Value::rpc_error(Value::Null, -32600, "Invalid request id"))?;
        let key = id.to_string();
        if self.seen_ids.contains(&key) {
            return Err(Value::rpc_error(
                id,
                -32600,
                "Request id has already been used",
            ));
        }
        if self.seen_ids.len() >= MAX_SEEN_CALLS {
            return Err(Value::rpc_error(
                id,
                -32000,
                "Session request-id limit reached; restart this MCP process",
            ));
        }
        if !self.ready {
            return Err(Value::rpc_error(
                id,
                -32002,
                "MCP initialization is incomplete",
            ));
        }
        let params = input.get("params").unwrap_or(&Value::Null);
        let progress_token = params
            .pointer("/_meta/progressToken")
            .filter(|token| token.is_string() || token.is_number())
            .cloned()
            .ok_or_else(|| {
                Value::rpc_error(
                    id.clone(),
                    -32602,
                    "Send requires _meta.progressToken so delivery status can be streamed",
                )
            })?;
        let query = Self::thread_query(params)
            .map_err(|message| Value::rpc_error(id.clone(), -32602, &message))?;
        self.seen_ids.insert(key.clone());
        Ok(CallRequest {
            id,
            key,
            progress_token,
            query,
        })
    }

    fn thread_query(params: &Value) -> Result<Query, String> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or("Missing tool name")?;
        let args = params
            .get("arguments")
            .and_then(Value::as_object)
            .ok_or("Missing arguments")?;
        let thread = params
            .pointer("/_meta/threadId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control))
            .ok_or("Missing or invalid harness-stamped _meta.threadId")?;
        match name {
            "send" => {
                if args.len() != 1 || !args.contains_key("query") {
                    return Err(
                        "Send accepts only query; sender, process and thread are transport-owned"
                            .into(),
                    );
                }
                let text = args
                    .get("query")
                    .and_then(Value::as_str)
                    .ok_or("query must be a Send datom")?;
                let mut budget = Budget {
                    remaining: 65_536,
                    reader: ReaderBudget { remaining: 65_536 },
                    depth: 0,
                    maximum_depth: 1_024,
                };
                let query = Potential::<Query>::from(text.to_owned())
                    .actualize(&mut budget)
                    .map_err(|e| format!("Invalid Send datom: {e:?}"))?;
                let Query::Send(request) = query else {
                    return Err(
                        "Tool accepts Send only; no Configure or caller-supplied SendThread".into(),
                    );
                };
                Ok(Query::SendThread(SendThread_Data {
                    send_request: request,
                    native_session_id: thread.to_owned(),
                }))
            }
            "observe_sends" => {
                if args.len() > 1 || args.keys().any(|key| key != "cursor") {
                    return Err("observe_sends accepts only an optional cursor".into());
                }
                let cursor = args
                    .get("cursor")
                    .map(|value| value.as_i64().ok_or("cursor must be an integer"))
                    .transpose()?;
                Ok(Query::ObserveThread(signal_message::ObserveThread_Data {
                    delivery_cursor_option: cursor,
                    native_session_id: thread.to_owned(),
                }))
            }
            _ => Err("Unknown tool".into()),
        }
    }

    fn synchronous(&mut self, input: &Value) -> Option<Value> {
        let id = input.get("id").cloned();
        let method = input.get("method").and_then(Value::as_str);
        if input.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Some(Value::rpc_error(
                id.unwrap_or(Value::Null),
                -32600,
                "Invalid Request",
            ));
        }
        if id.is_none() {
            if method == Some("notifications/initialized") && self.initialized {
                self.ready = true;
            }
            return None;
        }
        let id = id.unwrap();
        if !id.is_string() && !id.is_number() {
            return Some(Value::rpc_error(Value::Null, -32600, "Invalid request id"));
        }
        let result = match method {
            Some("initialize") if !self.initialized => {
                self.initialized = true;
                Ok(
                    json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"message-mcp-send","version":env!("CARGO_PKG_VERSION")}}),
                )
            }
            Some("ping") => Ok(json!({})),
            _ if !self.ready => Err((-32002, "MCP initialization is incomplete")),
            Some("tools/list") => Ok(
                json!({"tools":[{"name":"send","description":"Submit a Send and stream durable delivery status until terminal state. Cancellation stops observation, not the queued delivery.","inputSchema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}},{"name":"observe_sends","description":"Resume this authenticated sender's durable delivery stream from an optional cursor. The harness supplies threadId.","inputSchema":{"type":"object","properties":{"cursor":{"type":"integer"}},"additionalProperties":false}}]}),
            ),
            Some("tools/call") => return None,
            _ => Err((-32601, "Method not found")),
        };
        Some(match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err((code, message)) => Value::rpc_error(id, code, message),
        })
    }
}

trait BuildsRpcErrors {
    fn rpc_error(id: Value, code: i64, message: &str) -> Self;
}

impl BuildsRpcErrors for Value {
    fn rpc_error(id: Value, code: i64, message: &str) -> Self {
        json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
    }
}

trait WritesJsonLines {
    fn write_json(&self, value: &Value) -> io::Result<()>;
}

impl WritesJsonLines for Arc<Mutex<Box<dyn Write + Send>>> {
    fn write_json(&self, value: &Value) -> io::Result<()> {
        let mut output = self.lock().unwrap();
        writeln!(output, "{value}")?;
        output.flush()
    }
}

trait ReadsMessageResponses {
    fn read_response(&mut self) -> Result<Response, String>;
}

impl ReadsMessageResponses for UnixStream {
    fn read_response(&mut self) -> Result<Response, String> {
        let mut length = [0; 4];
        self.read_exact(&mut length)
            .map_err(|e| format!("Message stream ended: {e}"))?;
        let length = u32::from_be_bytes(length) as usize;
        if length > MAX_LINE as usize {
            return Err("oversized Message response".into());
        }
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes)
            .map_err(|e| format!("incomplete Message response: {e}"))?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&bytes)
            .map_err(|e| format!("invalid Message response: {e}"))
    }
}

trait IdentifiesTerminalResponse {
    fn terminal_for(&self, accepted_id: Option<&str>) -> bool;
}

impl IdentifiesTerminalResponse for Response {
    fn terminal_for(&self, accepted_id: Option<&str>) -> bool {
        match self {
            Response::Sent(_) | Response::Failed(_) => true,
            Response::SendAccepted(_) => false,
            Response::SendsObserved(observation) => accepted_id.is_some_and(|id| {
                observation.delivery_snapshot_vector.iter().any(|snapshot| {
                    snapshot.logical_delivery_id == id
                        && matches!(
                            snapshot.delivery_state,
                            DeliveryState::Delivered(_) | DeliveryState::Failed(_)
                        )
                })
            }),
            Response::SendChanged(snapshot) => {
                accepted_id == Some(snapshot.logical_delivery_id.as_str())
                    && matches!(
                        snapshot.delivery_state,
                        DeliveryState::Delivered(_) | DeliveryState::Failed(_)
                    )
            }
            Response::ObservationInterrupted(_) | Response::Refused(_) | Response::Configured => {
                true
            }
        }
    }
}

trait RunsMcpCall {
    fn run(
        self,
        sender: Sender,
        cancelled: Arc<AtomicBool>,
        output: Arc<Mutex<Box<dyn Write + Send>>>,
        active: Arc<Mutex<HashMap<String, ActiveCall>>>,
        connection_alive: Arc<AtomicBool>,
    );
}

impl RunsMcpCall for CallRequest {
    fn run(
        self,
        sender: Sender,
        cancelled: Arc<AtomicBool>,
        output: Arc<Mutex<Box<dyn Write + Send>>>,
        active: Arc<Mutex<HashMap<String, ActiveCall>>>,
        connection_alive: Arc<AtomicBool>,
    ) {
        let call = self;
        if cancelled.load(Ordering::Acquire) {
            if connection_alive.load(Ordering::Acquire) {
                let _ = output.write_json(&Value::rpc_error(
                        call.id.clone(),
                        -32800,
                        "Send admission outcome is unknown; do not resend. Resume with observe_sends using this sender's durable delivery cursor.",
                    ),
                );
            }
            active.lock().unwrap().remove(&call.key);
            return;
        }
        let mut peer = match sender.connect() {
            Ok(peer) => peer,
            Err(error) => {
                if connection_alive.load(Ordering::Acquire) {
                    let message = if cancelled.load(Ordering::Acquire) {
                        "Send admission outcome is unknown; do not resend. Resume with observe_sends from this sender's durable history.".to_owned()
                    } else {
                        error
                    };
                    let _ = output.write_json(&Value::rpc_error(
                        call.id.clone(),
                        if cancelled.load(Ordering::Acquire) {
                            -32800
                        } else {
                            -32001
                        },
                        &message,
                    ));
                }
                active.lock().unwrap().remove(&call.key);
                return;
            }
        };
        let observer = match peer.try_clone() {
            Ok(observer) => observer,
            Err(error) => {
                if connection_alive.load(Ordering::Acquire) {
                    let _ = output.write_json(&Value::rpc_error(
                        call.id.clone(),
                        -32001,
                        &format!("Send outcome unknown; observer could not be registered: {error}"),
                    ));
                }
                active.lock().unwrap().remove(&call.key);
                return;
            }
        };
        if let Some(entry) = active.lock().unwrap().get_mut(&call.key) {
            entry.observer = Some(observer);
        }
        if cancelled.load(Ordering::Acquire) {
            let _ = peer.shutdown(std::net::Shutdown::Both);
        } else if let Err(error) = sender.send(&mut peer, &call.query) {
            if connection_alive.load(Ordering::Acquire) {
                let message = if cancelled.load(Ordering::Acquire) {
                    "Send admission outcome is unknown; do not resend. Resume with observe_sends from this sender's durable history.".to_owned()
                } else {
                    error
                };
                let _ = output.write_json(&Value::rpc_error(
                    call.id.clone(),
                    if cancelled.load(Ordering::Acquire) {
                        -32800
                    } else {
                        -32001
                    },
                    &message,
                ));
            }
            active.lock().unwrap().remove(&call.key);
            return;
        }
        let mut accepted_id: Option<String> = None;
        let mut last_cursor: Option<i64> = None;
        let mut progress = 0_u64;
        let mut final_response: Option<Value> = None;
        loop {
            if cancelled.load(Ordering::Acquire) {
                break;
            }
            match peer.read_response() {
                Ok(response) => {
                    match &response {
                        Response::SendAccepted(accepted) => {
                            accepted_id = Some(accepted.logical_delivery_id.clone());
                            last_cursor = Some(accepted.delivery_cursor);
                        }
                        Response::SendsObserved(observation) => {
                            last_cursor = Some(observation.delivery_cursor)
                        }
                        Response::SendChanged(snapshot) => {
                            last_cursor = Some(snapshot.delivery_cursor)
                        }
                        _ => {}
                    }
                    let text = response.datomize(vec![]).protosize().compact();
                    progress += 1;
                    let notification = json!({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":call.progress_token.clone(),"progress":progress,"message":text}});
                    if output.write_json(&notification).is_err() {
                        cancelled.store(true, Ordering::Release);
                        connection_alive.store(false, Ordering::Release);
                        let _ = peer.shutdown(std::net::Shutdown::Both);
                        break;
                    }
                    if response.terminal_for(accepted_id.as_deref()) {
                        let failed = matches!(
                            response,
                            Response::Failed(_)
                                | Response::Refused(_)
                                | Response::ObservationInterrupted(_)
                        ) || matches!(&response, Response::SendChanged(snapshot) if matches!(snapshot.delivery_state, DeliveryState::Failed(_)))
                            || matches!(&response, Response::SendsObserved(observation) if observation.delivery_snapshot_vector.iter().any(|snapshot| accepted_id.as_deref() == Some(snapshot.logical_delivery_id.as_str()) && matches!(snapshot.delivery_state, DeliveryState::Failed(_))));
                        if matches!(response, Response::ObservationInterrupted(_)) {
                            let cursor = last_cursor
                                .map_or_else(|| "none".to_owned(), |cursor| cursor.to_string());
                            final_response = Some(Value::rpc_error(
                                call.id.clone(),
                                -32001,
                                &format!(
                                    "Observation interrupted at durable cursor {cursor}; delivery state is not Failed. Resume with observe_sends from this cursor."
                                ),
                            ));
                        } else {
                            final_response = Some(
                                json!({"jsonrpc":"2.0","id":call.id,"result":{"content":[{"type":"text","text":text}],"isError":failed}}),
                            );
                        }
                        break;
                    }
                }
                Err(error) => {
                    if !cancelled.load(Ordering::Acquire) {
                        let message = if accepted_id.is_some() {
                            format!(
                                "Observation interrupted; resume from the last progress cursor. Durable delivery was not canceled: {error}"
                            )
                        } else {
                            format!(
                                "Send admission outcome is unknown; do not retry blindly. Observe sender deliveries before another Send: {error}"
                            )
                        };
                        final_response = Some(Value::rpc_error(call.id.clone(), -32001, &message));
                    }
                    break;
                }
            }
        }
        active.lock().unwrap().remove(&call.key);
        if final_response.is_none()
            && cancelled.load(Ordering::Acquire)
            && connection_alive.load(Ordering::Acquire)
        {
            let message = match last_cursor {
            Some(cursor) => format!("Observation cancelled at durable cursor {cursor}; delivery was not cancelled. Resume from this cursor."),
            None => "Send admission was cancelled before acceptance and its outcome is unknown; do not resend blindly. Observe sender deliveries before another Send.".to_owned(),
        };
            final_response = Some(Value::rpc_error(call.id.clone(), -32800, &message));
        }
        if let Some(response) = final_response {
            let _ = output.write_json(&response);
        }
    }
}

trait ServesMcpStdio {
    fn run<R: BufRead>(&self, input: R, output: Arc<Mutex<Box<dyn Write + Send>>>) -> ExitCode;
}

impl ServesMcpStdio for Sender {
    fn run<R: BufRead>(&self, mut input: R, output: Arc<Mutex<Box<dyn Write + Send>>>) -> ExitCode {
        let sender = self.clone();
        let mut adapter = Adapter::new();
        let connection_alive = Arc::new(AtomicBool::new(true));
        let active: Arc<Mutex<HashMap<String, ActiveCall>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut workers: Vec<JoinHandle<()>> = Vec::new();
        loop {
            let mut line = String::new();
            match (&mut input).take(MAX_LINE + 1).read_line(&mut line) {
                Ok(0) => {
                    connection_alive.store(false, Ordering::Release);
                    break;
                }
                Ok(_) if line.len() as u64 > MAX_LINE => {
                    eprintln!("message-mcp-send: oversized input; no dispatch");
                    break;
                }
                Err(error) => {
                    eprintln!("message-mcp-send: {error}");
                    connection_alive.store(false, Ordering::Release);
                    break;
                }
                _ => {}
            }
            let input_value: Value = match serde_json::from_str(&line) {
                Ok(value) => value,
                Err(_) => {
                    if output.write_json(&json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}})).is_err() { connection_alive.store(false, Ordering::Release); break; }
                    continue;
                }
            };
            if input_value.get("method").and_then(Value::as_str) == Some("notifications/cancelled")
            {
                let request_id = input_value
                    .pointer("/params/requestId")
                    .map(Value::to_string);
                if let Some(request_id) = request_id
                    && let Some(call) = active.lock().unwrap().get(&request_id)
                {
                    call.cancelled.store(true, Ordering::Release);
                    if let Some(observer) = &call.observer {
                        let _ = observer.shutdown(std::net::Shutdown::Both);
                    }
                }
                continue;
            }
            if input_value.get("method").and_then(Value::as_str) == Some("tools/call") {
                let call = match adapter.call_request(&input_value) {
                    Ok(call) => call,
                    Err(response) => {
                        if output.write_json(&response).is_err() {
                            connection_alive.store(false, Ordering::Release);
                            break;
                        } else {
                            continue;
                        }
                    }
                };
                if active.lock().unwrap().len() >= MAX_ACTIVE_CALLS {
                    let response =
                        Value::rpc_error(call.id, -32000, "Too many active Send observers");
                    if output.write_json(&response).is_err() {
                        connection_alive.store(false, Ordering::Release);
                        break;
                    }
                    continue;
                }
                let cancelled = Arc::new(AtomicBool::new(false));
                active.lock().unwrap().insert(
                    call.key.clone(),
                    ActiveCall {
                        observer: None,
                        cancelled: cancelled.clone(),
                    },
                );
                let sender = sender.clone();
                let output = output.clone();
                let active = active.clone();
                let connection_alive = connection_alive.clone();
                workers.push(std::thread::spawn(move || {
                    call.run(sender, cancelled, output, active, connection_alive)
                }));
            } else if let Some(response) = adapter.synchronous(&input_value)
                && output.write_json(&response).is_err()
            {
                connection_alive.store(false, Ordering::Release);
                break;
            }
            let mut index = 0;
            while index < workers.len() {
                if workers[index].is_finished() {
                    let worker = workers.swap_remove(index);
                    let _ = worker.join();
                } else {
                    index += 1;
                }
            }
        }
        connection_alive.store(false, Ordering::Release);
        for call in active.lock().unwrap().values() {
            call.cancelled.store(true, Ordering::Release);
            if let Some(observer) = &call.observer {
                let _ = observer.shutdown(std::net::Shutdown::Both);
            }
        }
        for worker in workers {
            let _ = worker.join();
        }
        ExitCode::SUCCESS
    }
}

fn main() -> ExitCode {
    let sender = Sender {
        socket: env::var("MESSAGE_SOCKET").unwrap_or_else(|_| {
            DefaultConfiguration::from_environment()
                .ordinary_socket_path()
                .to_string_lossy()
                .into_owned()
        }),
    };
    let output: Arc<Mutex<Box<dyn Write + Send>>> = Arc::new(Mutex::new(Box::new(io::stdout())));
    sender.run(io::stdin().lock(), output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn send_call() -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"send","arguments":{"query":"Send.{ Address.{ Mind flow Secondary } Wake MiddleAbrupt.Question.«whole body» }"},"_meta":{"threadId":"recorded-thread","progressToken":"token-1"}}})
    }

    #[test]
    fn missing_progress_token_is_refused_before_admission() {
        let mut input = send_call();
        input["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove("progressToken");
        let mut adapter = Adapter::new();
        adapter.ready = true;
        assert!(adapter.call_request(&input).is_err());
    }

    #[test]
    fn missing_progress_token_never_opens_a_message_send() {
        use std::os::unix::net::UnixListener;

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("message.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut call = send_call();
        call["params"]["_meta"]
            .as_object_mut()
            .unwrap()
            .remove("progressToken");
        let input = format!(
            "{}\n{}\n{}\n",
            json!({"jsonrpc":"2.0","id":1,"method":"initialize"}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            call,
        );
        let output_state = Arc::new((Mutex::new(Vec::new()), std::sync::Condvar::new()));
        let output: Arc<Mutex<Box<dyn Write + Send>>> =
            Arc::new(Mutex::new(Box::new(SharedOutput(output_state.clone()))));
        Sender {
            socket: socket.to_string_lossy().into_owned(),
        }
        .run(io::Cursor::new(input), output);
        let response = String::from_utf8(output_state.0.lock().unwrap().clone()).unwrap();
        assert!(response.contains("progressToken"));
        assert!(
            matches!(listener.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock)
        );
    }

    #[test]
    fn thread_query_carries_the_exact_request_and_transport_thread() {
        let query = Adapter::thread_query(&send_call()["params"]).unwrap();
        let Query::SendThread(request) = query else {
            panic!("thread send expected")
        };
        assert_eq!(request.native_session_id, "recorded-thread");
        assert_eq!(
            request.send_request.message,
            signal_flow::Message::MiddleAbrupt(signal_flow::Request::Question("whole body".into()))
        );
    }

    #[test]
    fn observe_sends_resumes_from_the_supplied_durable_cursor() {
        let params = json!({"name":"observe_sends","arguments":{"cursor":7},"_meta":{"threadId":"recorded-thread"}});
        let Query::ObserveThread(request) = Adapter::thread_query(&params).unwrap() else {
            panic!("thread-scoped durable observation expected");
        };
        assert_eq!(request.native_session_id, "recorded-thread");
        assert_eq!(request.delivery_cursor_option, Some(7));
    }

    #[derive(Clone)]
    struct SharedOutput(Arc<(Mutex<Vec<u8>>, std::sync::Condvar)>);

    impl Write for SharedOutput {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let (buffer, changed) = &*self.0;
            buffer.lock().unwrap().extend_from_slice(bytes);
            changed.notify_all();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn write_frame(stream: &mut UnixStream, response: &Response) {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(response).unwrap();
        stream
            .write_all(&(bytes.len() as u32).to_be_bytes())
            .unwrap();
        stream.write_all(&bytes).unwrap();
    }

    fn wait_for_output(output: &Arc<(Mutex<Vec<u8>>, std::sync::Condvar)>, needle: &str) -> String {
        let (buffer, changed) = &**output;
        let mut bytes = buffer.lock().unwrap();
        loop {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            if text
                .find(needle)
                .is_some_and(|index| text[index..].contains('\n'))
            {
                return text;
            }
            let (next, timeout) = changed.wait_timeout(bytes, Duration::from_secs(5)).unwrap();
            bytes = next;
            assert!(
                !timeout.timed_out(),
                "timed out waiting for MCP output: {needle}"
            );
        }
    }

    fn final_count(output: &str, id: Value) -> usize {
        output
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .filter(|message| message.get("id") == Some(&id))
            .count()
    }

    fn wait_for_id(output: &Arc<(Mutex<Vec<u8>>, std::sync::Condvar)>, id: Value) -> String {
        let (buffer, changed) = &**output;
        let mut bytes = buffer.lock().unwrap();
        loop {
            let text = String::from_utf8_lossy(&bytes).into_owned();
            if final_count(&text, id.clone()) > 0 {
                return text;
            }
            let (next, timeout) = changed.wait_timeout(bytes, Duration::from_secs(5)).unwrap();
            bytes = next;
            assert!(
                !timeout.timed_out(),
                "timed out waiting for final JSON-RPC response: {text}"
            );
        }
    }

    #[test]
    fn queued_progress_and_concurrent_cancel_stop_only_observation() {
        use signal_flow::{DeliveryState, SendAcceptance};
        use std::os::unix::net::UnixListener;

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("message.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            let mut length = [0; 4];
            peer.read_exact(&mut length).unwrap();
            let mut query = vec![0; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut query).unwrap();
            let _: Query = rkyv::from_bytes::<Query, rkyv::rancor::Error>(&query).unwrap();
            write_frame(
                &mut peer,
                &Response::SendAccepted(SendAcceptance {
                    logical_delivery_id: "queued-1".into(),
                    delivery_cursor: 7,
                    delivery_state: DeliveryState::Queued,
                }),
            );
            // Cancellation closes this observation connection. The accepted
            // durable send remains queued; the client never sends a cancel to Flow.
            let mut byte = [0; 1];
            let _ = peer.read(&mut byte);
        });

        let input = json!({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"send","arguments":{"query":"Send.{ Address.{ Mind flow Secondary } Wake MiddleAbrupt.Question.«whole body» }"},"_meta":{"threadId":"thread","progressToken":"progress-10"}}});
        let (mut input_writer, input_reader) = UnixStream::pair().unwrap();
        let output_state = Arc::new((Mutex::new(Vec::new()), std::sync::Condvar::new()));
        let output: Arc<Mutex<Box<dyn Write + Send>>> =
            Arc::new(Mutex::new(Box::new(SharedOutput(output_state.clone()))));
        let sender = Sender {
            socket: socket.to_string_lossy().into_owned(),
        };
        let runner =
            std::thread::spawn(move || sender.run(io::BufReader::new(input_reader), output));

        writeln!(
            input_writer,
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}}"
        )
        .unwrap();
        writeln!(
            input_writer,
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}}"
        )
        .unwrap();
        writeln!(input_writer, "{input}").unwrap();
        let output_text = wait_for_output(&output_state, "SendAccepted");
        assert!(output_text.contains("notifications/progress"));
        assert!(output_text.contains("Queued"));
        writeln!(input_writer, "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":10}}}}").unwrap();
        let output_text = wait_for_output(&output_state, "delivery was not cancelled");
        assert_eq!(
            final_count(&output_text, json!(10)),
            1,
            "one final response for the call: {output_text}"
        );
        drop(input_writer);
        assert_eq!(runner.join().unwrap(), ExitCode::SUCCESS);
        server.join().unwrap();
    }

    #[test]
    fn cancellation_before_acceptance_reports_unknown_without_retry() {
        use std::os::unix::net::UnixListener;
        use std::sync::mpsc;

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("message.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let (received_tx, received_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            let mut length = [0; 4];
            peer.read_exact(&mut length).unwrap();
            let mut query = vec![0; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut query).unwrap();
            received_tx.send(()).unwrap();
            let mut byte = [0; 1];
            let _ = peer.read(&mut byte);
        });
        let call = json!({"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"send","arguments":{"query":"Send.{ Address.{ Mind flow Secondary } Wake MiddleAbrupt.Question.«whole body» }"},"_meta":{"threadId":"thread","progressToken":"progress-11"}}});
        let (mut input_writer, input_reader) = UnixStream::pair().unwrap();
        let output_state = Arc::new((Mutex::new(Vec::new()), std::sync::Condvar::new()));
        let output: Arc<Mutex<Box<dyn Write + Send>>> =
            Arc::new(Mutex::new(Box::new(SharedOutput(output_state.clone()))));
        let runner = std::thread::spawn(move || {
            Sender {
                socket: socket.to_string_lossy().into_owned(),
            }
            .run(io::BufReader::new(input_reader), output)
        });
        writeln!(
            input_writer,
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}}"
        )
        .unwrap();
        writeln!(
            input_writer,
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}}"
        )
        .unwrap();
        writeln!(input_writer, "{call}").unwrap();
        received_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("Send reached the Message peer");
        writeln!(input_writer, "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":11}}}}").unwrap();
        let text = wait_for_output(&output_state, "outcome is unknown");
        assert_eq!(
            final_count(&text, json!(11)),
            1,
            "one final response for the call: {text}"
        );
        assert!(!text.contains("notifications/progress"));
        drop(input_writer);
        assert_eq!(runner.join().unwrap(), ExitCode::SUCCESS);
        server.join().unwrap();
    }

    #[test]
    fn matching_native_delivery_has_one_final_response() {
        use signal_flow::{
            Address, DeliverySnapshot, DeliveryState, HarnessKind, Layer, NativeReceipt,
            SendAcceptance, SentBy,
        };
        use std::os::unix::net::UnixListener;

        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("message.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            let mut length = [0; 4];
            peer.read_exact(&mut length).unwrap();
            let mut query = vec![0; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut query).unwrap();
            write_frame(
                &mut peer,
                &Response::SendAccepted(SendAcceptance {
                    logical_delivery_id: "delivered-1".into(),
                    delivery_cursor: 1,
                    delivery_state: DeliveryState::Queued,
                }),
            );
            let address = Address {
                aspect: signal_flow::Aspect::Mind,
                topic: "flow".into(),
                layer: Layer::Secondary,
            };
            write_frame(
                &mut peer,
                &Response::SendChanged(DeliverySnapshot {
                    delivery_cursor: 2,
                    logical_delivery_id: "delivered-1".into(),
                    sent_by: SentBy::Sender(address.clone()),
                    address,
                    delivery_state: DeliveryState::Delivered(NativeReceipt {
                        harness_kind: HarnessKind::Codex,
                        native_session_id: "thread".into(),
                        native_prompt_id: "prompt-1".into(),
                    }),
                }),
            );
        });
        let call = json!({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"send","arguments":{"query":"Send.{ Address.{ Mind flow Secondary } Wake MiddleAbrupt.Question.«whole body» }"},"_meta":{"threadId":"thread","progressToken":"progress-10"}}});
        let (mut input_writer, input_reader) = UnixStream::pair().unwrap();
        let output_state = Arc::new((Mutex::new(Vec::new()), std::sync::Condvar::new()));
        let output: Arc<Mutex<Box<dyn Write + Send>>> =
            Arc::new(Mutex::new(Box::new(SharedOutput(output_state.clone()))));
        let runner = std::thread::spawn(move || {
            Sender {
                socket: socket.to_string_lossy().into_owned(),
            }
            .run(io::BufReader::new(input_reader), output)
        });
        writeln!(
            input_writer,
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}}"
        )
        .unwrap();
        writeln!(
            input_writer,
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}}"
        )
        .unwrap();
        writeln!(input_writer, "{call}").unwrap();
        let output = wait_for_id(&output_state, json!(10));
        assert_eq!(
            final_count(&output, json!(10)),
            1,
            "one final response for the call"
        );
        drop(input_writer);
        assert_eq!(runner.join().unwrap(), ExitCode::SUCCESS);
        server.join().unwrap();
        assert!(output.contains("notifications/progress"));
        assert!(output.contains("Delivered"));
    }
}
