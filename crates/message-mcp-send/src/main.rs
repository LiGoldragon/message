//! Stdio MCP adapter. The transport stamps threadId outside tool arguments.
//! The executable itself connects to Message so SO_PEERCRED names this peer.
use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use protos::{Compactable, Protosizable, ReaderBudget};
use serde_json::{Value, json};
use signal_message::{Query, Response, SendThread_Data};
use std::{
    env,
    io::{self, BufRead, Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
    time::Duration,
};

const MAX_LINE: u64 = 1024 * 1024;

struct Sender {
    socket: String,
}
trait Exchanges {
    fn exchange(&self, query: &Query) -> Result<Response, String>;
}
impl Exchanges for Sender {
    fn exchange(&self, query: &Query) -> Result<Response, String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|e| e.to_string())?;
        let timeout = Some(Duration::from_secs(10));
        peer.set_read_timeout(timeout).map_err(|e| e.to_string())?;
        peer.set_write_timeout(timeout).map_err(|e| e.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        let length = u32::try_from(bytes.len()).map_err(|_| "oversized Message frame")?;
        peer.write_all(&length.to_be_bytes())
            .and_then(|()| peer.write_all(&bytes))
            .map_err(|e| format!("Uncertain: write failed; do not retry: {e}"))?;
        let mut length = [0; 4];
        peer.read_exact(&mut length)
            .map_err(|e| format!("Uncertain: receipt missing; do not retry: {e}"))?;
        let length = u32::from_be_bytes(length) as usize;
        if length > MAX_LINE as usize {
            return Err("Uncertain: oversized receipt; do not retry".into());
        }
        let mut bytes = vec![0; length];
        peer.read_exact(&mut bytes)
            .map_err(|e| format!("Uncertain: incomplete receipt; do not retry: {e}"))?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&bytes)
            .map_err(|e| format!("Uncertain: invalid receipt; do not retry: {e}"))
    }
}

struct Adapter {
    initialized: bool,
    ready: bool,
}
trait ServesMcp {
    fn handle(&mut self, input: Value, sender: &dyn Exchanges) -> Option<Value>;
    fn thread_query(params: &Value) -> Result<Query, String>;
}
impl ServesMcp for Adapter {
    fn thread_query(params: &Value) -> Result<Query, String> {
        if params.get("name").and_then(Value::as_str) != Some("send") {
            return Err("Unknown tool".into());
        }
        let args = params
            .get("arguments")
            .and_then(Value::as_object)
            .ok_or("Missing arguments")?;
        if args.len() != 1 || !args.contains_key("query") {
            return Err(
                "Only query is a tool argument; sender, process and thread are transport-owned"
                    .into(),
            );
        }
        let thread = params
            .pointer("/_meta/threadId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control))
            .ok_or("Missing or invalid harness-stamped _meta.threadId")?;
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
            recipient: request.recipient,
            request: request.request,
            native_session_id: thread.to_owned(),
        }))
    }

    fn handle(&mut self, input: Value, sender: &dyn Exchanges) -> Option<Value> {
        let id = input.get("id").cloned();
        let method = input.get("method").and_then(Value::as_str);
        if input.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Some(
                json!({"jsonrpc":"2.0","id":id,"error":{"code":-32600,"message":"Invalid Request"}}),
            );
        }
        if id.is_none() {
            if method == Some("notifications/initialized") && self.initialized {
                self.ready = true;
            }
            return None;
        }
        let id = id.unwrap();
        if !id.is_string() && !id.is_number() {
            return Some(
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid request id"}}),
            );
        }
        let params = input.get("params").unwrap_or(&Value::Null);
        let result = match method {
            Some("initialize") if !self.initialized => {
                self.initialized = true;
                Ok(
                    json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"message-mcp-send","version":env!("CARGO_PKG_VERSION")}}),
                )
            }
            Some("ping") => Ok(json!({})),
            _ if !self.ready => Err((-32002, "MCP initialization is incomplete")),
            Some("tools/list") => Ok(
                json!({"tools":[{"name":"send","inputSchema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"],"additionalProperties":false}}]}),
            ),
            Some("tools/call") => {
                // Validation happens before any socket is opened. No retry loop.
                let outcome = Self::thread_query(params).and_then(|query| sender.exchange(&query));
                let (text, failed) = match outcome {
                    Ok(response) => {
                        let failed = matches!(response, Response::Refused(_));
                        (response.datomize(vec![]).protosize().compact(), failed)
                    }
                    Err(error) => (error, true),
                };
                Ok(json!({"content":[{"type":"text","text":text}],"isError":failed}))
            }
            _ => Err((-32601, "Method not found")),
        };
        Some(match result {
            Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
            Err((code, message)) => {
                json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
            }
        })
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
    let mut adapter = Adapter {
        initialized: false,
        ready: false,
    };
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    loop {
        let mut line = String::new();
        match (&mut input).take(MAX_LINE + 1).read_line(&mut line) {
            Ok(0) => return ExitCode::SUCCESS,
            Ok(_) if line.len() as u64 > MAX_LINE => {
                eprintln!("message-mcp-send: oversized input; no dispatch");
                return ExitCode::from(2);
            }
            Err(error) => {
                eprintln!("message-mcp-send: {error}");
                return ExitCode::from(2);
            }
            _ => {}
        }
        let reply = match serde_json::from_str(&line) {
            Ok(value) => adapter.handle(value, &sender),
            Err(_) => Some(
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}),
            ),
        };
        if let Some(reply) = reply {
            if writeln!(output, "{reply}")
                .and_then(|()| output.flush())
                .is_err()
            {
                return ExitCode::from(2);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    struct Fake {
        calls: RefCell<Vec<Query>>,
        fail: bool,
    }
    impl Exchanges for Fake {
        fn exchange(&self, query: &Query) -> Result<Response, String> {
            self.calls.borrow_mut().push(query.clone());
            if self.fail {
                Err("Uncertain: no receipt; do not retry".into())
            } else {
                Ok(Response::Queued)
            }
        }
    }
    fn call() -> Value {
        json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"send","arguments":{"query":"Send.{ Address.{ Mind flow Secondary } Question.«whole body» }"},"_meta":{"threadId":"recorded-thread"}}})
    }
    #[test]
    fn only_transport_metadata_supplies_thread_and_body_is_preserved() {
        let mut adapter = Adapter {
            initialized: true,
            ready: true,
        };
        let fake = Fake {
            calls: RefCell::new(vec![]),
            fail: false,
        };
        let reply = adapter.handle(call(), &fake).unwrap();
        assert_eq!(reply.pointer("/result/isError"), Some(&json!(false)));
        let calls = fake.calls.borrow();
        let [Query::SendThread(request)] = calls.as_slice() else {
            panic!("one thread send");
        };
        assert_eq!(request.native_session_id, "recorded-thread");
        assert_eq!(
            request.request,
            signal_message::Request::Question("whole body".into())
        );
    }
    #[test]
    fn missing_metadata_or_caller_identity_refuses_before_exchange() {
        let fake = Fake {
            calls: RefCell::new(vec![]),
            fail: false,
        };
        for field in ["threadId", "process", "sender", "_meta"] {
            let mut input = call();
            input["params"]["arguments"][field] = json!("spoof");
            let mut adapter = Adapter {
                initialized: true,
                ready: true,
            };
            assert_eq!(
                adapter
                    .handle(input, &fake)
                    .unwrap()
                    .pointer("/result/isError"),
                Some(&json!(true))
            );
        }
        let mut input = call();
        input["params"].as_object_mut().unwrap().remove("_meta");
        let mut adapter = Adapter {
            initialized: true,
            ready: true,
        };
        assert_eq!(
            adapter
                .handle(input, &fake)
                .unwrap()
                .pointer("/result/isError"),
            Some(&json!(true))
        );
        assert!(fake.calls.borrow().is_empty());
    }
    #[test]
    fn no_send_before_ready_and_no_retry_after_uncertainty() {
        let fake = Fake {
            calls: RefCell::new(vec![]),
            fail: true,
        };
        let mut adapter = Adapter {
            initialized: false,
            ready: false,
        };
        assert!(
            adapter
                .handle(call(), &fake)
                .unwrap()
                .get("error")
                .is_some()
        );
        assert!(fake.calls.borrow().is_empty());
        adapter.handle(json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}),&fake);
        adapter.handle(
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            &fake,
        );
        assert_eq!(
            adapter
                .handle(call(), &fake)
                .unwrap()
                .pointer("/result/isError"),
            Some(&json!(true))
        );
        assert_eq!(fake.calls.borrow().len(), 1);
    }
    #[test]
    fn direct_socket_transport_keeps_this_executable_as_message_peer() {
        use std::os::unix::net::UnixListener;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("message.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let expected_pid = std::process::id();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            let credentials = rustix::net::sockopt::socket_peercred(&peer).unwrap();
            assert_eq!(credentials.pid.as_raw_nonzero().get() as u32, expected_pid);
            let mut length = [0; 4];
            peer.read_exact(&mut length).unwrap();
            let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut bytes).unwrap();
            let query = rkyv::from_bytes::<Query, rkyv::rancor::Error>(&bytes).unwrap();
            let Query::SendThread(request) = query else {
                panic!("thread send");
            };
            assert_eq!(request.native_session_id, "recorded-thread");
            let response = rkyv::to_bytes::<rkyv::rancor::Error>(&Response::Queued).unwrap();
            peer.write_all(&(response.len() as u32).to_be_bytes())
                .unwrap();
            peer.write_all(&response).unwrap();
        });
        let sender = Sender {
            socket: path.to_string_lossy().into_owned(),
        };
        let query = Adapter::thread_query(&call()["params"]).unwrap();
        assert_eq!(sender.exchange(&query).unwrap(), Response::Queued);
        server.join().unwrap();
    }
}
