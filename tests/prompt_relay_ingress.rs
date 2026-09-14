use message::{Configuration, MessengerTables, client::MessageSocket};
use signal::{Restorable, Signal};
use signal_message::{
    AgentEndpoint, AgentEndpointBinding, AgentEndpointKind, AgentIdentityAssignment,
    ComponentMessageIngress, ComponentName, InternalComponentInstanceOrigin,
    MessageDaemonConfiguration, MessageOrigin, OwnerIdentity, ProcessPinSelection,
    PromptDispatchRequest, PromptInterpretationSelection, PromptReceiptObservation,
    PromptRelayDelivery, PromptRelayPermission, PromptRelayRejectionReason, PromptRelaySubmission,
    PromptTargetReadiness, PromptVariant, Query, Response, ResumeSelection, TypedPromptEnvelope,
};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixListener,
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);

fn start_time(pid: u32) -> i64 {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .unwrap()
        .rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap()
}

fn wait_for_path(path: &Path) {
    let until = Instant::now() + TIMEOUT;
    while !path.exists() {
        assert!(Instant::now() < until, "timed out waiting for {path:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct Helper {
    child: Child,
    input: ChildStdin,
    output: Receiver<std::io::Result<String>>,
}
impl Helper {
    fn spawn() -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "prompt_relay_ingress_test_helper", "--nocapture"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || forward_lines(stdout, send));
        Self {
            input: child.stdin.take().unwrap(),
            output,
            child,
        }
    }
    fn pid(&self) -> u32 {
        self.child.id()
    }
    fn command(&mut self, command: &str) {
        writeln!(self.input, "{command}").unwrap();
        self.input.flush().unwrap();
    }
    fn expect(&mut self, prefix: &str) -> String {
        loop {
            let line = self
                .output
                .recv_timeout(TIMEOUT)
                .unwrap_or_else(|error| panic!("timed out waiting for {prefix}: {error}"))
                .unwrap_or_else(|error| {
                    panic!("helper output while waiting for {prefix}: {error}")
                });
            if let Some(value) = line.strip_prefix(prefix) {
                return value.trim_end().to_owned();
            }
        }
    }
    fn stop(&mut self) {
        self.command("STOP");
        let _ = self.child.wait();
    }
}

fn forward_lines(stdout: ChildStdout, send: mpsc::Sender<std::io::Result<String>>) {
    let mut output = BufReader::new(stdout);
    loop {
        let mut line = String::new();
        match output.read_line(&mut line) {
            Ok(0) => return,
            Ok(_) => {
                if send.send(Ok(line)).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = send.send(Err(error));
                return;
            }
        }
    }
}
impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Daemon(Child);
impl Daemon {
    fn spawn(configuration: &Path) -> Self {
        Self(
            Command::new(env!("CARGO_BIN_EXE_message-daemon"))
                .arg(configuration)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
}
impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn response_name(response: Response) -> &'static str {
    match response {
        Response::PromptRelayAccepted(_) => "ACCEPTED",
        Response::PromptRelayRejected(rejection) => match rejection.prompt_relay_rejection_reason {
            PromptRelayRejectionReason::UnregisteredSource => "UNREGISTERED",
            PromptRelayRejectionReason::DestinationNotPermitted => "NOT_PERMITTED",
            PromptRelayRejectionReason::StoreRejected => "STORE_REJECTED",
            PromptRelayRejectionReason::RelayDisabled => "RELAY_DISABLED",
        },
        response => panic!("unexpected response: {response:?}"),
    }
}

fn helper_submit(parts: &[&str]) -> Response {
    let prompt_variant = match parts[4] {
        "human" => PromptVariant::HumanPrompt,
        "peer" => PromptVariant::PeerMessage,
        "receipt" => PromptVariant::DeliveryReceipt,
        value => panic!("unknown prompt variant {value}"),
    };
    MessageSocket::from_path(parts[1])
        .client()
        .submit(Query::SubmitPrompt(PromptRelaySubmission {
            destination_agent_identifier: parts[2].to_owned(),
            typed_prompt_envelope: TypedPromptEnvelope {
                prompt_variant,
                source_event_identifier: parts[3].to_owned(),
                raw_prompt_text: parts[5].to_owned(),
                prompt_interpretation_selection: PromptInterpretationSelection::None,
            },
        }))
        .unwrap()
}

#[test]
fn prompt_relay_ingress_test_helper() {
    let mut input = BufReader::new(std::io::stdin().lock());
    let mut output = std::io::stdout().lock();
    let mut destination_listener = None;
    loop {
        let mut line = String::new();
        if input.read_line(&mut line).unwrap() == 0 {
            return;
        }
        let parts: Vec<_> = line.trim_end().split('\t').collect();
        match parts.as_slice() {
            ["DESTINATION", path] => {
                destination_listener = Some(UnixListener::bind(path).unwrap());
                writeln!(output, "READY").unwrap();
            }
            ["CLIENT"] => writeln!(output, "READY").unwrap(),
            ["SUBMIT", ..] => {
                writeln!(output, "RESULT {}", response_name(helper_submit(&parts))).unwrap()
            }
            ["DISPATCH", socket, destination, source, event, readiness] => {
                let readiness = match *readiness {
                    "ready" => PromptTargetReadiness::Ready,
                    "busy" => PromptTargetReadiness::Busy,
                    "dirty" => PromptTargetReadiness::Dirty,
                    _ => panic!("unknown readiness"),
                };
                let response = MessageSocket::from_path(socket)
                    .client()
                    .submit(Query::DispatchPrompt(PromptDispatchRequest {
                        destination_agent_identifier: (*destination).to_owned(),
                        source_agent_identifier: (*source).to_owned(),
                        source_event_identifier: (*event).to_owned(),
                        prompt_target_readiness: readiness,
                    }))
                    .unwrap();
                writeln!(output, "RESULT {}", response_name(response)).unwrap()
            }
            ["OBSERVE", socket, destination, source, event] => {
                let response = MessageSocket::from_path(socket)
                    .client()
                    .submit(Query::ObservePromptReceipt(PromptReceiptObservation {
                        destination_agent_identifier: (*destination).to_owned(),
                        source_agent_identifier: (*source).to_owned(),
                        source_event_identifier: (*event).to_owned(),
                    }))
                    .unwrap();
                writeln!(output, "RESULT {}", response_name(response)).unwrap();
            }
            ["RESEAT", socket, identifier] => {
                let response = MessageSocket::from_path(socket)
                    .client()
                    .submit(Query::AssignAgentIdentity(AgentIdentityAssignment {
                        agent_identifier: (*identifier).to_owned(),
                        process_pin_selection: ProcessPinSelection::None,
                        resume_selection: ResumeSelection::None,
                    }))
                    .unwrap();
                assert!(
                    matches!(response, Response::AgentRegistryRejected(_)),
                    "{response:?}"
                );
                writeln!(output, "REGISTRY_REJECTED").unwrap();
            }
            ["REBIND", socket, identifier, endpoint, pid, started] => {
                let response = MessageSocket::from_path(socket)
                    .client()
                    .submit(Query::BindAgentEndpoint(AgentEndpointBinding {
                        agent_identifier: (*identifier).to_owned(),
                        agent_endpoint: AgentEndpoint {
                            agent_endpoint_kind: AgentEndpointKind::HarnessSocket,
                            endpoint_path: (*endpoint).to_owned(),
                        },
                        harness_pid: pid.parse().unwrap(),
                        harness_start_time: started.parse().unwrap(),
                    }))
                    .unwrap();
                assert!(
                    matches!(response, Response::AgentRegistryRejected(_)),
                    "{response:?}"
                );
                writeln!(output, "REGISTRY_REJECTED").unwrap();
            }
            ["OUTBOUND", source, destination, event, "human", raw, owner] => {
                let listener = destination_listener.as_ref().expect("destination listener");
                listener.set_nonblocking(false).unwrap();
                let (mut stream, _) = listener.accept().unwrap();
                let mut bytes = Vec::new();
                std::io::Read::read_to_end(&mut stream, &mut bytes).unwrap();
                let delivery = Signal::<PromptRelayDelivery>::from(bytes)
                    .restore()
                    .unwrap();
                assert_eq!(delivery.source_agent_identifier, *source);
                assert_eq!(delivery.destination_agent_identifier, *destination);
                assert_eq!(
                    delivery.typed_prompt_envelope.source_event_identifier,
                    *event
                );
                assert_eq!(delivery.typed_prompt_envelope.raw_prompt_text, *raw);
                assert!(matches!(
                    delivery.typed_prompt_envelope.prompt_variant,
                    PromptVariant::HumanPrompt
                ));
                assert!(
                    matches!(delivery.message_origin, MessageOrigin::InternalComponentInstance(origin) if origin.component_name == ComponentName::Harness && origin.component_instance_name == *owner)
                );
                writeln!(output, "OUTBOUND").unwrap();
            }
            ["NO_OUTBOUND"] => {
                let listener = destination_listener.as_ref().expect("destination listener");
                listener.set_nonblocking(true).unwrap();
                let until = Instant::now() + Duration::from_millis(250);
                loop {
                    match listener.accept() {
                        Ok(_) => panic!("unexpected second outbound delivery"),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= until {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("destination accept: {error}"),
                    }
                }
                listener.set_nonblocking(false).unwrap();
                writeln!(output, "NO_OUTBOUND").unwrap();
            }
            ["STOP"] => return,
            _ => panic!("invalid helper command {line:?}"),
        }
        output.flush().unwrap();
    }
}

#[test]
fn prompt_ingress_uses_kernel_peer_and_observation_key() {
    let directory = tempfile::tempdir().unwrap();
    let ingress = directory.path().join("prompt.sock");
    let ordinary = directory.path().join("message.sock");
    let destination_socket = directory.path().join("destination.sock");
    let configuration_path = directory.path().join("configuration");
    let mut source = Helper::spawn();
    let mut destination = Helper::spawn();
    let mut other = Helper::spawn();
    destination.command(&format!("DESTINATION\t{}", destination_socket.display()));
    destination.expect("READY");
    source.command("CLIENT");
    source.expect("READY");
    other.command("CLIENT");
    other.expect("READY");
    let contract = MessageDaemonConfiguration {
        message_socket_path: directory.path().join("message.sock").display().to_string(),
        message_socket_mode: 0o600,
        supervision_socket_path: directory.path().join("meta.sock").display().to_string(),
        supervision_socket_mode: 0o600,
        router_socket_path: directory.path().join("router.sock").display().to_string(),
        component_ingresses: vec![ComponentMessageIngress {
            internal_component_instance_origin: InternalComponentInstanceOrigin {
                component_name: ComponentName::Harness,
                component_instance_name: "test".into(),
            },
            ingress_socket_path: ingress.display().to_string(),
            socket_mode: 0o600,
        }],
        prompt_relay_permissions: vec![PromptRelayPermission {
            source_agent_identifier: "source".into(),
            destination_agent_identifier: "destination".into(),
        }],
        owner_identity: OwnerIdentity::UnixUser(rustix::process::getuid().as_raw().into()),
    };
    let configuration =
        Configuration::new(contract, directory.path().join("messenger.sema"), "test").unwrap();
    let tables = MessengerTables::open(configuration.database_path()).unwrap();
    for (identifier, helper) in [
        ("source", &source),
        ("destination", &destination),
        ("other", &other),
    ] {
        tables
            .seat_identity(&AgentIdentityAssignment {
                agent_identifier: identifier.into(),
                process_pin_selection: ProcessPinSelection::None,
                resume_selection: ResumeSelection::None,
            })
            .unwrap();
        tables
            .bind_endpoint(&AgentEndpointBinding {
                agent_identifier: identifier.into(),
                agent_endpoint: AgentEndpoint {
                    agent_endpoint_kind: AgentEndpointKind::HarnessSocket,
                    endpoint_path: if identifier == "destination" {
                        destination_socket.display().to_string()
                    } else {
                        directory
                            .path()
                            .join(format!("{identifier}.sock"))
                            .display()
                            .to_string()
                    },
                },
                harness_pid: helper.pid() as i64,
                harness_start_time: start_time(helper.pid()),
            })
            .unwrap();
    }
    drop(tables);
    configuration
        .write_binary_file(&configuration_path)
        .unwrap();
    let _daemon = Daemon::spawn(&configuration_path);
    wait_for_path(&ingress);
    wait_for_path(&ordinary);
    let ingress = ingress.display().to_string();
    let ordinary = ordinary.display().to_string();
    other.command(&format!("RESEAT\t{ordinary}\tsource"));
    other.expect("REGISTRY_REJECTED");
    other.command(&format!(
        "REBIND\t{ordinary}\tsource\t{}\t{}\t{}",
        directory.path().join("attacker.sock").display(),
        other.pid(),
        start_time(other.pid()),
    ));
    other.expect("REGISTRY_REJECTED");
    source.command(&format!(
        "SUBMIT\t{ingress}\tdestination\tevent\thuman\traw"
    ));
    assert_eq!(source.expect("RESULT "), "ACCEPTED");
    destination.command(&format!(
        "DISPATCH\t{ingress}\tdestination\tsource\tevent\tready"
    ));
    assert_eq!(destination.expect("RESULT "), "ACCEPTED");
    destination.command("OUTBOUND\tsource\tdestination\tevent\thuman\traw\ttest");
    assert_eq!(destination.expect("OUTBOUND"), "");
    assert_eq!(
        response_name(helper_submit(&[
            "SUBMIT",
            &ingress,
            "destination",
            "unregistered-event",
            "human",
            "raw",
        ])),
        "UNREGISTERED"
    );
    other.command(&format!(
        "SUBMIT\t{ingress}\tdestination\tother-event\thuman\traw"
    ));
    assert_eq!(other.expect("RESULT "), "NOT_PERMITTED");
    source.command(&format!(
        "SUBMIT\t{ingress}\tother\tnot-permitted\thuman\traw"
    ));
    assert_eq!(source.expect("RESULT "), "NOT_PERMITTED");
    source.command(&format!("OBSERVE\t{ingress}\tdestination\tsource\tevent"));
    assert_eq!(source.expect("RESULT "), "UNREGISTERED");
    other.command(&format!("OBSERVE\t{ingress}\tdestination\tsource\tevent"));
    assert_eq!(other.expect("RESULT "), "UNREGISTERED");
    destination.command(&format!("OBSERVE\t{ingress}\tdestination\tsource\twrong"));
    assert_eq!(destination.expect("RESULT "), "STORE_REJECTED");
    destination.command(&format!(
        "OBSERVE\t{ingress}\tdestination\twrong-source\tevent"
    ));
    assert_eq!(destination.expect("RESULT "), "STORE_REJECTED");
    destination.command(&format!("OBSERVE\t{ingress}\tdestination\tsource\tevent"));
    assert_eq!(destination.expect("RESULT "), "ACCEPTED");
    source.command(&format!(
        "SUBMIT\t{ingress}\tdestination\tevent\thuman\traw"
    ));
    assert_eq!(source.expect("RESULT "), "ACCEPTED");
    source.command(&format!(
        "SUBMIT\t{ingress}\tdestination\tpeer-event\tpeer\tpeer"
    ));
    assert_eq!(source.expect("RESULT "), "ACCEPTED");
    source.command(&format!(
        "SUBMIT\t{ingress}\tdestination\treceipt-event\treceipt\treceipt"
    ));
    assert_eq!(source.expect("RESULT "), "ACCEPTED");
    destination.command("NO_OUTBOUND");
    destination.expect("NO_OUTBOUND");
    source.stop();
    destination.stop();
    other.stop();
}
