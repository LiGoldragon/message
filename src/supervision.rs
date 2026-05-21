use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_frame::{ExchangeIdentifier, NonEmpty, Reply as SignalReply, SubReply};
use signal_persona::engine_management::{
    Frame as SupervisionFrame, FrameBody, Operation as SupervisionRequest,
    Query as SupervisionQuery, Reply as SupervisionReply,
};
use signal_persona::{
    ComponentHealth, ComponentHealthReport, ComponentIdentity, ComponentKind, ComponentName,
    ComponentReady, EngineManagementProtocolVersion, Presence, StopAcknowledgement,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionProfile {
    name: ComponentName,
    kind: ComponentKind,
    health: ComponentHealth,
}

impl SupervisionProfile {
    pub fn message() -> Self {
        Self {
            name: ComponentName::new("persona-message"),
            kind: ComponentKind::Message,
            health: ComponentHealth::Running,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupervisionSocketMode(u32);

impl SupervisionSocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct SupervisionListener {
    profile: SupervisionProfile,
    socket: PathBuf,
    mode: SupervisionSocketMode,
    stop_signal: SupervisionStopSignal,
}

impl SupervisionListener {
    pub fn new(
        profile: SupervisionProfile,
        socket: impl Into<PathBuf>,
        mode: SupervisionSocketMode,
    ) -> Self {
        Self {
            profile,
            socket: socket.into(),
            mode,
            stop_signal: SupervisionStopSignal::default(),
        }
    }

    pub fn with_stop_signal(mut self, stop_signal: SupervisionStopSignal) -> Self {
        self.stop_signal = stop_signal;
        self
    }

    pub fn spawn(self) -> std::io::Result<SupervisionHandle> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        listener.set_nonblocking(true)?;
        std::fs::set_permissions(
            &self.socket,
            std::fs::Permissions::from_mode(self.mode.as_octal()),
        )?;
        let server = SupervisionServer::new(self.profile, listener, self.stop_signal);
        Ok(SupervisionHandle {
            _thread: std::thread::spawn(move || server.run()),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct SupervisionStopSignal {
    requested: Arc<AtomicBool>,
}

impl SupervisionStopSignal {
    pub fn request_stop(&self) {
        self.requested.store(true, Ordering::Release);
    }

    pub fn is_stop_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

pub struct SupervisionHandle {
    _thread: JoinHandle<()>,
}

#[derive(Debug)]
pub struct SupervisionPhase {
    profile: SupervisionProfile,
    stop_signal: SupervisionStopSignal,
    request_count: u64,
}

impl SupervisionPhase {
    fn new(profile: SupervisionProfile, stop_signal: SupervisionStopSignal) -> Self {
        Self {
            profile,
            stop_signal,
            request_count: 0,
        }
    }

    async fn start(
        profile: SupervisionProfile,
        stop_signal: SupervisionStopSignal,
    ) -> ActorRef<Self> {
        let reference = Self::spawn(Self::new(profile, stop_signal));
        reference.wait_for_startup().await;
        reference
    }

    fn reply(&mut self, request: SupervisionRequest) -> SupervisionReply {
        self.request_count = self.request_count.saturating_add(1);
        match request {
            SupervisionRequest::Announce(Presence { .. }) => {
                SupervisionReply::Identified(ComponentIdentity {
                    name: self.profile.name.clone(),
                    kind: self.profile.kind,
                    engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
                    last_fatal_startup_error: None,
                })
            }
            SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(_)) => {
                SupervisionReply::Ready(ComponentReady {
                    component_started_at: None,
                })
            }
            SupervisionRequest::Query(SupervisionQuery::HealthStatus(_)) => {
                SupervisionReply::HealthReport(ComponentHealthReport {
                    health: self.profile.health,
                })
            }
            SupervisionRequest::Stop(_) => {
                self.stop_signal.request_stop();
                SupervisionReply::StopAcknowledged(StopAcknowledgement {
                    drain_completed_at: None,
                })
            }
        }
    }
}

#[derive(Debug, kameo::Reply)]
struct SupervisionPhaseReply {
    reply: SupervisionReply,
}

impl Actor for SupervisionPhase {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        phase: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(phase)
    }
}

#[derive(Debug)]
struct HandleSupervisionRequest {
    request: SupervisionRequest,
}

impl Message<HandleSupervisionRequest> for SupervisionPhase {
    type Reply = SupervisionPhaseReply;

    async fn handle(
        &mut self,
        message: HandleSupervisionRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SupervisionPhaseReply {
            reply: self.reply(message.request),
        }
    }
}

struct SupervisionServer {
    profile: SupervisionProfile,
    listener: UnixListener,
    codec: SupervisionFrameCodec,
    stop_signal: SupervisionStopSignal,
}

impl SupervisionServer {
    fn new(
        profile: SupervisionProfile,
        listener: UnixListener,
        stop_signal: SupervisionStopSignal,
    ) -> Self {
        Self {
            profile,
            listener,
            codec: SupervisionFrameCodec::new(1024 * 1024),
            stop_signal,
        }
    }

    fn run(self) {
        let runtime = tokio::runtime::Runtime::new().expect("supervision runtime starts");
        let phase = runtime.block_on(SupervisionPhase::start(
            self.profile.clone(),
            self.stop_signal.clone(),
        ));
        while !self.stop_signal.is_stop_requested() {
            match self.listener.accept() {
                Ok((mut stream, _address)) => {
                    let _ = self.serve_connection(&runtime, &phase, &mut stream);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => continue,
            }
        }
    }

    fn serve_connection(
        &self,
        runtime: &tokio::runtime::Runtime,
        phase: &ActorRef<SupervisionPhase>,
        stream: &mut UnixStream,
    ) -> std::io::Result<()> {
        while let Ok(request) = self.codec.read_request(stream) {
            let reply = runtime
                .block_on(
                    phase
                        .ask(HandleSupervisionRequest {
                            request: request.request,
                        })
                        .send(),
                )
                .map_err(io_error)?;
            self.codec
                .write_reply(stream, request.exchange, reply.reply)?;
            if self.stop_signal.is_stop_requested() {
                break;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct SupervisionFrameCodec {
    maximum_frame_bytes: usize,
}

impl SupervisionFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_request(
        &self,
        reader: &mut impl Read,
    ) -> std::io::Result<ReceivedSupervisionRequest> {
        let frame = self.read_frame(reader)?;
        match frame.into_body() {
            FrameBody::Request { exchange, request } => {
                let mut operations = request.payloads.into_vec();
                if operations.len() != 1 {
                    return Err(io_error(format!(
                        "expected one supervision operation, got {}",
                        operations.len()
                    )));
                }
                Ok(ReceivedSupervisionRequest {
                    exchange,
                    request: operations.remove(0),
                })
            }
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected supervision frame body: {other:?}"),
            )),
        }
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> std::io::Result<SupervisionReply> {
        let frame = self.read_frame(reader)?;
        match frame.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                SignalReply::Accepted { per_operation, .. } => {
                    let mut operations = per_operation.into_vec();
                    if operations.len() != 1 {
                        return Err(io_error(format!(
                            "expected one supervision reply operation, got {}",
                            operations.len()
                        )));
                    }
                    match operations.remove(0) {
                        SubReply::Ok(payload) => Ok(payload),
                        other => Err(io_error(format!("{other:?}"))),
                    }
                }
                SignalReply::Rejected { reason } => Err(io_error(reason)),
            },
            other => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unexpected supervision frame body: {other:?}"),
            )),
        }
    }

    pub fn write_reply(
        &self,
        writer: &mut impl Write,
        exchange: ExchangeIdentifier,
        reply: SupervisionReply,
    ) -> std::io::Result<()> {
        let frame = SupervisionFrame::new(FrameBody::Reply {
            exchange,
            reply: SignalReply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        let bytes = frame.encode_length_prefixed().map_err(io_error)?;
        writer.write_all(bytes.as_slice())?;
        writer.flush()
    }

    fn read_frame(&self, reader: &mut impl Read) -> std::io::Result<SupervisionFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("supervision frame length {length} exceeds maximum"),
            ));
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        SupervisionFrame::decode_length_prefixed(bytes.as_slice()).map_err(io_error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedSupervisionRequest {
    pub exchange: ExchangeIdentifier,
    pub request: SupervisionRequest,
}

fn io_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}
