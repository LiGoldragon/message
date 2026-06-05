use std::fmt::{Display, Formatter};
use std::io::BufReader;
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::{
    Actor, ActorRef, ActorStateAbsence, ActorTerminalOutcome, ActorTerminalReason, Spawn,
};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};
use signal_message::{
    ComponentMessageIngress, MessageDaemonConfiguration, MessageOperationKind, MessageReply,
    MessageRequest, MessageRequestUnimplemented, MessageUnimplementedReason,
    StampedMessageSubmission,
};
use signal_persona::TimestampNanos;
use signal_persona_origin::{
    ConnectionClass, InternalComponentInstanceOrigin, MessageOrigin, OwnerIdentity,
    UnixUserIdentifier,
};
use triad_runtime::{
    ListenerPollInterval, ListenerSocket, MultiListenerDaemon, MultiListenerDaemonError,
    MultiListenerRuntime, RequestErrorLog, SocketMode as RuntimeSocketMode,
};

use crate::error::{Error, Result};
use crate::router::{
    ReceivedMessageRequest, SignalMessageSocket, SignalRouterClient, SignalRouterFrameCodec,
    SignalRouterSocket,
};
use crate::supervision::{
    SupervisionHandle, SupervisionListener, SupervisionProfile, SupervisionSocketMode,
    SupervisionStopSignal,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDaemon {
    message_socket: SignalMessageSocket,
    message_socket_mode: SocketMode,
    router_socket: SignalRouterSocket,
    supervision_socket_path: PathBuf,
    supervision_socket_mode: SupervisionSocketMode,
    component_ingresses: Vec<ComponentMessageIngressBinding>,
    owner_identity: OwnerIdentity,
}

impl MessageDaemon {
    /// Canonical constructor — every production launch reads typed
    /// `MessageDaemonConfiguration` from argv via `nota-config` and
    /// hands the record here.
    pub fn from_configuration(configuration: MessageDaemonConfiguration) -> Self {
        Self {
            message_socket: SignalMessageSocket::from_path(
                configuration.message_socket_path.as_str(),
            ),
            message_socket_mode: SocketMode::from_octal(
                configuration.message_socket_mode.into_u32(),
            ),
            router_socket: SignalRouterSocket::from_path(configuration.router_socket_path.as_str()),
            supervision_socket_path: PathBuf::from(configuration.supervision_socket_path.as_str()),
            supervision_socket_mode: SupervisionSocketMode::from_octal(
                configuration.supervision_socket_mode.into_u32(),
            ),
            component_ingresses: configuration
                .component_ingresses
                .into_iter()
                .map(ComponentMessageIngressBinding::from_contract)
                .collect(),
            owner_identity: configuration.owner_identity,
        }
    }

    /// In-process constructor — every input field explicit. Tests
    /// use this when they build the daemon directly without going
    /// through a configuration file.
    pub fn from_input(input: MessageDaemonInput) -> Self {
        Self {
            message_socket: input.message_socket,
            message_socket_mode: input.message_socket_mode,
            router_socket: input.router_socket,
            supervision_socket_path: input.supervision_socket_path,
            supervision_socket_mode: input.supervision_socket_mode,
            component_ingresses: input.component_ingresses,
            owner_identity: input.owner_identity,
        }
    }

    pub fn run(self) -> Result<()> {
        let listener_sockets = self.listener_sockets();
        eprintln!(
            "message-daemon socket={}",
            self.message_socket.path().display()
        );
        MultiListenerDaemon::new(
            listener_sockets,
            MessageDaemonRuntime::new(MessageDaemonRuntimeInput {
                router_socket: self.router_socket,
                supervision_socket_path: self.supervision_socket_path,
                supervision_socket_mode: self.supervision_socket_mode,
                owner_identity: self.owner_identity,
            }),
            RequestErrorLog::new("message-daemon"),
        )
        .with_listener_poll_interval(ListenerPollInterval::from_millis(10))
        .run()
        .map_err(Self::multi_listener_error)
    }

    pub fn bind_listener(&self) -> Result<MessageSocketBinding> {
        MessageSocketBinder::new(
            self.message_socket.path().clone(),
            self.message_socket_mode,
            MessageIngressAuthority::ExternalPeer,
        )
        .bind()
    }

    fn listener_sockets(&self) -> Vec<ListenerSocket<MessageListener>> {
        let mut sockets = vec![
            ListenerSocket::new(
                MessageListener::ExternalPeer,
                self.message_socket.path().clone(),
            )
            .with_socket_mode(RuntimeSocketMode::new(self.message_socket_mode.as_octal())),
        ];
        for ingress in &self.component_ingresses {
            sockets.push(ingress.listener_socket());
        }
        sockets
    }

    fn multi_listener_error(error: MultiListenerDaemonError<Error, Error>) -> Error {
        match error {
            MultiListenerDaemonError::Listener(error) => Error::DaemonListener(error),
            MultiListenerDaemonError::Start(error) | MultiListenerDaemonError::Stop(error) => error,
        }
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        root: &ActorRef<MessageDaemonRoot>,
        accepted: AcceptedMessageConnection,
    ) -> Result<()> {
        let mut connection = MessageDaemonConnection::from_stream(accepted.stream)?;
        let ingress = MessageIngressContext::new(accepted.authority, connection.peer_credentials());
        let received = connection.read_request()?;
        let request = received.request.clone();
        let reply = match runtime
            .block_on(async { root.ask(ForwardMessageRequest { request, ingress }).await })
        {
            Ok(reply) => reply,
            Err(SendError::HandlerError(error)) => return Err(error),
            Err(error) => {
                return Err(Error::Actor {
                    operation: "forward message request",
                    detail: format!("{error:?}"),
                });
            }
        };
        connection.write_reply(received, reply)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentMessageIngressBinding {
    socket: SignalMessageSocket,
    socket_mode: SocketMode,
    origin: InternalComponentInstanceOrigin,
}

impl ComponentMessageIngressBinding {
    pub fn new(
        socket: SignalMessageSocket,
        socket_mode: SocketMode,
        origin: InternalComponentInstanceOrigin,
    ) -> Self {
        Self {
            socket,
            socket_mode,
            origin,
        }
    }

    pub fn from_contract(ingress: ComponentMessageIngress) -> Self {
        Self::new(
            SignalMessageSocket::from_path(ingress.socket_path.as_str()),
            SocketMode::from_octal(ingress.socket_mode.into_u32()),
            ingress.origin,
        )
    }

    fn listener_socket(&self) -> ListenerSocket<MessageListener> {
        ListenerSocket::new(
            MessageListener::InternalComponentInstance(self.origin.clone()),
            self.socket.path().clone(),
        )
        .with_socket_mode(RuntimeSocketMode::new(self.socket_mode.as_octal()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageListener {
    ExternalPeer,
    InternalComponentInstance(InternalComponentInstanceOrigin),
}

impl MessageListener {
    fn authority(&self) -> MessageIngressAuthority {
        match self {
            Self::ExternalPeer => MessageIngressAuthority::ExternalPeer,
            Self::InternalComponentInstance(origin) => {
                MessageIngressAuthority::InternalComponentInstance(origin.clone())
            }
        }
    }
}

impl Display for MessageListener {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExternalPeer => formatter.write_str("external-peer"),
            Self::InternalComponentInstance(_) => {
                formatter.write_str("internal-component-instance")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageDaemonRuntimeInput {
    router_socket: SignalRouterSocket,
    supervision_socket_path: PathBuf,
    supervision_socket_mode: SupervisionSocketMode,
    owner_identity: OwnerIdentity,
}

pub struct MessageDaemonRuntime {
    input: MessageDaemonRuntimeInput,
    phase: Option<MessageDaemonRuntimePhase>,
}

impl MessageDaemonRuntime {
    fn new(input: MessageDaemonRuntimeInput) -> Self {
        Self { input, phase: None }
    }

    fn phase(&self, operation: &'static str) -> Result<&MessageDaemonRuntimePhase> {
        self.phase.as_ref().ok_or_else(|| Error::Actor {
            operation,
            detail: "message daemon runtime was used before start".to_owned(),
        })
    }

    fn phase_mut(&mut self, operation: &'static str) -> Result<&mut MessageDaemonRuntimePhase> {
        self.phase.as_mut().ok_or_else(|| Error::Actor {
            operation,
            detail: "message daemon runtime was used before start".to_owned(),
        })
    }
}

impl MultiListenerRuntime for MessageDaemonRuntime {
    type Listener = MessageListener;
    type RequestError = Error;
    type StartError = Error;
    type StopError = Error;

    fn should_continue(&self) -> bool {
        match self.phase("check message daemon stop signal") {
            Ok(phase) => !phase.is_stop_requested(),
            Err(_) => true,
        }
    }

    fn start(&mut self) -> Result<()> {
        if self.phase.is_none() {
            self.phase = Some(MessageDaemonRuntimePhase::start(&self.input)?);
        }
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if let Some(phase) = self.phase.take() {
            phase.stop()
        } else {
            Ok(())
        }
    }

    fn handle_stream(&mut self, listener: Self::Listener, stream: UnixStream) -> Result<()> {
        self.phase_mut("handle message stream")?
            .handle_stream(listener, stream)
    }
}

pub struct MessageDaemonRuntimePhase {
    runtime: tokio::runtime::Runtime,
    root: ActorRef<MessageDaemonRoot>,
    stop_signal: SupervisionStopSignal,
    _supervision: SupervisionHandle,
}

impl MessageDaemonRuntimePhase {
    fn start(input: &MessageDaemonRuntimeInput) -> Result<Self> {
        let stop_signal = SupervisionStopSignal::default();
        let supervision = SupervisionListener::new(
            SupervisionProfile::message(),
            input.supervision_socket_path.clone(),
            input.supervision_socket_mode,
        )
        .with_stop_signal(stop_signal.clone())
        .spawn()?;
        let runtime = tokio::runtime::Runtime::new()?;
        let root = runtime.block_on(MessageDaemonRoot::start_root(MessageDaemonRootInput {
            router_socket: input.router_socket.clone(),
            owner_identity: input.owner_identity.clone(),
        }));
        Ok(Self {
            runtime,
            root,
            stop_signal,
            _supervision: supervision,
        })
    }

    fn is_stop_requested(&self) -> bool {
        self.stop_signal.is_stop_requested()
    }

    fn stop(self) -> Result<()> {
        self.stop_signal.request_stop();
        let outcome = self
            .runtime
            .block_on(MessageDaemonRoot::stop_root(self.root.clone()))?;
        MessageDaemonRoot::assert_stopped_outcome(outcome)
    }

    fn handle_stream(&mut self, listener: MessageListener, stream: UnixStream) -> Result<()> {
        MessageDaemon::handle_connection(
            &self.runtime,
            &self.root,
            AcceptedMessageConnection {
                stream,
                authority: listener.authority(),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSocketBinder {
    path: PathBuf,
    mode: SocketMode,
    authority: MessageIngressAuthority,
}

impl MessageSocketBinder {
    pub fn new(path: PathBuf, mode: SocketMode, authority: MessageIngressAuthority) -> Self {
        Self {
            path,
            mode,
            authority,
        }
    }

    pub fn bind(self) -> Result<MessageSocketBinding> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.path);
        let listener = UnixListener::bind(&self.path)?;
        std::fs::set_permissions(
            &self.path,
            std::fs::Permissions::from_mode(self.mode.as_octal()),
        )?;
        Ok(MessageSocketBinding::new(
            self.path,
            listener,
            self.authority,
        ))
    }
}

pub struct MessageSocketBinding {
    path: PathBuf,
    listener: UnixListener,
    authority: MessageIngressAuthority,
}

impl MessageSocketBinding {
    fn new(path: PathBuf, listener: UnixListener, authority: MessageIngressAuthority) -> Self {
        Self {
            path,
            listener,
            authority,
        }
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        self.listener.set_nonblocking(nonblocking)
    }

    pub fn accept(&self) -> std::io::Result<AcceptedMessageConnection> {
        self.listener
            .accept()
            .map(|(stream, _address)| AcceptedMessageConnection {
                stream,
                authority: self.authority.clone(),
            })
    }
}

impl Drop for MessageSocketBinding {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketMode(u32);

impl SocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDaemonInput {
    pub message_socket: SignalMessageSocket,
    pub message_socket_mode: SocketMode,
    pub router_socket: SignalRouterSocket,
    pub supervision_socket_path: PathBuf,
    pub supervision_socket_mode: SupervisionSocketMode,
    pub component_ingresses: Vec<ComponentMessageIngressBinding>,
    pub owner_identity: OwnerIdentity,
}

pub struct MessageDaemonConnection {
    stream: BufReader<UnixStream>,
    codec: SignalRouterFrameCodec,
    peer_credentials: PeerCredentials,
}

impl MessageDaemonConnection {
    pub fn from_stream(stream: UnixStream) -> Result<Self> {
        let peer_credentials = PeerCredentials::from_stream(&stream)?;
        Ok(Self {
            stream: BufReader::new(stream),
            codec: SignalRouterFrameCodec::default(),
            peer_credentials,
        })
    }

    pub fn peer_credentials(&self) -> PeerCredentials {
        self.peer_credentials
    }

    pub fn read_request(&mut self) -> Result<ReceivedMessageRequest> {
        let frame = self.codec.read_frame(&mut self.stream)?;
        self.codec.request_from_frame(frame)
    }

    pub fn write_reply(
        &mut self,
        request: ReceivedMessageRequest,
        reply: MessageReply,
    ) -> Result<()> {
        let frame = self
            .codec
            .reply_frame(request.exchange, request.verb, reply);
        self.codec.write_frame(self.stream.get_mut(), &frame)
    }
}

#[derive(Debug)]
pub struct MessageDaemonRoot {
    router: SignalRouterClient,
    owner_identity: OwnerIdentity,
    forwarded_count: u64,
}

impl MessageDaemonRoot {
    pub fn new(input: MessageDaemonRootInput) -> Self {
        Self {
            router: input.router_socket.client(),
            owner_identity: input.owner_identity,
            forwarded_count: 0,
        }
    }

    pub async fn start_root(input: MessageDaemonRootInput) -> ActorRef<Self> {
        Self::spawn(Self::new(input))
    }

    pub async fn stop_root(reference: ActorRef<Self>) -> Result<ActorTerminalOutcome> {
        reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::Actor {
                operation: "stop message daemon root",
                detail: format!("{error:?}"),
            })?;
        Ok(reference.wait_for_shutdown().await)
    }

    pub fn assert_stopped_outcome(outcome: ActorTerminalOutcome) -> Result<()> {
        if outcome.state != ActorStateAbsence::Dropped
            || outcome.reason != ActorTerminalReason::Stopped
        {
            return Err(Error::Actor {
                operation: "stop message daemon root",
                detail: format!("{outcome:?}"),
            });
        }
        Ok(())
    }

    pub fn stamp_request(
        &self,
        request: MessageRequest,
        ingress: MessageIngressContext,
    ) -> Result<ForwardDecision> {
        match request {
            MessageRequest::MessageSubmission(submission) => Ok(ForwardDecision::Forward(
                MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
                    submission,
                    origin: ingress.origin(&self.owner_identity),
                    stamped_at: Self::timestamp_now()?,
                }),
            )),
            MessageRequest::StampedMessageSubmission(_) => Ok(ForwardDecision::Reply(
                MessageReply::MessageRequestUnimplemented(MessageRequestUnimplemented {
                    operation: MessageOperationKind::StampedMessageSubmission,
                    reason: MessageUnimplementedReason::NotInPrototypeScope,
                }),
            )),
            MessageRequest::InboxQuery(query) => {
                Ok(ForwardDecision::Forward(MessageRequest::InboxQuery(query)))
            }
        }
    }

    fn forward(
        &mut self,
        request: MessageRequest,
        ingress: MessageIngressContext,
    ) -> Result<MessageReply> {
        match self.stamp_request(request, ingress)? {
            ForwardDecision::Forward(request) => {
                let reply = self.router.submit(request)?;
                self.forwarded_count = self.forwarded_count.saturating_add(1);
                Ok(reply)
            }
            ForwardDecision::Reply(reply) => Ok(reply),
        }
    }

    fn timestamp_now() -> Result<TimestampNanos> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::ClockBeforeUnixEpoch)?;
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        Ok(TimestampNanos::new(nanos))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDaemonRootInput {
    pub router_socket: SignalRouterSocket,
    pub owner_identity: OwnerIdentity,
}

impl Actor for MessageDaemonRoot {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct ForwardMessageRequest {
    request: MessageRequest,
    ingress: MessageIngressContext,
}

impl Message<ForwardMessageRequest> for MessageDaemonRoot {
    type Reply = Result<MessageReply>;

    async fn handle(
        &mut self,
        message: ForwardMessageRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.forward(message.request, message.ingress)
    }
}

pub struct AcceptedMessageConnection {
    stream: UnixStream,
    authority: MessageIngressAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageIngressContext {
    authority: MessageIngressAuthority,
    peer: PeerCredentials,
}

impl MessageIngressContext {
    pub fn new(authority: MessageIngressAuthority, peer: PeerCredentials) -> Self {
        Self { authority, peer }
    }

    pub fn external_peer(peer: PeerCredentials) -> Self {
        Self::new(MessageIngressAuthority::ExternalPeer, peer)
    }

    pub fn internal_component_instance(
        origin: InternalComponentInstanceOrigin,
        peer: PeerCredentials,
    ) -> Self {
        Self::new(
            MessageIngressAuthority::InternalComponentInstance(origin),
            peer,
        )
    }

    pub fn origin(&self, owner_identity: &OwnerIdentity) -> MessageOrigin {
        self.authority.origin_for_peer(owner_identity, self.peer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageIngressAuthority {
    ExternalPeer,
    InternalComponentInstance(InternalComponentInstanceOrigin),
}

impl MessageIngressAuthority {
    pub fn origin_for_peer(
        &self,
        owner_identity: &OwnerIdentity,
        peer: PeerCredentials,
    ) -> MessageOrigin {
        match self {
            Self::ExternalPeer => match owner_identity {
                OwnerIdentity::UnixUser(user_id) if peer.user_id == *user_id => {
                    MessageOrigin::External(ConnectionClass::Owner)
                }
                _ => MessageOrigin::External(ConnectionClass::NonOwnerUser(peer.user_id)),
            },
            Self::InternalComponentInstance(origin) => {
                MessageOrigin::InternalComponentInstance(origin.clone())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    user_id: UnixUserIdentifier,
}

impl PeerCredentials {
    pub fn from_user_id(user_id: UnixUserIdentifier) -> Self {
        Self { user_id }
    }

    pub fn from_stream(stream: &UnixStream) -> Result<Self> {
        let mut credentials = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
        let result = unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::addr_of_mut!(credentials).cast(),
                std::ptr::addr_of_mut!(length),
            )
        };
        if result != 0 {
            return Err(Error::PeerCredentials);
        }
        Ok(Self {
            user_id: UnixUserIdentifier::new(credentials.uid),
        })
    }
}

pub enum ForwardDecision {
    Forward(MessageRequest),
    Reply(MessageReply),
}
