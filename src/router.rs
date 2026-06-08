use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply as SignalReply,
    Request as SignalRequest, SessionEpoch, SubReply,
};
use signal_message::{
    ComponentInstanceName, ComponentName, ConnectionClass, Frame, FrameBody,
    InboxEntry as WireInboxEntry, InboxQuery as WireInboxQuery, Input as SignalMessageInput,
    InternalComponentInstanceOrigin, MessageBody, MessageKind as WireMessageKind, MessageOrigin,
    MessageRecipient, MessageSubmission as WireMessageSubmission, Output as SignalMessageOutput,
    OwnerIdentity, StampedMessageSubmission as WireStampedMessageSubmission,
    SubmissionRejectionReason as WireSubmissionRejectionReason, TimestampNanos, UnixUserIdentifier,
};
use triad_runtime::ConnectionContext;

use crate::error::{Error, Result};
use crate::schema::nexus::ForwardRequest;
use crate::schema::signal::{
    Body, InboxContents, InboxEntry, MessageKind, MessageSlot, MessageSubmission, Output, Sender,
    SubmissionAcceptance, SubmissionRejection, SubmissionRejectionReason,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalRouterSocket {
    path: PathBuf,
}

impl SignalRouterSocket {
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn client(&self) -> SignalRouterClient {
        SignalRouterClient::from_socket(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalRouterClient {
    socket: SignalRouterSocket,
    codec: SignalRouterFrameCodec,
}

impl SignalRouterClient {
    pub fn from_socket(socket: SignalRouterSocket) -> Self {
        Self {
            socket,
            codec: SignalRouterFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: SignalMessageInput) -> Result<SignalMessageOutput> {
        let mut stream = UnixStream::connect(&self.socket.path)?;
        let exchange = self.codec.connector_exchange();
        let frame = self.codec.request_frame_with_exchange(exchange, request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.codec.reply_from_frame_for_exchange(reply, exchange)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignalRouterFrameCodec {
    maximum_frame_bytes: usize,
}

impl SignalRouterFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, stream: &mut impl Read) -> Result<Frame> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::DaemonFrameTooLarge { bytes: length });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream.read_exact(&mut bytes[4..])?;
        Ok(Frame::decode_length_prefixed(&bytes)?)
    }

    pub fn write_frame(&self, stream: &mut UnixStream, frame: &Frame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        stream.write_all(&bytes)?;
        stream.flush()?;
        Ok(())
    }

    pub fn connector_exchange(&self) -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    pub fn request_frame(&self, request: SignalMessageInput) -> Frame {
        self.request_frame_with_exchange(self.connector_exchange(), request)
    }

    pub fn request_frame_with_exchange(
        &self,
        exchange: ExchangeIdentifier,
        request: SignalMessageInput,
    ) -> Frame {
        Frame::new(FrameBody::Request {
            exchange,
            request: SignalRequest::from_payload(request),
        })
    }

    pub fn request_from_frame(&self, frame: Frame) -> Result<ReceivedSignalMessageInput> {
        match frame.into_body() {
            FrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedDaemonInput {
                        got: format!(
                            "expected one message payload, got {}",
                            tail.len().saturating_add(1)
                        ),
                    });
                }
                Ok(ReceivedSignalMessageInput { exchange, request })
            }
            other => Err(Error::UnexpectedDaemonInput {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn reply_frame(&self, exchange: ExchangeIdentifier, reply: SignalMessageOutput) -> Frame {
        Frame::new(FrameBody::Reply {
            exchange,
            reply: SignalReply::committed(NonEmpty::single(SubReply::Ok(reply))),
        })
    }

    pub fn reply_from_frame(&self, frame: Frame) -> Result<SignalMessageOutput> {
        self.reply_from_frame_without_exchange_check(frame)
    }

    pub fn reply_from_frame_for_exchange(
        &self,
        frame: Frame,
        expected: ExchangeIdentifier,
    ) -> Result<SignalMessageOutput> {
        match frame.into_body() {
            FrameBody::Reply { exchange, reply } if exchange == expected => {
                self.payload_from_reply(reply)
            }
            FrameBody::Reply { exchange, .. } => Err(Error::UnexpectedRouterReply {
                got: format!("reply exchange {exchange:?} did not match {expected:?}"),
            }),
            other => Err(Error::UnexpectedRouterReply {
                got: format!("{other:?}"),
            }),
        }
    }

    fn reply_from_frame_without_exchange_check(&self, frame: Frame) -> Result<SignalMessageOutput> {
        match frame.into_body() {
            FrameBody::Reply { reply, .. } => self.payload_from_reply(reply),
            other => Err(Error::UnexpectedRouterReply {
                got: format!("{other:?}"),
            }),
        }
    }

    fn payload_from_reply(
        &self,
        reply: SignalReply<SignalMessageOutput>,
    ) -> Result<SignalMessageOutput> {
        match reply {
            SignalReply::Accepted { per_operation, .. } => {
                let (sub_reply, tail) = per_operation.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedRouterReply {
                        got: format!("expected one reply operation, got {}", tail.len() + 1),
                    });
                }
                match sub_reply {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedRouterReply {
                        got: format!("{other:?}"),
                    }),
                }
            }
            SignalReply::Rejected { reason } => Err(Error::UnexpectedRouterReply {
                got: format!("{reason:?}"),
            }),
        }
    }
}

impl Default for SignalRouterFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedSignalMessageInput {
    pub exchange: ExchangeIdentifier,
    pub request: SignalMessageInput,
}

/// The owner-identity policy that mints a connection's provenance origin from
/// its kernel-vouched peer credentials and the daemon's configured local
/// component instance.
///
/// This restores the origin classification the pre-triad-runtime daemon
/// performed: an accepted connection whose peer uid matches the engine owner is
/// the configured local harness instance; any other local Unix user is a
/// `NonOwnerUser(uid)`. The owner identity and local instance name are sourced
/// from daemon configuration, never from a payload — provenance crosses the
/// operating-system trust boundary at `SO_PEERCRED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginPolicy {
    owner_identity: OwnerIdentity,
    local_instance: ComponentInstanceName,
}

impl OriginPolicy {
    pub fn new(owner_identity: OwnerIdentity, local_instance: ComponentInstanceName) -> Self {
        Self {
            owner_identity,
            local_instance,
        }
    }

    /// The owner policy for a local Unix user owner identified by uid.
    pub fn for_owner_user_id(owner_user_id: u32, local_instance: impl Into<String>) -> Self {
        Self::new(
            OwnerIdentity::UnixUser(UnixUserIdentifier::new(u64::from(owner_user_id))),
            ComponentInstanceName::new(local_instance.into()),
        )
    }

    /// Classify an accepted connection's peer credentials into a typed origin.
    ///
    /// A peer uid that matches the owner's Unix uid is the configured local
    /// harness instance; any other local user is a non-owner. A `System`-owned
    /// engine has no Unix owner uid to match against, so every local connection
    /// classifies as a non-owner user — matching the old daemon's uid-gated
    /// trust boundary while preserving the component-instance identity router
    /// grants need.
    pub fn origin_for_connection(&self, connection: &ConnectionContext) -> MessageOrigin {
        let peer_user_id = UnixUserIdentifier::new(u64::from(connection.user_id()));
        match &self.owner_identity {
            OwnerIdentity::UnixUser(owner_user_id) if peer_user_id == *owner_user_id => {
                MessageOrigin::InternalComponentInstance(InternalComponentInstanceOrigin {
                    component: ComponentName::Harness,
                    instance: self.local_instance.clone(),
                })
            }
            _ => MessageOrigin::External(ConnectionClass::NonOwnerUser(peer_user_id)),
        }
    }
}

/// The outbound half of the message daemon: the schema `ForwardToRouter` effect
/// realized over the generated `signal-message` wire.
///
/// The daemon's inbound socket (`message.sock`) speaks the daemon-local schema
/// signal-frame format. The router ingress socket speaks the published
/// `signal-message` contract, now generated from `signal-message/schema/lib.schema`.
/// This forwarder is the projection seam from daemon-local `ForwardRequest`
/// values into `signal_message::Input` values and back from
/// `signal_message::Output` into daemon-local `Output`. Provenance is minted
/// here: the origin is derived from the accepted connection's peer credentials
/// through [`OriginPolicy`] (threaded in from the emitted working-input hook),
/// and the ingress timestamp is daemon-stamped. Provenance is never accepted
/// from the caller payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterForwarder {
    client: SignalRouterClient,
    origin_policy: OriginPolicy,
}

impl RouterForwarder {
    pub fn new(router_socket: SignalRouterSocket, origin_policy: OriginPolicy) -> Self {
        Self {
            client: router_socket.client(),
            origin_policy,
        }
    }

    pub fn from_configuration(configuration: &crate::config::Configuration) -> Self {
        Self::new(
            SignalRouterSocket::from_path(configuration.router_socket_path()),
            OriginPolicy::for_owner_user_id(
                configuration.owner_user_id(),
                configuration.owner_name(),
            ),
        )
    }

    /// Forward one schema `ForwardRequest` to the router and lift the reply back
    /// into a schema-side outcome. A router socket failure becomes
    /// `RouterForwardOutcome::Unreachable` rather than a hard error so the Nexus
    /// decision can reply with a typed unavailable result.
    ///
    /// `connection` carries the accepted stream's peer credentials, from which
    /// the stamped submission's origin is minted.
    pub fn forward(
        &self,
        request: ForwardRequest,
        connection: &ConnectionContext,
    ) -> Result<RouterForwardOutcome> {
        let wire_request = self.wire_request(request, connection);
        match self.client.submit(wire_request) {
            Ok(reply) => Ok(RouterForwardOutcome::Replied(Self::schema_output(reply))),
            Err(Error::Io(_)) | Err(Error::DaemonFrameTooLarge { .. }) => {
                Ok(RouterForwardOutcome::Unreachable)
            }
            Err(error) => Err(error),
        }
    }

    fn wire_request(
        &self,
        request: ForwardRequest,
        connection: &ConnectionContext,
    ) -> SignalMessageInput {
        match request {
            ForwardRequest::StampAndForward(submission) => {
                SignalMessageInput::SubmitStamped(self.stamp(submission, connection))
            }
            ForwardRequest::ForwardInboxQuery(query) => SignalMessageInput::QueryInbox(
                WireInboxQuery::new(MessageRecipient::new(query.into_payload())),
            ),
        }
    }

    /// Mint provenance onto a raw submission: the peer-credential-derived origin
    /// plus a daemon-minted ingress timestamp. Provenance is never accepted from
    /// the caller payload — the daemon stamps it.
    fn stamp(
        &self,
        submission: MessageSubmission,
        connection: &ConnectionContext,
    ) -> WireStampedMessageSubmission {
        WireStampedMessageSubmission {
            submission: Self::wire_submission(submission),
            origin: self.origin_policy.origin_for_connection(connection),
            stamped_at: Self::ingress_timestamp(),
        }
    }

    fn wire_submission(submission: MessageSubmission) -> WireMessageSubmission {
        WireMessageSubmission {
            recipient: MessageRecipient::new(submission.recipient),
            kind: Self::wire_kind(submission.message_kind),
            body: MessageBody::new(submission.body),
        }
    }

    fn wire_kind(kind: MessageKind) -> WireMessageKind {
        match kind {
            MessageKind::Send => WireMessageKind::Send,
            MessageKind::Inbox => WireMessageKind::Inbox,
        }
    }

    fn ingress_timestamp() -> TimestampNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
            .unwrap_or(0);
        TimestampNanos::new(nanos)
    }

    /// Translate a `signal-message` reply back into the daemon-local schema
    /// `Output` the emitted Signal plane replies with.
    fn schema_output(reply: SignalMessageOutput) -> Output {
        match reply {
            SignalMessageOutput::SubmissionAccepted(acceptance) => Output::SubmissionAccepted(
                SubmissionAcceptance(acceptance.into_payload().into_u64() as MessageSlot),
            ),
            SignalMessageOutput::SubmissionRejected(rejection) => Output::SubmissionRejected(
                SubmissionRejection(Self::schema_rejection_reason(rejection.into_payload())),
            ),
            SignalMessageOutput::InboxListing(listing) => Output::InboxListing(InboxContents(
                listing
                    .into_payload()
                    .into_iter()
                    .map(Self::schema_inbox_entry)
                    .collect(),
            )),
            SignalMessageOutput::MessageRequestUnimplemented(unimplemented) => {
                Output::Error(crate::schema::signal::ErrorReport(format!(
                    "router rejected operation {:?}: {:?}",
                    unimplemented.operation, unimplemented.reason
                )))
            }
        }
    }

    fn schema_inbox_entry(entry: WireInboxEntry) -> InboxEntry {
        InboxEntry {
            message_slot: entry.message_slot.into_u64() as MessageSlot,
            sender: entry.sender.as_str().to_owned() as Sender,
            body: entry.body.as_str().to_owned() as Body,
        }
    }

    fn schema_rejection_reason(reason: WireSubmissionRejectionReason) -> SubmissionRejectionReason {
        match reason {
            WireSubmissionRejectionReason::StoreRejected => {
                SubmissionRejectionReason::StoreRejected
            }
            WireSubmissionRejectionReason::RecipientNotFound => {
                SubmissionRejectionReason::RecipientNotFound
            }
        }
    }
}

/// The schema-side outcome of one router forward: a translated reply, or the
/// router being unreachable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouterForwardOutcome {
    Replied(Output),
    Unreachable,
}
