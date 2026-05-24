use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply as SignalReply, Request,
    SessionEpoch, SignalVerb, SubReply,
};
use signal_message::{Frame, FrameBody, MessageReply, MessageRequest};

use crate::error::{Error, Result};

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
pub struct SignalMessageSocket {
    path: PathBuf,
}

impl SignalMessageSocket {
    pub fn from_environment() -> Option<Self> {
        std::env::var_os("MESSAGE_SOCKET")
            .or_else(|| std::env::var_os("PERSONA_SOCKET_PATH"))
            .map(Self::from_path)
    }

    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn client(&self) -> SignalMessageClient {
        SignalMessageClient::from_socket(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMessageClient {
    socket: SignalMessageSocket,
    codec: SignalRouterFrameCodec,
}

impl SignalMessageClient {
    pub fn from_socket(socket: SignalMessageSocket) -> Self {
        Self {
            socket,
            codec: SignalRouterFrameCodec::default(),
        }
    }

    pub fn submit(&self, request: MessageRequest) -> Result<MessageReply> {
        let mut stream = UnixStream::connect(&self.socket.path)?;
        let exchange = self.codec.connector_exchange();
        let frame = self.codec.request_frame_with_exchange(exchange, request);
        self.codec.write_frame(&mut stream, &frame)?;
        let reply = self.codec.read_frame(&mut stream)?;
        self.codec.reply_from_frame_for_exchange(reply, exchange)
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

    pub fn submit(&self, request: MessageRequest) -> Result<MessageReply> {
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

    pub fn request_frame(&self, request: MessageRequest) -> Frame {
        self.request_frame_with_exchange(self.connector_exchange(), request)
    }

    pub fn request_frame_with_exchange(
        &self,
        exchange: ExchangeIdentifier,
        request: MessageRequest,
    ) -> Frame {
        Frame::new(FrameBody::Request {
            exchange,
            request: Request::from_payload(request),
        })
    }

    pub fn request_from_frame(&self, frame: Frame) -> Result<ReceivedMessageRequest> {
        match frame.into_body() {
            FrameBody::Request { exchange, request } => {
                let checked = request
                    .into_checked()
                    .map_err(|(reason, _)| Error::InvalidSignalRequest { reason })?;
                let (operation, tail) = checked.operations.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedDaemonInput {
                        got: format!(
                            "expected one message operation, got {}",
                            tail.len().saturating_add(1)
                        ),
                    });
                }
                Ok(ReceivedMessageRequest {
                    exchange,
                    verb: operation.verb,
                    request: operation.payload,
                })
            }
            other => Err(Error::UnexpectedDaemonInput {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn reply_frame(
        &self,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        reply: MessageReply,
    ) -> Frame {
        Frame::new(FrameBody::Reply {
            exchange,
            reply: SignalReply::completed(NonEmpty::single(SubReply::Ok {
                verb,
                payload: reply,
            })),
        })
    }

    pub fn reply_from_frame(&self, frame: Frame) -> Result<MessageReply> {
        self.reply_from_frame_without_exchange_check(frame)
    }

    pub fn reply_from_frame_for_exchange(
        &self,
        frame: Frame,
        expected: ExchangeIdentifier,
    ) -> Result<MessageReply> {
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

    fn reply_from_frame_without_exchange_check(&self, frame: Frame) -> Result<MessageReply> {
        match frame.into_body() {
            FrameBody::Reply { reply, .. } => self.payload_from_reply(reply),
            other => Err(Error::UnexpectedRouterReply {
                got: format!("{other:?}"),
            }),
        }
    }

    fn payload_from_reply(&self, reply: SignalReply<MessageReply>) -> Result<MessageReply> {
        match reply {
            SignalReply::Accepted { per_operation, .. } => {
                let (sub_reply, tail) = per_operation.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedRouterReply {
                        got: format!("expected one reply operation, got {}", tail.len() + 1),
                    });
                }
                match sub_reply {
                    SubReply::Ok { payload, .. } => Ok(payload),
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
pub struct ReceivedMessageRequest {
    pub exchange: ExchangeIdentifier,
    pub verb: SignalVerb,
    pub request: MessageRequest,
}
