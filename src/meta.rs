use std::path::{Path, PathBuf};

#[cfg(feature = "nota-text")]
use std::{fs, io::Write};

#[cfg(feature = "nota-text")]
use meta_signal_message::Request as MetaMessageRequest;
use meta_signal_message::{
    Frame as MetaMessageFrame, FrameBody as MetaMessageFrameBody, MetaMessageReply,
    Operation as MetaMessageOperation, Reason, RequestUnimplemented, UnimplementedOperationKind,
    UnimplementedReason,
};
#[cfg(feature = "nota-text")]
use nota_next::{NotaEncode, NotaSource};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload, SessionEpoch,
    SubReply,
};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
#[cfg(feature = "nota-text")]
use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::frame_bytes::LengthPrefixedFrameBytes;
use crate::{Error, Result};

#[cfg(feature = "nota-text")]
const DEFAULT_META_MESSAGE_SOCKET: &str = "/tmp/meta-message.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageEndpoint {
    socket: PathBuf,
}

impl MetaMessageEndpoint {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn as_path(&self) -> &Path {
        &self.socket
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaMessageFrameCodec {
    maximum_frame_bytes: usize,
}

#[derive(Debug)]
pub enum MetaMessageFrameDecode {
    NotMeta,
    UnexpectedFrame(&'static str),
}

impl MetaMessageFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    fn synthetic_exchange(&self) -> ExchangeIdentifier {
        let _codec_configuration = self.maximum_frame_bytes;
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    pub async fn read_frame_bytes(
        &self,
        stream: &mut UnixStream,
    ) -> Result<LengthPrefixedFrameBytes> {
        LengthPrefixedFrameBytes::read_from_stream(stream, self.maximum_frame_bytes).await
    }

    pub async fn read_frame(&self, stream: &mut UnixStream) -> Result<MetaMessageFrame> {
        let bytes = self.read_frame_bytes(stream).await?;
        Ok(MetaMessageFrame::decode_length_prefixed(bytes.as_slice())?)
    }

    pub async fn write_frame(
        &self,
        stream: &mut UnixStream,
        frame: &MetaMessageFrame,
    ) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        stream.write_all(bytes.as_slice()).await?;
        stream.flush().await?;
        Ok(())
    }

    pub fn request_frame(&self, operation: MetaMessageOperation) -> MetaMessageFrame {
        MetaMessageFrame::new(MetaMessageFrameBody::Request {
            exchange: self.synthetic_exchange(),
            request: operation.into_request(),
        })
    }

    pub fn reply_frame(
        &self,
        exchange: ExchangeIdentifier,
        reply: MetaMessageReply,
    ) -> MetaMessageFrame {
        MetaMessageFrame::new(MetaMessageFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        })
    }

    pub fn decode_request_frame(
        &self,
        bytes: &LengthPrefixedFrameBytes,
    ) -> std::result::Result<(ExchangeIdentifier, MetaMessageOperation), MetaMessageFrameDecode>
    {
        let frame = MetaMessageFrame::decode_length_prefixed(bytes.as_slice())
            .map_err(|_error| MetaMessageFrameDecode::NotMeta)?;
        match frame.into_body() {
            MetaMessageFrameBody::Request { exchange, request } => {
                let mut operations = request.payloads.into_vec();
                if operations.len() != 1 {
                    return Err(MetaMessageFrameDecode::UnexpectedFrame(
                        "expected one meta message operation",
                    ));
                }
                Ok((exchange, operations.remove(0)))
            }
            _ => Err(MetaMessageFrameDecode::UnexpectedFrame(
                "expected meta message request operation",
            )),
        }
    }

    pub async fn write_unimplemented_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        operation: MetaMessageOperation,
    ) -> Result<MetaMessageReply> {
        let reply = MetaMessageReply::RequestUnimplemented(RequestUnimplemented {
            unimplemented_operation_kind: UnimplementedOperationKind::new(operation.kind()),
            reason: Reason::new(UnimplementedReason::NotBuiltYet),
        });
        let frame = self.reply_frame(exchange, reply.clone());
        self.write_frame(stream, &frame).await?;
        Ok(reply)
    }

    pub fn reply_from_frame(&self, frame: MetaMessageFrame) -> Result<MetaMessageReply> {
        match frame.into_body() {
            MetaMessageFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedMetaSubReply(format!("{other:?}"))),
                },
                Reply::Rejected { reason } => Err(Error::MetaReplyRejected(reason)),
            },
            _ => Err(Error::UnexpectedMetaFrame(
                "expected meta message reply operation",
            )),
        }
    }
}

impl Default for MetaMessageFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

pub struct MetaMessageClient {
    endpoint: MetaMessageEndpoint,
    codec: MetaMessageFrameCodec,
}

impl MetaMessageClient {
    pub fn new(endpoint: MetaMessageEndpoint) -> Self {
        Self {
            endpoint,
            codec: MetaMessageFrameCodec::default(),
        }
    }

    pub async fn submit(&self, operation: MetaMessageOperation) -> Result<MetaMessageReply> {
        let mut stream = UnixStream::connect(self.endpoint.as_path()).await?;
        let frame = self.codec.request_frame(operation);
        self.codec.write_frame(&mut stream, &frame).await?;
        let reply = self.codec.read_frame(&mut stream).await?;
        self.codec.reply_from_frame(reply)
    }
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageCommand {
    command: ComponentCommand,
    environment: MetaMessageCommandEnvironment,
}

#[cfg(feature = "nota-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageCommandEnvironment {
    socket: String,
}

#[cfg(feature = "nota-text")]
impl MetaMessageCommand {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            environment: MetaMessageCommandEnvironment::from_process(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(
            arguments,
            MetaMessageCommandEnvironment::from_process(),
        )
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: MetaMessageCommandEnvironment,
    ) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub async fn run(self, mut output: impl Write) -> Result<()> {
        let operation = MetaMessageOperationSource::from_command(self.command)?.into_operation()?;
        let reply = MetaMessageClient::new(self.environment.endpoint())
            .submit(operation)
            .await?;
        writeln!(output, "{}", reply.to_nota())?;
        Ok(())
    }
}

#[cfg(feature = "nota-text")]
impl MetaMessageCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    fn from_process() -> Self {
        Self {
            socket: std::env::var("MESSAGE_META_SOCKET")
                .unwrap_or_else(|_| String::from(DEFAULT_META_MESSAGE_SOCKET)),
        }
    }

    fn endpoint(&self) -> MetaMessageEndpoint {
        MetaMessageEndpoint::new(PathBuf::from(&self.socket))
    }
}

#[cfg(feature = "nota-text")]
struct MetaMessageOperationSource {
    text: String,
}

#[cfg(feature = "nota-text")]
impl MetaMessageOperationSource {
    fn from_command(command: ComponentCommand) -> Result<Self> {
        match command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(Self::new(argument.into_string())),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                fs::read_to_string(&path)
                    .map(Self::new)
                    .map_err(|source| Error::ReadNotaFile { path, source })
            }
            ComponentArgument::SignalFile(file) => {
                let path = file.into_path();
                fs::read_to_string(&path)
                    .map(Self::new)
                    .map_err(|source| Error::ReadNotaFile { path, source })
            }
        }
    }

    fn new(text: String) -> Self {
        Self { text }
    }

    fn into_operation(self) -> Result<MetaMessageOperation> {
        let request = NotaSource::new(&self.text).parse::<MetaMessageRequest>()?;
        Ok(request.payloads().head().clone())
    }
}
