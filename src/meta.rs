use std::path::{Path, PathBuf};

#[cfg(feature = "dotos-text")]
use std::io::Write;

#[cfg(feature = "dotos-text")]
use dotos::{DotosEncode, DotosSource};
use meta_signal_message::schema::lib::{
    ContractMarker, Frame, FrameBody, SignalFrameError, z2VKyZ, z2VM7X, z2VR6z, z2VUdf, z2VY5P,
    z2VYLc, z2Vc2e,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use tokio::net::UnixStream;
use triad_runtime::{FrameBody as TransportBody, LengthPrefixedCodec, MaximumFrameLength};

use crate::{Error, Result};

#[cfg(feature = "dotos-text")]
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
    transport: LengthPrefixedCodec,
}

impl MetaMessageFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            transport: LengthPrefixedCodec::new(MaximumFrameLength::new(maximum_frame_bytes)),
        }
    }

    fn connector_exchange(&self) -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    pub async fn read_frame(&self, stream: &mut UnixStream) -> Result<Frame> {
        let body = self.transport.read_body_async(stream).await?;
        Ok(ContractMarker::decode_frame(body.bytes())?)
    }

    async fn write_encoded(&self, stream: &mut UnixStream, bytes: Vec<u8>) -> Result<()> {
        self.transport
            .write_body_async(stream, &TransportBody::new(bytes))
            .await?;
        Ok(())
    }

    pub async fn read_request(
        &self,
        stream: &mut UnixStream,
    ) -> Result<(ExchangeIdentifier, z2Vc2e)> {
        let body = self.transport.read_body_async(stream).await?;
        Ok(ContractMarker::decode_single_request(body.bytes())?)
    }

    pub async fn write_unimplemented_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        operation: z2Vc2e,
    ) -> Result<z2VYLc> {
        let operation_kind = match operation {
            z2Vc2e::z2VWNS(_) => z2VY5P::z2Vdbu,
        };
        let reply = z2VYLc::z2Vc4F(z2VR6z {
            field_0: z2VUdf::new(operation_kind),
            field_1: z2VKyZ::new(z2VM7X::z2VKwC),
        });
        self.write_encoded(stream, reply.clone().encode_reply_frame(exchange)?)
            .await?;
        Ok(reply)
    }

    pub fn reply_from_frame(&self, frame: Frame) -> Result<z2VYLc> {
        match frame.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
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

    async fn submit(&self, stream: &mut UnixStream, operation: z2Vc2e) -> Result<z2VYLc> {
        self.write_encoded(
            stream,
            operation.encode_request_frame(self.connector_exchange())?,
        )
        .await?;
        let frame = self.read_frame(stream).await?;
        self.reply_from_frame(frame)
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

    pub async fn submit(&self, operation: z2Vc2e) -> Result<z2VYLc> {
        let mut stream = UnixStream::connect(self.endpoint.as_path()).await?;
        self.codec.submit(&mut stream, operation).await
    }
}

#[cfg(feature = "dotos-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageCommand {
    arguments: Vec<String>,
    environment: MetaMessageCommandEnvironment,
}

#[cfg(feature = "dotos-text")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageCommandEnvironment {
    socket: String,
}

#[cfg(feature = "dotos-text")]
impl MetaMessageCommand {
    pub fn from_env() -> Self {
        Self {
            arguments: std::env::args().skip(1).collect(),
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
            arguments: arguments.into_iter().map(Into::into).collect(),
            environment,
        }
    }

    pub async fn run(self, mut output: impl Write) -> Result<()> {
        let [text] = self.arguments.as_slice() else {
            return Err(Error::InvalidMetaArgument {
                detail: format!(
                    "expected exactly one inline Dotos value, received {}",
                    self.arguments.len()
                ),
            });
        };
        let operation = DotosSource::new(text).parse::<z2Vc2e>()?;
        let reply = MetaMessageClient::new(self.environment.endpoint())
            .submit(operation)
            .await?;
        writeln!(output, "{}", reply.to_dotos())?;
        Ok(())
    }
}

#[cfg(feature = "dotos-text")]
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

impl From<SignalFrameError> for Error {
    fn from(error: SignalFrameError) -> Self {
        Self::MetaMessageFrame(error)
    }
}
