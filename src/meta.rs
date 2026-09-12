use std::path::{Path, PathBuf};

use std::io::Write;

use meta_signal_message::{
    OperationKind, Query, RequestUnimplemented, Response, UnimplementedReason,
};
use signal::{ByteViewable, Restorable, Signal, Signalizable};
use tokio::net::UnixStream;
use triad_runtime::{FrameBody as TransportBody, LengthPrefixedCodec, MaximumFrameLength};

use crate::Result;

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

    async fn write_encoded(&self, stream: &mut UnixStream, bytes: Vec<u8>) -> Result<()> {
        self.transport
            .write_body_async(stream, &TransportBody::new(bytes))
            .await?;
        Ok(())
    }

    pub async fn read_request(&self, stream: &mut UnixStream) -> Result<Query> {
        let body = self.transport.read_body_async(stream).await?;
        Ok(Signal::<Query>::from(body.bytes().to_vec()).restore()?)
    }

    /// The privileged relation is vocabulary only until a manager owns it;
    /// every request is answered with the typed unimplemented reply.
    pub async fn write_unimplemented_reply(
        &self,
        stream: &mut UnixStream,
        operation: Query,
    ) -> Result<Response> {
        let unimplemented_operation_kind = match operation {
            Query::Configure(_) => OperationKind::Configure,
        };
        let reply = Response::OperationUnimplemented(RequestUnimplemented {
            unimplemented_operation_kind,
            reason: UnimplementedReason::NotBuiltYet,
        });
        self.write_encoded(stream, reply.signalize()?.bytes().to_vec())
            .await?;
        Ok(reply)
    }

    async fn submit(&self, stream: &mut UnixStream, operation: Query) -> Result<Response> {
        self.write_encoded(stream, operation.signalize()?.bytes().to_vec())
            .await?;
        let body = self.transport.read_body_async(stream).await?;
        Ok(Signal::<Response>::from(body.bytes().to_vec()).restore()?)
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

    pub async fn submit(&self, operation: Query) -> Result<Response> {
        let mut stream = UnixStream::connect(self.endpoint.as_path()).await?;
        self.codec.submit(&mut stream, operation).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageCommand {
    arguments: Vec<String>,
    environment: MetaMessageCommandEnvironment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaMessageCommandEnvironment {
    socket: String,
}

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
        let text = crate::text::sole_argument(&self.arguments)?;
        let operation = crate::text::read::<Query>(text)?;
        let reply = MetaMessageClient::new(self.environment.endpoint())
            .submit(operation)
            .await?;
        writeln!(output, "{}", crate::text::write(&reply))?;
        Ok(())
    }
}

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
