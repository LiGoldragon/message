use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::error::Result;
use crate::schema::signal::{Input, Output};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageSocket {
    path: PathBuf,
}

impl MessageSocket {
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

    pub fn client(&self) -> MessageClient {
        MessageClient::from_socket(self.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageClient {
    socket: MessageSocket,
    codec: LengthPrefixedCodec,
}

impl MessageClient {
    pub fn from_socket(socket: MessageSocket) -> Self {
        Self {
            socket,
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn submit(&self, input: Input) -> Result<Output> {
        let mut stream = UnixStream::connect(self.socket.path())?;
        let request = FrameBody::new(input.encode_signal_frame()?);
        self.codec.write_body(&mut stream, &request)?;
        stream.flush()?;
        let reply = self.codec.read_body(&mut stream)?;
        let (_route, output) = Output::decode_signal_frame(&reply.into_bytes())?;
        Ok(output)
    }
}
