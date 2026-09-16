use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::error::Result;
use signal::{ByteViewable, Restorable, Signal, Signalizable};
use signal_message::{Query, Response};

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

    /// One connection carries one request and one reply. The body is the bare
    /// rkyv archive of the contract root; the length prefix is the framing.
    pub fn submit(&self, query: Query) -> Result<Response> {
        self.submit_with_timeout(query, Duration::from_secs(30))
    }

    /// Bounds connect, write, and reply waits for a single request/reply socket.
    /// The existing `submit` API retains a conservative default bound.
    pub fn submit_with_timeout(&self, query: Query, timeout: Duration) -> Result<Response> {
        let mut stream = UnixStream::connect(self.socket.path())?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        let request = FrameBody::new(query.signalize()?.bytes().to_vec());
        self.codec.write_body(&mut stream, &request)?;
        stream.flush()?;
        let reply = self.codec.read_body(&mut stream)?;
        Ok(Signal::<Response>::from(reply.into_bytes()).restore()?)
    }
}
