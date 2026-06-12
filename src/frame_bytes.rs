use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LengthPrefixedFrameBytes {
    bytes: Vec<u8>,
}

impl LengthPrefixedFrameBytes {
    pub async fn read_from_stream(
        stream: &mut UnixStream,
        maximum_frame_bytes: usize,
    ) -> Result<Self> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).await?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > maximum_frame_bytes {
            return Err(Error::DaemonFrameTooLarge { bytes: length });
        }

        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream.read_exact(&mut bytes[4..]).await?;
        Ok(Self { bytes })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}
