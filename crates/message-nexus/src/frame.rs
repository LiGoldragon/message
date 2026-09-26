//! Signal frames: a four-byte big-endian length, then the rkyv archive of
//! one request or reply. The same framing Flow speaks, so Message is an
//! ordinary client of both Flow sockets.

use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    rancor,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};
use std::io::{self, Read, Write};

/// The largest frame either side accepts.
pub const FRAME_LIMIT: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("socket: {0}")]
    Io(#[from] io::Error),
    #[error("archive: {0}")]
    Archive(#[from] rancor::Error),
    #[error("frame of {0} bytes exceeds the limit")]
    TooLarge(usize),
    /// The peer closed the connection between frames.
    #[error("the peer closed the connection")]
    Closed,
}

/// Writes and reads whole Signal frames on a byte stream.
pub trait FramedStream: Read + Write {
    fn write_frame<Value>(&mut self, value: &Value) -> Result<(), FrameError>
    where
        Value: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        let bytes = rkyv::to_bytes::<rancor::Error>(value)?;
        let length = u32::try_from(bytes.len()).map_err(|_| FrameError::TooLarge(bytes.len()))?;
        self.write_all(&length.to_be_bytes())?;
        self.write_all(&bytes)?;
        self.flush()?;
        Ok(())
    }

    fn read_frame<Value>(&mut self) -> Result<Value, FrameError>
    where
        Value: Archive,
        Value::Archived: for<'a> CheckBytes<HighValidator<'a, rancor::Error>>
            + Deserialize<Value, HighDeserializer<rancor::Error>>,
    {
        let mut length = [0; 4];
        match self.read_exact(&mut length) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(FrameError::Closed);
            }
            Err(error) => return Err(error.into()),
        }
        let length = u32::from_be_bytes(length) as usize;
        if length > FRAME_LIMIT {
            return Err(FrameError::TooLarge(length));
        }
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes)?;
        Ok(rkyv::from_bytes::<Value, rancor::Error>(&bytes)?)
    }
}

impl<Stream: Read + Write> FramedStream for Stream {}
