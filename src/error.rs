#[cfg(feature = "nota-text")]
use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("component argument error: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),

    #[cfg(feature = "nota-text")]
    #[error("failed to read NOTA file {}: {source}", path.display())]
    ReadNotaFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[cfg(feature = "nota-text")]
    #[error("nota: {0}")]
    Nota(#[from] nota_next::NotaDecodeError),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),

    #[error("triad frame: {0}")]
    TriadFrame(#[from] triad_runtime::FrameError),

    #[error("schema signal frame: {0}")]
    SchemaSignalFrame(#[from] crate::schema::signal::SignalFrameError),

    #[cfg(feature = "nota-text")]
    #[error("invalid validator argument: {detail}")]
    InvalidValidatorArgument { detail: String },

    #[cfg(feature = "nota-text")]
    #[error("message output validation failed: {detail}")]
    OutputValidation { detail: String },

    #[cfg(feature = "nota-text")]
    #[error("message daemon socket is not configured; set MESSAGE_SOCKET")]
    SignalMessageSocketMissing,

    #[error("signal frame is too large: {bytes} bytes")]
    DaemonFrameTooLarge { bytes: usize },

    #[error("router reply was not valid for this command: {got}")]
    UnexpectedRouterReply { got: String },

    #[error("daemon input was not a request frame: {got}")]
    UnexpectedDaemonInput { got: String },

    #[error("unexpected meta message frame: {0}")]
    UnexpectedMetaFrame(&'static str),

    #[error("unexpected meta message sub-reply: {0}")]
    UnexpectedMetaSubReply(String),

    #[error("meta message reply rejected before execution: {0}")]
    MetaReplyRejected(signal_frame::RequestRejectionReason),
}

pub type Result<T> = std::result::Result<T, Error>;
