use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

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
    #[error("inline Nota argument must be UTF-8: {got:?}")]
    InvalidInlineNotaArgument { got: String },

    #[cfg(feature = "nota-text")]
    #[error("missing NOTA input; pass one record such as '(Send designer [hello])'")]
    MissingInput,

    #[cfg(feature = "nota-text")]
    #[error("unexpected command-line argument: {got:?}")]
    UnexpectedArgument { got: String },

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
}

pub type Result<T> = std::result::Result<T, Error>;
