use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("nota: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_core::FrameError),

    #[error("inline Nota argument must be UTF-8: {got:?}")]
    InvalidInlineNotaArgument { got: String },

    #[error("missing NOTA input; pass one record such as '(Send designer \"hello\")'")]
    MissingInput,

    #[error("unexpected command-line argument: {got:?}")]
    UnexpectedArgument { got: String },

    #[error("invalid validator argument: {detail}")]
    InvalidValidatorArgument { detail: String },

    #[error("message output validation failed: {detail}")]
    OutputValidation { detail: String },

    #[error("message daemon socket is not configured; set MESSAGE_SOCKET")]
    SignalMessageSocketMissing,

    #[error("signal frame is too large: {bytes} bytes")]
    DaemonFrameTooLarge { bytes: usize },

    #[error("router reply was not valid for this command: {got}")]
    UnexpectedRouterReply { got: String },

    #[error("daemon input was not a request frame: {got}")]
    UnexpectedDaemonInput { got: String },

    #[error("daemon input failed Signal request checks: {reason}")]
    InvalidSignalRequest {
        reason: signal_core::RequestRejectionReason,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
