use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "dotos-text")]
    #[error("dotos: {0}")]
    Dotos(#[from] dotos::DotosDecodeError),

    #[error("triad frame: {0}")]
    TriadFrame(#[from] triad_runtime::FrameError),

    #[error("ordinary message frame: {0}")]
    OrdinaryMessageFrame(#[from] signal_message::schema::lib::SignalFrameError),

    #[error("meta message frame: {0}")]
    MetaMessageFrame(meta_signal_message::schema::lib::SignalFrameError),

    #[cfg(feature = "dotos-text")]
    #[error("invalid validator argument: {detail}")]
    InvalidValidatorArgument { detail: String },

    #[cfg(feature = "dotos-text")]
    #[error("invalid message command argument: {detail}")]
    InvalidCommandArgument { detail: String },

    #[cfg(feature = "dotos-text")]
    #[error("invalid meta-message argument: {detail}")]
    InvalidMetaArgument { detail: String },

    #[cfg(feature = "dotos-text")]
    #[error("message output validation failed: {detail}")]
    OutputValidation { detail: String },

    #[cfg(feature = "dotos-text")]
    #[error("message daemon socket is not configured; set MESSAGE_SOCKET")]
    SignalMessageSocketMissing,

    #[error("ordinary Message reply was not valid for this command: {got}")]
    UnexpectedOrdinaryReply { got: String },

    #[error("unexpected meta message frame: {0}")]
    UnexpectedMetaFrame(&'static str),

    #[error("unexpected meta message sub-reply: {0}")]
    UnexpectedMetaSubReply(String),

    #[error("meta message reply rejected before execution: {0}")]
    MetaReplyRejected(signal_frame::RequestRejectionReason),

    #[error("messenger store: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("pre-migration preserve of {store}: {message}")]
    PreMigrationPreserve { store: String, message: String },

    #[error("messenger store migration of {store}: {message}")]
    StoreMigration { store: String, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
