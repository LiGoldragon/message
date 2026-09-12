use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Text(#[from] crate::text::TextError),

    #[error("message archive: {0}")]
    Archive(#[from] rkyv::rancor::Error),

    #[error("triad frame: {0}")]
    TriadFrame(#[from] triad_runtime::FrameError),

    #[error("invalid validator argument: {detail}")]
    InvalidValidatorArgument { detail: String },

    #[error("invalid message command argument: {detail}")]
    InvalidCommandArgument { detail: String },

    #[error("invalid meta-message argument: {detail}")]
    InvalidMetaArgument { detail: String },

    #[error("message output validation failed: {detail}")]
    OutputValidation { detail: String },

    #[error("message daemon socket is not configured; set MESSAGE_SOCKET")]
    SignalMessageSocketMissing,

    #[error("ordinary Message reply was not valid for this command: {got}")]
    UnexpectedOrdinaryReply { got: String },

    #[error("messenger store: {0}")]
    SemaEngine(#[from] sema_engine::Error),

    #[error("pre-migration preserve of {store}: {message}")]
    PreMigrationPreserve { store: String, message: String },

    #[error("messenger store migration of {store}: {message}")]
    StoreMigration { store: String, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
