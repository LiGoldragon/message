//! Message's daemon hooks — the only daemon code message hand-writes.
//!
//! The uniform daemon skeleton (the `DaemonCommand` argv parsing, the async
//! decode -> execute -> encode spine, the async task-backed listener, and the
//! `ExitReport`-based entry) is EMITTED into `src/schema/daemon.rs` by
//! schema-rust-next's daemon emitter, driven by the `NexusDaemonShape` in
//! `build.rs`. Message fills only the record-1488 escape hatches through `impl
//! ComponentDaemon for MessageDaemon`: how to load its `Configuration`, how to
//! build its `MessageEngine` (`build_runtime`), how one working `Input`
//! becomes one `Output` (`handle_working_input`), and how the owner meta socket
//! returns typed skeleton-honest replies until live reconfiguration is wired.
//! Message has no durable store, so there is no `start`/`stop` resource setup.

use thiserror::Error;
use triad_runtime::{AcceptedConnection, EngineRequestError, FrameError};

use crate::{
    config::{Configuration, ConfigurationError},
    engine::MessageEngine,
    error::Error as MessageError,
    meta::{MetaMessageFrameCodec, MetaMessageFrameDecode},
    schema::{
        daemon::ComponentDaemon,
        signal::{Input, Output, SignalFrameError},
    },
};

/// The type-level selector for message's emitted daemon. It carries no runtime
/// data — it is the marker the emitted `DaemonCommand<MessageDaemon>` and the
/// generated runtime dispatch on, selecting message's `Configuration` /
/// `Engine` / `Error` through the `ComponentDaemon` associated types.
#[derive(Debug)]
pub struct MessageDaemon;

/// Message's daemon error: the engine-facing variants the emitted spine needs
/// (`From<FrameError>` / `From<SignalFrameError>`) plus message's domain error.
/// The emitted `DaemonError<MessageDaemon>` wraps this under its `Component`
/// arm. The `EngineRequest` arm absorbs the engine-actor mailbox-failure path
/// the emitted runtime translates through `From<EngineRequestError>` (actor not
/// running, stopped before replying, mailbox full, request timed out, or a
/// startup panic).
#[derive(Debug, Error)]
pub enum MessageDaemonError {
    #[error("daemon frame error: {0}")]
    Frame(#[from] FrameError),

    #[error("daemon signal frame error: {0}")]
    SignalFrame(#[from] SignalFrameError),

    #[error("daemon engine actor error: {0}")]
    EngineRequest(#[from] EngineRequestError),

    #[error("daemon component error: {0}")]
    Component(#[from] MessageError),
}

impl ComponentDaemon for MessageDaemon {
    type Configuration = Configuration;
    type ConfigurationError = ConfigurationError;
    type Engine = MessageEngine;
    type Error = MessageDaemonError;

    const PROCESS_NAME: &'static str = "message-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> Result<Self::Configuration, Self::ConfigurationError> {
        Configuration::from_binary_path(path)
    }

    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        Ok(MessageEngine::from_configuration(configuration))
    }

    async fn handle_working_input(
        engine: &mut Self::Engine,
        input: Input,
        connection: &triad_runtime::ConnectionContext,
    ) -> Result<Output, Self::Error> {
        Ok(engine.handle(input, connection).await?)
    }

    async fn handle_meta_connection(
        _engine: &mut Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let codec = MetaMessageFrameCodec::default();
        let first_frame = codec.read_frame_bytes(connection.stream_mut()).await?;
        match codec.decode_request_frame(&first_frame) {
            Ok((exchange, operation)) => {
                codec
                    .write_unimplemented_reply(connection.stream_mut(), exchange, operation)
                    .await?;
                Ok(())
            }
            Err(MetaMessageFrameDecode::NotMeta) => {
                Err(MessageError::UnexpectedMetaFrame("expected meta-signal-message frame").into())
            }
            Err(MetaMessageFrameDecode::UnexpectedFrame(message)) => {
                Err(MessageError::UnexpectedMetaFrame(message).into())
            }
        }
    }
}
