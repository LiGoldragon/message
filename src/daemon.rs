//! Two-listener messenger daemon over the canonical ordinary and meta frames.

use std::fmt::{Display, Formatter};

use signal::{ByteViewable, Restorable, Signal, Signalizable};
use signal_message::Query;
use thiserror::Error;
use triad_runtime::{
    AcceptedConnection, AsyncListenerSocket, AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon,
    AsyncMultiListenerDaemonError, FrameBody, LengthPrefixedCodec, RequestErrorLog,
};

use crate::{
    Configuration, ConfigurationError, Error as MessageError, MessageEngine,
    meta::MetaMessageFrameCodec,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ListenerRole {
    Ordinary,
    PromptRelay,
    Owner,
}

impl Display for ListenerRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary => formatter.write_str("ordinary"),
            Self::PromptRelay => formatter.write_str("prompt-relay"),
            Self::Owner => formatter.write_str("owner"),
        }
    }
}

#[derive(Debug)]
pub struct MessageDaemon {
    configuration: Configuration,
}

impl MessageDaemon {
    pub fn new(configuration: Configuration) -> Self {
        Self { configuration }
    }

    pub fn from_configuration_path(path: &std::path::Path) -> Result<Self, MessageDaemonError> {
        Ok(Self::new(Configuration::from_binary_path(path)?))
    }

    pub fn run(self) -> Result<(), MessageDaemonError> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(MessageDaemonError::Runtime)?
            .block_on(self.run_async())
    }

    async fn run_async(self) -> Result<(), MessageDaemonError> {
        let mut sockets = vec![
            AsyncListenerSocket::new(
                ListenerRole::Ordinary,
                self.configuration.socket_path().to_path_buf(),
            )
            .with_socket_mode(self.configuration.socket_mode()),
            AsyncListenerSocket::new(
                ListenerRole::Owner,
                self.configuration.meta_socket_path().to_path_buf(),
            )
            .with_socket_mode(self.configuration.meta_socket_mode()),
        ];
        for ingress in &self.configuration.contract().component_ingresses {
            sockets.push(
                AsyncListenerSocket::new(
                    ListenerRole::PromptRelay,
                    std::path::PathBuf::from(&ingress.ingress_socket_path),
                )
                .with_socket_mode(triad_runtime::SocketMode::new(ingress.socket_mode as u32)),
            );
        }
        let runtime = MessageRuntime {
            engine: tokio::sync::Mutex::new(MessageEngine::from_configuration(
                &self.configuration,
            )?),
            ordinary_codec: LengthPrefixedCodec::default(),
            meta_codec: MetaMessageFrameCodec::default(),
        };
        AsyncMultiListenerDaemon::new(sockets, runtime, RequestErrorLog::new("message-daemon"))
            .run()
            .await
            .map_err(MessageDaemonError::from_daemon)
    }
}

struct MessageRuntime {
    engine: tokio::sync::Mutex<MessageEngine>,
    ordinary_codec: LengthPrefixedCodec,
    meta_codec: MetaMessageFrameCodec,
}

impl AsyncMultiConnectionRuntime for MessageRuntime {
    type Listener = ListenerRole;
    type Error = MessageDaemonError;

    async fn handle_connection(
        &self,
        listener: Self::Listener,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        match listener {
            ListenerRole::Ordinary => {
                let body = self
                    .ordinary_codec
                    .read_body_async(connection.stream_mut())
                    .await?;
                let query = Signal::<Query>::from(body.bytes().to_vec()).restore()?;
                let context = *connection.context();
                let response = self.engine.lock().await.handle(query, &context).await?;
                self.ordinary_codec
                    .write_body_async(
                        connection.stream_mut(),
                        &FrameBody::new(response.signalize()?.bytes().to_vec()),
                    )
                    .await?;
                Ok(())
            }
            ListenerRole::PromptRelay => {
                let body = self
                    .ordinary_codec
                    .read_body_async(connection.stream_mut())
                    .await?;
                let query = Signal::<Query>::from(body.bytes().to_vec()).restore()?;
                if !matches!(
                    query,
                    Query::SubmitPrompt(_) | Query::ObservePromptReceipt(_)
                ) {
                    return Err(MessageDaemonError::Listener(
                        "prompt relay ingress only accepts typed prompt operations".into(),
                    ));
                }
                let context = *connection.context();
                let response = self.engine.lock().await.handle(query, &context).await?;
                self.ordinary_codec
                    .write_body_async(
                        connection.stream_mut(),
                        &FrameBody::new(response.signalize()?.bytes().to_vec()),
                    )
                    .await?;
                Ok(())
            }
            ListenerRole::Owner => {
                let operation = self
                    .meta_codec
                    .read_request(connection.stream_mut())
                    .await?;
                self.meta_codec
                    .write_unimplemented_reply(connection.stream_mut(), operation)
                    .await?;
                Ok(())
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum MessageDaemonError {
    #[error("configuration: {0}")]
    Configuration(#[from] ConfigurationError),
    #[error("runtime construction: {0}")]
    Runtime(std::io::Error),
    #[error("listener runtime: {0}")]
    Listener(String),
    #[error("component: {0}")]
    Component(#[from] MessageError),
    #[error("message archive: {0}")]
    Archive(#[from] rkyv::rancor::Error),
    #[error("transport frame: {0}")]
    TransportFrame(#[from] triad_runtime::FrameError),
}

impl MessageDaemonError {
    fn from_daemon(error: AsyncMultiListenerDaemonError<Self>) -> Self {
        match error {
            AsyncMultiListenerDaemonError::Listener(error) => Self::Listener(error.to_string()),
            AsyncMultiListenerDaemonError::Start(error)
            | AsyncMultiListenerDaemonError::Stop(error) => error,
        }
    }
}
