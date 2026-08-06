//! Two-listener messenger daemon over the canonical ordinary and meta frames.

use std::fmt::{Display, Formatter};

use signal_message::schema::lib::ContractMarker;
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
    Owner,
}

impl Display for ListenerRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary => formatter.write_str("ordinary"),
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
        let sockets = vec![
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
                let (exchange, input) = ContractMarker::decode_single_request(body.bytes())?;
                let context = *connection.context();
                let output = self.engine.lock().await.handle(input, &context).await?;
                self.ordinary_codec
                    .write_body_async(
                        connection.stream_mut(),
                        &FrameBody::new(output.encode_reply_frame(exchange)?),
                    )
                    .await?;
                Ok(())
            }
            ListenerRole::Owner => {
                let (exchange, operation) = self
                    .meta_codec
                    .read_request(connection.stream_mut())
                    .await?;
                self.meta_codec
                    .write_unimplemented_reply(connection.stream_mut(), exchange, operation)
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
    #[error("ordinary frame: {0}")]
    OrdinaryFrame(#[from] signal_message::schema::lib::SignalFrameError),
    #[error("meta frame: {0}")]
    MetaFrame(#[from] meta_signal_message::schema::lib::SignalFrameError),
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
