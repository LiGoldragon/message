//! Binary startup configuration for `message-daemon`.
//!
//! The public socket, owner, and ingress policy is the exact producer-owned
//! `MessageDaemonConfiguration` coordinate (`MessageDaemonConfiguration`). The messenger adds only
//! its private durable-store path and sender fallback label; those values are
//! runtime state, not a second wire contract.

use std::{fs, path::Path};

use signal_message::{MessageDaemonConfiguration, OwnerIdentity};
use thiserror::Error;
use triad_runtime::SocketMode;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq)]
pub struct Configuration {
    contract: MessageDaemonConfiguration,
    database_path: RuntimePath,
    owner_label: String,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct RuntimePath(String);

impl RuntimePath {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(path.as_ref().to_string_lossy().into_owned())
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl Configuration {
    pub fn new(
        contract: MessageDaemonConfiguration,
        database_path: impl AsRef<Path>,
        owner_label: impl Into<String>,
    ) -> Result<Self, ConfigurationError> {
        let value = Self {
            contract,
            database_path: RuntimePath::new(database_path),
            owner_label: owner_label.into(),
        };
        value.validate()?;
        Ok(value)
    }

    pub fn contract(&self) -> &MessageDaemonConfiguration {
        &self.contract
    }

    pub fn socket_path(&self) -> &Path {
        Path::new(&self.contract.message_socket_path)
    }

    pub fn socket_mode(&self) -> SocketMode {
        SocketMode::new(self.contract.message_socket_mode as u32)
    }

    pub fn meta_socket_path(&self) -> &Path {
        Path::new(&self.contract.supervision_socket_path)
    }

    pub fn meta_socket_mode(&self) -> SocketMode {
        SocketMode::new(self.contract.supervision_socket_mode as u32)
    }

    pub fn database_path(&self) -> &Path {
        self.database_path.as_path()
    }

    pub fn prompt_relay_permissions(&self) -> &[signal_message::PromptRelayPermission] { &self.contract.prompt_relay_permissions }

    pub fn owner_label(&self) -> &str {
        &self.owner_label
    }

    pub fn owner_user_id(&self) -> u32 {
        match &self.contract.owner_identity {
            OwnerIdentity::UnixUser(identifier) => *identifier as u32,
            OwnerIdentity::System(_) => unreachable!("validated Unix owner configuration"),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigurationError> {
        for (surface, mode) in [
            ("message", self.contract.message_socket_mode),
            ("supervision", self.contract.supervision_socket_mode),
        ] {
            if mode < 0 || mode > i64::from(u32::MAX) {
                return Err(ConfigurationError::SocketModeOutOfRange { surface, mode });
            }
        }
        match &self.contract.owner_identity {
            OwnerIdentity::UnixUser(identifier)
                if *identifier >= 0 && *identifier <= i64::from(u32::MAX) =>
            {
                Ok(())
            }
            OwnerIdentity::UnixUser(identifier) => {
                Err(ConfigurationError::OwnerUserOutOfRange { value: *identifier })
            }
            OwnerIdentity::System(_) => Err(ConfigurationError::SystemOwnerUnsupported),
        }
    }

    pub fn from_binary_path(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        let bytes = fs::read(path).map_err(ConfigurationError::Read)?;
        Self::from_binary_bytes(&bytes)
    }

    pub fn from_binary_bytes(bytes: &[u8]) -> Result<Self, ConfigurationError> {
        let value = rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|_| ConfigurationError::ArchiveDecode)?;
        value.validate()?;
        Ok(value)
    }

    pub fn to_binary_bytes(&self) -> Result<Vec<u8>, ConfigurationError> {
        self.validate()?;
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ConfigurationError::ArchiveEncode)
    }

    pub fn write_binary_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigurationError> {
        fs::write(path, self.to_binary_bytes()?).map_err(ConfigurationError::Write)
    }
}

/// The one inline Datom value `message-write-configuration` takes.
///
/// This is the component's startup text surface: a peer that launches the
/// daemon writes this value, so its shape is public and gated by a test
/// rather than buried in a binary.
#[derive(Debug, Clone, PartialEq, datom_codec::Datomizable, datom_codec::Composing)]
pub struct ConfigurationWriteRequest {
    pub contract: MessageDaemonConfiguration,
    pub database_path: String,
    pub owner_label: String,
    pub output_path: String,
}

/// What `message-write-configuration` prints when it has written the file.
#[derive(Debug, Clone, PartialEq, datom_codec::Datomizable, datom_codec::Composing)]
pub struct ConfigurationWritten {
    pub output_path: String,
}

impl ConfigurationWriteRequest {
    /// Validate the request and write the binary configuration it names.
    pub fn write(self) -> Result<ConfigurationWritten, ConfigurationError> {
        let configuration = Configuration::new(
            self.contract,
            Path::new(&self.database_path),
            self.owner_label,
        )?;
        configuration.write_binary_file(Path::new(&self.output_path))?;
        Ok(ConfigurationWritten {
            output_path: self.output_path,
        })
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("failed to read binary configuration: {0}")]
    Read(std::io::Error),
    #[error("failed to write binary configuration: {0}")]
    Write(std::io::Error),
    #[error("failed to encode binary configuration")]
    ArchiveEncode,
    #[error("failed to decode binary configuration")]
    ArchiveDecode,
    #[error("{surface} socket mode {mode} does not fit the operating-system mode width")]
    SocketModeOutOfRange { surface: &'static str, mode: i64 },
    #[error("owner Unix user identifier {value} does not fit the operating-system uid width")]
    OwnerUserOutOfRange { value: i64 },
    #[error("the messenger runtime currently requires a Unix-user owner")]
    SystemOwnerUnsupported,
}
