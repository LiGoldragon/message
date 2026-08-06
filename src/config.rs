//! Binary startup configuration for `message-daemon`.
//!
//! The public socket, owner, and ingress policy is the exact producer-owned
//! `MessageDaemonConfiguration` coordinate (`z2VL2C`). The messenger adds only
//! its private durable-store path and sender fallback label; those values are
//! runtime state, not a second wire contract.

use std::{fs, path::Path};

use signal_message::schema::lib::{z2VL2C, z2VUqb};
use thiserror::Error;
use triad_runtime::SocketMode;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    contract: z2VL2C,
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
        contract: z2VL2C,
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

    pub fn contract(&self) -> &z2VL2C {
        &self.contract
    }

    pub fn socket_path(&self) -> &Path {
        Path::new(self.contract.field_0.payload().payload())
    }

    pub fn socket_mode(&self) -> SocketMode {
        SocketMode::new(*self.contract.field_1.payload().payload() as u32)
    }

    pub fn meta_socket_path(&self) -> &Path {
        Path::new(self.contract.field_2.payload().payload())
    }

    pub fn meta_socket_mode(&self) -> SocketMode {
        SocketMode::new(*self.contract.field_3.payload().payload() as u32)
    }

    pub fn database_path(&self) -> &Path {
        self.database_path.as_path()
    }

    pub fn owner_label(&self) -> &str {
        &self.owner_label
    }

    pub fn owner_user_id(&self) -> u32 {
        match &self.contract.field_6 {
            z2VUqb::z2Vd9P(identifier) => *identifier.payload() as u32,
            z2VUqb::z2VZGs(_) => unreachable!("validated Unix owner configuration"),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigurationError> {
        for (surface, mode) in [
            ("message", *self.contract.field_1.payload().payload()),
            ("supervision", *self.contract.field_3.payload().payload()),
        ] {
            if mode > u64::from(u32::MAX) {
                return Err(ConfigurationError::SocketModeOutOfRange { surface, mode });
            }
        }
        match &self.contract.field_6 {
            z2VUqb::z2Vd9P(identifier) if *identifier.payload() <= u64::from(u32::MAX) => Ok(()),
            z2VUqb::z2Vd9P(identifier) => Err(ConfigurationError::OwnerUserOutOfRange {
                value: *identifier.payload(),
            }),
            z2VUqb::z2VZGs(_) => Err(ConfigurationError::SystemOwnerUnsupported),
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
    SocketModeOutOfRange { surface: &'static str, mode: u64 },
    #[error("owner Unix user identifier {value} does not fit the operating-system uid width")]
    OwnerUserOutOfRange { value: u64 },
    #[error("the messenger runtime currently requires a Unix-user owner")]
    SystemOwnerUnsupported,
}
