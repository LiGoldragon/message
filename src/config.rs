//! The daemon's typed startup configuration, loaded as a binary rkyv file
//! from the single argv argument (the single-argument rule).
//!
//! The emitted daemon (`schema/daemon.rs`) reads its socket layout through the
//! `triad_runtime::BindingSurface` trait. Message extends the uniform
//! surface with the two fields its forward-only runtime needs that no durable
//! store would supply: the router socket it forwards stamped submissions to,
//! the owner identity gate and local instance name it stamps onto submissions.
//! Message owns no durable database, so `database_path()` names an
//! unused-on-the-forward-path location kept only to satisfy the uniform trait
//! surface.

use std::{fs, path::Path};

use thiserror::Error;
use triad_runtime::{BindingSurface, SocketMode};

const MESSAGE_SOCKET_MODE: u32 = 0o660;
const META_SOCKET_MODE: u32 = 0o600;

/// Binary rkyv startup configuration for `message-daemon`.
///
/// `owner_user_id` is the local Unix user identifier of the engine owner. The
/// daemon compares it against each accepted connection's kernel-vouched peer uid
/// (`SO_PEERCRED`, read through `triad_runtime::ConnectionContext`) to gate
/// trust: a peer uid that matches the owner stamps the configured
/// `owner_name` as this daemon's local harness component instance; any other
/// local user is `NonOwnerUser(uid)`. Provenance is never accepted from the
/// caller payload — the daemon mints it from the operating-system trust
/// boundary plus daemon configuration.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    socket_path: ConfigurationPath,
    socket_mode: u32,
    meta_socket_path: ConfigurationPath,
    meta_socket_mode: u32,
    router_socket_path: ConfigurationPath,
    database_path: ConfigurationPath,
    owner_name: String,
    owner_user_id: u32,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct ConfigurationPath(String);

impl ConfigurationPath {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(path.as_ref().to_string_lossy().into_owned())
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl Configuration {
    pub fn new(
        socket_path: impl AsRef<Path>,
        meta_socket_path: impl AsRef<Path>,
        router_socket_path: impl AsRef<Path>,
        database_path: impl AsRef<Path>,
        owner_name: impl Into<String>,
        owner_user_id: u32,
    ) -> Self {
        Self {
            socket_path: ConfigurationPath::new(socket_path),
            socket_mode: MESSAGE_SOCKET_MODE,
            meta_socket_path: ConfigurationPath::new(meta_socket_path),
            meta_socket_mode: META_SOCKET_MODE,
            router_socket_path: ConfigurationPath::new(router_socket_path),
            database_path: ConfigurationPath::new(database_path),
            owner_name: owner_name.into(),
            owner_user_id,
        }
    }

    pub fn socket_path(&self) -> &Path {
        self.socket_path.as_path()
    }

    pub fn socket_mode(&self) -> SocketMode {
        SocketMode::new(self.socket_mode)
    }

    pub fn meta_socket_path(&self) -> &Path {
        self.meta_socket_path.as_path()
    }

    pub fn meta_socket_mode(&self) -> SocketMode {
        SocketMode::new(self.meta_socket_mode)
    }

    pub fn router_socket_path(&self) -> &Path {
        self.router_socket_path.as_path()
    }

    pub fn database_path(&self) -> &Path {
        self.database_path.as_path()
    }

    pub fn owner_name(&self) -> &str {
        &self.owner_name
    }

    /// The local Unix user identifier of the engine owner, compared against an
    /// accepted connection's peer uid to classify provenance.
    pub fn owner_user_id(&self) -> u32 {
        self.owner_user_id
    }

    pub fn from_binary_path(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        let bytes = fs::read(path).map_err(ConfigurationError::Read)?;
        Self::from_binary_bytes(&bytes)
    }

    pub fn from_binary_bytes(bytes: &[u8]) -> Result<Self, ConfigurationError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|_| ConfigurationError::ArchiveDecode)
    }

    pub fn to_binary_bytes(&self) -> Result<Vec<u8>, ConfigurationError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ConfigurationError::ArchiveEncode)
    }

    pub fn write_binary_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigurationError> {
        fs::write(path, self.to_binary_bytes()?).map_err(ConfigurationError::Write)
    }
}

impl BindingSurface for Configuration {
    fn socket_path(&self) -> &Path {
        Configuration::socket_path(self)
    }

    fn socket_mode(&self) -> Option<SocketMode> {
        Some(Configuration::socket_mode(self))
    }

    fn database_path(&self) -> &Path {
        Configuration::database_path(self)
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(Configuration::meta_socket_path(self))
    }

    fn meta_socket_mode(&self) -> Option<SocketMode> {
        Some(Configuration::meta_socket_mode(self))
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
}
