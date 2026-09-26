//! Where the Nexus lives by default, before any Configure.
//!
//! A Nexus starts with no arguments. Its defaults come from the user's
//! home and runtime directory, as Flow's do: the store at
//! `~/.local/state/message/message.sema`, its own sockets under
//! `$XDG_RUNTIME_DIR/message/`, and Flow's under `$XDG_RUNTIME_DIR/flow/`.
//! A new store persists these defaults; a populated store resumes what it
//! holds, and meta Configure changes it.

use meta_signal_message::MessageConfiguration;
use signal_flow::FlowAspect;
use std::{os::unix::fs::MetadataExt, path::PathBuf};

/// The two directories every default path hangs from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultConfiguration {
    home: PathBuf,
    runtime_directory: PathBuf,
}

impl DefaultConfiguration {
    const STATE_DIRECTORY: &'static str = ".local/state/message";
    const STORE_FILE: &'static str = "message.sema";

    pub fn new(home: PathBuf, runtime_directory: PathBuf) -> Self {
        Self {
            home,
            runtime_directory,
        }
    }

    /// Reads HOME and XDG_RUNTIME_DIR, falling back to `/run/user/<uid>`.
    pub fn from_environment() -> Self {
        let user_id = std::fs::metadata("/proc/self")
            .map(|metadata| metadata.uid())
            .unwrap_or(0);
        let absolute = |name: &str| {
            std::env::var_os(name)
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
        };
        Self {
            home: absolute("HOME").unwrap_or_else(|| PathBuf::from("/")),
            runtime_directory: absolute("XDG_RUNTIME_DIR")
                .unwrap_or_else(|| PathBuf::from(format!("/run/user/{user_id}"))),
        }
    }

    pub fn state_directory(&self) -> PathBuf {
        self.home.join(Self::STATE_DIRECTORY)
    }

    pub fn store_path(&self) -> PathBuf {
        self.state_directory().join(Self::STORE_FILE)
    }

    fn runtime(&self, relative: &str) -> String {
        self.runtime_directory
            .join(relative)
            .to_string_lossy()
            .into_owned()
    }

    /// Psyche seats deploy and manage flows, so they alone among flows
    /// reach Message's meta socket by default, as they do Flow's.
    pub fn message_configuration(&self) -> MessageConfiguration {
        MessageConfiguration {
            ordinary_socket_path: self.runtime("message/message.sock"),
            meta_socket_path: self.runtime("message/message-owner.sock"),
            flow_socket_path: self.runtime("flow/flow.sock"),
            flow_meta_socket_path: self.runtime("flow/flow-meta.sock"),
            meta_aspects: vec![FlowAspect::Psyche],
        }
    }
}
