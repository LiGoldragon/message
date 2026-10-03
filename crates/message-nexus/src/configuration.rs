//! The configuration a new store is seeded with.
//!
//! A Nexus starts with no arguments. Its defaults come from the user's home
//! and runtime directory through `message-defaults`, as Flow's do: the store
//! at `~/.local/state/message/message.sema`, its own sockets under
//! `$XDG_RUNTIME_DIR/message/`, and Flow's under `$XDG_RUNTIME_DIR/flow/`.
//! A new store persists these defaults; a populated store resumes what it
//! holds, and meta Configure changes it.

use message_defaults::{DefaultConfiguration, LaysOutDefaults};
use meta_signal_message::MessageConfiguration;
use signal_flow::FlowAspect;
use std::path::Path;

/// The whole Configure value the defaults stand for.
pub trait SeedsConfiguration {
    /// Psyche seats deploy and manage flows, so they alone among flows
    /// reach Message's meta socket by default, as they do Flow's.
    fn message_configuration(&self) -> MessageConfiguration;
}

/// A path as the wire carries it.
trait CarriedAsText {
    fn carried(&self) -> String;
}

impl CarriedAsText for Path {
    fn carried(&self) -> String {
        self.to_string_lossy().into_owned()
    }
}

impl SeedsConfiguration for DefaultConfiguration {
    fn message_configuration(&self) -> MessageConfiguration {
        MessageConfiguration {
            ordinary_socket_path: self.ordinary_socket_path().carried(),
            meta_socket_path: self.meta_socket_path().carried(),
            flow_socket_path: self.flow_socket_path().carried(),
            flow_meta_socket_path: self.flow_meta_socket_path().carried(),
            meta_aspects: vec![FlowAspect::Psyche],
        }
    }
}
