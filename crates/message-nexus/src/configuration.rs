//! The initial socket configuration, derived from the two anchors.

use message_defaults::{DefaultConfiguration, LaysOutDefaults};
use message_library::Configuration;
use std::path::Path;

pub trait SeedsConfiguration {
    fn message_configuration(&self) -> Configuration;
}

trait CarriedAsText {
    fn carried(&self) -> String;
}

impl CarriedAsText for Path {
    fn carried(&self) -> String {
        self.to_string_lossy().into_owned()
    }
}

impl SeedsConfiguration for DefaultConfiguration {
    fn message_configuration(&self) -> Configuration {
        Configuration {
            ordinary: self.ordinary_socket_path().carried(),
            meta: self.meta_socket_path().carried(),
            flow: self.flow_socket_path().carried(),
        }
    }
}
