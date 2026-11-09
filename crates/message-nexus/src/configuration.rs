//! The initial socket configuration, derived from the two anchors.

use message_defaults::{DefaultConfiguration, LaysOutDefaults};
use message_library::{Configuration, Flow, Meta, Ordinary};
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
            ordinary: Ordinary(self.ordinary_socket_path().carried()),
            meta: Meta(self.meta_socket_path().carried()),
            flow: Flow(self.flow_socket_path().carried()),
        }
    }
}
