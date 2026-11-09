//! Message routes typed requests through Flow.

extern crate signal_message as message_library;

pub mod configuration;
pub mod delivery_consumer;
pub mod flow_edge;
pub mod frame;
pub mod generated;
pub mod listener;
pub mod nexus;
pub mod peer;
pub mod placed_socket;
pub mod request_tracking;
pub mod service;
pub mod store;

pub use listener::ListensOnSockets;
pub use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
pub use nexus::{MessageNexus, OpensNexus};
