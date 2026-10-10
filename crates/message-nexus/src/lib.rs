//! Message Nexus: typed requests delivered through Flow.
//!
//! Message retains only its configuration. Flow owns recipient resolution,
//! queueing and the waking rule; Message never writes a pane or runs Herdr.

pub mod configuration;
pub mod delivery;
pub mod flow_edge;
pub mod frame;
pub mod generated;
pub mod ledger;
pub mod listener;
pub mod nexus;
pub mod peer;
pub mod service;
pub mod store;

pub use listener::ListensOnSockets;
pub use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
pub use nexus::{MessageNexus, OpensNexus};
