//! Message Nexus.
//!
//! Message owns everything durable about a message: the record, its
//! Priority, the sender (named from the peer, never the payload), the
//! ledger of receipts with every grade kept separate, parking until Flow
//! shows the recipient at rest, and the recipient's own Read. Flow is the
//! only pane writer: Message reaches a pane only through Flow's typed
//! Deliver, and never runs Herdr.

pub mod configuration;
pub mod delivery;
pub mod flow_edge;
pub mod frame;
pub mod ledger;
pub mod listener;
pub mod nexus;
pub mod peer;
pub mod service;
pub mod store;

pub use listener::ListensOnSockets;
pub use message_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
pub use nexus::{MessageNexus, OpensNexus};
